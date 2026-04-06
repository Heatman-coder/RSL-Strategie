import math
import logging
import numpy as np
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import defaultdict
import scipy.stats

logger = logging.getLogger(__name__)


def _build_summary_lookup(summary: Any, key_col: str) -> Dict[str, Dict[str, Any]]:
    if not hasattr(summary, "empty") or summary is None or summary.empty:
        return {}
    if key_col not in summary.columns:
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for _, row in summary.iterrows():
        key = str(row.get(key_col, "")).strip()
        if key:
            lookup[key] = row.to_dict()
    return lookup


def _resolve_base_label(stock: Any, config: Dict[str, Any]) -> str:
    if bool(config.get("candidate_use_momentum_score", True)):
        if bool(config.get("candidate_use_vol_adjust", True)) and getattr(stock, "mom_score_adj", None) is not None:
            return "MomScoreAdj"
        if getattr(stock, "mom_score", None) is not None:
            return "MomScore"
    rank = getattr(stock, "rsl_rang", None)
    if rank:
        return f"RSL-{rank}"
    return "RSL"


def _resolve_base_score(stock: Any, config: Dict[str, Any]) -> float:
    if bool(config.get("candidate_use_momentum_score", True)):
        if bool(config.get("candidate_use_vol_adjust", True)) and getattr(stock, "mom_score_adj", None) is not None:
            return float(stock.mom_score_adj)
        if getattr(stock, "mom_score", None) is not None:
            return float(stock.mom_score)
    return float(getattr(stock, "rsl", 0.0) or 0.0)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return float(default)
        return float(value)
    except Exception:
        return float(default)

def suggest_portfolio_candidates(
    stock_results: List[Any],
    industry_summary: Any,
    cluster_summary: Any,
    portfolio_symbols: Set[str],
    sell_list_symbols: Set[str],
    symbol_lookup: Dict[str, Any],
    config: Dict[str, Any],
    market_regime: Optional[Dict[str, Any]] = None,
    top_n_industries: Optional[int] = None,
    max_stocks_per_sector: int = 2,
    portfolio_size: int = 5,
    return_details: bool = False,
) -> Any:
    """
    Institutional Grade Candidate Selection Engine (v2).
    Implementiert Factor Orthogonalization, Sector-Neutral Z-Scoring 
    und explizite Alpha/Risk-Gewichtung.
    """
    # 1. Hygiene & Universe Setup
    # FIX 6: Stats auf dem GESAMTEN Universum berechnen (Selection Bias vermeiden),
    # nicht nur auf den gefilterten Kandidaten.
    
    # Settings
    # FIX: Robustheit gegen falsche Typen (falls config als Tuple kommt)
    if not isinstance(config, dict):
        logger.warning(f"WARNUNG: 'config' ist kein Dict, sondern {type(config)}. Nutze Fallback.")
        config = {}

    # Pre-extract configuration values for performance
    conf_get = config.get
    min_trust = int(conf_get('candidate_min_trust_score', 0))
    min_vol = float(config.get('candidate_min_avg_volume_eur', 0.0))
    min_mktcap = float(config.get('candidate_min_mktcap_m_eur', 0.0)) * 1_000_000
    excluded_countries = [c.strip().upper() for c in config.get('candidate_excluded_countries', [])]
    block_weak = bool(config.get('candidate_block_new_buys_in_weak_regime', False))
    accel_weight = float(config.get("candidate_accel_weight", 0.15))
    rsl_1w_weight = float(config.get("candidate_rsl_change_weight", 0.10))
    peer_weight = float(config.get("candidate_peer_spread_weight", 0.45))
    max_distance_52w_high_pct = float(config.get("candidate_max_distance_52w_high_pct", 0.0))
    
    regime_status = "NORMAL"
    if market_regime is not None and isinstance(market_regime, dict):
        regime_status = market_regime.get('regime', 'NORMAL')
    elif market_regime:
        logger.warning(f"WARNUNG: 'market_regime' ist kein Dict, sondern {type(market_regime)}. Ignoriere Regime-Status.")

    if block_weak and regime_status == "SCHWACH":
        logger.info("Marktregime SCHWACH: Keine neuen Kaufsignale generiert.")
        return []

    # Analyse-Pool: Alle Aktien mit validen Daten (fuer Stats)
    analysis_pool = []
    for s in stock_results:
        # Grobe Datenfehler rausfiltern, aber NICHT RSL > 1.0 filtern!
        if s.kurs < 0.1 or s.rsl <= 0.0: continue
        analysis_pool.append(s)

    if not analysis_pool:
        return []

    # 2. Data Preparation & Orthogonalization
    # Wir extrahieren Vektoren fuer die vektorisierte Berechnung
    # FIX: Nutze Raw Momentum (mom_score) statt Risk-Adjusted (mom_score_adj),
    # da Volatilitaet explizit im Risk-Modell (z_vol) beruecksichtigt wird.
    mom_vec = np.asarray([_coerce_float(s.mom_score, _coerce_float(s.mom_score_adj, s.rsl - 1.0)) for s in analysis_pool], dtype=float).ravel()
    accel_vec = np.asarray([_coerce_float(s.mom_accel) for s in analysis_pool], dtype=float).ravel()
    dd_vec = np.asarray([abs(_coerce_float(s.max_drawdown_6m)) for s in analysis_pool], dtype=float).ravel()
    peer_vec = np.asarray([_coerce_float(s.peer_spread) for s in analysis_pool], dtype=float).ravel()
    vol_vec = np.asarray([_coerce_float(s.mom_vol, 0.2) for s in analysis_pool], dtype=float).ravel()

    # FIX 1: Stabilisierte Dynamische Shrinkage via Spearman-Rangkorrelation
    def _dynamic_shrink(target_v, base_v):
        t_v = np.asarray(target_v, dtype=float).ravel()
        b_v = np.asarray(base_v, dtype=float).ravel()
        if t_v.size < 10 or b_v.size < 10: return 0.5
        
        # FIX: Vorab-Check auf Varianz, um Spearman ConstantInputWarning zu vermeiden
        if np.std(t_v) == 0 or np.std(b_v) == 0:
            return 0.5

        try:
            # Spearman ist robuster gegen Fat-Tails und instabile Regimes
            res = scipy.stats.spearmanr(t_v, b_v)
            # Statistik extrahieren und sicherstellen, dass es ein skalarer float ist
            corr = float(getattr(res, 'statistic', res[0]))
        except Exception:
            return 0.5

        # Fallback fuer konstante Zeitreihen (Standardabweichung 0)
        if math.isnan(corr) or math.isinf(corr): 
            return 0.5
        return float(np.clip(abs(corr), 0.3, 0.8))

    shrink_accel_mom = _dynamic_shrink(accel_vec, mom_vec)
    shrink_peer_mom = float(_dynamic_shrink(peer_vec, mom_vec))
    shrink_peer_accel = float(_dynamic_shrink(peer_vec, accel_vec))
    shrink_dd_mom = float(_dynamic_shrink(dd_vec, mom_vec))
    shrink_dd_accel = float(_dynamic_shrink(dd_vec, accel_vec))
    shrink_dd_peer = float(_dynamic_shrink(dd_vec, peer_vec))

    # FIX 3: Gram-Schmidt Style Orthogonalisierung (Sequential & Clean)
    # Verhindert Factor-Leakage durch saubere Trennung der Signale
    
    # 1. Accel gegen Momentum (Partial: dynamische Shrinkage)
    accel_res = orthogonalize_multi(accel_vec.tolist(), [mom_vec.tolist()], shrinkage=shrink_accel_mom)
    
    # 2. Peer gegen Momentum UND Accel (bereinigt)
    # FIX: Durchschnittliche Shrinkage statt Max, um Signalverlust zu vermeiden
    peer_shrink = float(np.mean([shrink_peer_mom, shrink_peer_accel]))
    peer_res = orthogonalize_multi(peer_vec.tolist(), [mom_vec.tolist(), accel_res], shrinkage=peer_shrink)
    
    # 3. Drawdown gegen Momentum, Accel UND Peer (alle bereinigt)
    dd_shrink = float(np.mean([shrink_dd_mom, shrink_dd_accel, shrink_dd_peer]))
    dd_res = orthogonalize_multi(dd_vec.tolist(), [mom_vec.tolist(), accel_res, peer_res], shrinkage=dd_shrink)
    
    # 3. Sector-Neutral Z-Scoring (Within-Sector Standardization)
    # Wir berechnen Z-Scores pro Sektor. Fallback auf Global, wenn Sektor zu klein.
    sector_map = defaultdict(list)
    for idx, s in enumerate(analysis_pool):
        sector_map[s.sector or "Unknown"].append(idx)

    # Globale Stats als Fallback
    global_stats = {
        'mom': _compute_robust_stats(mom_vec.tolist()),
        'accel': _compute_robust_stats(accel_res),
        'peer': _compute_robust_stats(peer_res),
        'dd': _compute_robust_stats(dd_res),
        'vol': _compute_robust_stats(vol_vec.tolist())
    }

    z_scores: List[Dict[str, float]] = [{} for _ in analysis_pool]

    for sector, indices in sector_map.items():
        # FIX 2: Sector Blending statt Hard Switch (Pro-Level Stabilitaet)
        # Kleine Sektoren verursachen instabile Z-Scores -> Blending mit Global Stats
        size = len(indices)
        w = min(1.0, size / 30.0) # Ab 30 Aktien voll sektor-neutral, darunter blending
        
        # Sektor-lokale Daten extrahieren
        s_mom = [mom_vec[i] for i in indices]
        s_accel = [accel_res[i] for i in indices]
        s_peer = [peer_res[i] for i in indices] # Nutzung der bereinigten Werte
        s_dd = [dd_res[i] for i in indices]
        s_vol = [vol_vec[i] for i in indices]
        
        local_stats = {
            'mom': _compute_robust_stats(s_mom), 'accel': _compute_robust_stats(s_accel),
            'peer': _compute_robust_stats(s_peer), 'dd': _compute_robust_stats(s_dd),
            'vol': _compute_robust_stats(s_vol)
        }
        
        # Blending der Statistiken
        stats_source = {
            'mom': _blend_stats(local_stats['mom'], global_stats['mom'], w, global_stats['mom'][1]),
            'accel': _blend_stats(local_stats['accel'], global_stats['accel'], w, global_stats['accel'][1]),
            'peer': _blend_stats(local_stats['peer'], global_stats['peer'], w, global_stats['peer'][1]),
            'dd': _blend_stats(local_stats['dd'], global_stats['dd'], w, global_stats['dd'][1]),
            'vol': _blend_stats(local_stats['vol'], global_stats['vol'], w, global_stats['vol'][1])
        }
        
        for i_local, idx_global in enumerate(indices):
            z_scores[idx_global] = {
                'z_mom': float(_zscore(float(mom_vec[idx_global]), *stats_source['mom'])),
                'z_accel': float(_zscore(float(accel_res[idx_global]), *stats_source['accel'])),
                'z_peer': float(_zscore(float(peer_res[idx_global]), *stats_source['peer'])),
                'z_dd': float(_zscore(float(dd_res[idx_global]), *stats_source['dd'])),
                'z_vol': float(_zscore(float(vol_vec[idx_global]), *stats_source['vol'])),
                'is_sector_neutral': float(w) # Speichern des Blending-Gewichts fuer Transparenz
            }
    
    # 4. Scoring Loop
    scored_candidates = []
    
    # Soft-Filter Definitionen
    if top_n_industries is None:
        top_n_industries = int(config.get('industry_top_n', 15))
    
    allowed_industries = set()
    if hasattr(industry_summary, 'head'): # Check if DataFrame
        if not industry_summary.empty and 'Branche' in industry_summary.columns:
             allowed_industries = set(industry_summary.head(top_n_industries)['Branche'].tolist())
    industry_lookup = _build_summary_lookup(industry_summary, "Branche")

    # Cluster-Filter
    allowed_clusters = set()
    use_cluster_filter = config.get('candidate_use_cluster_filter', True)
    if hasattr(cluster_summary, 'head') and not cluster_summary.empty:
        top_c = int(config.get('cluster_top_n', 5))
        if 'Cluster' in cluster_summary.columns:
            allowed_clusters = set(cluster_summary.head(top_c)['Cluster'].astype(str).tolist())
    cluster_lookup = _build_summary_lookup(cluster_summary, "Cluster")

    # Pre-calculate hold_rank for Top-% filter (Default 1% for 12k stocks)
    threshold = float(config.get("candidate_top_percent_threshold", 0.01) or 0.01)
    require_top = bool(config.get("candidate_require_top_percent", True))
    hold_rank = max(1, int(math.ceil(len(stock_results) * threshold))) if stock_results else None

    for i, stock in enumerate(analysis_pool):
        # Kandidaten-Filter (jetzt erst anwenden)
        if stock.rsl <= 1.0: continue
        if stock.yahoo_symbol in portfolio_symbols: continue
        if min_trust > 0 and stock.trust_score < min_trust: continue

        # --- RISING STAR LOGIC ---
        # Fokus auf Top 1% (hold_rank). Wir lassen Aktien bis Top 3% zu, 
        # WENN sie eine starke Aufwärtsdynamik haben (Wildcard).
        current_rank = int(getattr(stock, "rsl_rang", 0) or 0)
        is_top_tier = (hold_rank is not None and current_rank <= hold_rank)
        zs = z_scores[i]
        
        is_rising_star = False
        # Wenn nicht in Top 1%, aber in Top 3% -> Check auf "Auf dem Weg nach oben"
        if require_top and hold_rank is not None and not is_top_tier and current_rank <= (hold_rank * 3):
            # Kriterium: Positive RSL-Änderung (>1%) UND positive Beschleunigung (3M > 6M/12M)
            # UPDATE: Zusätzlicher Schutz durch R²-Smoothness (>0.6) und Volatilitäts-Cap (Z_Vol < 1.5)
            r2_val = _coerce_float(stock.trend_smoothness)
            if _coerce_float(stock.rsl_change_1w) > 0.01 and _coerce_float(stock.mom_accel) > 0:
                if r2_val > 0.6 and zs['z_vol'] < 1.5:
                    is_rising_star = True
        
        if require_top and hold_rank is not None and not is_top_tier and not is_rising_star:
            continue
        
        # FIX: Forgiving Liquidity Filter
        # Harter Ausschluss nur, wenn BEIDE Daten (Volumen & Market Cap) niedrig sind.
        # Verhindert, dass Large Caps mit fehlerhaften Yahoo-Volumendaten fliegen.
        curr_liq = _coerce_float(getattr(stock, "primary_liquidity_eur", getattr(stock, "avg_volume_eur", 0.0)))
        curr_mkt = _coerce_float(getattr(stock, "market_value", 0.0))
        if min_vol > 0 and curr_liq < min_vol and curr_mkt < 500_000_000: continue
        
        if min_mktcap > 0 and getattr(stock, "market_value", 0) < min_mktcap: continue
        if excluded_countries and str(getattr(stock, "land", "Unknown")).strip().upper() in excluded_countries:
            # FEATURE: Erlaube Aktien aus "ausgeschlossenen" Laendern, wenn sie an 
            # deutschen Börsen (Xetra/Frankfurt) handelbar sind (.DE / .F).
            y_sym = str(stock.yahoo_symbol).upper()
            if not (y_sym.endswith(".DE") or y_sym.endswith(".F")):
                continue
        
        # Soft-Filter Evaluation
        penalties_context = {}
        if allowed_industries and stock.industry not in allowed_industries:
            penalties_context['industry_out'] = 0.2 # FIX: Soften penalty to avoid Sector Momentum Bias
        if use_cluster_filter and allowed_clusters and stock.mom_cluster not in allowed_clusters:
            penalties_context['cluster_out'] = 0.3
            
        final_score, details = _calculate_institutional_score(stock, zs, config, penalties_context, regime_status)
        industry_row = industry_lookup.get(str(stock.industry or "").strip(), {})
        cluster_key = str(getattr(stock, "mom_cluster", "") or "").strip()
        cluster_row = cluster_lookup.get(cluster_key, {})
        details.update(
            {
                "symbol": getattr(stock, "yahoo_symbol", ""),
                "base_label": _resolve_base_label(stock, config),
                "base_score": _resolve_base_score(stock, config),
                "accel_component": _coerce_float(getattr(stock, "mom_accel", 0.0)) * accel_weight if bool(config.get("candidate_use_accel", True)) else 0.0,
                "rsl_change_component": _coerce_float(getattr(stock, "rsl_change_1w", 0.0)) * rsl_1w_weight if bool(config.get("candidate_use_rsl_change_1w", True)) else 0.0,
                "peer_spread_component": _coerce_float(getattr(stock, "peer_spread", 0.0)) * peer_weight if bool(config.get("candidate_use_peer_spread", True)) else 0.0,
                "industry_neutral_component": _coerce_float(getattr(stock, "mom_score_adj", None), _coerce_float(getattr(stock, "mom_score", None))) - _coerce_float(industry_row.get("Avg_MomScore_Adj"), _coerce_float(industry_row.get("Avg_MomScore"), 0.0)),
                "industry_rank": industry_row.get("Rank"),
                "industry_score": industry_row.get("Score"),
                "cluster": cluster_key or None,
                "cluster_rank": cluster_row.get("Rank"),
                "cluster_score": cluster_row.get("Score"),
                "trust_score": getattr(stock, "trust_score", None),
                "rsl_rank": getattr(stock, "rsl_rang", None),
                "hold_rank": hold_rank,
                "is_rising_star": is_rising_star,
                "distance_52w_high_pct": getattr(stock, "distance_52w_high_pct", None),
                "max_distance_52w_high_pct": max_distance_52w_high_pct,
                "peer_spread": getattr(stock, "peer_spread", None),
            }
        )
         
        if float(final_score) > -10.0: # Filter groben Muell
            scored_candidates.append((final_score, stock, details))

    # --- UPGRADE: Cross-Sectional Ranking ---
    # Ersetzt den absoluten Score durch einen relativen Rang (0.0 - 1.0).
    # Macht das Modell robuster gegen Regimewechsel und Skalierungseffekte.
    if scored_candidates:
        raw_scores = np.array([x[0] for x in scored_candidates])
        n_cands = len(raw_scores)
        if n_cands > 0:
            # FIX 1: Robustes Percentile Ranking via scipy.stats.rankdata
            ranks = scipy.stats.rankdata(raw_scores, method="average")
            percentiles = ranks / n_cands
            
            # Optional: Min-Max Normalisierung der Raw-Scores für Soft-Ranking
            s_min, s_max = raw_scores.min(), raw_scores.max()
            denom = (s_max - s_min) if s_max > s_min else 1.0
            
            new_scored = []
            for i, (_, stock, det) in enumerate(scored_candidates):
                p_rank = percentiles[i]
                raw_norm = float((raw_scores[i] - s_min) / denom)
                
                # FIX 6: Soft Ranking (Mix aus Perzentil und Magnitude)
                # Erhöhung der Perzentil-Gewichtung (0.85) für maximale Stabilität bei 12k Aktien
                final_combined = float(0.85 * float(p_rank) + 0.15 * raw_norm)
                
                det['percentile_rank'] = p_rank
                det['raw_model_score'] = raw_scores[i]
                det['final_score'] = final_combined
                new_scored.append((final_combined, stock, det))
            scored_candidates = new_scored

    # 5. Sortierung & Diversifikation
    # FIX: Expliziter Sort Key verhindert den Vergleich von StockData-Objekten (Ambiguity Error)
    scored_candidates.sort(key=lambda x: (float(x[0]), str(x[1].yahoo_symbol)), reverse=True)
    
    # Sektor-Constraints
    sector_counts: Dict[str, int] = defaultdict(int)
    industry_counts: Dict[str, int] = defaultdict(int)
    
    # Bestand zaehlen (ohne Sell-List)
    for sym in (portfolio_symbols - sell_list_symbols):
        s_obj = None
        if isinstance(symbol_lookup, dict):
            s_obj = symbol_lookup.get(sym)
        if s_obj:
            sector_counts[s_obj.sector] += 1
            industry_counts[s_obj.industry] += 1
            
    final_selection: List[Any] = []
    raw_max_industry = int(config.get('candidate_max_stocks_per_industry', 1))
    max_industry = raw_max_industry if raw_max_industry > 0 else None
    
    free_slots = portfolio_size - len(portfolio_symbols - sell_list_symbols)
    needed = free_slots if free_slots > 0 else portfolio_size # Wenn voll, zeige Top N als Info
    
    for score, stock, detail in scored_candidates:
        if len(final_selection) >= needed:
            break
            
        sec = stock.sector
        ind = stock.industry
        
        sector_ok = sector_counts[sec] < max_stocks_per_sector
        industry_ok = max_industry is None or industry_counts[ind] < max_industry

        if sector_ok and industry_ok:
            final_selection.append(stock)
            sector_counts[sec] += 1
            industry_counts[ind] += 1
            detail["selection_reason"] = (
                f"Sektor-Slot frei ({sector_counts[sec]-1}/{max_stocks_per_sector} -> {sector_counts[sec]}/{max_stocks_per_sector})"
                if max_industry is None
                else (
                    f"Sektor+Branchen-Slot frei ({sector_counts[sec]-1}/{max_stocks_per_sector}, {industry_counts[ind]-1}/{max_industry} -> {sector_counts[sec]}/{max_stocks_per_sector}, {industry_counts[ind]}/{max_industry})"
                )
            )
             
    if return_details:
        return final_selection, scored_candidates
    return final_selection


def orthogonalize_multi(target: List[float], bases: List[List[float]], shrinkage: float = 1.0) -> List[float]:
    """
    Multivariate Orthogonalisierung mittels Regression (Least Squares).
    Entfernt (partiell via shrinkage) den linearen Einfluss der Basis-Vektoren.
    """
    if bases is None or len(bases) == 0 or len(target) < 2:
        return target
    
    # Matrix X erstellen: [Intercept, Base1, Base2, ...]
    X_bases = np.column_stack([np.asarray(b).ravel() for b in bases])
    y = np.asarray(target).ravel()
    ones = np.ones(len(y))
    X = np.column_stack([ones, X_bases])
    
    try:
        # Beta berechnen: (X'X)^-1 X'y
        # rcond=None nutzt Maschinen-Praezision
        beta, residuals, rank, s = np.linalg.lstsq(X, y, rcond=None)
        
        # Vorhergesagte Werte
        y_hat = X @ beta
        
        # FIX 4: Klarere Orthogonalisierungs-Deklaration (Factor Blending)
        # y_resid = y - y_hat
        # return (1-shrink) * y + shrink * y_resid  == y - shrink * y_hat
        residual = y - y_hat
        return ((1.0 - shrinkage) * y + shrinkage * residual).tolist()
    except Exception:
        # Fallback bei numerischen Problemen
        return y.tolist()


def _compute_robust_stats(values: List[float]) -> Tuple[float, float]:
    """Berechnet Median und MAD (Robust Z-Score Basis)."""
    # FIX: 'if not values' knallt bei NumPy Arrays. Wir nutzen .size fuer eine eindeutige Pruefung.
    if values is None or np.asarray(values).size == 0:
        return 0.0, 1.0
    
    arr = np.asarray(values, dtype=float).ravel()
    
    # FIX: Winsorization (Clipping bei 2.5% / 97.5%) zur Stabilisierung gegen Faktor-Explosion
    if arr.size > 20:
        p_low, p_high = np.percentile(arr, [2.5, 97.5])
        arr = np.clip(arr, p_low, p_high)

    # FIX 4: Robust Statistics (Median / MAD) statt Mean/Std
    median = float(np.median(arr))
    mad = float(np.median(np.abs(arr - median)))
    
    # Sigma-Schaetzung aus MAD (fuer Normalverteilungskonsistenz)
    sigma = float(1.4826 * mad)
    
    # FIX 5: Stabilisierter Sigma-Floor bei near-zero Median
    # scale verhindert Explosion bei Faktoren wie Momentum-Beschleunigung (oft nah 0)
    scale = float(np.median(np.abs(arr)) + 1e-6) if arr.size > 0 else 1e-6
    sigma_final = float(max(0.05 * scale, sigma, 0.05))
    
    return median, sigma_final


def _zscore(val: float, mean: float, std: float) -> float:
    if std <= 0: return 0.0
    z = (val - mean) / std
    # Clamp to avoid outliers dominating (-3 to +3)
    return float(max(-3.0, min(3.0, float(z))))


def _calculate_institutional_score(stock: Any, zs: Dict[str, float], config: Dict[str, Any], external_penalties: Dict[str, float], regime: str) -> Tuple[float, Dict[str, Any]]:
    """
    Berechnet den Score basierend auf der 'Barra-Style' Logik:
    Final = Alpha - Risk * Quality
    Alles basiert auf Z-Scores.
    """
    
    # --- 1. ALPHA COMPONENTS (Weighted) ---
    # FIX 6: Explizites Factor Scaling (IC-basiert)
    # Momentum bleibt Anker, Accel & Peer liefern die Praezision
    W_MOM   = 1.0
    W_ACCEL = 0.6
    W_PEER  = 0.4
    
    alpha_score = float((zs['z_mom'] * W_MOM) + (zs['z_accel'] * W_ACCEL) + (zs['z_peer'] * W_PEER))
    momentum_core = zs['z_mom'] # fuer Details-Export

    # FIX 4: Winner-Continuation Bias (Mean Reversion Control)
    # Wenn Aktie extrem nah am 52W-Hoch klebt (0-5%), leicht dämpfen
    dist = stock.distance_52w_high_pct or 0.0
    stretch = float(max(0.0, (5.0 - float(dist)) / 5.0)) # 1.0 bei 0% Abstand, 0.0 bei >5%
    if stretch > 0:
        alpha_score *= (1.0 - 0.2 * stretch)
        
    # FIX: Crowding Threshold auf 1.5 gesenkt (aggressivere Dämpfung überhitzter Trends)
    exposure_raw = float(zs['z_mom'] + 0.5 * zs['z_accel'] + 0.3 * zs['z_peer'])
    exposure = max(0.0, exposure_raw)
    crowding_penalty = float(math.tanh(max(0.0, (exposure - 1.5) / 1.0)))
    alpha_score *= (1.0 - 0.25 * crowding_penalty)

    # FIX: Liquidity-Alpha-Weighting (Vermeidung von Microcap-Bias)
    liq_val = float(getattr(stock, "primary_liquidity_eur", getattr(stock, "avg_volume_eur", 0.0)))
    mkt_val = float(getattr(stock, "market_value", 0.0))
    
    # Sanity Check: Traue dem niedrigen Volumen nicht, wenn die Marktkapitalisierung hoch ist (> 500M)
    # Wenn mkt_val hoch, aber liq_val extrem niedrig -> Wahrscheinlich Datenfehler -> Neutral (1.0)
    is_data_gap = (mkt_val > 500_000_000 and liq_val < 100_000)

    if liq_val > 0 and not is_data_gap:
        # Skalierung: 1M EUR = 0.0, 100M EUR = 2.0, 1B EUR = 3.0
        log_liq = math.log10(max(1.0, liq_val / 1_000_000.0))
        # Sanfte Dämpfung für Werte unter 10M EUR, leichter Boost für High-Liquidity
        alpha_score *= (0.9 + 0.1 * np.clip(log_liq / 2.0, 0.0, 1.5))
    else:
        # Bei verdächtigen oder fehlenden Daten: Neutral behandeln (kein Abzug)
        alpha_score *= 1.0

    # --- 2. RISK COMPONENTS (Weighted) ---
    # Gewichte
    W_RISK_DD  = 0.7
    W_RISK_VOL = 0.5
    
    # FIX 2: Symmetric Risk Model Fix
    # Risk darf NICHT positiv wirken (kein Alpha-Boost durch Low Risk)
    # Vol wird weniger stark gewichtet als Drawdown (Tail Risk)
    z_dd = zs['z_dd']
    z_vol = zs['z_vol']
    
    # FIX 2: Tail-Risk Verstärkung für Drawdown
    # Extreme Drawdowns (z > 1.5) werden überproportional bestraft
    # UPDATE: Quadratische Verstärkung für glattere Übergänge in Crash-Szenarien
    dd_base = max(0.0, float(z_dd))
    tail_factor = 1.0 + 0.5 * (max(0.0, float(z_dd) - 1.5)**2)
    dd_component = W_RISK_DD * dd_base * tail_factor
    
    vol_component = W_RISK_VOL * max(0.0, z_vol * 0.5) # Vol weight reduced

    # FIX 2: Kalibrierter Stabilitäts-Bonus (v3: 0.03 Gewichtung)
    # Belohnt niedrige Drawdowns, ohne das Modell zu defensiv zu machen.
    alpha_score += 0.03 * math.tanh(max(0.0, -float(z_dd)))
    
    # --- 3. RECOVERY FIX (Improved) ---
    # Nur echte Recoveries erlauben, keine Dead Cat Bounces
    is_recovery = (
        (stock.mom_3m or 0) > 0 and 
        (stock.mom_6m or 0) > 0 and 
        (stock.mom_accel or 0) > 0 and
        (stock.trend_smoothness or 0) > 0.3
    )
    
    if is_recovery:
        dd_component *= 0.7 # 30% weniger Drawdown-Strafe

    risk_score = dd_component + vol_component

    # --- 4. QUALITY OVERLAY (Multiplikativ) ---
    # Log-Space Penalties fuer Datenqualitaet und Soft-Filter
    
    penalties = []
    
    # Externe Penalties (Soft Filters)
    for k, v in external_penalties.items():
        penalties.append(v)
    
    # Trust Penalty
    if stock.trust_score < 3:
        penalties.append(0.15 * (3 - stock.trust_score)) # 15% pro Trust Punkt
        
    # Gap Penalty (indirekt via Trust, aber hier explizit wenn Flag gesetzt)
    if stock.flag_gap == "WARN":
        penalties.append(0.10)
        
    # Distance 52W High Penalty (wenn zu weit weg)
    if dist > 20.0:
        penalties.append(0.05 + (dist - 20.0)/100.0)

    # Log-Space Summation
    log_penalty_sum = sum(math.log1p(-min(0.99, p)) for p in penalties)
    quality_multiplier = math.exp(log_penalty_sum)
    
    # Harder Quality Overlay: Basis auf 0.5 gesenkt (Low Quality bestraft nun bis zu 50%)
    quality_adjustment = 0.5 + 0.5 * quality_multiplier

    # --- FINAL CALCULATION ---
    # Raw Score = Alpha - Risk
    # FIX: Dynamisches Lambda basierend auf dem Marktregime
    if regime == "SCHWACH":
        LAMBDA_RISK = 1.2 # Risiko-avers
    elif regime == "STARK":
        LAMBDA_RISK = 0.6 # Aggressiv
    else:
        LAMBDA_RISK = 0.8 # Normal

    raw_score = alpha_score - (LAMBDA_RISK * risk_score)
    score_with_quality = raw_score * quality_adjustment
    
    # Echte Factor Density (Summe der absoluten Z-Scores)
    active_factor_score = sum(abs(zs[k]) for k in ['z_mom', 'z_accel', 'z_peer', 'z_dd', 'z_vol'])
    
    # FIX 1: Stabilisierter Factor Density Boost (Normalisiert via sqrt(n))
    # Verhindert Bevorzugung von "Factor Spam" bei verrauschten Signalen.
    density_boost = 1.0 + 0.10 * (float(active_factor_score) / math.sqrt(5.0))
    final_score = float(score_with_quality * density_boost)
    
    return float(final_score), {
        'base_score': momentum_core,
        'final_score': final_score,
        'accel_component': zs['z_accel'],
        'peer_spread_component': zs['z_peer'],
        'risk_score': risk_score,
        'penalty_multiplier': quality_multiplier,
        'active_factor_score': active_factor_score,
        'penalties': external_penalties,
        'is_sector_neutral': float(zs['is_sector_neutral'])
    }


def _blend_stats(local_stats: Tuple[float, float], global_stats: Tuple[float, float], weight: float, global_sigma_for_floor: float) -> Tuple[float, float]:
    """
    Mischt lokale und globale Statistiken. 
    Sigma wird via Varianz (Quadratwurzel der gewichteten Quadrate) gemischt.
    """
    l_med, l_sig = local_stats
    g_med, g_sig = global_stats
    blended_sigma = math.sqrt(weight * (l_sig**2) + (1.0 - weight) * (g_sig**2))
    # FIX 4: Robuster Sigma-Floor zur Vermeidung von Z-Score Explosionen
    # 0.05 als absolutes Minimum fuer die Stabilitaet
    blended_sigma = max(blended_sigma, 0.1 * global_sigma_for_floor, 0.05)
    return (
        weight * l_med + (1.0 - weight) * g_med,
        blended_sigma
    )
