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
            return _coerce_float(stock.mom_score_adj)
        if getattr(stock, "mom_score", None) is not None:
            return _coerce_float(stock.mom_score)
    return _coerce_float(getattr(stock, "rsl", 0.0), 0.0)


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
    ulcer_vec = np.asarray([_coerce_float(getattr(s, "ulcer_index_6m", 0.0)) for s in analysis_pool], dtype=float).ravel()
    peer_vec = np.asarray([_coerce_float(s.peer_spread) for s in analysis_pool], dtype=float).ravel()
    vol_vec = np.asarray([_coerce_float(s.mom_vol, 0.2) for s in analysis_pool], dtype=float).ravel()
    rsl_1w_vec = np.asarray([_coerce_float(s.rsl_change_1w) for s in analysis_pool], dtype=float).ravel()

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
        'ulcer': _compute_robust_stats(ulcer_vec.tolist()),
        'vol': _compute_robust_stats(vol_vec.tolist()),
        'rsl_1w': _compute_robust_stats(rsl_1w_vec.tolist())
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
        s_ulcer = [ulcer_vec[i] for i in indices]
        s_vol = [vol_vec[i] for i in indices]
        s_r1w = [rsl_1w_vec[i] for i in indices]
        
        local_stats = {
            'mom': _compute_robust_stats(s_mom), 'accel': _compute_robust_stats(s_accel),
            'peer': _compute_robust_stats(s_peer), 'dd': _compute_robust_stats(s_dd),
            'ulcer': _compute_robust_stats(s_ulcer),
            'vol': _compute_robust_stats(s_vol),
            'rsl_1w': _compute_robust_stats(s_r1w)
        }
        
        # Blending der Statistiken
        stats_source = {
            'mom': _blend_stats(local_stats['mom'], global_stats['mom'], w, global_stats['mom'][1]),
            'accel': _blend_stats(local_stats['accel'], global_stats['accel'], w, global_stats['accel'][1]),
            'peer': _blend_stats(local_stats['peer'], global_stats['peer'], w, global_stats['peer'][1]),
            'dd': _blend_stats(local_stats['dd'], global_stats['dd'], w, global_stats['dd'][1]),
            'ulcer': _blend_stats(local_stats['ulcer'], global_stats['ulcer'], w, global_stats['ulcer'][1]),
            'vol': _blend_stats(local_stats['vol'], global_stats['vol'], w, global_stats['vol'][1]),
            'rsl_1w': _blend_stats(local_stats['rsl_1w'], global_stats['rsl_1w'], w, global_stats['rsl_1w'][1])
        }
        
        for i_local, idx_global in enumerate(indices):
            z_scores[idx_global] = {
                'z_mom': float(_zscore(float(mom_vec[idx_global]), *stats_source['mom'])),
                'z_accel': float(_zscore(float(accel_res[idx_global]), *stats_source['accel'])),
                'z_peer': float(_zscore(float(peer_res[idx_global]), *stats_source['peer'])),
                'z_dd': float(_zscore(float(dd_res[idx_global]), *stats_source['dd'])),
                'z_ulcer': float(_zscore(float(ulcer_vec[idx_global]), *stats_source['ulcer'])),
                'z_vol': float(_zscore(float(vol_vec[idx_global]), *stats_source['vol'])),
                'z_rsl_1w': float(_zscore(float(rsl_1w_vec[idx_global]), *stats_source['rsl_1w'])),
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
        
        # Metriken für Filterung vorbereiten
        current_rank = int(getattr(stock, "rsl_rang", 0) or 0)
        is_top_tier = (hold_rank is not None and current_rank <= hold_rank)

        # FIX: Dynamischer Trust-Filter
        # Erlaubt Top-Performer (Top 1%) auch bei lückenhafter Datenbasis (Trust < 3)
        if min_trust > 0 and stock.trust_score < min_trust and not is_top_tier:
            continue

        # --- RISING STAR LOGIC ---
        # Fokus auf Top 1% (hold_rank). Wir lassen Aktien bis Top 3% zu, 
        # WENN sie eine starke Aufwärtsdynamik haben (Wildcard).
        zs = z_scores[i]
        
        is_rising_star = False
        # Wenn nicht in Top 1%, aber in Top 3% -> Check auf "Auf dem Weg nach oben"
        if require_top and hold_rank is not None and not is_top_tier and current_rank <= (hold_rank * 3):
            # Kriterium: Positive RSL-Änderung (>1%) UND positive Beschleunigung (3M > 6M/12M)
            # UPDATE: Zusätzlicher Schutz durch R²-Smoothness (>0.6) und Volatilitäts-Cap (Z_Vol < 1.5)
            r2_val = _coerce_float(stock.trend_smoothness)
            if _coerce_float(stock.rsl_change_1w) > 0.005 and _coerce_float(stock.mom_accel) > 0:
                if r2_val > 0.6 and zs['z_vol'] < 1.5:
                    is_rising_star = True
        
        if require_top and hold_rank is not None and not is_top_tier and not is_rising_star:
            continue
        
        # FIX: Forgiving Liquidity Filter
        # Harter Ausschluss nur, wenn BEIDE Daten (Volumen & Market Cap) niedrig sind.
        # Verhindert, dass Large Caps mit fehlerhaften Yahoo-Volumendaten fliegen.
        # Falls Market Cap 0 ist (oft bei Yahoo .DE/.F), wird sie als "unbekannt" 
        # gewertet und führt nicht zum automatischen Ausschluss.
        curr_liq = _coerce_float(getattr(stock, "primary_liquidity_eur", getattr(stock, "avg_volume_eur", 0.0)))
        curr_mkt = _coerce_float(getattr(stock, "market_value", 0.0))
        
        # Nur ausschließen, wenn Liquidität ODER Market Cap valide klein sind.
        # Wenn eins von beiden 0 ist, gehen wir von einem Datenfehler aus und lassen den Wert zu.
        if min_vol > 0 and curr_liq > 0 and curr_liq < min_vol:
            if curr_mkt > 0 and curr_mkt < 250_000_000:
                continue # Echter Small-Cap mit zu wenig Umsatz
        
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
            new_scored = []
            for i, (_, stock, det) in enumerate(scored_candidates):
                # Wir nutzen den rohen Modell-Score fuer die Aggregation in Summaries.
                # Perzentile sind gut fuer die Liste, aber schlecht fuer Durchschnitte.
                f_score = float(raw_scores[i])
                det['raw_model_score'] = f_score
                det['final_score'] = f_score
                new_scored.append((f_score, stock, det))
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


def orthogonalize_multi(
    target: List[float],
    bases: List[List[float]],
    shrinkage: float = 1.0,
    min_obs: int = 30
) -> List[float]:
    """
    Residualisiert target linear gegen die Basisfaktoren mittels OLS.
    shrinkage = 0.0 -> original
    shrinkage = 1.0 -> vollständiges Residuum
    """
    y_raw = np.asarray(target, dtype=float).ravel()

    if not bases:
        return y_raw.tolist()

    X_raw = np.column_stack([np.asarray(b, dtype=float).ravel() for b in bases])

    if len(y_raw) != len(X_raw):
        return y_raw.tolist()

    # NaN-Handling: Nur Zeilen ohne NaNs für die Regression nutzen
    mask = np.isfinite(y_raw) & np.all(np.isfinite(X_raw), axis=1)
    if mask.sum() < min_obs:
        return y_raw.tolist()

    y = y_raw[mask]
    X = X_raw[mask]

    # Robuste Standardisierung vor der Regression
    y_med, y_mad = np.median(y), np.median(np.abs(y - np.median(y)))
    y_scale = max(1.4826 * y_mad, 1e-8)
    y_std = (y - y_med) / y_scale

    X_std = np.zeros_like(X, dtype=float)
    for j in range(X.shape[1]):
        xj = X[:, j]
        x_med, x_mad = np.median(xj), np.median(np.abs(xj - np.median(xj)))
        X_std[:, j] = (xj - x_med) / max(1.4826 * x_mad, 1e-8)

    X_design = np.column_stack([np.ones(len(y_std)), X_std])

    try:
        beta, _, _, _ = np.linalg.lstsq(X_design, y_std, rcond=None)
        residual = y_std - (X_design @ beta)
        out_valid = (1.0 - shrinkage) * y_std + shrinkage * residual
    except Exception:
        return y_raw.tolist()

    out = np.full_like(y_raw, np.nan, dtype=float)
    out[mask] = out_valid
    out[~mask] = y_raw[~mask] # Fallback auf Original bei NaNs
    return out.tolist()


def _compute_robust_stats(values: List[float], min_sigma: float = 1e-6) -> Tuple[float, float]:
    """Robuste Lage- und Streuungsschätzung via Median und MAD."""
    arr = np.asarray(values, dtype=float).ravel()
    arr = arr[np.isfinite(arr)]

    if arr.size == 0:
        return 0.0, 1.0

    if arr.size > 20:
        arr = np.clip(arr, np.percentile(arr, 2.5), np.percentile(arr, 97.5))

    median = float(np.median(arr))
    mad = float(np.median(np.abs(arr - median)))
    sigma = float(max(1.4826 * mad, min_sigma))

    return median, sigma


def _zscore(val: float, mean: float, std: float) -> float:
    if std <= 0: return 0.0
    z = (val - mean) / std
    # Clamp to avoid outliers dominating (-3 to +3)
    return float(max(-3.0, min(3.0, float(z))))


def _calculate_institutional_score(
    stock: Any, 
    zs: Dict[str, float], 
    config: Dict[str, Any], 
    external_penalties: Dict[str, float], 
    regime: str
) -> Tuple[float, Dict[str, Any]]:
    """
    Berechnet den Score basierend auf der 'Barra-Style' Logik:
    Final = Alpha - Risk * Quality
    Alles basiert auf Z-Scores.
    """
    # Extraktion der Gewichte aus der Config
    accel_w = float(config.get("candidate_accel_weight", 0.15))
    rsl_1w_w = float(config.get("candidate_rsl_change_weight", 0.10))
    peer_w = float(config.get("candidate_peer_spread_weight", 0.40)) if bool(config.get("candidate_use_peer_spread", False)) else 0.0

    # 1) ALPHA (Regime-unabhängige Trendstärke)
    alpha = (
        1.00 * zs.get('z_mom', 0.0) +
        accel_w * zs.get('z_accel', 0.0) +
        peer_w * zs.get('z_peer', 0.0) +
        rsl_1w_w * zs.get('z_rsl_1w', 0.0)
    )

    # 2) RISK (Downside-fokussiert)
    z_ulcer = zs.get('z_ulcer', 0.0)
    z_dd = zs.get('z_dd', 0.0)
    z_vol = zs.get('z_vol', 0.0)

    downside_risk = (0.90 * z_ulcer + 0.35 * z_dd + 0.30 * z_vol)

    # Tail-Risk nur bei extremem Ulcer Index (Z > 2) quadratisch verstärken
    tail_factor = 1.0 + 0.40 * (max(0.0, z_ulcer - 2.0) ** 2)
    risk = downside_risk * tail_factor

    # 3) REGIME (Lambda Anpassung)
    lambda_map = {"SCHWACH": 1.50, "NORMAL": 1.0, "STARK": 0.50}
    lambda_risk = lambda_map.get(regime, 1.0)

    # 4) QUALITY (Penalty System)
    quality_penalty = sum(external_penalties.values())
    if getattr(stock, 'flag_gap', 'OK') != 'OK': quality_penalty += 0.08
    if getattr(stock, 'flag_stale', 'OK') != 'OK': quality_penalty += 0.08
    if getattr(stock, 'flag_scale', 'OK') != 'OK': quality_penalty += 0.20
    if getattr(stock, 'flag_history_length', 'OK') != 'OK': quality_penalty += 0.10

    # Trust Bonus nur bei perfekter Datenbasis
    trust_bonus = 0.03 * max(0, min(getattr(stock, 'trust_score', 3), 3) - 2)
    quality = np.clip(1.0 - quality_penalty + trust_bonus, 0.65, 1.03)

    # 5) DENSITY (Belohnt nur Übereinstimmung positiver Signale)
    # FIX: Robustes Handling von Z-Scores für die Dichte-Berechnung
    z_keys = ['z_mom', 'z_accel', 'z_peer', 'z_rsl_1w']
    alpha_signals = [max(0.0, _coerce_float(zs.get(k, 0.0))) for k in z_keys]
    
    density_strength = sum(alpha_signals) / max(1.0, len(alpha_signals))
    density_boost = np.clip(1.0 + 0.05 * density_strength, 1.0, 1.10)

    # Echte Factor Density für das Profil-Monitoring (Summe der absoluten Alpha-Z-Scores)
    active_factor_sum = sum(abs(_coerce_float(zs.get(k, 0.0))) for k in ['z_mom', 'z_accel', 'z_peer', 'z_dd', 'z_vol', 'z_ulcer'])

    # 6) FINALE BERECHNUNG
    raw_score = alpha - (lambda_risk * risk)
    final_score = raw_score * quality * density_boost

    return float(final_score), {
        "alpha": float(alpha),
        "risk": float(risk),
        "quality": float(quality),
        "density_boost": float(density_boost),
        "raw_score": float(raw_score),
        "active_factor_score": float(active_factor_sum),
        "penalty_multiplier": float(quality),
        "base_score": _coerce_float(zs.get('z_mom', 0.0))
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
