import datetime
import json
import os
import sys
import webbrowser
from collections import Counter, defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, cast

import pandas as pd
import numpy as np
from . import settings_catalog as settings_catalog_core

MAIN_EXPORT_COLUMN_ORDER = [
    "RSL",
    "Tr",
    "Ticker",
    "ISIN",
    "Name",
    "St",
    "Lk",
    "Sektor",
    "Branche",
    "Land",
    "Kurs",
    "ATR Buy",
    "ATR Sell",
    "Listing Umsatz 20T (Mio EUR)",
    "Primary Liquidity 20T (Mio EUR)",
    "Peer Spread",
    "Abst. 52W-Hoch %",
    "Mom 12M",
    "Trust",
    "Trend-Qual.",
    "Neu?",
    "Erfasst seit",
    "RSL-Rang",
    "ETFs/Boerse",
    "Orig. Ticker",
    "MktCap-Rang",
    "RSL 1W Diff",
    "Mom Cluster",
    "Mom 6M",
    "Mom 3M",
    "Mom Score",
    "Mom Vol 3M",
    "Mom Score adj",
    "Mom Accel",
    "SMA50",
    "Trust-Details",
    "Scale-Status",
    "Trend-Exzess",
    "Exzess-Datum",
    "Tage seit Exzess",
    "Exzess-Max %",
]


RAW_EXPORT_COLUMN_ORDER = [
    "yahoo_symbol",
    "original_ticker",
    "isin",
    "name",
    "status_marker",
    "in_depot",
    "is_candidate",
    "is_watchlist",
    "sector",
    "industry",
    "land",
    "source_etf",
    "listing_source",
    "rsl_rang",
    "mktcap_rang",
    "market_value",
    "market_cap",
    "kurs",
    "sma",
    "rsl",
    "atr",
    "atr_limit",
    "atr_sell_limit",
    "avg_volume_eur",
    "primary_liquidity_eur",
    "primary_liquidity_symbol",
    "primary_liquidity_basis",
    "rsl_change_1w",
    "rsl_past",
    "mom_12m",
    "mom_6m",
    "mom_3m",
    "mom_score",
    "mom_vol",
    "mom_score_adj",
    "mom_accel",
    "mom_cluster",
    "industry_median_rsl",
    "peer_spread",
    "high_52w",
    "distance_52w_high_pct",
    "max_drawdown_6m",
    "trend_sma50",
    "trend_smoothness",
    "trend_quality",
    "twss_score",
    "twss_date",
    "twss_days_ago",
    "twss_raw_pct",
    "twss_orientation",
    "flag_gap",
    "flag_liquidity",
    "flag_stale",
    "flag_scale",
    "flag_history_length",
    "history_length_reason",
    "scale_reason",
    "price_scale_ratio",
    "stale_days",
    "trust_score",
    "first_seen_date",
    "is_new",
    "multiscope_overall_status",
    "multiscope_primary_reason",
    "multiscope_reason_sell",
    "multiscope_reason_warn",
    "multiscope_trigger_scope_code",
    "multiscope_trigger_scope_text",
    "multiscope_pct_global",
    "multiscope_pct_sector",
    "multiscope_pct_industry",
]


def _normalize_export_cell(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(value)
    return value


def _build_main_export_dataframe(
    stock_results: List[Any],
    multiscope_status_map: Dict[str, Dict[str, Any]],
    candidate_symbols: Set[str],
    watchlist_set: Set[str],
    build_yahoo_quote_url: Callable[[str], str],
    threshold_rank: int = -1,
) -> pd.DataFrame:
    """Stellt das Haupt-DataFrame fuer den Excel-Export zusammen."""
    export_data = []
    for s in stock_results:
        d = s.to_dict()
        eval_info = multiscope_status_map.get(s.yahoo_symbol, {})

        for k in [
            'rsl', 'kurs', 'sma', 'trend_smoothness', 'twss_score', 'twss_raw_pct',
            'rsl_change_1w', 'mom_12m', 'mom_6m', 'mom_3m', 'mom_score', 'mom_vol',
            'mom_score_adj', 'mom_accel', 'atr_limit', 'atr_sell_limit', 'peer_spread', 'distance_52w_high_pct'
        ]:
            if d.get(k) is not None:
                try:
                    val = float(d[k])
                    if k in ('kurs', 'sma', 'atr_limit', 'atr_sell_limit') and val == 0.0:
                        d[k] = np.nan
                    else:
                        d[k] = val
                except (ValueError, TypeError):
                    d[k] = np.nan
            else:
                d[k] = np.nan

        trust_details = f"G:{s.flag_gap[0]} L:{s.flag_liquidity[0]} S:{s.flag_stale[0]}"

        rsl_diff = s.rsl_change_1w
        if rsl_diff > 0.01:
            rsl_dir = "UP"
        elif rsl_diff < -0.01:
            rsl_dir = "DN"
        else:
            rsl_dir = "FLAT"

        if s.trust_score < 2:
            twss_display = "Low Trust"  # This will be a string in a numeric column
        else:
            twss_display = d['twss_score']

        status = ""
        if s.in_depot == "JA":
            status = "D"
        elif s.yahoo_symbol in candidate_symbols:
            status = "K"
        elif str(s.yahoo_symbol).strip().upper() in watchlist_set:
            status = "W"
        
        # Markierung fuer die Top-25% Grenze (Threshold)
        if threshold_rank > 0 and s.rsl_rang == threshold_rank:
            if status:
                status += "/CUT"
            else:
                status = "CUT"

        link_type = "D" if str(s.yahoo_symbol or "").strip().isalnum() else "S"

        avg_vol_display = None
        if s.avg_volume_eur is not None and isinstance(s.avg_volume_eur, (int, float)):
            try:
                avg_vol_display = s.avg_volume_eur / 1_000_000
            except (ValueError, TypeError):
                pass
        primary_liquidity_display = None
        primary_liquidity_value = getattr(s, "primary_liquidity_eur", None)
        if primary_liquidity_value is not None and isinstance(primary_liquidity_value, (int, float)):
            try:
                primary_liquidity_display = primary_liquidity_value / 1_000_000
            except (ValueError, TypeError):
                pass

        atr_limit_value = d['atr_limit']
        if (pd.isna(atr_limit_value) or atr_limit_value == 0.0) and s.atr and s.kurs:
            try:
                atr_limit_value = float(s.kurs) - (1.0 * float(s.atr))
            except Exception:
                atr_limit_value = np.nan

        atr_sell_limit_value = d['atr_sell_limit']
        if (pd.isna(atr_sell_limit_value) or atr_sell_limit_value == 0.0) and s.atr and s.kurs:
            try:
                atr_sell_limit_value = float(s.kurs) + (settings_catalog_core.USER_SETTINGS_DEFAULTS['atr_multiplier_exit'] * float(s.atr))
            except Exception:
                atr_sell_limit_value = np.nan

        source_display_parts = []
        source_etf_txt = str(getattr(s, "source_etf", "") or "").strip()
        listing_source_txt = str(getattr(s, "listing_source", "") or "").strip()
        if source_etf_txt:
            source_display_parts.append(source_etf_txt)
        if listing_source_txt:
            source_display_parts.append(f"Boerse: {listing_source_txt}")
        source_display = " | ".join(source_display_parts)

        row = {
            'RSL-Rang': s.rsl_rang,
            'RSL': d['rsl'],
            'Tr': rsl_dir,
            'Ticker': s.yahoo_symbol,
            'ISIN': getattr(s, 'isin', ''),
            'Lk': link_type,
            'Name': s.name,
            'St': status,
            'MktCap-Rang': s.mktcap_rang,
            'Orig. Ticker': s.original_ticker,
            'Sektor': s.sector,
            'Branche': s.industry,
            'Land': s.land,
            'ETFs/Boerse': source_display,
            'RSL 1W Diff': d['rsl_change_1w'],
            'Mom Cluster': s.mom_cluster,
            'Mom 12M': d['mom_12m'], 'Mom 6M': d['mom_6m'], 'Mom 3M': d['mom_3m'],
            'Mom Score': d['mom_score'], 'Mom Vol 3M': d['mom_vol'],
            'Mom Score adj': d['mom_score_adj'], 'Mom Accel': d['mom_accel'],
            'Kurs': d['kurs'],
            'ATR Buy': atr_limit_value,
            'ATR Sell': atr_sell_limit_value,
            'Listing Umsatz 20T (Mio EUR)': avg_vol_display,
            'Primary Liquidity 20T (Mio EUR)': primary_liquidity_display,
            'Peer Spread': getattr(s, 'peer_spread', np.nan),
            'Abst. 52W-Hoch %': getattr(s, 'distance_52w_high_pct', np.nan),
            'SMA50': s.trend_sma50, 'Trend-Qual.': s.trend_quality,
            'Trend-Exzess': twss_display, 'Exzess-Datum': s.twss_date,
            'Tage seit Exzess': s.twss_days_ago, 'Exzess-Max %': d['twss_raw_pct'],
            'Trust': s.trust_score, 'Trust-Details': trust_details,
            'Scale-Status': s.flag_scale,
            'Neu?': "JA" if s.is_new else "NEIN",
            'Erfasst seit': s.first_seen_date
        }
        export_data.append(row)
    df = pd.DataFrame(export_data)
    ordered_cols = [col for col in MAIN_EXPORT_COLUMN_ORDER if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in ordered_cols]
    return df[ordered_cols + remaining_cols]


def _build_raw_export_dataframe(
    stock_results: List[Any],
    multiscope_status_map: Dict[str, Dict[str, Any]],
    candidate_symbols: Set[str],
    watchlist_set: Set[str],
    candidate_details_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """Vollstaendige technische Rohsicht fuer Audit und Plausibilitaetspruefung."""
    export_data: List[Dict[str, Any]] = []
    detail_column_names: Set[str] = set()
    candidate_details_map = candidate_details_map or {}

    for s in stock_results:
        symbol = str(getattr(s, "yahoo_symbol", "") or "").strip()
        symbol_upper = symbol.upper()
        row = {k: _normalize_export_cell(v) for k, v in s.to_dict().items()}

        status = ""
        if str(getattr(s, "in_depot", "") or "").strip().upper() == "JA":
            status = "D"
        elif symbol_upper in candidate_symbols:
            status = "K"
        elif symbol_upper in watchlist_set:
            status = "W"

        eval_info = multiscope_status_map.get(symbol, {}) or multiscope_status_map.get(symbol_upper, {})
        row.update(
            {
                "status_marker": status,
                "is_candidate": "JA" if symbol_upper in candidate_symbols else "NEIN",
                "is_watchlist": "JA" if symbol_upper in watchlist_set else "NEIN",
                "multiscope_overall_status": eval_info.get("overall_status", ""),
                "multiscope_primary_reason": eval_info.get("primary_reason", ""),
                "multiscope_reason_sell": eval_info.get("reason_sell", ""),
                "multiscope_reason_warn": eval_info.get("reason_warn", ""),
                "multiscope_trigger_scope_code": eval_info.get("trigger_scope_code", ""),
                "multiscope_trigger_scope_text": eval_info.get("trigger_scope_text", ""),
                "multiscope_pct_global": eval_info.get("pct_global", np.nan),
                "multiscope_pct_sector": eval_info.get("pct_sector", np.nan),
                "multiscope_pct_industry": eval_info.get("pct_industry", np.nan),
            }
        )

        candidate_detail = candidate_details_map.get(symbol_upper, {})
        if isinstance(candidate_detail, dict) and candidate_detail:
            for key, value in candidate_detail.items():
                prefixed_key = f"candidate_{key}"
                row[prefixed_key] = _normalize_export_cell(value)
                detail_column_names.add(prefixed_key)

        export_data.append(row)

    if not export_data:
        return pd.DataFrame()

    if detail_column_names:
        for row in export_data:
            for key in detail_column_names:
                row.setdefault(key, np.nan)

    df = pd.DataFrame(export_data)
    ordered_cols = [col for col in RAW_EXPORT_COLUMN_ORDER if col in df.columns]
    candidate_cols = sorted([col for col in df.columns if col.startswith("candidate_")])
    remaining_cols = [
        col for col in df.columns if col not in ordered_cols and col not in candidate_cols
    ]
    return df[ordered_cols + candidate_cols + remaining_cols]

def _print_candidate_profile(candidates: List[Any], details_map: Dict[str, Any]) -> None:
    if not candidates:
        return
    
    count = len(candidates)
    sum_mom = 0.0
    sum_vol = 0.0
    sum_dd = 0.0
    sum_density = 0.0
    sum_qual = 0.0
    
    for c in candidates:
        sym = str(c.yahoo_symbol).strip().upper()
        det = details_map.get(sym, {})
        
        sum_mom += float(det.get("base_score", 0.0) or 0.0)
        sum_vol += float(getattr(c, "mom_vol", 0.0) or 0.0)
        sum_dd += float(getattr(c, "max_drawdown_6m", 0.0) or 0.0)
        sum_density += float(det.get("active_factor_score", 0.0) or 0.0)
        sum_qual += float(det.get("penalty_multiplier", 1.0) or 1.0)
        
    print("-" * 100)
    print(f" STRATEGIE-PROFIL (Ø Top {count}):")
    print(f"   Momentum:    {sum_mom/count:.3f} (Trendstaerke)")
    print(f"   Risiko:      Vol {sum_vol/count*100:.1f}% | DD {sum_dd/count*100:.1f}%")
    print(f"   Qualitaet:   Score-Erhalt {sum_qual/count*100:.0f}% | Dichte {sum_density/count:.1f}")
    print("-" * 100)

def render_analysis_output(
    stock_results: List[Any],
    portfolio_mgr: Any,
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    update_last_run_cfg: bool,
    data_mgr: Optional[Any],
    config: Dict[str, Any],
    logger: Any,
    build_multiscope_status_map: Callable[..., Dict[str, Dict[str, Any]]],
    sort_portfolio_items_by_rank: Callable[..., List[Dict[str, Any]]],
    _format_percent_value: Callable[[Optional[float]], str],
    _format_percent_bar: Callable[..., str],
    _risk_bucket: Callable[[Optional[float]], str],
    _shorten_text: Callable[[str, int], str],
    build_yahoo_quote_url: Callable[[str], str],
    build_etf_rsl_summary: Callable[..., pd.DataFrame],
    build_sector_rsl_summary: Callable[..., pd.DataFrame],
    industry_summary_df: Optional[pd.DataFrame],
    cluster_summary_df: Optional[pd.DataFrame],
    save_excel_report_safely: Callable[..., bool],
    save_dataframe_safely: Callable[..., None],
    save_json_config: Callable[[str, Any], None],
    build_console_symbols: Callable[..., Dict[str, str]],
    suggest_portfolio_candidates: Optional[Callable] = None,
    market_regime: Optional[Dict[str, Any]] = None,
    integrity_drops_df: Optional[pd.DataFrame] = None,
    watchlist_symbols: Optional[set] = None,
) -> None:
    current_portfolio_dicts = portfolio_mgr.current_portfolio
    current_tickers = {
        str(p.get('Yahoo_Symbol', '')).strip().upper()
        for p in current_portfolio_dicts
        if str(p.get('Yahoo_Symbol', '')).strip()
    }
    portfolio_isins = {
        str(p.get('ISIN', p.get('isin', ''))).strip().upper()
        for p in current_portfolio_dicts
        if str(p.get('ISIN', p.get('isin', ''))).strip()
    }
    symbol_lookup = {str(s.yahoo_symbol).strip().upper(): s for s in stock_results if str(s.yahoo_symbol).strip()}

    watchlist_set = {str(x).strip().upper() for x in (watchlist_symbols or set()) if str(x).strip()}

    for s in stock_results:
        is_in = str(s.yahoo_symbol).strip().upper() in current_tickers
        if not is_in and hasattr(s, 'isin') and s.isin:
            is_in = str(s.isin).strip().upper() in portfolio_isins
        s.in_depot = "JA" if is_in else ""

    n_stocks = len(stock_results)
    hold_threshold_rank = max(1, int(n_stocks * config['top_percent_threshold']))
    warning_threshold_rank = max(1, int(hold_threshold_rank * 0.9))
    multiscope_status_map = build_multiscope_status_map(
        stock_results=stock_results,
        top_percent_threshold=config['top_percent_threshold'],
        etf_options=etf_options
    )
    heatmap_warn_pct = float(config.get('heatmap_warn_percent', 20.0) or 20.0)
    heatmap_full_pct = float(config.get('heatmap_full_percent', 25.0) or 25.0)
    if heatmap_warn_pct >= heatmap_full_pct:
        heatmap_warn_pct = max(0.0, heatmap_full_pct - 1.0)

    table_width = 170
    force_ascii_console = os.environ.get("RSL_ASCII_CONSOLE", "").strip().lower() in ("1", "true", "yes", "y")
    symbols = build_console_symbols(
        stdout_encoding=str(getattr(sys.stdout, "encoding", "") or ""),
        force_ascii=force_ascii_console
    )
    sym_market = symbols["sym_market"]
    sym_portfolio = symbols["sym_portfolio"]
    sym_hold = symbols["sym_hold"]
    sym_warn = symbols["sym_warn"]
    sym_sell = symbols["sym_sell"]
    sym_alert = symbols["sym_alert"]
    sym_delete = symbols["sym_delete"]
    sym_ok = symbols["sym_ok"]
    sym_fire = symbols["sym_fire"]
    trend_up = symbols["trend_up"]
    trend_down = symbols["trend_down"]
    trend_flat = symbols["trend_flat"]
    top_ind_set: set = set()
    sym_divider = symbols.get("sym_divider", "-")
    sym_new = symbols.get("sym_new", "*")
    workbook_filename = ""
    custom_profiles = settings_catalog_core.load_custom_profiles(str(config.get("strategy_profiles_file", "") or ""))
    defaults = settings_catalog_core.get_user_settings_defaults()
    active_strategy_settings: Dict[str, Any] = dict(defaults)
    active_strategy_settings.update({k: v for k, v in dict(config).items() if k in defaults})
    user_settings_path = str(config.get("user_settings_file", "") or "").strip()
    if user_settings_path and os.path.exists(user_settings_path):
        try:
            with open(user_settings_path, "r", encoding="utf-8") as f:
                raw_user_settings = json.load(f)
            if isinstance(raw_user_settings, dict):
                active_strategy_settings.update(raw_user_settings)
        except Exception:
            active_strategy_settings = dict(active_strategy_settings)
    active_strategy = settings_catalog_core.get_active_strategy_info(active_strategy_settings, custom_profiles)

    # Top-Branchen fuer Kandidaten-Vorauswahl ermitteln
    top_industries_for_candidates_df = pd.DataFrame()
    if isinstance(industry_summary_df, pd.DataFrame) and not industry_summary_df.empty and "Branche" in industry_summary_df.columns:
        try:
            top_n = int(config.get('industry_top_n', 10) or 10)
            score_min = float(config.get('industry_score_min', 0.0) or 0.0)
            breadth_min = float(config.get('industry_breadth_min', 0.25) or 0.25)
            min_size = int(config.get('industry_min_size', 5) or 5)
            
            filtered = industry_summary_df
            if "Aktien" in filtered.columns and min_size > 0:
                filtered = filtered[filtered["Aktien"] >= min_size]
            if "Breadth Ratio" in filtered.columns and breadth_min is not None:
                filtered = filtered[filtered["Breadth Ratio"] >= float(breadth_min)]
            if score_min > 0 and "Score" in filtered.columns:
                filtered = filtered[filtered["Score"] >= score_min]
            if "Branche" in filtered.columns:
                filtered = filtered[~filtered["Branche"].astype(str).str.strip().isin(["Unknown", "ETF"])]
            top_industries_for_candidates_df = filtered.head(top_n)
        except Exception as e:
            logger.warning(f"Fehler bei der Ermittlung der Top-Branchen: {e}")

    def _cache_age_hours(path_value: Any) -> Optional[float]:
        try:
            path = str(path_value or "").strip()
            if not path or not os.path.exists(path):
                return None
            return max(0.0, (datetime.datetime.now().timestamp() - os.path.getmtime(path)) / 3600.0)
        except Exception:
            return None

    def _get_sector_color(sector_name: Optional[str]) -> str:
        if not sector_name: return "\033[0m"
        sec_lower = str(sector_name).lower()
        if "technology" in sec_lower: return "\033[96m"      # Cyan
        if "health" in sec_lower: return "\033[95m"        # Magenta
        if "financial" in sec_lower: return "\033[94m"     # Light Blue
        if "communication" in sec_lower: return "\033[35m" # Dark Magenta
        if "discretionary" in sec_lower: return "\033[93m" # Bright Yellow
        if "staples" in sec_lower or "defensive" in sec_lower: return "\033[92m" # Bright Green
        if "energy" in sec_lower: return "\033[91m"        # Light Red
        if "industrial" in sec_lower: return "\033[37m"    # White
        if "material" in sec_lower: return "\033[33m"      # Dark Yellow
        if "utilit" in sec_lower: return "\033[36m"        # Dark Cyan
        if "real" in sec_lower: return "\033[34m"          # Dark Blue
        return "\033[0m"

    def _ask_yes_no(prompt: str, help_text: str) -> bool:
        while True:
            answer = input(prompt).strip().lower()
            if answer == "?":
                print(help_text)
                continue
            if answer in ("j", "y", "ja", "yes"):
                return True
            if answer in ("", "n", "no", "nein"):
                return False
            print("Bitte mit j/n antworten (oder ? fuer Hilfe).")

    def _risk_bucket_cfg(worst_pct: Optional[float]) -> str:
        if worst_pct is None:
            return "UNBEKANNT"
        try:
            value = float(worst_pct)
        except Exception:
            return "UNBEKANNT"
        if value > heatmap_full_pct:
            return "KRITISCH"
        if value >= heatmap_warn_pct:
            return "NAH"
        return "OK"

    print("\n\033[95m" + sym_divider * table_width + "\033[0m")
    print(f" {sym_market} MARKT-DASHBOARD ({datetime.date.today()})")
    print("\033[95m" + sym_divider * table_width + "\033[0m")
    print(f"   - Analysiert:       {n_stocks} Aktien")

    if market_regime:
        regime_str = market_regime.get('regime', 'UNBEKANNT')
        regime_colors = {"STARK": "\033[92m", "NORMAL": "\033[93m", "SCHWACH": "\033[91m"}
        r_col = regime_colors.get(regime_str, "")
        
        breadth_pct = market_regime.get('breadth_pct', 0)
        strong_count = market_regime.get('strong_count', 0)
        total_count = market_regime.get('total_count', 0)
        median_rsl = market_regime.get('median_rsl', None)
        
        regime_hint = ""
        if regime_str == "SCHWACH" and len(current_portfolio_dicts) > 3:
            regime_hint = " (Portfolio verkleinern auf 3)"
        elif regime_str == "STARK":
            regime_hint = " (Chancen nutzen!)"

        median_txt = "-"
        try:
            if median_rsl is not None:
                median_txt = f"{float(median_rsl):.2f}"
        except Exception:
            median_txt = "-"
        print(f"   - Markt-Zustand:    {r_col}{regime_str}\033[0m (Breadth: {breadth_pct:.1f}%, {strong_count}/{total_count}, Median RSL: {median_txt}){regime_hint}")

    print(f"   - GRUENE ZONE:      Rang 1 bis {hold_threshold_rank} (Top 25%)")
    print(f"   - WARN-ZONE:        Rang {warning_threshold_rank} bis {hold_threshold_rank}")

    all_etf_keys = list(etf_options.keys())
    is_all_selected = len(all_etf_keys) > 0 and len(selected_syms) == len(all_etf_keys) and set(selected_syms) == set(all_etf_keys)
    if is_all_selected:
        selected_label = "Alle"
    else:
        selected_label = ", ".join(selected_syms[:5]) if selected_syms else "-"
        if len(selected_syms) > 5:
            selected_label = f"{selected_label}, ... (+{len(selected_syms) - 5})"

    history_age = _cache_age_hours(config.get('history_cache_file'))
    etf_age = _cache_age_hours(config.get('etf_cache_file'))
    history_age_txt = f"{history_age:.1f}h" if history_age is not None else "-"
    etf_age_txt = f"{etf_age:.1f}h" if etf_age is not None else "-"
    print(f"   - ETFs:             {selected_label}")
    print(f"   - Strategie:        {active_strategy['label']}")
    print(f"   - Depotgroesse:     {len(current_portfolio_dicts)} Position(en)")
    print(f"   - Cache (H/ETF):    {history_age_txt} / {etf_age_txt}")
    print(
        f"   - Pct-Schwellen:    Warn {heatmap_warn_pct:.1f}% | "
        f"Voll {heatmap_full_pct:.1f}%"
    )
    print(
        f"   - Konsole:          enc={str(getattr(sys.stdout, 'encoding', '') or '-')} "
        f"| unicode={str(symbols.get('unicode_console', False)).lower()}"
    )
    if isinstance(cluster_summary_df, pd.DataFrame) and not cluster_summary_df.empty:
        top_clusters_display = []
        for _, row in cluster_summary_df.head(3).iterrows():
            cluster_id = str(row.get("Cluster", "")).strip()
            count = int(row.get("Anzahl", 0) or 0)
            score_val = row.get("Score")
            try:
                score_txt = f"{float(score_val):.2f}"
            except Exception:
                score_txt = "-"
            if cluster_id:
                top_clusters_display.append(f"{cluster_id} ({count}, {score_txt})")
        if top_clusters_display:
            print(f"   - Momentum-Cluster: {', '.join(top_clusters_display)}")
            print("     Legende: 2=stark, 1=neutral, 0=schwach | Reihenfolge: 12M/6M/3M/Accel")
    print("\033[95m" + sym_divider * table_width + "\033[0m\n")

    print("\033[1mTop-Branchen fuer Kandidaten-Vorauswahl:\033[0m")
    if not top_industries_for_candidates_df.empty:
        header = (
            f"\033[1m{'RANK':<5} | {'SEKTOR':<25} | {'BRANCHE':<35} | {'SCORE':<7} | {'BREADTH':<7} | {'LEADER':<7} | {'AKTIEN':<6}\033[0m"
        )
        print(header)
        print("-" * 110)
        
        for _, row in top_industries_for_candidates_df.iterrows():
            rank = row.get('Rank', '')
            branche = _shorten_text(str(row.get('Branche', '')), 35)
            # Rueckwaertskompatibilitaet:
            # Aeltere Exporte/Snapshots koennen den Spaltennamen noch mit der frueheren
            # Umlaut-/Mojibake-Variante enthalten. Wir lesen deshalb beide Varianten.
            sector_val = str(
                row.get(
                    'Sektor (repraesentativ)',
                    row.get('Sektor (reprÃ¤sentativ)', '')
                )
            )
            sector = _shorten_text(sector_val, 25)
            
            score_val = row.get('Score')
            score = f"{score_val:.2f}" if pd.notna(score_val) else '-'
            breadth_val = row.get('Breadth Ratio')
            breadth = f"{breadth_val:.2f}" if pd.notna(breadth_val) else '-'
            leader_val = row.get('Leader Ratio')
            leader = f"{leader_val:.2f}" if pd.notna(leader_val) else '-'
            aktien = row.get('Aktien', '')
            
            c_start = _get_sector_color(sector_val)
            
            print(f"{c_start}{rank:<5} | {sector:<25} | {branche:<35} | {score:<7} | {breadth:<7} | {leader:<7} | {str(aktien):<6}\033[0m")
    else:
        print("Keine Top-Branchen nach aktuellen Kriterien gefunden.")

    print("-" * table_width)

    portfolio_list = [s for s in stock_results if s.in_depot]

    if portfolio_list:
        print("\n" + "-" * table_width)
        print(" Portfolio-Pct-Heatmap (pro Depotwert, je hoeher desto schwaecher)")
        print(
            f" Balken ist bei {heatmap_full_pct:.1f}% voll, Marker '|' steht bei "
            f"{heatmap_warn_pct:.1f}%. Werte darueber sind kritisch."
        )
        print("-" * table_width)

        def _worst_pct(eval_data: Dict[str, Any]) -> float:
            vals: List[Optional[float]] = [
                cast(Optional[float], eval_data.get('pct_global')),
                cast(Optional[float], eval_data.get('pct_sector')),
                cast(Optional[float], eval_data.get('pct_industry'))
            ]
            clean_vals: List[float] = [float(v) for v in vals if v is not None]
            return float(max(clean_vals)) if clean_vals else -1.0

        heat_list = sorted(
            portfolio_list,
            key=lambda s: (s.rsl_rang, str(s.yahoo_symbol))
        )

        status_counts = Counter(
            str(multiscope_status_map.get(s.yahoo_symbol, {}).get('overall_status', 'HOLD') or 'HOLD').upper()
            for s in portfolio_list
        )
        print(
            f" Depot-Status: HOLD={status_counts.get('HOLD', 0)} | "
            f"WARN={status_counts.get('WARN', 0)} | SELL={status_counts.get('SELL', 0)}"
        )
        if heat_list:
            weakest_candidate = max(
                heat_list,
                key=lambda s: _worst_pct(multiscope_status_map.get(s.yahoo_symbol, {}))
            )
            weakest_eval = multiscope_status_map.get(weakest_candidate.yahoo_symbol, {})
            weakest_worst = _worst_pct(weakest_eval)
            weakest_worst_txt = _format_percent_value(weakest_worst) if weakest_worst >= 0 else "-"
            print(f" Kritischster Depotwert (schlechtester Pct): {weakest_candidate.yahoo_symbol} mit {weakest_worst_txt}")
        print("-" * table_width)

        sample_bar = _format_percent_bar(
            heatmap_full_pct,
            full_at=heatmap_full_pct,
            marker_at=heatmap_warn_pct
        )
        bar_col_width = max(30, len(sample_bar) + 1)
        heat_header = (
            f"{'Ticker':<10} {'STS':<5} {'Risiko':<9} "
            f"{'Sicht':<20} {'Bezug':<32} {'Balken'}"
        )
        print(heat_header)
        print("-" * table_width)

        for p in heat_list:
            sector_color = _get_sector_color(p.sector)
            eval_info = multiscope_status_map.get(p.yahoo_symbol, {})
            status_icon = str(eval_info.get('overall_status', 'HOLD') or 'HOLD').upper()
            trigger_text = str(eval_info.get('trigger_scope_text', '-') or '-')
            worst_pct = _worst_pct(eval_info)
            risk = _risk_bucket_cfg(worst_pct)
            sector_label = p.sector if p.sector else "Unbekannt"
            industry_label = p.industry if p.industry else "Unbekannt"

            def _fmt_scope_label(name, key, f_rank=None, f_total=None):
                r = eval_info.get(f'rank_{key}', f_rank)
                c = eval_info.get(f'count_{key}', f_total)
                if r is not None and c is not None:
                    return f"{name} ({r}/{c})"
                return name

            rows = [
                (
                    _fmt_scope_label("Gesamt", "global", p.rsl_rang, n_stocks),
                    "Gesamtliste",
                    _format_percent_bar(
                        eval_info.get('pct_global'),
                        full_at=heatmap_full_pct,
                        marker_at=heatmap_warn_pct
                    )
                ),
                (
                    _fmt_scope_label("Sektor", "sector"),
                    sector_label,
                    _format_percent_bar(
                        eval_info.get('pct_sector'),
                        full_at=heatmap_full_pct,
                        marker_at=heatmap_warn_pct
                    )
                ),
                (
                    _fmt_scope_label("Branche", "industry"),
                    industry_label,
                    _format_percent_bar(
                        eval_info.get('pct_industry'),
                        full_at=heatmap_full_pct,
                        marker_at=heatmap_warn_pct
                    )
                ),
                (
                    _fmt_scope_label("ETF", "etf"),
                    _shorten_text(str(eval_info.get("name_etf", "-")), 32),
                    _format_percent_bar(
                        eval_info.get('pct_etf'),
                        full_at=heatmap_full_pct,
                        marker_at=heatmap_warn_pct
                    )
                )
            ]

            print(f"{sector_color}>>> {p.name} ({p.yahoo_symbol})\033[0m")
            for idx, (scope_name, scope_ref, bar_value) in enumerate(rows):
                prefix_ticker = f"{p.yahoo_symbol:<10}" if idx == 0 else f"{'':<10}"
                prefix_status = f"{status_icon:<5}" if idx == 0 else f"{'':<5}"
                prefix_risk = f"{risk:<9}" if idx == 0 else f"{'':<9}"
                
                display_ref = f"{_shorten_text(scope_ref, 32):<32}"
                text_line = f"{prefix_ticker} {prefix_status} {prefix_risk} {scope_name:<20} {display_ref}"
                print(f"{sector_color}{text_line}\033[0m {bar_value}")

            if status_icon in ("WARN", "SELL"):
                trigger_line = f"{'':<28}Trigger: {_shorten_text(trigger_text, table_width - 38)}"
                print(f"{sector_color}{trigger_line}\033[0m")
            print("-" * table_width)
        

    print("\n" + "="*100)
    print(f" {sym_portfolio} INTERAKTIVES PORTFOLIO MANAGEMENT")
    print("="*100)

    symbols_to_remove: set = set()
    sell_signals: List[Dict[str, Any]] = []

    if current_portfolio_dicts:
        print("\n--- 1. BESTANDS-CHECK ---")
        portfolio_items_sorted = sort_portfolio_items_by_rank(current_portfolio_dicts, symbol_lookup)

        sell_signals, warn_signals, hold_signals = [], [], []
        for item in portfolio_items_sorted:
            ticker = str(item.get('Yahoo_Symbol', '')).strip().upper()
            stock = symbol_lookup.get(ticker)
            if not stock: continue
            eval_info = multiscope_status_map.get(ticker, {})
            status = eval_info.get('overall_status', "HOLD")
            signal_data = {'ticker': ticker, 'stock': stock, 'eval': eval_info}
            if status == "SELL": sell_signals.append(signal_data)
            elif status == "WARN": warn_signals.append(signal_data)
            else: hold_signals.append(signal_data)

        def _print_signal(sig_data, color_code):
            s = sig_data['stock']
            e = sig_data['eval']
            pct_out = f"Pct G/S/I={_format_percent_value(e.get('pct_global'))}/{_format_percent_value(e.get('pct_sector'))}/{_format_percent_value(e.get('pct_industry'))}"
            print(f"{color_code}{s.yahoo_symbol:<10} (Rang {s.rsl_rang}, RSL {s.rsl:.3f})\033[0m: {s.name}")
            print(f"   - Grund: {e.get('primary_reason', '-')}")
            print(f"   - Trigger: {e.get('trigger_scope_text', '-')}")
            print(f"   - {pct_out}")

        if sell_signals:
            print(f"\n{sym_sell} VERKAUFS-SIGNALE ({len(sell_signals)}):")
            for sig in sell_signals: _print_signal(sig, "\033[91m")

        if warn_signals:
            print(f"\n{sym_warn} WARN-SIGNALE ({len(warn_signals)}):")
            for sig in warn_signals: _print_signal(sig, "\033[93m")

        if hold_signals:
            print(f"\n{sym_hold} STABILE TRENDS ({len(hold_signals)}):")
            print(f"   {', '.join(s['ticker'] for s in hold_signals)}")

        potential_sells = [s['ticker'] for s in sell_signals] + [s['ticker'] for s in warn_signals]
        if potential_sells:
            print("\n" + "-" * 50)
            prompt = f"Geben Sie Ticker zum VERKAUFEN ein (kommagetrennt, 'alle' fuer {len(sell_signals)} Verkaufssignale, ENTER fuer keine): "
            user_input = input(prompt).strip().upper()

            if user_input == 'ALLE':
                symbols_to_remove.update([s['ticker'] for s in sell_signals])
            elif user_input:
                symbols_to_remove.update([s.strip() for s in user_input.split(',') if s.strip()])

            valid_sells = {s for s in symbols_to_remove if s in potential_sells}
            invalid_sells = symbols_to_remove - valid_sells
            if invalid_sells:
                print(f"   {sym_alert} Ungueltige Ticker ignoriert: {', '.join(sorted(list(invalid_sells)))}")
            symbols_to_remove = valid_sells
        else:
            print(f"\n{sym_ok} Alle Positionen haben ein 'HOLD'-Signal. Keine Aktion empfohlen.")

    if symbols_to_remove:
        new_pf = [p for p in portfolio_mgr.current_portfolio if p.get('Yahoo_Symbol') not in symbols_to_remove]
        removed_count = len(portfolio_mgr.current_portfolio) - len(new_pf)
        portfolio_mgr.current_portfolio = new_pf
        portfolio_mgr.save(new_pf)
        print(f"\nPortfolio aktualisiert: {removed_count} Position(en) entfernt.")
        current_tickers = {str(p.get('Yahoo_Symbol', '')).strip().upper() for p in new_pf if str(p.get('Yahoo_Symbol', '')).strip()}
        for s in stock_results:
            s.in_depot = "JA" if str(s.yahoo_symbol).strip().upper() in current_tickers else ""
    elif current_portfolio_dicts:
        print("\nPortfolio unveraendert.")

    # Initialisiere Listen fuer die Action-Summary, damit sie immer verfuegbar sind
    sold_list: List[str] = []
    keep_list: List[str] = []
    watch_list: List[str] = []
    kept_sell_list: List[str] = []

    def _format_ticker_line(items: List[str]) -> str:
        if not items:
            return "-"
        if len(items) <= 8:
            return ", ".join(items)
        return ", ".join(items[:8]) + f", ... (+{len(items) - 8})"

    # --- 2. ACTION-SUMMARY ---
    if current_portfolio_dicts:
        print("\n--- 2. ACTION-SUMMARY ---")
        sold_list = sorted(list(symbols_to_remove))

        remaining_with_status = []
        for item in portfolio_mgr.current_portfolio: # Das ist das *neue* Portfolio
            ticker = str(item.get('Yahoo_Symbol', '')).strip().upper()
            if ticker and ticker in symbol_lookup:
                status = str(multiscope_status_map.get(ticker, {}).get('overall_status', 'HOLD') or 'HOLD').upper()
                remaining_with_status.append({
                    'ticker': ticker,
                    'status': status,
                    'rank': symbol_lookup[ticker].rsl_rang
                })

        remaining_with_status.sort(key=lambda x: x['rank'])
        
        keep_list = [item['ticker'] for item in remaining_with_status if item['status'] == 'HOLD']
        watch_list = [item['ticker'] for item in remaining_with_status if item['status'] == 'WARN']
        kept_sell_list = [item['ticker'] for item in remaining_with_status if item['status'] == 'SELL']

        print(f" Verkauft  ({len(sold_list)}): {_format_ticker_line(sold_list)}")
        print(f" Behalten  ({len(keep_list)}): {_format_ticker_line(keep_list)}")
        print(f" Beobachten({len(watch_list)}): {_format_ticker_line(watch_list)} (Warn-Signal)")
        if kept_sell_list:
            print(f" Behalten* ({len(kept_sell_list)}): {_format_ticker_line(kept_sell_list)} (*trotz Verkaufs-Signal)")

    # --- Summaries for portfolio suggestions and export ---
    etf_summary_df = build_etf_rsl_summary(
        stock_results=stock_results,
        selected_syms=selected_syms,
        etf_options=etf_options,
        top_percent_threshold=config['top_percent_threshold'],
        config=config
    )
    sector_summary_df_orig = build_sector_rsl_summary(
        stock_results=stock_results,
        top_percent_threshold=config['top_percent_threshold']
    )

    suggested_candidates: List[Any] = []
    candidate_details_map: Dict[str, Dict[str, Any]] = {}
    # --- 3. KAUFKANDIDATEN NACH STRATEGIE ---
    if suggest_portfolio_candidates:
        print("\n--- 3. KAUFVORSCHLAEGE & TOP-KANDIDATEN (Early Momentum Strategie) ---")

        free_slots = 5 - (len(current_tickers) - len(symbols_to_remove))
        print(f"Portfolio: {len(current_tickers)} Werte | Geplante Verkaeufe: {len(symbols_to_remove)} | Freie Plaetze: {free_slots}")

        if config.get('candidate_use_momentum_score', True):
            mode_parts = ["Momentum 12/6/3"]
            if config.get('candidate_use_vol_adjust', True):
                mode_parts.append("Vol-Adj")
            if config.get('candidate_use_industry_neutral', True):
                mode_parts.append("Industry-N")
            if config.get('candidate_use_accel', True):
                mode_parts.append("Accel")
            if config.get('candidate_use_rsl_change_1w', False):
                mode_parts.append("RSL-1W")
            if config.get('candidate_use_peer_spread', False):
                mode_parts.append("PeerSpread")
            max_dist_52w = float(config.get('candidate_max_distance_52w_high_pct', 0.0) or 0.0)
            if max_dist_52w > 0:
                mode_parts.append(f"52W<={max_dist_52w:.1f}%")
            if config.get('candidate_block_new_buys_in_weak_regime', False):
                mode_parts.append("WeakRegimeStop")
            if config.get('cluster_enabled', True) and config.get('candidate_use_cluster_filter', True):
                top_n = int(config.get('cluster_top_n', 5) or 5)
                mode_parts.append(f"Cluster-Top {top_n}")
            min_vol = float(config.get('candidate_min_avg_volume_eur', 0.0) or 0.0)
            if min_vol > 0:
                mode_parts.append(f"PrimaryLiq {min_vol/1_000_000:.1f}M")
            print(f"Scoring: {' | '.join(mode_parts)}")
        else:
            print("Scoring: RSL (klassisch)")

        # Filter-Kette zur Transparenz
        try:
            top_industries = top_industries_for_candidates_df['Branche'].astype(str).tolist() if not top_industries_for_candidates_df.empty else []
            top_ind_set = set(top_industries)

            cluster_enabled = bool(config.get('cluster_enabled', True))
            cluster_filter = bool(config.get('candidate_use_cluster_filter', True))
            top_clusters: Optional[Set[str]] = None
            if cluster_enabled and cluster_filter and isinstance(cluster_summary_df, pd.DataFrame) and not cluster_summary_df.empty and "Cluster" in cluster_summary_df.columns:
                min_size = int(config.get('cluster_min_size', 0) or 0)
                top_n = int(config.get('cluster_top_n', 5) or 5)
                filtered = cluster_summary_df
                if min_size > 0 and "Anzahl" in cluster_summary_df.columns:
                    filtered = cluster_summary_df[cluster_summary_df["Anzahl"] >= min_size]
                top_clusters = set(filtered.head(top_n)["Cluster"].astype(str).tolist())

            total_count = len(stock_results)
            rsl_count = sum(1 for s in stock_results if s.rsl > 1.0)
            min_trust = int(config.get('candidate_min_trust_score', 0) or 0)
            weak_regime_block = bool(config.get('candidate_block_new_buys_in_weak_regime', False)) and isinstance(market_regime, dict) and str(market_regime.get('regime', '')).upper() == 'SCHWACH'
            trust_count = sum(
                1 for s in stock_results
                if s.rsl > 1.0 and (min_trust <= 0 or int(getattr(s, "trust_score", 0) or 0) >= min_trust)
            )
            max_dist_52w = float(config.get('candidate_max_distance_52w_high_pct', 0.0) or 0.0)
            dist_52w_count = sum(
                1 for s in stock_results
                if s.rsl > 1.0
                and (min_trust <= 0 or int(getattr(s, "trust_score", 0) or 0) >= min_trust)
                and (
                    max_dist_52w <= 0
                    or (
                        getattr(s, "distance_52w_high_pct", None) is not None
                        and not pd.isna(getattr(s, "distance_52w_high_pct", None))
                        and float(getattr(s, "distance_52w_high_pct", 0.0) or 0.0) <= max_dist_52w
                    )
                )
            )
            industry_count = sum(
                1 for s in stock_results
                if s.rsl > 1.0
                and (min_trust <= 0 or int(getattr(s, "trust_score", 0) or 0) >= min_trust)
                and (
                    max_dist_52w <= 0
                    or (
                        getattr(s, "distance_52w_high_pct", None) is not None
                        and not pd.isna(getattr(s, "distance_52w_high_pct", None))
                        and float(getattr(s, "distance_52w_high_pct", 0.0) or 0.0) <= max_dist_52w
                    )
                )
                and (not top_ind_set or s.industry in top_ind_set)
            )
            cluster_count = 0
            for s in stock_results:
                if s.rsl <= 1.0:
                    continue
                if min_trust > 0 and int(getattr(s, "trust_score", 0) or 0) < min_trust:
                    continue
                if max_dist_52w > 0:
                    dist_val = getattr(s, "distance_52w_high_pct", None)
                    if dist_val is None or pd.isna(dist_val) or float(dist_val or 0.0) > max_dist_52w:
                        continue
                if top_ind_set and s.industry not in top_ind_set:
                    continue
                if top_clusters is not None and s.mom_cluster not in top_clusters:
                    continue
                cluster_count += 1
            not_portfolio_count = 0
            for s in stock_results:
                if s.rsl <= 1.0:
                    continue
                if min_trust > 0 and int(getattr(s, "trust_score", 0) or 0) < min_trust:
                    continue
                if max_dist_52w > 0:
                    dist_val = getattr(s, "distance_52w_high_pct", None)
                    if dist_val is None or pd.isna(dist_val) or float(dist_val or 0.0) > max_dist_52w:
                        continue
                if top_ind_set and s.industry not in top_ind_set:
                    continue
                if top_clusters is not None and s.mom_cluster not in top_clusters:
                    continue
                if s.yahoo_symbol in current_tickers:
                    continue
                not_portfolio_count += 1

            print("Filter-Check:")
            print(f" - Universe: {total_count}")
            print(f" - rsl>1.0: {rsl_count}")
            if min_trust > 0:
                print(f" - Min Trust Score: {min_trust}+ ({trust_count})")
            if max_dist_52w > 0:
                print(f" - Max Abstand 52W-Hoch: {max_dist_52w:.1f}% ({dist_52w_count})")
            if config.get('candidate_require_top_percent', False):
                pct = float(config.get('candidate_top_percent_threshold', config.get('top_percent_threshold', 0.25)) or 0.25) * 100.0
                print(f" - Top-% Filter: aktiv ({pct:.1f}%)")
            if weak_regime_block:
                print(" - Marktregime-Kaufbremse: aktiv (SCHWACH -> keine neuen Kaeufe)")
            if top_industries:
                print(f" - Top-Branchen: {industry_count} (Top {len(top_industries)})")
            else:
                print(" - Top-Branchen: -")
            if top_clusters is not None:
                print(f" - Top-Cluster: {cluster_count} (Top {len(top_clusters)})")
            else:
                print(f" - Top-Cluster: {cluster_count} (aus)")
            print(f" - nicht im Depot: {not_portfolio_count}")

            if top_industries:
                short_list = ", ".join(top_industries[:8])
                more = f", ... (+{len(top_industries) - 8})" if len(top_industries) > 8 else ""
                print(f"Top-Branchen: {short_list}{more}")
            if top_clusters:
                top_clusters_list = list(top_clusters)
                short_clusters = ", ".join(top_clusters_list[:8])
                more_c = f", ... (+{len(top_clusters_list) - 8})" if len(top_clusters_list) > 8 else ""
                print(f"Top-Cluster: {short_clusters}{more_c}")
        except Exception:
            pass

        # Beruecksichtige aktuelle Holdings und Verkaufs-Signale
        candidate_result = suggest_portfolio_candidates(
            stock_results=stock_results,
            industry_summary=industry_summary_df,
            cluster_summary=cluster_summary_df,
            portfolio_symbols=current_tickers,
            sell_list_symbols=symbols_to_remove,
            symbol_lookup=symbol_lookup,
            config=config,
            market_regime=market_regime,
            return_details=True
        )
        if isinstance(candidate_result, tuple) and len(candidate_result) == 2:
            suggested_candidates = candidate_result[0] or []
            # FIX: Unterstuetzung fuer Tuple-Rueckgabe der neuen Candidate-Engine (Score, Stock, Details)
            for item in candidate_result[1] or []:
                if isinstance(item, tuple) and len(item) >= 3:
                    _, stock_obj, detail = item[:3]
                    symbol = str(stock_obj.yahoo_symbol).strip().upper()
                    if symbol:
                        candidate_details_map[symbol] = detail
                elif isinstance(item, dict):
                    # Fallback fuer Legacy-Format
                    symbol = str(item.get("symbol", "")).strip().upper()
                    if symbol:
                        candidate_details_map[symbol] = item
        else:
            suggested_candidates = candidate_result
        if not suggested_candidates:
            suggested_candidates = []

        if suggested_candidates:
            def _fmt_component(value: Any) -> str:
                try:
                    return f"{float(value):+.3f}"
                except Exception:
                    return "-"

            def _fmt_plain(value: Any) -> str:
                try:
                    return f"{float(value):.3f}"
                except Exception:
                    return "-"

            def _fmt_percent(value: Any) -> str:
                try:
                    return f"{float(value):.1f}%"
                except Exception:
                    return "-"

            if free_slots > 0:
                industry_limit_txt = int(config.get('candidate_max_stocks_per_industry', 0) or 0)
                if industry_limit_txt > 0:
                    print(f"\nKaufvorschlaege fuer {free_slots} freie(n) Platz/Plaetze (max. 2 pro Sektor, max. {industry_limit_txt} pro Branche):")
                else:
                    print(f"\nKaufvorschlaege fuer {free_slots} freie(n) Platz/Plaetze (max. 2 pro Sektor):")
            else:
                print("\nINFO: Portfolio ist voll. Dies sind die aktuell 5 besten potenziellen Nachkaufkandidaten:")
            for i, stock in enumerate(suggested_candidates, 1):
                mom_val = stock.mom_score_adj if config.get('candidate_use_vol_adjust', True) and stock.mom_score_adj is not None else stock.mom_score
                mom_txt = f"{mom_val:.2f}" if mom_val is not None else "-"
                cluster_txt = stock.mom_cluster if stock.mom_cluster else "-"
                stop_txt = f"{stock.atr_limit:.2f}" if stock.atr_limit else "-"
                s_color = _get_sector_color(stock.sector)
                line1 = f" {i}. >>> {stock.name} ({stock.yahoo_symbol})"
                line2 = f"    Kurs: {stock.kurs:.2f} | Stop: ~{stop_txt} | RSL: {stock.rsl:.3f} | Score: {mom_txt} | Clust: {cluster_txt}"
                line3 = f"    Branche: {_shorten_text(stock.industry, 40)} ({stock.sector})"
                detail = candidate_details_map.get(str(stock.yahoo_symbol).strip().upper(), {})
                if detail:
                    hold_rank = detail.get("hold_rank")
                    if hold_rank:
                        top_rank_txt = f"JA (Rang {detail.get('rsl_rank')}/{hold_rank})"
                    else:
                        top_rank_txt = f"aus (Rang {detail.get('rsl_rank')})"
                    score_line = (
                        f"    Logik:   {detail.get('base_label', 'Basis')}={_fmt_plain(detail.get('base_score'))}"
                        f" | Accel={_fmt_component(detail.get('accel_component'))}"
                        f" | RSL1W={_fmt_component(detail.get('rsl_change_component'))}"
                        f" | Peer={_fmt_component(detail.get('peer_spread_component'))}"
                        f" | IndustryAdj={_fmt_component(detail.get('industry_neutral_component'))}"
                        f" | Final={_fmt_plain(detail.get('final_score'))}"
                    )
                    dist_52w_txt = "-"
                    if detail.get("max_distance_52w_high_pct", 0):
                        try:
                            max_txt = f"<={float(detail.get('max_distance_52w_high_pct')):.1f}%"
                        except Exception:
                            max_txt = "-"
                        dist_52w_txt = f"{_fmt_percent(detail.get('distance_52w_high_pct'))} ({max_txt})"
                    else:
                        dist_52w_txt = _fmt_percent(detail.get('distance_52w_high_pct'))
                    filter_line = (
                        f"    Filter:  Branche #{detail.get('industry_rank', '-')}"
                        f" | Cluster #{detail.get('cluster_rank', '-') if detail.get('cluster') else '-'}"
                        f" | Trust {detail.get('trust_score', '-')}"
                        f" | Top-% {top_rank_txt}"
                        f" | 52W {dist_52w_txt}"
                    )
                    context_line = (
                        f"    Kontext: Branchen-Score={_fmt_plain(detail.get('industry_score'))}"
                        f" | Cluster-Score={_fmt_plain(detail.get('cluster_score'))}"
                        f" | Peer-Spread={_fmt_plain(detail.get('peer_spread'))}"
                        f" | Auswahl={detail.get('selection_reason', '-')}"
                    )
                    
                    # NEU: Penalty-Details anzeigen (Monitoring fuer Factor Overlap)
                    penalties_line = ""
                    mult = float(detail.get("penalty_multiplier", 1.0))
                    if mult < 0.99:
                        p_dict = detail.get("penalties", {})
                        
                        # 1. Sortieren nach Impact (groesste Penalty zuerst)
                        relevant_items = [(k, v) for k, v in p_dict.items() if v > 0.01]
                        relevant_items.sort(key=lambda x: x[1], reverse=True)
                        active_p = [f"{k}={v:.2f}" for k, v in relevant_items]
                        
                        # 2. Density Check (Factor Overlap Indikator)
                        # Nutzt jetzt den gewichteten Score aus der Engine
                        fac_score = detail.get("active_factor_score", 0.0)
                        density_info = ""
                        if fac_score >= 2.0:
                            # Kritisch bei sehr hoher Dichte oder wenn Multiplier stark drueckt (< 0.7)
                            is_critical = fac_score >= 3.0 or (fac_score >= 2.0 and mult < 0.7)
                            warn_mark = f" {sym_warn}" if is_critical else ""
                            density_info = f" | Density={fac_score:.1f}{warn_mark}"

                        if active_p:
                            penalties_line = f"    Risk-Adj: Mult={mult:.2f}{density_info} ({', '.join(active_p)})"

                    block = f"{line1}\n{line2}\n{line3}\n{score_line}\n{filter_line}\n{context_line}"
                    if penalties_line:
                        block += f"\n{penalties_line}"
                    print(f"{s_color}{block}\033[0m")
                else:
                    print(f"{s_color}{line1}\n{line2}\n{line3}\033[0m")
            
            _print_candidate_profile(suggested_candidates, candidate_details_map)

            # NEU: Expliziter Top-Pick Highlight
            if suggested_candidates:
                top_pick = suggested_candidates[0]
                print(f"\n \033[92m\033[1m>>> TOP PICK #1: {top_pick.yahoo_symbol} ({top_pick.name})\033[0m")
                print(f"     Sektor: {top_pick.sector} | Branche: {top_pick.industry}")
                print(f"     Grund:  Hoechster Final Score ({candidate_details_map.get(str(top_pick.yahoo_symbol).strip().upper(), {}).get('final_score', 0.0):.3f})")
                print("     (Dies ist die statistisch beste Einzelposition dieser Strategie)")

        else:
            weak_regime_block = bool(config.get('candidate_block_new_buys_in_weak_regime', False)) and isinstance(market_regime, dict) and str(market_regime.get('regime', '')).upper() == 'SCHWACH'
            if weak_regime_block:
                print("\nKeine neuen Kaeufe: Marktregime SCHWACH und Kaufbremse aktiv.")
            else:
                print("\nKeine geeigneten Kandidaten gefunden, die die Kriterien (Top-Branche, Diversifikation) erfuellen.")

    today = datetime.date.today()
    year, week, _ = today.isocalendar()

    # Hole Kandidaten-Symbole fuer den Excel-Export
    candidate_symbols = {c.yahoo_symbol for c in suggested_candidates} # type: ignore

    df_out = _build_main_export_dataframe(
        stock_results,
        multiscope_status_map,
        candidate_symbols,
        watchlist_set,
        build_yahoo_quote_url,
        threshold_rank=hold_threshold_rank
    )
    raw_df_out = _build_raw_export_dataframe(
        stock_results,
        multiscope_status_map,
        candidate_symbols,
        watchlist_set,
        candidate_details_map,
    )

    combo_name = "COMBO" if len(selected_syms) > 3 else "_".join(selected_syms)
    
    # Strategie-Suffix generieren
    preset_suffix = ""
    if active_strategy and active_strategy.get('label'):
        # Label fuer Dateinamen bereinigen (nur Alphanumerische Zeichen, Bindestrich, Punkt)
        raw_label = str(active_strategy['label'])
        safe_label = "".join(c if c.isalnum() or c in ('-', '.') else '_' for c in raw_label)
        # Doppelte Underscores entfernen und Trimmen
        while "__" in safe_label:
            safe_label = safe_label.replace("__", "_")
        safe_label = safe_label.strip('_')
        if safe_label:
            preset_suffix = f"_{safe_label}"

    report_dir = os.path.dirname(config['result_file_prefix'])
    report_base = os.path.basename(config['result_file_prefix'])

    held_symbols = sorted(
        {
            str(s.yahoo_symbol).strip().upper()
            for s in stock_results
            if str(getattr(s, "in_depot", "")).strip().upper() == "JA" and str(s.yahoo_symbol).strip()
        }
    )
    sell_signal_symbols = sorted({str(item.get('ticker', '')).strip().upper() for item in sell_signals if str(item.get('ticker', '')).strip()})
    etf_to_held: Dict[str, set] = {}
    etf_to_candidates: Dict[str, set] = {}
    etf_to_sell_signals: Dict[str, set] = {}
    sector_to_held: Dict[str, set] = {}
    sector_to_candidates: Dict[str, set] = {}
    sector_to_sell_signals: Dict[str, set] = {}
    industry_to_held: Dict[str, set] = {}
    industry_to_candidates: Dict[str, set] = {}
    industry_to_sell_signals: Dict[str, set] = {}
    cluster_to_held: Dict[str, set] = {}
    cluster_to_candidates: Dict[str, set] = {}
    cluster_to_sell_signals: Dict[str, set] = {}
    for s in stock_results:
        ysym = str(getattr(s, "yahoo_symbol", "")).strip().upper()
        if not ysym:
            continue
        memberships = [p.strip().upper() for p in str(getattr(s, "source_etf", "")).split(",") if p and p.strip()]
        sector_name = str(getattr(s, "sector", "")).strip() or "Unbekannt"
        industry_name = str(getattr(s, "industry", "")).strip() or "Unbekannt"
        cluster_name = str(getattr(s, "mom_cluster", "")).strip()
        is_held = str(getattr(s, "in_depot", "")).strip().upper() == "JA"
        is_candidate = ysym in candidate_symbols
        is_sell_signal = ysym in sell_signal_symbols

        if is_held:
            for etf_sym in memberships:
                etf_to_held.setdefault(etf_sym, set()).add(ysym)
            sector_to_held.setdefault(sector_name, set()).add(ysym)
            industry_to_held.setdefault(industry_name, set()).add(ysym)
            if cluster_name:
                cluster_to_held.setdefault(cluster_name, set()).add(ysym)
        if is_candidate:
            for etf_sym in memberships:
                etf_to_candidates.setdefault(etf_sym, set()).add(ysym)
            sector_to_candidates.setdefault(sector_name, set()).add(ysym)
            industry_to_candidates.setdefault(industry_name, set()).add(ysym)
            if cluster_name:
                cluster_to_candidates.setdefault(cluster_name, set()).add(ysym)
        if is_sell_signal:
            for etf_sym in memberships:
                etf_to_sell_signals.setdefault(etf_sym, set()).add(ysym)
            sector_to_sell_signals.setdefault(sector_name, set()).add(ysym)
            industry_to_sell_signals.setdefault(industry_name, set()).add(ysym)
            if cluster_name:
                cluster_to_sell_signals.setdefault(cluster_name, set()).add(ysym)

    def _join_held(values: set) -> str:
        if not values:
            return ""
        return ", ".join(sorted(values))

    held_col_name = "Depot-Ticker (Yahoo)"
    candidate_col_name = "Kaufkandidaten (Yahoo)"
    sell_col_name = "Verkaufssignale (Yahoo)"
    if isinstance(etf_summary_df, pd.DataFrame) and not etf_summary_df.empty and "ETF" in etf_summary_df.columns:
        etf_summary_df[held_col_name] = etf_summary_df["ETF"].astype(str).apply(
            lambda v: _join_held(set(held_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(etf_to_held.get(v.strip().upper(), set()))
        )
        etf_summary_df[candidate_col_name] = etf_summary_df["ETF"].astype(str).apply(
            lambda v: _join_held(set(candidate_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(etf_to_candidates.get(v.strip().upper(), set()))
        )
        etf_summary_df[sell_col_name] = etf_summary_df["ETF"].astype(str).apply(
            lambda v: _join_held(set(sell_signal_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(etf_to_sell_signals.get(v.strip().upper(), set()))
        )
    
    if isinstance(sector_summary_df_orig, pd.DataFrame) and not sector_summary_df_orig.empty and "Sektor" in sector_summary_df_orig.columns:
        sector_summary_df_orig[held_col_name] = sector_summary_df_orig["Sektor"].astype(str).apply(
            lambda v: _join_held(set(held_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(sector_to_held.get(v.strip(), set()))
        )
        sector_summary_df_orig[candidate_col_name] = sector_summary_df_orig["Sektor"].astype(str).apply(
            lambda v: _join_held(set(candidate_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(sector_to_candidates.get(v.strip(), set()))
        )
        sector_summary_df_orig[sell_col_name] = sector_summary_df_orig["Sektor"].astype(str).apply(
            lambda v: _join_held(set(sell_signal_symbols))
            if v.strip().upper() == "GESAMT"
            else _join_held(sector_to_sell_signals.get(v.strip(), set()))
        )
    if isinstance(industry_summary_df, pd.DataFrame) and not industry_summary_df.empty and "Branche" in industry_summary_df.columns:
        industry_summary_df[held_col_name] = industry_summary_df["Branche"].astype(str).apply(
                lambda v: _join_held(industry_to_held.get(v.strip(), set()))
            )
        industry_summary_df[candidate_col_name] = industry_summary_df["Branche"].astype(str).apply(
                lambda v: _join_held(industry_to_candidates.get(v.strip(), set()))
            )
        industry_summary_df[sell_col_name] = industry_summary_df["Branche"].astype(str).apply(
                lambda v: _join_held(industry_to_sell_signals.get(v.strip(), set()))
            )
    indikator_df = pd.DataFrame()
    try:
        docs_path = os.path.join(os.path.dirname(__file__), "..", "docs", "indikatoren.md")
        docs_path = os.path.abspath(docs_path)
        if os.path.exists(docs_path):
            with open(docs_path, "r", encoding="utf-8") as f:
                lines = [line.rstrip("\n") for line in f]
            indikator_df = pd.DataFrame({"Beschreibung": lines})
    except Exception:
        indikator_df = pd.DataFrame()

    # Konfigurations-Snapshot (neues Sheet)
    config_rows: List[Dict[str, Any]] = []
    now_ts = datetime.datetime.now().isoformat(timespec='seconds')
    config_rows.append({"Bereich": "Run", "Einstellung": "Zeitstempel", "Wert": now_ts})
    config_rows.append({"Bereich": "Run", "Einstellung": "ETFs", "Wert": ", ".join(selected_syms) if selected_syms else "-"})
    config_rows.append({"Bereich": "Run", "Einstellung": "Universe", "Wert": n_stocks})
    config_rows.append({"Bereich": "Run", "Einstellung": "Watchlist Count", "Wert": len(watchlist_set)})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Aktives Profil", "Wert": active_strategy["label"]})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Typ", "Wert": "MANUELL" if active_strategy["is_manual"] else active_strategy["source"].upper()})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Zweck", "Wert": active_strategy["summary"]})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Begruendung", "Wert": active_strategy["why"]})
    if active_strategy.get("market_context"):
        config_rows.append({"Bereich": "Strategie", "Einstellung": "Marktumfeld", "Wert": active_strategy["market_context"]})
    if active_strategy.get("best_for"):
        config_rows.append({"Bereich": "Strategie", "Einstellung": "Einsatz", "Wert": active_strategy["best_for"]})
    if active_strategy.get("review_trigger"):
        config_rows.append({"Bereich": "Strategie", "Einstellung": "Review-Trigger", "Wert": active_strategy["review_trigger"]})
    config_rows.append({"Bereich": "Cache", "Einstellung": "History-Cache (h)", "Wert": history_age_txt})
    config_rows.append({"Bereich": "Cache", "Einstellung": "ETF-Cache (h)", "Wert": etf_age_txt})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Top%-Schwelle (Gesamt)", "Wert": f"{config.get('top_percent_threshold', 0.25)*100:.0f}%"})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "Heatmap Warn/Voll", "Wert": f"{heatmap_warn_pct:.1f}% / {heatmap_full_pct:.1f}%"})
    config_rows.append({"Bereich": "Strategie", "Einstellung": "ATR Sell Multiplikator", "Wert": float(config.get('atr_multiplier_exit', 0.15) or 0.15)})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Top-Branchen", "Wert": int(config.get('industry_top_n', 10) or 10)})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Score-Min", "Wert": float(config.get('industry_score_min', 0.0) or 0.0)})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Breadth-Min", "Wert": float(config.get('industry_breadth_min', 0.25) or 0.25)})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Min-Size", "Wert": int(config.get('industry_min_size', 5) or 5)})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Trend aktiv", "Wert": "JA" if config.get('industry_trend_enabled', True) else "NEIN"})
    config_rows.append({"Bereich": "Branchen", "Einstellung": "Trend Wochen", "Wert": int(config.get('industry_trend_weeks', 4) or 4)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Top%-Filter aktiv", "Wert": "JA" if config.get('candidate_require_top_percent', False) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Top%-Schwelle", "Wert": f"{float(config.get('candidate_top_percent_threshold', config.get('top_percent_threshold', 0.25)) or 0.25)*100:.1f}%"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Trust-Min", "Wert": int(config.get('candidate_min_trust_score', 0) or 0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Min Primary Liquidity (EUR)", "Wert": float(config.get('candidate_min_avg_volume_eur', 0.0) or 0.0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Keine Kaeufe bei SCHWACH", "Wert": "JA" if config.get('candidate_block_new_buys_in_weak_regime', False) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Max Aktien pro Branche", "Wert": int(config.get('candidate_max_stocks_per_industry', 0) or 0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Momentum-Score", "Wert": "JA" if config.get('candidate_use_momentum_score', True) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Vol-Adjust", "Wert": "JA" if config.get('candidate_use_vol_adjust', True) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Industry-Neutral", "Wert": "JA" if config.get('candidate_use_industry_neutral', True) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Acceleration", "Wert": "JA" if config.get('candidate_use_accel', True) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Accel-Weight", "Wert": float(config.get('candidate_accel_weight', 0.2) or 0.0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "RSL-Change 1W", "Wert": "JA" if config.get('candidate_use_rsl_change_1w', False) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "RSL-Change Gewicht", "Wert": float(config.get('candidate_rsl_change_weight', 0.0) or 0.0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Peer-Spread", "Wert": "JA" if config.get('candidate_use_peer_spread', False) else "NEIN"})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Peer-Spread Gewicht", "Wert": float(config.get('candidate_peer_spread_weight', 0.0) or 0.0)})
    config_rows.append({"Bereich": "Kandidaten", "Einstellung": "Max Abstand 52W-Hoch", "Wert": float(config.get('candidate_max_distance_52w_high_pct', 0.0) or 0.0)})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Cluster aktiv", "Wert": "JA" if config.get('cluster_enabled', True) else "NEIN"})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Cluster-Filter", "Wert": "JA" if config.get('candidate_use_cluster_filter', True) else "NEIN"})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Top-N", "Wert": int(config.get('cluster_top_n', 5) or 5)})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Min-Size", "Wert": int(config.get('cluster_min_size', 0) or 0)})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Score-Wt Mom12", "Wert": float(config.get('cluster_score_w_mom12', 0.5) or 0.0)})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Score-Wt Mom6", "Wert": float(config.get('cluster_score_w_mom6', 0.3) or 0.0)})
    config_rows.append({"Bereich": "Cluster", "Einstellung": "Score-Wt Accel", "Wert": float(config.get('cluster_score_w_accel', 0.2) or 0.0)})
    config_rows.append({"Bereich": "Daten", "Einstellung": "History-Periode", "Wert": str(config.get('history_period', '-'))})
    config_rows.append({"Bereich": "Daten", "Einstellung": "Info-Cache Unknown (Tage)", "Wert": int(config.get('info_cache_unknown_expiry_days', 7) or 7)})
    config_rows.append({"Bereich": "Daten", "Einstellung": "Info-Fetch Delay (s)", "Wert": float(config.get('info_fetch_delay_s', 0.7) or 0.7)})
    config_rows.append({"Bereich": "Daten", "Einstellung": "Info-Fetch Logmodus", "Wert": "RUHIG" if config.get('info_fetch_quiet', True) else "NORMAL"})
    rate_min = float(config.get('rate_limit_delay_min_s', 20.0) or 20.0)
    rate_max = float(config.get('rate_limit_delay_max_s', 60.0) or 60.0)
    if rate_max < rate_min:
        rate_max = rate_min
    config_rows.append({"Bereich": "Daten", "Einstellung": "Rate-Limit Min/Max (s)", "Wert": f"{rate_min:.1f}/{rate_max:.1f}"})
    config_rows.append({"Bereich": "Daten", "Einstellung": "Rate-Limit Log-Intervall", "Wert": int(config.get('rate_limit_log_every', 10) or 10)})
    config_snapshot_df = pd.DataFrame(config_rows)

    # NEU: Numerische Werte erst vor dem Export formatieren
    industry_summary_for_export = pd.DataFrame()
    if isinstance(industry_summary_df, pd.DataFrame) and not industry_summary_df.empty:
        industry_summary_for_export = industry_summary_df.copy()
        # Markiere Top-Branchen (Kandidaten-Config)
        top_industries_set: set = set()
        try:
            score_min = float(config.get('industry_score_min', 0.0) or 0.0)
            breadth_min = float(config.get('industry_breadth_min', 0.25) or 0.25)
            min_size = int(config.get('industry_min_size', 5) or 5)
            top_n = int(config.get('industry_top_n', 10) or 10)
            filtered = industry_summary_df
            if "Aktien" in filtered.columns and min_size > 0:
                filtered = filtered[filtered["Aktien"] >= min_size]
            if "Breadth Ratio" in filtered.columns and breadth_min is not None:
                filtered = filtered[filtered["Breadth Ratio"] >= float(breadth_min)]
            if score_min > 0 and "Score" in filtered.columns:
                filtered = filtered[filtered["Score"] >= score_min]
            if "Branche" in filtered.columns:
                filtered = filtered[~filtered["Branche"].astype(str).str.strip().isin(["Unknown", "ETF"])]
            if not filtered.empty and "Branche" in filtered.columns:
                top_industries_set = set(filtered.head(top_n)["Branche"].astype(str).tolist())
        except Exception:
            top_industries_set = set()
        if "Branche" in industry_summary_for_export.columns:
            industry_summary_for_export["Top-Kandidat-Branche"] = industry_summary_for_export["Branche"].astype(str).apply(
                lambda v: "JA" if v in top_industries_set else ""
            )

    cluster_summary_for_export = pd.DataFrame()
    if isinstance(cluster_summary_df, pd.DataFrame) and not cluster_summary_df.empty:
        cluster_summary_for_export = cluster_summary_df.copy()
        if "Cluster" in cluster_summary_for_export.columns:
            cluster_summary_for_export[held_col_name] = cluster_summary_for_export["Cluster"].astype(str).apply(
                    lambda v: _join_held(cluster_to_held.get(v.strip(), set()))
                )
            cluster_summary_for_export[candidate_col_name] = cluster_summary_for_export["Cluster"].astype(str).apply(
                    lambda v: _join_held(cluster_to_candidates.get(v.strip(), set()))
                )
            cluster_summary_for_export[sell_col_name] = cluster_summary_for_export["Cluster"].astype(str).apply(
                    lambda v: _join_held(cluster_to_sell_signals.get(v.strip(), set()))
                )

    workbook_filename = os.path.join(
        report_dir,
        f"{report_base}_{year}_kw{week:02d}_{combo_name}{preset_suffix}.xlsx"
    )
    exported_paths: List[str] = []
    excel_sheets = {
        'main': df_out,
        'raw_data': raw_df_out,
        'etf_summary': etf_summary_df,
        'sector_summary': sector_summary_df_orig,
        'industry_summary': industry_summary_for_export,
        'cluster_summary': cluster_summary_for_export,
        'indikatoren': indikator_df,
        'config_snapshot': config_snapshot_df
    }
    if integrity_drops_df is not None and not integrity_drops_df.empty:
        excel_sheets['integrity_issues'] = integrity_drops_df

    workbook_saved = save_excel_report_safely(
        excel_sheets, 
        workbook_filename,
        logger,
        threshold_rank=hold_threshold_rank
    )
    if workbook_saved:
        exported_paths.append(workbook_filename)

    if not workbook_saved:
        logger.warning("Excel-Export fehlgeschlagen, fallback auf CSV-Dateien.")
        filename = os.path.join(report_dir, f"{report_base}_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv")
        save_dataframe_safely(df_out, filename, sep=';', index=False, encoding='utf-8-sig')
        raw_data_filename = os.path.join(
            report_dir,
            f"{report_base}_raw_data_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv"
        )
        save_dataframe_safely(raw_df_out, raw_data_filename, sep=';', index=False, encoding='utf-8-sig')
        etf_summary_filename = os.path.join(
            report_dir,
            f"{report_base}_etf_summary_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv"
        )
        save_dataframe_safely(etf_summary_df, etf_summary_filename, sep=';', index=False, encoding='utf-8-sig')
        sector_summary_filename = os.path.join(
            report_dir,
            f"{report_base}_sector_summary_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv"
        )
        save_dataframe_safely(sector_summary_df_orig, sector_summary_filename, sep=';', index=False, encoding='utf-8-sig')
        industry_summary_filename = os.path.join(
            report_dir,
            f"{report_base}_industry_summary_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv"
        )
        save_dataframe_safely(industry_summary_for_export, industry_summary_filename, sep=';', index=False, encoding='utf-8-sig')
        cluster_summary_filename = os.path.join(
            report_dir,
            f"{report_base}_cluster_summary_{year}_kw{week:02d}_{combo_name}{preset_suffix}.csv"
        )
        save_dataframe_safely(cluster_summary_for_export, cluster_summary_filename, sep=';', index=False, encoding='utf-8-sig')
        exported_paths.extend([
            filename,
            raw_data_filename,
            etf_summary_filename,
            sector_summary_filename,
            industry_summary_filename,
            cluster_summary_filename,
        ])

    if update_last_run_cfg:
        new_run_cfg = {
            'daily_mode': True,
            'limit': 0,
            'save_json': 'j',
            'active_strategy_key': active_strategy["key"],
            'active_strategy_label': active_strategy["label"],
            'active_strategy_manual': active_strategy["is_manual"],
            'active_strategy_source': active_strategy["source"],
            'active_preset_key': active_strategy["key"],
            'active_preset_label': active_strategy["label"],
            'active_preset_manual': active_strategy["is_manual"],
        }
        save_json_config(config['last_run_config_file'], new_run_cfg)

    if data_mgr and data_mgr.failed_tickers:
        failed_records = data_mgr.get_failed_records()
        total_fail_events = sum(int(r.get('count', 0)) for r in failed_records)

        failed_payload = {
            'run_date': today.isoformat(),
            'combo': combo_name,
            'unique_failed_tickers': len(failed_records),
            'total_failed_events': total_fail_events,
            'records': failed_records
        }
        save_json_config(config['failed_tickers_json'], failed_payload)
        exported_paths.append(config['failed_tickers_json'])

        try:
            csv_rows = []
            for rec in failed_records:
                csv_rows.append({
                    'Run Date': today.isoformat(),
                    'Combo': combo_name,
                    'Ticker': rec.get('ticker', ''),
                    'Fail Count': rec.get('count', 0),
                    'Top Reason': rec.get('top_reason', ''),
                    'Top Reason Count': rec.get('top_reason_count', 0),
                    'First Failed At': rec.get('first_failed_at', ''),
                    'Last Failed At': rec.get('last_failed_at', ''),
                    'Reasons JSON': json.dumps(rec.get('reasons', {}), ensure_ascii=False, sort_keys=True)
                })
            pd.DataFrame(csv_rows).to_csv(
                config['failed_tickers_csv'],
                sep=';',
                index=False,
                encoding='utf-8-sig'
            )
            exported_paths.append(config['failed_tickers_csv'])
        except Exception as e:
            logger.error(f"Fehler beim Schreiben von {config['failed_tickers_csv']}: {e}")

        try:
            legacy_symbols = [r.get('ticker', '') for r in failed_records if r.get('ticker')]
            with open(config['failed_tickers_log'], 'a', encoding='utf-8') as f:
                f.write(f"{today.isoformat()} - ETF {combo_name}: {', '.join(legacy_symbols)}\n")
            exported_paths.append(config['failed_tickers_log'])
        except Exception as e:
            logger.error(f"Fehler beim Schreiben von {config['failed_tickers_log']}: {e}")

        logger.info(
            f"Failed-Ticker Report gespeichert: {len(failed_records)} Ticker / {total_fail_events} Fehlerevents."
        )

    if data_mgr and data_mgr.young_tickers:
        young_records = data_mgr.get_young_records()
        total_young_events = sum(int(r.get('count', 0)) for r in young_records)

        young_payload = {
            'run_date': today.isoformat(),
            'combo': combo_name,
            'unique_young_tickers': len(young_records),
            'total_young_events': total_young_events,
            'records': young_records
        }
        save_json_config(config['young_tickers_json'], young_payload)
        exported_paths.append(config['young_tickers_json'])

        try:
            csv_rows = []
            for rec in young_records:
                csv_rows.append({
                    'Run Date': today.isoformat(),
                    'Combo': combo_name,
                    'Ticker': rec.get('ticker', ''),
                    'Event Count': rec.get('count', 0),
                    'Top Reason': rec.get('top_reason', ''),
                    'Top Reason Count': rec.get('top_reason_count', 0),
                    'First Seen As Young': rec.get('first_failed_at', ''),
                    'Last Seen As Young': rec.get('last_failed_at', ''),
                    'Reasons JSON': json.dumps(rec.get('reasons', {}), ensure_ascii=False, sort_keys=True)
                })
            pd.DataFrame(csv_rows).to_csv(
                config['young_tickers_csv'],
                sep=';',
                index=False,
                encoding='utf-8-sig'
            )
            exported_paths.append(config['young_tickers_csv'])
        except Exception as e:
            logger.error(f"Fehler beim Schreiben von {config['young_tickers_csv']}: {e}")

        try:
            legacy_symbols = [r.get('ticker', '') for r in young_records if r.get('ticker')]
            with open(config['young_tickers_log'], 'a', encoding='utf-8') as f:
                f.write(f"{today.isoformat()} - ETF {combo_name}: {', '.join(legacy_symbols)}\n")
            exported_paths.append(config['young_tickers_log'])
        except Exception as e:
            logger.error(f"Fehler beim Schreiben von {config['young_tickers_log']}: {e}")

        logger.info(
            f"Young-Ticker Report gespeichert: {len(young_records)} Ticker / {total_young_events} Ereignisse."
        )

    if exported_paths:
        print("\n--- OUTPUT-DATEIEN ---")
        for path in sorted(set(exported_paths)):
            print(f" - {path}")

    # Kurz-Run-Summary
    print("\n--- RUN-SUMMARY ---")
    print(f" {sym_ok} Universe: {n_stocks} Aktien") # type: ignore
    if top_ind_set:
        print(f" {sym_ok} Top-Branchen: {len(top_ind_set)}")
    else:
        print(f" {sym_alert} Top-Branchen: -")
    print(f" {sym_portfolio} Depot: {len(current_tickers)} | Keep {len(keep_list)} | Warn {len(watch_list)} | Sell {len(sold_list)}")
    print(f" {sym_fire} Kandidaten: {len(suggested_candidates)}")
    print(f" {sym_ok} Strategie: {active_strategy['label']}")
    if workbook_filename:
        print(f" {sym_ok} Report: {workbook_filename}")

    # --- 4. INTERAKTIVER EXPLORER ---
    print("\n" + "="*100)
    print(" EXPLORER & DETAILS")
    print("="*100)
    print(" Gib einen Ticker ein, um Details zu sehen.")
    print(" Befehle: [buy TICKER] Kaufen | [sell TICKER] Verkaufen | [t] Ticker-Suche | [w TICKER] Web | [r] Report oeffnen | [q] Beenden | [m] Hauptmenue")
    
    while True:
        try:
            cmd_raw = input("\n[?] Befehl/Ticker: ").strip()
            if not cmd_raw: continue
            
            cmd = cmd_raw.lower()
            if cmd in ('q', 'quit', 'exit', 'ende'):
                sys.exit(0)
            
            if cmd in ('m', 'menu', 'menue'):
                break
            
            if cmd.startswith("w ") or cmd.startswith("web "):
                parts = cmd_raw.split(" ", 1)
                if len(parts) > 1:
                    raw_tickers = parts[1].replace(",", " ").split()
                    for t_str in raw_tickers:
                        t_sym = t_str.strip().upper()
                        if t_sym:
                            url = build_yahoo_quote_url(t_sym)
                            if url:
                                print(f" {sym_ok} Oeffne {t_sym} im Browser...")
                                webbrowser.open(url)
                continue
            
            if cmd.startswith("buy "):
                ticker_to_buy = cmd[4:].strip().upper()
                if not ticker_to_buy:
                    print(f" {sym_alert} Bitte einen Ticker nach 'buy' angeben.")
                    continue
                
                if ticker_to_buy in current_tickers:
                    print(f" {sym_alert} Ticker '{ticker_to_buy}' ist bereits im Depot.")
                    continue
                
                stock_to_add = symbol_lookup.get(ticker_to_buy)
                if not stock_to_add:
                    print(f" {sym_alert} Ticker '{ticker_to_buy}' wurde im aktuellen Universum nicht gefunden.")
                    continue
                
                # Add to portfolio
                new_entry = {
                    'Yahoo_Symbol': stock_to_add.yahoo_symbol, 
                    'Name': stock_to_add.name,
                    'Kaufdatum': datetime.date.today().isoformat()
                }
                
                updated_pf = portfolio_mgr.current_portfolio + [new_entry]
                portfolio_mgr.save(updated_pf)
                portfolio_mgr.current_portfolio = updated_pf # Update local state
                current_tickers.add(ticker_to_buy)
                print(f" {sym_ok} Position {ticker_to_buy} hinzugefuegt.")
                continue

            if cmd.startswith("sell "):
                ticker_to_sell = cmd[5:].strip().upper()
                if not ticker_to_sell:
                    print(f" {sym_alert} Bitte einen Ticker angeben.")
                    continue
                new_pf = [p for p in portfolio_mgr.current_portfolio if str(p.get('Yahoo_Symbol', '')).strip().upper() != ticker_to_sell]
                if len(new_pf) < len(portfolio_mgr.current_portfolio):
                    portfolio_mgr.save(new_pf)
                    portfolio_mgr.current_portfolio = new_pf
                    if ticker_to_sell in current_tickers:
                        current_tickers.remove(ticker_to_sell)
                    print(f" {sym_ok} Position {ticker_to_sell} entfernt.")
                else:
                    print(f" {sym_alert} Ticker nicht im Depot.")
                continue

            if cmd == "t":
                q = input("Suchbegriff: ").strip().lower()
                if q:
                    hits = [s for s in stock_results if q in str(s.yahoo_symbol).lower() or q in str(s.name).lower()]
                    print(f"Gefunden: {len(hits)}")
                    for h in hits[:10]:
                        print(f" - {h.yahoo_symbol}: {h.name}")
                continue

            if cmd == "r":
                if workbook_filename and os.path.exists(workbook_filename):
                    print(f" {sym_ok} Oeffne Report...")
                    webbrowser.open(workbook_filename)
                else:
                    print(f" {sym_alert} Kein Report.")
                continue

            if cmd.upper() in symbol_lookup:
                s_obj = symbol_lookup[cmd.upper()]
                print(f"Details fuer {s_obj.yahoo_symbol}: RSL={s_obj.rsl:.3f}, Kurs={s_obj.kurs:.2f}, Branche={s_obj.industry}")
            else:
                print("Unbekannter Befehl oder Ticker.")

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"UI Fehler: {e}")
