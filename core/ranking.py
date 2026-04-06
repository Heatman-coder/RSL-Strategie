from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
import numpy as np


def calculate_market_regime(stock_results: List[Any]) -> Dict[str, Any]:
    """Berechnet die Marktbreite und das daraus resultierende Markt-Regime."""
    if not stock_results:
        return {'breadth_pct': 0, 'regime': 'UNBEKANNT', 'strong_count': 0, 'total_count': 0, 'median_rsl': 0}

    total_count = len(stock_results)
    strong_count = sum(1 for s in stock_results if s.rsl > 1.0)
    
    rsl_values = [float(s.rsl) for s in stock_results if hasattr(s, 'rsl')]
    median_rsl = float(np.median(rsl_values)) if rsl_values else 0.0
    breadth = (strong_count / total_count) if total_count > 0 else 0.0

    if breadth > 0.40: regime = "STARK"
    elif breadth >= 0.20: regime = "NORMAL"
    else: regime = "SCHWACH"

    if regime == "STARK" and median_rsl < 1.0: regime = "NORMAL"
    if regime == "SCHWACH" and median_rsl >= 1.0: regime = "NORMAL"

    return {'breadth_pct': breadth * 100, 'regime': regime, 'strong_count': strong_count, 'total_count': total_count, 'median_rsl': median_rsl}

def apply_relative_context_metrics(stock_results: List[Any]) -> None:
    """Berechnet Branchen-Mediane und Peer-Spreads."""
    from collections import defaultdict
    industry_rsl_map = defaultdict(list)
    for stock in stock_results:
        ind = str(getattr(stock, "industry", "Unknown")).strip() or "Unknown"
        industry_rsl_map[ind].append(float(stock.rsl))

    industry_median_map = {ind: float(np.median(vals)) for ind, vals in industry_rsl_map.items() if vals}

    for stock in stock_results:
        ind = str(getattr(stock, "industry", "Unknown")).strip() or "Unknown"
        median_rsl = industry_median_map.get(ind, 0.0)
        stock.industry_median_rsl = median_rsl
        stock.peer_spread = float(stock.rsl) - median_rsl if median_rsl > 0 else 0.0
        if getattr(stock, "mom_score_adj", None) is None and getattr(stock, "mom_score", None) is not None:
            vol = getattr(stock, "mom_vol", 0)
            stock.mom_score_adj = stock.mom_score / vol if vol and vol > 0 else stock.mom_score

def apply_standard_rankings(stock_results: List[Any]) -> None:
    """
    Berechnet RSL- und Marktkapitalisierungs-Ränge einheitlich auf Basis der aktuellen Liste.
    Führt am Ende eine finale Sortierung nach RSL (absteigend) durch.
    """
    if not stock_results:
        return

    # 1. RSL Ranking (Bester RSL = Rang 1)
    stock_results.sort(key=lambda x: (float(getattr(x, 'rsl', 0.0)) if not np.isnan(float(getattr(x, 'rsl', 0.0))) else -1.0), reverse=True)
    for i, s in enumerate(stock_results): s.rsl_rang = i + 1
    
    # 2. MktCap Ranking (Größte Firma = Rang 1)
    stock_results.sort(key=lambda x: (float(getattr(x, 'market_value', 0.0)) if not np.isnan(float(getattr(x, 'market_value', 0.0))) else -1.0), reverse=True)
    for i, s in enumerate(stock_results): s.mktcap_rang = i + 1
    
    # 3. Zurück zur RSL-Sortierung für die Anzeige im Dashboard
    stock_results.sort(key=lambda x: (float(getattr(x, 'rsl', 0.0)) if not np.isnan(float(getattr(x, 'rsl', 0.0))) else -1.0), reverse=True)

def split_source_etfs(source_value: str) -> List[str]:
    return [p.strip() for p in str(source_value).split(",") if p and p.strip()]


def rank_thresholds(universe_size: int, top_percent_threshold: float) -> Tuple[int, int]:
    hold_rank = max(1, int(universe_size * top_percent_threshold))
    warn_rank = max(1, int(hold_rank * 0.9))
    return hold_rank, warn_rank


def evaluate_rank(rank: int, universe_size: int, top_percent_threshold: float) -> Dict[str, Any]:
    hold_rank, warn_rank = rank_thresholds(universe_size, top_percent_threshold)
    if rank > hold_rank:
        status = "SELL"
    elif rank >= warn_rank:
        status = "WARN"
    else:
        status = "HOLD"
    return {
        "status": status,
        "rank": rank,
        "universe_size": universe_size,
        "hold_rank": hold_rank,
        "warn_rank": warn_rank,
        "distance": hold_rank - rank,
    }


def rank_percent(eval_result: Optional[Dict[str, Any]]) -> Optional[float]:
    if not eval_result:
        return None
    try:
        rank = int(eval_result.get("rank", 0))
        universe_size = int(eval_result.get("universe_size", 0))
        if rank <= 0 or universe_size <= 0:
            return None
        return (rank / universe_size) * 100.0
    except Exception:
        return None


def format_percent_value(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.1f}%".replace(".", ",")


def format_percent_bar(
    value: Optional[float],
    width: int = 20,
    full_at: float = 25.0,
    marker_at: float = 20.0,
) -> str:
    if value is None:
        return f"[{'-' * width}]   n/a"
    try:
        clipped = max(0.0, min(100.0, float(value)))
    except Exception:
        return f"[{'-' * width}]   n/a"
    effective_full_at = full_at if full_at > 0 else 25.0
    filled = int(round((clipped / effective_full_at) * width))
    filled = max(0, min(width, filled))
    bar_chars = ["-"] * width
    for idx in range(filled):
        bar_chars[idx] = "="
    marker_idx = max(0, min(width - 1, int(round(width * (marker_at / effective_full_at))) - 1))
    bar_chars[marker_idx] = "|"
    bar = "".join(bar_chars)
    pct_txt = f"{clipped:5.1f}%".replace(".", ",")
    overflow = " !" if clipped > effective_full_at else ""
    rendered = f"[{bar}] {pct_txt}{overflow}"
    if clipped > effective_full_at:
        return f"\033[91m{rendered}\033[0m"
    if clipped >= marker_at:
        return f"\033[93m{rendered}\033[0m"
    return f"\033[92m{rendered}\033[0m"


def risk_bucket(worst_pct: Optional[float]) -> str:
    if worst_pct is None:
        return "UNBEKANNT"
    try:
        value = float(worst_pct)
    except Exception:
        return "UNBEKANNT"
    if value > 25.0:
        return "KRITISCH"
    if value > 20.0:
        return "NAH"
    return "OK"


def shorten_text(value: str, max_len: int) -> str:
    s = str(value or "").strip()
    if len(s) <= max_len:
        return s
    if max_len <= 3:
        return s[:max_len]
    return s[: max_len - 3] + "..."


def format_scope_reason(scope_label: str, eval_result: Dict[str, Any]) -> str:
    return (
        f"{scope_label}: Rang {eval_result['rank']}/{eval_result['universe_size']} "
        f"(Top-25%-Grenze {eval_result['hold_rank']})"
    )


def summarize_reasons(reason_lines: List[str], max_items: int = 3) -> str:
    if not reason_lines:
        return ""
    if len(reason_lines) <= max_items:
        return "; ".join(reason_lines)
    return "; ".join(reason_lines[:max_items]) + f"; +{len(reason_lines) - max_items} weitere"


def sort_portfolio_items_by_rank(
    portfolio_items: List[Dict[str, Any]], symbol_lookup: Dict[str, Any]
) -> List[Dict[str, Any]]:
    def _sort_key(item: Dict[str, Any]) -> Tuple[int, str]:
        ticker = str(item.get("Yahoo_Symbol", "")).strip().upper()
        stock = symbol_lookup.get(ticker)
        rank = int(stock.rsl_rang) if stock else 10**9
        return rank, ticker

    return sorted(portfolio_items, key=_sort_key)


def build_multiscope_status_map(
    stock_results: List[Any],
    top_percent_threshold: float,
    etf_options: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if not stock_results:
        return {}

    symbol_to_stock = {s.yahoo_symbol: s for s in stock_results}

    sector_groups: Dict[str, List[Any]] = defaultdict(list)
    for s in stock_results:
        sector_name = str(s.sector).strip() if s.sector else "Unbekannt"
        if not sector_name:
            sector_name = "Unbekannt"
        sector_groups[sector_name].append(s)

    sector_eval_map: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for sector_name, members in sector_groups.items():
        members_sorted = sorted(members, key=lambda x: x.rsl, reverse=True)
        universe_size = len(members_sorted)
        for idx, member in enumerate(members_sorted, start=1):
            sector_eval_map[member.yahoo_symbol][sector_name] = evaluate_rank(idx, universe_size, top_percent_threshold)

    industry_groups: Dict[str, List[Any]] = defaultdict(list)
    for s in stock_results:
        industry_name = str(s.industry).strip() if s.industry else "Unbekannt"
        if not industry_name:
            industry_name = "Unbekannt"
        industry_groups[industry_name].append(s)

    industry_eval_map: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for industry_name, members in industry_groups.items():
        members_sorted = sorted(members, key=lambda x: x.rsl, reverse=True)
        universe_size = len(members_sorted)
        for idx, member in enumerate(members_sorted, start=1):
            industry_eval_map[member.yahoo_symbol][industry_name] = evaluate_rank(idx, universe_size, top_percent_threshold)

    etf_groups: Dict[str, List[Any]] = defaultdict(list)
    for s in stock_results:
        for etf_sym in set(split_source_etfs(s.source_etf)):
            etf_groups[etf_sym].append(s)

    etf_eval_map: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for etf_sym, members in etf_groups.items():
        members_sorted = sorted(members, key=lambda x: x.rsl, reverse=True)
        universe_size = len(members_sorted)
        for idx, member in enumerate(members_sorted, start=1):
            etf_eval_map[member.yahoo_symbol][etf_sym] = evaluate_rank(idx, universe_size, top_percent_threshold)

    status_map: Dict[str, Dict[str, Any]] = {}
    global_universe_size = len(stock_results)

    for symbol, stock in symbol_to_stock.items():
        reason_sell: List[str] = []
        reason_warn: List[str] = []
        scope_sell_codes = set()
        scope_warn_codes = set()
        scope_sell_labels: List[str] = []
        scope_warn_labels: List[str] = []

        def _append_unique(items: List[str], value: str):
            if value and value not in items:
                items.append(value)

        global_eval = evaluate_rank(stock.rsl_rang, global_universe_size, top_percent_threshold)
        if global_eval["status"] == "SELL":
            reason_sell.append(format_scope_reason("Gesamtliste", global_eval))
            scope_sell_codes.add("G")
            _append_unique(scope_sell_labels, "Gesamtliste")
        elif global_eval["status"] == "WARN":
            reason_warn.append(format_scope_reason("Gesamtliste", global_eval))
            scope_warn_codes.add("G")
            _append_unique(scope_warn_labels, "Gesamtliste")
        
        rank_global = global_eval.get("rank")
        count_global = global_eval.get("universe_size")

        sector_name = str(stock.sector).strip() if stock.sector else "Unbekannt"
        if not sector_name:
            sector_name = "Unbekannt"
        sector_eval = sector_eval_map.get(symbol, {}).get(sector_name)
        rank_sector, count_sector = None, None
        if sector_eval:
            rank_sector = sector_eval.get("rank")
            count_sector = sector_eval.get("universe_size")
            if sector_eval["status"] == "SELL":
                reason_sell.append(format_scope_reason(f"Sektor {sector_name}", sector_eval))
                scope_sell_codes.add("S")
                _append_unique(scope_sell_labels, f"Sektor: {sector_name}")
            elif sector_eval["status"] == "WARN":
                reason_warn.append(format_scope_reason(f"Sektor {sector_name}", sector_eval))
                scope_warn_codes.add("S")
                _append_unique(scope_warn_labels, f"Sektor: {sector_name}")

        industry_name = str(stock.industry).strip() if stock.industry else "Unbekannt"
        if not industry_name:
            industry_name = "Unbekannt"
        industry_eval = industry_eval_map.get(symbol, {}).get(industry_name)
        rank_industry, count_industry = None, None
        if industry_eval:
            rank_industry = industry_eval.get("rank")
            count_industry = industry_eval.get("universe_size")
            if industry_eval["status"] == "SELL":
                reason_sell.append(format_scope_reason(f"Branche {industry_name}", industry_eval))
                scope_sell_codes.add("I")
                _append_unique(scope_sell_labels, f"Branche: {industry_name}")
            elif industry_eval["status"] == "WARN":
                reason_warn.append(format_scope_reason(f"Branche {industry_name}", industry_eval))
                scope_warn_codes.add("I")
                _append_unique(scope_warn_labels, f"Branche: {industry_name}")

        worst_etf_eval = None
        worst_etf_sym = None
        for etf_sym in set(split_source_etfs(stock.source_etf)):
            etf_eval = etf_eval_map.get(symbol, {}).get(etf_sym)
            if not etf_eval:
                continue
            
            # Der "Trigger-ETF" ist derjenige mit dem schlechtesten relativen Rang (höchstes Perzentil)
            if worst_etf_eval is None or (etf_eval["rank"] / etf_eval["universe_size"]) > (worst_etf_eval["rank"] / worst_etf_eval["universe_size"]):
                worst_etf_eval = etf_eval
                worst_etf_sym = etf_sym

            etf_name = etf_options.get(etf_sym, {}).get("name", etf_sym)
            scope_label = f"ETF {etf_sym} ({etf_name})"
            if etf_eval["status"] == "SELL":
                reason_sell.append(format_scope_reason(scope_label, etf_eval))
                scope_sell_codes.add("E")
                _append_unique(scope_sell_labels, f"ETF: {etf_sym} ({etf_name})")
            elif etf_eval["status"] == "WARN":
                reason_warn.append(format_scope_reason(scope_label, etf_eval))
                scope_warn_codes.add("E")
                _append_unique(scope_warn_labels, f"ETF: {etf_sym} ({etf_name})")

        if reason_sell:
            overall_status = "SELL"
            primary_reason = summarize_reasons(reason_sell)
            trigger_scope_code = "".join([c for c in "GSIE" if c in scope_sell_codes])
            trigger_scope_labels = scope_sell_labels
        elif reason_warn:
            overall_status = "WARN"
            primary_reason = summarize_reasons(reason_warn)
            trigger_scope_code = "".join([c for c in "GSIE" if c in scope_warn_codes])
            trigger_scope_labels = scope_warn_labels
        else:
            overall_status = "HOLD"
            primary_reason = "Innerhalb Top-25% in Gesamtliste, Sektor und zugehoerigen ETFs."
            trigger_scope_code = "-"
            trigger_scope_labels = []

        pct_global = rank_percent(global_eval)
        pct_sector = rank_percent(sector_eval)
        pct_industry = rank_percent(industry_eval)
        
        pct_etf = rank_percent(worst_etf_eval)
        rank_etf = worst_etf_eval.get("rank") if worst_etf_eval else None
        count_etf = worst_etf_eval.get("universe_size") if worst_etf_eval else None
        name_etf = etf_options.get(worst_etf_sym, {}).get("name", worst_etf_sym) if worst_etf_sym else "-"

        trigger_scope_text = " | ".join(trigger_scope_labels) if trigger_scope_labels else "-"

        status_map[symbol] = {
            "overall_status": overall_status,
            "reason_sell": summarize_reasons(reason_sell),
            "reason_warn": summarize_reasons(reason_warn),
            "primary_reason": primary_reason,
            "trigger_scope_code": trigger_scope_code,
            "trigger_scope_text": trigger_scope_text,
            "pct_global": pct_global,
            "pct_sector": pct_sector,
            "pct_industry": pct_industry,
            "pct_etf": pct_etf,
            "rank_global": rank_global,
            "count_global": count_global,
            "rank_sector": rank_sector,
            "count_sector": count_sector,
            "rank_industry": rank_industry,
            "count_industry": count_industry,
            "rank_etf": rank_etf,
            "count_etf": count_etf,
            "name_etf": name_etf,
        }

    return status_map
