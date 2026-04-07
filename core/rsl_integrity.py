"""
Modul für die RSL-Integritätsprüfung.
PHILOSOPHIE: Aktien werden niemals allein aufgrund von hohen oder niedrigen Kennzahlen (wie RSL) ausgeschlossen.
Wenn Werte unplausibel erscheinen, muss die Ursache in der Datenbasis oder Berechnung untersucht werden,
anstatt den Wert zu löschen. Die Integritätsprüfung dient der Markierung und Warnung, nicht der Zensur.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd


@dataclass
class IntegrityAssessment:
    is_valid: bool = True
    needs_review: bool = False
    hard_fail_reasons: List[str] = field(default_factory=list)
    warning_reasons: List[str] = field(default_factory=list)
    review_reasons: List[str] = field(default_factory=list)

    def add_hard_fail(self, reason: str) -> None:
        if reason not in self.hard_fail_reasons:
            self.hard_fail_reasons.append(reason)
        self.is_valid = False

    def add_warning(self, reason: str) -> None:
        if reason not in self.warning_reasons:
            self.warning_reasons.append(reason)

    def add_review(self, reason: str) -> None:
        if reason not in self.review_reasons:
            self.review_reasons.append(reason)
        self.needs_review = True

    def to_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "needs_review": self.needs_review,
            "hard_fail_reasons": self.hard_fail_reasons,
            "warning_reasons": self.warning_reasons,
            "review_reasons": self.review_reasons,
        }

SECONDARY_LISTING_SUFFIXES = {
    ".BE",
    ".DE",
    ".DU",
    ".F",
    ".HM",
    ".MU",
    ".SG",
}


def _context_value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _clean_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _symbol_suffix(symbol: Any) -> str:
    sym = _clean_symbol(symbol)
    if "." not in sym:
        return ""
    return f".{sym.rsplit('.', 1)[-1]}"


def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    parsed = _optional_float(value)
    if parsed is None:
        return float(default)
    return parsed


def _home_suffix(item: Any, location_suffix_map: Dict[str, str]) -> str:
    land = str(_context_value(item, "land", "") or "").strip()
    return str(location_suffix_map.get(land, "") or "").strip().upper()


def get_history_status(item: Any, location_suffix_map: Dict[str, str]) -> str:
    original_ticker = _clean_symbol(_context_value(item, "original_ticker", ""))
    history_symbol = _clean_symbol(_context_value(item, "yahoo_symbol", original_ticker))
    primary_liquidity_symbol = _clean_symbol(_context_value(item, "primary_liquidity_symbol", ""))
    home_suffix = _home_suffix(item, location_suffix_map)
    original_suffix = _symbol_suffix(original_ticker)
    history_suffix = _symbol_suffix(history_symbol)

    history_matches_home = False
    if primary_liquidity_symbol:
        history_matches_home = history_symbol == primary_liquidity_symbol
    if not history_matches_home and home_suffix:
        history_matches_home = history_symbol.endswith(home_suffix)

    original_is_secondary = bool(
        original_suffix
        and original_suffix in SECONDARY_LISTING_SUFFIXES
        and original_suffix != home_suffix
    )
    history_is_secondary = bool(
        history_suffix
        and history_suffix in SECONDARY_LISTING_SUFFIXES
        and history_suffix != home_suffix
    )

    if history_matches_home and original_ticker and history_symbol and original_ticker != history_symbol:
        return "OVERRIDDEN_TO_HOME" if home_suffix and history_symbol.endswith(home_suffix) else "OVERRIDDEN_TO_BETTER_LISTING"
    if history_is_secondary:
        return "SECONDARY_HISTORY_ACTIVE"
    if original_is_secondary and history_symbol and history_symbol != original_ticker:
        return "OVERRIDDEN_TO_BETTER_LISTING"
    if home_suffix and history_symbol.endswith(home_suffix):
        return "HOME_HISTORY_ACTIVE"
    if history_symbol and "." not in history_symbol:
        return "PRIMARY_WITHOUT_SUFFIX"
    return "UNKNOWN"


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Dict[str, str],
    config: Dict[str, Any],
    raw_rsl: Any = None,
) -> List[str]:
    """
    Sammelt Integritätswarnungen. 
    WICHTIG: Gründe, die hier landen, führen im Standard-Modus nur zur Markierung, 
    nicht zum Ausschluss aus dem Ranking (außer bei mathematischer Unmöglichkeit).
    """
    reasons: List[str] = []
    
    # Extraktion der Zustands-Flags (numerisch/enum-basiert bevorzugt)
    trust_score = int(_context_value(item, "trust_score", 3))
    flag_stale = str(_context_value(item, "flag_stale", "OK")).upper()
    flag_hist = str(_context_value(item, "flag_history_length", "OK")).upper()
    flag_scale = str(_context_value(item, "flag_scale", "OK")).upper()

    # 1. Kritische Hardware-Fehler (Daten unvollständig)
    if flag_stale == "CRITICAL":
        reasons.append("critical_stale_data")
    if flag_hist == "CRITICAL":
        reasons.append("critical_history_length")
    
    # 2. Skalierungsfehler (Split-Glitches etc.)
    if flag_scale == "CRITICAL":
        reasons.append("critical_price_scale")
    elif flag_scale == "WARN":
        reasons.append("suspicious_price_scale")

    # 3. Vertrauenswürdigkeit (Aggregation)
    if trust_score < 1:
        reasons.append("low_trust_score")

    # 4. Mathematische Validität (Absolutes Minimum)
    raw_rsl_value = _optional_float(raw_rsl) or _optional_float(_context_value(item, "rsl", None))
    rsl_val = _safe_float(raw_rsl_value, 0.0)
    if rsl_val <= 0:
        reasons.append("no_valid_rsl_data")

    return list(dict.fromkeys(reasons))


def assess_integrity(item: Any, location_suffix_map: Dict[str, str], config: Dict[str, Any]) -> IntegrityAssessment:
    """Nutzt die bestehende Logik, um ein IntegrityAssessment Objekt zu befüllen."""
    assessment = IntegrityAssessment()
    reasons = get_rsl_integrity_drop_reasons(item, location_suffix_map, config)
    
    # Definition Hard Fail (identisch zu deiner bestehenden Logik)
    hard_fail_criteria = {"no_valid_rsl_data", "critical_history_length"}
    # Definition Review (Verdacht auf Skalierungsfehler oder niedriges Vertrauen)
    review_criteria = {"suspicious_price_scale", "low_trust_score"}

    for r in reasons:
        if r in hard_fail_criteria:
            assessment.add_hard_fail(r)
        elif r in review_criteria:
            assessment.add_review(r)
        else:
            # Alles andere (z.B. stale data, price scale critical) sind Warnings
            # Hinweis: critical_price_scale ist bei dir kein Hard Fail für den Ausschluss,
            # daher landet es hier in den Warnings.
            assessment.add_warning(r)
            
    # Falls wir Hard Fails haben, setzen wir is_valid auf False (passiert automatisch in add_hard_fail)
    if any(r in hard_fail_criteria for r in reasons):
        assessment.is_valid = False
        
    return assessment

def filter_stock_results_for_rsl_integrity(
    stock_results: List[Any],
    location_suffix_map: Dict[str, str],
    config: Dict[str, Any],
) -> Tuple[List[Any], pd.DataFrame]:
    valid_results: List[Any] = []
    dropped_rows: List[Dict[str, Any]] = []

    for stock in stock_results or []:
        assessment = assess_integrity(stock, location_suffix_map, config)
        reasons = get_rsl_integrity_drop_reasons(stock, location_suffix_map, config)
        
        has_hard_fail = not assessment.is_valid

        if not has_hard_fail:
            valid_results.append(stock)
            # Falls es Warnungen gab, hängen wir sie als Metadaten an das Objekt
            # HINWEIS: Wir fangen AttributeError ab, falls StockData __slots__ nutzt
            if reasons:
                try:
                    setattr(stock, "integrity_warnings", reasons)
                except AttributeError:
                    pass
        
        if reasons:
            # Diese Aktien landen NICHT in valid_results, werden aber im dropped_df erfasst
            dropped_rows.append(
                {
                    "original_ticker": _context_value(stock, "original_ticker", ""),
                    "yahoo_symbol": _context_value(stock, "yahoo_symbol", ""),
                    "name": _context_value(stock, "name", ""),
                    "land": _context_value(stock, "land", ""),
                    "history_status": get_history_status(stock, location_suffix_map),
                    "drop_reasons": ", ".join(reasons),
                    "is_valid": assessment.is_valid,
                    "needs_review": assessment.needs_review,
                    "hard_fail_reasons": "; ".join(assessment.hard_fail_reasons),
                    "warning_reasons": "; ".join(assessment.warning_reasons),
                    "review_reasons": "; ".join(assessment.review_reasons),
                    "rsl": _context_value(stock, "rsl", None),
                    "mom_12m": _context_value(stock, "mom_12m", None),
                    "mom_6m": _context_value(stock, "mom_6m", None),
                    "mom_3m": _context_value(stock, "mom_3m", None),
                    "rsl_change_1w": _context_value(stock, "rsl_change_1w", None),
                    "trust_score": _context_value(stock, "trust_score", None),
                    "trend_smoothness": _context_value(stock, "trend_smoothness", None),
                    "trend_quality": _context_value(stock, "trend_quality", None),
                    "flag_scale": _context_value(stock, "flag_scale", None),
                    "flag_stale": _context_value(stock, "flag_stale", None),
                    "stale_reason": _context_value(stock, "stale_reason", None),
                    "flag_history_length": _context_value(stock, "flag_history_length", None),
                    "history_length_reason": _context_value(stock, "history_length_reason", None),
                    "flag_gap": _context_value(stock, "flag_gap", None),
                }
            )

    columns = [
        "original_ticker",
        "yahoo_symbol",
        "name",
        "land",
        "history_status",
        "drop_reasons",
        "is_valid",
        "needs_review",
        "hard_fail_reasons",
        "warning_reasons",
        "review_reasons",
        "rsl",
        "mom_12m",
        "mom_6m",
        "mom_3m",
        "rsl_change_1w",
        "trust_score",
        "trend_smoothness",
        "trend_quality",
        "flag_scale",
        "flag_stale",
        "stale_reason",
        "flag_history_length",
        "history_length_reason",
        "flag_gap",
    ]
    dropped_df = pd.DataFrame(dropped_rows, columns=columns)
    return valid_results, dropped_df


def build_home_market_rsl_audit(
    results: List[Any],
    location_suffix_map: Dict[str, str],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for stock in results or []:
        history_status = get_history_status(stock, location_suffix_map)
        history_symbol = _clean_symbol(_context_value(stock, "yahoo_symbol", ""))
        primary_liquidity_symbol = _clean_symbol(_context_value(stock, "primary_liquidity_symbol", ""))
        home_suffix = _home_suffix(stock, location_suffix_map)
        history_matches_home = False
        if primary_liquidity_symbol:
            history_matches_home = history_symbol == primary_liquidity_symbol
        if not history_matches_home and home_suffix:
            history_matches_home = history_symbol.endswith(home_suffix)

        review_reasons: List[str] = []
        if history_status == "SECONDARY_HISTORY_ACTIVE":
            review_reasons.append("secondary_history_active")
            if primary_liquidity_symbol and primary_liquidity_symbol != history_symbol:
                review_reasons.append("secondary_without_override")

        flag_scale = str(_context_value(stock, "flag_scale", "") or "").strip().upper()
        if flag_scale and flag_scale != "OK":
            review_reasons.append("scale_flag_active")

        review_reasons = list(dict.fromkeys(review_reasons))
        review_score = 0
        weight_map = {
            "secondary_history_active": 5,
            "secondary_without_override": 4,
            "scale_flag_active": 3,
        }
        for reason in review_reasons:
            review_score += weight_map.get(reason, 1)

        rows.append(
            {
                "history_status": history_status,
                "needs_review": bool(review_reasons),
                "review_score": review_score,
                "review_reasons": ", ".join(review_reasons),
                "history_matches_home": bool(history_matches_home),
                "home_suffix": home_suffix,
                "original_ticker": _context_value(stock, "original_ticker", ""),
                "history_symbol": _context_value(stock, "yahoo_symbol", ""),
                "primary_liquidity_symbol": _context_value(stock, "primary_liquidity_symbol", ""),
                "primary_liquidity_basis": _context_value(stock, "primary_liquidity_basis", ""),
                "name": _context_value(stock, "name", ""),
                "isin": _context_value(stock, "isin", ""),
                "land": _context_value(stock, "land", ""),
                "listing_source": _context_value(stock, "listing_source", ""),
                "source_etf": _context_value(stock, "source_etf", ""),
                "rsl_rank": _context_value(stock, "rsl_rang", None),
                "rsl": _context_value(stock, "rsl", None),
                "mom_6m": _context_value(stock, "mom_6m", None),
                "mom_3m": _context_value(stock, "mom_3m", None),
                "rsl_change_1w": _context_value(stock, "rsl_change_1w", None),
                "trust_score": _context_value(stock, "trust_score", None),
                "flag_scale": _context_value(stock, "flag_scale", ""),
            }
        )

    columns = [
        "history_status",
        "needs_review",
        "review_score",
        "review_reasons",
        "history_matches_home",
        "home_suffix",
        "original_ticker",
        "history_symbol",
        "primary_liquidity_symbol",
        "primary_liquidity_basis",
        "name",
        "isin",
        "land",
        "listing_source",
        "source_etf",
        "rsl_rank",
        "rsl",
        "mom_6m",
        "mom_3m",
        "rsl_change_1w",
        "trust_score",
        "flag_scale",
    ]
    audit_df = pd.DataFrame(rows, columns=columns)
    if not audit_df.empty:
        audit_df = audit_df.sort_values(
            ["needs_review", "review_score", "rsl_rank", "original_ticker"],
            ascending=[False, False, True, True],
            na_position="last",
        ).reset_index(drop=True)
    return audit_df


def build_home_market_rsl_review_shortlist(audit_df: pd.DataFrame, top_rank: int = 300) -> pd.DataFrame:
    if audit_df is None or audit_df.empty:
        return pd.DataFrame(columns=list(getattr(audit_df, "columns", [])))

    work = audit_df.copy()
    if "needs_review" not in work.columns:
        return pd.DataFrame(columns=work.columns)

    try:
        top_rank = max(1, int(top_rank))
    except Exception:
        top_rank = 300

    if "rsl_rank" in work.columns:
        rank_series = pd.to_numeric(work["rsl_rank"], errors="coerce")
        work = work[rank_series <= float(top_rank)]

    work = work[work["needs_review"].astype(bool)]
    if work.empty:
        return work.reset_index(drop=True)

    sort_cols = [col for col in ["review_score", "rsl_rank", "original_ticker"] if col in work.columns]
    ascending = [False, True, True][: len(sort_cols)]
    return work.sort_values(sort_cols, ascending=ascending, na_position="last").reset_index(drop=True)
