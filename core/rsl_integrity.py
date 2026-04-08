from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


"""
RSL-Integritätslogik mit Fokus auf URSACHEN statt Symptomen.

PHILOSOPHIE
-----------
Ein hoher RSL-Wert ist kein Ausschlussgrund an sich. Wenn ein RSL-Wert
unplausibel hoch erscheint, muss die Datenursache identifiziert werden.
Nur die Ursache darf zu Review / Warning / Hard-Fail führen.

Diese Datei stellt daher Funktionen bereit für:
- Erkennung problematischer Preisserien
- Erkennung unplausibler Adj-Close-Anpassungen rund um Dividenden
- Aufbau einer "vertrauenswürdigeren" Preisbasis für RSL
- strukturierte Reasons (hard fail / warning / review)
- Rückwärtskompatibilität zu älteren drop_reasons-Workflows

RÜCKWÄRTSKOMPATIBILITÄT
-----------------------
- drop_reasons bleibt als Legacy-Feld verfügbar
- integrity_warnings bleibt als Legacy-Feld verfügbar
- get_rsl_integrity_drop_reasons(...) bleibt als Alias verfügbar

ERWARTETE DATEN
---------------
Die Funktionen sind defensiv gebaut und akzeptieren mehrere typische
Spaltennamen.

Für historische Kursdaten werden u. a. erkannt:

Datum:
- Date
- date
- Datetime
- index (falls DatetimeIndex)

Preise:
- Close
- Adj Close
- Open
- High
- Low

Volumen / Actions:
- Volume
- Dividends
- Stock Splits

RESULTAT
--------
Es wird eine "rsl_price"-Serie konstruiert:
- Standard: Adj Close
- Fallback: Close
- Lokaler Fallback rund um problematische Dividendenfenster

WICHTIG
-------
Diese Datei entfernt NICHT automatisch Werte nur wegen eines hohen RSL.
Sie markiert stattdessen die Ursachen.
Hard-Fails entstehen nur bei echten Datenproblemen.
"""


# ============================================================================
# Konfiguration
# ============================================================================

DEFAULT_CONFIG: Dict[str, Any] = {
    # Fenster rund um Dividendenereignisse
    "dividend_window_before": 20,
    "dividend_window_after": 7,

    # Trigger für "Adj Close weicht stark von Close ab"
    "adj_close_gap_warn_threshold": 0.08,   # 8 %
    "adj_close_gap_hard_threshold": 0.25,   # 25 %
    "adj_close_gap_hard_threshold_ratio": 2.0,  # Faktor 2.0 (oder 0.5) für extreme Abweichung
    "global_scale_fallback_min_fraction": 0.20,  # Mindestanteil Tage für globalen Fallback

    # Wenn die beobachtete Anpassung viel größer ist als die Dividende erklärt
    "dividend_multiplier_tolerance": 0.08,  # 8 %-Punkte Abweichung

    # Rendite-/Serienbruch-Trigger
    "daily_return_warn_threshold": 0.35,    # 35 %
    "daily_return_hard_threshold": 0.80,    # 80 %

    # Negative / nicht sinnvolle Preise
    "allow_zero_prices": False,

    # RSL Parameter
    "rsl_sma_window": 130,
    "min_history_rows_for_rsl": 130,

    # Sekundärmarkthinweis nur als Review
    "foreign_secondary_suffixes": [".F", ".BE", ".MU", ".DU", ".SG", ".HM"],

    # Wenn zu viele Fallback-Tage in der RSL-Basis vorkommen
    "fallback_fraction_warn": 0.10,
    "fallback_fraction_hard": 0.35,

    # Wenn zu viele Flat-Tage in Folge vorkommen
    "flat_run_warn": 10,
    "flat_run_hard": 25,
}


# ============================================================================
# Datenklassen
# ============================================================================

@dataclass
class IntegrityReasonSet:
    hard_fail_reasons: List[str] = field(default_factory=list)
    warning_reasons: List[str] = field(default_factory=list)
    review_reasons: List[str] = field(default_factory=list)

    def add(self, reason: str, severity: str = "review") -> None:
        severity = (severity or "review").lower().strip()

        if severity == "hard_fail":
            if reason not in self.hard_fail_reasons:
                self.hard_fail_reasons.append(reason)
        elif severity == "warning":
            if reason not in self.warning_reasons:
                self.warning_reasons.append(reason)
        else:
            if reason not in self.review_reasons:
                self.review_reasons.append(reason)

    def extend(self, other: "IntegrityReasonSet") -> None:
        for r in other.hard_fail_reasons:
            self.add(r, "hard_fail")
        for r in other.warning_reasons:
            self.add(r, "warning")
        for r in other.review_reasons:
            self.add(r, "review")

    def all_reasons(self) -> List[str]:
        return self.hard_fail_reasons + self.warning_reasons + self.review_reasons

    def legacy_drop_reasons(self) -> List[str]:
        """
        Legacy-Verhalten: Historisch wurden verschiedene Reasons pauschal
        als drop_reasons geführt.

        Für Rückwärtskompatibilität geben wir hier ALLE Reasons zurück.
        Die eigentliche Hard-Fail-Entscheidung basiert aber nur auf
        hard_fail_reasons.
        """
        return self.all_reasons()

    def has_hard_fail(self) -> bool:
        return len(self.hard_fail_reasons) > 0

    def has_any(self) -> bool:
        return bool(
            self.hard_fail_reasons
            or self.warning_reasons
            or self.review_reasons
        )


@dataclass
class PriceSeriesBuildResult:
    history: pd.DataFrame
    rsl_price_column: str
    used_close_fallback: bool
    reasons: IntegrityReasonSet
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Kleine Hilfsfunktionen
# ============================================================================

def _merge_config(user_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    if user_config:
        cfg.update(user_config)
    return cfg


def _safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, dict):
        return [value]
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    return [value]


def _unique_keep_order(values: Iterable[Any]) -> List[Any]:
    seen = set()
    out: List[Any] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if isinstance(out.index, pd.DatetimeIndex):
        return out.sort_index()

    date_col = _find_first_existing_column(
        out,
        ["Date", "date", "Datetime", "datetime", "timestamp"],
    )
    if date_col is not None:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).sort_values(date_col).set_index(date_col)
        return out

    try:
        out.index = pd.to_datetime(out.index, errors="coerce")
        out = out[~out.index.isna()].sort_index()
    except Exception:
        pass

    return out


def _coerce_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _get_price_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {
        "close": _find_first_existing_column(df, ["Close", "close", "Schluss", "schluss"]),
        "adj_close": _find_first_existing_column(df, ["Adj Close", "adj_close", "AdjClose", "adjusted_close"]),
        "open": _find_first_existing_column(df, ["Open", "open"]),
        "high": _find_first_existing_column(df, ["High", "high"]),
        "low": _find_first_existing_column(df, ["Low", "low"]),
        "volume": _find_first_existing_column(df, ["Volume", "volume", "Volumen", "volumen"]),
        "dividends": _find_first_existing_column(df, ["Dividends", "dividends", "Dividend", "dividend"]),
        "splits": _find_first_existing_column(df, ["Stock Splits", "stock_splits", "Splits", "splits"]),
    }


def _max_consecutive_equal(values: pd.Series) -> int:
    if values is None or len(values) == 0:
        return 0

    arr = values.astype(float).round(10).to_numpy()
    if len(arr) == 0:
        return 0

    max_run = 1
    current = 1

    for i in range(1, len(arr)):
        if np.isfinite(arr[i]) and np.isfinite(arr[i - 1]) and arr[i] == arr[i - 1]:
            current += 1
            max_run = max(max_run, current)
        else:
            current = 1

    return int(max_run)


def _safe_pct_diff(a: pd.Series, b: pd.Series) -> pd.Series:
    denom = b.replace(0, np.nan)
    return ((a - b).abs() / denom.abs()).replace([np.inf, -np.inf], np.nan)


def _rolling_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def _compute_rsl_from_series(price: pd.Series, sma_window: int = 130) -> pd.Series:
    sma = _rolling_sma(price, sma_window)
    return (price / sma).replace([np.inf, -np.inf], np.nan)


def _looks_like_foreign_secondary_listing(
    ticker: Optional[str],
    country: Optional[str],
    cfg: Dict[str, Any],
) -> bool:
    if not ticker:
        return False

    ticker = str(ticker).upper().strip()
    suffixes = cfg.get("foreign_secondary_suffixes", [])
    has_secondary_suffix = any(ticker.endswith(str(sfx).upper()) for sfx in suffixes)

    if not has_secondary_suffix:
        return False

    if not country:
        return True

    c = str(country).strip().lower()
    return c not in {"deutschland", "germany", "de", "ger"}


def _normalize_string(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _get_row_value(row: Any, candidates: List[str], default: Any = None) -> Any:
    if row is None:
        return default

    if isinstance(row, dict):
        for c in candidates:
            if c in row:
                return row[c]
        return default

    for c in candidates:
        try:
            if hasattr(row, c):
                return getattr(row, c)
        except Exception:
            pass

        try:
            if c in row:
                return row[c]
        except Exception:
            pass

    return default


def _extract_history_object(item: Any) -> Optional[pd.DataFrame]:
    history = _get_row_value(
        item,
        [
            "history",
            "price_history",
            "historical_data",
            "df_history",
            "history_df",
        ],
        None,
    )
    return history if isinstance(history, pd.DataFrame) else None


# ============================================================================
# Historienvalidierung
# ============================================================================

def normalize_history_frame(history_df: pd.DataFrame) -> pd.DataFrame:
    """
    Vereinheitlicht eine History-Tabelle:
    - DatetimeIndex
    - numerische Preis-/Action-Spalten
    """
    if history_df is None or len(history_df) == 0:
        return pd.DataFrame()

    df = _ensure_datetime_index(history_df)
    cols = _get_price_columns(df)

    numeric_cols = [c for c in cols.values() if c is not None]
    df = _coerce_numeric(df, numeric_cols)

    if cols["dividends"] is None:
        df["Dividends"] = 0.0
    if cols["splits"] is None:
        df["Stock Splits"] = 0.0

    return df.sort_index()


def validate_basic_history_integrity(
    history_df: pd.DataFrame,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[IntegrityReasonSet, Dict[str, Any]]:
    cfg = _merge_config(cfg)
    reasons = IntegrityReasonSet()
    diagnostics: Dict[str, Any] = {}

    df = normalize_history_frame(history_df)

    if df.empty:
        reasons.add("missing_history", "hard_fail")
        diagnostics["history_rows"] = 0
        return reasons, diagnostics

    cols = _get_price_columns(df)
    close_col = cols["close"]
    adj_col = cols["adj_close"]

    diagnostics["history_rows"] = int(len(df))

    if close_col is None and adj_col is None:
        reasons.add("missing_close_and_adjclose", "hard_fail")
        return reasons, diagnostics

    primary_price_col = adj_col or close_col
    price = df[primary_price_col].copy()

    nan_fraction = float(price.isna().mean()) if len(price) else 1.0
    diagnostics["primary_price_nan_fraction"] = nan_fraction

    if nan_fraction >= 0.50:
        reasons.add("price_series_too_many_missing_values", "hard_fail")
    elif nan_fraction >= 0.15:
        reasons.add("price_series_many_missing_values", "warning")

    valid_price = price.dropna()
    if len(valid_price) == 0:
        reasons.add("no_valid_price_values", "hard_fail")
        return reasons, diagnostics

    if (valid_price < 0).any():
        reasons.add("negative_price_values", "hard_fail")

    if not cfg.get("allow_zero_prices", False):
        if (valid_price <= 0).any():
            zero_frac = float((valid_price <= 0).mean())
            diagnostics["non_positive_price_fraction"] = zero_frac
            if zero_frac > 0.05:
                reasons.add("non_positive_prices_present", "hard_fail")
            else:
                reasons.add("sporadic_non_positive_prices_present", "warning")

    max_flat_run = _max_consecutive_equal(valid_price)
    diagnostics["max_flat_run"] = max_flat_run

    if max_flat_run >= cfg["flat_run_hard"]:
        reasons.add("stale_price_series_extreme_flat_run", "hard_fail")
    elif max_flat_run >= cfg["flat_run_warn"]:
        reasons.add("stale_price_series_flat_run", "warning")

    returns = valid_price.pct_change().replace([np.inf, -np.inf], np.nan)
    if len(returns.dropna()) > 0:
        max_abs_ret = float(returns.abs().max())
        diagnostics["max_abs_daily_return"] = max_abs_ret

        if max_abs_ret >= cfg["daily_return_hard_threshold"]:
            reasons.add("extreme_price_discontinuity", "hard_fail")
        elif max_abs_ret >= cfg["daily_return_warn_threshold"]:
            reasons.add("large_price_discontinuity", "warning")

    return reasons, diagnostics


# ============================================================================
# Dividenden-/AdjClose-Prüfung
# ============================================================================

def _get_dividend_col(df: pd.DataFrame) -> str:
    cols = _get_price_columns(df)
    if cols["dividends"] is not None:
        return cols["dividends"]

    if "Dividends" not in df.columns:
        df["Dividends"] = 0.0
    return "Dividends"


def _get_split_col(df: pd.DataFrame) -> str:
    cols = _get_price_columns(df)
    if cols["splits"] is not None:
        return cols["splits"]

    if "Stock Splits" not in df.columns:
        df["Stock Splits"] = 0.0
    return "Stock Splits"


def detect_dividend_adjustment_issues(
    history_df: pd.DataFrame,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[IntegrityReasonSet, Dict[str, Any], pd.DataFrame]:
    """
    Prüft, ob Adj Close rund um Dividendenereignisse unplausibel wirkt.

    Rückgabe:
    - IntegrityReasonSet
    - diagnostics
    - event_df mit pro Ereignis Diagnose
    """
    cfg = _merge_config(cfg)
    reasons = IntegrityReasonSet()
    diagnostics: Dict[str, Any] = {}

    df = normalize_history_frame(history_df)
    if df.empty:
        return reasons, diagnostics, pd.DataFrame()

    cols = _get_price_columns(df)
    close_col = cols["close"]
    adj_col = cols["adj_close"]

    if close_col is None or adj_col is None:
        diagnostics["skipped"] = "close_or_adjclose_missing"
        return reasons, diagnostics, pd.DataFrame()

    div_col = _get_dividend_col(df)
    split_col = _get_split_col(df)

    dividends = pd.to_numeric(df[div_col], errors="coerce").fillna(0.0)
    dividend_dates = df.index[dividends > 0].tolist()
    diagnostics["dividend_event_count"] = int(len(dividend_dates))

    if not dividend_dates:
        return reasons, diagnostics, pd.DataFrame()

    close = pd.to_numeric(df[close_col], errors="coerce")
    adj = pd.to_numeric(df[adj_col], errors="coerce")

    event_rows: List[Dict[str, Any]] = []

    for dt in dividend_dates:
        if dt not in df.index:
            continue

        loc = df.index.get_loc(dt)
        if isinstance(loc, slice) or isinstance(loc, np.ndarray):
            continue

        before_n = int(cfg["dividend_window_before"])
        after_n = int(cfg["dividend_window_after"])
        start = max(0, loc - before_n)
        end = min(len(df), loc + after_n + 1)

        w = df.iloc[start:end].copy()
        if len(w) < 3:
            continue

        w_close = pd.to_numeric(w[close_col], errors="coerce")
        w_adj = pd.to_numeric(w[adj_col], errors="coerce")
        w_div = pd.to_numeric(w[div_col], errors="coerce").fillna(0.0)
        w_splits = pd.to_numeric(w[split_col], errors="coerce").fillna(0.0)

        if (w_splits > 0).any():
            reasons.add("split_present_near_dividend_window", "review")

        ex_div = float(df.loc[dt, div_col]) if pd.notna(df.loc[dt, div_col]) else 0.0

        prior_close = np.nan
        prior_adj = np.nan
        for i in range(loc - 1, -1, -1):
            c_val = close.iloc[i]
            a_val = adj.iloc[i]
            if pd.notna(c_val) and c_val > 0:
                prior_close = float(c_val)
                if pd.notna(a_val):
                    prior_adj = float(a_val)
                break

        if not np.isfinite(prior_close) or prior_close <= 0:
            continue

        explained_gap = abs(ex_div / prior_close) if prior_close > 0 else np.nan

        gap_series = _safe_pct_diff(w_adj, w_close)
        median_gap = float(gap_series.median(skipna=True)) if len(gap_series.dropna()) else np.nan
        max_gap = float(gap_series.max(skipna=True)) if len(gap_series.dropna()) else np.nan

        close_ret = w_close.pct_change().replace([np.inf, -np.inf], np.nan)
        adj_ret = w_adj.pct_change().replace([np.inf, -np.inf], np.nan)

        max_abs_close_ret = float(close_ret.abs().max(skipna=True)) if len(close_ret.dropna()) else np.nan
        max_abs_adj_ret = float(adj_ret.abs().max(skipna=True)) if len(adj_ret.dropna()) else np.nan

        adj_worse_than_close = False
        if np.isfinite(max_abs_adj_ret) and np.isfinite(max_abs_close_ret):
            adj_worse_than_close = max_abs_adj_ret > (max_abs_close_ret + 0.15)

        negative_adj_present = bool((w_adj.dropna() < 0).any())

        event_problem_severity: Optional[str] = None
        event_reasons: List[str] = []

        if negative_adj_present:
            event_problem_severity = "hard_fail"
            event_reasons.append("negative_adjclose_in_dividend_window")

        if np.isfinite(median_gap) and np.isfinite(explained_gap):
            if median_gap > explained_gap + cfg["dividend_multiplier_tolerance"]:
                event_reasons.append("adjclose_close_gap_unplausible_for_dividend")

        if np.isfinite(max_gap):
            if max_gap >= cfg["adj_close_gap_hard_threshold"]:
                event_reasons.append("extreme_adjclose_close_gap_in_dividend_window")
                event_problem_severity = event_problem_severity or "hard_fail"
            elif max_gap >= cfg["adj_close_gap_warn_threshold"]:
                event_reasons.append("large_adjclose_close_gap_in_dividend_window")
                event_problem_severity = event_problem_severity or "warning"

        if adj_worse_than_close:
            event_reasons.append("adjclose_discontinuity_worse_than_close")
            if np.isfinite(max_abs_adj_ret) and max_abs_adj_ret >= cfg["daily_return_hard_threshold"]:
                event_problem_severity = event_problem_severity or "hard_fail"
            else:
                event_problem_severity = event_problem_severity or "warning"

        if event_reasons:
            sev = event_problem_severity or "warning"
            for r in event_reasons:
                reasons.add(r, sev)

            if "adjclose_close_gap_unplausible_for_dividend" in event_reasons and sev != "hard_fail":
                reasons.add("bad_dividend_adjustment", "warning")
            if "extreme_adjclose_close_gap_in_dividend_window" in event_reasons:
                reasons.add("bad_dividend_adjustment", "hard_fail")
            elif "large_adjclose_close_gap_in_dividend_window" in event_reasons:
                reasons.add("bad_dividend_adjustment", "warning")

        event_rows.append(
            {
                "event_date": dt,
                "dividend": ex_div,
                "prior_close": prior_close,
                "prior_adj": prior_adj,
                "window_dividend_sum": float(w_div.sum(skipna=True)),
                "explained_gap_estimate": explained_gap,
                "median_adj_close_gap": median_gap,
                "max_adj_close_gap": max_gap,
                "max_abs_close_return": max_abs_close_ret,
                "max_abs_adj_return": max_abs_adj_ret,
                "adj_worse_than_close": adj_worse_than_close,
                "negative_adj_present": negative_adj_present,
                "event_reasons": "|".join(event_reasons) if event_reasons else "",
                "event_severity": event_problem_severity or "",
            }
        )

    event_df = pd.DataFrame(event_rows)
    diagnostics["problem_dividend_events"] = (
        int((event_df["event_reasons"] != "").sum()) if not event_df.empty else 0
    )

    return reasons, diagnostics, event_df


# ============================================================================
# Aufbau der RSL-Preisserie
# ============================================================================

def build_rsl_price_series(
    history_df: pd.DataFrame,
    cfg: Optional[Dict[str, Any]] = None,
) -> PriceSeriesBuildResult:
    """
    Baut eine vertrauenswürdigere Preisserie für die RSL-Berechnung.

    Regel:
    - Standard: Adj Close
    - Falls Adj Close fehlt: Close
    - Falls rund um Dividendenfenster Adj Close unplausibel ist:
      lokal auf Close umschalten
    """
    cfg = _merge_config(cfg)
    reasons = IntegrityReasonSet()
    diagnostics: Dict[str, Any] = {}

    df = normalize_history_frame(history_df)

    if df.empty:
        reasons.add("missing_history", "hard_fail")
        return PriceSeriesBuildResult(
            history=df,
            rsl_price_column="rsl_price",
            used_close_fallback=False,
            reasons=reasons,
            diagnostics=diagnostics,
        )

    basic_reasons, basic_diag = validate_basic_history_integrity(df, cfg)
    reasons.extend(basic_reasons)
    diagnostics.update(basic_diag)

    cols = _get_price_columns(df)
    close_col = cols["close"]
    adj_col = cols["adj_close"]

    if close_col is None and adj_col is None:
        reasons.add("missing_close_and_adjclose", "hard_fail")
        df["rsl_price"] = np.nan
        df["rsl_price_source"] = "missing"
        return PriceSeriesBuildResult(df, "rsl_price", False, reasons, diagnostics)

    if adj_col is not None:
        df["rsl_price"] = pd.to_numeric(df[adj_col], errors="coerce")
        df["rsl_price_source"] = "adj_close"
    else:
        df["rsl_price"] = pd.to_numeric(df[close_col], errors="coerce")
        df["rsl_price_source"] = "close"
        reasons.add("adjclose_missing_close_used", "review")

    used_close_fallback = False
    global_fallback_reason = ""

    if close_col is not None and adj_col is not None:
        c_vals = pd.to_numeric(df[close_col], errors="coerce")
        a_vals = pd.to_numeric(df[adj_col], errors="coerce")

        ratio_series = (c_vals / a_vals.replace(0, np.nan)).dropna()
        if len(ratio_series) > 30:
            global_median_ratio = float(ratio_series.median())
            extreme_ratio_mask = (
                (ratio_series > cfg["adj_close_gap_hard_threshold_ratio"])
                | (ratio_series < 1.0 / cfg["adj_close_gap_hard_threshold_ratio"])
            )
            extreme_fraction = float(extreme_ratio_mask.mean())

            diagnostics["global_close_adj_ratio_median"] = global_median_ratio
            diagnostics["global_close_adj_ratio_extreme_fraction"] = extreme_fraction

            if extreme_fraction >= cfg["global_scale_fallback_min_fraction"]:
                df["rsl_price"] = c_vals
                df["rsl_price_source"] = "close_global_fallback"
                used_close_fallback = True
                global_fallback_reason = "global_adjclose_scale_issue"
                reasons.add("global_adjclose_scale_issue", "hard_fail")

    div_reasons, div_diag, event_df = detect_dividend_adjustment_issues(df, cfg)
    reasons.extend(div_reasons)
    diagnostics.update(div_diag)

    if not used_close_fallback and close_col is not None and adj_col is not None and not event_df.empty:
        problem_events = event_df[event_df["event_reasons"].astype(str) != ""].copy()

        if len(problem_events) > 0:
            before_n = int(cfg["dividend_window_before"])
            after_n = int(cfg["dividend_window_after"])

            use_close_mask = pd.Series(False, index=df.index)
            for _, row in problem_events.iterrows():
                dt = row["event_date"]
                if dt not in df.index:
                    continue
                loc = df.index.get_loc(dt)
                if isinstance(loc, slice) or isinstance(loc, np.ndarray):
                    continue
                start = max(0, loc - before_n)
                end = min(len(df), loc + after_n + 1)
                use_close_mask.iloc[start:end] = True

            close_numeric = pd.to_numeric(df[close_col], errors="coerce")
            fallback_days = int(use_close_mask.sum())
            diagnostics["local_close_fallback_days"] = fallback_days

            if fallback_days > 0:
                df.loc[use_close_mask, "rsl_price"] = close_numeric.loc[use_close_mask]
                df.loc[use_close_mask, "rsl_price_source"] = "close_local_fallback"
                used_close_fallback = True

    valid_rsl_price = pd.to_numeric(df["rsl_price"], errors="coerce")
    if len(df) > 0:
        fallback_fraction = float((df["rsl_price_source"] != "adj_close").mean())
    else:
        fallback_fraction = 0.0

    diagnostics["fallback_fraction"] = fallback_fraction
    diagnostics["global_fallback_reason"] = global_fallback_reason
    diagnostics["used_close_fallback"] = used_close_fallback

    if fallback_fraction >= cfg["fallback_fraction_hard"]:
        reasons.add("fallback_fraction_too_high", "hard_fail")
    elif fallback_fraction >= cfg["fallback_fraction_warn"]:
        reasons.add("fallback_fraction_elevated", "warning")

    min_rows = int(cfg["min_history_rows_for_rsl"])
    if len(valid_rsl_price.dropna()) < min_rows:
        reasons.add("insufficient_history_for_rsl", "hard_fail")

    rsl_series = _compute_rsl_from_series(valid_rsl_price, int(cfg["rsl_sma_window"]))
    df["rsl_value"] = rsl_series

    if len(rsl_series.dropna()) == 0 and len(valid_rsl_price.dropna()) >= min_rows:
        reasons.add("rsl_computation_failed", "hard_fail")

    return PriceSeriesBuildResult(
        history=df,
        rsl_price_column="rsl_price",
        used_close_fallback=used_close_fallback,
        reasons=reasons,
        diagnostics=diagnostics,
    )


# ============================================================================
# High-level API für einzelne Stocks / Ergebnislisten
# ============================================================================

def evaluate_stock_rsl_integrity(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> Dict[str, Any]:
    cfg = _merge_config(config)

    ticker = _normalize_string(
        _get_row_value(
            item,
            ["yahoo_symbol", "ticker", "Ticker", "Symbol", "symbol", "original_ticker"],
            None,
        )
    ) or None

    country = _normalize_string(
        _get_row_value(
            item,
            ["land", "Land", "country", "Country"],
            None,
        )
    ) or None

    history = _extract_history_object(item)

    result: Dict[str, Any] = {
        "ticker": ticker,
        "country": country,
        "raw_rsl": raw_rsl,
        "ranking_integrity_status": "missing_history",
        "excluded_from_ranking": True,
        "ranking_exclude_reason": "missing_history",
        "drop_reasons": ["missing_history"],
        "integrity_warnings": [],
        "hard_fail_reasons": ["missing_history"],
        "warning_reasons": [],
        "review_reasons": [],
        "used_close_fallback": False,
        "rsl_price_source": "missing",
        "fallback_fraction": None,
        "repair_applied": False,
        "repair_method": "",
        "repair_reason": "",
        "history": history,
        "diagnostics": {},
    }

    if history is None or not isinstance(history, pd.DataFrame) or len(history) == 0:
        if _looks_like_foreign_secondary_listing(ticker, country, cfg):
            result["review_reasons"] = ["foreign_secondary_listing_possible"]
            result["integrity_warnings"] = ["foreign_secondary_listing_possible"]
        return result

    built = build_rsl_price_series(history, cfg)
    reasons = built.reasons
    diagnostics = dict(built.diagnostics)

    review_reasons = list(reasons.review_reasons)
    if _looks_like_foreign_secondary_listing(ticker, country, cfg):
        if "foreign_secondary_listing_possible" not in review_reasons:
            review_reasons.append("foreign_secondary_listing_possible")

    hard_fail_reasons = list(reasons.hard_fail_reasons)
    warning_reasons = list(reasons.warning_reasons)

    excluded = len(hard_fail_reasons) > 0
    if excluded:
        status = "excluded_hard_fail"
        exclude_reason = hard_fail_reasons[0]
    elif built.used_close_fallback:
        status = "eligible_repaired"
        exclude_reason = ""
    else:
        status = "eligible_original"
        exclude_reason = ""

    rsl_source_series = built.history.get("rsl_price_source")
    rsl_price_source = ""
    if rsl_source_series is not None and len(rsl_source_series) > 0:
        last_non_na = rsl_source_series.dropna()
        if len(last_non_na) > 0:
            rsl_price_source = str(last_non_na.iloc[-1])

    result.update(
        {
            "ranking_integrity_status": status,
            "excluded_from_ranking": excluded,
            "ranking_exclude_reason": exclude_reason,
            "drop_reasons": _unique_keep_order(
                hard_fail_reasons + warning_reasons + review_reasons
            ),
            "integrity_warnings": _unique_keep_order(warning_reasons + review_reasons),
            "hard_fail_reasons": hard_fail_reasons,
            "warning_reasons": warning_reasons,
            "review_reasons": review_reasons,
            "used_close_fallback": bool(built.used_close_fallback),
            "rsl_price_source": rsl_price_source,
            "fallback_fraction": diagnostics.get("fallback_fraction"),
            "repair_applied": bool(built.used_close_fallback),
            "repair_method": "close_fallback" if built.used_close_fallback else "",
            "repair_reason": diagnostics.get("global_fallback_reason", "") or (
                "dividend_window_local_fallback" if built.used_close_fallback else ""
            ),
            "history": built.history,
            "diagnostics": diagnostics,
        }
    )

    return result


def _apply_integrity_fields_to_item(item: Any, info: Dict[str, Any]) -> Any:
    field_map = {
        "ranking_integrity_status": info.get("ranking_integrity_status", ""),
        "excluded_from_ranking": info.get("excluded_from_ranking", False),
        "ranking_exclude_reason": info.get("ranking_exclude_reason", ""),
        "drop_reasons": info.get("drop_reasons", []),
        "integrity_warnings": info.get("integrity_warnings", []),
        "hard_fail_reasons": info.get("hard_fail_reasons", []),
        "warning_reasons": info.get("warning_reasons", []),
        "review_reasons": info.get("review_reasons", []),
        "used_close_fallback": info.get("used_close_fallback", False),
        "rsl_price_source": info.get("rsl_price_source", ""),
        "fallback_fraction": info.get("fallback_fraction", None),
        "repair_applied": info.get("repair_applied", False),
        "repair_method": info.get("repair_method", ""),
        "repair_reason": info.get("repair_reason", ""),
        "diagnostics": info.get("diagnostics", {}),
        "history": info.get("history", None),
    }

    if isinstance(item, dict):
        out = dict(item)
        out.update(field_map)
        return out

    for key, value in field_map.items():
        try:
            setattr(item, key, value)
        except Exception:
            pass
    return item


def filter_stock_results_for_rsl_integrity(
    results: List[Any],
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Any], pd.DataFrame]:
    valid_results: List[Any] = []
    audit_rows: List[Dict[str, Any]] = []

    for stock in results or []:
        raw_rsl = _get_row_value(stock, ["rsl", "RSL", "rsl_value"], None)
        info = evaluate_stock_rsl_integrity(
            stock,
            location_suffix_map=location_suffix_map,
            config=config,
            raw_rsl=raw_rsl,
        )

        updated_stock = _apply_integrity_fields_to_item(stock, info)
        audit_rows.append(_audit_row_from_item(updated_stock))

        if not info.get("excluded_from_ranking", False):
            valid_results.append(updated_stock)

    audit_df = pd.DataFrame(audit_rows)
    return valid_results, audit_df


# ============================================================================
# Legacy-/Reason-API
# ============================================================================

def _extract_item_meta(item: Any) -> Tuple[Optional[str], Optional[str], Optional[pd.DataFrame]]:
    ticker = _get_row_value(
        item,
        ["yahoo_symbol", "ticker", "Ticker", "Symbol", "symbol", "original_ticker"],
        None,
    )
    country = _get_row_value(
        item,
        ["land", "Land", "country", "Country"],
        None,
    )
    history = _extract_history_object(item)
    return (
        _normalize_string(ticker) or None,
        _normalize_string(country) or None,
        history,
    )


def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    info = evaluate_stock_rsl_integrity(
        item,
        location_suffix_map=location_suffix_map,
        config=config,
        raw_rsl=raw_rsl,
    )
    return _unique_keep_order(info.get("drop_reasons", []))


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    return get_rsl_integrity_reasons(
        item,
        location_suffix_map=location_suffix_map,
        config=config,
        raw_rsl=raw_rsl,
    )


# ============================================================================
# Audit / Review DataFrames
# ============================================================================

def _audit_row_from_item(stock: Any) -> Dict[str, Any]:
    return {
        "ticker": _get_row_value(stock, ["yahoo_symbol", "ticker", "Ticker", "Symbol", "symbol"], ""),
        "name": _get_row_value(stock, ["name", "Name", "company_name"], ""),
        "country": _get_row_value(stock, ["land", "Land", "country", "Country"], ""),
        "rsl": _get_row_value(stock, ["rsl", "RSL", "rsl_value"], None),
        "rsl_rank": _get_row_value(stock, ["rsl_rank", "rank", "Rank"], None),
        "ranking_integrity_status": _get_row_value(stock, ["ranking_integrity_status"], ""),
        "excluded_from_ranking": _get_row_value(stock, ["excluded_from_ranking"], False),
        "ranking_exclude_reason": _get_row_value(stock, ["ranking_exclude_reason"], ""),
        "used_close_fallback": _get_row_value(stock, ["used_close_fallback"], False),
        "rsl_price_source": _get_row_value(stock, ["rsl_price_source"], ""),
        "fallback_fraction": _get_row_value(stock, ["fallback_fraction"], None),
        "repair_applied": _get_row_value(stock, ["repair_applied"], False),
        "repair_method": _get_row_value(stock, ["repair_method"], ""),
        "repair_reason": _get_row_value(stock, ["repair_reason"], ""),
        "drop_reasons": ", ".join(_safe_list(_get_row_value(stock, ["drop_reasons"], []))),
        "integrity_warnings": ", ".join(_safe_list(_get_row_value(stock, ["integrity_warnings"], []))),
        "hard_fail_reasons": ", ".join(_safe_list(_get_row_value(stock, ["hard_fail_reasons"], []))),
        "warning_reasons": ", ".join(_safe_list(_get_row_value(stock, ["warning_reasons"], []))),
        "review_reasons": ", ".join(_safe_list(_get_row_value(stock, ["review_reasons"], []))),
    }


def build_rsl_integrity_audit_df(results: List[Any]) -> pd.DataFrame:
    rows = [_audit_row_from_item(stock) for stock in (results or [])]
    return pd.DataFrame(rows)


def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame,
    top_rank: int = 300,
) -> pd.DataFrame:
    if audit_df is None or len(audit_df) == 0:
        return pd.DataFrame()

    df = audit_df.copy()

    if "rsl_rank" in df.columns:
        df["rsl_rank"] = pd.to_numeric(df["rsl_rank"], errors="coerce")
        df = df[(df["rsl_rank"].isna()) | (df["rsl_rank"] <= top_rank)]

    excluded = (
        df["excluded_from_ranking"].astype(bool)
        if "excluded_from_ranking" in df.columns
        else pd.Series(False, index=df.index)
    )
    close_fallback = (
        df["used_close_fallback"].astype(bool)
        if "used_close_fallback" in df.columns
        else pd.Series(False, index=df.index)
    )
    repaired = (
        df["repair_applied"].astype(bool)
        if "repair_applied" in df.columns
        else pd.Series(False, index=df.index)
    )
    status_not_original = (
        df["ranking_integrity_status"].astype(str).ne("eligible_original")
        if "ranking_integrity_status" in df.columns
        else pd.Series(False, index=df.index)
    )

    mask = excluded | close_fallback | repaired | status_not_original
    shortlist = df.loc[mask].copy()

    sort_cols = [
        c
        for c in ["excluded_from_ranking", "repair_applied", "used_close_fallback", "rsl_rank"]
        if c in shortlist.columns
    ]
    if sort_cols:
        shortlist = shortlist.sort_values(
            sort_cols,
            ascending=[False] * len(sort_cols),
            na_position="last",
        )

    return shortlist.reset_index(drop=True)
