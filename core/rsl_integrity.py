from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


"""
RSL-Integritätslogik mit Fokus auf URSACHEN statt Symptomen.

PHILOSOPHIE
-----------
Ein hoher RSL-Wert ist kein Ausschlussgrund an sich.
Wenn ein RSL-Wert unplausibel hoch erscheint, muss die Datenursache
identifiziert werden. Nur die Ursache darf zu Review / Warning /
Hard-Fail führen.

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
Spaltennamen. Für historische Kursdaten werden u. a. erkannt:

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
Sie markiert stattdessen die Ursachen. Hard-Fails entstehen nur bei
echten Datenproblemen.
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
    "adj_close_gap_hard_threshold_ratio": 2.0, # Faktor 2.0 (oder 0.5) für extreme Abweichung
    "global_scale_fallback_min_fraction": 0.20, # Mindestanteil Tage für globalen Fallback

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

    def all_reasons(self) -> List[str]:
        return self.hard_fail_reasons + self.warning_reasons + self.review_reasons

    def legacy_drop_reasons(self) -> List[str]:
        """
        Legacy-Verhalten:
        Historisch wurden verschiedene Reasons pauschal als drop_reasons geführt.
        Für Rückwärtskompatibilität geben wir hier ALLE Reasons zurück.
        Die eigentliche Hard-Fail-Entscheidung basiert aber nur auf hard_fail_reasons.
        """
        return self.all_reasons()

    def has_hard_fail(self) -> bool:
        return len(self.hard_fail_reasons) > 0

    def has_any(self) -> bool:
        return bool(self.hard_fail_reasons or self.warning_reasons or self.review_reasons)


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
    if pd.isna(value) if not isinstance(value, (list, tuple, set, dict)) else False:
        return []
    return [value]


def _unique_keep_order(values: Iterable[Any]) -> List[Any]:
    seen = set()
    out = []
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
        out = out.sort_index()
        return out

    date_col = _find_first_existing_column(out, ["Date", "date", "Datetime", "datetime", "timestamp"])
    if date_col is not None:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).sort_values(date_col).set_index(date_col)
        return out

    # Fallback: versuchen, den bestehenden Index zu konvertieren
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


def _looks_like_foreign_secondary_listing(ticker: Optional[str], country: Optional[str], cfg: Dict[str, Any]) -> bool:
    if not ticker:
        return False

    ticker = str(ticker).upper().strip()
    suffixes = cfg.get("foreign_secondary_suffixes", [])
    has_secondary_suffix = any(ticker.endswith(sfx.upper()) for sfx in suffixes)

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

    # pandas Series / namedtuple / object
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

    # Fehlende Actions-Spalten anlegen
    if cols["dividends"] is None:
        df["Dividends"] = 0.0
    if cols["splits"] is None:
        df["Stock Splits"] = 0.0

    # Wenn eben neu angelegt wurde, Spalten neu ermitteln
    cols = _get_price_columns(df)

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
    splits = pd.to_numeric(df[split_col], errors="coerce").fillna(0.0)

    dividend_dates = df.index[dividends > 0].tolist()
    diagnostics["dividend_event_count"] = int(len(dividend_dates))

    if not dividend_dates:
        return reasons, diagnostics, pd.DataFrame()

    close = pd.to_numeric(df[close_col], errors="coerce")
    adj = pd.to_numeric(df[adj_col], errors="coerce")

    event_rows = []

    for dt in dividend_dates:
        if dt not in df.index:
            continue

        loc = df.index.get_loc(dt)
        if isinstance(loc, slice):
            # sehr unwahrscheinlich bei eindeutigen Datumsindizes
            continue
        if isinstance(loc, np.ndarray):
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
            # Splits machen die Interpretation deutlich schwieriger.
            # Nicht sofort failen, aber als Review markieren.
            reasons.add("split_present_near_dividend_window", "review")

        # Ex-Tag-Dividende
        ex_div = float(df.loc[dt, div_col]) if pd.notna(df.loc[dt, div_col]) else 0.0

        # Preis am Vortag / nächster valider Preis davor
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

        # Falls kein gültiger Vortagespreis
        if not np.isfinite(prior_close) or prior_close <= 0:
            continue

        # Yahoo-artige theoretische Multiplikator-Idee:
        # multiplier ~= 1 - dividend / prev_close
        explained_gap = abs(ex_div / prior_close) if prior_close > 0 else np.nan

        # Tatsächliche mediane relative Differenz im Fenster
        gap_series = _safe_pct_diff(w_adj, w_close)
        median_gap = float(gap_series.median(skipna=True)) if len(gap_series.dropna()) else np.nan
        max_gap = float(gap_series.max(skipna=True)) if len(gap_series.dropna()) else np.nan

        # Renditebrüche vergleichen
        close_ret = w_close.pct_change().replace([np.inf, -np.inf], np.nan)
        adj_ret = w_adj.pct_change().replace([np.inf, -np.inf], np.nan)

        max_abs_close_ret = float(close_ret.abs().max(skipna=True)) if len(close_ret.dropna()) else np.nan
        max_abs_adj_ret = float(adj_ret.abs().max(skipna=True)) if len(adj_ret.dropna()) else np.nan

        # Hat Adj Close offensichtlich "schlimmere" Brüche?
        adj_worse_than_close = False
        if np.isfinite(max_abs_adj_ret) and np.isfinite(max_abs_close_ret):
            adj_worse_than_close = max_abs_adj_ret > (max_abs_close_ret + 0.15)

        # Zusätzlicher Sonderfall: negative Adj Close Werte
        negative_adj_present = bool((w_adj.dropna() < 0).any())

        # Plausibilitätsbewertung
        event_problem_severity = None
        event_reasons = []

        if negative_adj_present:
            event_problem_severity = "hard_fail"
            event_reasons.append("negative_adjclose_in_dividend_window")

        # Wenn die beobachtete Gap viel größer ist als die Dividende erklärt
        if np.isfinite(median_gap) and np.isfinite(explained_gap):
            # Beispiel:
            # Dividende erklärt 1 %, beobachtet 15 % => verdächtig
            if median_gap > explained_gap + cfg["dividend_multiplier_tolerance"]:
                event_reasons.append("adjclose_close_gap_unplausible_for_dividend")

        # Sehr große absolute Adj/Close-Abweichungen
        if np.isfinite(max_gap):
            if max_gap >= cfg["adj_close_gap_hard_threshold"]:
                event_reasons.append("extreme_adjclose_close_gap_in_dividend_window")
                event_problem_severity = event_problem_severity or "hard_fail"
            elif max_gap >= cfg["adj_close_gap_warn_threshold"]:
                event_reasons.append("large_adjclose_close_gap_in_dividend_window")
                event_problem_severity = event_problem_severity or "warning"

        # Adj-Serie wirkt kaputter als Close-Serie
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

        event_rows.append({
            "event_date": dt,
            "dividend": ex_div,
            "prior_close": prior_close,
            "explained_gap_estimate": explained_gap,
            "median_adj_close_gap": median_gap,
            "max_adj_close_gap": max_gap,
            "max_abs_close_return": max_abs_close_ret,
            "max_abs_adj_return": max_abs_adj_ret,
            "adj_worse_than_close": adj_worse_than_close,
            "negative_adj_present": negative_adj_present,
            "event_reasons": "|".join(event_reasons) if event_reasons else "",
            "event_severity": event_problem_severity or "",
        })

    event_df = pd.DataFrame(event_rows)
    diagnostics["problem_dividend_events"] = int((event_df["event_reasons"] != "").sum()) if not event_df.empty else 0

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
    for r in basic_reasons.hard_fail_reasons:
        reasons.add(r, "hard_fail")
    for r in basic_reasons.warning_reasons:
        reasons.add(r, "warning")
    for r in basic_reasons.review_reasons:
        reasons.add(r, "review")
    diagnostics.update(basic_diag)

    cols = _get_price_columns(df)
    close_col = cols["close"]
    adj_col = cols["adj_close"]

    if close_col is None and adj_col is None:
        reasons.add("missing_close_and_adjclose", "hard_fail")
        df["rsl_price"] = np.nan
        df["rsl_price_source"] = "missing"
        return PriceSeriesBuildResult(df, "rsl_price", False, reasons, diagnostics)

    # Standardquelle
    if adj_col is not None:
        df["rsl_price"] = pd.to_numeric(df[adj_col], errors="coerce")
        df["rsl_price_source"] = "adj_close"
    else:
        df["rsl_price"] = pd.to_numeric(df[close_col], errors="coerce")
        df["rsl_price_source"] = "close"
        reasons.add("adjclose_missing_close_used", "review")

    used_close_fallback = False
    global_fallback_reason = ""
    
    # --- GLOBALER SCALE-CHECK (Yahoo Floor Bug Detektion) ---
    # ARCHITEKTUR-REGEL: Wir vergleichen URSACHEN (Skalierung), nicht RSL-Symptome.
    if close_col is not None and adj_col is not None:
        c_vals = pd.to_numeric(df[close_col], errors="coerce")
        a_vals = pd.to_numeric(df[adj_col], errors="coerce")
        
        # Ratio-Analyse über die gesamte verfügbare Historie
        ratio_series = (c_vals / a_vals.replace(0, np.nan)).dropna()
        if len(ratio_series) > 30:
            global_median_ratio = float(ratio_series.median())
            extreme_ratio_mask = (ratio_series > cfg["adj_close_gap_hard_threshold_ratio"]) | (ratio_series < 1.0 / cfg["adj_close_gap_hard_threshold_ratio"])
            extreme_fraction = float(extreme_ratio_mask.mean())
            
            # Wenn Abweichung persistent (>20% der Tage) und extrem (Median), umschalten.
            if extreme_fraction >= cfg["global_scale_fallback_min_fraction"] and (global_median_ratio > 1.5 or global_median_ratio < 0.6):
                df["rsl_price"] = c_vals
                df["rsl_price_source"] = "close_global_scale_fallback"
                used_close_fallback = True
                global_fallback_reason = "extreme_adjclose_close_gap_global"
                reasons.add(global_fallback_reason, "warning")

    # Wenn beide vorhanden sind, Dividenden-/Adjustierungsprüfung
    if not used_close_fallback and close_col is not None and adj_col is not None:
        div_reasons, div_diag, event_df = detect_dividend_adjustment_issues(df, cfg)

        for r in div_reasons.hard_fail_reasons:
            reasons.add(r, "hard_fail")
        for r in div_reasons.warning_reasons:
            reasons.add(r, "warning")
        for r in div_reasons.review_reasons:
            reasons.add(r, "review")

        diagnostics["dividend_adjustment"] = div_diag
        diagnostics["dividend_events_table"] = event_df

        # Lokaler Fallback rund um problematische Dividendenfenster
        if not event_df.empty:
            problematic_events = event_df[event_df["event_reasons"].fillna("") != ""].copy()

            if len(problematic_events) > 0:
                close_series = pd.to_numeric(df[close_col], errors="coerce")

                for _, evt in problematic_events.iterrows():
                    dt = evt["event_date"]
                    if dt not in df.index:
                        continue

                    loc = df.index.get_loc(dt)
                    if isinstance(loc, slice) or isinstance(loc, np.ndarray):
                        continue

                    # Lokale Ratio-Prüfung im Fenster
                    w_start = max(0, loc - 5)
                    w_end = min(len(df), loc + 5)
                    w_close = pd.to_numeric(df[close_col].iloc[w_start:w_end], errors="coerce")
                    w_adj = pd.to_numeric(df[adj_col].iloc[w_start:w_end], errors="coerce")
                    
                    ratio_series_win = (w_close / w_adj.replace(0, np.nan)).dropna()
                    median_ratio_win = float(ratio_series_win.median()) if not ratio_series_win.empty else 1.0
                    
                    # Falls lokale Ratio massiv abweicht, korrigieren wir den gesamten Floor davor (Yahoo Floor Bug)
                    if median_ratio_win > 2.0 or median_ratio_win < 0.5:
                        start = 0
                        repair_mode = "global_pre_ex_fallback"
                    else:
                        start = max(0, loc - int(cfg["dividend_window_before"]))
                        repair_mode = "local_window_fallback"

                    end = min(len(df), loc + int(cfg["dividend_window_after"]) + 1)
                    fallback_idx = df.index[start:end]

                    # Nur dort ersetzen, wo Close plausibel > 0 ist
                    close_slice = close_series.loc[fallback_idx]
                    valid_mask = close_slice.notna() & (close_slice > 0)

                    if valid_mask.any():
                        idx_to_replace = close_slice.index[valid_mask]
                        df.loc[idx_to_replace, "rsl_price"] = close_slice.loc[idx_to_replace]
                        df.loc[idx_to_replace, "rsl_price_source"] = f"close_{repair_mode}"
                        used_close_fallback = True

                if used_close_fallback:
                    reasons.add("close_fallback_used", "warning")

    # Fallback falls rsl_price trotz allem leer ist
    if close_col is not None:
        close_series = pd.to_numeric(df[close_col], errors="coerce")
        missing_rsl_mask = df["rsl_price"].isna() & close_series.notna() & (close_series > 0)
        if missing_rsl_mask.any():
            df.loc[missing_rsl_mask, "rsl_price"] = close_series.loc[missing_rsl_mask]
            df.loc[missing_rsl_mask, "rsl_price_source"] = "close_fill_missing"
            reasons.add("close_used_to_fill_missing_adjclose", "review")

    # Validierung der finalen rsl_price-Serie
    rsl_price = pd.to_numeric(df["rsl_price"], errors="coerce")
    valid_rsl_price = rsl_price.dropna()

    if len(valid_rsl_price) == 0:
        reasons.add("no_valid_rsl_price_series", "hard_fail")
    else:
        if (valid_rsl_price <= 0).any():
            reasons.add("non_positive_values_in_rsl_price", "hard_fail")

        fallback_frac = float((df["rsl_price_source"].astype(str).str.contains("close", case=False, na=False)).mean())
        diagnostics["fallback_fraction"] = fallback_frac

        if fallback_frac >= cfg["fallback_fraction_hard"]:
            reasons.add("too_much_close_fallback_in_rsl_series", "hard_fail")
        elif fallback_frac >= cfg["fallback_fraction_warn"]:
            reasons.add("substantial_close_fallback_in_rsl_series", "warning")

    # Ermittle den dominanten Modus der Preisquelle für die Dokumentation
    source_counts = df["rsl_price_source"].value_counts()
    primary_source = str(source_counts.idxmax()) if not source_counts.empty else "adj_close"
    
    # Reparatur-Metadaten für StockData-Objekt aufbereiten
    repair_applied = used_close_fallback
    repair_method = primary_source
    repair_reason = global_fallback_reason or ("; ".join(reasons.warning_reasons) if used_close_fallback else "")

    return PriceSeriesBuildResult(
        history=df,
        rsl_price_column="rsl_price",
        used_close_fallback=used_close_fallback,
        reasons=reasons,
        diagnostics={
            **diagnostics,
            "fallback_fraction": fallback_frac,
            "rsl_price_source_mode": primary_source,
            "repair_applied": repair_applied,
            "repair_method": repair_method,
            "repair_reason": repair_reason
        },
    )


# ============================================================================
# RSL-Berechnung + Integritätsauswertung für eine Historie
# ============================================================================

def analyze_history_for_rsl_integrity(
    history_df: pd.DataFrame,
    ticker: Optional[str] = None,
    country: Optional[str] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Hauptanalyse für EINE Kursreihe.
    Gibt ein strukturiertes Dict zurück.
    """
    cfg = _merge_config(cfg)
    build = build_rsl_price_series(history_df, cfg)

    reasons = build.reasons
    df = build.history.copy()
    diagnostics = dict(build.diagnostics)

    if _looks_like_foreign_secondary_listing(ticker, country, cfg):
        reasons.add("foreign_secondary_listing_possible", "review")

    rsl_value = np.nan
    sma_value = np.nan

    if build.rsl_price_column in df.columns:
        rsl_series = _compute_rsl_from_series(
            pd.to_numeric(df[build.rsl_price_column], errors="coerce"),
            sma_window=int(cfg["rsl_sma_window"]),
        )
        df["rsl"] = rsl_series

        valid_rsl = rsl_series.dropna()
        if len(valid_rsl) > 0:
            rsl_value = float(valid_rsl.iloc[-1])

        sma = _rolling_sma(pd.to_numeric(df[build.rsl_price_column], errors="coerce"), int(cfg["rsl_sma_window"]))
        valid_sma = sma.dropna()
        if len(valid_sma) > 0:
            sma_value = float(valid_sma.iloc[-1])

    if len(df) < int(cfg["min_history_rows_for_rsl"]):
        reasons.add("insufficient_history_for_rsl_window", "hard_fail")

    result = {
        "history": df,
        "rsl_price_column": build.rsl_price_column,
        "used_close_fallback": build.used_close_fallback,
        "rsl_value": rsl_value,
        "rsl_sma": sma_value,
        "integrity_reasons": reasons.all_reasons(),
        "hard_fail_reasons": reasons.hard_fail_reasons,
        "warning_reasons": reasons.warning_reasons,
        "review_reasons": reasons.review_reasons,
        "drop_reasons": reasons.legacy_drop_reasons(),  # Legacy
        "integrity_warnings": reasons.warning_reasons,  # Legacy
        "has_hard_fail": reasons.has_hard_fail(),
        "diagnostics": diagnostics,
    }
    return result


# ============================================================================
# DataFrame-/Universe-Helfer
# ============================================================================

def _extract_history_object(row: Any) -> Optional[pd.DataFrame]:
    """
    Versucht, aus einer Zeile/Objekt die Historie zu extrahieren.
    Unterstützte Kandidaten:
    - history
    - price_history
    - hist
    - yf_history
    """
    candidates = ["history", "price_history", "hist", "yf_history"]
    for c in candidates:
        val = _get_row_value(row, [c], default=None)
        if isinstance(val, pd.DataFrame):
            return val
    return None


def _append_reasons_to_row_dict(row_dict: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
    row_dict = dict(row_dict)
    row_dict["integrity_reasons"] = analysis.get("integrity_reasons", [])
    row_dict["hard_fail_reasons"] = analysis.get("hard_fail_reasons", [])
    row_dict["warning_reasons"] = analysis.get("warning_reasons", [])
    row_dict["review_reasons"] = analysis.get("review_reasons", [])
    row_dict["drop_reasons"] = analysis.get("drop_reasons", [])  # Legacy
    row_dict["integrity_warnings"] = analysis.get("integrity_warnings", [])  # Legacy
    row_dict["has_hard_fail"] = bool(analysis.get("has_hard_fail", False))
    row_dict["used_close_fallback"] = bool(analysis.get("used_close_fallback", False))
    row_dict["rsl_price_source"] = analysis.get("diagnostics", {}).get("rsl_price_source_mode", "adj_close")
    row_dict["fallback_fraction"] = analysis.get("diagnostics", {}).get("fallback_fraction")

    if pd.isna(row_dict.get("RSL")) or row_dict.get("RSL") is None:
        row_dict["RSL"] = analysis.get("rsl_value", np.nan)

    return row_dict


def apply_rsl_integrity_to_universe(
    universe_df: pd.DataFrame,
    history_map: Optional[Dict[str, pd.DataFrame]] = None,
    ticker_col_candidates: Optional[List[str]] = None,
    country_col_candidates: Optional[List[str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analysiert je Zeile die Historie und ergänzt strukturierte Integritätsfelder.
    """
    if universe_df is None or len(universe_df) == 0:
        return pd.DataFrame() if universe_df is None else universe_df.copy()

    ticker_col_candidates = ticker_col_candidates or [
        "ticker", "Ticker", "Symbol", "symbol", "Yahoo Symbol", "yahoo_symbol", "original_ticker"
    ]
    country_col_candidates = country_col_candidates or [
        "country", "Country", "land", "Land"
    ]

    out_rows: List[Dict[str, Any]] = []

    for _, row in universe_df.iterrows():
        row_dict = row.to_dict()
        ticker = None
        country = None

        for c in ticker_col_candidates:
            if c in row_dict and str(row_dict.get(c) or "").strip():
                ticker = str(row_dict[c]).strip()
                break

        for c in country_col_candidates:
            if c in row_dict and str(row_dict.get(c) or "").strip():
                country = str(row_dict[c]).strip()
                break

        history = _extract_history_object(row_dict)
        if history is None and history_map and ticker:
            history = history_map.get(ticker)

        if isinstance(history, pd.DataFrame) and not history.empty:
            analysis = analyze_history_for_rsl_integrity(
                history_df=history,
                ticker=ticker,
                country=country,
                cfg=cfg,
            )
            row_dict = _append_reasons_to_row_dict(row_dict, analysis)
        else:
            row_dict.setdefault("integrity_reasons", [])
            row_dict.setdefault("hard_fail_reasons", [])
            row_dict.setdefault("warning_reasons", [])
            row_dict.setdefault("review_reasons", [])
            row_dict.setdefault("drop_reasons", [])
            row_dict.setdefault("integrity_warnings", [])
            row_dict.setdefault("has_hard_fail", False)
            row_dict.setdefault("used_close_fallback", False)
            row_dict.setdefault("rsl_price_source", "adj_close")
            row_dict.setdefault("fallback_fraction", None)

        out_rows.append(row_dict)

    return pd.DataFrame(out_rows)


def _extract_item_meta(item: Any) -> Tuple[Optional[str], Optional[str], Optional[pd.DataFrame]]:
    ticker = _get_row_value(item, ["yahoo_symbol", "ticker", "Ticker", "Symbol", "symbol", "original_ticker"], None)
    country = _get_row_value(item, ["land", "Land", "country", "Country"], None)
    history = _extract_history_object(item)
    return (
        _normalize_string(ticker) or None,
        _normalize_string(country) or None,
        history,
    ]


def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-/Kompatibilitätsfunktion.
    Liefert ALLE Integritäts-Reasons für ein Item.
    """
    ticker, country, history = _extract_item_meta(item)

    if history is None or not isinstance(history, pd.DataFrame) or history.empty:
        return []

    analysis = analyze_history_for_rsl_integrity(
        history_df=history,
        ticker=ticker,
        country=country,
        cfg=config,
    )
    return list(analysis.get("integrity_reasons", []))


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-/Kompatibilitätsfunktion.
    Wichtig: Nur echte Hard-Fails zurückgeben.
    Kein RSL-Symptom darf alleiniger Ausschlussgrund sein.
    """
    ticker, country, history = _extract_item_meta(item)

    if history is None or not isinstance(history, pd.DataFrame) or history.empty:
        return []

    analysis = analyze_history_for_rsl_integrity(
        history_df=history,
        ticker=ticker,
        country=country,
        cfg=config,
    )
    hard_fails = list(analysis.get("hard_fail_reasons", []))
    return [r for r in hard_fails if r not in {"no_valid_rsl_data", "invalid_rsl_value", "missing_rsl"}]


def _clean_hard_fail_reasons(reasons: List[str]) -> List[str]:
    """
    Entfernt reine RSL-Symptom-Reasons.
    Datenursachen bleiben erhalten.
    """
    blocked = {
        "no_valid_rsl_data",
        "invalid_rsl_value",
        "missing_rsl",
        "no_valid_rsl",
        "rsl_invalid",
    }
    return [r for r in reasons if r not in blocked]


def _set_if_attr(obj: Any, attr: str, value: Any) -> None:
    try:
        setattr(obj, attr, value)
    except Exception:
        pass


def filter_stock_results_for_rsl_integrity(
    results: List[Any],
    location_suffix_map: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    """
    Legacy-/Kompatibilitätsfunktion für final.py.

    Regeln:
    - Ausschluss nur bei echten Hard-Fail-Datenursachen.
    - Hoher / fehlender / auffälliger RSL ist NIE alleiniger Ausschlussgrund.
    - Falls Close-Fallback / Reparatur verwendet wurde und kein Hard-Fail vorliegt:
      -> eligible_repaired
    """
    if not results:
        return results

    for stock in results:
        ticker, country, history = _extract_item_meta(stock)

        if history is None or not isinstance(history, pd.DataFrame) or history.empty:
            _set_if_attr(stock, "rsl_eligible", True)
            _set_if_attr(stock, "excluded_from_ranking", False)
            _set_if_attr(stock, "ranking_exclude_reason", "")
            _set_if_attr(stock, "ranking_integrity_status", "eligible_original")
            _set_if_attr(stock, "used_close_fallback", False)
            _set_if_attr(stock, "rsl_price_source", "adj_close")
            _set_if_attr(stock, "fallback_fraction", None)
            continue

        analysis = analyze_history_for_rsl_integrity(
            history_df=history,
            ticker=ticker,
            country=country,
            cfg=config,
        )

        diagnostics = analysis.get("diagnostics", {}) or {}
        hard_fail_reasons = _clean_hard_fail_reasons(list(analysis.get("hard_fail_reasons", [])))
        warning_reasons = list(analysis.get("warning_reasons", []))
        review_reasons = list(analysis.get("review_reasons", []))

        used_close_fallback = bool(analysis.get("used_close_fallback", False))
        rsl_price_source = diagnostics.get("rsl_price_source_mode", "adj_close")
        fallback_fraction = diagnostics.get("fallback_fraction")
        has_hard_fail = len(hard_fail_reasons) > 0

        _set_if_attr(stock, "integrity_warnings", warning_reasons + review_reasons)
        _set_if_attr(stock, "used_close_fallback", used_close_fallback)
        _set_if_attr(stock, "rsl_price_source", rsl_price_source)
        _set_if_attr(stock, "fallback_fraction", fallback_fraction)
        _set_if_attr(stock, "rsl_eligible", not has_hard_fail)
        _set_if_attr(stock, "excluded_from_ranking", has_hard_fail)
        _set_if_attr(stock, "ranking_exclude_reason", "; ".join(hard_fail_reasons) if has_hard_fail else "")

        if has_hard_fail:
            _set_if_attr(stock, "ranking_integrity_status", "not_eligible_unreliable")
            _set_if_attr(stock, "repair_applied", False)
            _set_if_attr(stock, "repair_method", "")
            _set_if_attr(stock, "repair_reason", "")
        else:
            if used_close_fallback:
                _set_if_attr(stock, "ranking_integrity_status", "eligible_repaired")
                _set_if_attr(stock, "repair_applied", True)
                _set_if_attr(stock, "repair_method", str(rsl_price_source))
                _set_if_attr(stock, "repair_reason", "; ".join(warning_reasons + review_reasons))
            else:
                _set_if_attr(stock, "ranking_integrity_status", "eligible_original")
                _set_if_attr(stock, "repair_applied", False)
                _set_if_attr(stock, "repair_method", "")
                _set_if_attr(stock, "repair_reason", "")

        rsl_value = analysis.get("rsl_value", np.nan)
        rsl_sma = analysis.get("rsl_sma", np.nan)

        try:
            if np.isfinite(rsl_value):
                _set_if_attr(stock, "rsl", float(rsl_value))
        except Exception:
            pass

        try:
            if np.isfinite(rsl_sma):
                _set_if_attr(stock, "sma", float(rsl_sma))
        except Exception:
            pass

    return results


def build_home_market_rsl_audit(
    results: List[Any],
    location_suffix_map: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Baut ein Audit-DataFrame für spätere manuelle Prüfung.
    Minimal-kompatibel für final.py.
    """
    rows: List[Dict[str, Any]] = []

    for stock in results or []:
        rows.append({
            "ticker": _get_row_value(stock, ["yahoo_symbol", "ticker", "original_ticker"], ""),
            "name": _get_row_value(stock, ["name"], ""),
            "country": _get_row_value(stock, ["land", "country"], ""),
            "rsl": _get_row_value(stock, ["rsl"], np.nan),
            "sma": _get_row_value(stock, ["sma"], np.nan),
            "rsl_rank": _get_row_value(stock, ["rsl_rang"], 0),
            "ranking_integrity_status": _get_row_value(stock, ["ranking_integrity_status"], ""),
            "excluded_from_ranking": _get_row_value(stock, ["excluded_from_ranking"], False),
            "ranking_exclude_reason": _get_row_value(stock, ["ranking_exclude_reason"], ""),
            "used_close_fallback": _get_row_value(stock, ["used_close_fallback"], False),
            "rsl_price_source": _get_row_value(stock, ["rsl_price_source"], ""),
            "fallback_fraction": _get_row_value(stock, ["fallback_fraction"], None),
            "repair_applied": _get_row_value(stock, ["repair_applied"], False),
            "repair_method": _get_row_value(stock, ["repair_method"], ""),
            "repair_reason": _get_row_value(stock, ["repair_reason"], ""),
            "integrity_warnings": ", ".join(_safe_list(_get_row_value(stock, ["integrity_warnings"], []))),
        })

    return pd.DataFrame(rows)


def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame,
    top_rank: int = 300,
) -> pd.DataFrame:
    """
    Erstellt eine kompakte Review-Liste:
    - bevorzugt problematische / reparierte Fälle
    - optional auf Top-Rank beschränkt
    """
    if audit_df is None or len(audit_df) == 0:
        return pd.DataFrame()

    df = audit_df.copy()

    if "rsl_rank" in df.columns:
        df["rsl_rank"] = pd.to_numeric(df["rsl_rank"], errors="coerce")
        df = df[(df["rsl_rank"].isna()) | (df["rsl_rank"] <= top_rank)]

    mask = (
        df.get("excluded_from_ranking", False).astype(bool)
        | df.get("used_close_fallback", False).astype(bool)
        | df.get("repair_applied", False).astype(bool)
        | df.get("ranking_integrity_status", "").astype(str).ne("eligible_original")
    )

    shortlist = df.loc[mask].copy()

    sort_cols = [c for c in ["excluded_from_ranking", "repair_applied", "used_close_fallback", "rsl_rank"] if c in shortlist.columns]
    if sort_cols:
        shortlist = shortlist.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")

    return shortlist.reset_index(drop=True)
    
# ============================================================================
# Legacy-/Kompatibilitäts-Wrapper für final.py
# ============================================================================

def _extract_history_from_item(item: Any) -> Optional[pd.DataFrame]:
    """
    Versucht defensiv, eine Historie aus verschiedenen Objekt-/Dict-Formen zu ziehen.
    """
    if item is None:
        return None

    history_candidates = [
        "history",
        "hist",
        "price_history",
        "history_df",
        "kurshistorie",
    ]

    for key in history_candidates:
        try:
            if isinstance(item, dict) and key in item and isinstance(item[key], pd.DataFrame):
                return item[key]
        except Exception:
            pass

        try:
            value = getattr(item, key, None)
            if isinstance(value, pd.DataFrame):
                return value
        except Exception:
            pass

    return None


def _extract_ticker_country_from_item(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Holt Ticker/Land robust aus Dict, pandas-Series oder Objekt.
    """
    ticker = _get_row_value(
        item,
        ["yahoo_symbol", "ticker", "Ticker", "symbol", "Symbol", "original_ticker"],
        default=None,
    )
    country = _get_row_value(
        item,
        ["land", "Land", "country", "Country", "market_country"],
        default=None,
    )

    ticker = _normalize_string(ticker) or None
    country = _normalize_string(country) or None
    return ticker, country


def _analysis_from_item(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    history_df = _extract_history_from_item(item)
    ticker, country = _extract_ticker_country_from_item(item, location_suffix_map)

    if history_df is None or len(history_df) == 0:
        return {
            "history": pd.DataFrame(),
            "rsl_price_column": "rsl_price",
            "rsl_value": np.nan,
            "rsl_sma": np.nan,
            "integrity_reasons": ["missing_history_for_symbol"],
            "hard_fail_reasons": ["missing_history_for_symbol"],
            "warning_reasons": [],
            "review_reasons": [],
            "drop_reasons": ["missing_history_for_symbol"],
            "integrity_warnings": [],
            "has_hard_fail": True,
            "used_close_fallback": False,
            "diagnostics": {},
        }

    return analyze_history_for_rsl_integrity(
        history_df=history_df,
        ticker=ticker,
        country=country,
        cfg=cfg,
    )


def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-API: gibt alle Integrity-Reasons zurück.
    raw_rsl bleibt aus Kompatibilitätsgründen im Interface.
    """
    analysis = _analysis_from_item(item, location_suffix_map, cfg)
    return _unique_keep_order(_safe_list(analysis.get("integrity_reasons", [])))


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, Any]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-API: gibt drop_reasons zurück.
    Im neuen Core sind das kompatibel alle strukturierten Reasons;
    die echte Ausschlussentscheidung bleibt aber an has_hard_fail gekoppelt.
    """
    analysis = _analysis_from_item(item, location_suffix_map, cfg)
    return _unique_keep_order(_safe_list(analysis.get("drop_reasons", [])))


def filter_stock_results_for_rsl_integrity(
    results: List[Any],
    location_suffix_map: Optional[Dict[str, Any]] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """
    Legacy-API für final.py:
    - valid_results: Objekte ohne Hard-Fail
    - dropped_results: Audit-Dicts für Objekte mit Hard-Fail
    """
    if not results:
        return [], []

    valid_results: List[Any] = []
    dropped_results: List[Dict[str, Any]] = []

    for item in results:
        analysis = _analysis_from_item(item, location_suffix_map, cfg)
        ticker, country = _extract_ticker_country_from_item(item, location_suffix_map)

        if analysis.get("has_hard_fail", False):
            dropped_results.append(
                {
                    "Ticker": ticker or "",
                    "Land": country or "",
                    "RSL": analysis.get("rsl_value", np.nan),
                    "drop_reasons": analysis.get("drop_reasons", []),
                    "hard_fail_reasons": analysis.get("hard_fail_reasons", []),
                    "warning_reasons": analysis.get("warning_reasons", []),
                    "review_reasons": analysis.get("review_reasons", []),
                    "used_close_fallback": bool(analysis.get("used_close_fallback", False)),
                    "rsl_price_source": analysis.get("diagnostics", {}).get("rsl_price_source_mode", "adj_close"),
                    "fallback_fraction": float(analysis.get("diagnostics", {}).get("fallback_fraction", 0.0) or 0.0),
                }
            )
        else:
            valid_results.append(item)

    return valid_results, dropped_results


def build_home_market_rsl_audit(
    results: List[Any],
    location_suffix_map: Optional[Dict[str, Any]] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Baut ein Audit-DataFrame für Home-Market-/RSL-Prüfung.
    """
    rows: List[Dict[str, Any]] = []

    for item in results or []:
        analysis = _analysis_from_item(item, location_suffix_map, cfg)
        ticker, country = _extract_ticker_country_from_item(item, location_suffix_map)

        row = {
            "Ticker": ticker or "",
            "Land": country or "",
            "Name": _get_row_value(item, ["name", "Name"], default=""),
            "ISIN": _get_row_value(item, ["isin", "ISIN"], default=""),
            "RSL": _get_row_value(item, ["rsl", "RSL"], default=analysis.get("rsl_value", np.nan)),
            "ranking_integrity_status": _get_row_value(item, ["ranking_integrity_status"], default=""),
            "rsl_eligible": _get_row_value(item, ["rsl_eligible"], default=not analysis.get("has_hard_fail", False)),
            "excluded_from_ranking": _get_row_value(item, ["excluded_from_ranking"], default=bool(analysis.get("has_hard_fail", False))),
            "ranking_exclude_reason": _get_row_value(item, ["ranking_exclude_reason"], default=""),
            "integrity_reasons": analysis.get("integrity_reasons", []),
            "hard_fail_reasons": analysis.get("hard_fail_reasons", []),
            "warning_reasons": analysis.get("warning_reasons", []),
            "review_reasons": analysis.get("review_reasons", []),
            "drop_reasons": analysis.get("drop_reasons", []),
            "used_close_fallback": bool(analysis.get("used_close_fallback", False)),
            "rsl_price_source": analysis.get("diagnostics", {}).get("rsl_price_source_mode", "adj_close"),
            "fallback_fraction": float(analysis.get("diagnostics", {}).get("fallback_fraction", 0.0) or 0.0),
            "repair_applied": bool(_get_row_value(item, ["repair_applied"], default=False)),
            "repair_method": _get_row_value(item, ["repair_method"], default=""),
            "repair_reason": _get_row_value(item, ["repair_reason"], default=""),
            "has_hard_fail": bool(analysis.get("has_hard_fail", False)),
        }

# ============================================================================
# Legacy-/Kompatibilitäts-Wrapper für final.py
# ============================================================================

def _extract_history_from_item(item: Any) -> Optional[pd.DataFrame]:
    """
    Versucht defensiv, eine Historie aus dict-/objektartigen Strukturen zu lesen.
    """
    if item is None:
        return None

    candidates = [
        "history",
        "hist",
        "price_history",
        "history_df",
        "kurshistorie",
    ]

    if isinstance(item, dict):
        for key in candidates:
            value = item.get(key)
            if isinstance(value, pd.DataFrame):
                return value

    for key in candidates:
        try:
            value = getattr(item, key, None)
            if isinstance(value, pd.DataFrame):
                return value
        except Exception:
            pass

    return None


def _extract_symbol_from_item(item: Any) -> Optional[str]:
    candidates = [
        "yahoo_symbol",
        "ticker",
        "symbol",
        "original_ticker",
    ]

    if isinstance(item, dict):
        for key in candidates:
            value = item.get(key)
            if value:
                return str(value)

    for key in candidates:
        try:
            value = getattr(item, key, None)
            if value:
                return str(value)
        except Exception:
            pass

    return None


def _extract_country_from_item(item: Any) -> Optional[str]:
    candidates = ["land", "country", "market_country"]

    if isinstance(item, dict):
        for key in candidates:
            value = item.get(key)
            if value:
                return str(value)

    for key in candidates:
        try:
            value = getattr(item, key, None)
            if value:
                return str(value)
        except Exception:
            pass

    return None


def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-API: liefert die vollständigen Integrity-Reasons.
    """
    history = _extract_history_from_item(item)
    if history is None or history.empty:
        return ["missing_history_for_symbol"]

    analysis = analyze_history_for_rsl_integrity(
        history_df=history,
        ticker=_extract_symbol_from_item(item),
        country=_extract_country_from_item(item),
        cfg=cfg,
    )
    return list(analysis.get("integrity_reasons", []) or [])


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    """
    Legacy-API: liefert nur die Legacy-Drop-Reasons.
    """
    history = _extract_history_from_item(item)
    if history is None or history.empty:
        return ["missing_history_for_symbol"]

    analysis = analyze_history_for_rsl_integrity(
        history_df=history,
        ticker=_extract_symbol_from_item(item),
        country=_extract_country_from_item(item),
        cfg=cfg,
    )
    return list(analysis.get("drop_reasons", []) or [])


def filter_stock_results_for_rsl_integrity(
    results: Iterable[Any],
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
):
    """
    Legacy-API fuer final.py:
    - behaelt nur Objekte ohne Hard-Fail
    - markiert Problemfaelle optional am Objekt
    """
    filtered = []

    for item in results:
        history = _extract_history_from_item(item)
        if history is None or history.empty:
            continue

        analysis = analyze_history_for_rsl_integrity(
            history_df=history,
            ticker=_extract_symbol_from_item(item),
            country=_extract_country_from_item(item),
            cfg=cfg,
        )

        has_hard_fail = bool(analysis.get("has_hard_fail", False))

        try:
            if isinstance(item, dict):
                item["integrity_reasons"] = list(analysis.get("integrity_reasons", []) or [])
                item["integrity_warnings"] = list(analysis.get("integrity_warnings", []) or [])
                item["drop_reasons"] = list(analysis.get("drop_reasons", []) or [])
                item["used_close_fallback"] = bool(analysis.get("used_close_fallback", False))
                item["has_hard_fail"] = has_hard_fail
            else:
                setattr(item, "integrity_reasons", list(analysis.get("integrity_reasons", []) or []))
                setattr(item, "integrity_warnings", list(analysis.get("integrity_warnings", []) or []))
                setattr(item, "drop_reasons", list(analysis.get("drop_reasons", []) or []))
                setattr(item, "used_close_fallback", bool(analysis.get("used_close_fallback", False)))
                setattr(item, "has_hard_fail", has_hard_fail)
        except Exception:
            pass

        if not has_hard_fail:
            filtered.append(item)

    return filtered


def _infer_home_symbol(symbol: str, location_suffix_map: Optional[Dict[str, str]] = None) -> str:
    """
    Sehr defensiver Home-Market-Heuristik-Helper:
    wenn kein Mapping greift, bleibt das Symbol unverändert.
    """
    if not symbol:
        return symbol

    if not location_suffix_map:
        return symbol

    # Heuristik: wenn Symbol bereits einen bekannten Suffix hat, 그대로 lassen
    for suffix in location_suffix_map.values():
        if suffix and str(symbol).endswith(str(suffix)):
            return symbol

    return symbol


def build_home_market_rsl_audit(
    results: Iterable[Any],
    location_suffix_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Baut eine Audit-Tabelle für final.py.
    Minimal, robust, ohne neue Fachlogik zu erfinden.
    """
    rows: List[Dict[str, Any]] = []

    for item in results:
        symbol = _extract_symbol_from_item(item) or ""
        home_symbol = _infer_home_symbol(symbol, location_suffix_map)

        if isinstance(item, dict):
            row = dict(item)
        else:
            row = {}
            for attr in [
                "name", "land", "sector", "industry", "rsl", "kurs", "sma",
                "ranking_integrity_status", "integrity_reasons", "drop_reasons",
                "integrity_warnings", "used_close_fallback", "rsl_price_source",
                "ranking_exclude_reason", "rsl_rang", "market_value"
            ]:
                try:
                    row[attr] = getattr(item, attr, None)
                except Exception:
                    pass

        row["symbol"] = symbol
        row["home_symbol"] = home_symbol
        row["is_home_symbol"] = (symbol == home_symbol)
        rows.append(row)

    return pd.DataFrame(rows)


def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame,
    top_rank: int = 300,
) -> pd.DataFrame:
    """
    Minimaler Review-Shortlist-Builder für final.py.
    """
    if audit_df is None or audit_df.empty:
        return pd.DataFrame()

    df = audit_df.copy()

    if "rsl_rang" in df.columns:
        rank_mask = pd.to_numeric(df["rsl_rang"], errors="coerce").fillna(np.inf) <= top_rank
        df = df.loc[rank_mask].copy()

    review_mask = pd.Series(False, index=df.index)

    for col in ["integrity_reasons", "drop_reasons", "integrity_warnings"]:
        if col in df.columns:
            review_mask = review_mask | df[col].astype(str).str.len().gt(2)

    if "used_close_fallback" in df.columns:
        review_mask = review_mask | df["used_close_fallback"].fillna(False).astype(bool)

    if "is_home_symbol" in df.columns:
        review_mask = review_mask | (~df["is_home_symbol"].fillna(True))

    return df.loc[review_mask].copy()
        rows.append(row)

    audit_df = pd.DataFrame(rows)

    if not audit_df.empty:
        if "RSL" in audit_df.columns:
            audit_df["RSL"] = pd.to_numeric(audit_df["RSL"], errors="coerce")
        if "fallback_fraction" in audit_df.columns:
            audit_df["fallback_fraction"] = pd.to_numeric(audit_df["fallback_fraction"], errors="coerce").fillna(0.0)

    return audit_df


def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame,
    top_rank: int = 300,
) -> pd.DataFrame:
    """
    Baut eine kurze Review-Shortlist aus dem Audit.
    """
    if audit_df is None or len(audit_df) == 0:
        return pd.DataFrame()

    df = audit_df.copy()

    if "rsl_rang" in df.columns:
        df["rsl_rang"] = pd.to_numeric(df["rsl_rang"], errors="coerce")
        df = df[(df["rsl_rang"].isna()) | (df["rsl_rang"] <= top_rank)]

    def _has_entries(value: Any) -> bool:
        if isinstance(value, list):
            return len(value) > 0
        if value is None:
            return False
        if isinstance(value, float) and pd.isna(value):
            return False
        text = str(value).strip()
        return text not in {"", "[]", "nan", "None"}

    mask = (
        df.get("has_hard_fail", False).astype(bool)
        | df["review_reasons"].apply(_has_entries)
        | df["warning_reasons"].apply(_has_entries)
    )

    shortlist = df.loc[mask].copy()

    sort_cols = [c for c in ["has_hard_fail", "fallback_fraction", "RSL"] if c in shortlist.columns]
    if sort_cols:
        ascending = []
        for c in sort_cols:
            if c == "RSL":
                ascending.append(False)
            else:
                ascending.append(False)
        shortlist = shortlist.sort_values(sort_cols, ascending=ascending, na_position="last")

    return shortlist.reset_index(drop=True)

# ============================================================================
# Legacy-Wrapper fuer final.py / bestehende Aufrufer
# ============================================================================

def _legacy_get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _legacy_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    if isinstance(value, tuple):
        return [str(x) for x in value if str(x).strip()]
    if isinstance(value, set):
        return [str(x) for x in value if str(x).strip()]
    text = str(value).strip()
    return [text] if text else []


def _legacy_unique(seq: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in seq:
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    reasons: List[str] = []

    reasons.extend(_legacy_list(_legacy_get(item, "integrity_reasons", [])))
    reasons.extend(_legacy_list(_legacy_get(item, "hard_fail_reasons", [])))
    reasons.extend(_legacy_list(_legacy_get(item, "warning_reasons", [])))
    reasons.extend(_legacy_list(_legacy_get(item, "review_reasons", [])))
    reasons.extend(_legacy_list(_legacy_get(item, "drop_reasons", [])))
    reasons.extend(_legacy_list(_legacy_get(item, "integrity_warnings", [])))

    if bool(_legacy_get(item, "used_close_fallback", False)):
        reasons.append("close_fallback_used")

    source = str(_legacy_get(item, "rsl_price_source", "") or "").strip()
    if source:
        reasons.append(f"rsl_price_source:{source}")

    fallback_fraction = _legacy_get(item, "fallback_fraction", None)
    try:
        if fallback_fraction is not None:
            reasons.append(f"fallback_fraction:{float(fallback_fraction):.4f}")
    except Exception:
        pass

    return _legacy_unique(reasons)


def get_rsl_integrity_drop_reasons(
    item: Any,
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
    raw_rsl: Any = None,
) -> List[str]:
    hard = _legacy_list(_legacy_get(item, "hard_fail_reasons", []))
    if hard:
        return _legacy_unique(hard)

    has_hard_fail = bool(_legacy_get(item, "has_hard_fail", False))
    drop = _legacy_list(_legacy_get(item, "drop_reasons", []))

    if has_hard_fail:
        return _legacy_unique(drop or ["hard_fail"])

    # Legacy-Fallback: falls nur gemischte integrity_reasons vorhanden sind
    reasons = get_rsl_integrity_reasons(
        item,
        location_suffix_map=location_suffix_map,
        cfg=cfg,
        raw_rsl=raw_rsl,
    )
    hard_like = [
        r for r in reasons
        if any(token in str(r).lower() for token in [
            "hard_fail",
            "insufficient_history",
            "too_much_close_fallback",
            "invalid",
            "broken",
            "missing",
        ])
    ]
    return _legacy_unique(hard_like)


def filter_stock_results_for_rsl_integrity(
    results: Any,
    location_suffix_map: Optional[Dict[str, str]] = None,
    cfg: Optional[Dict[str, Any]] = None,
):
    if results is None:
        return results

    if isinstance(results, pd.DataFrame):
        if len(results) == 0:
            return results.copy()

        def _row_ok(row: pd.Series) -> bool:
            hard = bool(row.get("has_hard_fail", False))
            if hard:
                return False
            drop = row.get("drop_reasons", [])
            if isinstance(drop, str):
                drop = [drop]
            return len(drop or []) == 0

        mask = results.apply(_row_ok, axis=1)
        return results.loc[mask].copy()

    if isinstance(results, (list, tuple)):
        filtered = []
        for item in results:
            hard = bool(_legacy_get(item, "has_hard_fail", False))
            drop = get_rsl_integrity_drop_reasons(
                item,
                location_suffix_map=location_suffix_map,
                cfg=cfg,
            )
            if not hard and not drop:
                filtered.append(item)
        return filtered

    return results


def build_home_market_rsl_audit(
    results,
    location_suffix_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if not results:
        return pd.DataFrame(columns=[
            "Ticker",
            "Name",
            "Land",
            "Yahoo Symbol",
            "RSL",
            "RSL Rang",
            "used_close_fallback",
            "rsl_price_source",
            "fallback_fraction",
            "has_hard_fail",
            "drop_reasons",
            "integrity_reasons",
        ])

    for item in results:
        ticker = _legacy_get(item, "original_ticker", _legacy_get(item, "ticker", ""))
        yahoo_symbol = _legacy_get(item, "yahoo_symbol", ticker)
        name = _legacy_get(item, "name", "")
        land = _legacy_get(item, "land", _legacy_get(item, "country", ""))
        rsl = _legacy_get(item, "rsl", _legacy_get(item, "RSL", np.nan))
        rsl_rang = _legacy_get(item, "rsl_rang", _legacy_get(item, "RSL_Rang", 0))
        used_close_fallback = bool(_legacy_get(item, "used_close_fallback", False))
        rsl_price_source = str(_legacy_get(item, "rsl_price_source", "") or "")
        fallback_fraction = _legacy_get(item, "fallback_fraction", None)
        has_hard_fail = bool(_legacy_get(item, "has_hard_fail", False))
        drop_reasons = get_rsl_integrity_drop_reasons(item, location_suffix_map=location_suffix_map)
        integrity_reasons = get_rsl_integrity_reasons(item, location_suffix_map=location_suffix_map)

        rows.append({
            "Ticker": ticker,
            "Name": name,
            "Land": land,
            "Yahoo Symbol": yahoo_symbol,
            "RSL": rsl,
            "RSL Rang": rsl_rang,
            "used_close_fallback": used_close_fallback,
            "rsl_price_source": rsl_price_source,
            "fallback_fraction": fallback_fraction,
            "has_hard_fail": has_hard_fail,
            "drop_reasons": ", ".join(drop_reasons),
            "integrity_reasons": ", ".join(integrity_reasons),
        })

    audit_df = pd.DataFrame(rows)

    if "RSL Rang" in audit_df.columns:
        audit_df = audit_df.sort_values(
            by=["RSL Rang", "RSL"],
            ascending=[True, False],
            na_position="last",
        ).reset_index(drop=True)

    return audit_df


def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame,
    top_rank: int = 300,
) -> pd.DataFrame:
    if audit_df is None or len(audit_df) == 0:
        return pd.DataFrame(columns=list(audit_df.columns) if isinstance(audit_df, pd.DataFrame) else [])

    work = audit_df.copy()

    if "RSL Rang" in work.columns:
        work = work.loc[pd.to_numeric(work["RSL Rang"], errors="coerce") <= float(top_rank)].copy()

    if "drop_reasons" in work.columns:
        work = work.loc[work["drop_reasons"].fillna("").astype(str).str.strip() == ""].copy()

    if "integrity_reasons" in work.columns:
        work = work.loc[work["integrity_reasons"].fillna("").astype(str).str.strip() != ""].copy()

    return work.reset_index(drop=True)
