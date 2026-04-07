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

    # Wenn beide vorhanden sind, Dividenden-/Adjustierungsprüfung
    if close_col is not None and adj_col is not None:
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

                    start = max(0, loc - int(cfg["dividend_window_before"]))
                    end = min(len(df), loc + int(cfg["dividend_window_after"]) + 1)

                    fallback_idx = df.index[start:end]

                    # Nur dort ersetzen, wo Close plausibel > 0 ist
                    close_slice = close_series.loc[fallback_idx]
                    valid_mask = close_slice.notna() & (close_slice > 0)

                    if valid_mask.any():
                        idx_to_replace = close_slice.index[valid_mask]
                        df.loc[idx_to_replace, "rsl_price"] = close_slice.loc[idx_to_replace]
                        df.loc[idx_to_replace, "rsl_price_source"] = "close_fallback_dividend_window"
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

    return PriceSeriesBuildResult(
        history=df,
        rsl_price_column="rsl_price",
        used_close_fallback=used_close_fallback,
        reasons=reasons,
        diagnostics=diagnostics,
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

    # WICHTIG:
    # kein Hard-Fail nur wegen hohem RSL!
    # Aber wenn RSL extrem hoch ist UND Datenprobleme vorliegen, markieren wir den Zusammenhang.
    if np.isfinite(rsl_value):
        diagnostics["rsl_value"] = rsl_value
        diagnostics["rsl_sma"] = sma_value

        if rsl_value > 5:
            if reasons.has_any():
                reasons.add("extreme_rsl_with_detected_data_issue", "review")
            else:
                reasons.add("extreme_rsl_without_detected_cause_review_needed", "review")

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

    if pd.isna(row_dict.get("RSL")) or row_dict.get("RSL") is None:
        row_dict["RSL"] = analysis.get("rsl_value", np.nan)

    return row_dict


def apply_rsl_integrity_to_universe(
    universe_df: pd.DataFrame,
    history_map: Optional[Dict[str, pd.DataFrame]] = None,
    ticker_col: str = "Ticker",
    country_col: str = "Land",
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analysiert ein Universe/DataFrame und trennt:
    - valid_results: nur ohne Hard-Fail
    - dropped_df: nur mit Hard-Fail

    WICHTIG:
    Kein Ausschluss wegen hohem RSL selbst.
    Ausschluss nur über Hard-Fail-Datenursachen.
    """
    cfg = _merge_config(cfg)

    if universe_df is None or len(universe_df) == 0:
        empty = pd.DataFrame(columns=[
            ticker_col,
            "integrity_reasons",
            "hard_fail_reasons",
            "warning_reasons",
            "review_reasons",
            "drop_reasons",
            "integrity_warnings",
            "has_hard_fail",
            "used_close_fallback",
        ])
        return empty.copy(), empty.copy()

    rows_out: List[Dict[str, Any]] = []

    for _, row in universe_df.iterrows():
        row_dict = row.to_dict()
        ticker = _normalize_string(row_dict.get(ticker_col))
        country = _normalize_string(row_dict.get(country_col))

        history_df = None

        if history_map and ticker in history_map:
            history_df = history_map[ticker]
        else:
            history_df = _extract_history_object(row)

        if history_df is None or not isinstance(history_df, pd.DataFrame):
            reasons = IntegrityReasonSet()
            reasons.add("missing_history_for_symbol", "hard_fail")

            row_dict["integrity_reasons"] = reasons.all_reasons()
            row_dict["hard_fail_reasons"] = reasons.hard_fail_reasons
            row_dict["warning_reasons"] = reasons.warning_reasons
            row_dict["review_reasons"] = reasons.review_reasons
            row_dict["drop_reasons"] = reasons.legacy_drop_reasons()
            row_dict["integrity_warnings"] = reasons.warning_reasons
            row_dict["has_hard_fail"] = True
            row_dict["used_close_fallback"] = False
            rows_out.append(row_dict)
            continue

        analysis = analyze_history_for_rsl_integrity(
            history_df=history_df,
            ticker=ticker,
            country=country,
            cfg=cfg,
        )

        row_dict = _append_reasons_to_row_dict(row_dict, analysis)
        rows_out.append(row_dict)

    out_df = pd.DataFrame(rows_out)

    # Saubere Listenfelder garantieren
    list_cols = [
        "integrity_reasons",
        "hard_fail_reasons",
        "warning_reasons",
        "review_reasons",
        "drop_reasons",
        "integrity_warnings",
    ]
    for c in list_cols:
        if c in out_df.columns:
            out_df[c] = out_df[c].apply(_safe_list)

    has_hard_fail_mask = out_df["has_hard_fail"].fillna(False).astype(bool) if "has_hard_fail" in out_df.columns else pd.Series(False, index=out_df.index)

    valid_results = out_df.loc[~has_hard_fail_mask].copy()
    dropped_df = out_df.loc[has_hard_fail_mask].copy()

    return valid_results, dropped_df


# ============================================================================
# Rückwärtskompatible API
# ============================================================================

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

    @property
    def all_reasons(self) -> List[str]:
        return list(dict.fromkeys(self.hard_fail_reasons + self.warning_reasons + self.review_reasons))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "needs_review": self.needs_review,
            "hard_fail_reasons": list(self.hard_fail_reasons),
            "warning_reasons": list(self.warning_reasons),
            "review_reasons": list(self.review_reasons),
            "all_reasons": self.all_reasons,
        }

def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


SECONDARY_LISTING_SUFFIXES = {".BE", ".DE", ".DU", ".F", ".HM", ".MU", ".SG"}

def _optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return float(value)
    except Exception:
        return None

def get_rsl_integrity_reasons(
    item: Any,
    location_suffix_map: Dict[str, str],
    config: Dict[str, Any],
    raw_rsl: Any = None,
) -> List[str]:
    """
    Sammelt Integritätsgründe. Unterstützt sowohl history_df als auch StockData Objekte.
    """
    if isinstance(item, pd.DataFrame):
        return get_rsl_integrity_drop_reasons(
            history_df=item,
            ticker=None,
            country=None,
            cfg=config
        )
    
    reasons: List[str] = []
    trust_score = int(_get_row_value(item, ["trust_score"], 3))
    flag_stale = str(_get_row_value(item, ["flag_stale"], "OK")).upper()
    flag_hist = str(_get_row_value(item, ["flag_history_length"], "OK")).upper()
    flag_scale = str(_get_row_value(item, ["flag_scale"], "OK")).upper()

    if flag_stale == "CRITICAL":
        reasons.append("critical_stale_data")
    if flag_hist == "CRITICAL":
        reasons.append("critical_history_length")
    if flag_scale == "CRITICAL":
        reasons.append("critical_price_scale")
    elif flag_scale == "WARN":
        reasons.append("suspicious_price_scale")
    if trust_score < 1:
        reasons.append("low_trust_score")

    rsl_val = _coerce_float(_get_row_value(item, ["rsl"], 0.0))
    if rsl_val <= 0:
        reasons.append("no_valid_rsl_data")

    return list(dict.fromkeys(reasons))

def get_history_status(item: Any, location_suffix_map: Dict[str, str]) -> str:
    orig = str(_get_row_value(item, ["original_ticker"], "")).upper().strip()
    hist_sym = str(_get_row_value(item, ["yahoo_symbol"], orig)).upper().strip()
    land = str(_get_row_value(item, ["land"], "")).strip()
    home_suffix = str(location_suffix_map.get(land, "")).strip().upper()
    if home_suffix and not home_suffix.startswith("."): home_suffix = f".{home_suffix}"

    if home_suffix and hist_sym.endswith(home_suffix):
        return "OVERRIDDEN_TO_HOME" if orig != hist_sym else "HOME_HISTORY_ACTIVE"
    
    hist_sfx = f".{hist_sym.rsplit('.', 1)[-1]}" if "." in hist_sym else ""
    if hist_sfx in SECONDARY_LISTING_SUFFIXES:
        return "SECONDARY_HISTORY_ACTIVE"
    
    return "PRIMARY_WITHOUT_SUFFIX" if "." not in hist_sym else "UNKNOWN"

def assess_integrity(item: Any, location_suffix_map: Dict[str, str], config: Dict[str, Any]) -> IntegrityAssessment:
    assessment = IntegrityAssessment()
    reasons = get_rsl_integrity_reasons(item, location_suffix_map, config)
    
    hard_fail_criteria = {"no_valid_rsl_data", "critical_history_length"}
    review_criteria = {"suspicious_price_scale", "low_trust_score"}
    warning_criteria = {"critical_stale_data", "critical_price_scale"}

    for r in reasons:
        if r in hard_fail_criteria: assessment.add_hard_fail(r)
        elif r in review_criteria: assessment.add_review(r)
        else: assessment.add_warning(r)
    return assessment

def filter_stock_results_for_rsl_integrity(
    stock_results: List[Any],
    location_suffix_map: Dict[str, str],
    config: Dict[str, Any],
) -> Tuple[List[Any], pd.DataFrame]:
    valid_results: List[Any] = []
    issue_rows: List[Dict[str, Any]] = []

    for stock in stock_results or []:
        assessment = assess_integrity(stock, location_suffix_map, config)
        if assessment.all_reasons:
            try:
                setattr(stock, "integrity_warnings", assessment.all_reasons)
            except AttributeError: pass

        if assessment.is_valid:
            valid_results.append(stock)
        
        if not assessment.is_valid or assessment.all_reasons:
            issue_rows.append({
                "original_ticker": _get_row_value(stock, ["original_ticker"], ""),
                "yahoo_symbol": _get_row_value(stock, ["yahoo_symbol"], ""),
                "name": _get_row_value(stock, ["name"], ""),
                "land": _get_row_value(stock, ["land"], ""),
                "history_status": get_history_status(stock, location_suffix_map),
                "integrity_reasons": ", ".join(assessment.all_reasons),
                "is_valid": assessment.is_valid,
                "needs_review": assessment.needs_review,
                "hard_fail_reasons": "; ".join(assessment.hard_fail_reasons),
                "warning_reasons": "; ".join(assessment.warning_reasons),
                "review_reasons": "; ".join(assessment.review_reasons),
                "rsl": _get_row_value(stock, ["rsl"], None),
            })

    dropped_df = pd.DataFrame(issue_rows)
    return valid_results, dropped_df

def build_home_market_rsl_audit(results: List[Any], location_suffix_map: Dict[str, str]) -> pd.DataFrame:
    rows = []
    for stock in results or []:
        status = get_history_status(stock, location_suffix_map)
        rows.append({
            "history_status": status,
            "original_ticker": _get_row_value(stock, ["original_ticker"], ""),
            "history_symbol": _get_row_value(stock, ["yahoo_symbol"], ""),
            "name": _get_row_value(stock, ["name"], ""),
            "land": _get_row_value(stock, ["land"], ""),
            "rsl_rank": _get_row_value(stock, ["rsl_rang"], None),
            "needs_review": status == "SECONDARY_HISTORY_ACTIVE"
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["needs_review", "rsl_rank"], ascending=[False, True])
    return df

def build_home_market_rsl_review_shortlist(audit_df: pd.DataFrame, top_rank: int = 300) -> pd.DataFrame:
    if audit_df.empty: return audit_df
    work = audit_df.copy()
    if "rsl_rank" in work.columns:
        work = work[pd.to_numeric(work["rsl_rank"], errors='coerce') <= top_rank]
    if "needs_review" in work.columns:
        work = work[work["needs_review"] == True]
    return work

def get_rsl_integrity_drop_reasons(
    history_df: pd.DataFrame,
    ticker: Optional[str] = None,
    country: Optional[str] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Legacy-Alias:
    gibt weiterhin 'drop_reasons' zurück.
    Wichtig: Das sind ALLE identifizierten Reasons,
    nicht nur echte Hard-Fails.
    """
    analysis = analyze_history_for_rsl_integrity(
        history_df=history_df,
        ticker=ticker,
        country=country,
        cfg=cfg,
    )
    return analysis.get("drop_reasons", [])


def enrich_with_rsl_integrity(
    df: pd.DataFrame,
    history_map: Optional[Dict[str, pd.DataFrame]] = None,
    ticker_col: str = "Ticker",
    country_col: str = "Land",
    cfg: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Enriched DataFrame zurückgeben, ohne Aufteilung valid/dropped.
    """
    valid_df, dropped_df = apply_rsl_integrity_to_universe(
        universe_df=df,
        history_map=history_map,
        ticker_col=ticker_col,
        country_col=country_col,
        cfg=cfg,
    )

    if valid_df.empty and dropped_df.empty:
        return pd.DataFrame()

    if valid_df.empty:
        return dropped_df.copy()

    if dropped_df.empty:
        return valid_df.copy()

    out = pd.concat([valid_df, dropped_df], ignore_index=True)
    return out


# ============================================================================
# Optionale Hilfsfunktion: direkte RSL-Neuberechnung auf Basis der reparierten Serie
# ============================================================================

def recompute_rsl_column_from_history_map(
    universe_df: pd.DataFrame,
    history_map: Dict[str, pd.DataFrame],
    ticker_col: str = "Ticker",
    country_col: str = "Land",
    target_rsl_col: str = "RSL",
    cfg: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Berechnet die RSL-Spalte aus den Histories neu, wobei die in dieser Datei
    definierte reparierte Preisbasis genutzt wird.
    """
    cfg = _merge_config(cfg)
    out = universe_df.copy()

    if target_rsl_col not in out.columns:
        out[target_rsl_col] = np.nan

    used_close_fallback_list = []
    integrity_reasons_list = []
    hard_fail_reasons_list = []
    warning_reasons_list = []
    review_reasons_list = []
    drop_reasons_list = []

    for idx, row in out.iterrows():
        ticker = _normalize_string(row.get(ticker_col))
        country = _normalize_string(row.get(country_col))

        hist = history_map.get(ticker)
        if hist is None or not isinstance(hist, pd.DataFrame):
            out.at[idx, target_rsl_col] = np.nan
            used_close_fallback_list.append(False)
            integrity_reasons_list.append(["missing_history_for_symbol"])
            hard_fail_reasons_list.append(["missing_history_for_symbol"])
            warning_reasons_list.append([])
            review_reasons_list.append([])
            drop_reasons_list.append(["missing_history_for_symbol"])
            continue

        analysis = analyze_history_for_rsl_integrity(
            history_df=hist,
            ticker=ticker,
            country=country,
            cfg=cfg,
        )

        out.at[idx, target_rsl_col] = analysis.get("rsl_value", np.nan)
        used_close_fallback_list.append(bool(analysis.get("used_close_fallback", False)))
        integrity_reasons_list.append(_safe_list(analysis.get("integrity_reasons", [])))
        hard_fail_reasons_list.append(_safe_list(analysis.get("hard_fail_reasons", [])))
        warning_reasons_list.append(_safe_list(analysis.get("warning_reasons", [])))
        review_reasons_list.append(_safe_list(analysis.get("review_reasons", [])))
        drop_reasons_list.append(_safe_list(analysis.get("drop_reasons", [])))

    out["used_close_fallback"] = used_close_fallback_list
    out["integrity_reasons"] = integrity_reasons_list
    out["hard_fail_reasons"] = hard_fail_reasons_list
    out["warning_reasons"] = warning_reasons_list
    out["review_reasons"] = review_reasons_list
    out["drop_reasons"] = drop_reasons_list
    out["integrity_warnings"] = warning_reasons_list
    out["has_hard_fail"] = out["hard_fail_reasons"].apply(lambda x: len(_safe_list(x)) > 0)

    return out


# ============================================================================
# Komfortfunktion für Einzeltest
# ============================================================================

def debug_single_history(
    history_df: pd.DataFrame,
    ticker: Optional[str] = None,
    country: Optional[str] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Komfortfunktion zum Debuggen eines Einzelfalls.
    """
    analysis = analyze_history_for_rsl_integrity(
        history_df=history_df,
        ticker=ticker,
        country=country,
        cfg=cfg,
    )

    hist = analysis["history"].copy()

    return {
        "ticker": ticker,
        "country": country,
        "rsl_value": analysis.get("rsl_value"),
        "used_close_fallback": analysis.get("used_close_fallback"),
        "integrity_reasons": analysis.get("integrity_reasons"),
        "hard_fail_reasons": analysis.get("hard_fail_reasons"),
        "warning_reasons": analysis.get("warning_reasons"),
        "review_reasons": analysis.get("review_reasons"),
        "drop_reasons": analysis.get("drop_reasons"),
        "diagnostics": analysis.get("diagnostics", {}),
        "history_preview_tail": hist.tail(25),
    }
