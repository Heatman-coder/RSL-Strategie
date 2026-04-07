#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
data_manager.py

Zentrale Verwaltung für Marktdaten, Caching und technische Indikatoren.

Wichtige Änderung:
- Yahoo-History wird bewusst mit auto_adjust = False geladen
- Die operative RSL-Basis wird anschließend über core.rsl_integrity.build_rsl_price_series(...)
  konstruiert
- Dadurch greift die neue Dividend-/Adj-Close-Integritätslogik jetzt auch wirklich
  in der eigentlichen Ranking-Pipeline
"""

from __future__ import annotations

import os
import json
import time
import logging
import datetime
from dataclasses import dataclass, asdict, field
from threading import Lock
from typing import Dict, Any, Optional, List, Tuple, Union, cast

import numpy as np
import pandas as pd
import yfinance as yf

try:
    from core.rsl_integrity import build_rsl_price_series
except Exception:  # pragma: no cover
    build_rsl_price_series = None  # type: ignore


logger = logging.getLogger(__name__)


# ============================================================================
# DATENSTRUKTUR
# ============================================================================

@dataclass(slots=True, kw_only=True)
class StockData:
    original_ticker: str
    yahoo_symbol: str
    isin: str
    name: str
    sector: str
    industry: str
    land: str
    market_value: float
    kurs: float
    sma: float
    rsl: float

    atr: float = 0.0
    atr_limit: float = 0.0
    atr_sell_limit: float = 0.0

    avg_volume_eur: float = 0.0
    primary_liquidity_eur: float = 0.0
    primary_liquidity_symbol: str = ""
    primary_liquidity_basis: str = ""

    # DATA QUALITY FLAGS
    flag_gap: str = "OK"
    flag_liquidity: str = "OK"
    flag_stale: str = "OK"
    flag_scale: str = "OK"
    flag_history_length: str = "OK"

    history_length_reason: str = ""
    scale_reason: str = ""
    stale_reason: str = ""

    price_scale_ratio: float = 1.0
    stale_days: int = 0

    # MARKET TREND INDICATORS
    trend_sma50: str = "OK"
    trend_smoothness: float = 0.0
    trend_quality: str = "N/A"

    # TWSS
    twss_score: float = 0.0
    twss_date: str = ""
    twss_days_ago: int = 0
    twss_raw_pct: float = 0.0
    twss_orientation: str = "NIEDRIG"

    # Momentum & Ranking
    rsl_change_1w: float = 0.0
    rsl_past: Optional[float] = None

    mom_12m: Optional[float] = None
    mom_6m: Optional[float] = None
    mom_3m: Optional[float] = None
    mom_score: Optional[float] = None
    mom_vol: Optional[float] = None
    mom_score_adj: Optional[float] = None
    mom_accel: Optional[float] = None
    max_drawdown_6m: float = 0.0

    mom_cluster: str = ""
    industry_median_rsl: float = 0.0
    peer_spread: float = 0.0

    high_52w: float = 0.0
    distance_52w_high_pct: Optional[float] = None
    trust_score: int = 0

    rsl_rang: int = 0
    mktcap_rang: int = 0
    in_depot: str = ""
    source_etf: str = ""
    listing_source: str = ""
    market_cap: float = 0.0

    first_seen_date: str = ""
    is_new: bool = False

    integrity_warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# RATE LIMIT STATE
# ============================================================================

INFO_RATE_LIMIT_STATE = {
    "rate_limit_hits": 0,
    "extra_delay_s": 0.0,
    "last_hit_time": 0.0,
}


def _consume_rate_limit_hits() -> int:
    hits = int(cast(int, INFO_RATE_LIMIT_STATE["rate_limit_hits"]))
    INFO_RATE_LIMIT_STATE["rate_limit_hits"] = 0
    return hits


def retry_decorator(func):
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                msg = str(e)
                if "429" in msg or "Too Many Requests" in msg:
                    INFO_RATE_LIMIT_STATE["rate_limit_hits"] += 1
                    wait = 30 * (attempt + 1)
                    logger.warning(f"Rate Limit (429). Warte {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(2)
        return None
    return wrapper


def _calc_momentum(series: pd.Series, curr_price: float, lookback: int) -> Optional[float]:
    if len(series) < lookback:
        return None
    past_price = float(series.iloc[-lookback])
    if past_price <= 0:
        return None
    return (curr_price / past_price) - 1.0


# ============================================================================
# MANAGER
# ============================================================================

class MarketDataManager:
    def __init__(self, config: Union[Dict, str], currency_rates: Union[Dict, str, None]):
        if isinstance(config, str):
            info_cache_file = str(currency_rates or "ticker_info_cache.json")
            config = {
                "history_cache_file": config,
                "ticker_info_cache_file": info_cache_file,
            }
            currency_rates = {"DEFAULT": 1.0}
        elif not isinstance(config, dict):
            config = {}

        if not isinstance(currency_rates, dict):
            currency_rates = {"DEFAULT": 1.0}

        default_config = {
            "ticker_info_cache_file": "ticker_info_cache.json",
            "history_cache_file": "history_cache.json",
            "history_period": "18mo",
            "sma_length": 130,
            "sma_short_length": 50,
            "stale_window": 60,
            "max_flat_days": 15,
            "max_consecutive_flat": 20,
            "max_std_rel": 0.005,
            "min_total_range": 0.02,
            "max_total_return": 20.0,
            "price_scale_warn_ratio": 8.0,
            "price_scale_critical_ratio": 15.0,
            "price_scale_warn_jump": 0.8,
            "price_scale_critical_jump": 1.5,
            "mom_lookback_12m": 252,
            "mom_lookback_6m": 126,
            "mom_lookback_3m": 63,
            "mom_weight_12m": 0.5,
            "mom_weight_6m": 0.3,
            "mom_weight_3m": 0.2,
            "atr_period": 14,
            "atr_multiplier_limit": 1.0,
            "atr_multiplier_exit": 0.15,

            # TWSS
            "twss_decay_days": 60.0,

            # RSL-Integrity / Dividendenlogik
            "integrity_dividend_window_before": 20,
            "integrity_dividend_window_after": 7,
            "integrity_adj_close_gap_warn_threshold": 0.08,
            "integrity_adj_close_gap_hard_threshold": 0.25,
            "integrity_dividend_multiplier_tolerance": 0.08,
            "integrity_daily_return_warn_threshold": 0.35,
            "integrity_daily_return_hard_threshold": 0.80,
            "integrity_fallback_fraction_warn": 0.10,
            "integrity_fallback_fraction_hard": 0.35,
            "integrity_flat_run_warn": 10,
            "integrity_flat_run_hard": 25,
        }

        merged_config = dict(default_config)
        merged_config.update(config)

        self.config: Dict[str, Any] = merged_config
        self.currency_rates = dict(currency_rates)

        self.lock = Lock()
        self.cache: Dict[str, Any] = {}
        self.info_cache = self._load_json(str(self.config.get("ticker_info_cache_file", "ticker_info_cache.json")))

        self.failed_tickers: Dict[str, Any] = {}
        self.young_tickers: Dict[str, Any] = {}
        self.last_history_batch_used_network = False

        h_file = self.config.get("history_cache_file")
        if h_file and os.path.exists(h_file):
            try:
                with open(h_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.cache = data.get("data", {})
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_failed_records(self) -> List[Dict]:
        return list(self.failed_tickers.values())

    def get_young_records(self) -> List[Dict]:
        return list(self.young_tickers.values())

    def save_cache(self) -> None:
        path = str(self.config.get("history_cache_file", "history_cache.json"))
        payload = {
            "saved_at": datetime.datetime.now().isoformat(),
            "data": self.cache,
        }
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.error(f"Konnte History-Cache nicht speichern ({path}): {e}")

    def save_info_cache(self) -> None:
        path = self.config.get("ticker_info_cache_file", "ticker_info_cache.json")
        try:
            info_cache_dir = os.path.dirname(path)
            if info_cache_dir:
                os.makedirs(info_cache_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.info_cache, f, indent=2)
            logger.debug(f"Info-Cache erfolgreich gespeichert: {path}")
        except Exception as e:
            logger.error(f"Konnte Info-Cache nicht speichern ({path}): {e}")

    def clear_cache(self) -> None:
        self.cache = {}
        h_file = cast(Optional[str], self.config.get("history_cache_file"))
        if h_file and os.path.exists(str(h_file)):
            os.remove(str(h_file))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_json(self, path: Any) -> Dict[str, Any]:
        path_str = str(path) if path else ""
        if path_str and os.path.exists(path_str):
            try:
                with open(path_str, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, dict) else {}
            except json.JSONDecodeError as e:
                logger.error(f"JSON-Formatfehler in {path_str}: {e}")
            except Exception as e:
                logger.error(f"Fehler beim Laden von {path_str}: {e}")
        return {}

    def _get_cache_version_string(self) -> str:
        return datetime.date.today().isoformat()

    def _get_currency_factor(self, ticker: str) -> float:
        for suffix, rate in self.currency_rates.items():
            if ticker.endswith(suffix):
                return float(rate)
        return float(self.currency_rates.get("DEFAULT", 1.0))

    def _get_integrity_config(self) -> Dict[str, Any]:
        return {
            "dividend_window_before": int(self.config.get("integrity_dividend_window_before", 20)),
            "dividend_window_after": int(self.config.get("integrity_dividend_window_after", 7)),
            "adj_close_gap_warn_threshold": float(self.config.get("integrity_adj_close_gap_warn_threshold", 0.08)),
            "adj_close_gap_hard_threshold": float(self.config.get("integrity_adj_close_gap_hard_threshold", 0.25)),
            "dividend_multiplier_tolerance": float(self.config.get("integrity_dividend_multiplier_tolerance", 0.08)),
            "daily_return_warn_threshold": float(self.config.get("integrity_daily_return_warn_threshold", 0.35)),
            "daily_return_hard_threshold": float(self.config.get("integrity_daily_return_hard_threshold", 0.80)),
            "allow_zero_prices": False,
            "rsl_sma_window": int(self.config.get("sma_length", 130)),
            "min_history_rows_for_rsl": int(self.config.get("sma_length", 130)),
            "fallback_fraction_warn": float(self.config.get("integrity_fallback_fraction_warn", 0.10)),
            "fallback_fraction_hard": float(self.config.get("integrity_fallback_fraction_hard", 0.35)),
            "flat_run_warn": int(self.config.get("integrity_flat_run_warn", 10)),
            "flat_run_hard": int(self.config.get("integrity_flat_run_hard", 25)),
        }

    def _prepare_hist_prices(self, hist: pd.DataFrame, ticker: str) -> Tuple[pd.DataFrame, pd.Series, List[str], Dict[str, Any]]:
        """
        Baut die operative Preisbasis für RSL, SMA, Momentum und Flags.

        Rückgabe:
        - normiertes DataFrame
        - operative Preisserie (rsl_price)
        - integrity_warnings
        - integrity_diagnostics
        """
        df = hist.copy()
        df = df.sort_index()

        f = self._get_currency_factor(ticker)
        for col in ["Open", "High", "Low", "Close", "Adj Close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce") * f

        warnings: List[str] = []
        diagnostics: Dict[str, Any] = {}

        if build_rsl_price_series is not None:
            try:
                build_result = build_rsl_price_series(df, self._get_integrity_config())
                df = build_result.history.copy()
                rsl_col = build_result.rsl_price_column or "rsl_price"
                if rsl_col not in df.columns:
                    df["rsl_price"] = pd.to_numeric(df.get("Adj Close", df.get("Close")), errors="coerce")
                    rsl_col = "rsl_price"

                price_series = pd.to_numeric(df[rsl_col], errors="coerce").ffill()
                warnings = list(build_result.reasons.all_reasons()) if hasattr(build_result.reasons, "all_reasons") else []
                diagnostics = dict(getattr(build_result, "diagnostics", {}) or {})
            except Exception as e:
                logger.warning(f"RSL-Integrity-Fallback aktiv für {ticker}: {e}")
                df["rsl_price"] = pd.to_numeric(
                    df["Adj Close"] if "Adj Close" in df.columns else df["Close"],
                    errors="coerce",
                )
                price_series = df["rsl_price"].ffill()
                warnings = ["integrity_builder_error"]
                diagnostics = {"integrity_builder_error": str(e)}
        else:
            df["rsl_price"] = pd.to_numeric(
                df["Adj Close"] if "Adj Close" in df.columns else df["Close"],
                errors="coerce",
            )
            price_series = df["rsl_price"].ffill()
            warnings = ["integrity_module_missing"]
            diagnostics = {"integrity_module_missing": True}

        return df, price_series, warnings, diagnostics

    def _calculate_atr(self, hist_data: pd.DataFrame, curr_price: float) -> Tuple[float, float, float]:
        try:
            if not {"High", "Low", "Close"}.issubset(hist_data.columns):
                return 0.0, 0.0, 0.0

            high = pd.to_numeric(hist_data["High"], errors="coerce")
            low = pd.to_numeric(hist_data["Low"], errors="coerce")
            close = pd.to_numeric(hist_data["Close"], errors="coerce")

            prev_close = close.shift(1)
            tr = pd.concat(
                [
                    (high - low).abs(),
                    (high - prev_close).abs(),
                    (low - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)

            atr_period = int(self.config.get("atr_period", 14))
            atr = float(tr.rolling(atr_period).mean().iloc[-1]) if len(tr.dropna()) >= atr_period else 0.0

            atr_limit = curr_price - (float(self.config.get("atr_multiplier_limit", 1.0)) * atr)
            atr_sell_limit = curr_price + (float(self.config.get("atr_multiplier_exit", 0.15)) * atr)
            return atr, atr_limit, atr_sell_limit
        except Exception:
            return 0.0, 0.0, 0.0

    def _calculate_flags(
        self,
        hist_data: pd.DataFrame,
        price_series: pd.Series,
        curr_price: float,
        sma: float,
        is_young_history: bool,
        integrity_warnings: Optional[List[str]] = None,
        integrity_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        flags: Dict[str, Any] = {
            "flag_gap": "OK",
            "flag_liquidity": "OK",
            "flag_stale": "OK",
            "flag_scale": "OK",
            "flag_history_length": "OK",
            "history_length_reason": "",
            "scale_reason": "",
            "stale_reason": "",
            "price_scale_ratio": 1.0,
            "stale_days": 0,
            "trend_sma50": "OK",
            "trend_smoothness": 0.0,
            "trend_quality": "N/A",
            "trust_score": 0,
            "twss_score": 0.0,
            "twss_date": "",
            "twss_days_ago": 0,
            "twss_raw_pct": 0.0,
            "twss_orientation": "NIEDRIG",
            "rsl_change_1w": 0.0,
            "rsl_past": None,
            "atr": 0.0,
            "atr_limit": 0.0,
            "atr_sell_limit": 0.0,
            "high_52w": 0.0,
            "distance_52w_high_pct": None,
            "mom_12m": None,
            "mom_6m": None,
            "mom_3m": None,
            "max_drawdown_6m": 0.0,
            "mom_score": None,
            "mom_vol": None,
            "mom_score_adj": None,
            "mom_accel": None,
            "stale_days_max": 0,
            "integrity_warnings": list(integrity_warnings or []),
            "integrity_diagnostics": dict(integrity_diagnostics or {}),
        }

        hist_close = pd.to_numeric(price_series, errors="coerce").dropna()
        if len(hist_close) < 20:
            return flags

        try:
            max_jump = float(hist_close.pct_change().abs().replace([np.inf, -np.inf], np.nan).max())
        except Exception:
            max_jump = 0.0

        if max_jump >= 0.25:
            flags["flag_gap"] = "WARN"

        try:
            min_close = float(cast(Any, hist_close.min()))
            max_close = float(cast(Any, hist_close.max()))
            if min_close > 0:
                flags["price_scale_ratio"] = max_close / min_close

                warn_ratio = float(self.config.get("price_scale_warn_ratio", 8.0))
                critical_ratio = float(self.config.get("price_scale_critical_ratio", 15.0))
                warn_jump = float(self.config.get("price_scale_warn_jump", 0.8))
                critical_jump = float(self.config.get("price_scale_critical_jump", 1.5))

                if flags["price_scale_ratio"] >= critical_ratio or max_jump >= critical_jump:
                    flags["flag_scale"] = "CRITICAL"
                    flags["scale_reason"] = f"Preis-Skala kritisch (Ratio {flags['price_scale_ratio']:.1f})"
                elif flags["price_scale_ratio"] >= warn_ratio or max_jump >= warn_jump:
                    flags["flag_scale"] = "WARN"
                    flags["scale_reason"] = f"Preis-Skala auffaellig (Ratio {flags['price_scale_ratio']:.1f})"
        except Exception:
            pass

        # Trend SMA 50
        sma50_win = int(self.config.get("sma_short_length", 50))
        if len(hist_close) >= sma50_win:
            sma50 = float(hist_close.rolling(sma50_win).mean().iloc[-1])
            flags["trend_sma50"] = "OK" if curr_price >= sma50 else "WARN"

        # Stale / Flatline
        stale_window = int(self.config.get("stale_window", 60))
        recent = hist_close.tail(stale_window)
        is_zero = (recent.diff().abs() <= 1e-6).astype(int)
        max_flat_run = (is_zero * (is_zero.groupby((is_zero != is_zero.shift()).cumsum()).cumcount() + 1)).max()

        flags["stale_days"] = int(max_flat_run) if not pd.isna(max_flat_run) else 0
        flags["stale_days_max"] = flags["stale_days"]

        if flags["stale_days"] >= int(self.config.get("max_flat_days", 15)):
            flags["flag_stale"] = "WARN"
            flags["stale_reason"] = f"Geringe Liquiditaet ({flags['stale_days']} Tage flach)"

        if (hist_close <= 0).any():
            flags["flag_stale"] = "CRITICAL"
            flags["stale_reason"] = "Ungültige Daten: Null- oder Negativpreise"

        # Trend smoothness
        window_r2 = int(self.config.get("sma_length", 130))
        if len(hist_close) >= window_r2:
            subset = hist_close.tail(window_r2)
            if (subset > 0).all():
                log_prices = np.log(subset.values.astype(float))
                std_val = np.std(log_prices)
                if not np.isnan(std_val) and std_val > 1e-9:
                    with np.errstate(divide="ignore", invalid="ignore"):
                        r_matrix = np.corrcoef(np.arange(len(log_prices)), log_prices)
                        r = r_matrix[0, 1] if r_matrix.shape == (2, 2) else 0.0
                    r_squared = float(r ** 2) if not np.isnan(r) else 0.0
                else:
                    r_squared = 0.0

                flags["trend_smoothness"] = r_squared
                if r_squared > 0.85:
                    flags["trend_quality"] = "STABIL"
                elif r_squared > 0.65:
                    flags["trend_quality"] = "NORMAL"
                else:
                    flags["trend_quality"] = "WACKLIG"

        # TWSS
        if len(hist_close) >= 60:
            rets = hist_close.pct_change().dropna() * 100
            decay = np.exp(-np.arange(len(rets) - 1, -1, -1) / float(self.config.get("twss_decay_days", 60.0)))
            twss_series = rets * decay

            abs_twss = twss_series.abs()
            if not abs_twss.empty:
                max_idx = abs_twss.idxmax()
                flags["twss_score"] = float(abs_twss.loc[max_idx])
                flags["twss_date"] = str(max_idx.date())
                flags["twss_days_ago"] = (datetime.date.today() - max_idx.date()).days
                flags["twss_raw_pct"] = float(rets.loc[max_idx])

                if flags["twss_score"] > 60:
                    flags["twss_orientation"] = "HOCH"
                elif flags["twss_score"] > 25:
                    flags["twss_orientation"] = "MITTEL"

        # RSL-Verlauf / Momentum
        try:
            sma_len = int(self.config.get("sma_length", 130))
            rsl_series = (hist_close / hist_close.rolling(sma_len).mean()).replace([np.inf, -np.inf], np.nan)

            if len(rsl_series.dropna()) >= 6:
                flags["rsl_past"] = float(rsl_series.dropna().iloc[-6])
                if flags["rsl_past"] not in (None, 0):
                    flags["rsl_change_1w"] = float((rsl_series.dropna().iloc[-1] / flags["rsl_past"]) - 1.0)

            flags["mom_12m"] = _calc_momentum(hist_close, curr_price, int(self.config.get("mom_lookback_12m", 252)))
            flags["mom_6m"] = _calc_momentum(hist_close, curr_price, int(self.config.get("mom_lookback_6m", 126)))
            flags["mom_3m"] = _calc_momentum(hist_close, curr_price, int(self.config.get("mom_lookback_3m", 63)))

            vals = [flags["mom_12m"], flags["mom_6m"], flags["mom_3m"]]
            weights = [
                float(self.config.get("mom_weight_12m", 0.5)),
                float(self.config.get("mom_weight_6m", 0.3)),
                float(self.config.get("mom_weight_3m", 0.2)),
            ]

            weighted_sum = 0.0
            weight_sum = 0.0
            for v, w in zip(vals, weights):
                if v is not None:
                    weighted_sum += float(v) * w
                    weight_sum += w
            if weight_sum > 0:
                flags["mom_score"] = weighted_sum / weight_sum

            daily_ret = hist_close.pct_change().dropna()
            if len(daily_ret) >= 20:
                flags["mom_vol"] = float(daily_ret.tail(126).std()) if len(daily_ret) >= 126 else float(daily_ret.std())
                if flags["mom_score"] is not None and flags["mom_vol"] not in (None, 0):
                    flags["mom_score_adj"] = float(flags["mom_score"]) / max(float(flags["mom_vol"]), 1e-9)

            if flags["mom_3m"] is not None and flags["mom_6m"] is not None:
                flags["mom_accel"] = float(flags["mom_3m"]) - float(flags["mom_6m"])

            if len(hist_close) >= 252:
                flags["high_52w"] = float(hist_close.tail(252).max())
                if flags["high_52w"] > 0:
                    flags["distance_52w_high_pct"] = (curr_price / flags["high_52w"]) - 1.0

            if len(hist_close) >= 126:
                recent_6m = hist_close.tail(126)
                roll_max = recent_6m.cummax()
                dd = (recent_6m / roll_max) - 1.0
                flags["max_drawdown_6m"] = float(dd.min())
        except Exception:
            pass

        # ATR auf realen OHLC-Daten
        atr, atr_limit, atr_sell_limit = self._calculate_atr(hist_data, curr_price)
        flags["atr"] = atr
        flags["atr_limit"] = atr_limit
        flags["atr_sell_limit"] = atr_sell_limit

        # History length
        if is_young_history:
            flags["flag_history_length"] = "CRITICAL"
            flags["history_length_reason"] = f"Historie zu kurz (<{self.config.get('sma_length', 130)} Tage)"

        # Integrity-Warnungen zusätzlich berücksichtigen
        warns = set(flags.get("integrity_warnings", []))
        if any("hard" in w or "missing_history" in w or "missing_close" in w for w in warns):
            flags["flag_scale"] = "CRITICAL" if flags["flag_scale"] == "OK" else flags["flag_scale"]

        if any("close_fallback_used" == w for w in warns):
            if flags["flag_gap"] == "OK":
                flags["flag_gap"] = "WARN"

        # Trust Score
        t_score = 3
        if flags["flag_stale"] != "OK":
            t_score -= 1
        if flags["flag_gap"] != "OK":
            t_score -= 1
        if flags["flag_liquidity"] != "OK":
            t_score -= 1

        if flags["flag_scale"] == "CRITICAL" or flags["flag_history_length"] == "CRITICAL":
            flags["trust_score"] = 0
        else:
            if flags["flag_scale"] != "OK":
                t_score -= 1
            flags["trust_score"] = max(0, t_score)

        return flags

    def _compute_metrics_from_history(self, hist: pd.DataFrame, ticker: str) -> Optional[Tuple[float, float, float, Dict[str, Any]]]:
        if hist is None or hist.empty or len(hist) < 10:
            return None

        hist_prepared, price_series, integrity_warnings, integrity_diagnostics = self._prepare_hist_prices(hist, ticker)

        price_series = pd.to_numeric(price_series, errors="coerce").dropna()
        if price_series.empty:
            return None

        curr = float(price_series.iloc[-1])

        sma_len = int(self.config.get("sma_length", 130))
        is_young_history = False
        if len(price_series.dropna()) < sma_len:
            is_young_history = True
            self.young_tickers[ticker] = {
                "ticker": ticker,
                "count": 1,
                "top_reason": f"Historie zu kurz (<{sma_len})",
            }

        sma_series = price_series.rolling(sma_len, min_periods=sma_len).mean()
        sma = float(sma_series.iloc[-1]) if not sma_series.empty and not pd.isna(sma_series.iloc[-1]) else curr

        volume_col = "Volume" if "Volume" in hist_prepared.columns else None
        vol_eur = 0.0
        if volume_col is not None:
            try:
                vol_eur = float(pd.to_numeric(hist_prepared[volume_col], errors="coerce").ffill().tail(20).mean() * curr)
            except Exception:
                vol_eur = 0.0

        flags = self._calculate_flags(
            hist_data=hist_prepared,
            price_series=price_series,
            curr_price=curr,
            sma=sma,
            is_young_history=is_young_history,
            integrity_warnings=integrity_warnings,
            integrity_diagnostics=integrity_diagnostics,
        )

        return curr, sma, vol_eur, flags

    # ------------------------------------------------------------------
    # History API
    # ------------------------------------------------------------------

    def get_history_batch(self, tickers: List[str]) -> Dict[str, Tuple[float, float, float, Dict]]:
        version = self._get_cache_version_string()
        results: Dict[str, Tuple[float, float, float, Dict]] = {}
        to_fetch: List[str] = []

        for t in tickers:
            key = f"{t}_{version}"
            if key in self.cache:
                c = self.cache[key]
                results[t] = (c["curr"], c["sma"], c.get("vol_eur", 0.0), c["flags"])
            else:
                to_fetch.append(t)

        if not to_fetch:
            self.last_history_batch_used_network = False
            return results

        self.last_history_batch_used_network = True

        try:
            data = yf.download(
                to_fetch,
                period=self.config.get("history_period", "18mo"),
                group_by="ticker",
                auto_adjust=False,
                actions=True,
                threads=True,
                progress=False,
            )

            for t in to_fetch:
                try:
                    hist = data[t] if len(to_fetch) > 1 else data
                    if hist.empty or len(hist) < 10:
                        self.failed_tickers[t] = {
                            "ticker": t,
                            "count": 1,
                            "top_reason": "Download leer oder zu kurz",
                        }
                        continue

                    computed = self._compute_metrics_from_history(hist, t)
                    if computed is None:
                        self.failed_tickers[t] = {
                            "ticker": t,
                            "count": 1,
                            "top_reason": "Keine verwertbare Preisserie",
                        }
                        continue

                    curr, sma, vol_eur, flags = computed
                    results[t] = (curr, sma, vol_eur, flags)

                    with self.lock:
                        self.cache[f"{t}_{version}"] = {
                            "curr": curr,
                            "sma": sma,
                            "vol_eur": vol_eur,
                            "flags": flags,
                            "timestamp": time.time(),
                        }
                except Exception as inner_e:
                    logger.error(f"Fehler bei Batch-Verarbeitung {t}: {inner_e}")

        except Exception as e:
            logger.error(f"Fehler im Batch-Download: {e}")

        return results

    def get_history_single(self, ticker: str) -> Optional[Tuple[float, float, float, Dict]]:
        version = self._get_cache_version_string()
        key = f"{ticker}_{version}"

        if key in self.cache:
            c = self.cache[key]
            return (c["curr"], c["sma"], c.get("vol_eur", 0.0), c["flags"])

        try:
            hist = yf.Ticker(ticker).history(
                period=self.config.get("history_period", "18mo"),
                auto_adjust=False,
                actions=True,
            )
            if hist.empty:
                return None

            computed = self._compute_metrics_from_history(hist, ticker)
            if computed is None:
                return None

            curr, sma, vol_eur, flags = computed

            with self.lock:
                self.cache[key] = {
                    "curr": curr,
                    "sma": sma,
                    "vol_eur": vol_eur,
                    "flags": flags,
                    "timestamp": time.time(),
                }

            return (curr, sma, vol_eur, flags)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Info API
    # ------------------------------------------------------------------

    @retry_decorator
    def fetch_and_cache_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        if ticker in self.info_cache:
            return self.info_cache[ticker]

        info = yf.Ticker(ticker).info
        if info:
            clean_info = {
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "country": info.get("country", "Unknown"),
                "longName": info.get("longName", ticker),
                "marketCap": info.get("marketCap", 0),
                "cached_at": datetime.datetime.now().isoformat(),
            }
            with self.lock:
                self.info_cache[ticker] = clean_info
            return clean_info

        return None

    def get_cached_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        return self.info_cache.get(ticker)


# ============================================================================
# FIRST SEEN
# ============================================================================

class FirstSeenManager:
    def __init__(self, path: str):
        self.path = path
        self.data = self._load()

    def _load(self) -> Dict[str, str]:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save(self) -> None:
        folder = os.path.dirname(self.path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)

    def get_date_info(self, ticker: str) -> Tuple[str, bool]:
        today = datetime.date.today().isoformat()
        if ticker not in self.data:
            self.data[ticker] = today
            return today, True

        fs_date = self.data[ticker]
        diff = (datetime.date.today() - datetime.date.fromisoformat(fs_date)).days
        return fs_date, (diff <= 7)


# ============================================================================
# PORTFOLIO
# ============================================================================

class PortfolioManager:
    def __init__(self, path: str):
        self.path = path
        self.current_portfolio = self._load()

    def _load(self) -> List[Dict]:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        return []

    def is_in_depot(self, yahoo_symbol: str) -> bool:
        symbol = str(yahoo_symbol or "").strip().upper()
        return any(
            str(p.get("Yahoo_Symbol", "")).strip().upper() == symbol
            for p in self.current_portfolio
        )

    def save(self, portfolio: List[Dict]) -> None:
        portfolio_dir = os.path.dirname(self.path)
        if portfolio_dir:
            os.makedirs(portfolio_dir, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(portfolio, f, indent=2, ensure_ascii=False)
        self.current_portfolio = portfolio
