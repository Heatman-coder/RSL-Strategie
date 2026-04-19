#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_manager.py
Zentrale Verwaltung für Marktdaten, Caching und technische Indikatoren.
Optimiert für Yahoo Finance Batch-Abfragen und Datenqualitätsprüfung.

BERECHNUNGSPHILOSOPHIE:
1. Kennzahlen wie RSL (Relative Stärke) sind Verhältnisse (Kurs/SMA) und somit robust gegenüber Währungsfehlern.
2. Ein Ticker wird NIEMALS aufgrund von Grenzwerten (zu hoch/niedrig) gelöscht. 
3. Bei extremen Werten wird die Datenbasis (Scale, Gaps) geprüft, aber das Objekt bleibt für den Benutzer sichtbar.
"""
import os
import json
import time
import logging
import datetime
import pandas as pd
import numpy as np
import yfinance as yf
import concurrent.futures
from typing import Dict, Any, Optional, List, Tuple, Union, cast
from threading import Lock
from dataclasses import dataclass, asdict, field
from core import rsl_integrity as rsl_integrity_core
from core import final_support as support

logger = logging.getLogger(__name__)

# --- DATENSTRUKTUR ---
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
    ulcer_index_6m: float = 0.0
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

    # RSL INTEGRITY / RANKING
    integrity_warnings: List[str] = field(default_factory=list)
    drop_reasons: List[str] = field(default_factory=list)
    hard_fail_reasons: List[str] = field(default_factory=list)
    warning_reasons: List[str] = field(default_factory=list)
    review_reasons: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    excluded_from_ranking: bool = False
    ranking_exclude_reason: str = ""
    rsl_eligible: bool = True
    ranking_integrity_status: str = "eligible_original"
    used_close_fallback: bool = False
    rsl_price_source: str = "adj_close"
    fallback_fraction: float = 0.0
    repair_applied: bool = False
    repair_method: str = ""
    repair_reason: str = ""
    is_threshold_line: bool = False

    def to_dict(self):
        # Manuelle Konvertierung ist ca. 15x schneller als dataclasses.asdict
        return {attr: getattr(self, attr) for attr in self.__slots__}

# --- RATE LIMITING STATE ---
INFO_RATE_LIMIT_STATE = {
    'rate_limit_hits': 0,
    'extra_delay_s': 0.0,
    'last_hit_time': 0.0
}

def _consume_rate_limit_hits() -> int:
    hits = int(cast(int, INFO_RATE_LIMIT_STATE['rate_limit_hits']))
    INFO_RATE_LIMIT_STATE['rate_limit_hits'] = 0
    return hits

def retry_decorator(func):
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "Too Many Requests" in str(e):
                    INFO_RATE_LIMIT_STATE['rate_limit_hits'] += 1
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Rate Limit (429). Warte {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == max_retries - 1: raise e
                    time.sleep(2)
        return None
    return wrapper

# --- MANAGER KLASSEN ---

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
            "max_flat_days": 7,
            "max_consecutive_flat": 20,
            "max_std_rel": 0.005,
            "min_total_range": 0.02,
            "max_total_return": 10.0,
            "price_scale_warn_ratio": 25.0,
            "price_scale_critical_ratio": 50.0,
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
            "twss_decay_days": 60.0,
        }

        merged_config = dict(default_config)
        merged_config.update(config)

        self.config: Dict[str, Any] = merged_config
        self.currency_rates = dict(currency_rates)
        self.lock = Lock()
        self.cache: Dict[str, Any] = {}
        self.info_cache = self._load_json(str(self.config.get('ticker_info_cache_file', 'ticker_info_cache.json')))
        self.failed_tickers: Dict[str, Any] = {}
        self.young_tickers: Dict[str, Any] = {}
        self.last_history_batch_used_network = False
        
        # Cache für Historien laden
        h_file = self.config.get('history_cache_file')
        if h_file and os.path.exists(h_file):
            try:
                with open(h_file, 'r') as f:
                    data = json.load(f)
                    self.cache = data.get('data', {})
            except: pass

    def save_history_cache(self):
        """Speichert den aktuellen Kursdaten-Cache in die JSON-Datei."""
        path = self.config.get('history_cache_file')
        if not path: return
        try:
            version = self._get_cache_version_string()
            with self.lock:
                cache_snapshot = dict(self.cache)
            payload = {'version': version, 'data': cache_snapshot}
            with open(path, 'w') as f:
                json.dump(payload, f)
            logger.info(f"Kursdaten-Cache gespeichert ({len(cache_snapshot)} Einträge).")
        except Exception as e:
            logger.error(f"Fehler beim Speichern des History-Cache: {e}")

    def get_failed_records(self) -> List[Dict]:
        return list(self.failed_tickers.values())

    def get_young_records(self) -> List[Dict]:
        return list(self.young_tickers.values())

    def _load_json(self, path: Any) -> Dict[str, Any]:
        path_str = str(path) if path else ""
        if path_str and os.path.exists(path_str):
            try:
                with open(path_str, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data if isinstance(data, dict) else {}
            except json.JSONDecodeError as e:
                logger.error(f"JSON-Formatfehler in {path_str}: {e}")
            except Exception as e:
                logger.error(f"Fehler beim Laden von {path_str}: {e}")
        return {}

    def save_info_cache(self):
        path = self.config.get('ticker_info_cache_file', 'ticker_info_cache.json')
        try:
            info_cache_dir = os.path.dirname(path)
            if info_cache_dir:
                os.makedirs(info_cache_dir, exist_ok=True)
            # Atomares Schreiben: Erst in .tmp Datei, dann umbenennen
            tmp_path = f"{path}.tmp"
            with self.lock:
                info_snapshot = dict(self.info_cache)
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(info_snapshot, f)
            os.replace(tmp_path, path)
            logger.debug(f"Info-Cache erfolgreich gespeichert: {path}")
        except Exception as e:
            logger.error(f"Konnte Info-Cache nicht speichern ({path}): {e}")

    def clear_cache(self) -> None:
        """
        Löscht ausschließlich den Kursdaten-Cache (Historie) von der Festplatte und aus dem Speicher.
        
        SICHERHEITSHINWEIS: Andere Cache-Dateien (z.B. ticker_info_cache.json) dürfen hier NIEMALS 
        automatisch gelöscht werden, da sie wertvolle Metadaten enthalten.
        """
        self.cache = {}
        path = cast(Optional[str], self.config.get('history_cache_file'))
        if path and os.path.exists(str(path)):
            try:
                os.remove(str(path))
                logger.info(f"Kursdaten-Cache gelöscht: {path}")
            except Exception as e:
                logger.warning(f"Konnte Cache {path} nicht löschen: {e}")

    def _get_cache_version_string(self) -> str:
        return datetime.date.today().isoformat()

    def _get_currency_factor(self, ticker: str, info_currency: Optional[str] = None) -> float:
        # Prio 1: Nutze die von Yahoo gemeldete Waehrung (am sichersten)
        if info_currency:
            c_map = {
                "USD": "DEFAULT", "EUR": ".DE", "JPY": ".T", "GBP": ".L", "GBp": ".L",
                "HKD": ".HK", "CAD": ".TO", "AUD": ".AX", "CHF": ".SW", "SEK": ".ST",
                "NOK": ".OL", "SAR": ".SR", "TWD": ".TW", "KRW": ".KS", "BRL": ".SA"
            }
            c_key = c_map.get(info_currency)
            # Falls info_currency unbekannt, versuche es direkt als Suffix-Key
            if not c_key and f".{info_currency}" in self.currency_rates:
                c_key = f".{info_currency}"
                
            if c_key and c_key in self.currency_rates:
                # Sonderfall Pence (GBp) -> durch 100 teilen
                factor = self.currency_rates[c_key]
                return factor / 100.0 if info_currency == "GBp" else factor

        # Prio 2: Fallback auf Ticker-Suffix
        _, sep, suffix = ticker.rpartition('.')
        if sep:
            full_suffix = f".{suffix}"
            if full_suffix in self.currency_rates:
                return self.currency_rates[full_suffix]
        return self.currency_rates.get("DEFAULT", 1.0)

    def _calculate_flags(self, hist_data: pd.DataFrame, curr_price: float, sma: float, is_young_history: bool = False, price_series: Optional[pd.Series] = None) -> Dict[str, Any]:
        flags: Dict[str, Any] = {
            'flag_gap': "OK", 'flag_liquidity': "OK", 'flag_stale': "OK", 'flag_scale': "OK",
            'scale_reason': "", 'price_scale_ratio': 1.0, 'stale_days': 0, 'trend_sma50': "OK",
            'trend_smoothness': 0.0, 'trend_quality': "N/A", 'trust_score': 0,
            'twss_score': 0.0, 'twss_date': "", 'twss_days_ago': 0, 'twss_raw_pct': 0.0, 'twss_orientation': "NIEDRIG",
            'rsl_change_1w': 0.0, 'rsl_past': None, 'atr': 0.0, 'atr_limit': 0.0, 'atr_sell_limit': 0.0,
            'high_52w': 0.0, 'distance_52w_high_pct': None, 'mom_12m': None, 'mom_6m': None,
            'mom_3m': None, 'max_drawdown_6m': 0.0, 'mom_score': None, 'mom_vol': None,
            'flag_history_length': "OK",
            'history_length_reason': "",
            'mom_score_adj': None, 'mom_accel': None,
            'stale_days_max': 0
        }
        
        # Nutze die reparierte Serie, falls vorhanden, sonst Fallback auf Spalten
        if price_series is not None:
            hist_close = price_series.ffill()
        else:
            calc_col = 'Adj Close' if 'Adj Close' in hist_data.columns else 'Close'
            if calc_col not in hist_data.columns or hist_data.empty: return flags
            hist_close = hist_data[calc_col].dropna()

        if len(hist_close) < 20: return flags

        # Checks auf das SMA-Fenster begrenzen (ca. 130 Handelstage)
        recent_prices = hist_close.tail(130)

        try:
            max_jump = float(recent_prices.pct_change().abs().replace([np.inf, -np.inf], np.nan).max())
        except Exception:
            max_jump = 0.0
        if max_jump >= 0.25:
            flags['flag_gap'] = "WARN"

        try:
            min_close = float(cast(Any, recent_prices.min()))
            max_close = float(cast(Any, recent_prices.max()))
            if min_close > 0:
                flags['price_scale_ratio'] = max_close / min_close
                warn_ratio = float(self.config.get('price_scale_warn_ratio', 25.0))
                critical_ratio = float(self.config.get('price_scale_critical_ratio', 50.0))
                warn_jump = float(self.config.get('price_scale_warn_jump', 0.8))
                critical_jump = float(self.config.get('price_scale_critical_jump', 1.5))

                if flags['price_scale_ratio'] >= critical_ratio or max_jump >= critical_jump:
                    flags['flag_scale'] = "CRITICAL"
                    flags['scale_reason'] = f"Preis-Skala kritisch (Ratio {flags['price_scale_ratio']:.1f})"
                elif flags['price_scale_ratio'] >= warn_ratio or max_jump >= warn_jump:
                    flags['flag_scale'] = "WARN"
                    flags['scale_reason'] = f"Preis-Skala auffaellig (Ratio {flags['price_scale_ratio']:.1f})"
        except Exception:
            pass

        # 1. SMA 50 Trend
        sma50_win = int(self.config.get('sma_short_length', 50))
        if len(hist_close) >= sma50_win:
            sma50 = float(hist_close.rolling(sma50_win).mean().iloc[-1])
            flags['trend_sma50'] = "OK" if curr_price >= sma50 else "WARN"

        # 2. Stale / Flatline Check
        stale_window = int(self.config.get('stale_window', 60))
        recent = hist_close.tail(stale_window)
        is_zero = (recent.diff().abs() <= 1e-6).astype(int)
        max_flat_run = (is_zero * (is_zero.groupby((is_zero != is_zero.shift()).cumsum()).cumcount() + 1)).max()
        flags['stale_days'] = int(max_flat_run) if not pd.isna(max_flat_run) else 0
        flags['stale_days_max'] = flags['stale_days']

        # Inaktivität ist in Frankfurt normal. Markieren, aber nicht blockieren.
        if flags['stale_days'] >= int(self.config.get('max_flat_days', 25)):
            flags['flag_stale'] = "WARN"
            flags['stale_reason'] = f"Geringe Liquiditaet ({flags['stale_days']} Tage flach)"

        if (hist_close <= 0).any():
            flags['flag_stale'] = "CRITICAL"
            flags['stale_reason'] = "Ungültige Daten: Null- oder Negativpreise"

        # 3. Trend Smoothness & Direction (Linear Regression)
        window_r2 = 130
        if len(hist_close) >= window_r2:
            subset = hist_close.tail(window_r2)
            if (subset > 0).all():
                y = np.log(subset.values.astype(float))
                x = np.arange(len(y))
                
                # Regression rechnen: y = alpha + beta*x
                A = np.column_stack([np.ones(len(x)), x])
                beta, _, _, _ = np.linalg.lstsq(A, y, rcond=None)
                slope, r2 = float(beta[1]), 0.0

                # Bestimmtheitsmaß berechnen
                if np.std(y) > 1e-9:
                    r_matrix = np.corrcoef(x, y)
                    r2 = float(r_matrix[0, 1]**2)
                
                flags['trend_smoothness'] = r2
                # Nur positive Steigung ist qualitativ hochwertig
                if slope > 0:
                    if r2 > 0.85: flags['trend_quality'] = "STABIL"
                    elif r2 > 0.65: flags['trend_quality'] = "NORMAL"
                    else: flags['trend_quality'] = "WACKLIG"
                else:
                    flags['trend_quality'] = "FALLEND"

        # 4. TWSS (Spike Detection) - JETZT VOL-ADJUSTIERT
        # Misst abnormale Spikes im Verhältnis zur 63-Tage Volatilität
        if len(hist_close) >= 63:
            std_63 = hist_close.pct_change().tail(63).std()
            rets = hist_close.pct_change().dropna()
            
            # Normalisiere Renditen durch Volatilität (Z-Rendite)
            z_rets = rets / (std_63 if std_63 > 0 else 0.01)
            
            decay = np.exp(-np.arange(len(rets)-1, -1, -1) / float(self.config.get('twss_decay_days', 60.0)))
            twss_series = z_rets * decay * 10 # Skalierung für Lesbarkeit

            if not twss_series.empty:
                # Finde den Index des betragsmäßig größten Wertes, behalte aber das Vorzeichen
                max_idx = twss_series.abs().idxmax()
                flags['twss_score'] = float(twss_series[max_idx])
                idx_date = max_idx.date() if hasattr(max_idx, 'date') else None
                flags['twss_date'] = str(idx_date) if idx_date is not None else str(max_idx)
                flags['twss_days_ago'] = (datetime.date.today() - idx_date).days if idx_date is not None else 0
                flags['twss_raw_pct'] = float(rets[max_idx]) / 100.0
                if flags['twss_score'] > 60: flags['twss_orientation'] = "HOCH"
                elif flags['twss_score'] > 25: flags['twss_orientation'] = "MITTEL"

        # 5. Momentum - 12M ex 1M (Institutional Standard)
        # Wir nehmen den Preis von vor 21 Tagen als heutigen Anker für 12M
        if len(hist_close) >= 252:
            price_anchor_12m = float(hist_close.iloc[-21])
            denom = float(hist_close.iloc[-252])
            flags['mom_12m'] = (price_anchor_12m / denom) - 1.0 if denom > 0 else None
        else:
            flags['mom_12m'] = support.calc_momentum(hist_close, curr_price, 252)
            
        flags['mom_6m'] = support.calc_momentum(hist_close, curr_price, int(self.config.get('mom_lookback_6m', 126)))
        flags['mom_3m'] = support.calc_momentum(hist_close, curr_price, int(self.config.get('mom_lookback_3m', 63)))

        # 6. Risikomaße: Drawdown & Ulcer Index
        try:
            window_6m = hist_close.tail(126)
            rolling_max = window_6m.cummax().replace(0, np.nan)
            drawdowns = (window_6m / rolling_max).fillna(1.0) - 1.0
            flags['max_drawdown_6m'] = abs(float(drawdowns.min()))
            
            # Ulcer Index = Quadratwurzel des Durchschnitts der quadrierten Drawdowns
            # Bestraft tiefe und lange Rücksetzer überproportional
            flags['ulcer_index_6m'] = float(np.sqrt(np.mean(np.square(drawdowns * 100))))
        except Exception:
            flags['ulcer_index_6m'] = 0.0
        try:
            if len(hist_close) >= 6 and sma > 0:
                past_close = float(hist_close.iloc[-6])
                if len(hist_close) >= int(self.config.get('sma_length', 130)) + 5:
                    past_sma = float(hist_close.iloc[:-5].rolling(int(self.config.get('sma_length', 130))).mean().iloc[-1])
                    if past_sma > 0:
                        flags['rsl_past'] = past_close / past_sma
                        flags['rsl_change_1w'] = (curr_price / sma) - flags['rsl_past']
            flags['high_52w'] = float(hist_close.tail(252).max())
            if flags['high_52w'] > 0:
                flags['distance_52w_high_pct'] = max(0.0, (flags['high_52w'] - curr_price) / flags['high_52w'])
        except Exception:
            pass
        
        if flags['mom_12m'] is not None and flags['mom_6m'] is not None:
            w12, w6, w3 = self.config.get('mom_weight_12m', 0.5), self.config.get('mom_weight_6m', 0.3), self.config.get('mom_weight_3m', 0.2)
            m12, m6, m3 = flags['mom_12m'], flags['mom_6m'], (flags['mom_3m'] or 0.0)
            flags['mom_score'] = (m12 * w12) + (m6 * w6) + (m3 * w3)
            flags['mom_accel'] = m6 - m12
            try:
                daily_returns = hist_close.pct_change().dropna()
                vol = float(daily_returns.tail(int(self.config.get('mom_lookback_3m', 63))).std(ddof=0) * np.sqrt(252))
                flags['mom_vol'] = vol
                if vol > 0:
                    flags['mom_score_adj'] = flags['mom_score'] / vol
            except Exception:
                pass
            try:
                rolling_max = hist_close.tail(int(self.config.get('mom_lookback_6m', 126))).cummax()
                drawdown = ((hist_close.tail(int(self.config.get('mom_lookback_6m', 126))) / rolling_max) - 1.0).min()
                flags['max_drawdown_6m'] = abs(float(drawdown)) if not pd.isna(drawdown) else 0.0
            except Exception:
                pass

        # 6. ATR & Timing
        if 'High' in hist_data.columns and 'Low' in hist_data.columns:
            try:
                # Index-Alignment sicherstellen und Lücken füllen
                h, l, c = hist_data['High'].ffill(), hist_data['Low'].ffill(), hist_close.ffill()
                tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
                atr_series = tr.rolling(int(self.config.get('atr_period', 14)), min_periods=1).mean().ffill()
                atr = float(atr_series.dropna().iloc[-1]) if not atr_series.dropna().empty else 0.0
                
                flags['atr'] = atr
                flags['atr_limit'] = curr_price - (float(self.config.get('atr_multiplier_limit', 1.0) or 1.0) * atr)
                flags['atr_sell_limit'] = curr_price + (float(self.config.get('atr_multiplier_exit', 0.15)) * atr)
            except: pass

        # 8. History Length Check (130/130 rule)
        if is_young_history:
            flags['flag_history_length'] = "CRITICAL"
            flags['history_length_reason'] = f"Historie zu kurz (<{self.config.get('sma_length', 130)} Tage)"

        # 7. Trust Score
        t_score = 3
        if flags['flag_stale'] != "OK": t_score -= 1
        if flags['flag_gap'] != "OK": t_score -= 1
        if flags['flag_liquidity'] != "OK": t_score -= 1
        
        if flags['flag_scale'] == "CRITICAL" or flags['flag_history_length'] == "CRITICAL":
            flags['trust_score'] = 0
        else:
            if flags['flag_scale'] != "OK":
                t_score -= 1
            flags['trust_score'] = max(0, t_score)

        return flags

    def get_history_batch(self, tickers: List[str]) -> Dict[str, Tuple[float, float, float, Dict]]:
        version = self._get_cache_version_string()
        results = {}
        to_fetch = []
        
        sma_len = int(self.config.get('sma_length', 130))
        for t in tickers:
            key = f"{t}_{version}"
            if key in self.cache:
                c = self.cache[key]
                results[t] = (c['curr'], c['sma'], c.get('vol_eur', 0.0), c['flags'])
            else:
                to_fetch.append(t)
        
        if not to_fetch:
            self.last_history_batch_used_network = False
            return results

        self.last_history_batch_used_network = True
        try:
            # Batch-Download bleibt wie gehabt
            data = yf.download(to_fetch, period=self.config.get('history_period', '18mo'), group_by='ticker', auto_adjust=False, threads=True, progress=False)
            
            def _process_single(t):
                hist = data[t] if len(to_fetch) > 1 else data # type: ignore
                if hist.empty or len(hist) < 10:
                    self.failed_tickers[t] = {'ticker': t, 'count': 1, 'top_reason': 'Download leer oder zu kurz'}
                    return None
                
                info = self.info_cache.get(t, {})
                f = self._get_currency_factor(t, info_currency=info.get('currency'))
                hist_adj = hist.copy()
                for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                    if col in hist_adj.columns: hist_adj[col] *= f
                
                sma_len = int(self.config.get('sma_length', 130))

                # --- INTEGRITAETS-PRUEFUNG (Core Logik) ---
                core_cfg = {**self.config, "rsl_sma_window": sma_len, "min_history_rows_for_rsl": sma_len}
                analysis = rsl_integrity_core.analyze_history_for_rsl_integrity(hist_adj, ticker=t, cfg=core_cfg)
                
                repaired_df = analysis['history']
                clean_col = analysis['rsl_price_column']
                clean_series = repaired_df[clean_col].ffill()
                
                curr = float(clean_series.iloc[-1]) if not clean_series.empty else 0.0
                sma = analysis.get('rsl_sma', curr)
                rsl = analysis.get('rsl_value', 0.0)
                
                is_young_history = len(clean_series.dropna()) < sma_len
                if is_young_history:
                    self.young_tickers[t] = {'ticker': t, 'count': 1, 'top_reason': f'Historie zu kurz (<{sma_len})'}

                vol_eur = float(hist['Volume'].ffill().tail(20).mean() * curr) if 'Volume' in hist.columns else 0.0
                
                flags = self._calculate_flags(repaired_df, curr, sma, is_young_history, price_series=clean_series)

                # Marktkapitalisierung und Marktwert (EUR) berechnen
                info = self.info_cache.get(t, {})
                mkt_cap_raw = float(info.get('marketCap', 0) or 0)
                mkt_val_eur = mkt_cap_raw * f

                flags.update({
                    'rsl': rsl,
                    'rsl_past': analysis.get('rsl_past', 0.0),
                    'rsl_change_1w': analysis.get('rsl_change_1w', 0.0),
                    'integrity_reasons': analysis.get('integrity_reasons', []),
                    'used_close_fallback': bool(analysis.get('used_close_fallback', False)),
                    'excluded_from_ranking': analysis.get('excluded_from_ranking', False),
                    'ranking_integrity_status': analysis.get('ranking_integrity_status', 'eligible_original'),
                    'rsl_price_source': (analysis.get('diagnostics', {}) or {}).get('rsl_price_source_mode', 'adj_close'),
                    'fallback_fraction': float((analysis.get('diagnostics', {}) or {}).get('fallback_fraction', 0.0) or 0.0),
                    'market_cap': mkt_cap_raw,
                    'market_value': mkt_val_eur
                })
                return t, (curr, sma, vol_eur, flags)

            # Parallelisierung der CPU-lastigen Analyse-Logik
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('max_workers', 4)) as executor:
                process_futures = [executor.submit(_process_single, t) for t in to_fetch]
                for future in concurrent.futures.as_completed(process_futures):
                    res = future.result()
                    if res:
                        ticker_sym, val_tuple = res
                        results[ticker_sym] = val_tuple
                        with self.lock:
                            self.cache[f"{ticker_sym}_{version}"] = {'curr': val_tuple[0], 'sma': val_tuple[1], 'vol_eur': val_tuple[2], 'flags': val_tuple[3], 'timestamp': time.time()}

        except Exception as e:
            logger.error(f"Fehler im Batch-Download: {e}")
            
        return results

    def get_history_single(self, ticker: str) -> Optional[Tuple[float, float, float, Dict]]:
        version = self._get_cache_version_string()
        key = f"{ticker}_{version}"
        if key in self.cache:
            c = self.cache[key]
            return (c['curr'], c['sma'], c.get('vol_eur', 0.0), c['flags'])
        
        try:
            hist = yf.Ticker(ticker).history(period=self.config.get('history_period', '18mo'), auto_adjust=False)
            if hist.empty: return None
            info = self.get_cached_info(ticker) or {}
            f = self._get_currency_factor(ticker, info_currency=info.get('currency'))
            hist_adj = hist.copy()
            for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                if col in hist_adj.columns: hist_adj[col] *= f
            
            sma_len = int(self.config.get('sma_length', 130))
            core_cfg = {**self.config, "rsl_sma_window": sma_len, "min_history_rows_for_rsl": sma_len}
            analysis = rsl_integrity_core.analyze_history_for_rsl_integrity(hist_adj, ticker=ticker, cfg=core_cfg)
            
            repaired_df = analysis['history']
            clean_col = analysis['rsl_price_column']
            clean_series = repaired_df[clean_col].ffill()
            
            curr = float(clean_series.iloc[-1]) if not clean_series.empty else 0.0
            sma = analysis.get('rsl_sma', curr)
            rsl = analysis.get('rsl_value', 0.0)
            is_young_history = len(clean_series.dropna()) < sma_len
            if is_young_history:
                self.young_tickers[ticker] = {'ticker': ticker, 'count': 1, 'top_reason': f'Historie zu kurz (<{sma_len})'}

            vol_eur = float(hist['Volume'].ffill().tail(20).mean() * curr) if 'Volume' in hist.columns else 0.0
            flags = self._calculate_flags(repaired_df, curr, sma, is_young_history, price_series=clean_series)
            
            info = self.info_cache.get(ticker, {})
            mkt_cap_raw = float(info.get('marketCap', 0) or 0)
            mkt_val_eur = mkt_cap_raw * f
            
            flags.update({
                'rsl': rsl,
                'rsl_past': analysis.get('rsl_past', 0.0),
                'rsl_change_1w': analysis.get('rsl_change_1w', 0.0),
                'integrity_reasons': analysis.get('integrity_reasons', []),
                'used_close_fallback': bool(analysis.get('used_close_fallback', False)),
                'excluded_from_ranking': analysis.get('excluded_from_ranking', False),
                'ranking_integrity_status': analysis.get('ranking_integrity_status', 'eligible_original'),
                'rsl_price_source': (analysis.get('diagnostics', {}) or {}).get('rsl_price_source_mode', 'adj_close'),
                'fallback_fraction': float((analysis.get('diagnostics', {}) or {}).get('fallback_fraction', 0.0) or 0.0),
                'market_cap': mkt_cap_raw,
                'market_value': mkt_val_eur
            })
            
            with self.lock:
                self.cache[key] = {'curr': curr, 'sma': sma, 'vol_eur': vol_eur, 'flags': flags, 'timestamp': time.time()}
            return (curr, sma, vol_eur, flags)
        except: return None

    @retry_decorator
    def fetch_and_cache_info(self, ticker: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        # Cache nur nutzen, wenn kein Force-Refresh angefordert wurde
        # UND wenn die Marktkapitalisierung im Cache valide (> 0) ist.
        if not force_refresh and ticker in self.info_cache:
            cached = self.info_cache[ticker]
            if float(cached.get('marketCap', 0) or 0) > 0:
                return cached

        # Präventiver Delay vor dem Request, um Rate Limits zu vermeiden
        delay = float(self.config.get('info_fetch_delay_s', 2.0))
        if delay > 0:
            time.sleep(delay)

        info = yf.Ticker(ticker).info
        if info:
            # ISIN Validierung: Verhindert, dass 'nan' oder '0' als ISIN gecached werden
            raw_isin = str(info.get('isin', '')).strip().upper()
            valid_isin = ""
            if raw_isin and len(raw_isin) > 5 and raw_isin not in ("NAN", "NONE", "NULL", "0"):
                valid_isin = raw_isin

            # Market Cap Validierung: 0 oder negative Werte als 0 cachen
            mkt_cap = info.get('marketCap', 0)
            if (not isinstance(mkt_cap, (int, float)) or mkt_cap <= 0):
                # Fallback: sharesOutstanding * price
                shares = info.get('sharesOutstanding', 0)
                price = info.get('currentPrice') or info.get('regularMarketPreviousClose') or info.get('previousClose') or 0
                if shares and price:
                    mkt_cap = float(shares) * float(price)
                else:
                    mkt_cap = 0

            clean_info = {
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'country': info.get('country', 'Unknown'),
                'longName': info.get('longName', ticker),
                'isin': valid_isin,
                'marketCap': mkt_cap,
                'cached_at': datetime.datetime.now().isoformat(),
                'source_ticker': ticker,
                'copied_from_primary': False
            }
            with self.lock:
                self.info_cache[ticker] = clean_info
            return clean_info
        return None

    def get_cached_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        return self.info_cache.get(ticker)

class FirstSeenManager:
    def __init__(self, path: str):
        self.path = path
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as f: return json.load(f)
        return {}

    def save(self):
        with open(self.path, 'w') as f: json.dump(self.data, f, indent=2)

    def get_date_info(self, ticker: str) -> Tuple[str, bool]:
        today = datetime.date.today().isoformat()
        if ticker not in self.data:
            self.data[ticker] = today
            return today, True
        fs_date = self.data[ticker]
        diff = (datetime.date.today() - datetime.date.fromisoformat(fs_date)).days
        return fs_date, (diff <= 7)

class PortfolioManager:
    def __init__(self, path: str):
        self.path = path
        self.current_portfolio = self._load()

    def _load(self) -> List[Dict]:
        if os.path.exists(self.path):
            with open(self.path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []

    def is_in_depot(self, yahoo_symbol: str) -> bool:
        symbol = str(yahoo_symbol or "").strip().upper()
        return any(str(p.get('Yahoo_Symbol', '')).strip().upper() == symbol for p in self.current_portfolio)

    def save(self, portfolio: List[Dict]) -> None:
        portfolio_dir = os.path.dirname(self.path)
        if portfolio_dir:
            os.makedirs(portfolio_dir, exist_ok=True)
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(portfolio, f, indent=2, ensure_ascii=False)
