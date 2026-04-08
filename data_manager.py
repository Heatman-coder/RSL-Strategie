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
from typing import Dict, Any, Optional, List, Tuple, Union, cast
from threading import Lock
from dataclasses import dataclass, asdict, field
from core import rsl_integrity as rsl_integrity_core

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
    excluded_from_ranking: bool = False
    ranking_exclude_reason: str = ""
    rsl_eligible: bool = True
    ranking_integrity_status: str = "eligible_original"
    used_close_fallback: bool = False
    rsl_price_source: str = "adj_close"
    fallback_fraction: Optional[float] = None
    repair_applied: bool = False
    repair_method: str = ""
    repair_reason: str = ""

    def to_dict(self):
        return asdict(self)

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
                    wait = 30 * (attempt + 1)
                    logger.warning(f"Rate Limit (429). Warte {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == max_retries - 1: raise e
                    time.sleep(2)
        return None
    return wrapper

def _calc_momentum(series: pd.Series, curr_price: float, lookback: int) -> Optional[float]:
    if len(series) < lookback: return None
    past_price = float(series.iloc[-lookback])
    if past_price <= 0: return None
    return (curr_price / past_price) - 1.0

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
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.info_cache, f, indent=2)
            logger.debug(f"Info-Cache erfolgreich gespeichert: {path}")
        except Exception as e:
            logger.error(f"Konnte Info-Cache nicht speichern ({path}): {e}")

    def clear_cache(self) -> None:
        self.cache = {}
        h_file = cast(Optional[str], self.config.get('history_cache_file'))
        if h_file and os.path.exists(str(h_file)):
            os.remove(str(h_file))

    def _get_cache_version_string(self) -> str:
        return datetime.date.today().isoformat()

    def _get_currency_factor(self, ticker: str) -> float:
        for suffix, rate in self.currency_rates.items():
            if ticker.endswith(suffix): return rate
        return self.currency_rates.get("DEFAULT", 1.0)

    def _calculate_flags(self, hist_data: pd.DataFrame, curr_price: float, sma: float, is_young_history: bool, price_series: Optional[pd.Series] = None) -> Dict[str, Any]:
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
            hist_close = price_series.dropna()
        else:
            calc_col = 'Adj Close' if 'Adj Close' in hist_data.columns else 'Close'
            if calc_col not in hist_data.columns or hist_data.empty: return flags
            hist_close = hist_data[calc_col].dropna()

        if len(hist_close) < 20: return flags

        try:
            max_jump = float(hist_close.pct_change().abs().replace([np.inf, -np.inf], np.nan).max())
        except Exception:
            max_jump = 0.0
        if max_jump >= 0.25:
            flags['flag_gap'] = "WARN"

        try:
            min_close = float(cast(Any, hist_close.min()))
            max_close = float(cast(Any, hist_close.max()))
            if min_close > 0:
                flags['price_scale_ratio'] = max_close / min_close
                warn_ratio = float(self.config.get('price_scale_warn_ratio', 8.0))
                critical_ratio = float(self.config.get('price_scale_critical_ratio', 15.0))
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
        if flags['stale_days'] >= int(self.config.get('max_flat_days', 15)):
            flags['flag_stale'] = "WARN"
            flags['stale_reason'] = f"Geringe Liquiditaet ({flags['stale_days']} Tage flach)"

        if (hist_close <= 0).any():
            flags['flag_stale'] = "CRITICAL"
            flags['stale_reason'] = "Ungültige Daten: Null- oder Negativpreise"

        # 3. R2 Trend Smoothness
        window_r2 = 130
        if len(hist_close) >= window_r2:
            subset = hist_close.tail(window_r2)
            if (subset > 0).all():
                log_prices = np.log(subset.values.astype(float))
                # Robuster Check auf Varianz (vermeidet RuntimeWarning bei Division durch Null)
                std_val = np.std(log_prices)
                if not np.isnan(std_val) and std_val > 1e-9:
                    with np.errstate(divide='ignore', invalid='ignore'):
                        r_matrix = np.corrcoef(np.arange(len(log_prices)), log_prices)
                        r = r_matrix[0, 1] if r_matrix.shape == (2, 2) else 0.0
                        r_squared = float(r**2) if not np.isnan(r) else 0.0
                else:
                    r_squared = 0.0
                flags['trend_smoothness'] = r_squared
                if r_squared > 0.85: flags['trend_quality'] = "STABIL"
                elif r_squared > 0.65: flags['trend_quality'] = "NORMAL"
                else: flags['trend_quality'] = "WACKLIG"

        # 4. TWSS (Spike Detection)
        if len(hist_close) >= 60:
            rets = hist_close.pct_change().dropna() * 100
            decay = np.exp(-np.arange(len(rets)-1, -1, -1) / float(self.config.get('twss_decay_days', 60.0)))
            twss_series = rets * decay

            abs_twss = twss_series.abs()
            if not abs_twss.empty:
                max_idx = abs_twss.idxmax()
                flags['twss_score'] = float(abs_twss[max_idx])
                flags['twss_date'] = str(max_idx.date())
                flags['twss_days_ago'] = (datetime.date.today() - max_idx.date()).days
                flags['twss_raw_pct'] = float(rets[max_idx])
                if flags['twss_score'] > 60: flags['twss_orientation'] = "HOCH"
                elif flags['twss_score'] > 25: flags['twss_orientation'] = "MITTEL"

        # 5. Momentum
        flags['mom_12m'] = _calc_momentum(hist_close, curr_price, int(self.config.get('mom_lookback_12m', 252)))
        flags['mom_6m'] = _calc_momentum(hist_close, curr_price, int(self.config.get('mom_lookback_6m', 126)))
        flags['mom_3m'] = _calc_momentum(hist_close, curr_price, int(self.config.get('mom_lookback_3m', 63)))
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
                flags['distance_52w_high_pct'] = max(0.0, (flags['high_52w'] - curr_price) / flags['high_52w'] * 100.0)
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
                h, l, c = hist_data['High'], hist_data['Low'], hist_close
                tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
                atr = float(tr.rolling(int(self.config.get('atr_period', 14))).mean().iloc[-1])
                flags['atr'] = atr
                flags['atr_limit'] = curr_price - (float(self.config.get('atr_multiplier_limit', 1.0)) * atr)
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
            # WICHTIG: auto_adjust=False, damit wir Close vs Adj Close vergleichen koennen
            data = yf.download(to_fetch, period=self.config.get('history_period', '18mo'), group_by='ticker', auto_adjust=False, threads=True, progress=False)
            for t in to_fetch:
                hist = data[t] if len(to_fetch) > 1 else data
                if hist.empty or len(hist) < 10:
                    self.failed_tickers[t] = {'ticker': t, 'count': 1, 'top_reason': 'Download leer oder zu kurz'}
                    continue
                
                f = self._get_currency_factor(t)
                hist_adj = hist.copy()
                for col in ['Open', 'High', 'Low', 'Close']:
                    if col in hist_adj.columns: hist_adj[col] *= f
                if 'Adj Close' in hist_adj.columns: hist_adj['Adj Close'] *= f
                
                sma_len = int(self.config.get('sma_length', 130))

                # --- INTEGRITAETS-PRUEFUNG (Core Logik) ---
                core_cfg = {**self.config, "rsl_sma_window": sma_len, "min_history_rows_for_rsl": sma_len}
                analysis = rsl_integrity_core.analyze_history_for_rsl_integrity(hist_adj, ticker=t, cfg=core_cfg)
                
                repaired_df = analysis['history']
                clean_col = analysis['rsl_price_column']
                clean_series = repaired_df[clean_col].ffill()
                
                curr = float(clean_series.iloc[-1]) if not clean_series.empty else 0.0
                sma = analysis.get('rsl_sma', curr)
                
                # Metadaten aus Core-Analyse extrahieren
                used_fallback = bool(analysis.get('used_close_fallback', False))
                diag = analysis.get('diagnostics', {})
                
                is_young_history = len(clean_series.dropna()) < sma_len
                if is_young_history:
                    self.young_tickers[t] = {'ticker': t, 'count': 1, 'top_reason': f'Historie zu kurz (<{sma_len})'}

                vol_eur = float(hist['Volume'].ffill().tail(20).mean() * curr) if 'Volume' in hist.columns else 0.0
                
                # Flags auf der reparierten Serie berechnen, ohne das DataFrame zu mutieren
                flags = self._calculate_flags(hist_adj, curr, sma, is_young_history, price_series=clean_series)
                
                # Integritaets-Gründe in Flags einmischen
                flags['integrity_reasons'] = analysis.get('integrity_reasons', [])
                flags['used_close_fallback'] = used_fallback
                # Reparatur-Details konsistent übernehmen
                flags['repair_applied'] = diag.get('repair_applied', False)
                flags['repair_method'] = diag.get('repair_method', '')
                flags['repair_reason'] = diag.get('repair_reason', '')
                flags['fallback_fraction'] = diag.get('fallback_fraction', 0.0)
                flags['rsl_price_source'] = diag.get('rsl_price_source_mode', 'adj_close')
                flags['rsl_price_source_mode'] = diag.get('rsl_price_source_mode', 'adj_close')
                    
                results[t] = (curr, sma, vol_eur, flags)
                with self.lock:
                    self.cache[f"{t}_{version}"] = {'curr': curr, 'sma': sma, 'vol_eur': vol_eur, 'flags': flags, 'timestamp': time.time()}
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
            # auto_adjust=False fuer manuelle Pruefung
            hist = yf.Ticker(ticker).history(period=self.config.get('history_period', '18mo'), auto_adjust=False)
            if hist.empty: return None
            f = self._get_currency_factor(ticker)
            hist_adj = hist.copy()
            for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
                if col in hist_adj.columns: hist_adj[col] *= f
            
            sma_len = int(self.config.get('sma_length', 130))

            core_cfg = {**self.config, "rsl_sma_window": sma_len, "min_history_rows_for_rsl": sma_len}
            analysis = rsl_integrity_core.analyze_history_for_rsl_integrity(hist_adj, ticker=ticker, cfg=core_cfg)
            
            repaired_df = analysis['history']
            clean_col = analysis['rsl_price_column']
            clean_series = repaired_df[clean_col].ffill()
            used_fallback = bool(analysis.get('used_close_fallback', False))
            diag = analysis.get('diagnostics', {})
            
            curr = float(clean_series.iloc[-1]) if not clean_series.empty else 0.0
            sma = analysis.get('rsl_sma', curr)
            is_young_history = len(clean_series.dropna()) < sma_len
            if is_young_history:
                self.young_tickers[ticker] = {'ticker': ticker, 'count': 1, 'top_reason': f'Historie zu kurz (<{sma_len})'}

            vol_eur = float(hist['Volume'].ffill().tail(20).mean() * curr) if 'Volume' in hist.columns else 0.0
            flags = self._calculate_flags(hist_adj, curr, sma, is_young_history, price_series=clean_series)
            
            flags['integrity_reasons'] = analysis.get('integrity_reasons', [])
            flags['used_close_fallback'] = used_fallback
            # Reparatur-Details konsistent übernehmen
            flags['repair_applied'] = diag.get('repair_applied', False)
            flags['repair_method'] = diag.get('repair_method', '')
            flags['repair_reason'] = diag.get('repair_reason', '')
            flags['fallback_fraction'] = diag.get('fallback_fraction', 0.0)
            flags['rsl_price_source'] = diag.get('rsl_price_source_mode', 'adj_close')
            flags['rsl_price_source_mode'] = diag.get('rsl_price_source_mode', 'adj_close')
            
            with self.lock:
                self.cache[key] = {'curr': curr, 'sma': sma, 'vol_eur': vol_eur, 'flags': flags, 'timestamp': time.time()}
            return (curr, sma, vol_eur, flags)
        except: return None

    @retry_decorator
    def fetch_and_cache_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        if ticker in self.info_cache: return self.info_cache[ticker]
        info = yf.Ticker(ticker).info
        if info:
            clean_info = {
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'country': info.get('country', 'Unknown'),
                'longName': info.get('longName', ticker),
                'marketCap': info.get('marketCap', 0),
                'cached_at': datetime.datetime.now().isoformat()
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
