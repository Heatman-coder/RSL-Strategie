#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
global_acwi_v68_dashboard_plus.py
Refactoring based on User Request:
- FEATURE: Enhanced Console Dashboard.
- ADDED: RSL Trend Arrows (up/down) directly in the console.
- ADDED: Trend Quality (STAB/NORM/WACK) column.
- ADDED: Spike Warning (TWSS) column (shows value only if > 50).
- CORE: Logic remains v67 (Clean, Smart Cache, First Seen).
"""
import os
from dotenv import load_dotenv

import pandas as pd
import yfinance as yf
import os
import sys
import json
import time
import random
import datetime
import logging
import warnings
import re
import concurrent.futures
import csv
import math
from urllib.parse import quote
from typing import List, Tuple, Optional, Dict, Any, Callable, Union, Set, cast
from dataclasses import dataclass, asdict, fields
from collections import defaultdict
import numpy as np
from core import ranking as ranking_core
from core import summaries as summary_core
from core import quality_gate as quality_core
from core import reporting_excel as reporting_core
from core import console_symbols as console_core
from core import console_ui as console_ui_core
from core import data_pipeline as data_pipeline_core
from core import candidate_engine as candidate_core
from core import settings_ui as settings_ui_core
from core import settings_catalog as settings_catalog_core
from core import rsl_integrity as rsl_integrity_core
from core import final_support as final_support_core
from core import app_config as app_config_core
from core import app_support as app_support_core
from data_manager import (
    MarketDataManager, FirstSeenManager, PortfolioManager, 
    retry_decorator, StockData, _consume_rate_limit_hits
)
from core.entity_matching import normalize_name_for_dedup
from core.data_pipeline import load_selected_etf_universe
from core import etf_processor as etf_processor_core
from core.reporting_excel import save_excel_report_safely

app_support_core.fix_bom_in_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'core', 'reporting_excel.py'))
# --- KONSTANTEN ---
ACTION_BUY = "kaufen"
ACTION_SELL = "verkaufen"
ACTION_HOLD = "halten"
# --- PFAD FIXIEREN ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)
def get_path(filename: str) -> str:
    return app_config_core.get_path(SCRIPT_DIR, filename)
def get_report_path(filename: str) -> str:
    return app_config_core.get_report_path(REPORTS_DIR, filename)
def configure_best_console_mode() -> Dict[str, Any]:
    return app_support_core.configure_best_console_mode()
CONSOLE_RUNTIME = configure_best_console_mode()

def get_last_performance_duration() -> Optional[str]:
    return app_support_core.get_last_performance_duration(CONFIG)

def make_progress(total: int, desc: str, include_last_duration: bool = True):
    return app_support_core.make_progress(
        total=total,
        desc=desc,
        config=CONFIG,
        console_runtime=CONSOLE_RUNTIME,
        include_last_duration=include_last_duration,
    )
# --- LOGGING SETUP ---
logger = app_support_core.configure_logging(get_path('global_rsl.log'))
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)
# Unterdrücke technische NumPy-Warnungen bei Berechnungen mit fehlerhaften/flachen Kursdaten
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*invalid value encountered in.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message="Degrees of freedom <= 0 for slice")
# --- KONFIGURATION ---
CONFIG: Dict[str, Any] = {
    'batch_size': 50,
    'max_workers': 8,
    'batch_sleep_min_s': 0.5,
    'batch_sleep_max_s': 1.5,
    'min_price': 1.0,
    'max_retries': 4,
    'retry_base_delay': 5,
    'cache_duration_hours': 24,
    'history_period': "18mo",
    'required_days': 130,
   
    # ETF Cache
    'etf_cache_file': get_path("etf_holdings_cache.pkl"),
    'etf_cache_duration_hours': 168,
    'exchange_cache_file': get_path("exchange_universe_cache.pkl"),
    'industry_history_file': get_path("industry_history.json"),
    'etf_names_cache_file': get_path("etf_names_cache.json"),
    'country_cache_file': get_path("etf_country_cache.json"),
    # Info Cache (NEU)
    'info_cache_unknown_expiry_days': 7,
    'info_fetch_delay_s': settings_catalog_core.USER_SETTINGS_DEFAULTS['info_fetch_delay_s'],
    'info_fetch_quiet': True,
    'info_fetch_summary_max': 10,
    'rate_limit_delay_min_s': 20.0,
    'rate_limit_delay_max_s': 60.0,
    'rate_limit_backoff_step_s': 5.0,
    'rate_limit_log_every': 10,
   
    # First Seen Cache
    'first_seen_cache_file': get_path("first_seen_cache.json"),
   
    # DATEN-QUALITAET
    'min_liquidity': 0.60,
    'stale_window': 60,
    'min_unique_ratio': 0.05,
    'min_nonzero_ratio': 0.10,
    'max_consecutive_flat': 20,
    'max_std_rel': 0.005,
    'min_total_range': 0.02,
    'max_total_return': 10.0,
    'max_flat_days': 7,
    'max_gap_percent': 0.30,
    'price_scale_recent_window': 60,
    'price_scale_warn_ratio': 15.0,
    'price_scale_critical_ratio': 50.0,
    'price_scale_warn_jump': 0.8,
    'price_scale_critical_jump': 1.5,
    'price_scale_near_high_pct': 35.0,
   
    # TWSS (Time-Weighted Spike Score)
    'twss_decay_days': 60.0,
    
    # Industry-Score Robustheit
    'industry_breadth_min': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_breadth_min'],
    'industry_avg_rsl_cap': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_avg_rsl_cap'],
    'industry_min_size': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_min_size'],
    'industry_summary_include_unknown': True,
    'industry_trend_enabled': True,
    'industry_trend_weeks': 4,
    'industry_top_n': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_top_n'],
    'industry_score_min': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_score_min'],

    # Exchange Sources (Neu aus Beispiel.py)
    'url_xetra_instruments': "https://cashmarket.deutsche-boerse.com/resource/blob/1528/8b811ef41181e6f04d98ad511804ab8f/data/t7-xetr-allTradableInstruments.csv",
    'url_frankfurt_instruments': "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/babd7d2ee2ba6907ea2ac3bf74b3488f/data/t7-xfra-BF-allTradableInstruments.csv",
    'exchange_scan_enabled': True, 

    'industry_score_w_breadth': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_score_w_breadth'],
    'industry_score_w_avg': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_score_w_avg'],
    'industry_score_w_median': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_score_w_median'],
    'industry_score_w_leader': settings_catalog_core.USER_SETTINGS_DEFAULTS['industry_score_w_leader'],

    # Multi-Horizon Momentum (Kandidaten-Scoring)
    'mom_lookback_3m': 63,
    'mom_lookback_6m': 126,
    'mom_lookback_12m': 252,
    'mom_weight_12m': settings_catalog_core.USER_SETTINGS_DEFAULTS['mom_weight_12m'],
    'mom_weight_6m': settings_catalog_core.USER_SETTINGS_DEFAULTS['mom_weight_6m'],
    'mom_weight_3m': settings_catalog_core.USER_SETTINGS_DEFAULTS['mom_weight_3m'],
    'candidate_scoring_model': 'institutional',
    'mom_vol_lookback': 63,
    'candidate_use_momentum_score': True,
    'candidate_use_vol_adjust': True,
    'candidate_use_industry_neutral': True,
    'candidate_use_accel': True,
    'candidate_accel_weight': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_accel_weight'],
    'candidate_use_rsl_change_1w': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_use_rsl_change_1w'],
    'candidate_rsl_change_weight': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_rsl_change_weight'],
    'candidate_min_avg_volume_eur': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_min_avg_volume_eur'],
    'candidate_min_trust_score': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_min_trust_score'],
    'candidate_score_min': 0.0,
    'candidate_require_top_percent': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_require_top_percent'],
    'candidate_top_percent_threshold': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_top_percent_threshold'],
    'candidate_block_new_buys_in_weak_regime': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_block_new_buys_in_weak_regime'],
    'candidate_max_stocks_per_industry': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_max_stocks_per_industry'],
    'candidate_use_peer_spread': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_use_peer_spread'],
    'candidate_peer_spread_weight': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_peer_spread_weight'],
    'candidate_max_distance_52w_high_pct': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_max_distance_52w_high_pct'],
    'market_cap_refresh_listing_only_min_turnover_eur': 1000000.0,
    'market_cap_refresh_max_workers': 2,
    'market_cap_refresh_max_symbols': 80,

    # Momentum-Cluster
    'cluster_enabled': True,
    'cluster_top_n': settings_catalog_core.USER_SETTINGS_DEFAULTS['cluster_top_n'],
    'cluster_min_size': settings_catalog_core.USER_SETTINGS_DEFAULTS['cluster_min_size'],
    'cluster_score_w_mom12': settings_catalog_core.USER_SETTINGS_DEFAULTS['cluster_score_w_mom12'],
    'cluster_score_w_mom6': settings_catalog_core.USER_SETTINGS_DEFAULTS['cluster_score_w_mom6'],
    'cluster_score_w_accel': settings_catalog_core.USER_SETTINGS_DEFAULTS['cluster_score_w_accel'],
    'candidate_use_cluster_filter': settings_catalog_core.USER_SETTINGS_DEFAULTS['candidate_use_cluster_filter'],
   
    # INDIKATOREN
    'sma_length': 130,
    'sma_short_length': 50,
    'annual_factor': 252,
    # TIMING (ATR)
    'atr_period': 14,
    'atr_multiplier_limit': 1.0,
    'atr_multiplier_exit': settings_catalog_core.USER_SETTINGS_DEFAULTS['atr_multiplier_exit'],

    # STRATEGIE-PARAMETER
    'top_percent_threshold': 0.25,
    'heatmap_warn_percent': 20.0,
    'heatmap_full_percent': 25.0,
   
    # STRICT MODE (Datenqualitaet / Reproduzierbarkeit)
    'strict_mode': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_mode'],
    'strict_min_analyzed_stocks': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_min_analyzed_stocks'],
    'strict_min_coverage_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_min_coverage_ratio'],
    'strict_max_failed_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_failed_ratio'],
    'strict_max_young_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_young_ratio'],
    'strict_max_critical_drop_ratio': 0.60, # Erlaubt bis zu 60% Daten-Ausschuss im Riesen-Universum
    'strict_max_stale_warn_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_stale_warn_ratio'],
    'strict_max_gap_warn_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_gap_warn_ratio'],
    'strict_max_liquidity_warn_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_liquidity_warn_ratio'],
    'strict_max_low_trust_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_low_trust_ratio'],
    'strict_min_portfolio_coverage_ratio': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_min_portfolio_coverage_ratio'],
    'strict_max_invalid_numeric_count': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_invalid_numeric_count'],
    'strict_max_duplicate_symbols': settings_catalog_core.USER_SETTINGS_DEFAULTS['strict_max_duplicate_symbols'],
   
    # DATEIPFADE
    'result_file_prefix': get_report_path("global_rsl"),
    'mapping_file': get_path("ticker_map_v2.json"),
    'history_cache_file': get_path("history_cache.json"),
    'ticker_info_cache_file': get_path("ticker_info_cache.json"),
    'portfolio_file': get_path("current_portfolio.json"),
    'etf_config_file': get_path("etf_config.json"),
    'watchlist_file': get_path("watchlist_tickers.txt"),
    'manual_fix_file': get_path("manual_fix.json"),
    'blacklist_file': get_path("blacklist.json"),
    'location_suffix_map_file': get_path("location_suffix_map.json"),
    'exchange_suffix_map_file': get_path("exchange_suffix_map.json"),
    'unsupported_exchanges_file': get_path("unsupported_exchanges.json"),
    'failed_tickers_log': get_report_path("failed_tickers.log"),
    'failed_tickers_json': get_report_path("failed_tickers.json"),
    'failed_tickers_csv': get_report_path("failed_tickers.csv"),
    'young_tickers_log': get_report_path("young_tickers.log"),
    'young_tickers_json': get_report_path("young_tickers.json"),
    'young_tickers_csv': get_report_path("young_tickers.csv"),
    'performance_log_csv': get_report_path("performance_log.csv"),
    'dropped_tickers_log': get_report_path("dropped_tickers.log"),
    'last_console_output_file': get_report_path("last_console_output.txt"),
    'last_analysis_snapshot_file': get_report_path("last_analysis_snapshot.json"),
    'rsl_integrity_drop_file': get_report_path("rsl_integrity_drops.csv"),
    'home_market_rsl_audit_file': get_report_path("home_market_rsl_audit.csv"),
    'home_market_rsl_review_file': get_report_path("home_market_rsl_review.csv"),
    'home_market_rsl_review_top_rank': 300,
    'strict_report_file': get_report_path("strict_quality_report.json"),
    'user_settings_file': get_path("user_settings.json"),
    'strategy_profiles_file': get_path("strategy_profiles.json"),
    'currency_rates_file': get_path("currency_rates.json"),
    'last_run_config_file': get_path("last_run_config.json"),
    'run_stats_file': get_path("run_stats.json"),
    'base_url_template': "https://www.ishares.com/us/products/{id}/{slug}/1467271812596.ajax?fileType=csv&fileName={symbol}_holdings&dataType=fund"
}
# --- HELPER FUNCTIONS ---
load_json_config = final_support_core.load_json_config
save_json_config = final_support_core.save_json_config
load_watchlist_symbols = final_support_core.load_watchlist_symbols
_to_float = final_support_core.to_float
_to_bool = final_support_core.to_bool
_safe_positive_float = final_support_core.safe_positive_float

def _sanitize_heatmap_thresholds(warn_pct: Any, full_pct: Any) -> Tuple[float, float]:
    """Bereinigt und validiert Heatmap-Schwellenwerte.
    
    Args:
        warn_pct: Warnschwelle in Prozent
        full_pct: Vollschwelle in Prozent
        
    Returns:
        Tuple mit bereinigten Werten (warn_value, full_value)
    """
    # Konfigurierbare Standardwerte
    DEFAULT_WARN_PERCENT = 20.0
    DEFAULT_FULL_PERCENT = 25.0
    
    warn_value = max(0.0, _to_float(warn_pct, DEFAULT_WARN_PERCENT))
    full_value = max(1.0, _to_float(full_pct, DEFAULT_FULL_PERCENT))
    if warn_value >= full_value:
        warn_value = max(0.0, full_value - 1.0)
    return warn_value, full_value

load_user_settings = lambda: {**settings_catalog_core.get_user_settings_defaults(), **load_json_config(CONFIG['user_settings_file'])}

def apply_user_settings(settings: Dict[str, Any]):
    app_config_core.apply_user_settings(
        config=CONFIG,
        settings=settings,
        to_float=_to_float,
        to_bool=_to_bool,
        normalize_weights=final_support_core.normalize_weights,
    )
def save_user_settings(settings: Dict[str, Any]) -> None:
    app_config_core.save_user_settings(CONFIG, settings, save_json_config)
def save_dataframe_safely(df: pd.DataFrame, filename: str, **kwargs) -> None:
    app_support_core.save_dataframe_safely(df, filename, logger, **kwargs)
def normalize_sector_name(raw_sector: Any) -> str:
    return app_support_core.normalize_sector_name(raw_sector)
def build_yahoo_quote_url(yahoo_symbol: str) -> str:
    return app_support_core.build_yahoo_quote_url(yahoo_symbol)


def _calc_momentum(series: pd.Series, curr_price: float, lookback: int) -> Optional[float]:
    """Calculates the momentum over a given lookback period."""
    try:
        lb = int(lookback)
    except Exception:
        return None
    if lb <= 0 or len(series) < lb:
        return None
    try:
        past = float(series.iloc[-lb])
    except Exception:
        return None
    if past <= 0:
        return None
    return (curr_price / past) - 1.0

def _safe_positive_float(val: Any) -> float:
    try:
        f = float(val)
        return f if f > 0 else 0.0
    except (ValueError, TypeError):
        return 0.0

def _resolve_market_cap_from_info(info: Dict) -> float:
    return app_support_core.resolve_market_cap_from_info(info, _safe_positive_float)
    if not info: return 0.0
    return _safe_positive_float(info.get('marketCap', 0.0))

def get_currency_rate_for_ticker(ticker: str) -> float:
    """Ermittelt den korrekten Umrechnungsfaktor zu EUR basierend auf dem Ticker-Suffix."""
    return app_support_core.get_currency_rate_for_ticker(ticker, CURRENCY_RATES)
    t = str(ticker or "").strip().upper()
    # Prüfe auf bekannte Suffixe in der globalen CURRENCY_RATES
    for suffix, rate in CURRENCY_RATES.items():
        if suffix != "DEFAULT" and t.endswith(suffix):
            return float(rate)
    # Fallback auf Standard (meist USD -> EUR), wenn kein Suffix passt
    return float(CURRENCY_RATES.get("DEFAULT", 1.0))

def _resolve_market_value_from_sources(row: pd.Series, info: Dict, ticker: str = "") -> float:
    return app_support_core.resolve_market_value_from_sources(
        row=row,
        info=info,
        ticker=ticker,
        currency_rates=CURRENCY_RATES,
        safe_positive_float=_safe_positive_float,
    )
    val = _safe_positive_float(row.get('Market_Value'))
    if val <= 0:
        val = _safe_positive_float(row.get('Market Value'))
    if val <= 0:
        mkt_cap = _resolve_market_cap_from_info(info)
        rate = get_currency_rate_for_ticker(ticker)
        val = mkt_cap * rate
    return val

def apply_primary_liquidity_context(results: List[StockData]):
    """Berechnet die primäre Liquidität über verschiedene Listings hinweg."""
    app_support_core.apply_primary_liquidity_context(results, CURRENCY_RATES, _to_float)
    return
    # Safety: Namen-Normalisierung fuer Snapshots
    # Safety: Sicherstellen, dass Namen immer Strings sind (verhindert float/NaN Fehler aus Snapshots)
    for s in results:
        if not isinstance(s.name, str):
            s.name = str(s.name or "Unknown").strip()
            if s.name.lower() in ('nan', 'none', ''): s.name = "Unknown"

    groups = defaultdict(list)
    
    # ISIN-Backfilling
    name_to_isin = {}
    for s in results:
        isin = str(s.isin or "").strip().upper()
        if isin and len(isin) > 5 and isin != 'NAN':
            # Nutze die robuste Normalisierung aus entity_matching
            name_key = normalize_name_for_dedup(s.name)
            if name_key and len(name_key) > 3:
                name_to_isin[name_key] = isin
            
    # Fehlende ISINs ergaenzen, falls wir den Namen schonmal mit ISIN hatten
    for s in results:
        isin = str(s.isin or "").strip().upper()
        if not isin or len(isin) <= 5 or isin == 'NAN':
            name_key = normalize_name_for_dedup(s.name)
            if name_key in name_to_isin:
                s.isin = name_to_isin[name_key]

    # 2. Schritt: Gruppierung nach ISIN (Prio) oder normalisiertem Namen
    for s in results:
        key = s.isin if (s.isin and len(s.isin) > 5) else normalize_name_for_dedup(s.name)
        groups[key].append(s)
    
    for key, items in groups.items():
        if not items: continue
        # Das Listing mit dem hoechsten Umsatz gewinnt
        best = max(items, key=lambda x: _to_float(x.avg_volume_eur, -1.0))
        
        # NEU: Die beste verfügbare Market Cap in der Gruppe finden
        best_mkt_obj = max(items, key=lambda x: _to_float(getattr(x, "market_cap", 0.0), -1.0))
        best_mkt_cap = _to_float(getattr(best_mkt_obj, "market_cap", 0.0), 0.0)

        for s in items:
            s.primary_liquidity_eur = best.avg_volume_eur
            s.primary_liquidity_symbol = best.yahoo_symbol
            s.primary_liquidity_basis = "ISIN" if (s.isin and len(s.isin) > 5) else "Name"

            # Falls dieses Listing keine Market Cap hat, nimm die beste aus der Gruppe
            if _to_float(getattr(s, "market_cap", 0.0), 0.0) <= 0 and best_mkt_cap > 0:
                s.market_cap = best_mkt_cap
                rate = get_currency_rate_for_ticker(s.yahoo_symbol)
                s.market_value = best_mkt_cap * rate

def update_live_currency_rates():
    """Holt aktuelle Wechselkurse von Yahoo Finance für eine genauere Umrechnung."""
    app_support_core.update_live_currency_rates(CURRENCY_RATES, logger, yf)
    return
    logger.info("Aktualisiere Wechselkurse via Yahoo Finance...")
    pairs = {"EURUSD=X": "DEFAULT", "EURJPY=X": ".T", "EURGBP=X": ".L", "EURHKD=X": ".HK"}
    try:
        # Wir laden die Kehrwerte, da yfinance Kurse meist als 1 EUR = X USD angibt
        data = yf.download(list(pairs.keys()), period="1d", interval="1m", progress=False)
        if not data.empty and 'Close' in data.columns:
            for pair, suffix in pairs.items():
                last_price = data['Close'][pair].iloc[-1]
                if last_price > 0:
                    # Da wir von Fremdwährung in EUR umrechnen: 1 / Kurs
                    rate = 1.0 / float(last_price)
                    CURRENCY_RATES[suffix] = rate
            logger.info("Wechselkurse erfolgreich aktualisiert.")
    except Exception as e:
        logger.warning(f"Live-FX Update fehlgeschlagen (nutze Fallbacks): {e}")
    
    # Sicherheits-Check: Euro-Suffixe MÜSSEN immer 1.0 sein
    for euro_sfx in [".DE", ".F", ".PA", ".AS", ".MC", ".MI"]:
        CURRENCY_RATES[euro_sfx] = 1.0
    CURRENCY_RATES["EUR"] = 1.0

sanitize_ticker_symbol = final_support_core.sanitize_ticker_symbol
is_plausible_ticker = final_support_core.is_plausible_ticker
generate_candidates = lambda orig, land, exchange: final_support_core.generate_candidates(orig, land, exchange, UNSUPPORTED_EXCHANGES, EXCHANGE_SUFFIX_MAP, LOCATION_SUFFIX_MAP)
download_ishares_csv = lambda url, log_label=True: final_support_core.download_ishares_csv(url, logger, log_label)
_log_info_fetch_summary = lambda msg, mgr: final_support_core.log_info_fetch_summary(msg, mgr, logger)
_parse_etf_selection_input = final_support_core.parse_etf_selection_input
parse_ishares_url = final_support_core.parse_ishares_url
_merge_tokens = final_support_core.merge_tokens
_parse_source_tokens = final_support_core.parse_tokens
_history_priority_score = lambda item: final_support_core.history_priority_score(item, LOCATION_SUFFIX_MAP)

def _stock_history_priority_score(s: StockData) -> int:
    return final_support_core.stock_history_priority_score(s, LOCATION_SUFFIX_MAP)

def get_rsl_integrity_reasons(item: Any, raw_rsl: Any = None) -> List[str]:
    return rsl_integrity_core.get_rsl_integrity_reasons(
        item, LOCATION_SUFFIX_MAP, CONFIG, raw_rsl=raw_rsl
    )

def filter_stock_results_for_rsl_integrity(results):
    return rsl_integrity_core.filter_stock_results_for_rsl_integrity(
        results, LOCATION_SUFFIX_MAP, CONFIG
    )

def synchronize_portfolio_symbols_with_stock_results(portfolio_mgr, results):
    return final_support_core.synchronize_portfolio_symbols_with_stock_results(portfolio_mgr, results)

def build_home_market_rsl_audit(results):
    base_df = rsl_integrity_core.build_home_market_rsl_audit(results, LOCATION_SUFFIX_MAP)
    rows = []
    by_symbol = {str(getattr(stock, "yahoo_symbol", "")).upper(): stock for stock in results}
    for _, row in base_df.iterrows():
        symbol = str(row.get("ticker", "")).upper()
        stock = by_symbol.get(symbol)
        original_ticker = str(getattr(stock, "original_ticker", symbol) or symbol)
        primary_symbol = str(getattr(stock, "primary_liquidity_symbol", "") or "")
        history_matches_home = bool(primary_symbol) and symbol == primary_symbol.upper()
        history_status = "OVERRIDDEN_TO_HOME" if history_matches_home else ("SECONDARY_HISTORY_ACTIVE" if primary_symbol else "UNKNOWN")
        review_reasons = [x for x in str(row.get("review_reasons", "") or "").split(",") if x]
        if primary_symbol and not history_matches_home:
            review_reasons.extend(["secondary_without_override", "secondary_history_active"])
        if _to_float(getattr(stock, "rsl", 0.0), 0.0) >= 1.5 and _to_float(getattr(stock, "mom_6m", 0.0), 0.0) < 0.05:
            review_reasons.append("high_rsl_vs_weak_6m")
        merged = dict(row)
        merged.update(
            {
                "original_ticker": original_ticker,
                "yahoo_symbol": symbol,
                "primary_liquidity_symbol": primary_symbol,
                "rsl_rank": getattr(stock, "rsl_rang", row.get("rsl_rank")),
                "history_status": history_status,
                "history_matches_home": history_matches_home,
                "needs_review": bool(review_reasons),
                "review_reasons": ",".join(dict.fromkeys(review_reasons)),
            }
        )
        rows.append(merged)
    return pd.DataFrame(rows)

def build_home_market_rsl_review_shortlist(
    audit_df: pd.DataFrame, top_rank: int = 300
) -> pd.DataFrame:
    work = audit_df.copy()
    rank_col = "rsl_rank" if "rsl_rank" in work.columns else "RSL-Rang"
    if rank_col in work.columns:
        work = work[work[rank_col].fillna(999999) <= top_rank]
    if "needs_review" in work.columns:
        work = work[work["needs_review"] == True]
    return work.reset_index(drop=True)

def save_home_market_rsl_audit(results):
    return app_support_core.save_home_market_rsl_audit(
        results=results,
        config=CONFIG,
        location_suffix_map=LOCATION_SUFFIX_MAP,
        save_dataframe_safely_func=save_dataframe_safely,
    )


def run_fundamental_data_download(data_mgr: MarketDataManager) -> None:
    """Lädt gezielt Fundamentaldaten für das Universum nach, priorisiert auf Primär-Listings."""
    print("\n\033[94m" + "="*70)
    print(" FUNDAMENTALDATEN-DOWNLOAD (Market Cap, ISIN, Sektor)")
    print("="*70 + "\033[0m")
    
    # 1. Universum bestimmen
    etf_config = load_json_config(CONFIG['etf_config_file'])
    selected_syms = etf_config.get('selected_symbols', [])
    etf_options = etf_config.get('options', {})
    
    if not selected_syms:
        print("Keine ETFs ausgewählt. Bitte zuerst Auswahl treffen.")
        selected_syms, etf_options = select_etf_interactive()
        if not selected_syms: return

    df = _prepare_ticker_universe(selected_syms, etf_options)
    if df.empty: return
    
    # Gruppierung für Qualitäts-Optimierung: Wir laden nur den "besten" Ticker pro Firma
    # (z.B. NASDAQ vor Frankfurt), da dort die Market Cap Daten bei Yahoo verlässlicher sind.
    from core.data_pipeline import _get_ticker_priority
    
    df['_prio'] = df['Ticker'].apply(_get_ticker_priority)
    if 'ISIN' in df.columns and _has_meaningful_isin_data(df):
        df['_group_id'] = df['ISIN'].fillna(df['Ticker'])
    else:
        df['_group_id'] = df['Name'].apply(normalize_name_for_dedup)
        
    # Besten Ticker pro Gruppe finden (Deduplizierung für den Download)
    best_tickers_df = df.sort_values('_prio').drop_duplicates(subset=['_group_id'], keep='first')
    unique_firms = sorted(list(set(best_tickers_df['Ticker'].astype(str).unique())))
    total_firms = len(unique_firms)
    
    # 2. Status Quo prüfen
    to_update_delta = []
    to_update_old = []
    has_data = 0
    now = datetime.datetime.now()
    three_months_ago = now - datetime.timedelta(days=90)
    
    for t in unique_firms:
        info = data_mgr.get_cached_info(t)
        if info and _to_float(info.get('marketCap', 0), 0) > 0:
            has_data += 1
            # Check age
            try:
                cached_at = datetime.datetime.fromisoformat(info.get('cached_at', ""))
                if cached_at < three_months_ago:
                    to_update_old.append(t)
            except:
                to_update_old.append(t)
        else:
            to_update_delta.append(t)
            
    coverage = (has_data / total_firms * 100) if total_firms > 0 else 0
    print(f"\nStatus des Universums ({total_firms} Firmen/Gruppen):")
    print(f" - Abdeckung (Market Cap > 0): \033[92m{coverage:.1f}%\033[0m")
    print(f" - Fehlende Daten (Delta):      {len(to_update_delta)}")
    print(f" - Veraltete Daten (> 3 Mon.):  {len(to_update_old)}")
    
    if not to_update_delta and not to_update_old:
        print("\n\033[92mDatenbestand ist perfekt und aktuell!\033[0m")
        return

    print("\nOptionen:")
    print(" [1] Nur Delta laden (fehlende/0)")
    print(" [2] Delta + Veraltete laden (> 3 Monate)")
    print(" [0] Abbrechen")
    
    mode_in = input("Wahl [1]: ").strip()
    if mode_in == "0": return
    mode = mode_in if mode_in else "1"
    
    targets = to_update_delta if mode == "1" else sorted(list(set(to_update_delta + to_update_old)))
    
    print(f"\nStarte Download für {len(targets)} Primär-Ticker (Batch-Drosselung aktiv)...")
    with make_progress(total=len(targets), desc="Fundamental Download", include_last_duration=False) as pbar, \
         concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        try:
            # Im Refresh-Modus (2) müssen wir den Cache-Eintrag löschen, damit yfinance neu abfragt
            if mode == "2":
                for t in to_update_old:
                    if t in data_mgr.info_cache:
                        with data_mgr.lock: del data_mgr.info_cache[t]

            # Einreichen in kleineren Sub-Batches, um Rate-Limits besser zu managen
            sub_batch_size = 50
            for j in range(0, len(targets), sub_batch_size):
                sub_targets = targets[j : j + sub_batch_size]
                futures = {executor.submit(data_mgr.fetch_and_cache_info, sym, force_refresh=True): sym for sym in sub_targets}
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    future.result()
                    pbar.update(1)
                    
                    # Notfall-Bremse: Wenn zu viele Rate-Limits auftreten
                    if _consume_rate_limit_hits() > 3:
                        print("\n\033[93mHohe Rate-Limit Aktivitaet erkannt. Kurze Abkuehlphase...\033[0m")
                        time.sleep(30)
                
                # Regelmäßiges Zwischenspeichern
                data_mgr.save_info_cache()
                
        except KeyboardInterrupt:
            print("\nDownload abgebrochen.")
        finally:
            data_mgr.save_info_cache()
            print(f"\n\033[92mDownload beendet. Datenqualität wurde optimiert.\033[0m")


def refresh_market_caps_for_relevant_exchange_stocks(stock_results: List[StockData], data_mgr: Any) -> int:
    if not stock_results or data_mgr is None:
        return 0

    updated = 0
    fetched_any = False
    max_symbols = int(CONFIG.get("market_cap_refresh_max_symbols", 80) or 80)

    candidates = [
        stock for stock in stock_results
        if _safe_positive_float(getattr(stock, "market_cap", 0.0)) <= 0
        or _safe_positive_float(getattr(stock, "market_value", 0.0)) <= 0
    ]
    candidates.sort(
        key=lambda stock: (
            -_safe_positive_float(getattr(stock, "primary_liquidity_eur", getattr(stock, "avg_volume_eur", 0.0))),
            -int(getattr(stock, "trust_score", 0) or 0),
        )
    )

    for stock in candidates[:max_symbols]:
        symbol = str(getattr(stock, "yahoo_symbol", "") or "").strip().upper()
        if not symbol:
            continue
        try:
            info = data_mgr.get_cached_info(symbol)
        except Exception:
            info = None
        if not info:
            try:
                info = data_mgr.fetch_and_cache_info(symbol)
                fetched_any = fetched_any or bool(info)
            except Exception:
                info = None
        market_cap = _resolve_market_cap_from_info(info or {})
        if market_cap <= 0:
            continue
        # Waehrungsumrechnung sicherstellen
        rate = get_currency_rate_for_ticker(symbol)
        if _safe_positive_float(getattr(stock, "market_cap", 0.0)) <= 0:
            stock.market_cap = market_cap
            updated += 1
        if _safe_positive_float(getattr(stock, "market_value", 0.0)) <= 0:
            stock.market_value = market_cap * rate

    if fetched_any and hasattr(data_mgr, "save_info_cache"):
        try:
            data_mgr.save_info_cache()
        except Exception as exc:
            logger.warning(f"Market-Cap-Refresh konnte Info-Cache nicht speichern: {exc}")

    return updated


TickerMapper = final_support_core.TickerMapper


def _normalize_name_for_dedup_key(name: Any) -> str:
    return final_support_core.normalize_name_for_dedup_key(name)


def _has_meaningful_isin_data(df: pd.DataFrame) -> bool:
    return final_support_core.has_meaningful_isin_data(df)


def build_history_symbol_overrides(raw_df: pd.DataFrame, current_df: pd.DataFrame) -> Dict[str, str]:
    return final_support_core.build_history_symbol_overrides(
        raw_df=raw_df,
        current_df=current_df,
        location_suffix_map=LOCATION_SUFFIX_MAP,
        unsupported_exchanges=UNSUPPORTED_EXCHANGES,
        exchange_suffix_map=EXCHANGE_SUFFIX_MAP,
    )

# --- GLOBALE DATEN LADEN ---
apply_user_settings(load_user_settings())
UNSUPPORTED_EXCHANGES = cast(List[str], load_json_config(CONFIG['unsupported_exchanges_file'], is_list=True))
LOCATION_SUFFIX_MAP = cast(Dict[str, str], load_json_config(CONFIG['location_suffix_map_file']))
EXCHANGE_SUFFIX_MAP = cast(Dict[str, str], load_json_config(CONFIG['exchange_suffix_map_file']))

# Lade Währungsraten aus der JSON-Datei. Dies sind die Fallback-Werte, wenn Live-Update fehlschlägt.
# Wenn die Datei leer ist oder nicht existiert, nutze minimale Standardwerte.
CURRENCY_RATES = cast(Dict[str, Any], load_json_config(CONFIG['currency_rates_file']))
if not CURRENCY_RATES:
    # Minimale Default-Werte, um einen initialen Start zu ermöglichen
    CURRENCY_RATES = {
    "DEFAULT": 0.95,  # USD -> EUR
    ".DE": 1.0,       # EUR (Basis)
    ".F": 1.0,        # EUR (Frankfurt)
    ".HK": 0.12,
    ".L": 1.20,
    ".T": 0.006,
    ".TO": 0.68,
    ".AX": 0.61,
    ".SR": 0.25,
    ".TW": 0.029,
    ".KS": 0.00068,
    ".SA": 0.16,
    # Weitere wichtige Währungen sollten in currency_rates.json gepflegt werden
    # Die Live-Update-Funktion holt EURUSD, EURJPY, EURGBP
}
def get_user_input(prompt: str, default: Optional[str] = None, valid_options: Optional[List[str]] = None) -> str:
    return app_support_core.get_user_input(prompt, default=default, valid_options=valid_options)
def _cache_age_hours(file_path: str) -> Optional[float]:
    try:
        if not file_path or not os.path.exists(file_path):
            return None
        mtime = os.path.getmtime(file_path)
        return max(0.0, (time.time() - mtime) / 3600.0)
    except Exception:
        return None
def _format_age_text(hours: Optional[float]) -> str:
    if hours is None:
        return "-"
    return f"{hours:.1f}h"
def _summarize_etf_selection(selected_syms: List[str], etf_options: Dict[str, Any], max_items: int = 5) -> str:
    if not selected_syms:
        return "-"

    all_etf_keys = list(etf_options.keys())
    if len(all_etf_keys) > 0 and len(selected_syms) == len(all_etf_keys) and set(selected_syms) == set(all_etf_keys):
        return "Alle"

    if len(selected_syms) <= max_items:
        return ", ".join(selected_syms)
    head = ", ".join(selected_syms[:max_items])
    return f"{head}, ... (+{len(selected_syms) - max_items})"
def print_run_status_header(selected_syms: List[str], portfolio_size: int, etf_options: Dict[str, Any]):
    table_width = 100
    history_age = _cache_age_hours(CONFIG['history_cache_file'])
    etf_age = _cache_age_hours(CONFIG['etf_cache_file'])
    console_mode = f"{CONSOLE_RUNTIME.get('encoding')} / ANSI={str(CONSOLE_RUNTIME.get('ansi')).lower()}"
    cand_top_pct = float(CONFIG.get('candidate_top_percent_threshold', CONFIG.get('top_percent_threshold', 0.25)) or 0.25)
    cand_top_pct = cand_top_pct * 100.0
    require_top = bool(CONFIG.get('candidate_require_top_percent', False))
    min_trust = int(CONFIG.get('candidate_min_trust_score', 0) or 0)
    cluster_filter = bool(CONFIG.get('cluster_enabled', True)) and bool(CONFIG.get('candidate_use_cluster_filter', True))
    min_sleep = float(CONFIG.get('batch_sleep_min_s', 0.5))
    max_sleep = float(CONFIG.get('batch_sleep_max_s', 1.5))
    print("\n" + "=" * table_width)
    print(" LAUF-STATUS")
    print("=" * table_width)
    print(f" ETFs: {_summarize_etf_selection(selected_syms, etf_options)}")
    print(f" Portfolio: {portfolio_size} Position(en)")
    print(f" Cache: History={_format_age_text(history_age)} | ETF={_format_age_text(etf_age)}")
    print(f" History: {CONFIG.get('history_period', '-')} | Info-Unknown-Expiry: {CONFIG.get('info_cache_unknown_expiry_days', 7)}d")
    print(
        f" Schwellen: Warn={CONFIG['heatmap_warn_percent']:.1f}% | "
        f"Voll={CONFIG['heatmap_full_percent']:.1f}%"
    )
    print(
        f" Kandidaten: Top% {cand_top_pct:.0f}% ({'an' if require_top else 'aus'}) | "
        f"Trust-Min {min_trust} | Cluster-Filter {'an' if cluster_filter else 'aus'}"
    )
    print(
        f" Branchen: Top {CONFIG.get('industry_top_n', '-')}, "
        f"Score-Min {CONFIG.get('industry_score_min', 0.0):.2f}, "
        f"Breadth-Min {CONFIG.get('industry_breadth_min', 0.25):.2f}, "
        f"Min-Size {CONFIG.get('industry_min_size', 10)}"
    )
    print(
        f" Delays: Batch-Sleep {min_sleep:.2f}-{max_sleep:.2f}s | "
        f"Info-Fetch {float(CONFIG.get('info_fetch_delay_s', 0.7) or 0.7):.2f}s"
    )
    print(f" Konsole: {console_mode}")
    print("=" * table_width)
def _configure_user_settings_legacy():
    settings = load_user_settings()
    while True:
        def _rebalance_cluster_weights(target_key: str, target_value: float):
            base_w12 = _to_float(settings.get('cluster_score_w_mom12', CONFIG['cluster_score_w_mom12']), CONFIG['cluster_score_w_mom12'])
            base_w6 = _to_float(settings.get('cluster_score_w_mom6', CONFIG['cluster_score_w_mom6']), CONFIG['cluster_score_w_mom6'])
            base_acc = _to_float(settings.get('cluster_score_w_accel', CONFIG['cluster_score_w_accel']), CONFIG['cluster_score_w_accel'])
            target = max(0.0, min(1.0, float(target_value)))
            if target_key == "mom12":
                remaining = max(0.0, 1.0 - target)
                other_sum = base_w6 + base_acc
                if other_sum <= 0:
                    w6_ratio = 0.6
                    acc_ratio = 0.4
                else:
                    w6_ratio = base_w6 / other_sum
                    acc_ratio = base_acc / other_sum
                settings['cluster_score_w_mom12'] = target
                settings['cluster_score_w_mom6'] = remaining * w6_ratio
                settings['cluster_score_w_accel'] = remaining * acc_ratio
                return
            if target_key == "mom6":
                remaining = max(0.0, 1.0 - target)
                other_sum = base_w12 + base_acc
                if other_sum <= 0:
                    w12_ratio = 0.7
                    acc_ratio = 0.3
                else:
                    w12_ratio = base_w12 / other_sum
                    acc_ratio = base_acc / other_sum
                settings['cluster_score_w_mom12'] = remaining * w12_ratio
                settings['cluster_score_w_mom6'] = target
                settings['cluster_score_w_accel'] = remaining * acc_ratio
                return
            if target_key == "accel":
                remaining = max(0.0, 1.0 - target)
                other_sum = base_w12 + base_w6
                if other_sum <= 0:
                    w12_ratio = 0.5
                    w6_ratio = 0.5
                else:
                    w12_ratio = base_w12 / other_sum
                    w6_ratio = base_w6 / other_sum
                settings['cluster_score_w_mom12'] = remaining * w12_ratio
                settings['cluster_score_w_mom6'] = remaining * w6_ratio
                settings['cluster_score_w_accel'] = target
                return
        # Lade aktuelle Werte für die Anzeige, mit Fallback auf die Default-Konfiguration
        warn_current, full_current = _sanitize_heatmap_thresholds(
            settings.get('heatmap_warn_percent', CONFIG['heatmap_warn_percent']),
            settings.get('heatmap_full_percent', CONFIG['heatmap_full_percent'])
        )
        hist_cache_h = _to_float(settings.get('cache_duration_hours', CONFIG['cache_duration_hours']), CONFIG['cache_duration_hours'])
        etf_cache_h = _to_float(settings.get('etf_cache_duration_hours', CONFIG['etf_cache_duration_hours']), CONFIG['etf_cache_duration_hours'])
        info_expiry_d = int(_to_float(settings.get('info_cache_unknown_expiry_days', CONFIG['info_cache_unknown_expiry_days']), CONFIG['info_cache_unknown_expiry_days']))
        info_delay_s = _to_float(settings.get('info_fetch_delay_s', CONFIG['info_fetch_delay_s']), CONFIG['info_fetch_delay_s'])
        info_quiet = _to_bool(settings.get('info_fetch_quiet', CONFIG['info_fetch_quiet']), CONFIG['info_fetch_quiet'])
        rate_min_delay = _to_float(settings.get('rate_limit_delay_min_s', CONFIG['rate_limit_delay_min_s']), CONFIG['rate_limit_delay_min_s'])
        rate_max_delay = _to_float(settings.get('rate_limit_delay_max_s', CONFIG['rate_limit_delay_max_s']), CONFIG['rate_limit_delay_max_s'])
        if rate_max_delay < rate_min_delay:
            rate_max_delay = rate_min_delay
        rate_log_every = int(_to_float(settings.get('rate_limit_log_every', CONFIG['rate_limit_log_every']), CONFIG['rate_limit_log_every']))
        mom_w12_raw = _to_float(settings.get('mom_weight_12m', CONFIG['mom_weight_12m']), CONFIG['mom_weight_12m'])
        mom_w6_raw = _to_float(settings.get('mom_weight_6m', CONFIG['mom_weight_6m']), CONFIG['mom_weight_6m'])
        mom_w3_raw = _to_float(settings.get('mom_weight_3m', CONFIG['mom_weight_3m']), CONFIG['mom_weight_3m'])
        mom_w12, mom_w6, mom_w3 = final_support_core.normalize_weights(mom_w12_raw, mom_w6_raw, mom_w3_raw)
        mom_lb12 = int(_to_float(settings.get('mom_lookback_12m', CONFIG['mom_lookback_12m']), CONFIG['mom_lookback_12m']))
        mom_lb6 = int(_to_float(settings.get('mom_lookback_6m', CONFIG['mom_lookback_6m']), CONFIG['mom_lookback_6m']))
        mom_lb3 = int(_to_float(settings.get('mom_lookback_3m', CONFIG['mom_lookback_3m']), CONFIG['mom_lookback_3m']))
        mom_vol_lb = int(_to_float(settings.get('mom_vol_lookback', CONFIG['mom_vol_lookback']), CONFIG['mom_vol_lookback']))
        cand_use_mom = _to_bool(settings.get('candidate_use_momentum_score', CONFIG['candidate_use_momentum_score']), CONFIG['candidate_use_momentum_score'])
        cand_use_vol = _to_bool(settings.get('candidate_use_vol_adjust', CONFIG['candidate_use_vol_adjust']), CONFIG['candidate_use_vol_adjust'])
        cand_use_ind = _to_bool(settings.get('candidate_use_industry_neutral', CONFIG['candidate_use_industry_neutral']), CONFIG['candidate_use_industry_neutral'])
        cand_use_accel = _to_bool(settings.get('candidate_use_accel', CONFIG['candidate_use_accel']), CONFIG['candidate_use_accel'])
        cand_accel_w = _to_float(settings.get('candidate_accel_weight', CONFIG['candidate_accel_weight']), CONFIG['candidate_accel_weight'])
        cand_use_rsl_change = _to_bool(
            settings.get('candidate_use_rsl_change_1w', CONFIG['candidate_use_rsl_change_1w']),
            CONFIG['candidate_use_rsl_change_1w']
        )
        cand_rsl_w = _to_float(
            settings.get('candidate_rsl_change_weight', CONFIG['candidate_rsl_change_weight']),
            CONFIG['candidate_rsl_change_weight']
        )
        cand_min_vol = _to_float(settings.get('candidate_min_avg_volume_eur', CONFIG['candidate_min_avg_volume_eur']), CONFIG['candidate_min_avg_volume_eur'])
        cand_score_min = _to_float(settings.get('candidate_score_min', CONFIG['candidate_score_min']), CONFIG['candidate_score_min'])
        cand_min_trust = int(_to_float(settings.get('candidate_min_trust_score', CONFIG['candidate_min_trust_score']), CONFIG['candidate_min_trust_score']))
        cand_require_top = _to_bool(
            settings.get('candidate_require_top_percent', CONFIG['candidate_require_top_percent']),
            CONFIG['candidate_require_top_percent']
        )
        cand_top_pct = _to_float(
            settings.get('candidate_top_percent_threshold', CONFIG['candidate_top_percent_threshold']),
            CONFIG['candidate_top_percent_threshold']
        )
        trend_enabled = _to_bool(settings.get('industry_trend_enabled', CONFIG['industry_trend_enabled']), CONFIG['industry_trend_enabled'])
        trend_weeks = int(_to_float(settings.get('industry_trend_weeks', CONFIG['industry_trend_weeks']), CONFIG['industry_trend_weeks']))
        cluster_enabled = _to_bool(settings.get('cluster_enabled', CONFIG['cluster_enabled']), CONFIG['cluster_enabled'])
        cluster_top_n = int(_to_float(settings.get('cluster_top_n', CONFIG['cluster_top_n']), CONFIG['cluster_top_n']))
        cluster_min_size = int(_to_float(settings.get('cluster_min_size', CONFIG['cluster_min_size']), CONFIG['cluster_min_size']))
        c_w_mom_raw = _to_float(settings.get('cluster_score_w_mom12', CONFIG['cluster_score_w_mom12']), CONFIG['cluster_score_w_mom12'])
        c_w_mom6_raw = _to_float(settings.get('cluster_score_w_mom6', CONFIG['cluster_score_w_mom6']), CONFIG['cluster_score_w_mom6'])
        c_w_acc_raw = _to_float(settings.get('cluster_score_w_accel', CONFIG['cluster_score_w_accel']), CONFIG['cluster_score_w_accel'])
        c_w_mom, c_w_mom6, c_w_acc = final_support_core.normalize_weights(c_w_mom_raw, c_w_mom6_raw, c_w_acc_raw)
        cand_use_cluster = _to_bool(
            settings.get('candidate_use_cluster_filter', CONFIG['candidate_use_cluster_filter']),
            CONFIG['candidate_use_cluster_filter']
        )
        print("\n\033[96m--- EINSTELLUNGEN ---\033[0m")
        print("\n\033[93m-- Heatmap --\033[0m")
        print(f"1) Heatmap-Warnschwelle: {warn_current:.1f}%")
        print(f"2) Heatmap-Vollschwelle: {full_current:.1f}%")
        print("\n\033[93m-- Caching --\033[0m")
        print(f"3) Kurs-Cache Gueltigkeit (Stunden): {hist_cache_h}")
        print(f"4) ETF-Holdings-Cache Gueltigkeit (Std): {etf_cache_h}")
        print(f"5) Info-Cache 'Unknown' Ablauf (Tage): {info_expiry_d}")
        print(f"6) Info-Fetch Delay (Sekunden): {info_delay_s:.2f}")
        print(f"7) History Zeitraum (z.B. 12mo/18mo/24mo): {CONFIG['history_period']}")
        print("\n\033[93m-- Strategie --\033[0m")
        print(f"8) Top-Branchen fuer Kandidaten: {CONFIG['industry_top_n']}")
        print(f"9) Branchen-Score Mindestwert: {CONFIG['industry_score_min']:.2f}")
        print(f"10) Score-Gewicht Breadth: {CONFIG['industry_score_w_breadth']:.2f}")
        print(f"11) Score-Gewicht Avg RSL: {CONFIG['industry_score_w_avg']:.2f}")
        print(f"12) Score-Gewicht Median RSL: {CONFIG['industry_score_w_median']:.2f}")
        print(f"13) Score-Gewicht Leader: {CONFIG['industry_score_w_leader']:.2f}")
        print("\n\033[93m-- Kandidaten-Scoring --\033[0m")
        print(f"14) Momentum-Score nutzen: {'ja' if cand_use_mom else 'nein'}")
        print(f"15) Vol-Adjustierung nutzen: {'ja' if cand_use_vol else 'nein'}")
        print(f"16) Industry-Neutralisierung nutzen: {'ja' if cand_use_ind else 'nein'}")
        print(f"17) Momentum-Beschleunigung nutzen: {'ja' if cand_use_accel else 'nein'}")
        print(f"18) Accel-Gewicht: {cand_accel_w:.2f}")
        print(f"19) Min Primary Liquidity (Mio EUR): {cand_min_vol / 1_000_000:.1f}")
        print(f"20) Min Trust Score (0-3): {cand_min_trust}")
        print(f"21) Score-Minimum: {cand_score_min:.4f}")
        print(f"22) Nur Top-{int(cand_top_pct*100)}% zulassen: {'ja' if cand_require_top else 'nein'}")
        print(f"23) Nachkauf-Schwelle in %: {cand_top_pct*100:.1f}%")
        print(f"24) Momentum-Gewicht 12M: {mom_w12:.2f}")
        print(f"25) Momentum-Gewicht 6M: {mom_w6:.2f}")
        print(f"26) Momentum-Gewicht 3M: {mom_w3:.2f}")
        print(f"27) Lookback 12M (Tage): {mom_lb12}")
        print(f"28) Lookback 6M (Tage): {mom_lb6}")
        print(f"29) Lookback 3M (Tage): {mom_lb3}")
        print(f"30) Vol-Lookback (Tage): {mom_vol_lb}")
        print("31) Preset anwenden (Ausgewogen/Konservativ/Aggressiv)")
        print("\n\033[93m-- Momentum-Cluster --\033[0m")
        print(f"32) Cluster nutzen: {'ja' if cluster_enabled else 'nein'}")
        print(f"33) Kandidaten-Filter nach Top-Clustern: {'ja' if cand_use_cluster else 'nein'}")
        print(f"34) Top-Cluster Anzahl: {cluster_top_n}")
        print(f"35) Min Cluster-Groesse: {cluster_min_size}")
        print(f"36) Cluster-Score Gewicht Mom12: {c_w_mom:.2f}")
        print(f"37) Cluster-Score Gewicht Mom6: {c_w_mom6:.2f}")
        print(f"38) Cluster-Score Gewicht Accel: {c_w_acc:.2f}")
        print("\n\033[93m-- Aktionen --\033[0m")
        print("39) Kurs-Cache (history_cache.json) jetzt leeren")
        print("40) ETF-Cache (etf_holdings_cache.pkl) jetzt leeren")
        print("41) Alle Cache-Einstellungen auf Standard zuruecksetzen")
        print("42) Heatmap-Einstellungen auf Standard (20/25) zuruecksetzen")
        print("\n\033[93m-- Kandidaten (Zusatz) --\033[0m")
        print(f"43) RSL 1W-Change nutzen: {'ja' if cand_use_rsl_change else 'nein'}")
        print(f"44) RSL 1W-Gewicht: {cand_rsl_w:.3f}")
        print("\n\033[93m-- Branchen Trend (Hist) --\033[0m")
        print(f"45) Branchen-Trend anzeigen: {'ja' if trend_enabled else 'nein'}")
        print(f"46) Branchen-Trend Wochen: {trend_weeks}")
        print("\n\033[93m-- Info / Rate-Limit --\033[0m")
        print(f"47) Info-Fetch Logmodus: {'ruhig' if info_quiet else 'normal'}")
        print(f"48) Rate-Limit Min-Delay (Sek): {rate_min_delay:.1f}")
        print(f"49) Rate-Limit Max-Delay (Sek): {rate_max_delay:.1f}")
        print(f"50) Rate-Limit Log-Intervall (Events): {rate_log_every}")
        print("0) Zurueck zum Hauptmenue")
       
        choice = input("Auswahl [0]: ").strip()
        if choice in ("", "0"):
            return
       
        made_change = False
        if choice == "1":
            val = input(f"Neue Warnschwelle in % (aktuell: {warn_current:.1f}): ").strip().replace(",", ".")
            if val:
                settings['heatmap_warn_percent'] = _to_float(val, warn_current)
                made_change = True
        elif choice == "2":
            val = input(f"Neue Vollschwelle in % (aktuell: {full_current:.1f}): ").strip().replace(",", ".")
            if val:
                settings['heatmap_full_percent'] = _to_float(val, full_current)
                made_change = True
        elif choice == "3":
            val = input(f"Neue Gueltigkeit fuer Kurs-Cache in Stunden (aktuell: {hist_cache_h}): ").strip()
            if val:
                settings['cache_duration_hours'] = _to_float(val, hist_cache_h)
                made_change = True
        elif choice == "4":
            val = input(f"Neue Gueltigkeit fuer ETF-Cache in Stunden (aktuell: {etf_cache_h}): ").strip()
            if val:
                settings['etf_cache_duration_hours'] = _to_float(val, etf_cache_h)
                made_change = True
        elif choice == "5":
            val = input(f"Neuer Ablauf fuer 'Unknown' Info-Eintraege in Tagen (aktuell: {info_expiry_d}): ").strip()
            if val:
                settings['info_cache_unknown_expiry_days'] = int(_to_float(val, info_expiry_d))
                made_change = True
        elif choice == "6":
            val = input(f"Neuer Info-Fetch Delay in Sekunden (aktuell: {info_delay_s:.2f}): ").strip().replace(",", ".")
            if val:
                settings['info_fetch_delay_s'] = _to_float(val, info_delay_s)
                made_change = True
        elif choice == "7":
            val = input(f"Neuer History Zeitraum (aktuell: {CONFIG['history_period']}): ").strip()
            if val:
                settings['history_period'] = val
                made_change = True
        elif choice == "8":
            val = input(f"Neue Anzahl Top-Branchen (aktuell: {CONFIG['industry_top_n']}): ").strip()
            if val:
                settings['industry_top_n'] = int(_to_float(val, CONFIG['industry_top_n']))
                made_change = True
        elif choice == "9":
            val = input(f"Neuer Score-Mindestwert (aktuell: {CONFIG['industry_score_min']:.2f}): ").strip().replace(",", ".")
            if val:
                settings['industry_score_min'] = _to_float(val, CONFIG['industry_score_min'])
                made_change = True
        elif choice == "10":
            val = input(f"Neues Gewicht Breadth (aktuell: {CONFIG['industry_score_w_breadth']:.2f}): ").strip().replace(",", ".")
            if val:
                settings['industry_score_w_breadth'] = _to_float(val, CONFIG['industry_score_w_breadth'])
                made_change = True
        elif choice == "11":
            val = input(f"Neues Gewicht Avg RSL (aktuell: {CONFIG['industry_score_w_avg']:.2f}): ").strip().replace(",", ".")
            if val:
                settings['industry_score_w_avg'] = _to_float(val, CONFIG['industry_score_w_avg'])
                made_change = True
        elif choice == "12":
            val = input(f"Neues Gewicht Median RSL (aktuell: {CONFIG['industry_score_w_median']:.2f}): ").strip().replace(",", ".")
            if val:
                settings['industry_score_w_median'] = _to_float(val, CONFIG['industry_score_w_median'])
                made_change = True
        elif choice == "13":
            val = input(f"Neues Gewicht Leader (aktuell: {CONFIG['industry_score_w_leader']:.2f}): ").strip().replace(",", ".")
            if val:
                settings['industry_score_w_leader'] = _to_float(val, CONFIG['industry_score_w_leader'])
                made_change = True
        elif choice == "14":
            val = input(f"Momentum-Score nutzen? (j/n, aktuell: {'j' if cand_use_mom else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_momentum_score'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "15":
            val = input(f"Vol-Adjustierung nutzen? (j/n, aktuell: {'j' if cand_use_vol else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_vol_adjust'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "16":
            val = input(f"Industry-Neutralisierung nutzen? (j/n, aktuell: {'j' if cand_use_ind else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_industry_neutral'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "17":
            val = input(f"Momentum-Beschleunigung nutzen? (j/n, aktuell: {'j' if cand_use_accel else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_accel'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "18":
            val = input(f"Neues Accel-Gewicht (aktuell: {cand_accel_w:.2f}): ").strip().replace(",",".")
            if val:
                settings['candidate_accel_weight'] = _to_float(val, cand_accel_w)
                made_change = True
        elif choice == "19": # Min Primary Liquidity
            val = input(f"Min Primary Liquidity in Mio EUR (aktuell: {cand_min_vol / 1_000_000:.1f}): ").strip().replace(",",".")
            if val:
                settings['candidate_min_avg_volume_eur'] = _to_float(val, cand_min_vol / 1_000_000) * 1_000_000
                made_change = True
        elif choice == "20":
            val = input(f"Min Trust Score (0-3, aktuell: {cand_min_trust}): ").strip().replace(",", ".")
            if val:
                settings['candidate_min_trust_score'] = int(_to_float(val, cand_min_trust))
                made_change = True
        elif choice == "21":
            val = input(f"Neues Score-Minimum (aktuell: {cand_score_min:.4f}): ").strip().replace(",", ".")
            if val:
                settings['candidate_score_min'] = _to_float(val, cand_score_min)
                made_change = True
        elif choice == "22":
            val = input(f"Nur Top-{int(cand_top_pct*100)}% zulassen? (j/n, aktuell: {'j' if cand_require_top else 'n'}): ").strip().lower()
            if val:
                settings['candidate_require_top_percent'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "23":
            val = input(f"Neue Nachkauf-Schwelle in % (aktuell: {cand_top_pct*100:.1f}%): ").strip().replace(",", ".")
            if val:
                settings['candidate_top_percent_threshold'] = _to_float(val, cand_top_pct) / 100.0
                made_change = True
        elif choice == "24":
            val = input(f"Neues Gewicht 12M (aktuell: {mom_w12:.2f}): ").strip().replace(",", ".")
            if val:
                settings['mom_weight_12m'] = _to_float(val, mom_w12)
                made_change = True
        elif choice == "25":
            val = input(f"Neues Gewicht 6M (aktuell: {mom_w6:.2f}): ").strip().replace(",", ".")
            if val:
                settings['mom_weight_6m'] = _to_float(val, mom_w6)
                made_change = True
        elif choice == "26":
            val = input(f"Neues Gewicht 3M (aktuell: {mom_w3:.2f}): ").strip().replace(",", ".")
            if val:
                settings['mom_weight_3m'] = _to_float(val, mom_w3)
                made_change = True
        elif choice == "27":
            val = input(f"Neuer Lookback 12M in Tagen (aktuell: {mom_lb12}): ").strip()
            if val:
                settings['mom_lookback_12m'] = int(_to_float(val, mom_lb12))
                made_change = True
        elif choice == "28":
            val = input(f"Neuer Lookback 6M in Tagen (aktuell: {mom_lb6}): ").strip()
            if val:
                settings['mom_lookback_6m'] = int(_to_float(val, mom_lb6))
                made_change = True
        elif choice == "29":
            val = input(f"Neuer Lookback 3M in Tagen (aktuell: {mom_lb3}): ").strip()
            if val:
                settings['mom_lookback_3m'] = int(_to_float(val, mom_lb3))
                made_change = True
        elif choice == "30":
            val = input(f"Neuer Vol-Lookback in Tagen (aktuell: {mom_vol_lb}): ").strip()
            if val:
                settings['mom_vol_lookback'] = int(_to_float(val, mom_vol_lb))
                made_change = True
        elif choice == "31":
            preset = input("Preset [s=Standard, d=Defensiv, y=Dynamisch]: ").strip().lower()
            preset_aliases = {
                's': 'standard',
                'std': 'standard',
                'standard': 'standard',
                'a': 'standard',
                'aus': 'standard',
                'ausgewogen': 'standard',
                'd': 'defensiv',
                'def': 'defensiv',
                'defensiv': 'defensiv',
                'k': 'defensiv',
                'kon': 'defensiv',
                'konservativ': 'defensiv',
                'y': 'dynamisch',
                'dyn': 'dynamisch',
                'dynamisch': 'dynamisch',
                'g': 'dynamisch',
                'agg': 'dynamisch',
                'aggressiv': 'dynamisch',
            }
            preset_key = preset_aliases.get(preset)
            if not preset_key:
                print("Ungueltiges Preset.")
                continue
            settings.update(settings_catalog_core.get_settings_presets()[preset_key]['values'])
            made_change = True
        elif choice == "32":
            val = input(f"Cluster nutzen? (j/n, aktuell: {'j' if cluster_enabled else 'n'}): ").strip().lower()
            if val:
                settings['cluster_enabled'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "33":
            val = input(f"Kandidaten-Filter nach Top-Clustern? (j/n, aktuell: {'j' if cand_use_cluster else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_cluster_filter'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "34":
            val = input(f"Top-Cluster Anzahl (aktuell: {cluster_top_n}): ").strip()
            if val:
                settings['cluster_top_n'] = int(_to_float(val, cluster_top_n))
                made_change = True
        elif choice == "35":
            val = input(f"Min Cluster-Groesse (aktuell: {cluster_min_size}): ").strip()
            if val:
                settings['cluster_min_size'] = int(_to_float(val, cluster_min_size))
                made_change = True
        elif choice == "36":
            val = input(f"Neues Gewicht Mom12 (aktuell: {c_w_mom:.2f}): ").strip().replace(",", ".")
            if val:
                _rebalance_cluster_weights("mom12", _to_float(val, c_w_mom))
                print("Hinweis: Gewichte wurden proportional angepasst (Summe = 1.0).")
                made_change = True
        elif choice == "37":
            val = input(f"Neues Gewicht Mom6 (aktuell: {c_w_mom6:.2f}): ").strip().replace(",", ".")
            if val:
                _rebalance_cluster_weights("mom6", _to_float(val, c_w_mom6))
                print("Hinweis: Gewichte wurden proportional angepasst (Summe = 1.0).")
                made_change = True
        elif choice == "38":
            val = input(f"Neues Gewicht Accel (aktuell: {c_w_acc:.2f}): ").strip().replace(",", ".")
            if val:
                _rebalance_cluster_weights("accel", _to_float(val, c_w_acc))
                print("Hinweis: Gewichte wurden proportional angepasst (Summe = 1.0).")
                made_change = True
        elif choice == "39":
            try:
                if os.path.exists(CONFIG['history_cache_file']):
                    os.remove(CONFIG['history_cache_file'])
                    print("Kurs-Cache (history_cache.json) wurde geloescht.")
                else:
                    print("Kurs-Cache existiert nicht (bereits geloescht).")
                made_change = True
            except Exception as e:
                print(f"Fehler beim Loeschen des Kurs-Cache: {e}")
        elif choice == "40":
            try:
                if os.path.exists(CONFIG['etf_cache_file']):
                    os.remove(CONFIG['etf_cache_file'])
                    print("ETF-Cache (etf_holdings_cache.pkl) wurde geloescht.")
                else:
                    print("ETF-Cache existiert nicht (bereits geloescht).")
                made_change = True
            except Exception as e:
                print(f"Fehler beim Loeschen des ETF-Cache: {e}")
        elif choice == "41":
            for key in ['cache_duration_hours', 'etf_cache_duration_hours', 'info_cache_unknown_expiry_days']:
                if key in settings:
                    del settings[key]
            print("Cache-Einstellungen auf Standard zurueckgesetzt.")
            made_change = True
        elif choice == "42":
            settings['heatmap_warn_percent'] = 20.0
            settings['heatmap_full_percent'] = 25.0
            print("Heatmap-Einstellungen auf Standard zurueckgesetzt.")
            made_change = True
        elif choice == "43":
            val = input(f"RSL 1W-Change nutzen? (j/n, aktuell: {'j' if cand_use_rsl_change else 'n'}): ").strip().lower()
            if val:
                settings['candidate_use_rsl_change_1w'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "44":
            val = input(f"Neues RSL 1W-Gewicht (aktuell: {cand_rsl_w:.3f}): ").strip().replace(",", ".")
            if val:
                settings['candidate_rsl_change_weight'] = _to_float(val, cand_rsl_w)
                made_change = True
        elif choice == "45":
            val = input(f"Branchen-Trend anzeigen? (j/n, aktuell: {'j' if trend_enabled else 'n'}): ").strip().lower()
            if val:
                settings['industry_trend_enabled'] = val in ("j", "y", "ja", "yes", "true", "1")
                made_change = True
        elif choice == "46":
            val = input(f"Branchen-Trend Wochen (aktuell: {trend_weeks}): ").strip()
            if val:
                settings['industry_trend_weeks'] = int(_to_float(val, trend_weeks))
                made_change = True
        elif choice == "47":
            val = input(f"Info-Fetch Logmodus (ruhig/normal, aktuell: {'ruhig' if info_quiet else 'normal'}): ").strip().lower()
            if val:
                settings['info_fetch_quiet'] = val in ("ruhig", "quiet", "q", "1", "j", "ja", "y", "yes", "true")
                made_change = True
        elif choice == "48":
            val = input(f"Rate-Limit Min-Delay in Sekunden (aktuell: {rate_min_delay:.1f}): ").strip().replace(",", ".")
            if val:
                settings['rate_limit_delay_min_s'] = _to_float(val, rate_min_delay)
                made_change = True
        elif choice == "49":
            val = input(f"Rate-Limit Max-Delay in Sekunden (aktuell: {rate_max_delay:.1f}): ").strip().replace(",", ".")
            if val:
                settings['rate_limit_delay_max_s'] = _to_float(val, rate_max_delay)
                made_change = True
        elif choice == "50":
            val = input(f"Rate-Limit Log-Intervall (Events, aktuell: {rate_log_every}): ").strip().replace(",", ".")
            if val:
                settings['rate_limit_log_every'] = int(_to_float(val, rate_log_every))
                made_change = True
        else:
            print("Ungueltige Auswahl.")
            continue
        if made_change:
            nw12, nw6, nw3 = final_support_core.normalize_weights(
                settings.get('mom_weight_12m', CONFIG['mom_weight_12m']),
                settings.get('mom_weight_6m', CONFIG['mom_weight_6m']),
                settings.get('mom_weight_3m', CONFIG['mom_weight_3m'])
            )
            settings['mom_weight_12m'] = nw12
            settings['mom_weight_6m'] = nw6
            settings['mom_weight_3m'] = nw3
            c_w_mom_norm, c_w_mom6_norm, c_w_acc_norm = final_support_core.normalize_weights(
                settings.get('cluster_score_w_mom12', CONFIG['cluster_score_w_mom12']),
                settings.get('cluster_score_w_mom6', CONFIG['cluster_score_w_mom6']),
                settings.get('cluster_score_w_accel', CONFIG['cluster_score_w_accel'])
            )
            settings['cluster_score_w_mom12'] = c_w_mom_norm
            settings['cluster_score_w_mom6'] = c_w_mom6_norm
            settings['cluster_score_w_accel'] = c_w_acc_norm
            save_user_settings(settings)
            apply_user_settings(settings)
            print("Einstellungen gespeichert und angewendet.")
def configure_user_settings_interactive():
    return settings_ui_core.configure_user_settings_interactive(
        config=CONFIG,
        defaults=settings_catalog_core.get_user_settings_defaults(),
        presets=settings_catalog_core.get_settings_presets(),
        load_user_settings=load_user_settings,
        save_user_settings=save_user_settings,
        apply_user_settings=apply_user_settings,
        to_float=_to_float,
        sanitize_heatmap=_sanitize_heatmap_thresholds,
        legacy_menu=_configure_user_settings_legacy,
    )
class TeeStream:
    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]
    def write(self, data):
        for stream in self.streams:
            stream.write(data)
        return len(data)
    def flush(self):
        for stream in self.streams:
            stream.flush()
    def isatty(self):
        return any(getattr(stream, "isatty", lambda: False)() for stream in self.streams)
    def fileno(self):
        if not self.streams:
            raise OSError("no stream")
        return self.streams[0].fileno()
class ConsoleCapture:
    def __init__(self, capture_file: str):
        self.capture_file = capture_file
        self.file_handle = None
        self.orig_stdout = None
        self.orig_stderr = None
    @staticmethod
    def _set_stream_handlers(target_stream):
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                handler.setStream(target_stream)
    def __enter__(self):
        capture_dir = os.path.dirname(self.capture_file)
        if capture_dir:
            os.makedirs(capture_dir, exist_ok=True)
        self.file_handle = open(self.capture_file, 'w', encoding='utf-8')
        self.orig_stdout = sys.stdout
        self.orig_stderr = sys.stderr
        tee_out = TeeStream(self.orig_stdout, self.file_handle)
        tee_err = TeeStream(self.orig_stderr, self.file_handle)
        sys.stdout = tee_out
        sys.stderr = tee_err
        self._set_stream_handlers(tee_err)
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self.orig_stderr is not None:
                self._set_stream_handlers(self.orig_stderr)
        except Exception:
            pass
        sys.stdout = self.orig_stdout if self.orig_stdout is not None else sys.__stdout__
        sys.stderr = self.orig_stderr if self.orig_stderr is not None else sys.__stderr__
        if self.file_handle:
            self.file_handle.flush()
            self.file_handle.close()
        return False
def save_analysis_snapshot(
    stock_results: List["StockData"],
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    integrity_drops_df: pd.DataFrame = None,
    universe_audit_df: pd.DataFrame = None
):
    return app_support_core.save_analysis_snapshot(
        snapshot_file=CONFIG['last_analysis_snapshot_file'],
        stock_results=stock_results,
        selected_syms=selected_syms,
        etf_options=etf_options,
        save_json_config=save_json_config,
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df,
    )
def load_analysis_snapshot() -> Optional[Dict[str, Any]]:
    return app_support_core.load_analysis_snapshot(
        snapshot_file=CONFIG['last_analysis_snapshot_file'],
        load_json_config=load_json_config,
        currency_rates=CURRENCY_RATES,
        to_float=_to_float,
    )
def select_etf_interactive() -> Tuple[List[str], Dict[str, Any]]:
    return app_support_core.select_etf_interactive(
        config=CONFIG,
        load_json_config=load_json_config,
        save_json_config=save_json_config,
        parse_etf_selection_input=_parse_etf_selection_input,
        parse_ishares_url=parse_ishares_url,
    )
    etf_config = load_json_config(CONFIG['etf_config_file'])
    if not etf_config or 'options' not in etf_config or 'selected_symbols' not in etf_config:
        etf_config = {'selected_symbols': [], 'options': {}}
   
    while True:
        current_selection = etf_config.get('selected_symbols', [])
        etf_options = etf_config.get('options', {})
        
        if current_selection:
            print(f"\nAktuelle Auswahl: {', '.join(current_selection)}")
            quick_reply = input("Auswahl aendern? (j/n oder direkte ETF-Auswahl wie IVV): ").strip()
            if not quick_reply:
                quick_reply = "n"
            if quick_reply.lower() not in ("j", "y", "ja", "yes", "n", "no", "nein"):
                parsed_selection = _parse_etf_selection_input(quick_reply, etf_options)
                if parsed_selection:
                    etf_config['selected_symbols'] = parsed_selection
                    save_json_config(CONFIG['etf_config_file'], etf_config)
                    return parsed_selection, etf_options
                print("Direkte ETF-Auswahl nicht erkannt. Wechsle in das Auswahlmenue.")
            elif quick_reply.lower() not in ("j", "y", "ja", "yes"):
                return current_selection, etf_options
        
        print("\nWaehle ETFs (Mehrfachauswahl moeglich, z.B. '1, 3, 5', 'IVV, SOXX' oder 'all'):")
        opts = list(etf_options.keys())
        for i, sym in enumerate(opts, 1):
            print(f"{i}. {sym} - {etf_options[sym]['name']}")
        print(f"{len(opts)+1}. XETRA - Deutsche Börse Xetra")
        print(f"{len(opts)+2}. FRA   - Deutsche Börse Frankfurt")
        
        print("\nOder 'all' für alles, 'add' zum Hinzufügen, 'remove' zum Entfernen, '?' für Hilfe.")
        choice = input("Wahl (z.B. 1,2,FRA): ").strip().lower()
        if choice == '?':
            print("\nHilfe ETF-Auswahl:")
            print(" - Nummern: Auswahl per Index, z.B. 1,3,5")
            print(" - Symbole: direkte Auswahl per Symbol, z.B. IVV oder IVV,SOXX")
            print(" - all: alle ETFs auswaehlen")
            print(" - add: ETF manuell oder per iShares-URL hinzufuegen")
            print(" - remove: ETF aus Liste entfernen")
            print(" - Enter: Eingabe wiederholen")
            continue
       
        if choice == 'add':
            user_in = input("Gib das Symbol ODER die volle iShares CSV-URL ein: ").strip()
            parsed = parse_ishares_url(user_in)
            if parsed:
                print(f"URL erkannt! Symbol: {parsed['symbol']}")
                name = input(f"Name fuer {parsed['symbol']}: ").strip()
                etf_config['options'][parsed['symbol']] = {'name': name, 'id': parsed['id'], 'slug': parsed['slug']}
            else:
                s = user_in.upper()
                if not s: continue
                etf_config['options'][s] = {'name': input("Name: "), 'id': input("ID: "), 'slug': input("Slug: ")}
           
            save_json_config(CONFIG['etf_config_file'], etf_config)
            print("ETF-Liste aktualisiert.")
            continue
        elif choice == 'remove':
            user_in = input("Symbol oder Nummer zum Entfernen: ").strip()
            symbol_to_remove = None
            if user_in.isdigit():
                idx = int(user_in) - 1
                if 0 <= idx < len(opts):
                    symbol_to_remove = opts[idx]
            else:
                if user_in.upper() in cast(Dict, etf_config['options']):
                    symbol_to_remove = user_in.upper()
            if symbol_to_remove:
                del etf_config['options'][symbol_to_remove]
                if symbol_to_remove in etf_config.get('selected_symbols', []):
                    etf_config['selected_symbols'].remove(symbol_to_remove)
                save_json_config(CONFIG['etf_config_file'], etf_config)
                print(f"{symbol_to_remove} wurde entfernt.")
            else:
                print(f"'{user_in}' nicht in der Liste gefunden.")
            continue
       
        elif choice == 'all':
             full_selection = opts + ["XETRA", "FRA"]
             etf_config['selected_symbols'] = full_selection
             save_json_config(CONFIG['etf_config_file'], etf_config)
             return full_selection, etf_options
        else:
            try:
                new_selection = _parse_etf_selection_input(choice, etf_options)
                if new_selection:
                    etf_config['selected_symbols'] = new_selection
                    save_json_config(CONFIG['etf_config_file'], etf_config)
                    return new_selection, etf_options
                else:
                    print("Keine gueltige Auswahl.")
            except: print("Ungueltig.")
def render_analysis_output(
    stock_results: List[StockData],
    portfolio_mgr: PortfolioManager,
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    update_last_run_cfg: bool = True,
    data_mgr: Optional[MarketDataManager] = None,
    industry_summary_df: Optional[pd.DataFrame] = None,
    cluster_summary_df: Optional[pd.DataFrame] = None,
    suggest_portfolio_candidates: Optional[Callable] = None,
    market_regime: Optional[Dict[str, Any]] = None,
    integrity_drops_df: Optional[pd.DataFrame] = None,
    universe_audit_df: Optional[pd.DataFrame] = None,
    watchlist_symbols: Optional[set] = None
):
    return app_support_core.render_analysis_output(
        stock_results=stock_results,
        portfolio_mgr=portfolio_mgr,
        selected_syms=selected_syms,
        etf_options=etf_options,
        config=CONFIG,
        logger=logger,
        save_json_config=save_json_config,
        save_dataframe_safely_func=save_dataframe_safely,
        save_excel_report_safely=save_excel_report_safely,
        build_console_symbols=console_core.build_console_symbols,
        build_yahoo_quote_url_func=build_yahoo_quote_url,
        data_mgr=data_mgr,
        update_last_run_cfg=update_last_run_cfg,
        industry_summary_df=industry_summary_df,
        cluster_summary_df=cluster_summary_df,
        suggest_portfolio_candidates=suggest_portfolio_candidates,
        market_regime=market_regime,
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df,
        watchlist_symbols=watchlist_symbols,
    )
def rerender_last_analysis() -> bool:
    """
    Baut die Analyse aus dem letzten Snapshot neu auf, ohne Daten neu zu laden.
    
    Returns:
        True bei Erfolg, False bei Fehler.
    """
    # Lade die Benutzereinstellungen, um sicherzustellen, dass die Anzeige aktuell ist
    try:
        user_settings = load_user_settings()
        apply_user_settings(user_settings)
        logger.info("Benutzereinstellungen fuer Re-Render geladen und angewendet.")
    except Exception as e:
        logger.error(f"Fehler beim Anwenden der Benutzereinstellungen fuer Re-Render: {e}")
        # Trotzdem weitermachen, aber mit potenziell veralteten Config-Werten

    # Lade den Snapshot der letzten Analyse
    snapshot = load_analysis_snapshot()
    if not snapshot:
        print("\nKein letzter Analysesnapshot gefunden. Bitte zuerst einen normalen Lauf ausfuehren.")
        return False
    stock_results = snapshot['stock_results']

    selected_syms = snapshot.get('selected_syms', [])
    etf_options = snapshot.get('etf_options', {})

    saved_at = snapshot.get('saved_at', '')
    logger.info(f"Nutze letzten Analysesnapshot ohne neuen Download. Stand: {saved_at or 'unbekannt'}")
    logger.info(f"Re-Render: Regeln werden neu auf Snapshot angewendet. Stand: {saved_at or 'unbekannt'}")
    portfolio_mgr = PortfolioManager(CONFIG['portfolio_file'])
    print(f"\nLetzter Datenstand geladen: {saved_at or 'unbekannt'}")
    print(f"Analyse wird ohne neuen Download mit {len(stock_results)} gespeicherten Werten neu aufgebaut.")

    # Daten aus Snapshot extrahieren (Fallback auf verschiedene Keys für Abwärtskompatibilität)
    integrity_drops_df = snapshot.get('integrity_drops_df')
    if integrity_drops_df is None or (isinstance(integrity_drops_df, pd.DataFrame) and integrity_drops_df.empty):
        integrity_drops_df = pd.DataFrame(snapshot.get('integrity_drops', []))
        
    universe_audit_df = snapshot.get('universe_audit_df')
    if universe_audit_df is None or (isinstance(universe_audit_df, pd.DataFrame) and universe_audit_df.empty):
        universe_audit_df = pd.DataFrame(snapshot.get('universe_audit', []))

    # Re-Evaluation der validen Stocks (erlaubt CONFIG-Anpassungen für Warnungen)
    stock_results, new_warnings_df = filter_stock_results_for_rsl_integrity(stock_results)
    
    # Kombinieren mit den gespeicherten Fehlern (Wichtig: Snapshot-Daten mit excluded=True müssen erhalten bleiben)
    if not new_warnings_df.empty:
        if integrity_drops_df.empty:
            integrity_drops_df = new_warnings_df
        else:
            integrity_drops_df = pd.concat([integrity_drops_df, new_warnings_df], ignore_index=True).drop_duplicates(subset=['ticker'], keep='first')

    if not integrity_drops_df.empty:
        integrity_summary = quality_core.summarize_integrity_flags(integrity_drops_df)
        print(f"\nIntegritaets-Check Ergebnis: {quality_core.quality_gate_status(integrity_summary)}")

    # --- NEU: Metadata-Repair fuer Snapshots ---
    # Wir laden die Caches, um leere Felder im Snapshot on-the-fly zu füllen
    raw_countries = load_json_config(CONFIG['country_cache_file'])
    country_lookup = {str(k).strip().upper(): v for k, v in cast(Dict, raw_countries).items()} if isinstance(raw_countries, dict) else {}
    raw_info = load_json_config(CONFIG['ticker_info_cache_file'])
    info_cache = cast(Dict[str, Any], raw_info) if isinstance(raw_info, dict) else {}
    
    # Globalen ISIN-Lookup und Ticker-ISIN-Map für Reparatur bauen
    # Globalen ISIN-Lookup, Ticker-ISIN-Map und Name-ISIN-Map für Reparatur bauen
    global_isin_map = {}
    ticker_to_isin_cache = {}
    name_to_isin_cache = {}
    for t_sym, t_info in info_cache.items():
        isin = t_info.get('isin')
        if isin and len(isin) > 5 and isin not in ("NAN", "NONE"):
            ticker_to_isin_cache[t_sym.upper()] = isin
            # Namens-Brücke bauen: Normalisierte Namen zu ISIN mappen
            n_key = normalize_name_for_dedup(t_info.get('longName', ''))
            if n_key and len(n_key) > 3:
                name_to_isin_cache[n_key] = isin

            mkt_cap = _to_float(t_info.get('marketCap', 0), 0)
            if isin not in global_isin_map or mkt_cap > _to_float(global_isin_map[isin].get('marketCap', 0), 0):
                global_isin_map[isin] = t_info

    repaired_count = 0
    for s in stock_results:
        y_sym = str(s.yahoo_symbol).strip().upper()
        # 1. Land reparieren (Prio 1: Country-Cache, Prio 2: Yahoo-Info-Cache)
        if not s.land or str(s.land).lower() in ('nan', 'unknown', 'none', ''):
            if y_sym in country_lookup:
                s.land = country_lookup[y_sym]
                repaired_count += 1
            elif y_sym in info_cache:
                s.land = info_cache.get(y_sym, {}).get('country', '')
                if s.land: repaired_count += 1
        
        # 2. Branche/Sektor reparieren (falls im Snapshot 'Unknown')
        if not s.industry or s.industry == 'Unknown':
            if y_sym in info_cache:
                s.industry = info_cache[y_sym].get('industry', 'Unknown')
                s.sector = info_cache[y_sym].get('sector', 'Unknown')

        # 3. Marktwert reparieren via ISIN-Map (Primär-Listing-Daten nutzen)
        if _safe_positive_float(getattr(s, 'market_value', 0.0)) <= 0:
            isin_s = getattr(s, 'isin', '')
            if not isin_s or isin_s.lower() in ('nan', 'none'):
                isin_s = ticker_to_isin_cache.get(y_sym, '')
            
            # Letzte Rettung: Namens-Abgleich falls ISIN fehlt
            if not isin_s:
                isin_s = name_to_isin_cache.get(normalize_name_for_dedup(s.name), '')
            
            info_to_use = info_cache.get(y_sym, {})
            found_cap = _resolve_market_cap_from_info(info_to_use)
            if found_cap <= 0 and isin_s in global_isin_map:
                info_to_use = global_isin_map[isin_s]
            
            repaired_market_cap = _resolve_market_cap_from_info(info_to_use)
            if repaired_market_cap > 0:
                rate = get_currency_rate_for_ticker(y_sym)
                s.market_cap = repaired_market_cap
                s.market_value = repaired_market_cap * rate

    if repaired_count > 0:
        logger.info(f"Snapshot-Repair: {repaired_count} fehlende Länderinformationen aus Caches wiederhergestellt.")

    # --- KONSOLIDIERUNG & FILTERUNG ---
    # Blacklist anwenden, bevor Ränge neu berechnet werden
    blacklist = set(load_json_config(CONFIG['blacklist_file'], is_list=True))
    blacklist = {x.strip().upper() for x in blacklist if x}
    if blacklist:
        orig_count = len(stock_results)
        stock_results = [s for s in stock_results if str(s.yahoo_symbol).strip().upper() not in blacklist]
        logger.info(f"Snapshot gefiltert: {orig_count - len(stock_results)} Blacklist-Eintraege entfernt.")

    # Ränge einheitlich neu berechnen
    ranking_core.apply_standard_rankings(stock_results)

    # Berechne Metriken und Summaries für die Anzeige
    synchronize_portfolio_symbols_with_stock_results(portfolio_mgr, stock_results)
    ranking_core.apply_relative_context_metrics(stock_results)
    industry_summary_df = summary_core.build_industry_rsl_summary(stock_results, CONFIG)
    market_regime = ranking_core.calculate_market_regime(stock_results)
    cluster_summary_df, cluster_map = summary_core.build_momentum_cluster_summary(stock_results, CONFIG)
    
    if cluster_map:
        for s in stock_results:
            sym = str(getattr(s, "yahoo_symbol", "")).strip().upper()
            s.mom_cluster = cluster_map.get(sym, "")
            
    watchlist_symbols = load_watchlist_symbols(str(CONFIG.get('watchlist_file', '')))
    save_home_market_rsl_audit(stock_results)
    render_analysis_output(
        stock_results=stock_results,
        portfolio_mgr=portfolio_mgr,
        selected_syms=selected_syms,
        etf_options=etf_options,
        update_last_run_cfg=False,
        data_mgr=None,
        suggest_portfolio_candidates=candidate_core.suggest_portfolio_candidates,
        industry_summary_df=industry_summary_df,
        cluster_summary_df=cluster_summary_df,
        market_regime=market_regime,
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df,
        watchlist_symbols=watchlist_symbols
    )
    return True

def show_ticker_history_interactive():
    app_support_core.show_ticker_history_interactive(yf)

def _auto_adjust_delays() -> None:
    app_support_core.auto_adjust_delays(CONFIG, load_json_config, save_json_config, logger)

# --- MAIN EXECUTION ---
def _setup_run_environment() -> None:
    """Initialisiert FX-Kurse und passt Delays an."""
    logger.info("\n--- GLOBAL RSL V68 (Dashboard Plus) ---")
def _initialize_run_settings(data_mgr: MarketDataManager) -> None:
    app_support_core.initialize_run_settings(
        data_mgr=data_mgr,
        config=CONFIG,
        logger=logger,
        load_json_config=load_json_config,
        save_json_config=save_json_config,
        currency_rates=CURRENCY_RATES,
    )

def show_main_menu(has_snapshot: bool) -> str:
    """Zeigt das Hauptmenü an und gibt die Auswahl des Benutzers zurück."""
    return app_support_core.show_main_menu(has_snapshot)

def _prepare_ticker_universe(selected_syms: List[str], etf_options: Dict[str, Any]) -> pd.DataFrame:
    """Lädt, integriert und bereinigt das Ticker-Universum."""
    return app_support_core.prepare_ticker_universe(
        selected_syms=selected_syms,
        etf_options=etf_options,
        config=CONFIG,
        logger=logger,
        make_progress_fn=make_progress,
        download_ishares_csv=download_ishares_csv,
    )

def run_analysis_pipeline(
    data_mgr: MarketDataManager, 
    portfolio_mgr: PortfolioManager, 
    first_seen_mgr: FirstSeenManager,
    mapper: TickerMapper,
    filter_tokens: Optional[Set[str]] = None,
    force_clear_cache: bool = False
) -> None:
    """Zentrale Pipeline für den vollständigen Analyse-Workflow (Refactored)."""
    load_dotenv()
    _setup_run_environment()

    # Cache-Steuerung: Muss VOR der Initialisierung passieren, damit die Logs stimmen
    if force_clear_cache:
        data_mgr.clear_cache()
        logger.info("Kursdaten-Cache (History) wurde geleert.")

    _initialize_run_settings(data_mgr)

    # --- 1. SETUP & SELECTION ---
    manual_fix = cast(Dict, load_json_config(CONFIG['manual_fix_file']))
    blacklist = {x.strip().upper() for x in cast(List, load_json_config(CONFIG['blacklist_file'], is_list=True)) if x}
    
    last_run_cfg = load_json_config(CONFIG['last_run_config_file'])
    use_last_settings = False
    selected_syms = []
    etf_options = {}
    if isinstance(last_run_cfg, dict) and last_run_cfg:
        etf_config = cast(Dict[str, Any], load_json_config(CONFIG['etf_config_file']))
        etf_options = cast(Dict[str, Any], etf_config.get('options', {}))
        selected_syms = cast(List[str], etf_config.get('selected_symbols', []))
       
        if selected_syms:
            all_etf_keys = list(etf_options.keys())
            is_all_selected = len(all_etf_keys) > 0 and len(selected_syms) == len(all_etf_keys) and set(selected_syms) == set(all_etf_keys)
            selection_text = "Alle" if is_all_selected else ', '.join(selected_syms)

            print(f"\nLetzte ETF-Auswahl: {selection_text}")
            quick_reply = input("Diese Auswahl wiederholen? (j/n oder direkte ETF-Auswahl wie IVV): ").strip()
            if not quick_reply:
                quick_reply = "j"
            if quick_reply.lower() not in ("j", "y", "ja", "yes", "n", "no", "nein"):
                parsed_selection = _parse_etf_selection_input(quick_reply, etf_options)
                if parsed_selection:
                    selected_syms = parsed_selection
                    etf_config['selected_symbols'] = parsed_selection
                    save_json_config(CONFIG['etf_config_file'], etf_config)
                    use_last_settings = True
                else:
                    print("Direkte ETF-Auswahl nicht erkannt. Wechsle in das Auswahlmenue.")
                    use_last_settings = False
            elif quick_reply.lower() in ("j", "y", "ja", "yes"):
                use_last_settings = True
            else:
                use_last_settings = False
    if not use_last_settings:
        selected_syms, etf_options = select_etf_interactive()

    print_run_status_header(
        selected_syms=selected_syms,
        portfolio_size=len(portfolio_mgr.current_portfolio),
        etf_options=etf_options
    )
   
    # PERFORMANCE START
    perf_start_time = time.time()
    # --- 2. UNIVERSE PREPARATION ---
    audit_trail = {} # Ticker -> {'status': str, 'detail': str, 'yahoo': str}

    df = _prepare_ticker_universe(selected_syms, etf_options)
    if df.empty:
        logger.error("Ticker-Universum konnte nicht vorbereitet werden.")
        return

    # --- 3. FILTERING (Applied if called from Ad-hoc menu) ---
    for t in df['Ticker'].unique():
        audit_trail[t] = {'status': 'PENDING', 'detail': '', 'yahoo': ''}

    # 1. Globalen ISIN-Lookup bauen (VOR der Filterung fuer Ad-hoc)
    ticker_to_isin_cache = {}
    name_to_isin_cache = {}
    global_isin_map = {}
    for t_sym, t_info in data_mgr.info_cache.items():
        isin = t_info.get('isin')
        if isin and len(isin) > 5 and isin not in ("NAN", "NONE"):
            ticker_to_isin_cache[t_sym.upper()] = isin
            n_key = normalize_name_for_dedup(t_info.get('longName', ''))
            if n_key: name_to_isin_cache[n_key] = isin
            mkt_cap = float(t_info.get('marketCap', 0) or 0)
            if isin not in global_isin_map or mkt_cap > float(global_isin_map[isin].get('marketCap', 0) or 0):
                global_isin_map[isin] = t_info

    if filter_tokens:
        mask = df['Ticker'].str.upper().str.strip().isin(filter_tokens)
        if 'ISIN' in df.columns:
            mask |= df['ISIN'].str.upper().str.strip().isin(filter_tokens).fillna(False)
        
        df_filtered = df[mask].copy()
        found_tokens = set(df_filtered['Ticker'].str.upper().str.strip())
        if 'ISIN' in df.columns:
            found_tokens.update(df_filtered['ISIN'].str.upper().str.strip().dropna())
        
        missing_tokens = filter_tokens - found_tokens
        if missing_tokens:
            ad_hoc_rows = []
            for token in missing_tokens:
                is_isin = bool(re.match(r'^[A-Z]{2}[A-Z0-9]{9}\d$', str(token)))
                ticker_to_use = token if not is_isin else ""
                
                # Falls ISIN, versuche sie via Yahoo Search in einen Ticker aufzuloesen
                if is_isin:
                    try:
                        search_res = yf.Search(token, max_results=1).tickers
                        if search_res:
                            ticker_to_use = search_res[0].get('symbol', "")
                    except Exception: pass

                ad_hoc_rows.append({
                    'Ticker': ticker_to_use,
                    'Name': f"Ad-hoc: {token}", 
                    'ISIN': token if is_isin else ticker_to_isin_cache.get(token.upper(), ""),
                    'Sector': 'Unknown', 'Industry': 'Unknown', 'Land': 'Unknown',
                    'Market Value': 0.0, 'Source_ETF': 'MANUAL', 'Listing_Source': 'MANUAL'
                })
            if ad_hoc_rows:
                df_filtered = pd.concat([df_filtered, pd.DataFrame(ad_hoc_rows)], ignore_index=True)
        
        if not df_filtered.empty:
            df = df_filtered
            logger.info(f"Ad-hoc Filter aktiv: Analyse beschraenkt auf {len(df)} Ticker.")
        else:
            logger.warning("Ad-hoc Filter ergab keine Treffer im Universum.")

    final_rows = len(df)

    history_symbol_overrides = final_support_core.build_history_symbol_overrides(
        df, df, LOCATION_SUFFIX_MAP, UNSUPPORTED_EXCHANGES, EXCHANGE_SUFFIX_MAP
    )

    # --- VERARBEITUNG ---
    # --- 3. DATA COLLECTION & PROCESSING ---
    batch_queue = []
    complex_queue = []
    unresolved_origs = []
    sector_skips = {} # orig -> sector
    
    dropped_critical = []

    for _, row in df.iterrows():
        orig = row['Ticker']
        land = row.get('Land', 'Unknown')
        audit_trail[orig] = {'status': 'PENDING', 'detail': '', 'yahoo': ''}
        orig_clean = sanitize_ticker_symbol(orig)
        preferred_history_sym = history_symbol_overrides.get(str(orig_clean).strip().upper(), "")
        u_key = f"{orig}_{land}"
       
        if str(orig).strip().upper() in blacklist or (orig_clean and orig_clean in blacklist):
            continue
        if orig in manual_fix and manual_fix[orig]:
            fixed_sym = sanitize_ticker_symbol(manual_fix[orig])
            if is_plausible_ticker(fixed_sym):
                batch_queue.append((u_key, orig, fixed_sym, row))
            else:
                unresolved_origs.append(orig)
                complex_queue.append((u_key, orig, row))
            continue
        cached_sym = mapper.get(u_key)
        if cached_sym:
            cached_clean = sanitize_ticker_symbol(cached_sym)
            if not is_plausible_ticker(cached_clean):
                cached_sym = None
            else:
                cached_sym = cached_clean
       
        if cached_sym and str(cached_sym).strip().upper() in blacklist:
            continue
        if cached_sym:
            chosen_sym = preferred_history_sym or cached_sym
            batch_queue.append((u_key, orig, chosen_sym, row))
        else:
            cands = [preferred_history_sym] if preferred_history_sym else generate_candidates(orig, land, row.get('Exchange', ''))
            if cands:
                batch_queue.append((u_key, orig, cands[0], row))
            else:
                unresolved_origs.append(orig)
                complex_queue.append((u_key, orig, row))
    stock_results: List[StockData] = []
    # --- INTELLIGENTE INDUSTRY INFO PRÜFUNG ---
    # Identifiziere Ticker, die weder im Info-Cache noch im iShares-DF Sektor-Daten haben
    missing_info_syms = []
    for item in batch_queue:
        y_sym = item[2]
        if y_sym not in data_mgr.info_cache:
            missing_info_syms.append(y_sym)
    missing_info_syms = sorted(list(set(missing_info_syms)))

    if missing_info_syms:
        logger.info(f"Lade Industry-Informationen fuer {len(missing_info_syms)} Ticker nach (einmalig)...")
        if len(missing_info_syms) < 5:
            time.sleep(2) # Kurze Atempause fuer die API
        # Drosselung auf max 2 Worker für Info-Fetch, um Yahoo Rate-Limits zu umgehen
        with make_progress(total=len(missing_info_syms), desc="Industry Info") as pbar, \
             concurrent.futures.ThreadPoolExecutor(max_workers=min(2, CONFIG['max_workers'])) as executor:
            try:
                futures = {executor.submit(data_mgr.fetch_and_cache_info, sym): sym for sym in missing_info_syms}
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        future.result() 
                    except Exception as e:
                        logger.debug(f"Info-Fetch Fehler: {e}")
                    
                    pbar.update(1)
                    if (i + 1) % 50 == 0: # Speicherrate erhoeht (alle 50)
                        data_mgr.save_info_cache()
            except KeyboardInterrupt:
                logger.warning("Download durch Benutzer unterbrochen. Speichere Zwischenstand...")
            finally:
                data_mgr.save_info_cache()
    _log_info_fetch_summary("Industry Info", data_mgr)
    if batch_queue:
        unique_syms = list(set(x[2] for x in batch_queue))
        chunk_size = CONFIG['batch_size']

        batch_map = defaultdict(list)
        for item in batch_queue:
            batch_map[item[2]].append(item)
        with make_progress(total=len(batch_queue), desc="Batch") as pbar:
            for i in range(0, len(unique_syms), chunk_size):
                chunk = unique_syms[i:i+chunk_size]
                data_map = data_mgr.get_history_batch(chunk)

                for y_sym, (curr, sma, vol_eur, flags) in data_map.items():
                    if y_sym in batch_map:
                        for u_key, orig, _, row in batch_map[y_sym]:
                            # WICHTIG: Yahoo-Ticker im Audit vermerken
                            audit_trail[orig]['yahoo'] = y_sym
                            # Simpler Weg: Wenn Info nicht im Cache, nutzen wir Standardwerte
                            info = data_mgr.get_cached_info(y_sym) or {}
                            sector_final = info.get('sector', row.get('Sector', 'Unknown'))
                            industry_final = info.get('industry', row.get('Industry', 'Unknown'))

                            # Filter: Nur echte Aktien zulassen (Skip ETFs, Bonds, Funds etc. laut Yahoo)
                            if sector_final in ('ETF', 'MUTUALFUND', 'BOND', 'INDEX', 'CURRENCY', 'FUTURE', 'OPTION'):
                                sector_skips[orig] = sector_final
                                pbar.update(1)
                                continue

                            # Land-Logik: Prio 1 = Deine Liste/row, Prio 2 = Yahoo (info)
                            land_raw = row.get('Land')
                            land_final = str(land_raw).strip() if pd.notna(land_raw) else ""
                            if not land_final or land_final.lower() in ('unknown', 'nan', 'none', ''):
                                land_final = info.get('country', '')
                            
                            isin_final = str(row.get('ISIN', '')).strip() if pd.notna(row.get('ISIN', '')) else ''
                            if (not isin_final or isin_final.lower() in ('nan', 'none')) and info.get('isin'):
                                isin_final = str(info['isin']).strip()

                            if (not isin_final or isin_final.lower() in ('nan', 'none')) and y_sym in ticker_to_isin_cache:
                                isin_final = ticker_to_isin_cache[y_sym]

                            # Namens-Fallback falls Yahoo für diesen Ticker gar keine ISIN liefert
                            if not isin_final or isin_final.lower() in ('nan', 'none'):
                                isin_final = name_to_isin_cache.get(normalize_name_for_dedup(row.get('Name', '')), '')

                            # PROPAGATION FALLBACK: Falls dieser Ticker keine Market Cap hat, nutze Primär-Daten der ISIN
                            if _resolve_market_cap_from_info(info) <= 0 and isin_final in global_isin_map:
                                info = global_isin_map[isin_final]

                            market_cap_final = _resolve_market_cap_from_info(info)
                            market_value_final = _resolve_market_value_from_sources(row, info, y_sym)
                            fs_date, is_new = first_seen_mgr.get_date_info(y_sym)

                            stock_results.append(StockData(
                                original_ticker=orig,
                                yahoo_symbol=y_sym,
                                isin=isin_final,
                                name=row.get('Name', ''),
                                sector=sector_final,
                                industry=industry_final,
                                land=land_final,
                                market_value=market_value_final,
                                kurs=curr,
                                sma=sma,
                                rsl=flags.get('rsl', 0.0),
                                atr=flags.get('atr', 0.0),
                                atr_limit=flags.get('atr_limit', 0.0),
                                atr_sell_limit=flags.get('atr_sell_limit', 0.0),
                                avg_volume_eur=vol_eur,
                                source_etf=str(row.get('Source_ETF', '')).strip() if pd.notna(row.get('Source_ETF', '')) else '',
                                listing_source=str(row.get('Listing_Source', '')).strip() if pd.notna(row.get('Listing_Source', '')) else '',
                                market_cap=market_cap_final,
                                flag_gap=flags['flag_gap'],
                                flag_liquidity=flags['flag_liquidity'],
                                flag_stale=flags['flag_stale'],
                                stale_reason=flags.get('stale_reason', ''),
                                flag_scale=flags.get('flag_scale', 'OK'),
                                scale_reason=flags.get('scale_reason', ''),
                                price_scale_ratio=float(flags.get('price_scale_ratio', 1.0) or 1.0),
                                trend_sma50=flags['trend_sma50'],
                                flag_history_length=flags.get('flag_history_length', 'OK'),
                                history_length_reason=flags.get('history_length_reason', ''),
                                trend_smoothness=flags['trend_smoothness'],
                                trend_quality=flags['trend_quality'],
                                trust_score=flags['trust_score'],
                                twss_score=flags['twss_score'],
                                twss_date=flags['twss_date'],
                                twss_days_ago=flags['twss_days_ago'],
                                twss_raw_pct=flags['twss_raw_pct'],
                                twss_orientation=flags['twss_orientation'],
                                rsl_change_1w=flags['rsl_change_1w'],
                                rsl_past=flags.get('rsl_past'),
                                mom_12m=flags.get('mom_12m'),
                                mom_6m=flags.get('mom_6m'),
                                mom_3m=flags.get('mom_3m'),
                                mom_score=flags.get('mom_score'),
                                mom_vol=flags.get('mom_vol'),
                                mom_score_adj=flags.get('mom_score_adj'),
                                mom_accel=flags.get('mom_accel'),
                                max_drawdown_6m=flags.get('max_drawdown_6m', 0.0),
                                ulcer_index_6m=flags.get('ulcer_index_6m', 0.0),
                                high_52w=flags.get('high_52w', 0.0),
                                distance_52w_high_pct=flags.get('distance_52w_high_pct'),
                                stale_days=flags.get('stale_days', 0),
                                first_seen_date=fs_date,
                                is_new=is_new,
                                integrity_warnings=flags.get('integrity_reasons', []),
                                used_close_fallback=flags.get('used_close_fallback', False),
                                rsl_price_source=flags.get('rsl_price_source', 'adj_close'),
                                repair_applied=flags.get('repair_applied', False),
                                repair_method=flags.get('repair_method', ''),
                                repair_reason=flags.get('repair_reason', ''),
                                fallback_fraction=flags.get('fallback_fraction', 0.0),
                                excluded_from_ranking=flags.get('excluded_from_ranking', False),
                                ranking_exclude_reason=flags.get('integrity_reasons', [""])[0] if flags.get('excluded_from_ranking') else "",
                                ranking_integrity_status=flags.get('ranking_integrity_status', 'eligible_original')
                            ))
                            if not mapper.get(u_key): mapper.set(u_key, y_sym)
                            pbar.update(1)
                        del batch_map[y_sym]
                unresolved_syms = [sym for sym in chunk if sym not in data_map]
                for unresolved_sym in unresolved_syms:
                    if unresolved_sym in batch_map:
                        for u_key, orig, _, row in batch_map[unresolved_sym]:
                            complex_queue.append((u_key, orig, row))
                            pbar.update(1)
                        del batch_map[unresolved_sym] 
                min_sleep = float(CONFIG.get('batch_sleep_min_s', 0.5))
                max_sleep = float(CONFIG.get('batch_sleep_max_s', 1.5))
                if max_sleep < min_sleep: max_sleep = min_sleep
                if data_mgr.last_history_batch_used_network and max_sleep > 0:
                    time.sleep(random.uniform(min_sleep, max_sleep))
                    
    # WICHTIG: Cache speichern, damit Option 1 beim nächsten Mal schnell ist!
    if data_mgr:
        data_mgr.save_history_cache()

    def process_fallback(item):
        u_key, orig, row = item
        orig_clean = sanitize_ticker_symbol(orig)
        preferred_history_sym = history_symbol_overrides.get(str(orig_clean).strip().upper(), "")
        cands = [preferred_history_sym] if preferred_history_sym else generate_candidates(orig, row.get('Land', ''), row.get('Exchange', ''))
        for cand in cands:
            if str(cand).strip().upper() in blacklist:
                continue
            res = data_mgr.get_history_single(cand)
            if res: return u_key, orig, cand, row, res
        return None
    if complex_queue:
        complex_queue = list({x[0]: x for x in complex_queue}.values())
        
        # --- FALLBACK-OPTIMIERUNG (PRE-BATCHING) ---
        all_fb_cands = set()
        fb_list = []        
        for _, orig, row in complex_queue:
            orig_clean = sanitize_ticker_symbol(orig)
            pref = history_symbol_overrides.get(str(orig_clean).strip().upper(), "")
            cands = [pref] if pref else generate_candidates(orig, row.get('Land', ''), row.get('Exchange', ''))
            all_fb_cands.update(cands)
            
        if all_fb_cands:
            fb_list = sorted(list(all_fb_cands))
            logger.info(f"Fallback-Speedup: Lade {len(fb_list)} alternative Ticker vorab in Batches...")            
     

            # Historien in Batches laden (Sektoren lassen wir hier weg, da zu langsam)
            fb_chunk = CONFIG['batch_size']
            with make_progress(total=len(fb_list), desc="Batch (Fallback)", include_last_duration=False) as pbar:
                for i in range(0, len(fb_list), fb_chunk):
                    chunk = fb_list[i : i + fb_chunk]
                    data_mgr.get_history_batch(chunk)
                    pbar.update(len(chunk))
        # --- ENDE OPTIMIERUNG ---
        
        with make_progress(total=len(complex_queue), desc="Fallback") as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor: # type: ignore
                fallback_futures = [executor.submit(process_fallback, item) for item in complex_queue]
                for future in concurrent.futures.as_completed(fallback_futures):
                    res = future.result()
                    if res:
                        u_key, orig, y_sym, row, (curr, sma, vol_eur, flags) = res
                       
                        audit_trail[orig]['yahoo'] = y_sym
                        if flags['flag_stale'] == "CRITICAL":
                            dropped_critical.append(f"{y_sym} ({orig}): {flags.get('stale_reason', 'Critical Stale')}")
                        # Simpler Weg: Wenn Info nicht im Cache, nutzen wir Standardwerte aus der Liste
                        info = data_mgr.get_cached_info(y_sym) or {}
                        sector_final = info.get('sector', row.get('Sector', 'Unknown'))
                        industry_final = info.get('industry', row.get('Industry', 'Unknown'))
                        fs_date, is_new = first_seen_mgr.get_date_info(y_sym)

                        # Filter: Nur echte Aktien zulassen (Skip ETFs, Bonds, Funds etc. laut Yahoo)
                        if sector_final in ('ETF', 'MUTUALFUND', 'BOND', 'INDEX', 'CURRENCY', 'FUTURE', 'OPTION'):
                            sector_skips[orig] = sector_final
                            pbar.update(1)
                            continue

                        # Land-Logik: Prio 1 = Deine Liste/row, Prio 2 = Yahoo (info)
                        land_raw = row.get('Land')
                        land_final = str(land_raw).strip() if pd.notna(land_raw) else ""
                        if not land_final or land_final.lower() in ('unknown', 'nan', 'none', ''):
                            land_final = info.get('country', '')
                        
                        audit_trail[orig]['yahoo'] = y_sym

                        isin_final = str(row.get('ISIN', '')).strip() if pd.notna(row.get('ISIN', '')) else ''
                        if (not isin_final or isin_final.lower() in ('nan', 'none')) and info.get('isin'):
                            isin_final = str(info['isin']).strip()

                        if (not isin_final or isin_final.lower() in ('nan', 'none')) and y_sym in ticker_to_isin_cache:
                            isin_final = ticker_to_isin_cache[y_sym]

                        # Namens-Fallback auch im Fallback-Prozessor
                        if not isin_final or isin_final.lower() in ('nan', 'none'):
                            isin_final = name_to_isin_cache.get(normalize_name_for_dedup(row.get('Name', '')), '')

                        # PROPAGATION FALLBACK: Falls dieser Ticker keine Market Cap hat, nutze Primär-Daten der ISIN
                        if _resolve_market_cap_from_info(info) <= 0 and isin_final in global_isin_map:
                            info = global_isin_map[isin_final]

                        market_cap_final = _resolve_market_cap_from_info(info)
                        market_value_final = _resolve_market_value_from_sources(row, info, y_sym)

                        stock_results.append(StockData(
                            original_ticker=orig,
                            yahoo_symbol=y_sym,
                            isin=isin_final,
                            name=row.get('Name', ''),
                            sector=sector_final,
                            industry=industry_final,
                            land=land_final,
                            market_value=market_value_final,
                            kurs=curr,
                            sma=sma,
                            rsl=flags.get('rsl', 0.0),
                            atr=flags.get('atr', 0.0),
                            atr_limit=flags.get('atr_limit', 0.0),
                            atr_sell_limit=flags.get('atr_sell_limit', 0.0),
                            avg_volume_eur=vol_eur,
                            source_etf=str(row.get('Source_ETF', '')).strip() if pd.notna(row.get('Source_ETF', '')) else '',
                            listing_source=str(row.get('Listing_Source', '')).strip() if pd.notna(row.get('Listing_Source', '')) else '',
                            market_cap=market_cap_final,
                            flag_gap=flags['flag_gap'],
                            flag_liquidity=flags['flag_liquidity'],
                            flag_stale=flags['flag_stale'],
                            stale_reason=flags.get('stale_reason', ''),
                            flag_scale=flags.get('flag_scale', 'OK'),
                            scale_reason=flags.get('scale_reason', ''),
                            price_scale_ratio=float(flags.get('price_scale_ratio', 1.0) or 1.0),
                            trend_sma50=flags['trend_sma50'],
                            flag_history_length=flags.get('flag_history_length', 'OK'),
                            history_length_reason=flags.get('history_length_reason', ''),
                            trend_smoothness=flags['trend_smoothness'],
                            trend_quality=flags['trend_quality'],
                            trust_score=flags['trust_score'],
                            twss_score=flags['twss_score'],
                            twss_date=flags['twss_date'],
                            twss_days_ago=flags['twss_days_ago'],
                            twss_raw_pct=flags['twss_raw_pct'],
                            twss_orientation=flags['twss_orientation'],
                            rsl_change_1w=flags['rsl_change_1w'],
                            rsl_past=flags.get('rsl_past'),
                            mom_12m=flags.get('mom_12m'),
                            mom_6m=flags.get('mom_6m'),
                            mom_3m=flags.get('mom_3m'),
                            mom_score=flags.get('mom_score'),
                            mom_vol=flags.get('mom_vol'),
                            mom_score_adj=flags.get('mom_score_adj'),
                            mom_accel=flags.get('mom_accel'),
                            max_drawdown_6m=flags.get('max_drawdown_6m', 0.0),
                            ulcer_index_6m=flags.get('ulcer_index_6m', 0.0),
                            high_52w=flags.get('high_52w', 0.0),
                            distance_52w_high_pct=flags.get('distance_52w_high_pct'),
                            stale_days=flags.get('stale_days', 0),
                            first_seen_date=fs_date,
                            is_new=is_new,
                            integrity_warnings=flags.get('integrity_reasons', []),
                            used_close_fallback=flags.get('used_close_fallback', False),
                            rsl_price_source=flags.get('rsl_price_source', 'adj_close'),
                            repair_applied=flags.get('repair_applied', False),
                            repair_method=flags.get('repair_method', ''),
                            repair_reason=flags.get('repair_reason', ''),
                            fallback_fraction=flags.get('fallback_fraction', 0.0),
                            excluded_from_ranking=flags.get('excluded_from_ranking', False),
                            ranking_exclude_reason=flags.get('integrity_reasons', [""])[0] if flags.get('excluded_from_ranking') else "",
                            ranking_integrity_status=flags.get('ranking_integrity_status', 'eligible_original')
                        ))
                        mapper.set(u_key, y_sym)
                    pbar.update(1)
    stock_results, integrity_drops_df = filter_stock_results_for_rsl_integrity(stock_results)
    save_dataframe_safely(
        integrity_drops_df,
        CONFIG['rsl_integrity_drop_file'],
        sep=';',
        index=False,
        encoding='utf-8-sig',
    )
    if not integrity_drops_df.empty:
        actual_drops = integrity_drops_df[integrity_drops_df["excluded_from_ranking"] == True]
        for _, row in integrity_drops_df.iterrows():
            if row.get("excluded_from_ranking"):
                dropped_critical.append(
                    f"{row.get('yahoo_symbol', '')} ({row.get('original_ticker', '')}): RSL-Integritaet -> {row.get('integrity_reasons', '')}"
                )
        
        integrity_summary = quality_core.summarize_integrity_flags(integrity_drops_df)
        print(f"\nIntegritaets-Check Ergebnis: {quality_core.quality_gate_status(integrity_summary)}")
        
        if "is_valid" in integrity_drops_df.columns:
            print(f" - Hard fails: {integrity_summary['hard_fail_count']}")
        if "needs_review" in integrity_drops_df.columns:
            print(f" - Needs review: {integrity_summary['review_count']}")
        if "warning_reasons" in integrity_drops_df.columns:
            print(f" - Warnings: {integrity_summary['warning_count']}")

        if len(actual_drops) > 0:
            logger.info(f"[WARN] {len(actual_drops)} Ticker weisen kritische RSL-Fehler auf und wurden gefiltert.")
    refresh_market_caps_for_relevant_exchange_stocks(stock_results, data_mgr)
    mapper.save_if_dirty()
    if dropped_critical:
        with open(CONFIG['dropped_tickers_log'], 'w', encoding='utf-8') as f:
            f.write(f"--- Dropped Tickers Run: {datetime.date.today()} ---\n")
            for line in dropped_critical:
                f.write(line + "\n")
        logger.info(f"[INFO] {len(dropped_critical)} Ticker mit Daten-Warnungen markiert.")
    if not stock_results:
        logger.error("Keine Ergebnisse.")
        return
    # --- FILTERUNG ---
    initial_count = len(stock_results)
    
    # PHILOSOPHIE: Ausschluss nach Preis deaktiviert, um keine Daten zu zensieren.
    # Alle Aktien werden verarbeitet, die Berechnung oder Datenbasis muss bei Bedarf manuell geprüft werden.
    logger.info(f"Verarbeite {len(stock_results)} Aktien. Preis-Filter ist deaktiviert.")

    processed_at_start = {s.yahoo_symbol for s in stock_results}

    before_dedupe_count = len(stock_results)
    stock_results = data_pipeline_core.perform_final_deduplication(stock_results)
    after_dedupe_set = {s.yahoo_symbol for s in stock_results}

    # NEU: Zusaetzliche Liquiditaets-Deduplikation (Entfernt Sekundaer-Listings)
    apply_primary_liquidity_context(stock_results)
    symbol_lookup = {str(s.yahoo_symbol).strip().upper(): s for s in stock_results if str(s.yahoo_symbol).strip()}
    before_liq_dedupe = len(stock_results)
    # Wir deaktivieren das Löschen von Neben-Listings, um die Vollständigkeit zu wahren.
    # Das System priorisiert das Haupt-Listing weiterhin automatisch in den Metriken.
    pre_liq_set = {s.yahoo_symbol for s in stock_results}
    final_analyzed_set = pre_liq_set

    # --- FILTERUNG & KONSOLIDIERUNG VOR RANKING ---
    # Blacklist anwenden, damit sie nicht in die Ränge und den Snapshot einfließt
    if blacklist:
        pre_black_count = len(stock_results)
        stock_results = [s for s in stock_results if str(s.yahoo_symbol).strip().upper() not in blacklist]
        if len(stock_results) < pre_black_count:
            logger.info(f"Blacklist-Filter: {pre_black_count - len(stock_results)} Aktien entfernt.")

    # --- UNIVERSE AUDIT DATAFRAME BAUEN ---
    rsl_drops_map = {str(row['ticker']).strip().upper(): row['drop_reasons'] for _, row in integrity_drops_df.iterrows()} if not integrity_drops_df.empty else {}
    
    audit_rows = []
    for orig, trail in audit_trail.items():
        y = trail['yahoo']
        y_upper = str(y).strip().upper()
        status = "ANALYZED"
        detail = "Erfolgreich verarbeitet"
        
        if orig in sector_skips:
            status, detail = "SKIPPED_SECTOR", f"Typ: {sector_skips[orig]}"
        elif not y_upper:
            status, detail = "SKIPPED_UNRESOLVED", "Kein Yahoo-Ticker gefunden"
        elif y_upper in rsl_drops_map:
            status, detail = "DROPPED_DATA_QUALITY", str(rsl_drops_map[y_upper])
        elif y_upper in processed_at_start and y_upper not in after_dedupe_set:
            status, detail = "DROPPED_DEDUP", "Duplikat (Name/ISIN)"
        # Neuer Status für analysierte Neben-Listings (bleiben im main sheet sichtbar)
        elif y_upper in symbol_lookup:
             s_obj = symbol_lookup[y_upper]
             primary_symbol = str(getattr(s_obj, 'primary_liquidity_symbol', '')).strip().upper()
             if primary_symbol and y_upper != primary_symbol:
                 status = "ANALYZED_SECONDARY"
                 detail = f"Neben-Listing. Primär: {s_obj.primary_liquidity_symbol}"
        elif y_upper not in final_analyzed_set:
            status, detail = "DROPPED_OTHER", "Gefiltert (Blacklist/Sonstiges)"
            
        audit_rows.append({'Original Ticker': orig, 'Yahoo Ticker': y, 'Status': status, 'Details': detail})
    universe_audit_df = pd.DataFrame(audit_rows).sort_values(by=['Status', 'Original Ticker'])

    # Portfolio-Status abgleichen (wichtig für Markierungen in den Summaries)
    synchronize_portfolio_symbols_with_stock_results(portfolio_mgr, stock_results)

    # --- RANGFOLGEN BERECHNEN ---
    ranking_core.apply_standard_rankings(stock_results)

    # --- SCHWELLEN-MARKIERUNG FÜR EXCEL ---
    try:
        threshold = float(CONFIG.get("candidate_top_percent_threshold", 0.01))
        threshold_rank = max(1, int(math.ceil(len(stock_results) * threshold)))
        for s in stock_results:
            if getattr(s, 'rsl_rang', 0) == threshold_rank:
                s.is_threshold_line = True
                break
    except Exception as e:
        logger.debug(f"Markierung der Schwelle fehlgeschlagen: {e}")

    # --- ANALYSE & SUMMARIES ---
    ranking_core.apply_relative_context_metrics(stock_results)
    industry_summary_df = summary_core.build_industry_rsl_summary(stock_results, CONFIG)
    market_regime = ranking_core.calculate_market_regime(stock_results)
    cluster_summary_df, cluster_map = summary_core.build_momentum_cluster_summary(stock_results, CONFIG)
    if cluster_map:
        for s in stock_results:
            sym = str(getattr(s, "yahoo_symbol", "")).strip().upper()
            s.mom_cluster = cluster_map.get(sym, "")
    
    save_analysis_snapshot(
        stock_results, 
        selected_syms, 
        etf_options, 
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df
    )
    logger.info("Snapshot vor Quality-Gate gesichert.")

    portfolio_symbols = [str(p.get('Yahoo_Symbol', '')).strip().upper() for p in portfolio_mgr.current_portfolio if p.get('Yahoo_Symbol')]

    save_home_market_rsl_audit(stock_results)

    # PERFORMANCE END & LOGGING
    perf_duration = time.time() - perf_start_time
    all_etf_keys = list(etf_options.keys())
    # Pruefen ob ALLE ETFs gewaehlt sind (fuer Vergleichbarkeit)
    is_all_selected_final = (len(all_etf_keys) > 0 and 
                             len(selected_syms) == len(all_etf_keys) and 
                             set(selected_syms) == set(all_etf_keys))

    if is_all_selected_final:
        try:
            log_file = CONFIG['performance_log_csv']
            file_exists = os.path.exists(log_file)
            with open(log_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                if not file_exists:
                    writer.writerow([
                        'Date', 'Duration (s)', 'Tickers', 'Workers', 'Batch Size', 
                        'Sleep Min', 'Sleep Max', 'Info Delay', 'Info Quiet', 'Cache Hours'
                    ])
                
                writer.writerow([
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    f"{perf_duration:.2f}",
                    len(stock_results),
                    CONFIG['max_workers'],
                    CONFIG['batch_size'],
                    CONFIG['batch_sleep_min_s'],
                    CONFIG['batch_sleep_max_s'],
                    CONFIG['info_fetch_delay_s'],
                    1 if CONFIG['info_fetch_quiet'] else 0,
                    CONFIG['cache_duration_hours']
                ])
            logger.info(f"Performance-Log geschrieben: {perf_duration:.2f}s fuer {len(stock_results)} Ticker.")
        except Exception as e:
            logger.error(f"Fehler beim Schreiben des Performance-Logs: {e}")

    quality_core.run_quality_gate(
        stock_results=stock_results,
        universe_candidates=final_rows,
        dropped_critical_count=len(dropped_critical), # type: ignore
        data_mgr=data_mgr,
        portfolio_symbols=portfolio_symbols,
        config=CONFIG,
        save_json_config_func=save_json_config,
        logger=logger,
        report_file=CONFIG['strict_report_file']
    )

    watchlist_symbols = load_watchlist_symbols(str(CONFIG.get('watchlist_file', ''))) or set()
    
    # Rate-Limit Statistik
    try:
        total_hits = _consume_rate_limit_hits()
        save_json_config(CONFIG['run_stats_file'], {
            'last_run_at': datetime.datetime.now().isoformat(timespec='seconds'),
            'last_rate_limit_hits': total_hits
        })
    except Exception: pass

    render_analysis_output(
        stock_results=stock_results,
        portfolio_mgr=portfolio_mgr,
        selected_syms=selected_syms,
        etf_options=etf_options,
        update_last_run_cfg=not use_last_settings,
        data_mgr=data_mgr,
        suggest_portfolio_candidates=candidate_core.suggest_portfolio_candidates,
        industry_summary_df=industry_summary_df,
        cluster_summary_df=cluster_summary_df,
        market_regime=market_regime,
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df,
        watchlist_symbols=watchlist_symbols
    )

def main() -> None:
    """Haupteinstiegspunkt mit Menüführung."""
    load_dotenv()
    
    mapper = TickerMapper(CONFIG['mapping_file'])
    data_mgr = MarketDataManager(CONFIG, CURRENCY_RATES)
    portfolio_mgr = PortfolioManager(CONFIG['portfolio_file'])
    first_seen_mgr = FirstSeenManager(CONFIG['first_seen_cache_file'])

    capture_file = str(CONFIG['last_console_output_file'])
    snapshot_file = str(CONFIG['last_analysis_snapshot_file'])

    while True:
        has_snapshot = os.path.exists(snapshot_file)
        choice = show_main_menu(has_snapshot)
        
        try:
            if choice == "0":
                logger.info("Programm beendet.")
                break
            elif choice in ("", "2"):
                print("\n\033[94m--- ANALYSE-START ---\033[0m")
                print(" [1] Schnell-Start (Nutzt Cache von heute)")
                print(" [2] Voll-Download (Leert Kurs-Cache, holt Historie frisch)")
                sub_choice = get_user_input("Wahl [1]: ", "1")
                
                with ConsoleCapture(capture_file):
                    run_analysis_pipeline(data_mgr, portfolio_mgr, first_seen_mgr, mapper, force_clear_cache=(sub_choice=="2"))
            elif choice == "1" and has_snapshot:
                with ConsoleCapture(capture_file):
                    if not rerender_last_analysis():
                        logger.warning("Re-Render fehlgeschlagen.")
            elif choice == "3":
                configure_user_settings_interactive()
            elif choice == "4":
                show_ticker_history_interactive()
            elif choice == "5":
                with ConsoleCapture(capture_file):
                    run_fundamental_data_download(data_mgr)
            elif choice == "6":
                print("\n" + "-" * 50)
                print(" AD-HOC ANALYSE (TEST-MODUS)")
                print(" (Ticker/ISINs kopieren oder eingeben. ENTER auf leerer Zeile zum Starten.)")
                
                collected_lines = []
                while True:
                    line = input(" > ").strip()
                    if not line:
                        break
                    collected_lines.append(line)
                
                filter_input = " ".join(collected_lines)
                tokens = {t.strip().upper() for t in filter_input.replace(",", " ").split() if t.strip()}
                if tokens:
                    with ConsoleCapture(capture_file):
                        run_analysis_pipeline(data_mgr, portfolio_mgr, first_seen_mgr, mapper, filter_tokens=tokens)
            elif choice == "7":
                print("\n\033[96m" + "="*70)
                print(" HILFE: DATEN-INTEGRITAET & FEHLERMELDUNGEN")
                print("="*70 + "\033[0m")
                print("\033[1mHaeufige Fehler-Codes im Sheet 'integrity_issues':\033[0m\n")
                print("\033[93mtechnical_scale_break_suspected\033[0m")
                print(" -> Preis-Sprung um Faktor 15+. Oft Einheiten-Fehler (z.B. Pence vs Pfund).")
                print("\033[93mstale_price_series / flat_run\033[0m")
                print(" -> Kurs hat sich tagelang nicht bewegt. Keine Liquiditaet am Marktplatz.")
                print("\033[93minsufficient_trading_participation\033[0m")
                print(" -> Aktie wurde an zu wenigen Tagen tatsaechlich gehandelt.")
                print("\033[93mbad_dividend_adjustment\033[0m")
                print(" -> Yahoo hat Dividende falsch eingerechnet. System nutzt Close-Fallback.")
                print("\033[93mprice_jump_without_volume_confirmation\033[0m")
                print(" -> Großer Sprung ohne Handelsvolumen. Wahrscheinlich ein 'Bad Tick'.")
                print("\033[93minsufficient_history_for_rsl\033[0m")
                print(" -> Weniger als 130 Tage Historie vorhanden (SMA Berechnung unmoeglich).")
                print("\n\033[1mStatus-Bedeutung:\033[0m")
                print(" - \033[92meligible_original\033[0m:  Daten perfekt.")
                print(" - \033[93meligible_repaired\033[0m:  Datenfehler wurden automatisch korrigiert.")
                print(" - \033[91mexcluded_hard_fail\033[0m: Aktie unbrauchbar, aus Analyse entfernt.")
                print("\n\033[90mDetaillierte Beschreibungen findest du in: docs/indikatoren.md\033[0m")
                input("\n[ENTER] Zurueck zum Menue...")
            else:
                print("Ungueltige Auswahl.")
        except KeyboardInterrupt:
            print("\nAbbruch durch Benutzer. Zurueck zum Menue.")
        except Exception as e:
            logger.exception(f"Kritischer Fehler im Hauptablauf: {e}")

if __name__ == "__main__":
    main()
