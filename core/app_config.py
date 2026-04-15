import os
from typing import Any, Callable, Dict

from . import settings_catalog as settings_catalog_core


def get_path(script_dir: str, filename: str) -> str:
    return os.path.join(script_dir, filename)


def get_report_path(reports_dir: str, filename: str) -> str:
    return os.path.join(reports_dir, filename)


def build_base_config(script_dir: str, reports_dir: str) -> Dict[str, Any]:
    defaults = settings_catalog_core.USER_SETTINGS_DEFAULTS
    return {
        "batch_size": 50,
        "max_workers": 8,
        "batch_sleep_min_s": 0.5,
        "batch_sleep_max_s": 1.5,
        "min_price": 1.0,
        "max_retries": 4,
        "retry_base_delay": 5,
        "cache_duration_hours": 24,
        "history_period": "18mo",
        "required_days": 130,
        "etf_cache_file": get_path(script_dir, "etf_holdings_cache.pkl"),
        "etf_cache_duration_hours": 168,
        "exchange_cache_file": get_path(script_dir, "exchange_universe_cache.pkl"),
        "industry_history_file": get_path(script_dir, "industry_history.json"),
        "etf_names_cache_file": get_path(script_dir, "etf_names_cache.json"),
        "country_cache_file": get_path(script_dir, "etf_country_cache.json"),
        "info_cache_unknown_expiry_days": 7,
        "info_fetch_delay_s": defaults["info_fetch_delay_s"],
        "info_fetch_quiet": True,
        "info_fetch_summary_max": 10,
        "rate_limit_delay_min_s": 20.0,
        "rate_limit_delay_max_s": 60.0,
        "rate_limit_backoff_step_s": 5.0,
        "rate_limit_log_every": 10,
        "first_seen_cache_file": get_path(script_dir, "first_seen_cache.json"),
        "min_liquidity": 0.60,
        "stale_window": 60,
        "min_unique_ratio": 0.05,
        "min_nonzero_ratio": 0.10,
        "max_consecutive_flat": 20,
        "max_std_rel": 0.005,
        "min_total_range": 0.02,
        "max_total_return": 10.0,
        "max_flat_days": 15,
        "max_gap_percent": 0.30,
        "price_scale_recent_window": 60,
        "price_scale_warn_ratio": 25.0,
        "price_scale_critical_ratio": 50.0,
        "price_scale_warn_jump": 0.8,
        "price_scale_critical_jump": 1.5,
        "price_scale_near_high_pct": 35.0,
        "twss_decay_days": 60.0,
        "industry_breadth_min": defaults["industry_breadth_min"],
        "industry_avg_rsl_cap": defaults["industry_avg_rsl_cap"],
        "industry_min_size": defaults["industry_min_size"],
        "industry_summary_include_unknown": True,
        "industry_trend_enabled": True,
        "industry_trend_weeks": 4,
        "industry_top_n": defaults["industry_top_n"],
        "industry_score_min": defaults["industry_score_min"],
        "url_xetra_instruments": "https://cashmarket.deutsche-boerse.com/resource/blob/1528/8b811ef41181e6f04d98ad511804ab8f/data/t7-xetr-allTradableInstruments.csv",
        "url_frankfurt_instruments": "https://www.cashmarket.deutsche-boerse.com/resource/blob/2289108/babd7d2ee2ba6907ea2ac3bf74b3488f/data/t7-xfra-BF-allTradableInstruments.csv",
        "exchange_scan_enabled": True,
        "industry_score_w_breadth": defaults["industry_score_w_breadth"],
        "industry_score_w_avg": defaults["industry_score_w_avg"],
        "industry_score_w_median": defaults["industry_score_w_median"],
        "industry_score_w_leader": defaults["industry_score_w_leader"],
        "mom_lookback_3m": 63,
        "mom_lookback_6m": 126,
        "mom_lookback_12m": 252,
        "mom_weight_12m": defaults["mom_weight_12m"],
        "mom_weight_6m": defaults["mom_weight_6m"],
        "mom_weight_3m": defaults["mom_weight_3m"],
        "candidate_scoring_model": "institutional",
        "mom_vol_lookback": 63,
        "candidate_use_momentum_score": True,
        "candidate_use_vol_adjust": True,
        "candidate_use_industry_neutral": True,
        "candidate_use_accel": True,
        "candidate_accel_weight": defaults["candidate_accel_weight"],
        "candidate_use_rsl_change_1w": defaults["candidate_use_rsl_change_1w"],
        "candidate_rsl_change_weight": defaults["candidate_rsl_change_weight"],
        "candidate_min_avg_volume_eur": defaults["candidate_min_avg_volume_eur"],
        "candidate_min_trust_score": defaults["candidate_min_trust_score"],
        "candidate_score_min": 0.0,
        "candidate_require_top_percent": defaults["candidate_require_top_percent"],
        "candidate_top_percent_threshold": defaults["candidate_top_percent_threshold"],
        "candidate_block_new_buys_in_weak_regime": defaults["candidate_block_new_buys_in_weak_regime"],
        "candidate_max_stocks_per_industry": defaults["candidate_max_stocks_per_industry"],
        "candidate_use_peer_spread": defaults["candidate_use_peer_spread"],
        "candidate_peer_spread_weight": defaults["candidate_peer_spread_weight"],
        "candidate_max_distance_52w_high_pct": defaults["candidate_max_distance_52w_high_pct"],
        "market_cap_refresh_listing_only_min_turnover_eur": 1000000.0,
        "market_cap_refresh_max_workers": 2,
        "market_cap_refresh_max_symbols": 80,
        "cluster_enabled": True,
        "cluster_top_n": defaults["cluster_top_n"],
        "cluster_min_size": defaults["cluster_min_size"],
        "cluster_score_w_mom12": defaults["cluster_score_w_mom12"],
        "cluster_score_w_mom6": defaults["cluster_score_w_mom6"],
        "cluster_score_w_accel": defaults["cluster_score_w_accel"],
        "candidate_use_cluster_filter": defaults["candidate_use_cluster_filter"],
        "sma_length": 130,
        "sma_short_length": 50,
        "annual_factor": 252,
        "atr_period": 14,
        "atr_multiplier_limit": 1.0,
        "atr_multiplier_exit": defaults["atr_multiplier_exit"],
        "top_percent_threshold": 0.25,
        "heatmap_warn_percent": 20.0,
        "heatmap_full_percent": 25.0,
        "strict_mode": defaults["strict_mode"],
        "strict_min_analyzed_stocks": defaults["strict_min_analyzed_stocks"],
        "strict_min_coverage_ratio": defaults["strict_min_coverage_ratio"],
        "strict_max_failed_ratio": defaults["strict_max_failed_ratio"],
        "strict_max_young_ratio": defaults["strict_max_young_ratio"],
        "strict_max_critical_drop_ratio": defaults["strict_max_critical_drop_ratio"],
        "strict_max_stale_warn_ratio": defaults["strict_max_stale_warn_ratio"],
        "strict_max_gap_warn_ratio": defaults["strict_max_gap_warn_ratio"],
        "strict_max_liquidity_warn_ratio": defaults["strict_max_liquidity_warn_ratio"],
        "strict_max_low_trust_ratio": defaults["strict_max_low_trust_ratio"],
        "strict_min_portfolio_coverage_ratio": defaults["strict_min_portfolio_coverage_ratio"],
        "strict_max_invalid_numeric_count": defaults["strict_max_invalid_numeric_count"],
        "strict_max_duplicate_symbols": defaults["strict_max_duplicate_symbols"],
        "result_file_prefix": get_report_path(reports_dir, "global_rsl"),
        "mapping_file": get_path(script_dir, "ticker_map_v2.json"),
        "history_cache_file": get_path(script_dir, "history_cache.json"),
        "ticker_info_cache_file": get_path(script_dir, "ticker_info_cache.json"),
        "portfolio_file": get_path(script_dir, "current_portfolio.json"),
        "etf_config_file": get_path(script_dir, "etf_config.json"),
        "watchlist_file": get_path(script_dir, "watchlist_tickers.txt"),
        "manual_fix_file": get_path(script_dir, "manual_fix.json"),
        "blacklist_file": get_path(script_dir, "blacklist.json"),
        "location_suffix_map_file": get_path(script_dir, "location_suffix_map.json"),
        "exchange_suffix_map_file": get_path(script_dir, "exchange_suffix_map.json"),
        "unsupported_exchanges_file": get_path(script_dir, "unsupported_exchanges.json"),
        "failed_tickers_log": get_report_path(reports_dir, "failed_tickers.log"),
        "failed_tickers_json": get_report_path(reports_dir, "failed_tickers.json"),
        "failed_tickers_csv": get_report_path(reports_dir, "failed_tickers.csv"),
        "young_tickers_log": get_report_path(reports_dir, "young_tickers.log"),
        "young_tickers_json": get_report_path(reports_dir, "young_tickers.json"),
        "young_tickers_csv": get_report_path(reports_dir, "young_tickers.csv"),
        "performance_log_csv": get_report_path(reports_dir, "performance_log.csv"),
        "dropped_tickers_log": get_report_path(reports_dir, "dropped_tickers.log"),
        "last_console_output_file": get_report_path(reports_dir, "last_console_output.txt"),
        "last_analysis_snapshot_file": get_report_path(reports_dir, "last_analysis_snapshot.json"),
        "rsl_integrity_drop_file": get_report_path(reports_dir, "rsl_integrity_drops.csv"),
        "home_market_rsl_audit_file": get_report_path(reports_dir, "home_market_rsl_audit.csv"),
        "home_market_rsl_review_file": get_report_path(reports_dir, "home_market_rsl_review.csv"),
        "home_market_rsl_review_top_rank": 300,
        "strict_report_file": get_report_path(reports_dir, "strict_quality_report.json"),
        "user_settings_file": get_path(script_dir, "user_settings.json"),
        "strategy_profiles_file": get_path(script_dir, "strategy_profiles.json"),
        "currency_rates_file": get_path(script_dir, "currency_rates.json"),
        "last_run_config_file": get_path(script_dir, "last_run_config.json"),
        "run_stats_file": get_path(script_dir, "run_stats.json"),
        "base_url_template": "https://www.ishares.com/us/products/{id}/{slug}/1467271812596.ajax?fileType=csv&fileName={symbol}_holdings&dataType=fund",
    }


def sanitize_heatmap_thresholds(
    warn_pct: Any,
    full_pct: Any,
    to_float: Callable[[Any, float], float],
) -> tuple[float, float]:
    warn_value = max(0.0, to_float(warn_pct, 20.0))
    full_value = max(1.0, to_float(full_pct, 25.0))
    if warn_value >= full_value:
        warn_value = max(0.0, full_value - 1.0)
    return warn_value, full_value


def load_user_settings(
    config: Dict[str, Any],
    load_json_config: Callable[..., Any],
) -> Dict[str, Any]:
    return {
        **settings_catalog_core.get_user_settings_defaults(),
        **load_json_config(config["user_settings_file"]),
    }


def save_user_settings(
    config: Dict[str, Any],
    settings: Dict[str, Any],
    save_json_config: Callable[[str, Any], None],
) -> None:
    save_json_config(config["user_settings_file"], settings)


def apply_user_settings(
    config: Dict[str, Any],
    settings: Dict[str, Any],
    to_float: Callable[[Any, float], float],
    to_bool: Callable[[Any, bool], bool],
    normalize_weights: Callable[..., tuple[float, ...]],
) -> None:
    warn_value, full_value = sanitize_heatmap_thresholds(
        settings.get("heatmap_warn_percent", config["heatmap_warn_percent"]),
        settings.get("heatmap_full_percent", config["heatmap_full_percent"]),
        to_float,
    )
    config["heatmap_warn_percent"] = warn_value
    config["heatmap_full_percent"] = full_value
    scalar_float_keys = (
        "cache_duration_hours",
        "etf_cache_duration_hours",
        "info_fetch_delay_s",
        "batch_sleep_min_s",
        "batch_sleep_max_s",
        "rate_limit_delay_min_s",
        "rate_limit_delay_max_s",
        "rate_limit_backoff_step_s",
        "industry_score_min",
        "industry_breadth_min",
        "industry_avg_rsl_cap",
        "industry_score_w_breadth",
        "industry_score_w_avg",
        "industry_score_w_median",
        "industry_score_w_leader",
        "atr_multiplier_exit",
        "candidate_accel_weight",
        "candidate_rsl_change_weight",
        "candidate_min_avg_volume_eur",
        "candidate_score_min",
        "candidate_top_percent_threshold",
        "candidate_peer_spread_weight",
        "candidate_max_distance_52w_high_pct",
        "cluster_score_w_mom12",
        "cluster_score_w_mom6",
        "cluster_score_w_accel",
        "strict_min_coverage_ratio",
        "strict_max_failed_ratio",
        "strict_max_young_ratio",
        "strict_max_critical_drop_ratio",
        "strict_max_stale_warn_ratio",
        "strict_max_gap_warn_ratio",
        "strict_max_liquidity_warn_ratio",
        "strict_max_low_trust_ratio",
        "strict_min_portfolio_coverage_ratio",
    )
    scalar_int_keys = (
        "info_cache_unknown_expiry_days",
        "rate_limit_log_every",
        "industry_top_n",
        "industry_min_size",
        "industry_trend_weeks",
        "mom_lookback_12m",
        "mom_lookback_6m",
        "mom_lookback_3m",
        "mom_vol_lookback",
        "candidate_min_trust_score",
        "candidate_max_stocks_per_industry",
        "cluster_top_n",
        "cluster_min_size",
        "strict_min_analyzed_stocks",
        "strict_max_invalid_numeric_count",
        "strict_max_duplicate_symbols",
    )
    for key in scalar_float_keys:
        config[key] = to_float(settings.get(key, config[key]), config[key])
    for key in scalar_int_keys:
        config[key] = int(to_float(settings.get(key, config[key]), config[key]))
    bool_keys = (
        "info_fetch_quiet",
        "industry_summary_include_unknown",
        "industry_trend_enabled",
        "candidate_use_momentum_score",
        "candidate_use_vol_adjust",
        "candidate_use_industry_neutral",
        "candidate_use_accel",
        "candidate_use_rsl_change_1w",
        "candidate_require_top_percent",
        "candidate_block_new_buys_in_weak_regime",
        "candidate_use_peer_spread",
        "cluster_enabled",
        "candidate_use_cluster_filter",
        "strict_mode",
    )
    for key in bool_keys:
        config[key] = to_bool(settings.get(key, config[key]), config[key])
    if config["batch_sleep_max_s"] < config["batch_sleep_min_s"]:
        config["batch_sleep_max_s"] = config["batch_sleep_min_s"]
    if config["rate_limit_delay_max_s"] < config["rate_limit_delay_min_s"]:
        config["rate_limit_delay_max_s"] = config["rate_limit_delay_min_s"]
    config["history_period"] = str(settings.get("history_period", config["history_period"]) or config["history_period"]).strip()
    config["mom_weight_12m"], config["mom_weight_6m"], config["mom_weight_3m"] = normalize_weights(
        to_float(settings.get("mom_weight_12m", config["mom_weight_12m"]), config["mom_weight_12m"]),
        to_float(settings.get("mom_weight_6m", config["mom_weight_6m"]), config["mom_weight_6m"]),
        to_float(settings.get("mom_weight_3m", config["mom_weight_3m"]), config["mom_weight_3m"]),
    )
