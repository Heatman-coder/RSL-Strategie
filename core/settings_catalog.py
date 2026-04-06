import json
import os
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple


USER_SETTINGS_DEFAULTS: Dict[str, Any] = {
    "heatmap_warn_percent": 20.0,
    "heatmap_full_percent": 25.0,
    "cache_duration_hours": 24,
    "etf_cache_duration_hours": 168,
    "info_cache_unknown_expiry_days": 7,
    "info_fetch_delay_s": 0.9,
    "info_fetch_quiet": True,
    "batch_sleep_min_s": 0.2,
    "batch_sleep_max_s": 0.6,
    "rate_limit_delay_min_s": 20.0,
    "rate_limit_delay_max_s": 60.0,
    "rate_limit_backoff_step_s": 5.0,
    "rate_limit_log_every": 10,
    "history_period": "18mo",
    "industry_top_n": 15,
    "industry_score_min": 0.15,
    "industry_breadth_min": 0.25,
    "industry_min_size": 5,
    "industry_avg_rsl_cap": 0.6,
    "industry_summary_include_unknown": True,
    "industry_trend_enabled": True,
    "industry_trend_weeks": 4,
    "industry_score_w_breadth": 0.45,
    "industry_score_w_avg": 0.20,
    "industry_score_w_median": 0.20,
    "industry_score_w_leader": 0.15,
    "mom_weight_12m": 0.5,
    "mom_weight_6m": 0.3,
    "mom_weight_3m": 0.2,
    "mom_lookback_12m": 252,
    "mom_lookback_6m": 126,
    "mom_lookback_3m": 63,
    "mom_vol_lookback": 63,
    "atr_multiplier_exit": 0.15,
    "candidate_scoring_model": "institutional",
    "candidate_use_momentum_score": True,
    "candidate_use_vol_adjust": True,
    "candidate_use_industry_neutral": True,
    "candidate_use_accel": True,
    "candidate_accel_weight": 0.15,
    "candidate_use_rsl_change_1w": True,
    "candidate_rsl_change_weight": 0.10,
    "candidate_min_avg_volume_eur": 0.0,
    "candidate_min_trust_score": 3,
    "candidate_min_mktcap_m_eur": 250.0,
    "candidate_excluded_countries": ["South Korea", "Turkey", "Israel", "Taiwan", "China"],
    "candidate_score_min": 0.0,
    "candidate_require_top_percent": True,
    "candidate_top_percent_threshold": 0.01,
    "candidate_block_new_buys_in_weak_regime": False,
    "candidate_max_stocks_per_industry": 0,
    "candidate_use_peer_spread": False,
    "candidate_peer_spread_weight": 0.35,
    "candidate_max_distance_52w_high_pct": 0.0,
    "cluster_enabled": True,
    "cluster_top_n": 5,
    "cluster_min_size": 25,
    "cluster_score_w_mom12": 0.50,
    "cluster_score_w_mom6": 0.25,
    "cluster_score_w_accel": 0.25,
    "candidate_use_cluster_filter": True,
    "strict_mode": False,
    "strict_min_analyzed_stocks": 200,
    "strict_min_coverage_ratio": 0.55,
    "strict_max_failed_ratio": 0.35,
    "strict_max_young_ratio": 0.35,
    "strict_max_critical_drop_ratio": 0.50,
    "strict_max_stale_warn_ratio": 0.50,
    "strict_max_gap_warn_ratio": 0.50,
    "strict_max_liquidity_warn_ratio": 0.60,
    "strict_max_low_trust_ratio": 0.70,
    "strict_min_portfolio_coverage_ratio": 0.50,
    "strict_max_invalid_numeric_count": 0,
    "strict_max_duplicate_symbols": 0,
}


SETTINGS_PRESETS: Dict[str, Dict[str, Any]] = {
    "standard": {
        "label": "Standard - Ausgewogen Pro",
        "summary": "Robuster Alltag fuer grosses Universum und 5er-Depot.",
        "why": "Breite Branchenauswahl, harter Top-2%-Kauffilter und Trust 3 fuer saubere, stabile Kandidaten.",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 15,
            "industry_score_min": 0.15,
            "industry_breadth_min": 0.25,
            "industry_score_w_breadth": 0.45,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.15,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": True,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": True,
            "candidate_accel_weight": 0.15,
            "candidate_use_rsl_change_1w": True,
            "candidate_rsl_change_weight": 0.10,
            "candidate_min_avg_volume_eur": 0.0,
            "candidate_min_trust_score": 3,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.01,
            "candidate_block_new_buys_in_weak_regime": False,
            "candidate_max_stocks_per_industry": 0,
            "candidate_use_peer_spread": False,
            "candidate_peer_spread_weight": 0.35,
            "candidate_max_distance_52w_high_pct": 0.0,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 5,
            "cluster_min_size": 25,
            "cluster_score_w_mom12": 0.50,
            "cluster_score_w_mom6": 0.25,
            "cluster_score_w_accel": 0.25,
            "mom_weight_12m": 0.50,
            "mom_weight_6m": 0.30,
            "mom_weight_3m": 0.20,
            "history_period": "18mo",
            "info_fetch_delay_s": 0.9,
        },
    },
    "defensiv": {
        "label": "Defensiv - Qualitaet zuerst",
        "summary": "Weniger Turnover, strengere Daten- und Kandidatenqualitaet.",
        "why": "Engerer Kandidatenfilter und hoeheres Volumen senken Rauschen, dafuer kommen weniger fruehe Trends durch.",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 12,
            "industry_score_min": 0.18,
            "industry_breadth_min": 0.28,
            "industry_score_w_breadth": 0.50,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.10,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": True,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": False,
            "candidate_accel_weight": 0.0,
            "candidate_use_rsl_change_1w": False,
            "candidate_rsl_change_weight": 0.0,
            "candidate_min_avg_volume_eur": 5_000_000.0,
            "candidate_min_trust_score": 3,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.01,
            "candidate_block_new_buys_in_weak_regime": True,
            "candidate_max_stocks_per_industry": 1,
            "candidate_use_peer_spread": True,
            "candidate_peer_spread_weight": 0.45,
            "candidate_max_distance_52w_high_pct": 10.0,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 4,
            "cluster_min_size": 30,
            "cluster_score_w_mom12": 0.60,
            "cluster_score_w_mom6": 0.25,
            "cluster_score_w_accel": 0.15,
            "mom_weight_12m": 0.60,
            "mom_weight_6m": 0.30,
            "mom_weight_3m": 0.10,
            "history_period": "18mo",
            "info_fetch_delay_s": 1.0,
        },
    },
    "dynamisch": {
        "label": "Dynamisch - Fruehe Trends",
        "summary": "Offener fuer neue Trendwellen, mehr Kandidaten und mehr Bewegung.",
        "why": "Mehr Branchen, etwas lockerer Breadth-Filter und mehr Gewicht auf Beschleunigung erkennen fruehe Trends schneller.",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 20,
            "industry_score_min": 0.12,
            "industry_breadth_min": 0.20,
            "industry_score_w_breadth": 0.40,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.20,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": True,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": True,
            "candidate_accel_weight": 0.20,
            "candidate_use_rsl_change_1w": True,
            "candidate_rsl_change_weight": 0.12,
            "candidate_min_avg_volume_eur": 0.0,
            "candidate_min_trust_score": 2,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.03,
            "candidate_block_new_buys_in_weak_regime": False,
            "candidate_max_stocks_per_industry": 0,
            "candidate_use_peer_spread": True,
            "candidate_peer_spread_weight": 0.25,
            "candidate_max_distance_52w_high_pct": 18.0,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 6,
            "cluster_min_size": 20,
            "cluster_score_w_mom12": 0.45,
            "cluster_score_w_mom6": 0.25,
            "cluster_score_w_accel": 0.30,
            "mom_weight_12m": 0.45,
            "mom_weight_6m": 0.30,
            "mom_weight_3m": 0.25,
            "history_period": "18mo",
            "info_fetch_delay_s": 0.8,
        },
    },
}


BUILTIN_CUSTOM_PROFILES: Dict[str, Dict[str, Any]] = {
    "selektiv": {
        "label": "Selektiv - Markt-Fokus",
        "summary": "Fokussiert auf absolute Top-Leader in gemischten Marktphasen.",
        "why": "Bei mittlerer Marktbreite ist Selektion entscheidend. Konzentration auf Top-10 Branchen und Top-1.5% Aktien filtert das Rauschen.",
        "market_context": "Normal / Gemischt",
        "best_for": "Stock-Picking",
        "review_trigger": "Woechentlich",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 10,
            "industry_score_min": 0.20,
            "industry_breadth_min": 0.30,
            "industry_score_w_breadth": 0.40,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.20,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": True,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": True,
            "candidate_accel_weight": 0.20,
            "candidate_use_rsl_change_1w": True,
            "candidate_rsl_change_weight": 0.10,
            "candidate_min_avg_volume_eur": 0.0,
            "candidate_min_trust_score": 3,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.015,
            "candidate_block_new_buys_in_weak_regime": True,
            "candidate_max_stocks_per_industry": 1,
            "candidate_use_peer_spread": True,
            "candidate_peer_spread_weight": 0.30,
            "candidate_max_distance_52w_high_pct": 15.0,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 3,
            "cluster_min_size": 15,
            "cluster_score_w_mom12": 0.50,
            "cluster_score_w_mom6": 0.30,
            "cluster_score_w_accel": 0.20,
            "mom_weight_12m": 0.40,
            "mom_weight_6m": 0.35,
            "mom_weight_3m": 0.25,
            "history_period": "18mo",
            "info_fetch_delay_s": 0.9,
        },
    },
    "q1_2026_breakout": {
        "label": "Q1 2026 - Breakout & Speed",
        "summary": "Aggressiver Fokus auf frische Trends und Beschleunigung.",
        "why": "Optimiert fuer das Q1-Umfeld: Gewichtet kurzfristiges Momentum (3M) und Acceleration hoeher, um neue Marktfuehrer frueh zu erkennen. Toleriert hoehere Volatilitaet.",
        "market_context": "Jahresanfang / Sektor-Rotationen",
        "best_for": "High Beta / Early Adopters",
        "review_trigger": "Monatlich",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 20,
            "industry_score_min": 0.10,
            "industry_breadth_min": 0.20,
            "industry_score_w_breadth": 0.30,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.30,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": False,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": True,
            "candidate_accel_weight": 0.35,
            "candidate_use_rsl_change_1w": True,
            "candidate_rsl_change_weight": 0.15,
            "candidate_min_avg_volume_eur": 0.0,
            "candidate_min_trust_score": 3,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.03,
            "candidate_block_new_buys_in_weak_regime": False,
            "candidate_max_stocks_per_industry": 2,
            "candidate_use_peer_spread": True,
            "candidate_peer_spread_weight": 0.25,
            "candidate_max_distance_52w_high_pct": 7.5,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 5,
            "cluster_min_size": 10,
            "cluster_score_w_mom12": 0.20,
            "cluster_score_w_mom6": 0.30,
            "cluster_score_w_accel": 0.50,
            "mom_weight_12m": 0.20,
            "mom_weight_6m": 0.30,
            "mom_weight_3m": 0.50,
            "history_period": "18mo",
            "info_fetch_delay_s": 0.8,
        },
    },
    "small_cap_champions": {
        "label": "Small Cap - Hidden Champions",
        "summary": "Sucht gezielt nach momentumstarken Nebenwerten mit institutionellem Fussabdruck.",
        "why": "Small Caps bieten oft hoeheres Alpha, erfordern aber angepasste Filter (Volatilitaet tolerieren, breiteres Universum).",
        "market_context": "Risk-On / Breite Rallyes",
        "best_for": "High Risk / High Reward Satellite",
        "review_trigger": "Monatlich",
        "values": {
            "candidate_scoring_model": "institutional",
            "industry_top_n": 25,
            "industry_score_min": 0.10,
            "industry_breadth_min": 0.15,
            "industry_score_w_breadth": 0.30,
            "industry_score_w_avg": 0.20,
            "industry_score_w_median": 0.20,
            "industry_score_w_leader": 0.30,
            "candidate_use_momentum_score": True,
            "candidate_use_vol_adjust": False,
            "candidate_use_industry_neutral": True,
            "candidate_use_accel": True,
            "candidate_accel_weight": 0.25,
            "candidate_use_rsl_change_1w": True,
            "candidate_rsl_change_weight": 0.10,
            "candidate_min_avg_volume_eur": 300000.0,
            "candidate_min_trust_score": 2,
            "candidate_score_min": 0.0,
            "candidate_require_top_percent": True,
            "candidate_top_percent_threshold": 0.05,
            "candidate_block_new_buys_in_weak_regime": False,
            "candidate_max_stocks_per_industry": 2,
            "candidate_use_peer_spread": True,
            "candidate_peer_spread_weight": 0.35,
            "candidate_max_distance_52w_high_pct": 20.0,
            "cluster_enabled": True,
            "candidate_use_cluster_filter": True,
            "cluster_top_n": 8,
            "cluster_min_size": 5,
            "cluster_score_w_mom12": 0.30,
            "cluster_score_w_mom6": 0.30,
            "cluster_score_w_accel": 0.40,
            "mom_weight_12m": 0.30,
            "mom_weight_6m": 0.30,
            "mom_weight_3m": 0.40,
            "history_period": "18mo",
            "info_fetch_delay_s": 0.7,
        },
    },
}


SETTING_LABELS: Dict[str, str] = {
    "industry_top_n": "Top-Branchen",
    "industry_score_min": "Branchen-Score-Min",
    "industry_breadth_min": "Breadth-Min",
    "industry_score_w_breadth": "Gewicht Breadth",
    "industry_score_w_avg": "Gewicht Avg RSL",
    "industry_score_w_median": "Gewicht Median RSL",
    "industry_score_w_leader": "Gewicht Leader",
    "candidate_top_percent_threshold": "Top-%-Schwelle",
    "candidate_require_top_percent": "Top-%-Filter",
    "candidate_min_trust_score": "Min Trust Score",
    "candidate_min_avg_volume_eur": "Min Primary Liquidity",
    "candidate_block_new_buys_in_weak_regime": "Kaufstopp bei SCHWACH",
    "candidate_max_stocks_per_industry": "Max Aktien pro Branche",
    "candidate_use_accel": "Momentum-Beschleunigung",
    "candidate_accel_weight": "Accel-Gewicht",
    "candidate_use_rsl_change_1w": "RSL 1W-Change",
    "candidate_rsl_change_weight": "RSL 1W-Gewicht",
    "candidate_use_peer_spread": "Peer-Spread nutzen",
    "candidate_peer_spread_weight": "Peer-Spread Gewicht",
    "candidate_max_distance_52w_high_pct": "Max Abstand 52W-Hoch",
    "mom_weight_12m": "Momentum 12M",
    "mom_weight_6m": "Momentum 6M",
    "mom_weight_3m": "Momentum 3M",
    "cluster_top_n": "Top-Cluster",
    "cluster_min_size": "Min Cluster-Groesse",
    "cluster_score_w_mom12": "Cluster Gewicht Mom12",
    "cluster_score_w_mom6": "Cluster Gewicht Mom6",
    "cluster_score_w_accel": "Cluster Gewicht Accel",
    "history_period": "History Zeitraum",
    "info_fetch_delay_s": "Info-Fetch Delay",
}


PRESET_DISPLAY_ORDER: List[str] = ["standard", "defensiv", "dynamisch"]
STRATEGY_METADATA_KEY = "_active_strategy_key"
STRATEGY_METADATA_SOURCE = "_active_strategy_source"
PROFILE_SOURCE_PRESET = "preset"
PROFILE_SOURCE_CUSTOM = "custom"
STRATEGY_INFO_FIELDS: Tuple[str, ...] = ("market_context", "best_for", "review_trigger")


def get_user_settings_defaults() -> Dict[str, Any]:
    return deepcopy(USER_SETTINGS_DEFAULTS)


def get_settings_presets() -> Dict[str, Dict[str, Any]]:
    return deepcopy(SETTINGS_PRESETS)


def get_preset_keys() -> List[str]:
    return list(PRESET_DISPLAY_ORDER)


def apply_preset(settings: Dict[str, Any], preset_key: str) -> Dict[str, Any]:
    preset = SETTINGS_PRESETS.get(preset_key)
    if not preset:
        raise KeyError(preset_key)
    merged = dict(settings)
    merged.update(deepcopy(preset["values"]))
    return merged


def load_custom_profiles(file_path: str) -> Dict[str, Dict[str, Any]]:
    # Start mit Built-in Profilen
    profiles: Dict[str, Dict[str, Any]] = deepcopy(BUILTIN_CUSTOM_PROFILES)

    path = str(file_path or "").strip()
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for raw_key, raw_profile in raw.items():
                    key = str(raw_key or "").strip()
                    if not key:
                        continue
                    profile = _sanitize_profile_definition(key, raw_profile)
                    if profile:
                        # Benutzer-Profile ueberschreiben Built-ins bei gleichem Key
                        profiles[key] = profile
        except Exception:
            pass

    return profiles


def save_custom_profiles(file_path: str, profiles: Dict[str, Dict[str, Any]]) -> None:
    path = str(file_path or "").strip()
    if not path:
        return
    serializable: Dict[str, Dict[str, Any]] = {}
    for key, profile in profiles.items():
        clean = _sanitize_profile_definition(key, profile)
        if clean:
            serializable[key] = clean
    with open(path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)


def build_custom_profile_key(label: str, existing_keys: Optional[List[str]] = None) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", str(label or "").strip().lower()).strip("_")
    if not base:
        base = "strategie"
    used = {str(item).strip().lower() for item in (existing_keys or []) if str(item).strip()}
    candidate = base
    index = 2
    while candidate.lower() in used:
        candidate = f"{base}_{index}"
        index += 1
    return candidate


def upsert_custom_profile(
    file_path: str,
    label: str,
    settings: Dict[str, Any],
    summary: str = "",
    why: str = "",
    market_context: str = "",
    best_for: str = "",
    review_trigger: str = "",
    profile_key: str = "",
) -> str:
    profiles = load_custom_profiles(file_path)
    key = str(profile_key or "").strip()
    if not key:
        key = build_custom_profile_key(label, list(profiles.keys()))
    profiles[key] = {
        "label": _normalize_profile_label(label) or key,
        "summary": str(summary or "").strip() or "Eigenes Strategieprofil fuer Tests und Vergleiche.",
        "why": str(why or "").strip() or "Benutzerdefiniert gespeichert, um alternative Parameter sauber getrennt zu testen.",
        "market_context": str(market_context or "").strip(),
        "best_for": str(best_for or "").strip(),
        "review_trigger": str(review_trigger or "").strip(),
        "values": _extract_profile_values(settings),
    }
    save_custom_profiles(file_path, profiles)
    return key


def delete_custom_profile(file_path: str, profile_key: str) -> bool:
    key = str(profile_key or "").strip()
    if not key:
        return False
    profiles = load_custom_profiles(file_path)
    if key not in profiles:
        return False
    del profiles[key]
    save_custom_profiles(file_path, profiles)
    return True


def detect_matching_preset(settings: Dict[str, Any]) -> Optional[str]:
    for preset_key in PRESET_DISPLAY_ORDER:
        preset_values = SETTINGS_PRESETS[preset_key]["values"]
        if all(_values_equal(settings.get(key), value) for key, value in preset_values.items()):
            return preset_key
    return None


def detect_matching_strategy_profile(
    settings: Dict[str, Any],
    custom_profiles: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    profiles = custom_profiles or {}
    metadata_key = str(settings.get(STRATEGY_METADATA_KEY, "") or "").strip()
    metadata_source = str(settings.get(STRATEGY_METADATA_SOURCE, "") or "").strip().lower()

    matched = _match_profile_from_metadata(settings, metadata_key, metadata_source, profiles)
    if matched:
        return matched

    preset_key = detect_matching_preset(settings)
    if preset_key:
        preset = SETTINGS_PRESETS[preset_key]
        return {
            "key": preset_key,
            "label": str(preset.get("label", preset_key)),
            "summary": str(preset.get("summary", "")),
            "why": str(preset.get("why", "")),
            "market_context": str(preset.get("market_context", "")),
            "best_for": str(preset.get("best_for", "")),
            "review_trigger": str(preset.get("review_trigger", "")),
            "is_manual": False,
            "is_custom": False,
            "source": PROFILE_SOURCE_PRESET,
        }

    for profile_key, profile in profiles.items():
        values = profile.get("values", {})
        if _profile_matches(settings, values):
            return {
                "key": profile_key,
                "label": str(profile.get("label", profile_key)),
                "summary": str(profile.get("summary", "")),
                "why": str(profile.get("why", "")),
                "market_context": str(profile.get("market_context", "")),
                "best_for": str(profile.get("best_for", "")),
                "review_trigger": str(profile.get("review_trigger", "")),
                "is_manual": False,
                "is_custom": True,
                "source": PROFILE_SOURCE_CUSTOM,
            }
    return None


def refresh_strategy_metadata(
    settings: Dict[str, Any],
    custom_profiles: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    updated = dict(settings)
    info = detect_matching_strategy_profile(updated, custom_profiles)
    if info:
        updated[STRATEGY_METADATA_KEY] = info["key"]
        updated[STRATEGY_METADATA_SOURCE] = info["source"]
    else:
        updated.pop(STRATEGY_METADATA_KEY, None)
        updated.pop(STRATEGY_METADATA_SOURCE, None)
    return updated


def get_active_preset_info(settings: Dict[str, Any]) -> Dict[str, Any]:
    preset_key = detect_matching_preset(settings)
    if not preset_key:
        return {
            "key": "",
            "label": "Manuelle Kombination",
            "summary": "Mindestens ein Kernwert weicht von allen Preset-Paketen ab.",
            "why": "Die Einstellungen wurden individuell angepasst.",
            "is_manual": True,
        }
    preset = SETTINGS_PRESETS[preset_key]
    return {
        "key": preset_key,
        "label": str(preset.get("label", preset_key)),
        "summary": str(preset.get("summary", "")),
        "why": str(preset.get("why", "")),
        "market_context": str(preset.get("market_context", "")),
        "best_for": str(preset.get("best_for", "")),
        "review_trigger": str(preset.get("review_trigger", "")),
        "is_manual": False,
        "is_custom": False,
        "source": PROFILE_SOURCE_PRESET,
    }


def get_active_strategy_info(
    settings: Dict[str, Any],
    custom_profiles: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    matched = detect_matching_strategy_profile(settings, custom_profiles)
    if matched:
        return matched
    return {
        "key": "",
        "label": "Manuelle Kombination",
        "summary": "Mindestens ein Kernwert weicht von allen Strategieprofilen ab.",
        "why": "Die Einstellungen wurden individuell angepasst.",
        "market_context": "",
        "best_for": "",
        "review_trigger": "",
        "is_manual": True,
        "is_custom": False,
        "source": "manual",
    }


def get_settings_diff(settings: Dict[str, Any], baseline: Optional[Dict[str, Any]] = None) -> List[Tuple[str, Any, Any]]:
    base = baseline or USER_SETTINGS_DEFAULTS
    diff: List[Tuple[str, Any, Any]] = []
    for key in base:
        current = settings.get(key, base[key])
        if not _values_equal(current, base[key]):
            diff.append((key, current, base[key]))
    return diff


def format_setting_value(key: str, value: Any) -> str:
    if key in {
        "candidate_top_percent_threshold",
        "heatmap_warn_percent",
        "heatmap_full_percent",
        "candidate_max_distance_52w_high_pct",
    }:
        factor = 100.0 if key == "candidate_top_percent_threshold" else 1.0
        return f"{float(value) * factor:.1f}%"
    if key == "candidate_min_avg_volume_eur":
        return f"{float(value) / 1_000_000:.1f} Mio EUR"
    if key in {"info_fetch_delay_s"}:
        return f"{float(value):.2f}s"
    if isinstance(value, bool):
        return "an" if value else "aus"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) == bool(right)
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(float(left) - float(right)) < 1e-9
    return str(left) == str(right)


def _normalize_profile_label(label: Any) -> str:
    text = re.sub(r"\s+", " ", str(label or "").strip())
    return text


def _extract_profile_values(settings: Dict[str, Any]) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    for key, default_value in USER_SETTINGS_DEFAULTS.items():
        values[key] = deepcopy(settings.get(key, default_value))
    return values


def _sanitize_profile_definition(profile_key: str, raw_profile: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_profile, dict):
        return None
    if "values" in raw_profile and isinstance(raw_profile.get("values"), dict):
        values_raw = raw_profile.get("values", {})
        label = _normalize_profile_label(raw_profile.get("label", profile_key)) or profile_key
        summary = str(raw_profile.get("summary", "") or "").strip()
        why = str(raw_profile.get("why", "") or "").strip()
    else:
        values_raw = raw_profile
        label = _normalize_profile_label(profile_key) or profile_key
        summary = ""
        why = ""
    return {
        "label": label,
        "summary": summary,
        "why": why,
        "market_context": str(raw_profile.get("market_context", "") or "").strip(),
        "best_for": str(raw_profile.get("best_for", "") or "").strip(),
        "review_trigger": str(raw_profile.get("review_trigger", "") or "").strip(),
        "values": _extract_profile_values(values_raw),
    }


def _profile_matches(settings: Dict[str, Any], profile_values: Dict[str, Any]) -> bool:
    return all(_values_equal(settings.get(key), value) for key, value in profile_values.items())


def _match_profile_from_metadata(
    settings: Dict[str, Any],
    profile_key: str,
    profile_source: str,
    custom_profiles: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not profile_key or profile_source not in {PROFILE_SOURCE_PRESET, PROFILE_SOURCE_CUSTOM}:
        return None
    if profile_source == PROFILE_SOURCE_PRESET:
        profile = SETTINGS_PRESETS.get(profile_key)
        if profile and _profile_matches(settings, profile.get("values", {})):
            return {
                "key": profile_key,
                "label": str(profile.get("label", profile_key)),
                "summary": str(profile.get("summary", "")),
                "why": str(profile.get("why", "")),
                "market_context": str(profile.get("market_context", "")),
                "best_for": str(profile.get("best_for", "")),
                "review_trigger": str(profile.get("review_trigger", "")),
                "is_manual": False,
                "is_custom": False,
                "source": PROFILE_SOURCE_PRESET,
            }
        return None
    profile = custom_profiles.get(profile_key)
    if profile and _profile_matches(settings, profile.get("values", {})):
            return {
                "key": profile_key,
                "label": str(profile.get("label", profile_key)),
                "summary": str(profile.get("summary", "")),
                "why": str(profile.get("why", "")),
                "market_context": str(profile.get("market_context", "")),
                "best_for": str(profile.get("best_for", "")),
                "review_trigger": str(profile.get("review_trigger", "")),
                "is_manual": False,
                "is_custom": True,
                "source": PROFILE_SOURCE_CUSTOM,
            }
    return None
