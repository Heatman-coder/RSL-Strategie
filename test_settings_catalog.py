from pathlib import Path
from uuid import uuid4

from core import settings_catalog as settings_catalog_core


def _profile_test_file(name: str) -> str:
    tmp_dir = Path("tests") / "_tmp_profiles"
    tmp_dir.mkdir(exist_ok=True)
    file_path = tmp_dir / f"{uuid4().hex}_{name}"
    return str(file_path)


def test_default_settings_match_standard_preset_for_preset_keys():
    defaults = settings_catalog_core.get_user_settings_defaults()
    standard = settings_catalog_core.get_settings_presets()["standard"]["values"]

    for key, value in standard.items():
        assert defaults[key] == value


def test_detect_matching_preset_returns_standard_for_defaults():
    defaults = settings_catalog_core.get_user_settings_defaults()
    assert settings_catalog_core.detect_matching_preset(defaults) == "standard"


def test_get_settings_diff_reports_only_changed_values():
    defaults = settings_catalog_core.get_user_settings_defaults()
    changed = dict(defaults)
    changed["industry_top_n"] = 20
    changed["candidate_top_percent_threshold"] = 0.03

    diff = settings_catalog_core.get_settings_diff(changed, defaults)
    diff_keys = [item[0] for item in diff]

    assert diff_keys == ["industry_top_n", "candidate_top_percent_threshold"]


def test_get_active_preset_info_detects_manual_combination():
    defaults = settings_catalog_core.get_user_settings_defaults()
    changed = dict(defaults)
    changed["industry_top_n"] = 17

    info = settings_catalog_core.get_active_preset_info(changed)

    assert info["label"] == "Manuelle Kombination"
    assert info["is_manual"] is True


def test_custom_profile_roundtrip_and_detection():
    defaults = settings_catalog_core.get_user_settings_defaults()
    changed = dict(defaults)
    changed["industry_top_n"] = 18
    changed["candidate_top_percent_threshold"] = 0.03

    profile_file = _profile_test_file("strategy_profiles_roundtrip.json")
    profile_key = settings_catalog_core.upsert_custom_profile(
        profile_file,
        label="Mein Testprofil",
        settings=changed,
        summary="Test",
        why="Zum Vergleichen",
        market_context="Trendmarkt",
        best_for="Leader",
        review_trigger="Bei Regimewechsel",
    )

    profiles = settings_catalog_core.load_custom_profiles(profile_file)
    info = settings_catalog_core.get_active_strategy_info(changed, profiles)

    assert profile_key in profiles
    assert info["label"] == "Mein Testprofil"
    assert info["source"] == settings_catalog_core.PROFILE_SOURCE_CUSTOM
    assert info["is_manual"] is False
    assert info["market_context"] == "Trendmarkt"
    assert info["best_for"] == "Leader"
    assert info["review_trigger"] == "Bei Regimewechsel"


def test_refresh_strategy_metadata_prefers_selected_custom_profile():
    defaults = settings_catalog_core.get_user_settings_defaults()
    profile_file = _profile_test_file("strategy_profiles_selected.json")
    profile_key = settings_catalog_core.upsert_custom_profile(
        profile_file,
        label="Standard als Kopie",
        settings=defaults,
    )
    profiles = settings_catalog_core.load_custom_profiles(profile_file)
    selected = dict(defaults)
    selected[settings_catalog_core.STRATEGY_METADATA_KEY] = profile_key
    selected[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_CUSTOM

    refreshed = settings_catalog_core.refresh_strategy_metadata(selected, profiles)
    info = settings_catalog_core.get_active_strategy_info(refreshed, profiles)

    assert info["label"] == "Standard als Kopie"
    assert info["source"] == settings_catalog_core.PROFILE_SOURCE_CUSTOM


def test_delete_custom_profile_removes_entry():
    profile_file = _profile_test_file("strategy_profiles_delete.json")
    defaults = settings_catalog_core.get_user_settings_defaults()
    profile_key = settings_catalog_core.upsert_custom_profile(
        profile_file,
        label="Loeschprofil",
        settings=defaults,
    )

    assert settings_catalog_core.delete_custom_profile(profile_file, profile_key) is True
    profiles = settings_catalog_core.load_custom_profiles(profile_file)
    assert profile_key not in profiles


def test_upsert_custom_profile_can_overwrite_existing_key():
    profile_file = _profile_test_file("strategy_profiles_overwrite.json")
    defaults = settings_catalog_core.get_user_settings_defaults()
    first_key = settings_catalog_core.upsert_custom_profile(
        profile_file,
        label="Erstes Profil",
        settings=defaults,
        summary="Alt",
    )
    changed = dict(defaults)
    changed["industry_top_n"] = 22

    second_key = settings_catalog_core.upsert_custom_profile(
        profile_file,
        label="Aktualisiertes Profil",
        settings=changed,
        summary="Neu",
        profile_key=first_key,
    )
    profiles = settings_catalog_core.load_custom_profiles(profile_file)

    assert first_key == second_key
    assert profiles[first_key]["label"] == "Aktualisiertes Profil"
    assert profiles[first_key]["summary"] == "Neu"
    assert profiles[first_key]["values"]["industry_top_n"] == 22


def test_build_custom_profile_key_avoids_duplicates():
    key = settings_catalog_core.build_custom_profile_key(
        "Mein Profil",
        existing_keys=["mein_profil", "mein_profil_2"],
    )
    assert key == "mein_profil_3"
