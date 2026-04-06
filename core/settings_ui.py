import os
from typing import Any, Callable, Dict, Tuple, cast

from . import settings_catalog as settings_catalog_core


def configure_user_settings_interactive(
    config: Dict[str, Any],
    defaults: Dict[str, Any],
    presets: Dict[str, Dict[str, Any]],
    load_user_settings: Callable[[], Dict[str, Any]],
    save_user_settings: Callable[[Dict[str, Any]], None],
    apply_user_settings: Callable[[Dict[str, Any]], None],
    to_float: Callable[[Any, float], float],
    sanitize_heatmap: Callable[[Any, Any], Tuple[float, float]],
    legacy_menu: Callable[[], None],
) -> None:
    settings = load_user_settings()
    profile_file = str(config.get("strategy_profiles_file", "") or "").strip()

    def get_custom_profiles() -> Dict[str, Dict[str, Any]]:
        return settings_catalog_core.load_custom_profiles(profile_file)

    def save_now() -> None:
        refreshed = settings_catalog_core.refresh_strategy_metadata(settings, get_custom_profiles())
        settings.clear()
        settings.update(refreshed)
        save_user_settings(settings)
        apply_user_settings(settings)
        print("Einstellungen gespeichert und angewendet.")

    def bool_input(prompt: str) -> bool:
        return input(prompt).strip().lower() in (
            "j", "ja", "y", "yes", "1", "true", "an", "on"
        )

    def show_strategy_profiles() -> None:
        def ordered_custom_profiles() -> list:
            return sorted(get_custom_profiles().items(), key=lambda item: str(item[1].get("label", item[0])).lower())

        def print_custom_profile_overview(items: list) -> None:
            for idx, (_, profile) in enumerate(items, start=1):
                values = profile["values"]
                print(f"\n{idx}) {profile['label']}")
                if profile.get("summary"):
                    print(f"   Zweck: {profile['summary']}")
                if profile.get("why"):
                    print(f"   Warum so: {profile['why']}")
                if profile.get("market_context"):
                    print(f"   Marktumfeld: {profile['market_context']}")
                if profile.get("best_for"):
                    print(f"   Einsatz: {profile['best_for']}")
                if profile.get("review_trigger"):
                    print(f"   Review: {profile['review_trigger']}")
                print(
                    "   Profil: "
                    f"Top-Branchen={int(values['industry_top_n'])}, "
                    f"Score-Min={float(values['industry_score_min']):.2f}, "
                    f"Top-%={float(values['candidate_top_percent_threshold']) * 100:.1f}%, "
                    f"Trust>={int(values['candidate_min_trust_score'])}, "
                    f"Cluster={int(values['cluster_top_n'])}/{int(values['cluster_min_size'])}"
                )

        while True:
            custom_profiles = get_custom_profiles()
            active_strategy = settings_catalog_core.get_active_strategy_info(settings, custom_profiles)
            print("\n--- STRATEGIEPROFILE ---")
            print(f"Aktiv: {active_strategy['label']}")
            print("Die Standardpakete bleiben fest. Eigene Profile liegen separat und koennen gefahrlos getestet werden.")
            print("1) Standard-Pakete ansehen / anwenden")
            print("2) Eigene Strategieprofile laden")
            print("3) Aktuelle Einstellungen speichern / ueberschreiben / duplizieren")
            print("4) Eigenes Strategieprofil loeschen")
            print("0) Zurueck")
            sub = input("Auswahl [0]: ").strip()
            if sub in ("", "0"):
                return
            if sub == "1":
                print("\n--- STANDARD-PAKETE ---")
                print("Die Pakete aendern die strategischen Kernwerte gemeinsam, damit sie zusammenpassen.")
                for idx, preset_key in enumerate(settings_catalog_core.get_preset_keys(), start=1):
                    preset = presets[preset_key]
                    values = preset["values"]
                    print(f"\n{idx}) {preset['label']}")
                    print(f"   Zweck: {preset['summary']}")
                    print(f"   Warum so: {preset['why']}")
                    if preset.get("market_context"):
                        print(f"   Marktumfeld: {preset['market_context']}")
                    if preset.get("best_for"):
                        print(f"   Einsatz: {preset['best_for']}")
                    if preset.get("review_trigger"):
                        print(f"   Review: {preset['review_trigger']}")
                    print(
                        "   Enthalten: "
                        f"Top-Branchen={int(values['industry_top_n'])}, "
                        f"Branchen-Score-Min={float(values['industry_score_min']):.2f}, "
                        f"Top-%={float(values['candidate_top_percent_threshold']) * 100:.1f}%, "
                        f"Trust>={int(values['candidate_min_trust_score'])}, "
                        f"Accel={settings_catalog_core.format_setting_value('candidate_use_accel', values['candidate_use_accel'])} "
                        f"({float(values['candidate_accel_weight']):.2f}), "
                        f"Cluster={int(values['cluster_top_n'])}/{int(values['cluster_min_size'])}, "
                        f"Mom={float(values['mom_weight_12m']):.2f}/{float(values['mom_weight_6m']):.2f}/{float(values['mom_weight_3m']):.2f}"
                    )
                choice = input("\nPaket anwenden [1-3, Enter=zurueck]: ").strip()
                key_map = {"1": "standard", "2": "defensiv", "3": "dynamisch"}
                preset_key = key_map.get(choice)
                if preset_key:
                    settings.update(presets[preset_key]["values"])
                    settings[settings_catalog_core.STRATEGY_METADATA_KEY] = preset_key
                    settings[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_PRESET
                    save_now()
            elif sub == "2":
                if not custom_profiles:
                    print("Noch keine eigenen Strategieprofile vorhanden.")
                    continue
                ordered_profiles = ordered_custom_profiles()
                print("\n--- EIGENE STRATEGIEPROFILE ---")
                print_custom_profile_overview(ordered_profiles)
                choice = input("\nProfil laden [Nummer, Enter=zurueck]: ").strip()
                if choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(ordered_profiles):
                        profile_key, profile = ordered_profiles[idx]
                        settings.update(profile["values"])
                        settings[settings_catalog_core.STRATEGY_METADATA_KEY] = profile_key
                        settings[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_CUSTOM
                        save_now()
            elif sub == "3":
                print("\n--- PROFIL SPEICHERN ---")
                print("1) Als neues Profil speichern")
                print("2) Bestehendes Profil ueberschreiben")
                print("3) Bestehendes Profil als Kopie speichern")
                save_choice = input("Auswahl [0]: ").strip()
                if save_choice in ("", "0"):
                    continue
                if save_choice == "1":
                    label = input("Name fuer neues Strategieprofil: ").strip()
                    if not label:
                        print("Speichern abgebrochen: Name fehlt.")
                        continue
                    summary: str = input("Kurzbeschreibung / Zweck [optional]: ").strip()
                    why: str = input("Warum dieses Profil? [optional]: ").strip()
                    profile_key = settings_catalog_core.upsert_custom_profile(
                        file_path=profile_file,
                        label=label,
                        settings=settings,
                        summary=summary,
                        why=why,
                    )
                    settings[settings_catalog_core.STRATEGY_METADATA_KEY] = profile_key
                    settings[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_CUSTOM
                    save_now()
                    print(f"Profil gespeichert: {label}")
                elif save_choice in {"2", "3"}:
                    ordered_profiles = ordered_custom_profiles()
                    if not ordered_profiles:
                        print("Noch keine eigenen Strategieprofile vorhanden.")
                        continue
                    print("\n--- EIGENE STRATEGIEPROFILE ---")
                    print_custom_profile_overview(ordered_profiles)
                    choice = input("Profil waehlen [Nummer, Enter=zurueck]: ").strip()
                    if not choice.isdigit():
                        continue
                    idx = int(choice) - 1
                    if not (0 <= idx < len(ordered_profiles)):
                        continue
                    target_key, target_profile = ordered_profiles[idx]
                    if save_choice == "2":
                        label = input(f"Name [{target_profile['label']}]: ").strip() or str(target_profile["label"])
                        summary = input(f"Kurzbeschreibung [{target_profile.get('summary', '')}]: ").strip() or str(target_profile.get("summary", ""))
                        why = input(f"Warum [{target_profile.get('why', '')}]: ").strip() or str(target_profile.get("why", ""))
                        profile_key = settings_catalog_core.upsert_custom_profile(
                            file_path=profile_file,
                            label=label,
                            settings=settings,
                            summary=summary,
                            why=why,
                            profile_key=target_key,
                        )
                        settings[settings_catalog_core.STRATEGY_METADATA_KEY] = profile_key
                        settings[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_CUSTOM
                        save_now()
                        print(f"Profil ueberschrieben: {label}")
                    else:
                        default_label = f"{target_profile['label']} Kopie"
                        label = input(f"Name fuer Kopie [{default_label}]: ").strip() or default_label
                        summary = input(f"Kurzbeschreibung [{target_profile.get('summary', '')}]: ").strip() or str(target_profile.get("summary", ""))
                        why = input(f"Warum [{target_profile.get('why', '')}]: ").strip() or str(target_profile.get("why", ""))
                        profile_key = settings_catalog_core.upsert_custom_profile(
                            file_path=profile_file,
                            label=label,
                            settings=settings,
                            summary=summary,
                            why=why,
                        )
                        settings[settings_catalog_core.STRATEGY_METADATA_KEY] = profile_key
                        settings[settings_catalog_core.STRATEGY_METADATA_SOURCE] = settings_catalog_core.PROFILE_SOURCE_CUSTOM
                        save_now()
                        print(f"Profil als Kopie gespeichert: {label}")
            elif sub == "4":
                if not custom_profiles:
                    print("Keine eigenen Strategieprofile zum Loeschen vorhanden.")
                    continue
                ordered_profiles = ordered_custom_profiles()
                print("\n--- STRATEGIEPROFIL LOESCHEN ---")
                for idx, (_, profile) in enumerate(ordered_profiles, start=1):
                    print(f"{idx}) {profile['label']}")
                choice = input("Profil loeschen [Nummer, Enter=zurueck]: ").strip()
                if not choice.isdigit():
                    continue
                idx = int(choice) - 1
                if not (0 <= idx < len(ordered_profiles)):
                    continue
                profile_key, profile = ordered_profiles[idx]
                if not bool_input(f"'{profile['label']}' wirklich loeschen? (j/n): "):
                    print("Loeschen abgebrochen.")
                    continue
                if settings_catalog_core.delete_custom_profile(profile_file, profile_key):
                    if settings.get(settings_catalog_core.STRATEGY_METADATA_KEY) == profile_key and str(settings.get(settings_catalog_core.STRATEGY_METADATA_SOURCE, "")).lower() == settings_catalog_core.PROFILE_SOURCE_CUSTOM:
                        settings.pop(settings_catalog_core.STRATEGY_METADATA_KEY, None)
                        settings.pop(settings_catalog_core.STRATEGY_METADATA_SOURCE, None)
                        save_now()
                    print(f"Profil geloescht: {profile['label']}")

    def show_changed_settings() -> None:
        print("\n--- ABWEICHUNGEN VOM STANDARD ---")
        active_strategy = settings_catalog_core.get_active_strategy_info(settings, get_custom_profiles())
        print(f"Aktives Profil: {active_strategy['label']}")
        diff = settings_catalog_core.get_settings_diff(settings, defaults)
        if not diff:
            print("Keine Abweichungen. Es ist exakt der Standard aktiv.")
            return
        for key, current, baseline in diff:
            label = settings_catalog_core.SETTING_LABELS.get(key, key)
            print(
                f"- {label}: {settings_catalog_core.format_setting_value(key, current)} "
                f"(Standard: {settings_catalog_core.format_setting_value(key, baseline)})"
            )

    while True:
        warn_value, full_value = sanitize_heatmap(
            settings.get("heatmap_warn_percent", config["heatmap_warn_percent"]),
            settings.get("heatmap_full_percent", config["heatmap_full_percent"]),
        )
        active_strategy = settings_catalog_core.get_active_strategy_info(settings, get_custom_profiles())
        active_strategy_label = active_strategy["label"]

        print("\n\033[96m--- EINSTELLUNGEN ---\033[0m")
        print("Empfohlen fuer Alltag: 1 Strategieprofile, 2 Strategie, 3 Kandidaten, 4 Daten")
        print(f" Aktives Profil: {active_strategy_label}")
        print(
            f" Strategie: Top-Branchen={int(to_float(settings.get('industry_top_n', config['industry_top_n']), config['industry_top_n']))}"
            f" | Score-Min={to_float(settings.get('industry_score_min', config['industry_score_min']), config['industry_score_min']):.2f}"
            f" | Breadth-Min={to_float(settings.get('industry_breadth_min', config['industry_breadth_min']), config['industry_breadth_min']):.2f}"
        )
        print(
            f" Kandidaten: Top-%-Filter={'an' if bool(settings.get('candidate_require_top_percent', config['candidate_require_top_percent'])) else 'aus'}"
            f" | Schwelle={to_float(settings.get('candidate_top_percent_threshold', config['candidate_top_percent_threshold']), config['candidate_top_percent_threshold']) * 100:.1f}%"
            f" | Trust>={int(to_float(settings.get('candidate_min_trust_score', config['candidate_min_trust_score']), config['candidate_min_trust_score']))}"
        )
        print(
            f" Cluster: Filter={'an' if bool(settings.get('candidate_use_cluster_filter', config['candidate_use_cluster_filter'])) else 'aus'}"
            f" | Top={int(to_float(settings.get('cluster_top_n', config['cluster_top_n']), config['cluster_top_n']))}"
            f" | Aktiv={'an' if bool(settings.get('cluster_enabled', config['cluster_enabled'])) else 'aus'}"
        )
        print(
            f" Daten: History={str(settings.get('history_period', config['history_period']) or config['history_period'])}"
            f" | Info Delay={to_float(settings.get('info_fetch_delay_s', config['info_fetch_delay_s']), config['info_fetch_delay_s']):.2f}s"
            f" | Cache={to_float(settings.get('cache_duration_hours', config['cache_duration_hours']), config['cache_duration_hours']):.0f}h"
        )
        print(f" Anzeige: Heatmap={warn_value:.1f}/{full_value:.1f}% | Strict={'an' if bool(settings.get('strict_mode', config['strict_mode'])) else 'aus'}")
        print("\n1) Strategieprofile & Presets")
        print("2) Kernstrategie & Branchen")
        print("3) Kandidaten & Qualitaet")
        print("4) Daten & Anzeige")
        print("5) Reset / Aktionen")
        print("6) Nur Abweichungen vom Standard anzeigen")
        print("7) Expertenmodus (alle Einstellungen)")
        print("0) Zurueck")
        choice = input("Auswahl [0]: ").strip()
        if choice in ("", "0"):
            return
        if choice == "1":
            show_strategy_profiles()
        elif choice == "2":
            print("\n--- KERNSTRATEGIE & BRANCHEN ---")
            print("1) Top-Branchen")
            print("2) Branchen-Score-Min")
            print("3) Breadth-Min")
            print("4) Branchen-Trend an/aus")
            print("5) Branchen-Trend Wochen")
            sub = input("Auswahl [0]: ").strip()
            if sub == "1":
                settings["industry_top_n"] = int(to_float(input("Neue Anzahl Top-Branchen: ").strip(), config["industry_top_n"]))
                save_now()
            elif sub == "2":
                settings["industry_score_min"] = to_float(input("Neuer Branchen-Score-Min: ").strip().replace(",", "."), config["industry_score_min"])
                save_now()
            elif sub == "3":
                settings["industry_breadth_min"] = to_float(input("Neuer Breadth-Min: ").strip().replace(",", "."), config["industry_breadth_min"])
                save_now()
            elif sub == "4":
                settings["industry_trend_enabled"] = bool_input("Branchen-Trend aktiv? (j/n): ")
                save_now()
            elif sub == "5":
                settings["industry_trend_weeks"] = int(to_float(input("Branchen-Trend Wochen: ").strip(), config["industry_trend_weeks"]))
                save_now()
        elif choice == "3":
            print("\n--- KANDIDATEN & QUALITAET ---")
            print("1) Nur Top-% zulassen")
            print("2) Nachkauf-Schwelle in %")
            print("3) Min Trust Score")
            print("4) Min Primary Liquidity in Mio EUR")
            print("5) Momentum-Score an/aus")
            print("6) Vol-Adjustierung an/aus")
            print("7) Industry-Neutralisierung an/aus")
            print("8) Momentum-Beschleunigung an/aus")
            print("9) Accel-Gewicht")
            print("10) RSL 1W-Change an/aus")
            print("11) RSL 1W-Gewicht")
            print("12) Cluster-Filter an/aus")
            print("13) Top-Cluster")
            print("14) Strict Mode an/aus")
            print("15) Keine neuen Kaeufe bei Markt SCHWACH")
            print("16) Max Aktien pro Branche (0=aus)")
            print("17) Peer-Spread an/aus")
            print("18) Peer-Spread Gewicht")
            print("19) Max Abstand zum 52W-Hoch in % (0=aus)")
            sub = input("Auswahl [0]: ").strip()
            if sub == "1":
                settings["candidate_require_top_percent"] = bool_input("Nur Top-% zulassen? (j/n): ")
                save_now()
            elif sub == "2":
                settings["candidate_top_percent_threshold"] = to_float(input("Neue Nachkauf-Schwelle in %: ").strip().replace(",", "."), config["candidate_top_percent_threshold"] * 100.0) / 100.0
                save_now()
            elif sub == "3":
                settings["candidate_min_trust_score"] = int(to_float(input("Min Trust Score (0-3): ").strip(), config["candidate_min_trust_score"]))
                save_now()
            elif sub == "4":
                settings["candidate_min_avg_volume_eur"] = to_float(input("Min Primary Liquidity in Mio EUR: ").strip().replace(",", "."), 0.0) * 1_000_000
                save_now()
            elif sub == "5":
                settings["candidate_use_momentum_score"] = bool_input("Momentum-Score nutzen? (j/n): ")
                save_now()
            elif sub == "6":
                settings["candidate_use_vol_adjust"] = bool_input("Vol-Adjustierung nutzen? (j/n): ")
                save_now()
            elif sub == "7":
                settings["candidate_use_industry_neutral"] = bool_input("Industry-Neutralisierung nutzen? (j/n): ")
                save_now()
            elif sub == "8":
                settings["candidate_use_accel"] = bool_input("Momentum-Beschleunigung nutzen? (j/n): ")
                save_now()
            elif sub == "9":
                settings["candidate_accel_weight"] = to_float(input("Neues Accel-Gewicht: ").strip().replace(",", "."), config["candidate_accel_weight"])
                save_now()
            elif sub == "10":
                settings["candidate_use_rsl_change_1w"] = bool_input("RSL 1W-Change nutzen? (j/n): ")
                save_now()
            elif sub == "11":
                settings["candidate_rsl_change_weight"] = to_float(input("Neues RSL 1W-Gewicht: ").strip().replace(",", "."), config["candidate_rsl_change_weight"])
                save_now()
            elif sub == "12":
                settings["candidate_use_cluster_filter"] = bool_input("Cluster-Filter nutzen? (j/n): ")
                save_now()
            elif sub == "13":
                settings["cluster_top_n"] = int(to_float(input("Neue Top-Cluster Anzahl: ").strip(), config["cluster_top_n"]))
                save_now()
            elif sub == "14":
                settings["strict_mode"] = bool_input("Strict Mode aktiv? (j/n): ")
                save_now()
            elif sub == "15":
                settings["candidate_block_new_buys_in_weak_regime"] = bool_input("Keine neuen Kaeufe bei Marktregime SCHWACH? (j/n): ")
                save_now()
            elif sub == "16":
                settings["candidate_max_stocks_per_industry"] = int(to_float(input("Max Aktien pro Branche (0=aus): ").strip(), config["candidate_max_stocks_per_industry"]))
                save_now()
            elif sub == "17":
                settings["candidate_use_peer_spread"] = bool_input("Peer-Spread im Kandidaten-Scoring nutzen? (j/n): ")
                save_now()
            elif sub == "18":
                settings["candidate_peer_spread_weight"] = to_float(input("Neues Peer-Spread Gewicht: ").strip().replace(",", "."), config["candidate_peer_spread_weight"])
                save_now()
            elif sub == "19":
                settings["candidate_max_distance_52w_high_pct"] = to_float(input("Max Abstand zum 52W-Hoch in % (0=aus): ").strip().replace(",", "."), config["candidate_max_distance_52w_high_pct"])
                save_now()
        elif choice == "4":
            print("\n--- DATEN & ANZEIGE ---")
            print("1) Kurs-Cache Gueltigkeit (h)")
            print("2) ETF-Cache Gueltigkeit (h)")
            print("3) Info-Fetch Delay (s)")
            print("4) History Zeitraum")
            print("5) Heatmap-Warnschwelle")
            print("6) Heatmap-Vollschwelle")
            sub = input("Auswahl [0]: ").strip()
            if sub == "1":
                settings["cache_duration_hours"] = to_float(input("Neue Gueltigkeit fuer Kurs-Cache (h): ").strip(), config["cache_duration_hours"])
                save_now()
            elif sub == "2":
                settings["etf_cache_duration_hours"] = to_float(input("Neue Gueltigkeit fuer ETF-Cache (h): ").strip(), config["etf_cache_duration_hours"])
                save_now()
            elif sub == "3":
                settings["info_fetch_delay_s"] = to_float(input("Neuer Info-Fetch Delay (s): ").strip().replace(",", "."), config["info_fetch_delay_s"])
                save_now()
            elif sub == "4":
                settings["history_period"] = input("Neuer History Zeitraum (z.B. 18mo/24mo): ").strip()
                save_now()
            elif sub == "5":
                settings["heatmap_warn_percent"] = to_float(input("Neue Warnschwelle in %: ").strip().replace(",", "."), config["heatmap_warn_percent"])
                save_now()
            elif sub == "6":
                settings["heatmap_full_percent"] = to_float(input("Neue Vollschwelle in %: ").strip().replace(",", "."), config["heatmap_full_percent"])
                save_now()
        elif choice == "5":
            print("\n--- RESET / AKTIONEN ---")
            print("1) Kurs-Cache loeschen")
            print("2) ETF-Cache loeschen")
            print("3) Heatmap auf Standard")
            print("4) Cache-/Rate-Limit auf Standard")
            print("5) Alle Einstellungen auf Standard")
            sub = input("Auswahl [0]: ").strip()
            if sub == "1":
                if os.path.exists(config["history_cache_file"]):
                    os.remove(config["history_cache_file"])
                    print("Kurs-Cache wurde geloescht.")
            elif sub == "2":
                if os.path.exists(config["etf_cache_file"]):
                    os.remove(config["etf_cache_file"])
                    print("ETF-Cache wurde geloescht.")
            elif sub == "3":
                settings["heatmap_warn_percent"] = defaults["heatmap_warn_percent"]
                settings["heatmap_full_percent"] = defaults["heatmap_full_percent"]
                save_now()
            elif sub == "4":
                for key in (
                    "cache_duration_hours",
                    "etf_cache_duration_hours",
                    "info_cache_unknown_expiry_days",
                    "info_fetch_delay_s",
                    "info_fetch_quiet",
                    "batch_sleep_min_s",
                    "batch_sleep_max_s",
                    "rate_limit_delay_min_s",
                    "rate_limit_delay_max_s",
                    "rate_limit_backoff_step_s",
                    "rate_limit_log_every",
                ):
                    settings[key] = defaults[key]
                save_now()
            elif sub == "5":
                settings.clear()
                settings.update(defaults)
                save_now()
        elif choice == "6":
            show_changed_settings()
        elif choice == "7":
            legacy_menu()
