#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core import app_config as app_config_core
from core import app_support as app_support_core
from core import financedatabase_universe as fd_universe_core
from core import final_support as final_support_core


class _NullProgress:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __enter__(self) -> "_NullProgress":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def update(self, n: int = 1) -> None:
        return None


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("fd_universe_audit")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _load_runtime_config(project_root: Path) -> Dict[str, Any]:
    config = app_config_core.build_base_config(str(project_root), str(project_root / "reports"))
    settings = final_support_core.load_json_config(config["user_settings_file"])
    if isinstance(settings, dict):
        app_config_core.apply_user_settings(
            config=config,
            settings=settings,
            to_float=final_support_core.to_float,
            to_bool=final_support_core.to_bool,
            normalize_weights=final_support_core.normalize_weights,
        )
    # Audit soll bevorzugt mit vorhandenen lokalen Artefakten arbeiten und nicht
    # unnötig Netz-Downloads triggern, nur weil ein Cache formal abgelaufen ist.
    config["etf_cache_duration_hours"] = max(float(config.get("etf_cache_duration_hours", 168) or 168), 24 * 365)
    return config


def _load_selection(project_root: Path, explicit_symbols: List[str]) -> Dict[str, Any]:
    etf_config_path = project_root / "etf_config.json"
    etf_config = json.loads(etf_config_path.read_text(encoding="utf-8")) if etf_config_path.exists() else {}
    selected_syms = explicit_symbols or list(etf_config.get("selected_symbols", []))
    etf_options = dict(etf_config.get("options", {}))
    return {
        "selected_syms": selected_syms,
        "etf_options": etf_options,
    }


def _prepare_universe(
    selected_syms: List[str],
    etf_options: Dict[str, Any],
    config: Dict[str, Any],
    logger: logging.Logger,
) -> pd.DataFrame:
    return app_support_core.prepare_ticker_universe(
        selected_syms=selected_syms,
        etf_options=etf_options,
        config=config,
        logger=logger,
        make_progress_fn=_NullProgress,
        download_ishares_csv=lambda url, log_label=True: final_support_core.download_ishares_csv(url, logger, log_label),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit fuer die FinanceDatabase-Integration im Universumsaufbau.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Projektwurzel. Standard ist das Repo-Root.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=[],
        help="Optionale Auswahl statt etf_config.json, z.B. IVV SOXX XETRA.",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    logger = _build_logger()
    config = _load_runtime_config(project_root)
    selection = _load_selection(project_root, [str(item).strip().upper() for item in args.symbols if str(item).strip()])
    selected_syms = [str(item).strip().upper() for item in selection["selected_syms"] if str(item).strip()]
    etf_options = selection["etf_options"]

    base_selected = [symbol for symbol in selected_syms if symbol != "FDB"]
    compare_selected = list(dict.fromkeys(base_selected + ["FDB"]))

    logger.info("Basis-Auswahl: %s", ", ".join(base_selected) if base_selected else "(leer)")
    logger.info("Vergleich mit FDB: %s", ", ".join(compare_selected))

    base_df = _prepare_universe(base_selected, etf_options, config, logger) if base_selected else pd.DataFrame()
    location_suffix_map = final_support_core.load_json_config(config["location_suffix_map_file"])
    exchange_suffix_map = final_support_core.load_json_config(config["exchange_suffix_map_file"])
    unsupported_exchanges = final_support_core.load_json_config(config["unsupported_exchanges_file"], is_list=True)
    fd_audit = fd_universe_core.audit_financedatabase_universe(
        existing_df=base_df,
        config=config,
        logger_obj=logger,
        location_suffix_map=location_suffix_map if isinstance(location_suffix_map, dict) else {},
        exchange_suffix_map=exchange_suffix_map if isinstance(exchange_suffix_map, dict) else {},
        unsupported_exchanges=unsupported_exchanges if isinstance(unsupported_exchanges, list) else [],
        normalize_sector_name=app_support_core.normalize_sector_name,
    )
    after_df = _prepare_universe(compare_selected, etf_options, config, logger)

    reports_dir = project_root / "reports" / "fd_audit"
    reports_dir.mkdir(parents=True, exist_ok=True)

    accepted_path = reports_dir / "fd_added_candidates.csv"
    rejected_path = reports_dir / "fd_rejected_candidates.csv"
    summary_path = reports_dir / "fd_universe_audit_summary.json"

    accepted_df = fd_audit["accepted_df"]
    rejected_df = fd_audit["rejected_df"]
    accepted_df.to_csv(accepted_path, sep=";", index=False, encoding="utf-8-sig")
    rejected_df.to_csv(rejected_path, sep=";", index=False, encoding="utf-8-sig")

    reason_counts = (
        rejected_df["rejection_reasons"].value_counts(dropna=False).to_dict()
        if not rejected_df.empty and "rejection_reasons" in rejected_df.columns
        else {}
    )
    summary = {
        "base_selection": base_selected,
        "comparison_selection": compare_selected,
        "base_universe_size": int(len(base_df)),
        "after_universe_size": int(len(after_df)),
        "actual_delta": int(len(after_df) - len(base_df)),
        "fd_raw_size": int(fd_audit["fd_raw_size"]),
        "fd_added_size": int(fd_audit["accepted_size"]),
        "fd_rejected_size": int(len(rejected_df)),
        "top_rejection_reasons": reason_counts,
        "accepted_output": str(accepted_path),
        "rejected_output": str(rejected_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\nFinanceDatabase Universe Audit")
    print(f"Basis-Auswahl:      {', '.join(base_selected) if base_selected else '(leer)'}")
    print(f"Mit FDB:            {', '.join(compare_selected)}")
    print(f"Basis Universum:    {len(base_df)}")
    print(f"Mit FDB Universum:  {len(after_df)}")
    print(f"Delta real:         {len(after_df) - len(base_df)}")
    print(f"FD roh geladen:     {fd_audit['fd_raw_size']}")
    print(f"FD hinzugefuegt:    {fd_audit['accepted_size']}")
    print(f"FD abgelehnt:       {len(rejected_df)}")
    if reason_counts:
        print("\nTop Ablehnungsgruende:")
        for reason, count in list(reason_counts.items())[:10]:
            print(f" - {reason}: {count}")
    print(f"\nCSV hinzugefuegt:   {accepted_path}")
    print(f"CSV abgelehnt:      {rejected_path}")
    print(f"Summary JSON:       {summary_path}")


if __name__ == "__main__":
    main()
