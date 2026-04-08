import datetime
from typing import Any, Callable, Dict, List

import numpy as np
import pandas as pd


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def build_quality_report(
    stock_results: List[Any],
    universe_candidates: int,
    failed_records_count: int,
    young_records_count: int,
    dropped_critical_count: int,
    portfolio_symbols: List[str],
) -> Dict[str, Any]:
    analyzed_count = len(stock_results)
    candidate_count = max(0, int(universe_candidates))
    symbol_keys = [str(s.yahoo_symbol).strip().upper() for s in stock_results if str(s.yahoo_symbol).strip()]
    duplicate_symbols = max(0, len(symbol_keys) - len(set(symbol_keys)))

    invalid_numeric_count = 0
    stale_warn_count = 0
    gap_warn_count = 0
    liquidity_warn_count = 0
    low_trust_count = 0
    for s in stock_results:
        if str(s.flag_stale).upper() == "WARN":
            stale_warn_count += 1
        if str(s.flag_gap).upper() != "OK":
            gap_warn_count += 1
        if str(s.flag_liquidity).upper() != "OK":
            liquidity_warn_count += 1
        if int(s.trust_score) < 2:
            low_trust_count += 1

        try:
            kurs_ok = np.isfinite(float(s.kurs)) and float(s.kurs) > 0.0
            sma_ok = np.isfinite(float(s.sma)) and float(s.sma) > 0.0
            rsl_ok = np.isfinite(float(s.rsl)) and float(s.rsl) > 0.0
            rank_ok = int(s.rsl_rang) > 0
            if not (kurs_ok and sma_ok and rsl_ok and rank_ok):
                invalid_numeric_count += 1
        except Exception:
            invalid_numeric_count += 1

    analyzed_symbol_set = set(symbol_keys)
    portfolio_set = {str(t).strip().upper() for t in portfolio_symbols if str(t).strip()}
    portfolio_found_count = len(analyzed_symbol_set & portfolio_set)

    metrics = {
        "analyzed_count": analyzed_count,
        "candidate_count": candidate_count,
        "failed_records_count": int(failed_records_count),
        "young_records_count": int(young_records_count),
        "dropped_critical_count": int(dropped_critical_count),
        "stale_warn_count": stale_warn_count,
        "gap_warn_count": gap_warn_count,
        "liquidity_warn_count": liquidity_warn_count,
        "low_trust_count": low_trust_count,
        "invalid_numeric_count": invalid_numeric_count,
        "duplicate_symbols": duplicate_symbols,
        "portfolio_total_count": len(portfolio_set),
        "portfolio_found_count": portfolio_found_count,
    }

    ratios = {
        "coverage_ratio": safe_ratio(analyzed_count, candidate_count),
        "failed_ratio": safe_ratio(int(failed_records_count), candidate_count),
        "young_ratio": safe_ratio(int(young_records_count), candidate_count),
        "critical_drop_ratio": safe_ratio(int(dropped_critical_count), candidate_count),
        "stale_warn_ratio": safe_ratio(stale_warn_count, analyzed_count),
        "gap_warn_ratio": safe_ratio(gap_warn_count, analyzed_count),
        "liquidity_warn_ratio": safe_ratio(liquidity_warn_count, analyzed_count),
        "low_trust_ratio": safe_ratio(low_trust_count, analyzed_count),
        "portfolio_coverage_ratio": safe_ratio(portfolio_found_count, len(portfolio_set)),
    }

    return {
        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "metrics": metrics,
        "ratios": ratios,
    }


def evaluate_strict_quality_failures(report: Dict[str, Any], cfg: Dict[str, Any]) -> List[str]:
    metrics = report.get("metrics", {})
    ratios = report.get("ratios", {})
    failures: List[str] = []

    def _metric(name: str) -> int:
        try:
            return int(metrics.get(name, 0))
        except Exception:
            return 0

    def _ratio(name: str) -> float:
        try:
            return float(ratios.get(name, 0.0))
        except Exception:
            return 0.0

    if _metric("analyzed_count") < int(cfg.get("strict_min_analyzed_stocks", 0)):
        failures.append(
            f"Zu wenige analysierte Werte: {_metric('analyzed_count')} < {int(cfg.get('strict_min_analyzed_stocks', 0))}"
        )
    if _ratio("coverage_ratio") < float(cfg.get("strict_min_coverage_ratio", 0.0)):
        failures.append(
            f"Coverage zu niedrig: {_ratio('coverage_ratio'):.2%} < {float(cfg.get('strict_min_coverage_ratio', 0.0)):.2%}"
        )
    if _ratio("failed_ratio") > float(cfg.get("strict_max_failed_ratio", 1.0)):
        failures.append(
            f"Failed-Ratio zu hoch: {_ratio('failed_ratio'):.2%} > {float(cfg.get('strict_max_failed_ratio', 1.0)):.2%}"
        )
    if _ratio("young_ratio") > float(cfg.get("strict_max_young_ratio", 1.0)):
        failures.append(
            f"Young-Ratio zu hoch: {_ratio('young_ratio'):.2%} > {float(cfg.get('strict_max_young_ratio', 1.0)):.2%}"
        )
    if _ratio("critical_drop_ratio") > float(cfg.get("strict_max_critical_drop_ratio", 1.0)):
        failures.append(
            f"Critical-Drop-Ratio zu hoch: {_ratio('critical_drop_ratio'):.2%} > {float(cfg.get('strict_max_critical_drop_ratio', 1.0)):.2%}"
        )
    if _ratio("stale_warn_ratio") > float(cfg.get("strict_max_stale_warn_ratio", 1.0)):
        failures.append(
            f"Stale-WARN-Ratio zu hoch: {_ratio('stale_warn_ratio'):.2%} > {float(cfg.get('strict_max_stale_warn_ratio', 1.0)):.2%}"
        )
    if _ratio("gap_warn_ratio") > float(cfg.get("strict_max_gap_warn_ratio", 1.0)):
        failures.append(
            f"Gap-WARN-Ratio zu hoch: {_ratio('gap_warn_ratio'):.2%} > {float(cfg.get('strict_max_gap_warn_ratio', 1.0)):.2%}"
        )
    if _ratio("liquidity_warn_ratio") > float(cfg.get("strict_max_liquidity_warn_ratio", 1.0)):
        failures.append(
            f"Liquidity-WARN-Ratio zu hoch: {_ratio('liquidity_warn_ratio'):.2%} > {float(cfg.get('strict_max_liquidity_warn_ratio', 1.0)):.2%}"
        )
    if _ratio("low_trust_ratio") > float(cfg.get("strict_max_low_trust_ratio", 1.0)):
        failures.append(
            f"Low-Trust-Ratio zu hoch: {_ratio('low_trust_ratio'):.2%} > {float(cfg.get('strict_max_low_trust_ratio', 1.0)):.2%}"
        )
    if _metric("portfolio_total_count") > 0 and _ratio("portfolio_coverage_ratio") < float(
        cfg.get("strict_min_portfolio_coverage_ratio", 0.0)
    ):
        failures.append(
            f"Portfolio-Coverage zu niedrig: {_ratio('portfolio_coverage_ratio'):.2%} < {float(cfg.get('strict_min_portfolio_coverage_ratio', 0.0)):.2%}"
        )
    if _metric("invalid_numeric_count") > int(cfg.get("strict_max_invalid_numeric_count", 0)):
        failures.append(
            f"Ungueltige numerische Werte: {_metric('invalid_numeric_count')} > {int(cfg.get('strict_max_invalid_numeric_count', 0))}"
        )
    if _metric("duplicate_symbols") > int(cfg.get("strict_max_duplicate_symbols", 0)):
        failures.append(
            f"Doppelte Yahoo-Symbole: {_metric('duplicate_symbols')} > {int(cfg.get('strict_max_duplicate_symbols', 0))}"
        )

    return failures


def run_quality_gate(
    stock_results: List[Any],
    universe_candidates: int,
    dropped_critical_count: int,
    data_mgr: Any,
    portfolio_symbols: List[str],
    config: Dict[str, Any],
    save_json_config_func: Callable[[str, Any], None],
    logger: Any,
    report_file: str,
    print_fn: Callable[[str], None] = print,
) -> Dict[str, Any]:
    failed_records = data_mgr.get_failed_records() if data_mgr else []
    young_records = data_mgr.get_young_records() if data_mgr else []

    report = build_quality_report(
        stock_results=stock_results,
        universe_candidates=universe_candidates,
        failed_records_count=len(failed_records),
        young_records_count=len(young_records),
        dropped_critical_count=dropped_critical_count,
        portfolio_symbols=portfolio_symbols,
    )

    failures = evaluate_strict_quality_failures(report, config)
    strict_enabled = bool(config.get("strict_mode", False))
    report["strict_mode"] = strict_enabled
    report["strict_status"] = "FAIL" if (strict_enabled and failures) else "PASS"
    report["strict_failures"] = failures
    save_json_config_func(report_file, report)

    metrics = report.get("metrics", {})
    ratios = report.get("ratios", {})
    logger.info(
        "Quality Report: analyzed=%s candidates=%s coverage=%.2f%% failed=%.2f%% critical_drop=%.2f%%",
        metrics.get("analyzed_count", 0),
        metrics.get("candidate_count", 0),
        100.0 * float(ratios.get("coverage_ratio", 0.0)),
        100.0 * float(ratios.get("failed_ratio", 0.0)),
        100.0 * float(ratios.get("critical_drop_ratio", 0.0)),
    )

    status_text = "FAIL" if (strict_enabled and failures) else ("PASS" if strict_enabled else "OFF")
    print_fn("\n" + "=" * 90)
    print_fn(f" [STRICT] QUALITY GATE: {status_text}")
    print_fn("=" * 90)
    print_fn(
        " analyzed={a} | candidates={c} | coverage={cov:.1f}% | failed={fail:.1f}% | critical_drop={crit:.1f}%".format(
            a=int(metrics.get("analyzed_count", 0)),
            c=int(metrics.get("candidate_count", 0)),
            cov=100.0 * float(ratios.get("coverage_ratio", 0.0)),
            fail=100.0 * float(ratios.get("failed_ratio", 0.0)),
            crit=100.0 * float(ratios.get("critical_drop_ratio", 0.0)),
        )
    )
    print_fn(
        " stale_warn={stale:.1f}% | gap_warn={gap:.1f}% | liquidity_warn={liq:.1f}% | low_trust={trust:.1f}%".format(
            stale=100.0 * float(ratios.get("stale_warn_ratio", 0.0)),
            gap=100.0 * float(ratios.get("gap_warn_ratio", 0.0)),
            liq=100.0 * float(ratios.get("liquidity_warn_ratio", 0.0)),
            trust=100.0 * float(ratios.get("low_trust_ratio", 0.0)),
        )
    )
    if strict_enabled:
        print_fn(
            " limits: analyzed>={min_n} | coverage>={cov:.1f}% | failed<={fail:.1f}% | young<={young:.1f}% | critical_drop<={crit:.1f}%".format(
                min_n=int(config.get("strict_min_analyzed_stocks", 0)),
                cov=100.0 * float(config.get("strict_min_coverage_ratio", 0.0)),
                fail=100.0 * float(config.get("strict_max_failed_ratio", 1.0)),
                young=100.0 * float(config.get("strict_max_young_ratio", 1.0)),
                crit=100.0 * float(config.get("strict_max_critical_drop_ratio", 1.0)),
            )
        )
        print_fn(
            "         stale<={stale:.1f}% | gap<={gap:.1f}% | liquidity<={liq:.1f}% | low_trust<={trust:.1f}% | portfolio_cov>={pcov:.1f}%".format(
                stale=100.0 * float(config.get("strict_max_stale_warn_ratio", 1.0)),
                gap=100.0 * float(config.get("strict_max_gap_warn_ratio", 1.0)),
                liq=100.0 * float(config.get("strict_max_liquidity_warn_ratio", 1.0)),
                trust=100.0 * float(config.get("strict_max_low_trust_ratio", 1.0)),
                pcov=100.0 * float(config.get("strict_min_portfolio_coverage_ratio", 0.0)),
            )
        )
        print_fn(
            "         invalid_numeric<={inv} | duplicate_symbols<={dupe}".format(
                inv=int(config.get("strict_max_invalid_numeric_count", 0)),
                dupe=int(config.get("strict_max_duplicate_symbols", 0)),
            )
        )
    if strict_enabled and failures:
        print_fn(" Top-3 Gruende:")
        for idx, reason in enumerate(failures[:3], start=1):
            print_fn(f"  {idx}. {reason}")
        remaining = len(failures) - 3
        if remaining > 0:
            print_fn(f"  +{remaining} weitere")
    print_fn("=" * 90)

    if strict_enabled and failures:
        for failure in failures:
            logger.error(f"[STRICT] {failure}")
        raise RuntimeError(
            f"Strict-Mode: Qualitaetsgrenzen verletzt ({len(failures)}). "
            f"Siehe {report_file}."
        )
    if strict_enabled:
        logger.info("[STRICT] Alle Qualitaetschecks bestanden.")
    return report


def summarize_integrity_flags(df: pd.DataFrame) -> dict:
    """Erstellt eine Statistik über die neuen Integritäts-Flags im DataFrame."""
    return {
        "total_rows": int(len(df)),
        "hard_fail_count": int((df["excluded_from_ranking"] == True).sum()) if "excluded_from_ranking" in df.columns else 0,
        "review_count": int(df["ranking_integrity_status"].astype(str).str.contains("review", case=False).sum()) if "ranking_integrity_status" in df.columns else 0,
        "warning_count": int(
            df["warning_reasons"].fillna("").astype(str).str.len().gt(0).sum()
        ) if "warning_reasons" in df.columns else 0,
    }


def quality_gate_status(summary: dict) -> str:
    """Bewertet den Status basierend auf der Integritäts-Zusammenfassung."""
    if summary["total_rows"] == 0:
        return "FAIL"

    hard_fail_ratio = summary["hard_fail_count"] / summary["total_rows"]

    if hard_fail_ratio > 0.25:
        return "FAIL"
    if hard_fail_ratio > 0.10 or summary["review_count"] > 0:
        return "REVIEW"

    return "PASS"
