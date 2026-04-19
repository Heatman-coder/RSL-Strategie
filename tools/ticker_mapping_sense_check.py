import collections
import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


def suffix_of(symbol: str) -> str:
    symbol = (symbol or "").strip()
    if "." not in symbol:
        return ""
    return "." + symbol.rsplit(".", 1)[1]


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    reports_dir = root / "reports"
    reports_dir.mkdir(exist_ok=True)

    ticker_map = json.loads((root / "ticker_map_v2.json").read_text(encoding="utf-8"))
    manual_fix = json.loads((root / "manual_fix.json").read_text(encoding="utf-8"))
    exchange_map = json.loads((root / "exchange_suffix_map.json").read_text(encoding="utf-8"))

    df = pd.read_pickle(root / "etf_holdings_cache.pkl")
    required_cols = ["Ticker", "Land", "Exchange"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise RuntimeError(f"Missing columns in etf_holdings_cache.pkl: {missing_cols}")

    df = df[required_cols].copy()
    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()
    df = df[(df["Ticker"] != "") & (df["Land"] != "")]
    df["key"] = df["Ticker"] + "_" + df["Land"]

    universe = (
        df.drop_duplicates(subset=["key"], keep="first")[["key", "Ticker", "Land", "Exchange"]]
        .sort_values("key")
        .reset_index(drop=True)
    )
    universe_keys = set(universe["key"].tolist())

    findings = []
    for row in universe.itertuples(index=False):
        key = row.key
        ticker = row.Ticker
        land = row.Land
        exchange = row.Exchange

        mapped = (ticker_map.get(key) or "").strip()
        expected_suffix = (exchange_map.get(exchange) or "").strip()

        if not mapped:
            findings.append(
                {
                    "scope": "universe",
                    "severity": "high_confidence",
                    "issue_code": "UNMAPPED_UNIVERSE_KEY",
                    "key": key,
                    "ticker": ticker,
                    "land": land,
                    "exchange": exchange,
                    "mapped": "",
                    "expected_suffix": expected_suffix,
                    "detail": "Kein Mapping fuer plausiblen Ticker im ETF-Universum.",
                }
            )
            continue

        if expected_suffix:
            actual_suffix = suffix_of(mapped)
            if actual_suffix != expected_suffix:
                findings.append(
                    {
                        "scope": "universe",
                        "severity": "review",
                        "issue_code": "EXCHANGE_SUFFIX_MISMATCH",
                        "key": key,
                        "ticker": ticker,
                        "land": land,
                        "exchange": exchange,
                        "mapped": mapped,
                        "expected_suffix": expected_suffix,
                        "detail": "Mapped-Suffix passt nicht zur Exchange-Suffix-Tabelle.",
                    }
                )

    mapped_to_keys = collections.defaultdict(list)
    for key, mapped in ticker_map.items():
        mapped_symbol = (mapped or "").strip()
        if not mapped_symbol:
            continue
        if key not in universe_keys:
            continue
        mapped_to_keys[mapped_symbol].append(key)

    for mapped_symbol, keys in sorted(mapped_to_keys.items()):
        unique_keys = sorted(set(keys))
        if len(unique_keys) <= 1:
            continue
        findings.append(
            {
                "scope": "universe",
                "severity": "review",
                "issue_code": "MANY_KEYS_TO_ONE_MAPPED",
                "key": " | ".join(unique_keys),
                "ticker": "",
                "land": "",
                "exchange": "",
                "mapped": mapped_symbol,
                "expected_suffix": "",
                "detail": f"{len(unique_keys)} Universe-Keys zeigen auf denselben Yahoo-Ticker.",
            }
        )

    severity_order = {"high_confidence": 0, "review": 1}
    findings.sort(
        key=lambda row: (
            severity_order.get(row["severity"], 9),
            row["issue_code"],
            row["key"],
        )
    )

    fields = [
        "scope",
        "severity",
        "issue_code",
        "key",
        "ticker",
        "land",
        "exchange",
        "mapped",
        "expected_suffix",
        "detail",
    ]

    all_path = reports_dir / "ticker_mapping_sense_check_findings.csv"
    high_conf_path = reports_dir / "ticker_mapping_sense_check_high_confidence.csv"
    review_path = reports_dir / "ticker_mapping_sense_check_review.csv"

    def write_csv(path: Path, rows) -> None:
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    write_csv(all_path, findings)
    write_csv(high_conf_path, [r for r in findings if r["severity"] == "high_confidence"])
    write_csv(review_path, [r for r in findings if r["severity"] == "review"])

    issue_counts = collections.Counter(row["issue_code"] for row in findings)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "universe_unique_candidate_keys": int(universe.shape[0]),
        "mapping_entries": len(ticker_map),
        "manual_fix_entries": len(manual_fix),
        "total_findings": len(findings),
        "high_confidence_findings": sum(1 for row in findings if row["severity"] == "high_confidence"),
        "review_findings": sum(1 for row in findings if row["severity"] == "review"),
        "findings_by_issue": dict(sorted(issue_counts.items(), key=lambda item: (-item[1], item[0]))),
        # Backward-compatible aliases for older tooling/reads.
        "top_issue_counts": dict(sorted(issue_counts.items(), key=lambda item: (-item[1], item[0]))),
        "files": [
            "reports\\ticker_mapping_sense_check_findings.csv",
            "reports\\ticker_mapping_sense_check_high_confidence.csv",
            "reports\\ticker_mapping_sense_check_review.csv",
        ],
        "generated_files": [
            "reports\\ticker_mapping_sense_check_findings.csv",
            "reports\\ticker_mapping_sense_check_high_confidence.csv",
            "reports\\ticker_mapping_sense_check_review.csv",
        ],
    }

    summary_path = reports_dir / "ticker_mapping_sense_check_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
