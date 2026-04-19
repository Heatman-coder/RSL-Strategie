#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fd_candidate_discovery.py

Read-only Discovery-Test:
Kann FinanceDatabase (FD) deinem Projekt neue nutzbare Aktien liefern?

Ziele:
- bestehendes Projektuniversum laden
- FD-Equities laden
- bekannte Werte via Ticker / Normalized Ticker / ISIN herausfiltern
- neue FD-Kandidaten identifizieren
- optional Stichprobe via yfinance testen
- Reports als CSV schreiben

WICHTIG:
- keine Änderungen an Produktivcode
- keine Projektdateien ändern
- nur lesen + CSV schreiben
"""

from __future__ import annotations

import os
import sys
import json
import time
import random
import traceback
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

try:
    import financedatabase as fd
except ImportError:
    print("Fehler: financedatabase ist nicht installiert.")
    print("Installiere es mit: pip install financedatabase")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    yf = None


# ============================================================
# KONFIGURATION
# ============================================================

DEFAULT_SAMPLE_SIZE = 300
DEFAULT_HISTORY_PERIOD = "3y"
DEFAULT_SLEEP_SECONDS = 0.2

# Falls Skript in /tools liegt und Projektroot eine Ebene höher ist.
DEFAULT_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)

CACHE_FILE_REL = "ticker_info_cache.json"
SNAPSHOT_FILE_REL = os.path.join("reports", "last_analysis_snapshot.json")
OUTPUT_DIR_REL = os.path.join("reports", "fd_discovery")

# Snapshot-Feldmapping defensiv anpassbar
SNAP_FIELDS = {
    "root_key": "stock_results",
    "ticker": "yahoo_symbol",
    "isin": "isin",
    "sector": "sector",
    "country": "land",
    "name": "name",
}

# Bekannte Yahoo-Suffixe / Börsensuffixe zur vorsichtigen Normalisierung
KNOWN_SUFFIXES = [
    ".DE", ".F", ".SG", ".DU", ".BE", ".HM", ".MU",
    ".T", ".L", ".HK", ".TO", ".AX", ".OL", ".ST", ".CO",
    ".KS", ".KQ", ".TW", ".TWO", ".SS", ".SZ", ".SN", ".SW",
    ".PA", ".AS", ".BR", ".MI", ".MC", ".HE", ".WA", ".PR",
    ".VI", ".LS", ".IR", ".NS", ".BO", ".SI", ".JK", ".KL",
    ".BK", ".MX", ".SA", ".JO", ".QA", ".BA", ".BC", ".BD",
]

# Minimale Anforderungen für "RSL-geeignet"
MIN_HISTORY_ROWS_FOR_RSL = 200

# FD-Spalten, die wir gern hätten, aber defensiv behandeln
PREFERRED_FD_COLUMNS = [
    "name",
    "summary",
    "currency",
    "country",
    "sector",
    "industry",
    "market",
    "exchange",
    "exchange_name",
    "mic_code",
    "isin",
    "figi",
    "composite_figi",
    "share_class_figi",
]


# ============================================================
# HILFSFUNKTIONEN
# ============================================================

def safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def normalize_isin(value: Any) -> str:
    s = safe_str(value).upper().replace(" ", "")
    return s


def normalize_ticker(value: Any) -> str:
    """
    Vorsichtige Normalisierung:
    - uppercase
    - bekannte Börsensuffixe entfernen
    - sonst Original belassen
    """
    s = safe_str(value).upper()
    if not s:
        return ""
    for suffix in sorted(KNOWN_SUFFIXES, key=len, reverse=True):
        if s.endswith(suffix):
            return s[:-len(suffix)]
    return s


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def first_existing_column(columns: Iterable[str], candidates: List[str]) -> Optional[str]:
    colset = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in colset:
            return colset[candidate.lower()]
    return None


def choose_fd_column_map(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Ordnet sinnvolle semantische Felder auf tatsächlich vorhandene FD-Spalten.
    """
    cols = list(df.columns)

    return {
        "ticker": None,  # kommt aus Index
        "name": first_existing_column(cols, ["name"]),
        "summary": first_existing_column(cols, ["summary"]),
        "currency": first_existing_column(cols, ["currency"]),
        "country": first_existing_column(cols, ["country"]),
        "sector": first_existing_column(cols, ["sector"]),
        "industry": first_existing_column(cols, ["industry"]),
        "market": first_existing_column(cols, ["market"]),
        "exchange": first_existing_column(cols, ["exchange", "exchange_name"]),
        "isin": first_existing_column(cols, ["isin"]),
        "figi": first_existing_column(cols, ["figi", "composite_figi", "share_class_figi"]),
    }


def get_row_value(row: pd.Series, colname: Optional[str]) -> str:
    if not colname:
        return ""
    if colname not in row.index:
        return ""
    return safe_str(row[colname])


def read_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# PROJEKTUNIVERSUM LADEN
# ============================================================

def load_project_universe(project_root: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cache_file = os.path.join(project_root, CACHE_FILE_REL)
    snapshot_file = os.path.join(project_root, SNAPSHOT_FILE_REL)

    rows: List[Dict[str, Any]] = []
    stats = {
        "cache_found": os.path.exists(cache_file),
        "snapshot_found": os.path.exists(snapshot_file),
        "cache_rows": 0,
        "snapshot_rows": 0,
    }

    # 1) Cache
    if os.path.exists(cache_file):
        data = read_json_file(cache_file)
        if isinstance(data, dict):
            for ticker, info in data.items():
                if not isinstance(info, dict):
                    info = {}
                rows.append({
                    "source": "cache",
                    "ticker": safe_str(ticker),
                    "ticker_norm": normalize_ticker(ticker),
                    "isin": normalize_isin(info.get("isin", "")),
                    "name": safe_str(info.get("name", "")),
                    "sector": safe_str(info.get("sector", "")),
                    "country": safe_str(info.get("country", "")),
                })
                stats["cache_rows"] += 1

    # 2) Snapshot
    if os.path.exists(snapshot_file):
        data = read_json_file(snapshot_file)
        snap_list = data.get(SNAP_FIELDS["root_key"], [])
        if isinstance(snap_list, list):
            for item in snap_list:
                if not isinstance(item, dict):
                    continue
                ticker = safe_str(item.get(SNAP_FIELDS["ticker"], ""))
                if not ticker:
                    continue
                rows.append({
                    "source": "snapshot",
                    "ticker": ticker,
                    "ticker_norm": normalize_ticker(ticker),
                    "isin": normalize_isin(item.get(SNAP_FIELDS["isin"], "")),
                    "name": safe_str(item.get(SNAP_FIELDS["name"], "")),
                    "sector": safe_str(item.get(SNAP_FIELDS["sector"], "")),
                    "country": safe_str(item.get(SNAP_FIELDS["country"], "")),
                })
                stats["snapshot_rows"] += 1

    if not rows:
        return pd.DataFrame(), stats

    df = pd.DataFrame(rows)

    # Duplikate entfernen, aber Infos soweit möglich erhalten
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["ticker_norm"] = df["ticker_norm"].astype(str).str.strip()
    df["isin"] = df["isin"].astype(str).str.strip()

    # Für später hilfreiche Sätze
    return df, stats


def build_project_sets(df: pd.DataFrame) -> Dict[str, Set[str]]:
    tickers = {
        safe_str(x).upper()
        for x in df["ticker"].dropna().tolist()
        if safe_str(x)
    }
    tickers_norm = {
        safe_str(x).upper()
        for x in df["ticker_norm"].dropna().tolist()
        if safe_str(x)
    }
    isins = {
        normalize_isin(x)
        for x in df["isin"].dropna().tolist()
        if normalize_isin(x)
    }

    return {
        "ticker": tickers,
        "ticker_norm": tickers_norm,
        "isin": isins,
    }


# ============================================================
# FD LADEN UND AUFBEREITEN
# ============================================================

def load_fd_equities() -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    print("Lade FinanceDatabase Equities ...")
    equities = fd.Equities()
    df = equities.select()
    if df is None or df.empty:
        raise RuntimeError("FinanceDatabase Equities ist leer oder konnte nicht geladen werden.")

    # Index als Ticker übernehmen
    df = df.copy()
    df.index = df.index.map(lambda x: safe_str(x).upper())
    df.reset_index(inplace=True)
    df.rename(columns={"index": "fd_ticker"}, inplace=True)

    colmap = choose_fd_column_map(df)
    return df, colmap


def build_fd_candidates(fd_df: pd.DataFrame, colmap: Dict[str, Optional[str]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for _, row in fd_df.iterrows():
        ticker = safe_str(row.get("fd_ticker", "")).upper()
        if not ticker:
            continue

        entry = {
            "fd_ticker": ticker,
            "fd_ticker_norm": normalize_ticker(ticker),
            "fd_name": get_row_value(row, colmap["name"]),
            "fd_summary": get_row_value(row, colmap["summary"]),
            "fd_currency": get_row_value(row, colmap["currency"]),
            "fd_country": get_row_value(row, colmap["country"]),
            "fd_sector": get_row_value(row, colmap["sector"]),
            "fd_industry": get_row_value(row, colmap["industry"]),
            "fd_market": get_row_value(row, colmap["market"]),
            "fd_exchange": get_row_value(row, colmap["exchange"]),
            "fd_isin": normalize_isin(get_row_value(row, colmap["isin"])),
            "fd_figi": get_row_value(row, colmap["figi"]),
        }

        rows.append(entry)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["has_isin"] = out["fd_isin"].astype(str).str.len() > 0
    out["has_country"] = out["fd_country"].astype(str).str.len() > 0
    out["has_sector"] = out["fd_sector"].astype(str).str.len() > 0
    out["has_industry"] = out["fd_industry"].astype(str).str.len() > 0
    out["has_name"] = out["fd_name"].astype(str).str.len() > 0

    return out


# ============================================================
# MATCHING GEGEN BESTEHENDES UNIVERSUM
# ============================================================

def classify_fd_candidates(
    fd_candidates: pd.DataFrame,
    project_sets: Dict[str, Set[str]]
) -> pd.DataFrame:
    df = fd_candidates.copy()

    df["known_by_ticker"] = df["fd_ticker"].isin(project_sets["ticker"])
    df["known_by_ticker_norm"] = df["fd_ticker_norm"].isin(project_sets["ticker_norm"])
    df["known_by_isin"] = df["fd_isin"].isin(project_sets["isin"]) & (df["fd_isin"] != "")

    def classify(row: pd.Series) -> str:
        if row["known_by_isin"]:
            return "KNOWN_BY_ISIN"
        if row["known_by_ticker"]:
            return "KNOWN_BY_TICKER"
        if row["known_by_ticker_norm"]:
            return "KNOWN_BY_NORMALIZED_TICKER"
        return "NEW_CANDIDATE"

    df["discovery_status"] = df.apply(classify, axis=1)
    return df


# ============================================================
# YFINANCE TEST
# ============================================================

def yf_test_one_ticker(ticker: str, period: str = DEFAULT_HISTORY_PERIOD) -> Dict[str, Any]:
    result = {
        "yf_ok": False,
        "yf_rows": 0,
        "yf_first_date": "",
        "yf_last_date": "",
        "yf_has_close": False,
        "yf_has_adj_close": False,
        "yf_nonempty_close_rows": 0,
        "rsl_usable": False,
        "yf_error": "",
    }

    if yf is None:
        result["yf_error"] = "yfinance not installed"
        return result

    try:
        hist = yf.download(
            ticker,
            period=period,
            progress=False,
            auto_adjust=False,
            threads=False,
        )

        if hist is None or hist.empty:
            result["yf_error"] = "empty_history"
            return result

        result["yf_ok"] = True
        result["yf_rows"] = len(hist)
        result["yf_first_date"] = str(hist.index.min().date()) if len(hist.index) else ""
        result["yf_last_date"] = str(hist.index.max().date()) if len(hist.index) else ""

        if "Close" in hist.columns:
            result["yf_has_close"] = True
            result["yf_nonempty_close_rows"] = int(hist["Close"].dropna().shape[0])

        if "Adj Close" in hist.columns:
            result["yf_has_adj_close"] = True

        if result["yf_nonempty_close_rows"] >= MIN_HISTORY_ROWS_FOR_RSL:
            result["rsl_usable"] = True

        return result

    except Exception as e:
        result["yf_error"] = f"{type(e).__name__}: {e}"
        return result


def run_yf_sample_test(
    new_candidates: pd.DataFrame,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    period: str = DEFAULT_HISTORY_PERIOD,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
    random_seed: int = 42,
) -> pd.DataFrame:
    if new_candidates.empty:
        return pd.DataFrame()

    sample_size = min(sample_size, len(new_candidates))
    sample_df = new_candidates.sample(n=sample_size, random_state=random_seed).copy()

    results: List[Dict[str, Any]] = []
    print(f"Starte yfinance-Stichprobentest mit {sample_size} Kandidaten ...")

    for idx, (_, row) in enumerate(sample_df.iterrows(), start=1):
        ticker = safe_str(row["fd_ticker"]).upper()
        print(f"[{idx}/{sample_size}] Teste {ticker}")
        yf_result = yf_test_one_ticker(ticker, period=period)

        out_row = row.to_dict()
        out_row.update(yf_result)
        results.append(out_row)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return pd.DataFrame(results)


# ============================================================
# REPORTING
# ============================================================

def print_project_summary(project_df: pd.DataFrame, stats: Dict[str, Any]) -> None:
    print("\n--- Projektuniversum ---")
    print(f"Cache gefunden:    {'JA' if stats['cache_found'] else 'NEIN'}")
    print(f"Snapshot gefunden: {'JA' if stats['snapshot_found'] else 'NEIN'}")
    print(f"Cache-Zeilen:      {stats['cache_rows']}")
    print(f"Snapshot-Zeilen:   {stats['snapshot_rows']}")
    print(f"Gesamt Rohzeilen:  {len(project_df)}")

    if not project_df.empty:
        unique_ticker = project_df["ticker"].astype(str).str.upper().nunique()
        unique_ticker_norm = project_df["ticker_norm"].astype(str).str.upper().nunique()
        unique_isin = project_df[project_df["isin"].astype(str).str.len() > 0]["isin"].nunique()

        print(f"Unique Ticker:     {unique_ticker}")
        print(f"Unique NormTicker: {unique_ticker_norm}")
        print(f"Unique ISIN:       {unique_isin}")


def print_fd_summary(fd_df: pd.DataFrame, colmap: Dict[str, Optional[str]], fd_candidates: pd.DataFrame) -> None:
    print("\n--- FinanceDatabase ---")
    print(f"FD Shape:          {fd_df.shape}")
    print("Erkannte FD-Felder:")
    for k, v in colmap.items():
        print(f"  - {k}: {v}")

    print(f"FD Kandidaten:     {len(fd_candidates)}")
    if len(fd_candidates) > 0:
        print(f"Mit ISIN:          {fd_candidates['has_isin'].mean() * 100:,.1f}%")
        print(f"Mit Country:       {fd_candidates['has_country'].mean() * 100:,.1f}%")
        print(f"Mit Sector:        {fd_candidates['has_sector'].mean() * 100:,.1f}%")
        print(f"Mit Industry:      {fd_candidates['has_industry'].mean() * 100:,.1f}%")
        print(f"Mit Name:          {fd_candidates['has_name'].mean() * 100:,.1f}%")


def print_discovery_summary(classified_df: pd.DataFrame) -> None:
    print("\n--- Discovery-Ergebnis ---")
    total = len(classified_df)
    if total == 0:
        print("Keine FD-Kandidaten vorhanden.")
        return

    counts = classified_df["discovery_status"].value_counts(dropna=False).to_dict()

    for key in [
        "KNOWN_BY_ISIN",
        "KNOWN_BY_TICKER",
        "KNOWN_BY_NORMALIZED_TICKER",
        "NEW_CANDIDATE",
    ]:
        value = counts.get(key, 0)
        pct = (value / total) * 100 if total else 0
        print(f"{key:27s}: {value:8d} ({pct:5.1f}%)")

    new_df = classified_df[classified_df["discovery_status"] == "NEW_CANDIDATE"]
    if not new_df.empty:
        print("\nNeue Kandidaten - Metadatenqualität:")
        print(f"Mit ISIN:                    {new_df['has_isin'].mean() * 100:,.1f}%")
        print(f"Mit Country:                 {new_df['has_country'].mean() * 100:,.1f}%")
        print(f"Mit Sector:                  {new_df['has_sector'].mean() * 100:,.1f}%")
        print(f"Mit Industry:                {new_df['has_industry'].mean() * 100:,.1f}%")
        print(f"Mit Name:                    {new_df['has_name'].mean() * 100:,.1f}%")

        top_countries = new_df["fd_country"].replace("", pd.NA).dropna().value_counts().head(10)
        if not top_countries.empty:
            print("\nTop Länder neuer Kandidaten:")
            for name, count in top_countries.items():
                print(f"  - {name}: {count}")

        top_sectors = new_df["fd_sector"].replace("", pd.NA).dropna().value_counts().head(10)
        if not top_sectors.empty:
            print("\nTop Sektoren neuer Kandidaten:")
            for name, count in top_sectors.items():
                print(f"  - {name}: {count}")


def print_yf_summary(yf_df: pd.DataFrame) -> None:
    print("\n--- yfinance Stichprobe ---")
    if yf_df.empty:
        print("Kein yfinance-Test durchgeführt oder keine Kandidaten vorhanden.")
        return

    total = len(yf_df)
    ok = int(yf_df["yf_ok"].sum())
    rsl = int(yf_df["rsl_usable"].sum())
    has_isin = int(yf_df["has_isin"].sum())

    print(f"Getestete Kandidaten:        {total}")
    print(f"Yahoo-Historie vorhanden:    {ok} ({ok / total * 100:,.1f}%)")
    print(f"RSL-tauglich:                {rsl} ({rsl / total * 100:,.1f}%)")
    print(f"Mit ISIN:                    {has_isin} ({has_isin / total * 100:,.1f}%)")

    if ok > 0:
        ok_df = yf_df[yf_df["yf_ok"]]
        median_rows = int(ok_df["yf_rows"].median()) if not ok_df["yf_rows"].dropna().empty else 0
        print(f"Median Historienzeilen:      {median_rows}")

    top_errors = (
        yf_df["yf_error"]
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .head(10)
    )
    if not top_errors.empty:
        print("\nHäufigste yfinance-Fehler:")
        for err, count in top_errors.items():
            print(f"  - {err}: {count}")


# ============================================================
# CSV EXPORT
# ============================================================

def write_csv(df: pd.DataFrame, path: str) -> None:
    ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=False, encoding="utf-8-sig")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    project_root = DEFAULT_PROJECT_ROOT

    if len(sys.argv) > 1:
        project_root = os.path.abspath(sys.argv[1])

    output_dir = os.path.join(project_root, OUTPUT_DIR_REL)
    ensure_dir(output_dir)

    print("=" * 70)
    print("FD CANDIDATE DISCOVERY")
    print("=" * 70)
    print(f"Projekt-Root: {project_root}")
    print(f"Output-Ordner: {output_dir}")

    try:
        # 1) Projektuniversum laden
        project_df, project_stats = load_project_universe(project_root)
        print_project_summary(project_df, project_stats)

        if project_df.empty:
            print("\nFEHLER:")
            print("Keine Projektdaten gefunden. Bitte Root-Pfad / JSON-Strukturen prüfen.")
            sys.exit(1)

        project_sets = build_project_sets(project_df)

        # 2) FD laden
        fd_df, colmap = load_fd_equities()
        fd_candidates = build_fd_candidates(fd_df, colmap)
        print_fd_summary(fd_df, colmap, fd_candidates)

        if fd_candidates.empty:
            print("\nFEHLER: Keine FD-Kandidaten erzeugt.")
            sys.exit(1)

        # 3) Discovery-Klassifikation
        classified_df = classify_fd_candidates(fd_candidates, project_sets)
        print_discovery_summary(classified_df)

        # 4) CSVs schreiben
        all_candidates_csv = os.path.join(output_dir, "fd_all_candidates.csv")
        new_candidates_csv = os.path.join(output_dir, "fd_new_candidates.csv")
        known_candidates_csv = os.path.join(output_dir, "fd_known_candidates.csv")

        write_csv(classified_df, all_candidates_csv)
        write_csv(
            classified_df[classified_df["discovery_status"] == "NEW_CANDIDATE"].copy(),
            new_candidates_csv,
        )
        write_csv(
            classified_df[classified_df["discovery_status"] != "NEW_CANDIDATE"].copy(),
            known_candidates_csv,
        )

        print("\nCSV-Ausgaben:")
        print(f"- Alle Kandidaten:  {all_candidates_csv}")
        print(f"- Neue Kandidaten:  {new_candidates_csv}")
        print(f"- Bekannte Werte:   {known_candidates_csv}")

        # 5) Optional yfinance Stichprobe
        if yf is None:
            print("\nHinweis: yfinance ist nicht installiert, daher kein Yahoo-Test.")
            print("Installiere optional mit: pip install yfinance")
            return

        new_candidates = classified_df[classified_df["discovery_status"] == "NEW_CANDIDATE"].copy()

        if new_candidates.empty:
            print("\nKeine neuen Kandidaten gefunden. yfinance-Test wird übersprungen.")
            return

        yf_sample_df = run_yf_sample_test(
            new_candidates=new_candidates,
            sample_size=DEFAULT_SAMPLE_SIZE,
            period=DEFAULT_HISTORY_PERIOD,
            sleep_seconds=DEFAULT_SLEEP_SECONDS,
            random_seed=42,
        )

        yf_sample_csv = os.path.join(output_dir, "fd_new_candidates_yf_sample.csv")
        write_csv(yf_sample_df, yf_sample_csv)

        print_yf_summary(yf_sample_df)
        print(f"\nYFinance-Sample-CSV: {yf_sample_csv}")

        print("\nFERTIG.")

    except Exception as e:
        print("\nUNERWARTETER FEHLER:")
        print(f"{type(e).__name__}: {e}")
        print("\nTraceback:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
