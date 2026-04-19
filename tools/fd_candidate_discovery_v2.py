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
from typing import Any, Dict, List, Optional, Set, Tuple

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

DEFAULT_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)

CACHE_FILE_REL = "ticker_info_cache.json"
SNAPSHOT_FILE_REL = os.path.join("reports", "last_analysis_snapshot.json")

OUTPUT_DIR_REL = os.path.join("reports", "fd_discovery")

OUTPUT_ALL = "fd_all_candidates.csv"
OUTPUT_NEW = "fd_new_candidates.csv"
OUTPUT_KNOWN = "fd_known_candidates.csv"
OUTPUT_YF_SAMPLE = "fd_new_candidates_yf_sample.csv"
OUTPUT_SUMMARY = "fd_discovery_summary.json"


# ============================================================
# HILFSFUNKTIONEN
# ============================================================

def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_ticker(ticker: str) -> str:
    """
    Einfache Normalisierung für Matching.
    Ziel: pragmatisch, nicht perfekt.
    """
    t = safe_str(ticker).upper()
    if not t:
        return ""

    replacements = [
        (".", ""),
        ("-", ""),
        ("/", ""),
        (" ", ""),
        ("_", ""),
    ]
    for old, new in replacements:
        t = t.replace(old, new)

    return t


def normalize_isin(isin: str) -> str:
    return safe_str(isin).upper().replace(" ", "")


def ensure_output_dir(project_root: str) -> str:
    out_dir = os.path.join(project_root, OUTPUT_DIR_REL)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def read_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_summary_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def looks_like_isin(value: str) -> bool:
    v = normalize_isin(value)
    if len(v) != 12:
        return False
    if not v[:2].isalpha():
        return False
    if not v[2:].isalnum():
        return False
    return True


# ============================================================
# PROJEKTUNIVERSUM LADEN
# ============================================================

def extract_rows_from_cache(cache_data: Any) -> List[Dict[str, Any]]:
    """
    Erwartet typischerweise dict[ticker] = {...}
    Unterstützt defensiv auch Listen.
    """
    rows: List[Dict[str, Any]] = []

    if isinstance(cache_data, dict):
        for k, v in cache_data.items():
            row = {}
            if isinstance(v, dict):
                row.update(v)
            row["_source_key"] = k
            rows.append(row)
        return rows

    if isinstance(cache_data, list):
        for item in cache_data:
            if isinstance(item, dict):
                rows.append(item)
        return rows

    return rows


def extract_rows_from_snapshot(snapshot_data: Any) -> List[Dict[str, Any]]:
    """
    Unterstützt mehrere mögliche Strukturen defensiv.
    """
    rows: List[Dict[str, Any]] = []

    if isinstance(snapshot_data, list):
        for item in snapshot_data:
            if isinstance(item, dict):
                rows.append(item)
        return rows

    if isinstance(snapshot_data, dict):
        for candidate_key in [
            "rows",
            "data",
            "records",
            "tickers",
            "results",
            "snapshot",
        ]:
            val = snapshot_data.get(candidate_key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        rows.append(item)
                if rows:
                    return rows

        for _, val in snapshot_data.items():
            if isinstance(val, list) and val and isinstance(val[0], dict):
                rows.extend(val)
                return rows

    return rows


def choose_project_field(row: Dict[str, Any], candidates: List[str]) -> str:
    for c in candidates:
        if c in row and safe_str(row.get(c)):
            return safe_str(row.get(c))
    return ""


def load_project_universe(project_root: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cache_path = os.path.join(project_root, CACHE_FILE_REL)
    snapshot_path = os.path.join(project_root, SNAPSHOT_FILE_REL)

    cache_exists = os.path.exists(cache_path)
    snapshot_exists = os.path.exists(snapshot_path)

    cache_rows: List[Dict[str, Any]] = []
    snapshot_rows: List[Dict[str, Any]] = []

    if cache_exists:
        try:
            cache_data = read_json_file(cache_path)
            cache_rows = extract_rows_from_cache(cache_data)
        except Exception as e:
            print(f"WARNUNG: Cache konnte nicht gelesen werden: {e}")

    if snapshot_exists:
        try:
            snapshot_data = read_json_file(snapshot_path)
            snapshot_rows = extract_rows_from_snapshot(snapshot_data)
        except Exception as e:
            print(f"WARNUNG: Snapshot konnte nicht gelesen werden: {e}")

    all_rows = []

    for row in cache_rows:
        ticker = choose_project_field(row, ["ticker", "symbol", "_source_key", "yahoo_ticker"])
        isin = choose_project_field(row, ["isin", "ISIN"])
        all_rows.append({
            "source": "cache",
            "ticker": ticker,
            "norm_ticker": normalize_ticker(ticker),
            "isin": normalize_isin(isin),
            "raw": row,
        })

    for row in snapshot_rows:
        ticker = choose_project_field(row, ["ticker", "symbol", "yahoo_ticker", "_source_key"])
        isin = choose_project_field(row, ["isin", "ISIN"])
        all_rows.append({
            "source": "snapshot",
            "ticker": ticker,
            "norm_ticker": normalize_ticker(ticker),
            "isin": normalize_isin(isin),
            "raw": row,
        })

    df = pd.DataFrame(all_rows)

    if df.empty:
        df = pd.DataFrame(columns=["source", "ticker", "norm_ticker", "isin", "raw"])

    unique_tickers = set(t for t in df["ticker"].tolist() if safe_str(t))
    unique_norm_tickers = set(t for t in df["norm_ticker"].tolist() if safe_str(t))
    unique_isins = set(i for i in df["isin"].tolist() if looks_like_isin(i))

    stats = {
        "cache_exists": cache_exists,
        "snapshot_exists": snapshot_exists,
        "cache_rows": len(cache_rows),
        "snapshot_rows": len(snapshot_rows),
        "total_rows": len(df),
        "unique_tickers": len(unique_tickers),
        "unique_norm_tickers": len(unique_norm_tickers),
        "unique_isins": len(unique_isins),
        "ticker_set": unique_tickers,
        "norm_ticker_set": unique_norm_tickers,
        "isin_set": unique_isins,
    }

    return df, stats


# ============================================================
# FINANCEDATABASE LADEN
# ============================================================

def choose_fd_column_map(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols_lower = {c.lower(): c for c in df.columns}

    def pick(options: List[str]) -> Optional[str]:
        for opt in options:
            if opt.lower() in cols_lower:
                return cols_lower[opt.lower()]
        return None

    return {
        "ticker": "fd_ticker" if "fd_ticker" in df.columns else None,
        "name": pick(["name", "company", "company_name"]),
        "summary": pick(["summary", "description", "long_description"]),
        "currency": pick(["currency"]),
        "country": pick(["country"]),
        "sector": pick(["sector"]),
        "industry": pick(["industry"]),
        "market": pick(["market"]),
        "exchange": pick(["exchange"]),
        "isin": pick(["isin"]),
        "figi": pick(["figi"]),
    }


def load_fd_equities() -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    print("Lade FinanceDatabase Equities ...")
    equities = fd.Equities()
    df = equities.select()

    if df is None or df.empty:
        raise RuntimeError("FinanceDatabase Equities ist leer oder konnte nicht geladen werden.")

    df = df.copy()

    original_index_name = df.index.name
    df.index = df.index.map(lambda x: safe_str(x).upper())
    df.reset_index(inplace=True)

    possible_index_cols: List[str] = []
    if original_index_name:
        possible_index_cols.append(original_index_name)

    possible_index_cols.extend([
        "index",
        "symbol",
        "ticker",
        "code",
    ])

    ticker_col = None
    for col in possible_index_cols:
        if col in df.columns:
            ticker_col = col
            break

    if ticker_col is None:
        ticker_col = df.columns[0]
        print(f"WARNUNG: Konnte Ticker-Spalte nicht eindeutig erkennen. Verwende Fallback: {ticker_col}")

    df.rename(columns={ticker_col: "fd_ticker"}, inplace=True)

    if "fd_ticker" not in df.columns:
        raise RuntimeError("FD-Ticker-Spalte konnte nicht erzeugt werden.")

    df["fd_ticker"] = df["fd_ticker"].map(lambda x: safe_str(x).upper())
    nonempty_tickers = (df["fd_ticker"] != "").sum()

    print(f"FD Indexname vorher: {original_index_name}")
    print(f"Verwendete FD-Ticker-Spalte: {ticker_col}")
    print(f"Nichtleere FD-Ticker: {nonempty_tickers} von {len(df)}")
    print(f"Erste FD-Spalten: {df.columns.tolist()[:12]}")

    colmap = choose_fd_column_map(df)
    return df, colmap


# ============================================================
# KANDIDATEN BAUEN
# ============================================================

def build_fd_candidates(
    fd_df: pd.DataFrame,
    colmap: Dict[str, Optional[str]],
    known_tickers: Set[str],
    known_norm_tickers: Set[str],
    known_isins: Set[str],
) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []

    for _, row in fd_df.iterrows():
        ticker = safe_str(row["fd_ticker"]).upper() if "fd_ticker" in row.index else ""
        if not ticker:
            continue

        norm_ticker = normalize_ticker(ticker)

        isin = ""
        if colmap.get("isin"):
            isin = normalize_isin(row.get(colmap["isin"], ""))

        name = safe_str(row.get(colmap["name"], "")) if colmap.get("name") else ""
        summary = safe_str(row.get(colmap["summary"], "")) if colmap.get("summary") else ""
        currency = safe_str(row.get(colmap["currency"], "")) if colmap.get("currency") else ""
        country = safe_str(row.get(colmap["country"], "")) if colmap.get("country") else ""
        sector = safe_str(row.get(colmap["sector"], "")) if colmap.get("sector") else ""
        industry = safe_str(row.get(colmap["industry"], "")) if colmap.get("industry") else ""
        market = safe_str(row.get(colmap["market"], "")) if colmap.get("market") else ""
        exchange = safe_str(row.get(colmap["exchange"], "")) if colmap.get("exchange") else ""
        figi = safe_str(row.get(colmap["figi"], "")) if colmap.get("figi") else ""

        known_by_ticker = ticker in known_tickers
        known_by_norm = norm_ticker in known_norm_tickers if norm_ticker else False
        known_by_isin = isin in known_isins if looks_like_isin(isin) else False

        if known_by_ticker:
            status = "KNOWN_BY_TICKER"
        elif known_by_norm:
            status = "KNOWN_BY_NORMALIZED_TICKER"
        elif known_by_isin:
            status = "KNOWN_BY_ISIN"
        else:
            status = "NEW_CANDIDATE"

        records.append({
            "fd_ticker": ticker,
            "fd_norm_ticker": norm_ticker,
            "fd_isin": isin,
            "fd_name": name,
            "fd_summary": summary,
            "fd_currency": currency,
            "fd_country": country,
            "fd_sector": sector,
            "fd_industry": industry,
            "fd_market": market,
            "fd_exchange": exchange,
            "fd_figi": figi,
            "known_by_ticker": known_by_ticker,
            "known_by_normalized_ticker": known_by_norm,
            "known_by_isin": known_by_isin,
            "candidate_status": status,
            "has_isin": looks_like_isin(isin),
            "has_country": bool(country),
            "has_sector": bool(sector),
            "has_industry": bool(industry),
            "has_name": bool(name),
        })

    return pd.DataFrame(records)


# ============================================================
# YFINANCE STICHPROBE
# ============================================================

def test_yfinance_candidate(
    ticker: str,
    period: str = DEFAULT_HISTORY_PERIOD,
) -> Dict[str, Any]:
    result = {
        "fd_ticker": ticker,
        "yf_ok": False,
        "yf_rows": 0,
        "rsl_usable": False,
        "yf_first_date": "",
        "yf_last_date": "",
        "yf_error": "",
    }

    if yf is None:
        result["yf_error"] = "yfinance nicht installiert"
        return result

    try:
        hist = yf.Ticker(ticker).history(period=period, auto_adjust=False)

        if hist is None or hist.empty:
            result["yf_error"] = "leere Historie"
            return result

        result["yf_ok"] = True
        result["yf_rows"] = int(len(hist))
        result["rsl_usable"] = len(hist) >= 200

        try:
            result["yf_first_date"] = str(hist.index.min())
            result["yf_last_date"] = str(hist.index.max())
        except Exception:
            pass

        return result

    except Exception as e:
        result["yf_error"] = safe_str(e)
        return result


def run_yf_sample(
    df_new: pd.DataFrame,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    period: str = DEFAULT_HISTORY_PERIOD,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> pd.DataFrame:
    if df_new.empty:
        return pd.DataFrame()

    sample_size = min(sample_size, len(df_new))

    # leichte Priorisierung:
    # zuerst mit ISIN / Name / Country / Sector
    df_ranked = df_new.copy()
    df_ranked["priority_score"] = (
        df_ranked["has_isin"].astype(int) * 4
        + df_ranked["has_name"].astype(int) * 2
        + df_ranked["has_country"].astype(int) * 1
        + df_ranked["has_sector"].astype(int) * 1
    )

    df_ranked = df_ranked.sort_values(
        by=["priority_score", "fd_country", "fd_sector", "fd_ticker"],
        ascending=[False, True, True, True]
    )

    # Aus Top-Pool randomisierte Stichprobe ziehen
    top_pool_size = min(max(sample_size * 3, sample_size), len(df_ranked))
    top_pool = df_ranked.head(top_pool_size)

    sample_records = top_pool.sample(
        n=sample_size,
        random_state=42
    ) if len(top_pool) > sample_size else top_pool

    results: List[Dict[str, Any]] = []

    print()
    print("--- yfinance Stichprobe ---")
    print(f"Stichprobengröße: {len(sample_records)}")
    print(f"Historien-Periode: {period}")

    for i, (_, row) in enumerate(sample_records.iterrows(), start=1):
        ticker = safe_str(row["fd_ticker"])
        print(f"[{i}/{len(sample_records)}] Teste {ticker} ...")

        yf_result = test_yfinance_candidate(ticker, period=period)

        merged = dict(row.to_dict())
        merged.update(yf_result)
        results.append(merged)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return pd.DataFrame(results)


# ============================================================
# SUMMARY
# ============================================================

def build_summary(
    project_stats: Dict[str, Any],
    fd_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    yf_sample_df: pd.DataFrame,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "project_universe": {
            "cache_exists": project_stats["cache_exists"],
            "snapshot_exists": project_stats["snapshot_exists"],
            "cache_rows": project_stats["cache_rows"],
            "snapshot_rows": project_stats["snapshot_rows"],
            "total_rows": project_stats["total_rows"],
            "unique_tickers": project_stats["unique_tickers"],
            "unique_norm_tickers": project_stats["unique_norm_tickers"],
            "unique_isins": project_stats["unique_isins"],
        },
        "financedatabase": {
            "rows": int(len(fd_df)),
        },
        "candidates": {},
        "yfinance_sample": {},
    }

    if not candidates_df.empty:
        status_counts = candidates_df["candidate_status"].value_counts(dropna=False).to_dict()

        summary["candidates"] = {
            "rows": int(len(candidates_df)),
            "new_candidates": int((candidates_df["candidate_status"] == "NEW_CANDIDATE").sum()),
            "known_by_ticker": int((candidates_df["candidate_status"] == "KNOWN_BY_TICKER").sum()),
            "known_by_normalized_ticker": int((candidates_df["candidate_status"] == "KNOWN_BY_NORMALIZED_TICKER").sum()),
            "known_by_isin": int((candidates_df["candidate_status"] == "KNOWN_BY_ISIN").sum()),
            "has_isin": int(candidates_df["has_isin"].sum()),
            "has_country": int(candidates_df["has_country"].sum()),
            "has_sector": int(candidates_df["has_sector"].sum()),
            "has_industry": int(candidates_df["has_industry"].sum()),
            "status_counts": status_counts,
        }

    if not yf_sample_df.empty:
        summary["yfinance_sample"] = {
            "rows": int(len(yf_sample_df)),
            "yf_ok": int(yf_sample_df["yf_ok"].sum()),
            "rsl_usable": int(yf_sample_df["rsl_usable"].sum()),
            "with_isin": int(yf_sample_df["has_isin"].sum()) if "has_isin" in yf_sample_df.columns else 0,
            "ok_rate": round(float(yf_sample_df["yf_ok"].mean()) * 100, 2),
            "rsl_rate": round(float(yf_sample_df["rsl_usable"].mean()) * 100, 2),
        }

    return summary


def print_summary(summary: Dict[str, Any]) -> None:
    pu = summary.get("project_universe", {})
    fd_info = summary.get("financedatabase", {})
    cand = summary.get("candidates", {})
    yf_info = summary.get("yfinance_sample", {})

    print()
    print("======================================================================")
    print("ZUSAMMENFASSUNG")
    print("======================================================================")

    print("--- Projektuniversum ---")
    print(f"Cache vorhanden:            {pu.get('cache_exists')}")
    print(f"Snapshot vorhanden:         {pu.get('snapshot_exists')}")
    print(f"Unique Ticker:              {pu.get('unique_tickers')}")
    print(f"Unique NormTicker:          {pu.get('unique_norm_tickers')}")
    print(f"Unique ISIN:                {pu.get('unique_isins')}")

    print()
    print("--- FinanceDatabase ---")
    print(f"FD Zeilen:                  {fd_info.get('rows')}")

    print()
    print("--- Kandidaten ---")
    print(f"Alle FD-Kandidaten:         {cand.get('rows', 0)}")
    print(f"Neue Kandidaten:            {cand.get('new_candidates', 0)}")
    print(f"Bekannt via Ticker:         {cand.get('known_by_ticker', 0)}")
    print(f"Bekannt via NormTicker:     {cand.get('known_by_normalized_ticker', 0)}")
    print(f"Bekannt via ISIN:           {cand.get('known_by_isin', 0)}")
    print(f"Mit ISIN:                   {cand.get('has_isin', 0)}")
    print(f"Mit Country:                {cand.get('has_country', 0)}")
    print(f"Mit Sector:                 {cand.get('has_sector', 0)}")
    print(f"Mit Industry:               {cand.get('has_industry', 0)}")

    if yf_info:
        print()
        print("--- yfinance Stichprobe ---")
        print(f"Getestete Werte:            {yf_info.get('rows', 0)}")
        print(f"Yahoo OK:                   {yf_info.get('yf_ok', 0)}")
        print(f"RSL nutzbar:                {yf_info.get('rsl_usable', 0)}")
        print(f"Yahoo-OK-Quote:             {yf_info.get('ok_rate', 0)} %")
        print(f"RSL-Quote:                  {yf_info.get('rsl_rate', 0)} %")


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    project_root = DEFAULT_PROJECT_ROOT
    if len(sys.argv) > 1:
        project_root = os.path.abspath(sys.argv[1])

    output_dir = ensure_output_dir(project_root)

    print("======================================================================")
    print("FD CANDIDATE DISCOVERY")
    print("======================================================================")
    print(f"Projekt-Root: {project_root}")
    print(f"Output-Ordner: {output_dir}")
    print()

    # 1) Projektuniversum laden
    project_df, project_stats = load_project_universe(project_root)

    print("--- Projektuniversum ---")
    print(f"Cache gefunden:    {'JA' if project_stats['cache_exists'] else 'NEIN'}")
    print(f"Snapshot gefunden: {'JA' if project_stats['snapshot_exists'] else 'NEIN'}")
    print(f"Cache-Zeilen:      {project_stats['cache_rows']}")
    print(f"Snapshot-Zeilen:   {project_stats['snapshot_rows']}")
    print(f"Gesamt Rohzeilen:  {project_stats['total_rows']}")
    print(f"Unique Ticker:     {project_stats['unique_tickers']}")
    print(f"Unique NormTicker: {project_stats['unique_norm_tickers']}")
    print(f"Unique ISIN:       {project_stats['unique_isins']}")

    # 2) FinanceDatabase laden
    fd_df, colmap = load_fd_equities()

    print()
    print("--- FinanceDatabase ---")
    print(f"FD Shape:          {fd_df.shape}")
    print("Erkannte FD-Felder:")
    for k, v in colmap.items():
        print(f"  - {k}: {v}")

    # 3) Kandidaten bauen
    candidates_df = build_fd_candidates(
        fd_df=fd_df,
        colmap=colmap,
        known_tickers=project_stats["ticker_set"],
        known_norm_tickers=project_stats["norm_ticker_set"],
        known_isins=project_stats["isin_set"],
    )

    print(f"FD Kandidaten:     {len(candidates_df)}")

    if candidates_df.empty:
        print()
        print("FEHLER: Keine FD-Kandidaten erzeugt.")
        return 1

    # 4) Aufteilen
    df_new = candidates_df[candidates_df["candidate_status"] == "NEW_CANDIDATE"].copy()
    df_known = candidates_df[candidates_df["candidate_status"] != "NEW_CANDIDATE"].copy()

    # 5) CSV schreiben
    all_path = os.path.join(output_dir, OUTPUT_ALL)
    new_path = os.path.join(output_dir, OUTPUT_NEW)
    known_path = os.path.join(output_dir, OUTPUT_KNOWN)
    yf_path = os.path.join(output_dir, OUTPUT_YF_SAMPLE)
    summary_path = os.path.join(output_dir, OUTPUT_SUMMARY)

    candidates_df.to_csv(all_path, index=False, encoding="utf-8-sig")
    df_new.to_csv(new_path, index=False, encoding="utf-8-sig")
    df_known.to_csv(known_path, index=False, encoding="utf-8-sig")

    print()
    print("--- CSV-Ausgaben ---")
    print(f"Alle Kandidaten:   {all_path}")
    print(f"Neue Kandidaten:   {new_path}")
    print(f"Bekannte Werte:    {known_path}")

    # 6) yfinance-Stichprobe nur für neue Kandidaten
    yf_sample_df = pd.DataFrame()
    if not df_new.empty:
        yf_sample_df = run_yf_sample(
            df_new=df_new,
            sample_size=DEFAULT_SAMPLE_SIZE,
            period=DEFAULT_HISTORY_PERIOD,
            sleep_seconds=DEFAULT_SLEEP_SECONDS,
        )
        if not yf_sample_df.empty:
            yf_sample_df.to_csv(yf_path, index=False, encoding="utf-8-sig")
            print(f"yfinance Sample:   {yf_path}")
    else:
        print()
        print("Keine neuen Kandidaten gefunden; yfinance-Stichprobe übersprungen.")

    # 7) Summary
    summary = build_summary(
        project_stats=project_stats,
        fd_df=fd_df,
        candidates_df=candidates_df,
        yf_sample_df=yf_sample_df,
    )
    write_summary_json(summary_path, summary)
    print(f"Summary JSON:      {summary_path}")

    print_summary(summary)

    print()
    print("Fertig.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nAbgebrochen.")
        raise SystemExit(130)
    except Exception as e:
        print()
        print("UNBEHANDELTER FEHLER:")
        print(safe_str(e))
        print()
        traceback.print_exc()
        raise SystemExit(1)
