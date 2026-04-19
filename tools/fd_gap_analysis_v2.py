#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fd_gap_analysis.py

Read-only Validierung von FinanceDatabase (FD) gegen Projekt-Bestandsdaten.

Ziele:
- Keine Projektdateien verändern
- Nur lesen + Audit-CSV schreiben
- FD-Treffer klassifizieren in:
  - ISIN
  - Direct
  - Normalized
  - Ambiguous
  - NoHit
- Defensive Behandlung fehlender Spalten / variabler JSON-Strukturen

Benutzung:
    python fd_gap_analysis.py

Optional:
    python fd_gap_analysis.py /pfad/zu/deinem/projektroot
"""

from __future__ import annotations

import os
import sys
import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import financedatabase as fd
except ImportError:
    print("Fehler: financedatabase ist nicht installiert. Bitte ausführen:")
    print("  pip install financedatabase")
    sys.exit(1)


# ============================================================
# KONFIGURATION
# ============================================================

DEFAULT_PROJECT_ROOT = os.path.abspath(
    sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
)

CACHE_CANDIDATES = [
    "ticker_info_cache.json",
    os.path.join("data", "ticker_info_cache.json"),
    os.path.join("cache", "ticker_info_cache.json"),
]

SNAPSHOT_CANDIDATES = [
    os.path.join("reports", "last_analysis_snapshot.json"),
    "last_analysis_snapshot.json",
]

AUDIT_OUTPUT_REL = os.path.join("reports", "fd_audit_report.csv")

# Snapshot-Feldannahmen: leicht anpassbar
SNAP_ROOT_KEYS = ["stock_results", "results", "stocks", "items"]
SNAP_TICKER_KEYS = ["yahoo_symbol", "symbol", "ticker"]
SNAP_ISIN_KEYS = ["isin", "ISIN"]
SNAP_SECTOR_KEYS = ["sector", "sektor"]
SNAP_COUNTRY_KEYS = ["land", "country", "nation"]

KNOWN_EXCHANGE_SUFFIXES = [
    ".DE", ".F", ".SG", ".DU", ".BE", ".HM", ".MU",
    ".PA", ".AS", ".BR", ".MI", ".MC", ".LS", ".VI", ".HE", ".ST", ".OL", ".CO", ".SW",
    ".L", ".IR", ".WA", ".PR",
    ".T", ".HK", ".TW", ".TWO", ".SS", ".SZ", ".KS", ".KQ",
    ".SI", ".JK", ".KL", ".BK",
    ".NS", ".BO",
    ".TO", ".V",
    ".AX",
    ".MX", ".SA", ".BA", ".BC", ".BD", ".JO", ".QA",
]

FD_COL_CANDIDATES = {
    "name": ["name", "company", "company_name"],
    "isin": ["isin", "ISIN"],
    "sector": ["sector", "gics_sector"],
    "industry": ["industry", "gics_industry"],
    "country": ["country", "country_name"],
}


# ============================================================
# HILFSFUNKTIONEN
# ============================================================

def first_existing_path(project_root: str, candidates: List[str]) -> Optional[str]:
    for rel in candidates:
        path = os.path.join(project_root, rel)
        if os.path.exists(path):
            return path
    return None


def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def normalize_isin(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().upper()
    if s in ("", "NAN", "NONE", "NULL"):
        return ""
    return s


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.lower() in ("nan", "none", "null"):
        return ""
    return s


def normalize_ticker(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def strip_known_suffix(yahoo_ticker: str) -> str:
    t = normalize_ticker(yahoo_ticker)
    for suffix in sorted(KNOWN_EXCHANGE_SUFFIXES, key=len, reverse=True):
        if t.endswith(suffix):
            return t[: -len(suffix)]
    return t


def detect_column(df: pd.DataFrame, logical_name: str) -> Optional[str]:
    for candidate in FD_COL_CANDIDATES.get(logical_name, []):
        if candidate in df.columns:
            return candidate
    return None


def safe_get(row: pd.Series, col: Optional[str], default: str = "") -> str:
    if not col:
        return default
    if col not in row.index:
        return default
    value = row[col]
    if pd.isna(value):
        return default
    return str(value).strip()


def load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_first_key(d: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    for key in candidates:
        if key in d:
            return key
    return None


def extract_snapshot_rows(snapshot_obj: Any) -> List[Dict[str, Any]]:
    if isinstance(snapshot_obj, list):
        return [x for x in snapshot_obj if isinstance(x, dict)]

    if not isinstance(snapshot_obj, dict):
        return []

    root_key = find_first_key(snapshot_obj, SNAP_ROOT_KEYS)
    if root_key and isinstance(snapshot_obj[root_key], list):
        return [x for x in snapshot_obj[root_key] if isinstance(x, dict)]

    return []


def row_to_fd_record(row: pd.Series, fd_cols: Dict[str, Optional[str]], fd_index_name: str) -> Dict[str, str]:
    return {
        "ticker": normalize_ticker(row.name if row.name is not None else ""),
        "name": safe_get(row, fd_cols["name"]),
        "isin": normalize_isin(safe_get(row, fd_cols["isin"])),
        "sector": safe_get(row, fd_cols["sector"]),
        "industry": safe_get(row, fd_cols["industry"]),
        "country": safe_get(row, fd_cols["country"]),
        "index_name": fd_index_name,
    }


def candidate_signature(rec: Dict[str, str]) -> str:
    parts = [
        rec.get("ticker", ""),
        rec.get("name", ""),
        rec.get("isin", ""),
        rec.get("country", ""),
    ]
    return " | ".join(parts)


# ============================================================
# INPUT LADEN
# ============================================================

def load_project_data(project_root: str, cache_file: Optional[str], snapshot_file: Optional[str]) -> Dict[str, Dict[str, str]]:
    all_data: Dict[str, Dict[str, str]] = {}

    if cache_file:
        print(f" - Lade Cache: {cache_file}")
        cache_obj = load_json_file(cache_file)

        if isinstance(cache_obj, dict):
            for ticker, info in cache_obj.items():
                if not isinstance(info, dict):
                    continue
                t = normalize_ticker(ticker)
                if not t:
                    continue

                all_data[t] = {
                    "isin": normalize_isin(info.get("isin")),
                    "sector": normalize_text(info.get("sector")),
                    "country": normalize_text(info.get("country")),
                    "source": "cache",
                }

    if snapshot_file:
        print(f" - Lade Snapshot: {snapshot_file}")
        snap_obj = load_json_file(snapshot_file)
        rows = extract_snapshot_rows(snap_obj)

        for item in rows:
            ticker_key = find_first_key(item, SNAP_TICKER_KEYS)
            isin_key = find_first_key(item, SNAP_ISIN_KEYS)
            sector_key = find_first_key(item, SNAP_SECTOR_KEYS)
            country_key = find_first_key(item, SNAP_COUNTRY_KEYS)

            t = normalize_ticker(item.get(ticker_key, "")) if ticker_key else ""
            if not t:
                continue

            if t not in all_data:
                all_data[t] = {
                    "isin": normalize_isin(item.get(isin_key, "")) if isin_key else "",
                    "sector": normalize_text(item.get(sector_key, "")) if sector_key else "",
                    "country": normalize_text(item.get(country_key, "")) if country_key else "",
                    "source": "snapshot",
                }

    return all_data


# ============================================================
# FD LADEN + INDIZES BAUEN
# ============================================================

def load_fd_equities() -> pd.DataFrame:
    print("\n--- Lade FinanceDatabase Equities ---")
    df = fd.Equities().select()

    if not isinstance(df, pd.DataFrame) or df.empty:
        raise RuntimeError("FinanceDatabase lieferte kein nutzbares DataFrame.")

    # Index auf String / uppercase normalisieren
    df = df.copy()
    df.index = df.index.map(lambda x: normalize_ticker(x))

    return df


def build_fd_indices(fd_df: pd.DataFrame, fd_cols: Dict[str, Optional[str]]) -> Tuple[Dict[str, List[pd.Series]], Dict[str, List[pd.Series]]]:
    ticker_map: Dict[str, List[pd.Series]] = {}
    isin_map: Dict[str, List[pd.Series]] = {}

    for idx, row in fd_df.iterrows():
        ticker = normalize_ticker(idx)
        isin = normalize_isin(safe_get(row, fd_cols["isin"]))

        ticker_map.setdefault(ticker, []).append(row)

        if isin:
            isin_map.setdefault(isin, []).append(row)

    return ticker_map, isin_map


# ============================================================
# MATCHING
# ============================================================

def dedupe_candidates(rows: List[pd.Series], fd_cols: Dict[str, Optional[str]]) -> List[Dict[str, str]]:
    seen = set()
    out: List[Dict[str, str]] = []

    for row in rows:
        rec = row_to_fd_record(row, fd_cols, fd_index_name=str(row.name))
        sig = candidate_signature(rec)
        if sig not in seen:
            seen.add(sig)
            out.append(rec)

    return out


def classify_match(
    yahoo_ticker: str,
    project_isin: str,
    ticker_map: Dict[str, List[pd.Series]],
    isin_map: Dict[str, List[pd.Series]],
    fd_cols: Dict[str, Optional[str]],
) -> Tuple[str, List[Dict[str, str]]]:
    """
    Reihenfolge:
    1. ISIN
    2. Direct ticker
    3. Normalized ticker
    """

    p_isin = normalize_isin(project_isin)
    y_ticker = normalize_ticker(yahoo_ticker)

    # 1) ISIN match
    if p_isin and p_isin in isin_map:
        candidates = dedupe_candidates(isin_map[p_isin], fd_cols)
        if len(candidates) == 1:
            return "ISIN", candidates
        elif len(candidates) > 1:
            return "Ambiguous", candidates

    # 2) direct ticker
    if y_ticker in ticker_map:
        candidates = dedupe_candidates(ticker_map[y_ticker], fd_cols)
        if len(candidates) == 1:
            return "Direct", candidates
        elif len(candidates) > 1:
            return "Ambiguous", candidates

    # 3) normalized ticker
    base = strip_known_suffix(y_ticker)
    if base and base != y_ticker and base in ticker_map:
        candidates = dedupe_candidates(ticker_map[base], fd_cols)
        if len(candidates) == 1:
            return "Normalized", candidates
        elif len(candidates) > 1:
            return "Ambiguous", candidates

    return "NoHit", []


# ============================================================
# HAUPTLAUF
# ============================================================

def run_analysis(project_root: str) -> None:
    print("============================================================")
    print("FD GAP ANALYSIS")
    print("============================================================")
    print(f"Projekt-Root: {project_root}")

    cache_file = first_existing_path(project_root, CACHE_CANDIDATES)
    snapshot_file = first_existing_path(project_root, SNAPSHOT_CANDIDATES)
    audit_output = os.path.join(project_root, AUDIT_OUTPUT_REL)

    print("\n--- Eingabedateien ---")
    print(f"Cache gefunden:    {cache_file if cache_file else 'NEIN'}")
    print(f"Snapshot gefunden: {snapshot_file if snapshot_file else 'NEIN'}")
    print(f"Audit-Ausgabe:     {audit_output}")

    project_data = load_project_data(project_root, cache_file, snapshot_file)
    if not project_data:
        raise RuntimeError("Keine Projektdaten gefunden. Bitte Pfade / JSON-Strukturen prüfen.")

    print(f"\nGeladene Projekt-Ticker: {len(project_data)}")

    fd_df = load_fd_equities()

    print("\n--- FD Diagnose ---")
    print(f"FD Shape: {fd_df.shape}")
    print(f"FD Index Beispiel: {list(fd_df.index[:10])}")
    print(f"FD Columns: {list(fd_df.columns)}")

    fd_cols = {logical: detect_column(fd_df, logical) for logical in FD_COL_CANDIDATES.keys()}

    print("\nErkannte FD-Spalten:")
    for logical, actual in fd_cols.items():
        print(f" - {logical}: {actual if actual else 'NICHT GEFUNDEN'}")

    ticker_map, isin_map = build_fd_indices(fd_df, fd_cols)

    stats = {
        "total": 0,
        "ISIN": 0,
        "Direct": 0,
        "Normalized": 0,
        "Ambiguous": 0,
        "NoHit": 0,
        "isin_conflict": 0,
        "field_isin": 0,
        "field_sector": 0,
        "field_industry": 0,
        "field_country": 0,
    }

    audit_rows: List[Dict[str, Any]] = []

    print("\n--- Starte Matching ---")
    for yahoo_ticker, pdata in project_data.items():
        stats["total"] += 1

        project_isin = normalize_isin(pdata.get("isin", ""))
        project_sector = normalize_text(pdata.get("sector", ""))
        project_country = normalize_text(pdata.get("country", ""))
        normalized_ticker = strip_known_suffix(yahoo_ticker)

        match_status, candidates = classify_match(
            yahoo_ticker=yahoo_ticker,
            project_isin=project_isin,
            ticker_map=ticker_map,
            isin_map=isin_map,
            fd_cols=fd_cols,
        )

        stats[match_status] += 1

        chosen = candidates[0] if len(candidates) == 1 else None

        candidate_list_str = " || ".join(candidate_signature(c) for c in candidates)

        fd_name = chosen.get("name", "") if chosen else ""
        fd_isin = chosen.get("isin", "") if chosen else ""
        fd_sector = chosen.get("sector", "") if chosen else ""
        fd_industry = chosen.get("industry", "") if chosen else ""
        fd_country = chosen.get("country", "") if chosen else ""
        fd_ticker = chosen.get("ticker", "") if chosen else ""

        if chosen:
            if fd_isin:
                stats["field_isin"] += 1
            if fd_sector:
                stats["field_sector"] += 1
            if fd_industry:
                stats["field_industry"] += 1
            if fd_country:
                stats["field_country"] += 1

            if project_isin and fd_isin and project_isin != fd_isin:
                stats["isin_conflict"] += 1

        audit_rows.append({
            "Yahoo_Ticker": yahoo_ticker,
            "Normalized_Ticker": normalized_ticker,
            "Project_Source": pdata.get("source", ""),
            "Project_ISIN": project_isin,
            "Project_Sector": project_sector,
            "Project_Country": project_country,
            "Match_Status": match_status,
            "Candidates_Count": len(candidates),
            "FD_Ticker": fd_ticker,
            "FD_Name": fd_name,
            "FD_ISIN": fd_isin,
            "FD_Sector": fd_sector,
            "FD_Industry": fd_industry,
            "FD_Country": fd_country,
            "ISIN_Conflict": "YES" if chosen and project_isin and fd_isin and project_isin != fd_isin else "",
            "All_Candidates": candidate_list_str,
        })

    ensure_parent_dir(audit_output)
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(audit_output, index=False, sep=";")

    valid_hits = stats["ISIN"] + stats["Direct"] + stats["Normalized"]

    print("\n============================================================")
    print("ERGEBNIS")
    print("============================================================")
    print(f"Analysierte Ticker:            {stats['total']}")
    print(f"Treffer über ISIN:            {stats['ISIN']} ({stats['ISIN'] / stats['total']:.1%})")
    print(f"Treffer über Direct:          {stats['Direct']} ({stats['Direct'] / stats['total']:.1%})")
    print(f"Treffer über Normalized:      {stats['Normalized']} ({stats['Normalized'] / stats['total']:.1%})")
    print(f"Mehrdeutig:                   {stats['Ambiguous']} ({stats['Ambiguous'] / stats['total']:.1%})")
    print(f"Kein Treffer:                 {stats['NoHit']} ({stats['NoHit'] / stats['total']:.1%})")
    print("-" * 60)
    print(f"ISIN-Konflikte:               {stats['isin_conflict']}")

    if valid_hits > 0:
        print("-" * 60)
        print("FD Feldabdeckung bei eindeutigen Treffern:")
        print(f"ISIN vorhanden:               {stats['field_isin'] / valid_hits:.1%}")
        print(f"Sektor vorhanden:             {stats['field_sector'] / valid_hits:.1%}")
        print(f"Industrie vorhanden:          {stats['field_industry'] / valid_hits:.1%}")
        print(f"Land vorhanden:               {stats['field_country'] / valid_hits:.1%}")

    print("-" * 60)
    print(f"Audit-Datei geschrieben:      {audit_output}")

    print("\n--- Nächste manuelle Checks ---")
    print("1. Zeilen mit Match_Status = Ambiguous prüfen")
    print("2. Zeilen mit ISIN_Conflict = YES prüfen")
    print("3. Deutsche / europäische Ticker ohne Treffer separat ansehen")
    print("4. Prüfen, ob FD eher als ISIN-Fallback oder als Sector-Enrichment taugt")


if __name__ == "__main__":
    try:
        run_analysis(DEFAULT_PROJECT_ROOT)
    except Exception as exc:
        print("\nFEHLER:")
        print(str(exc))
        sys.exit(1)
