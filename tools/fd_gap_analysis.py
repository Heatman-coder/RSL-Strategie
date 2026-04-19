#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fd_gap_analysis.py
Read-only Validierung von FinanceDatabase gegen Projekt-Bestandsdaten.
Version: 1.0
"""

import os
import json
import pandas as pd
try:
    import financedatabase as fd
except ImportError:
    print("Fehler: financedatabase ist nicht installiert. (pip install financedatabase)")
    exit(1)

# --- KONFIGURATION (Pfade an dein Projekt angepasst) ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_FILE = os.path.join(PROJECT_ROOT, "ticker_info_cache.json")
SNAPSHOT_FILE = os.path.join(PROJECT_ROOT, "reports", "last_analysis_snapshot.json")
AUDIT_OUTPUT = os.path.join(PROJECT_ROOT, "reports", "fd_audit_report.csv")

def load_input_data():
    """Lädt Ticker aus Cache und Snapshot für eine breite Testbasis."""
    all_data = {}
    
    # 1. Hauptquelle: Ticker Info Cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            for t, info in cache.items():
                all_data[t] = {
                    "isin": info.get("isin", ""),
                    "sector": info.get("sector", "Unknown"),
                    "country": info.get("country", "Unknown"),
                    "source": "cache"
                }
    
    # 2. Ergänzung: Letzter Snapshot (falls vorhanden)
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            snap = json.load(f)
            for s in snap.get("stock_results", []):
                t = s.get("yahoo_symbol")
                if t and t not in all_data:
                    all_data[t] = {
                        "isin": s.get("isin", ""),
                        "sector": s.get("sector", "Unknown"),
                        "country": s.get("land", "Unknown"),
                        "source": "snapshot"
                    }
                    
    return all_data

def run_analysis():
    print(f"--- Starte FD Gap-Analyse ---")
    project_data = load_input_data()
    if not project_data:
        print("Keine Eingabedaten gefunden. Bitte Pfade prüfen.")
        return

    print(f"Lade FinanceDatabase (Equities)...")
    # Wir laden das gesamte DataFrame für effiziente Lookups
    fd_df = fd.Equities().select()
    
    # Hilfs-Dicts für O(1) Lookups bauen
    # ISIN -> [Index-Einträge]
    isin_lookup = fd_df[fd_df['isin'].notna()].groupby('isin').groups
    # Ticker -> [Index-Einträge]
    ticker_lookup = fd_df.groupby(fd_df.index).groups

    stats = {
        "total": len(project_data),
        "direct_hit": 0, "norm_hit": 0, "isin_hit": 0,
        "ambiguous": 0, "no_hit": 0, "isin_conflict": 0,
        "field_isin": 0, "field_sector": 0, "field_industry": 0
    }
    
    audit_log = []

    for y_ticker, p_info in project_data.items():
        match_type = "None"
        fd_entry = None
        candidates = []
        
        # 1. ISIN Match (Sicherster Weg)
        p_isin = p_info["isin"]
        if p_isin and len(p_isin) > 5:
            if p_isin in isin_lookup:
                indices = isin_lookup[p_isin]
                candidates = [fd_df.iloc[fd_df.index.get_loc(i)] if isinstance(i, str) else fd_df.loc[i] for i in indices]
                match_type = "ISIN"

        # 2. Direct Ticker Match
        if match_type == "None" and y_ticker in ticker_lookup:
            indices = ticker_lookup[y_ticker]
            candidates = [fd_df.loc[i] for i in indices]
            match_type = "Direct"

        # 3. Normalized Ticker Match (Suffix weg)
        if match_type == "None":
            base_ticker = y_ticker.split('.')[0]
            if base_ticker in ticker_lookup:
                indices = ticker_lookup[base_ticker]
                candidates = [fd_df.loc[i] for i in indices]
                match_type = "Normalized"

        # Auswertung der Kandidaten
        if not candidates:
            stats["no_hit"] += 1
        elif len(candidates) > 1 and match_type != "ISIN":
            # Bei Ticker-Matches ist Mehrdeutigkeit ein Risiko
            stats["ambiguous"] += 1
            match_type = "Ambiguous"
        else:
            # Wir haben einen (ggf. eindeutigen) Treffer
            fd_entry = candidates[0]
            if match_type == "ISIN": stats["isin_hit"] += 1
            elif match_type == "Direct": stats["direct_hit"] += 1
            elif match_type == "Normalized": stats["norm_hit"] += 1
            
            # Konflikt Check
            fd_isin = str(fd_entry.get('isin', ''))
            if p_isin and fd_isin and p_isin != fd_isin:
                stats["isin_conflict"] += 1
            
            # Feld-Abdeckung
            if fd_isin and fd_isin != "nan": stats["field_isin"] += 1
            if str(fd_entry.get('sector')) != "nan": stats["field_sector"] += 1
            if str(fd_entry.get('industry')) != "nan": stats["field_industry"] += 1

        # Audit Zeile bauen
        audit_log.append({
            "Yahoo_Ticker": y_ticker,
            "Project_ISIN": p_isin,
            "Match_Status": match_type,
            "FD_Name": fd_entry.get('name') if fd_entry is not None else "",
            "FD_ISIN": fd_entry.get('isin') if fd_entry is not None else "",
            "FD_Sector": fd_entry.get('sector') if fd_entry is not None else "",
            "FD_Country": fd_entry.get('country') if fd_entry is not None else "",
            "Candidates_Count": len(candidates)
        })

    # CSV Export
    pd.DataFrame(audit_log).to_csv(AUDIT_OUTPUT, index=False, sep=";")
    
    # Zusammenfassung
    print("\n" + "="*40)
    print("ERGEBNIS DER GAP-ANALYSE")
    print("="*40)
    print(f"Analysierte Ticker (Projekt): {stats['total']}")
    print(f"Treffer über ISIN:           {stats['isin_hit']} ({stats['isin_hit']/stats['total']:.1%})")
    print(f"Treffer über Ticker (direkt): {stats['direct_hit']} ({stats['direct_hit']/stats['total']:.1%})")
    print(f"Treffer über Ticker (norm.):  {stats['norm_hit']} ({stats['norm_hit']/stats['total']:.1%})")
    print(f"Mehrdeutige Treffer:         {stats['ambiguous']}")
    print(f"Kein Treffer in FD:          {stats['no_hit']}")
    print("-" * 40)
    print(f"ISIN-Konflikte (Yahoo vs FD): {stats['isin_conflict']}")
    print("-" * 40)
    print("FD DATENQUALITÄT BEI TREFFERN:")
    valid_hits = stats['total'] - stats['no_hit'] - stats['ambiguous']
    if valid_hits > 0:
        print(f"ISIN vorhanden:              {stats['field_isin']/valid_hits:.1%}")
        print(f"Sektor vorhanden:            {stats['field_sector']/valid_hits:.1%}")
        print(f"Industrie vorhanden:         {stats['field_industry']/valid_hits:.1%}")
    
    print(f"\nAudit-Bericht erstellt: {AUDIT_OUTPUT}")

if __name__ == "__main__":
    run_analysis()