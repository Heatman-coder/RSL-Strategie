#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

try:
    import financedatabase as fd
except ImportError:
    print("Fehler: financedatabase nicht installiert.")
    print("Installiere mit: pip install financedatabase")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("Fehler: pandas nicht installiert.")
    print("Installiere mit: pip install pandas")
    sys.exit(1)


def open_file(filepath):
    """Öffnet Datei plattformabhängig"""
    try:
        if sys.platform.startswith("win"):
            os.startfile(filepath)
        elif sys.platform.startswith("darwin"):
            os.system(f"open \"{filepath}\"")
        else:
            os.system(f"xdg-open \"{filepath}\"")
    except Exception as e:
        print(f"Konnte Datei nicht automatisch öffnen: {e}")


def main():
    output_file = os.path.abspath("fd_equities_raw.xlsx")

    print("=" * 60)
    print("FINANCEDATABASE RAW EXPORT (EXCEL)")
    print("=" * 60)

    print("Lade FinanceDatabase Equities ...")
    df = fd.Equities().select()

    print(f"Shape (original): {df.shape}")
    print(f"Index-Name: {df.index.name}")

    # Index → Spalte (Ticker sichern)
    index_name = df.index.name if df.index.name else "index"
    df = df.reset_index()

    if index_name in df.columns:
        df.rename(columns={index_name: "ticker"}, inplace=True)
    elif "index" in df.columns:
        df.rename(columns={"index": "ticker"}, inplace=True)

    print(f"Shape (export): {df.shape}")
    print(f"Spaltenanzahl: {len(df.columns)}")

    print("Beispiel-Spalten:")
    print(df.columns.tolist()[:15])

    # Excel Export
    print(f"Speichere Excel-Datei: {output_file}")
    try:
        df.to_excel(output_file, index=False, engine="openpyxl")
    except ImportError:
        print("Fehler: openpyxl fehlt.")
        print("Installiere mit: pip install openpyxl")
        sys.exit(1)

    print("Export erfolgreich.")

    # Datei öffnen
    print("Öffne Datei ...")
    open_file(output_file)

    print("Fertig.")


if __name__ == "__main__":
    main()
