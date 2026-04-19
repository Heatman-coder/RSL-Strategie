import financedatabase as fd
import pandas as pd
import os
import sys

def main():
    print("📥 Lade alle Equities aus FinanceDatabase...")

    # Daten laden
    equities = fd.Equities()
    df = equities.select()

    # Ticker aus Index holen
    df = df.reset_index().rename(columns={"index": "ticker"})

    print(f"\n✅ Anzahl Aktien: {len(df)}")
    print(f"✅ Anzahl Spalten: {len(df.columns)}")

    # Alle Spalten anzeigen
    print("\n🧾 Alle verfügbaren Spalten:")
    for col in df.columns:
        print(f"- {col}")

    # Datenqualität
    print("\n📊 Fehlende Werte pro Spalte (Top 20):")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    # Dateinamen
    excel_file = "fd_all_equities_full.xlsx"
    csv_file = "fd_all_equities_full.csv"

    print("\n💾 Speichere Dateien...")

    # Excel speichern
    try:
        df.to_excel(excel_file, index=False)
        print(f"✅ Excel gespeichert: {excel_file}")
    except Exception as e:
        print(f"⚠️ Excel fehlgeschlagen: {e}")

    # CSV als Backup
    df.to_csv(csv_file, index=False)
    print(f"✅ CSV gespeichert: {csv_file}")

    # Beispiel-Datensatz anzeigen
    print("\n🔎 Beispiel Datensatz:")
    print(df.iloc[0].to_dict())

    # Datei automatisch öffnen (je nach OS)
    try:
        print("\n📂 Öffne Excel-Datei...")
        if sys.platform == "win32":
            os.startfile(excel_file)
        elif sys.platform == "darwin":
            os.system(f"open '{excel_file}'")
        else:
            os.system(f"xdg-open '{excel_file}'")
    except Exception as e:
        print(f"⚠️ Konnte Datei nicht automatisch öffnen: {e}")

    print("\n🎉 Fertig!")

if __name__ == "__main__":
    main()
