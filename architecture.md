# Architektur

## Zielbild

Der Code ist jetzt in einen orchestrierenden Einstieg (`final.py`) plus Kernmodule aufgeteilt:

- Konfiguration und Pfade
- Datenbeschaffung und Cache
- Ranking/Statuslogik
- Quality Gate (Strict Mode)
- Console UI
- Excel/CSV Reporting
- Startup/Orchestrierung

Aktive Module:
- `core/ranking.py`: Ranking, Prozent/Balken, Multi-Scope-Status, Portfolio-Sortierung
- `core/quality_gate.py`: Qualitätsmetriken, Strict-Auswertung, Quality-Gate
- `core/summaries.py`: `etf_summary`, `sector_summary`, `industry_summary`
- `core/candidate_engine.py`: Kaufkandidaten-Logik mit Branchen-/Cluster-/Momentum-Filtern
- `core/settings_ui.py`: gruppiertes Einstellungsmenue mit Alltags- und Expertenzugang
- `core/reporting_excel.py`: Excel-Export, Formatierung, Highlighting
- `core/console_symbols.py`: konsistente Symbol-/Pfeil-Auswahl mit ASCII-Fallback
- `core/console_ui.py`: Dashboard/Heatmap/Bestandscheck + Report-Export Flow
- `core/data_pipeline.py`: ETF-Cache/Download/Deduplizierung fuer das Analyse-Universum

`final.py` bleibt Entry Point und UI/Orchestrierungs-Schicht.

## Datenfluss (End-to-End)

1. ETF-Auswahl laden/abfragen (`etf_config.json`)
2. ETF-Holdings laden (Cache oder Download)
3. Ticker normalisieren, mappen, deduplizieren
4. Kurs-/Historien-Daten laden (History-Cache + Fallback)
5. Filter/Flags/Indikatoren berechnen
6. Ranking aufbauen (`rsl_rang`, `mktcap_rang`)
7. Quality Gate prüfen (`run_quality_gate`)
8. Snapshot speichern (`last_analysis_snapshot.json`)
9. Console-Output + Portfolio-Dialog rendern
10. Excel/CSV Reports schreiben

## Hauptbausteine in `final.py`

- `MarketDataManager`: History/Info-Cache, Batch-/Fallback-Download, Fehlertracking
- `PortfolioManager`: Persistenz aktuelles Portfolio
- Wrapper auf `core/*` für Ranking/Summaries/Quality Gate
- Wrapper auf `core/data_pipeline.py` fuer ETF-Universum
- `render_analysis_output`: schlanker Wrapper, delegiert an `core/console_ui.py`
- `save_excel_report_safely`: XLSX-Erzeugung und Formatierung
- `save_analysis_snapshot` / `load_analysis_snapshot`: Rerender ohne Download

## Betriebsmodi

- Neuer Lauf: kompletter Download + Analyse
- Snapshot-Rerender: gleiche Anzeige auf zuletzt gespeichertem Datenstand ohne Download

## Nächster sinnvoller Schritt

- Optional weitere Aufteilung von `final.py` in:
  - `core/reporting_pipeline.py` (CSV/JSON Export-Flow)
  - `core/data_pipeline.py` (Download/Cache/Fallback-Orchestrierung)

Die kritischen Logikblöcke sind bereits modularisiert.

## Aktuelle Priorität

Der wichtigste Stabilitätshebel ist jetzt nicht noch mehr Feature-Ausbau, sondern:

- Kernlogik nur noch in `core/*`
- `final.py` als dünner Orchestrator
- Regressionstests für jede ausgelagerte Kernregel
