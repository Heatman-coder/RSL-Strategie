﻿# Global RSL Scanner

Dieses Projekt analysiert ein ETF-basiertes Aktienuniversum mit RSL-Logik, Multi-Scope-Bewertung (Gesamt, Sektor, Branche, ETF), Portfolio-Workflow, Snapshot-Rerender und Excel/Konsolen-Reporting.

## Schnellstart

```powershell
python final.py
```

Beim Start:
- `1` letzter Datenstand neu anzeigen (ohne neuen Download, aus Snapshot)
- `2` neuer Lauf mit Download/Analyse
- `3` nur Reports-Ordner/letzte Dateien anzeigen

Das Einstellungsmenü (`3`) wurde konsolidiert und bietet nun auch Strategie-Profile und Experten-Optionen.

## Kernfunktionen

- RSL-Ranking auf grossem Universum (mehrere ETFs)
- Multi-Scope Status pro Aktie: `HOLD`, `WARN`, `SELL`
- Institutional Grade Candidate Engine: Factor Orthogonalization, Sector-Neutral Z-Scoring und Risiko-Parität.
- Portfolio-Bestandscheck mit Entscheidungsdialog
- Excel-Report mit `main`, `raw_data`, `etf_summary`, `sector_summary`, `industry_summary`, `cluster_summary`, `integrity_issues`, `indikatoren`, `config_snapshot`
- Strict Quality Gate mit Lauf-Abbruch bei Qualitaetsverletzungen
- Snapshot/Rerender ohne erneuten Download

## Wichtige Dateien

- Hauptskript: `final.py`
- Module:
  - `core/ranking.py`
  - `core/quality_gate.py`
  - `core/summaries.py`
  - `core/candidate_engine.py`
  - `core/settings_ui.py`
  - `core/reporting_excel.py`
  - `core/console_symbols.py`
  - `core/console_ui.py`
  - `core/data_pipeline.py`
  - `core/rsl_integrity.py` (NEU: Datenqualitäts-Wächter)
  - `core/settings_catalog.py` (NEU: Strategie-Profile & Defaults)
  - `core/final_support.py` (NEU: Orchestrierungs-Helfer)
- Tests: `tests/test_core_rules.py`
- Reports: `reports/`

## Dokumentation

- Architektur: `docs/architecture.md`
- Konfiguration: `docs/config.md`
- Outputs: `docs/outputs.md`
- AI/Dev Handoff: `docs/ai_handoff.md`

## Tests

```powershell
python -m pytest -q
```

Die Tests decken aktuell vor allem Ranking, Multi-Scope-Status, Quality Gate und Kandidatenlogik ab.

## Betriebshinweise

- `final.py` versucht beim Start automatisch den bestmoeglichen Konsolenmodus
  auf Windows zu aktivieren (UTF-8 + ANSI-Farben).
- Wenn die Konsole Unicode schlecht darstellt:
  - `RSL_ASCII_CONSOLE=1` setzen (Windows PowerShell):
  ```powershell
  $env:RSL_ASCII_CONSOLE='1'
  python final.py
  ```
- Optional Emoji-Symbole aktivieren:
  ```powershell
  $env:RSL_EMOJI='1'
  python final.py
  ```
- Strict-Report wird nach jedem Lauf gespeichert:
  - `reports/strict_quality_report.json`
