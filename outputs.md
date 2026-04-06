# Outputs

## Hauptausgaben

- `reports/global_rsl_<jahr>_kw<kw>_<combo>.xlsx`
  - Sheet `main`
  - Sheet `raw_data`
  - Sheet `etf_summary`
  - Sheet `sector_summary`
  - Sheet `industry_summary`
  - Sheet `cluster_summary`
  - Sheet `indikatoren`
  - Sheet `config_snapshot`
    - enthaelt auch das aktive Strategieprofil inklusive Typ, Zweck und Begruendung

Fallback bei Excel-Problem:
- CSV-Dateien im Report-Verzeichnis

## QualitÃ¤ts- und Diagnoseausgaben

- `reports/strict_quality_report.json`
  - Metriken, Ratios, Strict-Status, Fail-GrÃ¼nde
- `reports/failed_tickers.json/.csv/.log`
- `reports/young_tickers.json/.csv/.log`
- `reports/dropped_tickers.log`

## Laufzeitspeicher

- `reports/last_analysis_snapshot.json`
  - Datenstand fÃ¼r Rerender ohne Download
- `reports/last_console_output.txt`
  - letzter Konsolenoutput
- `last_run_config.json`
  - letzter Lauf inkl. aktivem Strategieprofil

- `strategy_profiles.json`
  - gespeicherte benutzerdefinierte Strategieprofile fuer Tests und Varianten

## Caches

- `history_cache.json`
  - Yahoo-Historien (zeitlich begrenzt)
- `etf_holdings_cache.pkl`
  - ETF-Holdings (zeitlich begrenzt)
- `first_seen_cache.json`
  - Erstsichtungsdatum je Symbol

## Wie man die wichtigsten Outputs liest

1. `strict_quality_report.json`
   - Erstes Artefakt prÃ¼fen: `strict_status`
   - Bei `FAIL`: `strict_failures` lesen

2. `main`-Sheet
   - Basisliste aller analysierten Werte mit Ranking/Qualitaetsfeldern
   - Alltagsrelevante Kernspalten bleiben sichtbar
   - Experten-/Diagnosefelder wie detaillierte Momentum-Unterkomponenten oder Exzess-Rohfelder sind standardmaessig ausgeblendet und koennen bei Bedarf in Excel eingeblendet werden
   - Basisliste aller analysierten Werte mit Ranking/QualitÃ¤tsfeldern
   - Vollstaendige technische Rohsicht zusaetzlich im Sheet `raw_data`

3. `etf_summary` und `sector_summary`
   - Grenzrang und Durchschnitts-RSL je ETF/Sektor plus Gesamtzeile

4. `industry_summary`
   - Branchen-Breadth, Leader, Median/Avg/Top-RSL, Score und Trend gegen historische RSL-Basis

5. `cluster_summary`
   - Momentum-Cluster mit Groesse, Anteil und Cluster-Score

6. `failed_tickers`/`young_tickers`
   - UrsachenhÃ¤ufigkeit und wiederkehrende Datenprobleme
