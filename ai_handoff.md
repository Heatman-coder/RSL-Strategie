# AI / Dev Handoff

## Zweck

Diese Datei ist die operative Übergabe für neue Entwickler oder andere KI-Assistenten.

## Nicht verhandelbare Invarianten

- `top_percent_threshold` ist Kern der Entscheidungslogik.
- `HOLD/WARN/SELL`-Semantik darf nicht implizit geändert werden.
- Multi-Scope-Status muss Gesamt + Sektor + ETF berücksichtigen.
- `ranking_core.apply_standard_rankings` ist die einzige Quelle für RSL- und Mkt-Ränge.
- `run_quality_gate` muss vor Snapshot/Output laufen.
- Snapshot-Rerender darf keinen neuen Download auslösen.

## Aktuelle Modulgrenzen

- `core/ranking.py`
  - `_evaluate_rank`-Logik
  - Zentrales Ranking (`apply_standard_rankings`)
  - Multi-Scope-Status
  - Prozent-/Balkenformatierung
  - Portfolio-Sortierung
- `core/quality_gate.py`
  - Qualitätsreport
  - Strict-Grenzen
  - Quality-Gate-Ausgabe/Abbruch
- `core/summaries.py`
  - ETF-, Sektor- und Industrie-Summaries
- `core/candidate_engine.py`
  - Vektorisierte Faktor-Berechnung (NumPy/SciPy)
  - Orthogonalisierung & Z-Scoring
  - Institutional Alpha/Risk Scoring
- `core/rsl_integrity.py`
  - Statistische Prüfung der Datenintegrität
  - Filterung von Skalenbrüchen und RSL-Glitches
- `core/reporting_excel.py`
  - XLSX-Export, Spaltenbreiten, Row-Highlighting, Hyperlinks
- `core/console_symbols.py`
  - Unicode/ASCII-Symbole für Konsole
- `core/console_ui.py`
  - Dashboard-Rendering, Heatmap-Output, Bestandscheck, Export-Flow
- `core/data_pipeline.py`
  - ETF-Universum laden (Cache + fehlende ETFs nachladen + Deduplizierung)

`final.py` ruft diese Module auf und enthält primär Orchestrierung.

## Sichere Änderungsstrategie

1. Kleine, isolierte Änderungen
2. Vorher/Nachher-Syntaxcheck
3. Core-Regeln mit Tests absichern (`tests/test_core_rules.py`)
4. Bei Logikänderungen Doku aktualisieren (`README`, `docs/*`)

## Test-Strategie

- Bevorzuge Tests direkt gegen `core/*`, nicht nur gegen `final.py`
- `final.py` sollte nur noch dünne Kompatibilitäts-Wrapper und Orchestrierung enthalten

## Typische Fehlerquellen

- Encoding/Unicode-Probleme in der Konsole
- Unbeabsichtigte Änderung von Schwellwertlogik
- Inkonsistente Sortierung zwischen Dashboard/Heatmap/Bestandscheck
- Änderungen am Datenfluss, die Strict-Checks umgehen

## Erweiterungen mit geringem Risiko

- Zusätzliche Konsolenkennzahlen im Strict-Block
- Neue Report-Spalten ohne Änderung der Kernlogik
- Weitere Tests für Grenzfälle

## Erweiterungen mit höherem Risiko

- Änderungen an `core/ranking.py`, `core/quality_gate.py` Kernfunktionen
- Eingriffe in Cache-Strategie oder Fallback-Download
- Refactoring ohne Regressionstests

## Vor jedem Release prüfen

1. Start in beiden Modi (neu / Snapshot-Rerender)
2. Strict-Block sichtbar und plausibel
3. Excel mit 3 Sheets erzeugt
4. `failed/young/strict` Artefakte geschrieben
5. Keine kaputten Konsolenzeichen im Zielterminal
