﻿# Konfiguration

Alle Laufparameter liegen zentral in `CONFIG` in `final.py`.

Hinweis: Ein Teil der Parameter wird zur Laufzeit aus `user_settings.json` uebernommen und ueberschreibt die Defaults aus `CONFIG`.

## Automatische Optimierung

- Das System protokolliert Rate-Limits in `run_stats.json`.
- Beim nächsten Start werden `batch_sleep` und `info_fetch_delay_s` automatisch angepasst (langsamer bei Fehlern, schneller bei Erfolg).
- Währungskurse werden beim Start live über Yahoo (`EURUSD=X` etc.) aktualisiert.

## Wichtigste Betriebsparameter

- `batch_size`, `max_workers`: Parallelitaet bei Datenabrufen
- `cache_duration_hours`: Gueltigkeit History-Cache (Stunden)
- `etf_cache_duration_hours`: Gueltigkeit ETF-Holdings-Cache (Stunden)
- `batch_sleep_min_s`, `batch_sleep_max_s`: Pausen zwischen Batch-Downloads
- `required_days`, `sma_length`, `sma_short_length`: Mindesthistorie/Indikatorfenster
- `top_percent_threshold`: Top-Schwelle (z. B. `0.25` fuer Top 25%)

## Heatmap-Schwellen

- `heatmap_warn_percent`: Marker/Farbgrenze fuer Warnbereich (Default `20.0`)
- `heatmap_full_percent`: Voll-/Kritischgrenze der Balken (Default `25.0`)

Die Werte koennen im Startmenue unter `4 Einstellungen` angepasst werden und
werden in `user_settings.json` gespeichert.

Das UI ist jetzt zweistufig:

- Alltag / Schnellzugriff: gruppierte Hauptbereiche fuer Strategie, Kandidaten, Daten und Anzeige
- Expertenmodus: vollstaendige alte Detailansicht fuer Feintuning
- In der Alltagsansicht gibt es zusaetzlich `Strategieprofile & Presets` und `Nur Abweichungen vom Standard anzeigen`

Alltagsansicht:

- zeigt nur die groessten strategischen Hebel
- versteckt Feintuning wie Cluster-Gewichte, Branchencaps, Batch-Sleeps oder einzelne Strict-Grenzen
- der Expertenmodus bleibt vollstaendig erhalten

## Standard und Preset-Pakete

Der Standard ist bewusst auf ein grosses Aktienuniversum und ein kleines Fokus-Depot ausgelegt.

**Default: `Standard - Ausgewogen Pro`**

Warum dieser Standard:

- `Top-Branchen = 15`: breit genug fuer ein grosses Universum, ohne das Branchenfilter zu eng zu machen
- `Branchen-Score-Min = 0.15`: laesst neue Trends durch, schneidet aber offensichtliches Rauschen ab
- `Breadth-Min = 0.25`: verhindert fragile Branchen mit nur wenigen starken Aktien
- `Top-% = 2%`: Kaufkandidaten bleiben qualitativ hoch; bei vielen tausend Aktien ist 25% dafuer zu weit
- `Trust Score >= 3`: fuer Kaufvorschlaege ist saubere Datenqualitaet wichtiger als maximale Menge
- `Accel = 0.15` und `RSL 1W = 0.10`: frische Trends werden erfasst, dominieren das Ranking aber nicht
- `Cluster 0.50 / 0.25 / 0.25`: Langfrist-Momentum bleibt Kern, Beschleunigung wird sinnvoll mitbewertet

Verfuegbare Pakete:

- `Standard - Ausgewogen Pro`: robuster Alltag fuer grosses Universum und 5er-Depot
- `Defensiv - Qualitaet zuerst`: weniger Turnover, hoehere Daten- und Liquiditaetsqualitaet
- `Dynamisch - Fruehe Trends`: offener fuer neue Trendwellen, dafuer beweglicher

Die Pakete aendern mehrere Parameter gemeinsam, damit keine unstimmigen Mischungen entstehen.

## Eigene Strategieprofile

Zusaetzlich zu den 3 festen Paketen koennen eigene Strategieprofile gespeichert werden.

Ziel:

- neue Ideen testen, ohne die Standardpakete zu veraendern
- verschiedene Parameterkombinationen sauber benennen
- spaeter im Report nachvollziehen, welches Profil aktiv war

Speicherort:

- `strategy_profiles.json`

Verhalten:

- Standardpakete bleiben unveraenderlich
- eigene Profile koennen im Settings-Menue geladen, neu gespeichert, ueberschrieben, dupliziert und geloescht werden
- wenn Einstellungen von allen Profilen abweichen, wird `Manuelle Kombination` angezeigt
- eigene Profile koennen zusaetzlich Begruendung, Marktumfeld, Einsatz und Review-Trigger enthalten

## Datenqualitaetsparameter

- `min_liquidity`
- `stale_window`, `min_unique_ratio`, `min_nonzero_ratio`
- `max_flat_days`, `max_gap_percent`
- `twss_decay_days`
- `info_fetch_quiet`, `rate_limit_delay_*`, `rate_limit_log_every`

Diese Werte beeinflussen Flags (`flag_stale`, `flag_gap`, `flag_liquidity`) und `trust_score`.

## Strict Mode

- `strict_mode`: `True`/`False`
- `strict_min_analyzed_stocks`
- `strict_min_coverage_ratio`
- `strict_max_failed_ratio`
- `strict_max_young_ratio`
- `strict_max_critical_drop_ratio`
- `strict_max_stale_warn_ratio`
- `strict_max_gap_warn_ratio`
- `strict_max_liquidity_warn_ratio`
- `strict_max_low_trust_ratio`
- `strict_min_portfolio_coverage_ratio`
- `strict_max_invalid_numeric_count`
- `strict_max_duplicate_symbols`

Verhalten:
- Wenn `strict_mode=True` und eine Grenze verletzt wird, wird der Lauf mit Fehler beendet.
- Ein Report wird immer geschrieben: `reports/strict_quality_report.json`.

## Pfadparameter (Auszug)

- `result_file_prefix`: Basisname fuer Reports
- `history_cache_file`: History-Cache JSON
- `etf_cache_file`: ETF-Holdings-Cache PKL
- `portfolio_file`: aktuelles Portfolio JSON
- `last_analysis_snapshot_file`: Snapshot fuer Rerender
- `last_console_output_file`: letzter Konsolenlog
- `user_settings_file`: persistente UX-Einstellungen
- `strategy_profiles_file`: gespeicherte eigene Strategieprofile

## Kandidaten-Scoring

Wichtige Parameter:

- `candidate_use_momentum_score`
- `candidate_use_vol_adjust`
- `candidate_use_industry_neutral`
- `candidate_use_accel`
- `candidate_use_rsl_change_1w`
- `candidate_min_avg_volume_eur`
- `candidate_min_trust_score`
- `candidate_score_min`
- `candidate_require_top_percent`
- `candidate_top_percent_threshold`
- `candidate_block_new_buys_in_weak_regime`
- `candidate_max_stocks_per_industry`
- `candidate_use_peer_spread`
- `candidate_peer_spread_weight`
- `candidate_max_distance_52w_high_pct`

Diese Logik lebt primär in `core/candidate_engine.py`.

## Konsolen-Encoding

Optional kann ASCII erzwungen werden:

```powershell
$env:RSL_ASCII_CONSOLE='1'
python final.py
```

Damit werden problematische Unicode-Symbole in manchen Terminals vermieden.
