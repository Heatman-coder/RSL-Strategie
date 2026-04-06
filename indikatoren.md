# Indikatoren und Kennzahlen (Excel-Outputs)

Dieses Dokument erklaert alle Spalten aus den Excel-Sheets (`main`, `etf_summary`, `sector_summary`, `industry_summary`).
Ziel: klare Bedeutung, Interpretation und Einsatz in der Analyse.

## Sheet: main

- `RSL-Rang`: Rang nach RSL (1 = bester Wert). Direkte Top-25%-Grenze fuer HOLD/WARN/SELL.
- `St`: Kurzstatus im `main`-Sheet (`D` = Depot, `K` = Kandidat, `W` = Watchlist).
- `Neu?`: `JA` wenn die Aktie neu im Universum ist (First-Seen-Logik).
- `Erfasst seit`: Datum, an dem der Wert erstmals gesehen wurde.
- `MktCap-Rang`: Rang nach Market Cap innerhalb des Universums.
- `Orig. Ticker`: Original-Ticker aus der ETF-Liste (vor Mapping).
- `Ticker`: Yahoo-Ticker, der fuer den Datenabruf genutzt wird.
- `Lk`: Linktyp im `main`-Sheet (`D` = direkter Yahoo-Link, `S` = Such-Fallback fuer problematische Suffix-Ticker).
- `Name`: Aktienname.
- `Sektor`: Sektor (normalisiert).
- `Branche`: Branche (aus Yahoo Info Cache).
- `Land`: Land/Region des Emittenten.
- `ETFs`: Liste der ETFs, in denen der Wert enthalten ist.
- `RSL`: Relative Staerke (Kurs / SMA). Hoeher = staerkerer Trend.
- `RSL-Trend`: Richtung des 1-Wochen-Trends (UP/DN/FLAT).
- `RSL 1W Diff`: 1-Wochen-Aenderung der RSL (numerischer Delta-Wert).
- `Mom Cluster`: Bucket-Cluster auf Basis 12/6/3M Momentum + Accel (z. B. 2222 = durchgehend stark).
- `Mom 12M`: 12-Monats-Momentum (Kurs vs. Kurs vor 12M; 0,20 = +20%).
- `Mom 6M`: 6-Monats-Momentum (Kurs vs. Kurs vor 6M).
- `Mom 3M`: 3-Monats-Momentum (Kurs vs. Kurs vor 3M).
- `Mom Score`: Gewichteter Momentum-Score (12/6/3M Gewichte ueber CONFIG steuerbar).
- `Mom Vol 3M`: Annualisierte Volatilitaet aus den letzten ~3 Monaten (je hoeher, desto volatiler).
- `Mom Score adj`: Momentum-Score / Volatilitaet (Risiko-adjustierter Trend).
- `Mom Accel`: Momentum-Beschleunigung (3M minus 12M; falls 12M fehlt, 3M minus 6M).
- `Kurs`: Aktueller Kurs (normalisiert in EUR).
- `SMA`: Gleitender Durchschnitt (Default 130 Tage).
- `ATR`: Average True Range als Volatilitaetsmass (intern fuer Kauf-/Verkaufs-Anhaltspunkte genutzt).
- `ATR Buy`: Kauf-Anhaltspunkt auf ATR-Basis (unter dem aktuellen Kurs).
- `ATR Sell`: Verkauf-Anhaltspunkt auf ATR-Basis (ueber dem aktuellen Kurs). Formel: `Kurs + ATR * Exit_Multiplier`.
- `Listing Umsatz 20T (Mio EUR)`: 20-Tage Durchschnittsumsatz des konkreten Yahoo-Listings in Mio EUR. Gut als Listing-Hinweis, aber nicht als firmenweite Liquiditaet.
- `Primary Liquidity 20T (Mio EUR)`: abgeleitete Liquiditaet des bevorzugten verknuepften Listings derselben Firma (ISIN, sonst Name). Diese Kennzahl ist robuster fuer Strategie-Filter als der reine Listing-Umsatz.
- `Peer Spread`: `RSL des Titels - Median RSL seiner Branche`. Positiv = echter Leader innerhalb seiner Peer-Group.
- `Abst. 52W-Hoch %`: prozentualer Abstand des aktuellen Kurses zum 52-Wochen-Hoch. Niedrig = starker, intakter Trend.
- `SMA50`: Kurzfristiger Trend-Status (OK/WARN), basierend auf SMA50-Flag.
- `Trend-Qual.`: Qualitaetsstufe (z. B. STABIL/NORMAL/WACKLIG).
- `Trend-Exzess`: TWSS-Score; zeigt Ueberhitzung/Exzess im Trend.
- `Exzess-Datum`: Datum des letzten TWSS-Exzesses.
- `Tage seit Exzess`: Tage seit dem letzten Exzess.
- `Exzess-Max %`: Maximaler Sprung (%) im Exzessfenster.
- `Trust`: Datenvertrauen (niedrig = vorsichtige Interpretation).
- `Trust-Details`: Kurzflags (G=Gap, L=Liquidity, S=Stale).
- `Ticker-Hyperlink`: Im `main`-Sheet ist der Ticker direkt klickbar; je nach `Lk` fuehrt er direkt zu Yahoo oder ueber einen Such-Fallback.

## Sheet: etf_summary

- `Durchschnitt RSL`: Mittelwert der RSL-Werte aller Titel im ETF.
- `ETF`: ETF-Symbol.
- `ETF Name (voll)`: Voller ETF-Name.
- `Anzahl Werte (analysiert)`: Anzahl der analysierten Titel im ETF.
- `Top-% Schwelle`: Prozent-Schwelle (z. B. 25%).
- `Grenzrang Top-%`: Rang an der Schwelle (z. B. Rang 125 bei 500 Werten).
- `RSL am Grenzrang`: RSL-Wert am Grenzrang.
- `Depot-Ticker (Yahoo)`: Deine gehaltenen Ticker innerhalb des ETF (kommagetrennt).

## Sheet: sector_summary

- `Durchschnitt RSL`: Mittelwert der RSL-Werte im Sektor.
- `Sektor`: Sektor-Name.
- `Anzahl Werte (analysiert)`: Anzahl der analysierten Titel im Sektor.
- `Top-% Schwelle`: Prozent-Schwelle (z. B. 25%).
- `Grenzrang Top-%`: Rang an der Schwelle.
- `RSL am Grenzrang`: RSL-Wert am Grenzrang.
- `Depot-Ticker (Yahoo)`: Deine gehaltenen Ticker innerhalb des Sektors (kommagetrennt).

## Sheet: industry_summary

- `Branche`: Branchen-Name.
- `Sektor (repraesentativ)`: Haufigster Sektor in der Branche.
- `Aktien`: Anzahl Titel in der Branche.
- `Breadth > 1.1`: Anzahl Titel mit RSL > 1.1.
- `Breadth Ratio`: Anteil der Titel mit RSL > 1.1.
- `Strong Breadth > 1.2`: Anzahl Titel mit RSL > 1.2.
- `Strong Breadth Ratio`: Anteil der Titel mit RSL > 1.2.
- `Leader > 1.3`: Anzahl Titel mit RSL > 1.3.
- `Leader Ratio`: Anteil der Titel mit RSL > 1.3.
- `Avg RSL`: Durchschnittliche RSL der Branche.
- `Median RSL`: Median-RSL der Branche.
- `Top RSL`: Hoechster RSL-Wert in der Branche.
- `Score`: Branchen-Score (gewichtete Summe aus Breadth, Avg RSL, Median RSL und Leader Ratio; Gewichte ueber CONFIG steuerbar).
- `Score (vor XW)`: Branchen-Score vor X Wochen (aus Kursdaten abgeleitet, nicht laufabhaengig).
- `Score Trend XW`: Differenz `Score - Score (vor XW)`; zeigt, ob eine Branche relativ staerker wird.
- `Depot-Ticker (Yahoo)`: Deine gehaltenen Ticker in der Branche (kommagetrennt).

## Sheet: cluster_summary

- `Cluster`: Cluster-ID (Bucket-Signatur, z. B. 2222 = 12M/6M/3M/Accel stark).
- `Anzahl`: Anzahl Aktien im Cluster.
- `Avg_Mom12`: Durchschnitt 12M Momentum im Cluster.
- `Avg_Mom6`: Durchschnitt 6M Momentum im Cluster.
- `Avg_Mom3`: Durchschnitt 3M Momentum im Cluster.
- `Avg_Accel`: Durchschnitt Momentum-Beschleunigung.
- `Avg_RSL`: Durchschnitt RSL im Cluster.
- `Cluster Anteil %`: Anteil am Gesamtuniversum (nur mit Momentum-Daten).
- `Score`: Cluster-Score (Gewichte Mom12/Mom6/Accel konfigurierbar).
- `Rank`: Rang nach Score.

## Sheet: integrity_issues

- `drop_reasons`: Grund, warum ein Ticker aus der Hauptliste entfernt wurde (z.B. Kurs-Lücken, Skalierungsfehler oder extreme RSL-Ausreißer).
- `raw_rsl`: Der ursprüngliche (potenziell fehlerhafte) RSL-Wert vor dem Filter.

## Praktische Interpretation (Kurzregeln)

- `RSL > 1.0` bedeutet Kurs ueber SMA und damit Trendstaerke.
- `RSL am Grenzrang` zeigt die minimale RSL, um in den Top-% zu bleiben.
- `Durchschnitt RSL` hilft, die Trendqualitaet eines ETF/Sektors zu vergleichen.
- `Trust < 2` = vorsichtig interpretieren (Datenqualitaet).
- `Trend-Exzess` hoch + `Exzess-Max %` hoch = Risiko fuer Ruecksetzer.

## Kandidaten-Scoring (Praxis, wie Profis es nutzen)

- **Institutional Engine (v2)**: Das System nutzt nun ein Multi-Faktor-Modell (Alpha vs. Risk).
  - **Alpha**: Momentum, Beschleunigung (Accel) und Peer-Spread (Vorsprung zur Branche).
  - **Risk**: Drawdown-Tiefe und Volatilität werden über Z-Scores sektor-neutral gewichtet.
  - **Orthogonalisierung**: Mathematische Bereinigung von Signalen, damit z.B. "Accel" nur den Teil des Trends bewertet, der nicht schon im normalen Momentum enthalten ist.
- **Industry-Neutralisierung**: Vergleicht Aktien nur innerhalb ihrer Sektoren (Sector-Neutral Z-Score), um Branchen-Bias zu vermeiden.
- **Min Avg-Umsatz**: filtert illiquide Titel; erhoeht Umsetzbarkeit.
- **Min Trust Score**: filtert schlechte Datenqualitaet (2 = ok, 3 = sauber).
- **Score-Minimum**: schneidet schwaches Rauschen ab.
- **Keine neuen Kaeufe bei Markt SCHWACH**: blockiert neue Kandidaten in schwachem Gesamtmarkt und reduziert Momentum-Crash-Risiko.
- **Max Aktien pro Branche**: echte Zusatzdiversifikation. `1` ist sinnvoll; `2` waere bei `max 2 pro Sektor` oft redundant.
- **Peer-Spread**: bevorzugt Titel, die nicht nur in einer starken Branche liegen, sondern ihre Branche selbst schlagen.
- **Max Abstand zum 52W-Hoch**: zwingt Kaufkandidaten zu intakten Trends statt tiefer Rebounds.

## Profi-Workflow (kurz)

1. **Universum stabil halten** (gleiche ETFs, keine staendigen Aenderungen).
2. **Basislauf mit Defaults**, dann immer nur **einen Parameter** pro Run aendern.
3. **Stabilitaet testen**: gleiche Signale ueber mehrere Laeufe? Dann gut.
4. **Turnover beobachten**: wenn zu viele Wechsel, Gewicht 12M hoch oder Accel aus.
5. **Risikolevel steuern**: Vol-Adjustierung an + Min-Volumen rauf.

## Momentum-Cluster (Praxis)

- **Top-Cluster filtern**: Fokussiert die Kandidatenauswahl auf die Aktien mit den staerksten und gesuendesten Trendmustern.
- **Min Cluster-Groesse**: Reduziert Rauschen. Zu kleine Cluster sind oft statistisch nicht signifikant und koennen instabil sein.
- **Interpretation (Charakter des Trends)**: Cluster sind thematische Momentum-Gruppen, nicht Branchen. Sie helfen, die Art des Trends zu verstehen:
  - `2222`: Der "High-Flyer". Stark auf allen Zeitebenen und beschleunigt noch. Sehr dynamisch, aber potenziell ueberhitzt.
  - `2220`: Der "stabile Leader". Etablierter, starker Trend, dessen Beschleunigung aber nachgelassen hat. Oft verlaesslicher.
  - `2122`: Der "Turnaround-Kandidat". Hatte eine Pause (neutrale 6M-Staerke) und beschleunigt jetzt wieder stark. Sehr interessant fuer neue Trendwellen.
  - `2211`: Ein solider, aber nicht mehr explosiver Aufwaertstrend.
- **Strategie-Anpassung**: Ueber die `cluster_score_w_*`-Gewichte in den Einstellungen kann man steuern, welche Art von Cluster (z.B. mehr Beschleunigung vs. mehr Langfrist-Staerke) im Ranking bevorzugt wird.

## Presets (Empfehlung)

- **Standard - Ausgewogen Pro (Default)**:
  - Gewichte: 12M/6M/3M = 0.50/0.30/0.20
  - Top-Branchen: 15
  - Branchen-Score-Min: 0.15
  - Top-%-Filter: an, Schwelle 2%
  - Vol-Adjustierung: an
  - Industry-Neutralisierung: an
  - Accel: an (Gewicht 0.15)
  - RSL 1W-Change: an (Gewicht 0.10)
  - Min Trust Score: 3
  - Cluster: 0.50 / 0.25 / 0.25
  - Sinn: robust fuer grosse Universen, aber trotzdem offen fuer neue Trends

- **Defensiv - Qualitaet zuerst**:
  - Gewichte: 12M/6M/3M = 0.60/0.30/0.10
  - Top-Branchen: 12
  - Branchen-Score-Min: 0.18
  - Top-%-Filter: an, Schwelle 1%
  - Vol-Adjustierung: an
  - Industry-Neutralisierung: an
  - Accel: aus
  - RSL 1W-Change: aus
  - Min Avg-Umsatz: 5 Mio EUR
  - Min Trust Score: 3
  - Cluster: 0.60 / 0.25 / 0.15
  - Sinn: ruhiger, selektiver, weniger Fehl-Signale

- **Dynamisch - Fruehe Trends**:
  - Gewichte: 12M/6M/3M = 0.45/0.30/0.25
  - Top-Branchen: 20
  - Branchen-Score-Min: 0.12
  - Top-%-Filter: an, Schwelle 3%
  - Vol-Adjustierung: an
  - Industry-Neutralisierung: an
  - Accel: an (Gewicht 0.20)
  - RSL 1W-Change: an (Gewicht 0.12)
  - Min Trust Score: 2
  - Cluster: 0.45 / 0.25 / 0.30
  - Sinn: fruehe Trendphasen schneller sehen, dafuer mehr Bewegung im Kandidatenfeld
