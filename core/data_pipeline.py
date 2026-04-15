import os
import time
import io
import re
import requests # type: ignore
from collections import defaultdict
from typing import Any, Callable, Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
from core.entity_matching import normalize_name_for_dedup

# Falls yfinance in get_history_batch None zurückgibt
def _safe_get_columns(data):
    return getattr(data, 'columns', None)

def _get_ticker_priority(ticker: Any) -> int:
    t = str(ticker or "").strip().upper()
    # 1. Prio: US-Original (kein Punkt) oder Native Maerkte (z.B. .OL fuer Norwegen)
    native_suffixes = [".T", ".L", ".HK", ".TO", ".AX", ".OL", ".ST", ".CO", ".KS", ".KQ", ".TW", ".TWO", ".SS", ".SZ", ".SN"]
    if any(t.endswith(s) for s in native_suffixes):
        return 1
    if t and "." not in t: return 2
    # 2. Prio: Xetra (.DE)
    if t.endswith(".DE"): return 10
    # 3. Prio: Andere deutsche Regionalboersen
    if t.endswith((".F", ".SG", ".DU", ".BE", ".HM", ".MU")):
        return 100
    return 200

def _merge_unique_source_values(series: pd.Series) -> str:
    values = []
    for value in series:
        for part in str(value or "").split(","):
            cleaned = part.strip()
            if cleaned and cleaned.lower() != "nan":
                values.append(cleaned)
    return ", ".join(sorted(set(values)))


def _has_valid_isin_data(df: pd.DataFrame) -> bool:
    if "ISIN" not in df.columns:
        return False
    try:
        cleaned = df["ISIN"].astype(str).str.strip().str.upper()
    except Exception:
        return False
    valid = cleaned[~cleaned.isin(["", "NAN", "NONE"])]
    if valid.empty:
        return False
    # Toleranterer Check: 2 Buchstaben + 10 alphanumerische Zeichen
    return bool((valid.str.match(r'^[A-Z]{2}[A-Z0-9]{10}$')).any())


def load_selected_etf_universe(
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    logger: Any,
    download_ishares_csv: Callable[..., pd.DataFrame],
    normalize_sector_name: Callable[[Any], str],
    print_fn: Callable[[str], None] = print,
    progress_fn: Any = None,
) -> Tuple[pd.DataFrame, int]:
    master_df = pd.DataFrame()
    etf_cache_file = str(config.get("etf_cache_file", "") or "")
    cache_hours = float(config.get("etf_cache_duration_hours", 168) or 168)
    base_url_template = str(config.get("base_url_template", "") or "")

    # 1) Cache laden (falls vorhanden und frisch)
    if etf_cache_file and os.path.exists(etf_cache_file):
        try:
            cache_time = os.path.getmtime(etf_cache_file)
            if (time.time() - cache_time) / 3600 < cache_hours:
                print_fn(f"\nLade ETF-Daten aus Cache (gueltig fuer {int(cache_hours)}h)...")
                master_df = pd.read_pickle(etf_cache_file)
            else:
                print_fn("\nETF-Cache abgelaufen. Lade neu...")
        except Exception as e:
            logger.warning(f"Konnte ETF-Cache nicht laden: {e}")

    # 2) Pruefen, welche ETFs fehlen
    if not master_df.empty and "Source_ETF" in master_df.columns:
        existing_sources = set(master_df["Source_ETF"].astype(str).unique())
    else:
        existing_sources = set()
    missing_syms = [sym for sym in selected_syms if sym not in existing_sources]

    # 3) Fehlende ETFs nachladen
    if missing_syms:
        print_fn(f"\nLade {len(missing_syms)} fehlende ETFs nach: {', '.join(missing_syms)}")
        new_data_df = pd.DataFrame()
        iterator = missing_syms
        if progress_fn:
            with progress_fn(total=len(missing_syms), desc="ETF Holdings") as pbar:
                for sym in iterator:
                    if sym not in etf_options:
                        logger.warning(f"Keine Daten fuer {sym} gefunden, ueberspringe.")
                        pbar.update(1)
                        continue
                    etf_data = etf_options[sym]
                    csv_url = base_url_template.format(id=etf_data["id"], slug=etf_data["slug"], symbol=sym)
                    try:
                        current_df = download_ishares_csv(csv_url, log_label=False)
                        if current_df is not None and not current_df.empty:
                            current_df["Source_ETF"] = sym
                            new_data_df = pd.concat([new_data_df, current_df], ignore_index=True)
                        else:
                            logger.warning(f"Keine Daten fuer {sym} erhalten.")
                    except Exception as e:
                        logger.error(f"Fehler beim Laden von {sym}: {e}")
                    pbar.update(1)
        else:
            for sym in iterator:
                if sym not in etf_options:
                    logger.warning(f"Keine Daten fuer {sym} gefunden, ueberspringe.")
                    continue
                etf_data = etf_options[sym]
                csv_url = base_url_template.format(id=etf_data["id"], slug=etf_data["slug"], symbol=sym)
                try:
                    current_df = download_ishares_csv(csv_url)
                    if current_df is not None and not current_df.empty:
                        current_df["Source_ETF"] = sym
                        new_data_df = pd.concat([new_data_df, current_df], ignore_index=True)
                    else:
                        logger.warning(f"Keine Daten fuer {sym} erhalten.")
                except Exception as e:
                    logger.error(f"Fehler beim Laden von {sym}: {e}")

        # 4) Zusammenfuehren und Cache speichern
        if not new_data_df.empty:
            master_df = pd.concat([master_df, new_data_df], ignore_index=True)
            if etf_cache_file:
                try:
                    master_df.to_pickle(etf_cache_file)
                    logger.info("ETF-Cache aktualisiert und gespeichert.")
                except Exception as e:
                    logger.error(f"Konnte ETF-Cache nicht speichern: {e}")

    if master_df.empty:
        logger.error("Keine Daten geladen. Abbruch.")
        return pd.DataFrame(), 0

    # 5) Auf aktuelle ETF-Auswahl filtern
    if selected_syms:
        if "Source_ETF" not in master_df.columns:
            logger.error("Cache-/Downloaddaten enthalten keine Spalte 'Source_ETF'.")
            return pd.DataFrame(), 0
        all_etf_keys = list(etf_options.keys())
        is_all_selected = len(all_etf_keys) > 0 and len(selected_syms) == len(all_etf_keys) and set(selected_syms) == set(all_etf_keys)
        selection_text = "Alle" if is_all_selected else ', '.join(selected_syms)

        logger.info(f"Filtere Daten auf Auswahl: {selection_text}")
        master_df = master_df[master_df["Source_ETF"].isin(selected_syms)]
        if master_df.empty:
            logger.error("Keine Daten fuer die gewaehlte Auswahl im Cache gefunden.")
            return pd.DataFrame(), 0

    # Spalten-Normalisierung: Symbol -> Ticker, Isin -> ISIN
    rename_map = {
        "Symbol": "Ticker",
        "Isin": "ISIN"
    }
    master_df = master_df.rename(columns={k: v for k, v in rename_map.items() if k in master_df.columns})

    if "Sector" in master_df.columns:
        master_df["Sector"] = master_df["Sector"].apply(normalize_sector_name)

    if "Location" in master_df.columns and "Land" not in master_df.columns:
        master_df = master_df.rename(columns={"Location": "Land"})

    if "Listing_Source" not in master_df.columns:
        master_df["Listing_Source"] = ""

    if "Ticker" not in master_df.columns or "Source_ETF" not in master_df.columns:
        logger.error("Cache-/Downloaddaten enthalten nicht alle benoetigten Spalten (Ticker, Source_ETF).")
        return pd.DataFrame(), 0

    def _normalize_name_for_dedup(name: Any) -> str:
        return normalize_name_for_dedup(name)

    # Deduplizierung nach ISIN (falls vorhanden), sonst Name, sonst Ticker
    # Wir fassen die Quellen (Source_ETF) fuer dieselbe ISIN zusammen
    if _has_valid_isin_data(master_df):
        # ISIN-Spalte vereinheitlichen und Platzhalter entfernen
        master_df["ISIN"] = master_df["ISIN"].astype(str).str.strip().str.upper()
        master_df.loc[master_df["ISIN"].isin(["", "NAN", "NONE", "0", "NULL"]), "ISIN"] = np.nan
        master_df["_dedup_id"] = master_df["ISIN"].fillna(master_df["Ticker"]).astype(str).str.strip().str.upper()
    elif "Name" in master_df.columns:
        master_df["_norm_name"] = master_df["Name"].apply(_normalize_name_for_dedup)
        # Nutze Namen als ID, wenn er laenger als 4 Zeichen ist
        master_df["_dedup_id"] = master_df["_norm_name"].where(master_df["_norm_name"].str.len() > 4, master_df["Ticker"].astype(str).str.strip().str.upper())
    else:
        master_df["_dedup_id"] = master_df["Ticker"].astype(str).str.strip().str.upper()
    
    # Metadaten innerhalb der Gruppen vererben (Sektor/Land von Haupt-Tickern bevorzugen)
    propagate_cols = ['Sector', 'Industry', 'Land']
    if "ISIN" in master_df.columns:
        propagate_cols.append('ISIN')
    for col in propagate_cols:
        if col in master_df.columns:
            master_df[col] = master_df.groupby('_dedup_id')[col].transform(
                lambda x: x.replace(['Unknown', 'nan', 'NAN', 'None', '', 'nan'], np.nan).ffill().bfill()
            ).fillna(master_df[col])

    source_map = master_df.groupby("_dedup_id")["Source_ETF"].apply(_merge_unique_source_values).to_dict()
    listing_map = master_df.groupby("_dedup_id")["Listing_Source"].apply(_merge_unique_source_values).to_dict()

    # Deduplizierung mit Priorität (Native/Home-Exchanges > Frankfurt)
    master_df["_prio"] = master_df["Ticker"].apply(_get_ticker_priority)
    master_df = master_df.sort_values("_prio").drop_duplicates(subset=["_dedup_id"], keep="first").copy()
    master_df["Source_ETF"] = master_df["_dedup_id"].map(source_map)
    master_df["Listing_Source"] = master_df["_dedup_id"].map(listing_map)
    master_df.drop(columns=["_dedup_id", "_prio"], errors="ignore", inplace=True)

    final_rows = len(master_df)
    logger.info(f"Pool-Groesse: {final_rows} (Dedupliziert)")
    return master_df, final_rows

def load_exchange_universe(
    config: Dict[str, Any],
    logger: Any,
    normalize_sector_name: Callable[[Any], str]
) -> pd.DataFrame:
    """Laedt Instrumentenlisten direkt von der Deutschen Boerse (Xetra/Frankfurt)."""
    cache_file = config.get('exchange_cache_file')
    cache_hours = float(config.get('etf_cache_duration_hours', 168))

    if cache_file and os.path.exists(cache_file):
        mtime = os.path.getmtime(cache_file)
        if (time.time() - mtime) / 3600 < cache_hours:
            try:
                logger.info("Lade Exchange-Daten aus Cache...")
                return pd.read_pickle(cache_file)
            except Exception as e:
                logger.warning(f"Fehler beim Laden des Exchange-Cache: {e}")

    sources = [
        (config.get('url_xetra_instruments'), ".DE", "XETRA"),
        (config.get('url_frankfurt_instruments'), ".F", "FRA")
    ]
    
    combined_df = pd.DataFrame()
    headers = {'User-Agent': 'Mozilla/5.0'}

    for url, suffix, label in sources:
        if not url: continue
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            # Robustes CSV-Parsing: Header-Suche statt festem Skip
            text = response.text
            lines = text.splitlines()
            header_idx = 0
            for idx, line in enumerate(lines[:15]):
                if "ISIN" in line.upper() and ("MNEMONIC" in line.upper() or "TICKER" in line.upper()):
                    header_idx = idx
                    break
            
            df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])), sep=None, engine='python')
            df.columns = df.columns.str.strip()
            
            # 1. Filter: Nur Common Stocks (Aktien) wie in beispiel.py
            type_col = next((c for c in ['Instrument Type', 'Product Type'] if c in df.columns), None)
            if type_col:
                df = df[df[type_col].astype(str).str.strip().str.upper() == 'CS']

            if 'ISIN' in df.columns and 'Mnemonic' in df.columns:
                df = df.dropna(subset=['ISIN', 'Mnemonic']).copy()
                df['Ticker'] = df['Mnemonic'].astype(str).str.strip() + suffix
                df['Source_ETF'] = ""
                df['Listing_Source'] = label
                # Spalten auf Projekt-Standard mappen
                df = df.rename(columns={
                    'Instrument Name': 'Name',
                    'Long Name': 'Name',
                    'Country': 'Land'
                })
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                logger.info(f"Exchange {label} geladen: {len(df)} Aktien")
        except Exception as e:
            logger.error(f"Fehler beim Laden von Exchange-Daten ({url}): {e}")

    if combined_df.empty:
        return pd.DataFrame()

    # Wichtige Felder sicherstellen
    if 'Sector' not in combined_df.columns:
        combined_df['Sector'] = 'Unknown'
    else:
        combined_df['Sector'] = combined_df['Sector'].fillna('Unknown').apply(normalize_sector_name)
        
    if 'Market Value' not in combined_df.columns:
        combined_df['Market Value'] = 0.0

    # Priorisierung fuer Exchange-Daten (Xetra bevorzugt vor Frankfurt)
    combined_df['priority'] = combined_df['Ticker'].apply(_get_ticker_priority)
    # Innerhalb der Exchange-Integration deduplizieren wir nach ISIN
    combined_df = combined_df.sort_values('priority').drop_duplicates(subset=['ISIN'], keep='first').copy()
    combined_df.drop(columns=['priority'], errors='ignore', inplace=True)

    keep_cols = ['Ticker', 'Name', 'ISIN', 'Sector', 'Land', 'Market Value', 'Source_ETF', 'Listing_Source']
    available_cols = [c for c in keep_cols if c in combined_df.columns]
    result = combined_df[available_cols]

    if cache_file:
        try:
            result.to_pickle(cache_file)
            logger.info("Exchange-Cache aktualisiert und gespeichert.")
        except Exception as e:
            logger.warning(f"Konnte Exchange-Cache nicht speichern: {e}")

    return result

def perform_final_deduplication(results: List[Any]) -> List[Any]:
    """Führt eine globale ISIN- und Namens-Deduplizierung am Ende der Pipeline durch."""
    if results is None or len(results) == 0: return []
    from core import final_support as support

    # STUFE 1: Nach Yahoo-Ticker gruppieren
    ticker_groups = defaultdict(list)
    for s in results:
        ticker_groups[str(s.yahoo_symbol).strip().upper()].append(s)

    dedup_ticker = []
    for sym, group in ticker_groups.items():
        if len(group) == 1:
            dedup_ticker.append(group[0])
            continue
        group.sort(key=lambda s: (
            -int(getattr(s, 'trust_score', 0)),
            -len(str(getattr(s, 'isin', "") or "")),
            -len(str(getattr(s, 'source_etf', "") or ""))
        ))
        best = group[0]
        all_sources = set()
        all_listings = set()
        for s in group:
            all_sources.update(support.parse_tokens(getattr(s, 'source_etf', "")))
            all_listings.update(support.parse_tokens(getattr(s, 'listing_source', "")))
        best.source_etf = ", ".join(sorted(all_sources))
        best.listing_source = ", ".join(sorted(all_listings))
        dedup_ticker.append(best)

    # STUFE 2: Nach ISIN/Name gruppieren
    final_groups = defaultdict(list)
    for s in dedup_ticker:
        isin = str(getattr(s, 'isin', "") or "").strip().upper()
        if len(isin) > 5 and isin not in ("NAN", "NONE"):
            key = f"ISIN_{isin}"
        else:
            name_key = support.normalize_name_for_dedup_key(getattr(s, 'name', ""))
            key = f"NAME_{name_key}" if len(name_key) > 4 else f"TICKER_{s.yahoo_symbol}"
        final_groups[key].append(s)

    final_deduped = []
    for key, group in final_groups.items():
        if len(group) == 1:
            final_deduped.append(group[0])
            continue
        # Sortierung: ETF-Herkunft (Prio 1) -> Trust-Score -> Liquiditaet -> Ticker-Prioritaet
        # Nutze primary_liquidity_eur falls vorhanden, sonst avg_volume_eur
        group.sort(key=lambda s: (
            0 if str(getattr(s, 'source_etf', '')).strip() and getattr(s, 'source_etf') != 'MANUAL' else 1,
            -int(getattr(s, 'trust_score', 0)), 
            -float(getattr(s, 'primary_liquidity_eur', getattr(s, 'avg_volume_eur', 0.0))), 
            support.stock_history_priority_score(s, {}))
        )
        best = group[0]
        all_sources = set()
        all_listings = set()
        for s in group:
            all_sources.update(support.parse_tokens(getattr(s, 'source_etf', "")))
            all_listings.update(support.parse_tokens(getattr(s, 'listing_source', "")))
        
        best.source_etf = ", ".join(sorted(all_sources))
        best.listing_source = ", ".join(sorted(all_listings))
        
        final_deduped.append(best)

    return final_deduped
