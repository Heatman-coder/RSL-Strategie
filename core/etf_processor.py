"""
ETF-Verarbeitungsmodul für das yfinance-Projekt.
Enthält Funktionen für ETF-Datenverarbeitung, Kandidatengenerierung und Deduplizierung.
"""
import re
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd


def sanitize_ticker_symbol(raw: str) -> str:
    """Bereinigt ein Ticker-Symbol.
    
    Args:
        raw: Rohes Ticker-Symbol
        
    Returns:
        Bereinigtes Ticker-Symbol
    """
    if raw is None:
        return ""
    s = str(raw).upper().strip()
    s = s.replace(" ", "-").replace("_", "-").replace("/", "-")
    s = s.replace('"', "").replace("'", "")
    s = re.sub(r"[^A-Z0-9.\-]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"\.{2,}", ".", s)
    return s.strip("-.")


_TICKER_SANITY_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")


def is_plausible_ticker(symbol: str) -> bool:
    """Prüft, ob ein Ticker-Symbol plausibel ist.
    
    Args:
        symbol: Ticker-Symbol
        
    Returns:
        True wenn plausibel, sonst False
    """
    if not symbol:
        return False
    if symbol.count('.') > 2:
        return False
    return bool(_TICKER_SANITY_RE.match(symbol))


def generate_candidates(
    original: str,
    land: str,
    exchange: str,
    unsupported_exchanges: List[str],
    exchange_suffix_map: Dict[str, str],
    location_suffix_map: Dict[str, str]
) -> List[str]:
    """Generiert Kandidaten-Ticker für ein gegebenes Original.
    
    Args:
        original: Originaler Ticker
        land: Land des Tickers
        Exchange: Börse
        unsupported_exchanges: Liste nicht unterstützter Börsen
        exchange_suffix_map: Mapping Börse -> Suffix
        location_suffix_map: Mapping Land -> Suffix
        
    Returns:
        Liste von Kandidaten-Tickern
    """
    try:
        if exchange in unsupported_exchanges:
            return []
        cands = []
        base = sanitize_ticker_symbol(original)
        if not base:
            return []
        
        # Suffix-Bestimmung (Mapping-basiert)
        determined_suffix = exchange_suffix_map.get(exchange, location_suffix_map.get(land, ''))
        determined_suffix = str(determined_suffix).upper().strip() if determined_suffix else ""
        if determined_suffix and not determined_suffix.startswith('.'):
            determined_suffix = f".{determined_suffix}"
        determined_suffix = re.sub(r"[^A-Z0-9.]", "", determined_suffix)
        
        # Extraktion des Stamm-Tickers (ohne Suffix)
        has_explicit_suffix = "." in base
        ticker_stem = base.split('.')[0] if has_explicit_suffix else base
        
        # Sonderfall: iShares 'UW'/'UN'/'UQ' Suffixe (Nasdaq/NYSE) entfernen
        if re.search(r' (UW|UN|UQ|UA|N|O|P)$', str(original).upper()):
            ticker_stem = str(original).upper().split()[0]
            has_explicit_suffix = False

        # Spezialbehandlung fuer numerische Maerkte (z.B. Japan, HK)
        if ticker_stem.isdigit():
            if 'Hong Kong' in exchange or land == 'Hong Kong':
                ticker_stem, determined_suffix = f"{int(ticker_stem):04d}", '.HK'
            elif 'KOSDAQ' in str(exchange).upper():
                ticker_stem, determined_suffix = ticker_stem.zfill(6), '.KQ'
            elif 'Korea' in exchange or land in ('South Korea', 'Korea (South)'):
                ticker_stem = ticker_stem.zfill(6)
                determined_suffix = determined_suffix or '.KS'
            elif 'Shanghai' in exchange:
                determined_suffix = '.SS'
            elif 'Shenzhen' in exchange:
                determined_suffix = '.SZ'
            elif 'Japan' in exchange or land == 'Japan':
                determined_suffix = '.T'
            # Schutz: Numerische Ticker aus Fernost nicht mit westlichen Suffixen mischen
            if ticker_stem.isdigit() and determined_suffix in ('.DE', '.F', '.L', '.PA'):
                determined_suffix = "" 

            elif land == 'Taiwan':
                determined_suffix = '.TW'
        
        # Kandidaten sammeln
        if has_explicit_suffix:
            cands.append(base) # Original (z.B. 6971.T)
            cands.append(ticker_stem) # Stamm (z.B. 6971)
            if determined_suffix:
                cands.append(f"{ticker_stem}{determined_suffix}") # Korrigierter Suffix
        else:
            if determined_suffix:
                cands.append(f"{ticker_stem}{determined_suffix}")
            cands.append(ticker_stem)
        
        if land == 'China' or 'China' in exchange:
            if determined_suffix == '.SS':
                cands.append(f"{ticker_stem}.SZ")
            elif determined_suffix == '.SZ':
                cands.append(f"{ticker_stem}.SS")
            cands.append(f"{ticker_stem}.HK")
            if ticker_stem.isdigit():
                cands.append(f"{int(ticker_stem):04d}.HK")
        
        if '.SS' in base or determined_suffix == '.SS':
            cands.append(f"{ticker_stem}.HK")
            cands.append(ticker_stem)
        
        normalized = []
        for cand in cands:
            clean = sanitize_ticker_symbol(cand)
            if is_plausible_ticker(clean):
                normalized.append(clean)
        return list(dict.fromkeys(normalized))
    except Exception:
        fallback = sanitize_ticker_symbol(original)
        return [fallback] if is_plausible_ticker(fallback) else []


def deduplicate_stock_results_by_yahoo(stock_results: List[Any]) -> List[Any]:
    """Dedupliziert StockData-Objekte nach Yahoo-Symbol.
    
    Args:
        stock_results: Liste der StockData-Objekte
        
    Returns:
        Deduplizierte Liste
    """
    def _as_float(v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return -1.0
    
    def _source_set(value: str) -> set:
        return {p.strip() for p in str(value).split(',') if p and p.strip()}
    
    deduped: Dict[str, Any] = {}
    duplicate_hits = 0
    
    for stock in stock_results:
        key = str(stock.yahoo_symbol).strip().upper()
        if not key:
            continue
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = stock
            continue
        duplicate_hits += 1
        current_score = (_as_float(existing.market_value), _as_float(existing.rsl), int(existing.trust_score))
        new_score = (_as_float(stock.market_value), _as_float(stock.rsl), int(stock.trust_score))
        if new_score > current_score:
            primary, secondary = stock, existing
        else:
            primary, secondary = existing, stock
        merged_sources = sorted(_source_set(primary.source_etf) | _source_set(secondary.source_etf))
        primary.source_etf = ", ".join(merged_sources)
        if hasattr(primary, "listing_source") or hasattr(secondary, "listing_source"):
            merged_listings = sorted(
                _source_set(getattr(primary, "listing_source", ""))
                | _source_set(getattr(secondary, "listing_source", ""))
            )
            if hasattr(primary, "listing_source"):
                primary.listing_source = ", ".join(merged_listings)
        if secondary.first_seen_date and (not primary.first_seen_date or secondary.first_seen_date < primary.first_seen_date):
            primary.first_seen_date = secondary.first_seen_date
        primary.is_new = bool(primary.is_new or secondary.is_new)
        if secondary.in_depot == "JA":
            primary.in_depot = "JA"
        deduped[key] = primary
    
    if duplicate_hits > 0:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Yahoo-Symbol-Deduplizierung: {duplicate_hits} Duplikat-Zeilen entfernt.")
    
    return list(deduped.values())


def parse_ishares_url(url: str) -> Optional[Dict[str, str]]:
    """Parst eine iShares-URL und extrahiert ID, Slug und Symbol.
    
    Args:
        url: iShares-URL
        
    Returns:
        Dictionary mit id, slug und symbol oder None
    """
    try:
        id_match = re.search(r'products/(\d+)/', url)
        slug_match = re.search(r'products/\d+/([^/]+)/', url)
        symbol_match = re.search(r'fileName=([A-Z0-9]+)_holdings', url)
        
        if id_match and slug_match and symbol_match:
            return {
                'id': id_match.group(1),
                'slug': slug_match.group(1),
                'symbol': symbol_match.group(1).upper()
            }
    except Exception:
        pass
    return None
