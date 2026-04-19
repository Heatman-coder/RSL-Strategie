from __future__ import annotations

import io
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests # type: ignore

from core import etf_processor as etf_processor_core
from core.entity_matching import normalize_name_for_dedup


def _item_value(item: Any, *keys: str) -> Any:
    if isinstance(item, dict):
        for key in keys:
            if key in item:
                return item.get(key)
        return None
    for key in keys:
        if hasattr(item, key):
            return getattr(item, key)
    return None


def load_json_config(file_path: str, is_list: bool = False) -> Any:
    """Lädt eine JSON-Konfigurationsdatei sicher."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return [] if is_list else {}


def save_json_config(file_path: str, data: Any) -> None:
    """Speichert Daten sicher als JSON."""
    try:
        folder = os.path.dirname(file_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return float(default)


def to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        val = value.strip().lower()
        if val in ("1", "true", "yes", "y", "ja", "on"):
            return True
        if val in ("0", "false", "no", "n", "nein", "off"):
            return False
    return bool(default)


def safe_positive_float(val: Any, default: float = 0.0) -> float:
    """Konvertiert zu Float und stellt sicher, dass der Wert positiv ist."""
    try:
        f = float(val)
        return f if f > 0 else float(default)
    except (ValueError, TypeError):
        return float(default)


def normalize_weights(*weights: Any) -> Tuple[float, ...]:
    """Normalisiert eine beliebige Anzahl an Gewichten auf die Summe 1.0."""
    vals = [max(0.0, to_float(w)) for w in weights]
    total = sum(vals)
    if total <= 0:
        return tuple(1.0 / len(vals) for _ in vals)
    return tuple(v / total for v in vals)


def calc_momentum(series: pd.Series, curr_price: float, lookback: int) -> Optional[float]:
    """Berechnet das Momentum über einen Zeitraum unter Berücksichtigung von Fehlwerten."""
    if lookback <= 0 or len(series) < lookback:
        return None
    past_price = float(series.iloc[-lookback])
    if past_price <= 0:
        return None
    return (curr_price / past_price) - 1.0


def load_watchlist_symbols(file_path: str) -> set:
    path = str(file_path or "").strip()
    if not path:
        return set()
    try:
        if not os.path.exists(path):
            return set()
        if path.lower().endswith(".json"):
            items = load_json_config(path, is_list=True)
            return {str(x).strip().upper() for x in items if str(x).strip()}
        with open(path, 'r', encoding='utf-8') as f:
            raw = f.read()
        for sep in [",", ";"]:
            raw = raw.replace(sep, "\n")
        symbols = []
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            symbols.append(line)
        return {s.strip().upper() for s in symbols if s.strip()}
    except Exception:
        return set()


def parse_tokens(value: Any) -> Set[str]:
    """Zerlegt einen kommagetrennten String in ein Set von bereinigten Tokens."""
    if not value:
        return set()
    return {p.strip() for p in str(value).split(",") if p and p.strip()}


def merge_tokens(series: pd.Series) -> str:
    values: List[str] = []
    for value in series:
        for part in str(value or "").split(","):
            cleaned = part.strip()
            if cleaned and cleaned.lower() != "nan":
                values.append(cleaned)
    return ", ".join(sorted(set(values)))


def parse_etf_selection_input(inp: str, opts: Dict[str, Any]) -> List[str]:
    if not inp:
        return []
    if inp.lower() == "all":
        return list(opts.keys()) + ["XETRA", "FRA", "FDB"]
    keys = list(opts.keys()) + ["XETRA", "FRA", "FDB"]
    selected: List[str] = []
    for part in (p.strip().upper() for p in inp.replace(",", " ").split()):
        if part in opts or part in ("XETRA", "FRA", "FDB"):
            selected.append(part)
            continue
        if part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(keys):
                selected.append(keys[idx])
    return list(dict.fromkeys(selected))


def parse_ishares_url(url: str):
    return etf_processor_core.parse_ishares_url(url)


def sanitize_ticker_symbol(value: Any) -> str:
    return etf_processor_core.sanitize_ticker_symbol(str(value or ""))


def is_plausible_ticker(symbol: str) -> bool:
    return etf_processor_core.is_plausible_ticker(str(symbol or ""))


def generate_candidates(
    orig: str,
    land: str,
    exchange: str,
    unsupported_exchanges: List[str],
    exchange_suffix_map: Dict[str, str],
    location_suffix_map: Dict[str, str],
) -> List[str]:
    return etf_processor_core.generate_candidates(
        original=orig,
        land=land,
        exchange=exchange,
        unsupported_exchanges=unsupported_exchanges,
        exchange_suffix_map=exchange_suffix_map,
        location_suffix_map=location_suffix_map,
    )


def download_ishares_csv(url: str, logger: Any, log_label: bool = True) -> pd.DataFrame:
    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        text = response.content.decode("utf-8-sig", errors="replace")
        lines = text.splitlines()
        header_idx = None
        for idx, line in enumerate(lines):
            normalized = line.strip().strip('"')
            if normalized.startswith("Ticker,") or normalized.startswith("Ticker;"):
                header_idx = idx
                break
        if header_idx is None:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
        return df.dropna(how="all")
    except Exception as exc:
        if log_label:
            logger.warning(f"ETF-CSV konnte nicht geladen werden: {exc}")
        return pd.DataFrame()


def log_info_fetch_summary(msg: str, mgr: Any, logger: Any) -> None:
    try:
        cache_size = len(getattr(mgr, "info_cache", {}) or {})
        logger.info(f"{msg}: Info-Cache groesse {cache_size}")
    except Exception:
        logger.info(msg)


def normalize_name_for_dedup_key(name: Any) -> str:
    return normalize_name_for_dedup(name)


def has_meaningful_isin_data(df: pd.DataFrame) -> bool:
    if "ISIN" not in df.columns:
        return False
    try:
        cleaned = df["ISIN"].astype(str).str.replace(r"[^A-Z0-9]", "", regex=True).str.upper()
    except Exception:
        return False
    valid = cleaned[~cleaned.isin(["", "NAN", "NONE"])]
    if valid.empty:
        return False
    return bool((valid.str.len() > 5).any())


def history_priority_score(item: Any, location_suffix_map: Dict[str, str]) -> int:
    ticker = sanitize_ticker_symbol(_item_value(item, "Ticker", "ticker", "yahoo_symbol"))
    land = str(_item_value(item, "Land", "land") or "").strip()
    exchange_text = " ".join(
        str(_item_value(item, key) or "")
        for key in ("Exchange", "exchange", "Listing_Source", "listing_source")
    ).upper()
    home_suffix = str(location_suffix_map.get(land, "") or "").strip().upper()
    if home_suffix and not home_suffix.startswith("."):
        home_suffix = f".{home_suffix}"

    if not ticker:
        return 999
    if home_suffix and ticker.endswith(home_suffix):
        return 1
    if "." not in ticker:
        return 5
    if "XETRA" in exchange_text or ticker.endswith(".DE"):
        return 20

    native_suffixes = {".T", ".L", ".HK", ".TO", ".AX", ".OL", ".ST", ".CO", ".KS", ".KQ", ".TW", ".TWO", ".SS", ".SZ"}
    if any(ticker.endswith(suffix) for suffix in native_suffixes):
        return 30
    if "FRA" in exchange_text or ticker.endswith((".F", ".SG", ".DU", ".BE", ".HM", ".MU")):
        return 100
    return 50


def stock_history_priority_score(stock: Any, location_suffix_map: Dict[str, str]) -> int:
    return history_priority_score(
        {
            "Ticker": _item_value(stock, "yahoo_symbol", "Ticker"),
            "Land": _item_value(stock, "land", "Land"),
            "Listing_Source": _item_value(stock, "listing_source", "Listing_Source"),
        },
        location_suffix_map,
    )


class TickerMapper:
    def __init__(self, path: str):
        self.path = path
        self.data = self._load()
        self.dirty = False

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.dirty = True

    def save_if_dirty(self) -> None:
        if not self.dirty:
            return
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)


def build_history_symbol_overrides(
    raw_df: pd.DataFrame,
    current_df: pd.DataFrame,
    location_suffix_map: Dict[str, str],
    unsupported_exchanges: List[str],
    exchange_suffix_map: Dict[str, str],
) -> Dict[str, str]:
    if raw_df is None or raw_df.empty or current_df is None or current_df.empty or "Ticker" not in raw_df.columns:
        return {}

    work = raw_df.copy()
    for col in ["Source_ETF", "Listing_Source", "ISIN", "Name", "Land", "Exchange"]:
        if col not in work.columns:
            work[col] = ""

    work["TICKER_CLEAN"] = work["Ticker"].astype(str).str.strip().str.upper()
    work["ISIN_CLEAN"] = (
        work["ISIN"].astype(str).str.replace(r"[^A-Z0-9]", "", regex=True).str.upper().replace("NAN", "")
    )
    work["NAME_CLEAN"] = work["Name"].apply(normalize_name_for_dedup_key)
    work["_hist_prio"] = work.apply(lambda row: history_priority_score(row, location_suffix_map), axis=1)

    def _best_candidate(group: pd.DataFrame, current_ticker: str) -> str:
        if group is None or group.empty:
            return ""
        candidates = group[group["TICKER_CLEAN"] != current_ticker].copy()
        if candidates.empty:
            return ""
        candidates = candidates.sort_values(["_hist_prio", "TICKER_CLEAN"])
        best_row = candidates.iloc[0]
        best = str(best_row.get("TICKER_CLEAN", "")).strip().upper()
        if best and "." not in best:
            best_candidates = generate_candidates(
                orig=best,
                land=str(best_row.get("Land", "") or ""),
                exchange=str(best_row.get("Exchange", "") or ""),
                unsupported_exchanges=unsupported_exchanges,
                exchange_suffix_map=exchange_suffix_map,
                location_suffix_map=location_suffix_map,
            )
            if best_candidates:
                best = str(best_candidates[0]).strip().upper()
        return best if best and best != current_ticker else ""

    isin_groups: Dict[str, pd.DataFrame] = {}
    if has_meaningful_isin_data(work):
        valid_isin = work["ISIN_CLEAN"].str.len() > 5
        if valid_isin.any():
            isin_groups = {key: grp.copy() for key, grp in work[valid_isin].groupby("ISIN_CLEAN")}

    valid_name = work["NAME_CLEAN"].str.len() > 4
    name_groups = {key: grp.copy() for key, grp in work[valid_name].groupby("NAME_CLEAN")}

    overrides: Dict[str, str] = {}
    for _, row in current_df.iterrows():
        current_ticker = str(row.get("Ticker", "")).strip().upper()
        if not current_ticker:
            continue

        current_isin = str(row.get("ISIN", "")).replace(" ", "").strip().upper()
        current_name = normalize_name_for_dedup_key(row.get("Name", ""))
        chosen = ""

        if current_isin:
            current_isin = sanitize_ticker_symbol(current_isin).replace(".", "").replace("-", "")
        if current_isin and len(current_isin) > 5 and current_isin in isin_groups:
            chosen = _best_candidate(isin_groups[current_isin], current_ticker)

        if not chosen and current_name and current_name in name_groups:
            name_group = name_groups[current_name]
            preferred = name_group[
                (name_group["Source_ETF"].astype(str).str.strip() != "")
                | (name_group["Listing_Source"].astype(str).str.upper().str.contains("XETRA", na=False))
                | (~name_group["TICKER_CLEAN"].str.endswith(".F"))
            ]
            chosen = _best_candidate(preferred if not preferred.empty else name_group, current_ticker)

        current_prio = history_priority_score(row, location_suffix_map)
        chosen_prio = history_priority_score({"Ticker": chosen, "Land": row.get("Land", "")}, location_suffix_map)
        if chosen and chosen != current_ticker and chosen_prio < current_prio:
            overrides[current_ticker] = chosen
    return overrides


def synchronize_portfolio_symbols_with_stock_results(portfolio_mgr: Any, results: List[Any]) -> int:
    if not portfolio_mgr or not hasattr(portfolio_mgr, "current_portfolio"):
        return 0

    by_symbol: Dict[str, Any] = {}
    by_isin: Dict[str, Any] = {}
    by_name: Dict[str, List[Any]] = defaultdict(list)
    for stock in results or []:
        original = str(getattr(stock, "original_ticker", "") or "").strip().upper()
        yahoo_symbol = str(getattr(stock, "yahoo_symbol", "") or "").strip().upper()
        if original:
            by_symbol[original] = stock
        if yahoo_symbol:
            by_symbol[yahoo_symbol] = stock
        
        isin = str(getattr(stock, "isin", "") or "").strip().upper()
        if isin and len(isin) > 5 and isin not in ("NAN", "NONE"):
            by_isin[isin] = stock

        name_key = normalize_name_for_dedup_key(getattr(stock, "name", ""))
        if name_key:
            by_name[name_key].append(stock)

    changed = 0
    for item in portfolio_mgr.current_portfolio:
        current_symbol = str(item.get("Yahoo_Symbol", "") or "").strip().upper()
        original_ticker = str(item.get("Original_Ticker", "") or "").strip().upper()
        isin = str(item.get("ISIN", item.get("isin", "")) or "").strip().upper()
        name_key = normalize_name_for_dedup_key(item.get("Name", ""))

        match = None
        if original_ticker and original_ticker in by_symbol:
            match = by_symbol[original_ticker]
        elif current_symbol and current_symbol in by_symbol:
            match = by_symbol[current_symbol]
        elif isin and isin in by_isin:
            match = by_isin[isin]
        elif name_key and len(by_name.get(name_key, [])) == 1:
            match = by_name[name_key][0]

        if match is None:
            continue

        new_symbol = str(getattr(match, "yahoo_symbol", "") or "").strip().upper()
        if not new_symbol:
            continue
        if current_symbol != new_symbol:
            item["Yahoo_Symbol"] = new_symbol
            changed += 1
        if not item.get("Original_Ticker"):
            item["Original_Ticker"] = str(getattr(match, "original_ticker", new_symbol) or new_symbol).strip().upper()

    if changed and hasattr(portfolio_mgr, "save"):
        portfolio_mgr.save(portfolio_mgr.current_portfolio)

    updated_symbols = {
        str(item.get("Yahoo_Symbol", "") or "").strip().upper()
        for item in getattr(portfolio_mgr, "current_portfolio", [])
        if str(item.get("Yahoo_Symbol", "") or "").strip()
    }
    for stock in results or []:
        stock.in_depot = "JA" if str(getattr(stock, "yahoo_symbol", "") or "").strip().upper() in updated_symbols else ""

    return changed
