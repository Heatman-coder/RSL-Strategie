import codecs
import datetime
import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, fields
from typing import Any, Callable, Dict, List, Optional, Tuple, cast
from urllib.parse import quote

import pandas as pd
import yfinance as yf

from data_manager import MarketDataManager, PortfolioManager, StockData
from . import console_ui as console_ui_core
from . import data_pipeline as data_pipeline_core
from . import financedatabase_universe as financedatabase_universe_core
from . import final_support as final_support_core
from . import ranking as ranking_core
from . import rsl_integrity as rsl_integrity_core
from . import summaries as summary_core
from .entity_matching import normalize_name_for_dedup

try:
    from tqdm import tqdm as _tqdm
    tqdm = _tqdm
except ImportError:
    class tqdm_fallback:
        def __init__(self, iterable=None, total=None, **kwargs):
            self.iterable = iterable
            self.total = total
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_value, traceback):
            return None
        def update(self, n=1):
            return None
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
    tqdm = tqdm_fallback  # type: ignore


def fix_bom_in_file(file_path: str) -> None:
    bom = codecs.BOM_UTF8
    try:
        with open(file_path, "rb") as handle:
            content = handle.read()
        if content.startswith(bom):
            with open(file_path, "wb") as handle:
                handle.write(content[len(bom):])
    except Exception:
        pass


def configure_best_console_mode() -> Dict[str, Any]:
    state: Dict[str, Any] = {"encoding": "", "unicode": False, "ansi": False}
    try:
        os.environ.setdefault("PYTHONUTF8", "1")
        os.environ.setdefault("RSL_ASCII_CONSOLE", "0")
    except Exception:
        pass
    if os.name == "nt":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleOutputCP(65001)
            kernel32.SetConsoleCP(65001)
        except Exception:
            pass
        try:
            for stream in (sys.stdout, sys.stderr):
                if hasattr(stream, "reconfigure"):
                    stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            enable_virtual_terminal_processing = 0x0004
            for handle_id in (-11, -12):
                handle = kernel32.GetStdHandle(handle_id)
                if handle in (0, -1):
                    continue
                mode = ctypes.c_uint32()
                if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                    kernel32.SetConsoleMode(handle, mode.value | enable_virtual_terminal_processing)
            state["ansi"] = True
        except Exception:
            state["ansi"] = False
    enc = str(getattr(sys.stdout, "encoding", "") or "").lower()
    state["encoding"] = enc
    state["unicode"] = "utf" in enc
    return state


def configure_logging(log_file: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    logging.getLogger("yahoo_connector").setLevel(logging.CRITICAL)
    return logger


def get_last_performance_duration(config: Dict[str, Any]) -> Optional[str]:
    log_file = config.get("performance_log_csv")
    if not log_file or not os.path.exists(log_file):
        return None
    try:
        with open(log_file, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
        if len(lines) < 2:
            return None
        parts = lines[-1].strip().split(";")
        if len(parts) > 1:
            return f"{float(parts[1]):.1f}s"
    except Exception:
        return None
    return None


def make_progress(
    total: int,
    desc: str,
    config: Dict[str, Any],
    console_runtime: Dict[str, Any],
    include_last_duration: bool = True,
):
    last_duration = get_last_performance_duration(config) if include_last_duration else None
    full_desc = f"{desc} (letzter Lauf: {last_duration})" if last_duration else desc
    return tqdm(
        total=total,
        desc=full_desc,
        dynamic_ncols=True,
        ascii=not bool(console_runtime.get("unicode", False)),
        leave=False,
    )


def save_dataframe_safely(df: pd.DataFrame, filename: str, logger: Any, **kwargs) -> None:
    while True:
        try:
            df.to_csv(filename, **kwargs)
            logger.info(f"Gespeichert: {filename}")
            return
        except PermissionError:
            print(f"\nACHTUNG: Die Datei '{os.path.basename(filename)}' ist noch geoeffnet!")
            print("--> Bitte schliessen Sie die Datei in Excel/Editor.")
            user_in = input("--> Druecken Sie ENTER, um es erneut zu versuchen (oder 'x' zum Abbrechen): ")
            if user_in.strip().lower() == "x":
                logger.warning("Speichern durch Benutzer abgebrochen. Daten sind verloren.")
                return
        except Exception as exc:
            logger.error(f"Kritischer Fehler beim Speichern von {filename}: {exc}")
            return


def normalize_sector_name(raw_sector: Any) -> str:
    sector_map = {
        "industrials": "Industrials",
        "industrial": "Industrials",
        "financials": "Financials",
        "financialinstitutions": "Financials",
        "consumerdefensive": "Consumer Staples",
        "consumerdiscretionary": "Consumer Discretionary",
        "healthcare": "Health Care",
        "informationtechnology": "Information Technology",
        "materials": "Materials",
        "realestate": "Real Estate",
        "consumerstaples": "Consumer Staples",
        "communication": "Communication",
        "communicationservices": "Communication",
        "energy": "Energy",
        "utilities": "Utilities",
        "utility": "Utilities",
        "agency": "Other",
        "other": "Other",
        "otherunknown": "Other",
    }
    if raw_sector is None:
        return "Unbekannt"
    sector_name = str(raw_sector).strip()
    if not sector_name:
        return "Unbekannt"
    key = re.sub(r"[^a-z0-9]+", "", sector_name.lower())
    return sector_map.get(key, sector_name)


def build_yahoo_quote_url(yahoo_symbol: str) -> str:
    symbol = str(yahoo_symbol or "").strip()
    if not symbol:
        return ""
    if symbol.isalnum():
        path_symbol = quote(symbol, safe=".-_")
        query_symbol = quote(symbol, safe=".-_")
        return f"https://finance.yahoo.com/quote/{path_symbol}/?p={query_symbol}"
    search_query = quote(f'site:finance.yahoo.com/quote "{symbol}"', safe="")
    return f"https://www.google.com/search?q={search_query}"


def resolve_market_cap_from_info(info: Dict[str, Any], safe_positive_float: Callable[[Any, float], float]) -> float:
    if not info:
        return 0.0
    try:
        return safe_positive_float(info.get("marketCap", 0.0), 0.0)
    except TypeError:
        return safe_positive_float(info.get("marketCap", 0.0))


def get_currency_rate_for_ticker(ticker: str, currency_rates: Dict[str, Any]) -> float:
    normalized = str(ticker or "").strip().upper()
    for suffix, rate in currency_rates.items():
        if suffix != "DEFAULT" and normalized.endswith(suffix):
            return float(rate)
    return float(currency_rates.get("DEFAULT", 1.0))


def resolve_market_value_from_sources(
    row: pd.Series,
    info: Dict[str, Any],
    ticker: str,
    currency_rates: Dict[str, Any],
    safe_positive_float: Callable[[Any, float], float],
) -> float:
    try:
        value = safe_positive_float(row.get("Market_Value"), 0.0)
    except TypeError:
        value = safe_positive_float(row.get("Market_Value"))
    if value <= 0:
        try:
            value = safe_positive_float(row.get("Market Value"), 0.0)
        except TypeError:
            value = safe_positive_float(row.get("Market Value"))
    if value <= 0:
        market_cap = resolve_market_cap_from_info(info, safe_positive_float)
        value = market_cap * get_currency_rate_for_ticker(ticker, currency_rates)
    return value


def apply_primary_liquidity_context(
    results: List[StockData],
    currency_rates: Dict[str, Any],
    to_float: Callable[[Any, float], float],
) -> None:
    for stock in results:
        if not isinstance(stock.name, str):
            stock.name = str(stock.name or "Unknown").strip()
            if stock.name.lower() in ("nan", "none", ""):
                stock.name = "Unknown"
    groups: Dict[str, List[StockData]] = defaultdict(list)
    name_to_isin: Dict[str, str] = {}
    for stock in results:
        isin = str(stock.isin or "").strip().upper()
        if isin and len(isin) > 5 and isin != "NAN":
            name_key = normalize_name_for_dedup(stock.name)
            if name_key and len(name_key) > 3:
                name_to_isin[name_key] = isin
    for stock in results:
        isin = str(stock.isin or "").strip().upper()
        if not isin or len(isin) <= 5 or isin == "NAN":
            name_key = normalize_name_for_dedup(stock.name)
            if name_key in name_to_isin:
                stock.isin = name_to_isin[name_key]
    for stock in results:
        key = stock.isin if stock.isin and len(stock.isin) > 5 else normalize_name_for_dedup(stock.name)
        groups[key].append(stock)
    for items in groups.values():
        if not items:
            continue
        best = max(items, key=lambda item: to_float(item.avg_volume_eur, -1.0))
        best_market_obj = max(items, key=lambda item: to_float(getattr(item, "market_cap", 0.0), -1.0))
        best_market_cap = to_float(getattr(best_market_obj, "market_cap", 0.0), 0.0)
        best_market_rate = get_currency_rate_for_ticker(best_market_obj.yahoo_symbol, currency_rates)
        for stock in items:
            stock.primary_liquidity_eur = best.avg_volume_eur
            stock.primary_liquidity_symbol = best.yahoo_symbol
            stock.primary_liquidity_basis = "ISIN" if stock.isin and len(stock.isin) > 5 else "Name"

            # Aggressivere Vererbung der Market Cap innerhalb der Gruppe
            current_mkt = to_float(getattr(stock, "market_cap", 0.0), 0.0)
            if current_mkt <= 0 and best_market_cap > 0:
                stock.market_cap = best_market_cap
                stock.market_value = best_market_cap * best_market_rate


def update_live_currency_rates(currency_rates: Dict[str, Any], logger: Any, yf_module: Any = yf) -> None:
    logger.info("Aktualisiere Wechselkurse via Yahoo Finance...")
    # Mapping von Währungspaaren auf Listen von Suffixen
    pairs_to_suffixes = {
        "EURUSD=X": ["DEFAULT"], "EURJPY=X": [".T"], "EURGBP=X": [".L"],
        "EURHKD=X": [".HK"], "EURCAD=X": [".TO"], "EURAUD=X": [".AX"],
        "EURCHF=X": [".SW"], "EURSEK=X": [".ST"], "EURNOK=X": [".OL"],
        "EURSAR=X": [".SR"], "EURBRL=X": [".SA"], "EURINR=X": [".NS"],
        "EURCNY=X": [".SS", ".SZ"],
        "EURTWD=X": [".TW", ".TWO"], "EURKRW=X": [".KS", ".KQ"],
        "EURMXN=X": [".MX"], "EURIDR=X": [".JK"], "EURTHB=X": [".BK"]
    }
    try:
        data = yf_module.download(list(pairs_to_suffixes.keys()), period="1d", interval="1m", progress=False)
        if not data.empty and "Close" in data.columns:
            for pair, suffixes in pairs_to_suffixes.items():
                last_price = data["Close"][pair].iloc[-1]
                if last_price > 0:
                    rate = 1.0 / float(last_price)
                    for sfx in suffixes:
                        currency_rates[sfx] = rate
            logger.info("Wechselkurse erfolgreich aktualisiert.")
    except Exception as exc:
        logger.warning(f"Live-FX Update fehlgeschlagen (nutze Fallbacks): {exc}")
    for euro_suffix in [".DE", ".F", ".PA", ".AS", ".MC", ".MI"]:
        currency_rates[euro_suffix] = 1.0
    currency_rates["EUR"] = 1.0


def get_user_input(prompt: str, default: Optional[str] = None, valid_options: Optional[List[str]] = None) -> str:
    while True:
        user_in = input(prompt).strip().lower()
        if not user_in and default is not None:
            return default
        if valid_options and user_in not in valid_options:
            print(f"Bitte waehle eine der Optionen: {', '.join(valid_options)}")
            continue
        return user_in


def cache_age_hours(file_path: str) -> Optional[float]:
    try:
        if not file_path or not os.path.exists(file_path):
            return None
        return max(0.0, (time.time() - os.path.getmtime(file_path)) / 3600.0)
    except Exception:
        return None


def format_age_text(hours: Optional[float]) -> str:
    if hours is None:
        return "-"
    return f"{hours:.1f}h"


def summarize_etf_selection(selected_syms: List[str], etf_options: Dict[str, Any], max_items: int = 5) -> str:
    if not selected_syms:
        return "-"
    all_etf_keys = list(etf_options.keys())
    if all_etf_keys and len(selected_syms) == len(all_etf_keys) and set(selected_syms) == set(all_etf_keys):
        return "Alle"
    if len(selected_syms) <= max_items:
        return ", ".join(selected_syms)
    return f'{", ".join(selected_syms[:max_items])}, ... (+{len(selected_syms) - max_items})'


def print_run_status_header(
    selected_syms: List[str],
    portfolio_size: int,
    etf_options: Dict[str, Any],
    config: Dict[str, Any],
    console_runtime: Dict[str, Any],
) -> None:
    table_width = 100
    history_age = cache_age_hours(config["history_cache_file"])
    etf_age = cache_age_hours(config["etf_cache_file"])
    console_mode = f'{console_runtime.get("encoding")} / ANSI={str(console_runtime.get("ansi")).lower()}'
    cand_top_pct = float(config.get("candidate_top_percent_threshold", config.get("top_percent_threshold", 0.25)) or 0.25) * 100.0
    require_top = bool(config.get("candidate_require_top_percent", False))
    min_trust = int(config.get("candidate_min_trust_score", 0) or 0)
    cluster_filter = bool(config.get("cluster_enabled", True)) and bool(config.get("candidate_use_cluster_filter", True))
    min_sleep = float(config.get("batch_sleep_min_s", 0.5))
    max_sleep = float(config.get("batch_sleep_max_s", 1.5))
    print("\n" + "=" * table_width)
    print(" LAUF-STATUS")
    print("=" * table_width)
    print(f" ETFs: {summarize_etf_selection(selected_syms, etf_options)}")
    print(f" Portfolio: {portfolio_size} Position(en)")
    print(f" Cache: History={format_age_text(history_age)} | ETF={format_age_text(etf_age)}")
    print(f' History: {config.get("history_period", "-")} | Info-Unknown-Expiry: {config.get("info_cache_unknown_expiry_days", 7)}d')
    print(f' Schwellen: Warn={config["heatmap_warn_percent"]:.1f}% | Voll={config["heatmap_full_percent"]:.1f}%')
    print(f" Kandidaten: Top% {cand_top_pct:.0f}% ({'an' if require_top else 'aus'}) | Trust-Min {min_trust} | Cluster-Filter {'an' if cluster_filter else 'aus'}")
    print(
        f' Branchen: Top {config.get("industry_top_n", "-")}, '
        f'Score-Min {config.get("industry_score_min", 0.0):.2f}, '
        f'Breadth-Min {config.get("industry_breadth_min", 0.25):.2f}, '
        f'Min-Size {config.get("industry_min_size", 10)}'
    )
    print(f'Delays: Batch-Sleep {min_sleep:.2f}-{max_sleep:.2f}s | Info-Fetch {float(config.get("info_fetch_delay_s", 0.7) or 0.7):.2f}s')
    print(f" Konsole: {console_mode}")
    print("=" * table_width)


class TeeStream:
    def __init__(self, *streams: Any):
        self.streams = streams
    def write(self, data: str) -> None:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


class ConsoleCapture:
    def __init__(self, output_file: str):
        self.output_file = output_file
        self.file = None
        self.old_stdout = None
        self.old_stderr = None
    def __enter__(self):
        folder = os.path.dirname(self.output_file)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self.file = open(self.output_file, "w", encoding="utf-8")
        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        tee = TeeStream(self.old_stdout, self.file)
        sys.stdout = tee
        sys.stderr = tee
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.old_stdout is not None:
            sys.stdout = self.old_stdout
        if self.old_stderr is not None:
            sys.stderr = self.old_stderr
        if self.file:
            self.file.close()


def save_analysis_snapshot(
    snapshot_file: str,
    stock_results: List[StockData],
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    save_json_config: Callable[[str, Any], None],
    integrity_drops_df: Optional[pd.DataFrame] = None,
    universe_audit_df: Optional[pd.DataFrame] = None,
) -> None:
    payload = {
        "saved_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "selected_syms": list(selected_syms),
        "etf_options": etf_options,
        "stock_results": [asdict(stock) for stock in stock_results],
        "integrity_drops": integrity_drops_df.to_dict(orient="records") if integrity_drops_df is not None else [],
        "universe_audit": universe_audit_df.to_dict(orient="records") if universe_audit_df is not None else [],
    }
    save_json_config(snapshot_file, payload)


def load_analysis_snapshot(
    snapshot_file: str,
    load_json_config: Callable[..., Any],
    currency_rates: Dict[str, Any],
    to_float: Callable[[Any, float], float],
) -> Optional[Dict[str, Any]]:
    snapshot = load_json_config(snapshot_file)
    if not snapshot or "stock_results" not in snapshot:
        return None
    field_names = {field.name for field in fields(StockData)}
    stocks: List[StockData] = []
    for item in snapshot.get("stock_results", []):
        if not isinstance(item, dict):
            continue
        row = {key: value for key, value in item.items() if key in field_names}
        if pd.isna(row.get("name")) or row.get("name") in ("", None):
            row["name"] = "Unknown"
        row["name"] = str(row["name"])
        row.setdefault("industry", "Unknown")
        row.setdefault("isin", "")
        row.setdefault("listing_source", "")
        row.setdefault("flag_scale", "OK")
        row.setdefault("scale_reason", "")
        row["flag_stale"] = str(row.get("flag_stale", "OK"))
        row["stale_reason"] = str(row.get("stale_reason", ""))
        row["flag_history_length"] = str(row.get("flag_history_length", "OK"))
        row["history_length_reason"] = str(row.get("history_length_reason", ""))
        row["integrity_warnings"] = item.get("integrity_warnings", [])
        row["drop_reasons"] = item.get("drop_reasons", [])
        row["hard_fail_reasons"] = item.get("hard_fail_reasons", [])
        row["warning_reasons"] = item.get("warning_reasons", [])
        row["review_reasons"] = item.get("review_reasons", [])
        row["diagnostics"] = item.get("diagnostics", {})
        row["ranking_integrity_status"] = item.get("ranking_integrity_status", "eligible_original")
        row["excluded_from_ranking"] = bool(item.get("excluded_from_ranking", False))
        row["ranking_exclude_reason"] = item.get("ranking_exclude_reason", "")
        row["repair_applied"] = bool(item.get("repair_applied", False))
        row["repair_method"] = item.get("repair_method", "")
        row["repair_reason"] = item.get("repair_reason", "")
        row["used_close_fallback"] = bool(item.get("used_close_fallback", False))
        row["fallback_fraction"] = item.get("fallback_fraction")
        row["rsl_price_source"] = item.get("rsl_price_source", "adj_close")
        row.setdefault("price_scale_ratio", 1.0)
        if "avg_volume_eur" not in row:
            row["avg_volume_eur"] = row.get("avg_volume_usd", 0.0)
        row.pop("avg_volume_usd", None)
        row.setdefault("primary_liquidity_eur", row.get("avg_volume_eur", 0.0))
        row.setdefault("primary_liquidity_symbol", "")
        row.setdefault("primary_liquidity_basis", "")
        row.setdefault("trust_score", 0)
        if str(row.get("isin", "")).strip().lower() in ("nan", "none"):
            row["isin"] = ""
        if str(row.get("listing_source", "")).strip().lower() in ("nan", "none"):
            row["listing_source"] = ""
        source_parts = [part.strip() for part in str(row.get("source_etf", "") or "").split(",") if part and part.strip()]
        listing_parts = [part.strip() for part in str(row.get("listing_source", "") or "").split(",") if part and part.strip()]
        moved_listing_parts = [part for part in source_parts if part in {"FRA", "XETRA"}]
        if moved_listing_parts:
            source_parts = [part for part in source_parts if part not in {"FRA", "XETRA"}]
            listing_parts = sorted(set(listing_parts + moved_listing_parts))
            row["source_etf"] = ", ".join(source_parts)
            row["listing_source"] = ", ".join(listing_parts)
        try:
            stocks.append(StockData(**row))
        except TypeError:
            continue
    if not stocks:
        return None
    integrity_drops_df = pd.DataFrame(snapshot.get("integrity_drops", []))
    universe_audit_df = pd.DataFrame(snapshot.get("universe_audit", []))
    apply_primary_liquidity_context(stocks, currency_rates, to_float)
    stocks.sort(key=lambda stock: (stock.rsl if not pd.isna(stock.rsl) else -1.0), reverse=True)
    for index, stock in enumerate(stocks, start=1):
        stock.rsl_rang = index
    stocks.sort(key=lambda stock: (stock.market_value if not pd.isna(stock.market_value) else -1.0), reverse=True)
    for index, stock in enumerate(stocks, start=1):
        stock.mktcap_rang = index
    stocks.sort(key=lambda stock: (stock.rsl if not pd.isna(stock.rsl) else -1.0), reverse=True)
    return {
        "saved_at": snapshot.get("saved_at", ""),
        "selected_syms": snapshot.get("selected_syms", []),
        "etf_options": snapshot.get("etf_options", {}),
        "stock_results": stocks,
        "integrity_drops_df": integrity_drops_df,
        "universe_audit_df": universe_audit_df,
    }


def select_etf_interactive(
    config: Dict[str, Any],
    load_json_config: Callable[..., Any],
    save_json_config: Callable[[str, Any], None],
    parse_etf_selection_input: Callable[[str, Dict[str, Any]], List[str]],
    parse_ishares_url: Callable[[str], Any],
) -> Tuple[List[str], Dict[str, Any]]:
    etf_config = load_json_config(config["etf_config_file"])
    if not etf_config or "options" not in etf_config or "selected_symbols" not in etf_config:
        etf_config = {"selected_symbols": [], "options": {}}
    while True:
        current_selection = etf_config.get("selected_symbols", [])
        etf_options = etf_config.get("options", {})
        if current_selection:
            print(f"\nAktuelle Auswahl: {', '.join(current_selection)}")
            quick_reply = input("Auswahl aendern? (j/n oder direkte ETF-Auswahl wie IVV): ").strip() or "n"
            if quick_reply.lower() not in ("j", "y", "ja", "yes", "n", "no", "nein"):
                parsed_selection = parse_etf_selection_input(quick_reply, etf_options)
                if parsed_selection:
                    etf_config["selected_symbols"] = parsed_selection
                    save_json_config(config["etf_config_file"], etf_config)
                    return parsed_selection, etf_options
                print("Direkte ETF-Auswahl nicht erkannt. Wechsle in das Auswahlmenue.")
            elif quick_reply.lower() not in ("j", "y", "ja", "yes"):
                return current_selection, etf_options
        print("\nWaehle ETFs (Mehrfachauswahl moeglich, z.B. '1, 3, 5', 'IVV, SOXX' oder 'all'):")
        opts = list(etf_options.keys())
        for index, symbol in enumerate(opts, start=1):
            print(f"{index}. {symbol} - {etf_options[symbol]['name']}")
        print(f"{len(opts)+1}. XETRA - Deutsche Boerse Xetra")
        print(f"{len(opts)+2}. FRA   - Deutsche Boerse Frankfurt")
        print(f"{len(opts)+3}. FDB   - FinanceDatabase Zusatzuniversum (defensiv gefiltert)")
        print("\nOder 'all' fuer alles, 'add' zum Hinzufuegen, 'remove' zum Entfernen, '?' fuer Hilfe.")
        choice = input("Wahl (z.B. 1,2,FRA): ").strip().lower()
        if choice == "?":
            print("\nHilfe ETF-Auswahl:")
            print(" - Nummern: Auswahl per Index, z.B. 1,3,5")
            print(" - Symbole: direkte Auswahl per Symbol, z.B. IVV oder IVV,SOXX")
            print(" - all: alle ETFs auswaehlen")
            print(" - FDB: FinanceDatabase als Zusatzquelle aktivieren")
            print(" - add: ETF manuell oder per iShares-URL hinzufuegen")
            print(" - remove: ETF aus Liste entfernen")
            print(" - Enter: Eingabe wiederholen")
            continue
        if choice == "add":
            user_in = input("Gib das Symbol ODER die volle iShares CSV-URL ein: ").strip()
            parsed = parse_ishares_url(user_in)
            if parsed:
                print(f"URL erkannt! Symbol: {parsed['symbol']}")
                name = input(f"Name fuer {parsed['symbol']}: ").strip()
                etf_config["options"][parsed["symbol"]] = {"name": name, "id": parsed["id"], "slug": parsed["slug"]}
            else:
                symbol = user_in.upper()
                if not symbol:
                    continue
                etf_config["options"][symbol] = {"name": input("Name: "), "id": input("ID: "), "slug": input("Slug: ")}
            save_json_config(config["etf_config_file"], etf_config)
            print("ETF-Liste aktualisiert.")
            continue
        if choice == "remove":
            user_in = input("Symbol oder Nummer zum Entfernen: ").strip()
            symbol_to_remove = None
            if user_in.isdigit():
                idx = int(user_in) - 1
                if 0 <= idx < len(opts):
                    symbol_to_remove = opts[idx]
            elif user_in.upper() in cast(Dict[str, Any], etf_config["options"]):
                symbol_to_remove = user_in.upper()
            if symbol_to_remove:
                del etf_config["options"][symbol_to_remove]
                if symbol_to_remove in etf_config.get("selected_symbols", []):
                    etf_config["selected_symbols"].remove(symbol_to_remove)
                save_json_config(config["etf_config_file"], etf_config)
                print(f"{symbol_to_remove} wurde entfernt.")
            else:
                print(f"'{user_in}' nicht in der Liste gefunden.")
            continue
        if choice == "all":
            full_selection = opts + ["XETRA", "FRA", "FDB"]
            etf_config["selected_symbols"] = full_selection
            save_json_config(config["etf_config_file"], etf_config)
            return full_selection, etf_options
        try:
            new_selection = parse_etf_selection_input(choice, etf_options)
            if new_selection:
                etf_config["selected_symbols"] = new_selection
                save_json_config(config["etf_config_file"], etf_config)
                return new_selection, etf_options
            print("Keine gueltige Auswahl.")
        except Exception:
            print("Ungueltig.")


def render_analysis_output(
    stock_results: List[StockData],
    portfolio_mgr: PortfolioManager,
    selected_syms: List[str],
    etf_options: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    logger: Any,
    save_json_config: Callable[[str, Any], None],
    save_dataframe_safely_func: Callable[..., None],
    save_excel_report_safely: Callable[..., bool],
    build_console_symbols: Callable[..., Dict[str, str]],
    build_yahoo_quote_url_func: Callable[[str], str],
    data_mgr: Optional[MarketDataManager] = None,
    update_last_run_cfg: bool = True,
    industry_summary_df: Optional[pd.DataFrame] = None,
    cluster_summary_df: Optional[pd.DataFrame] = None,
    suggest_portfolio_candidates: Optional[Callable] = None,
    market_regime: Optional[Dict[str, Any]] = None,
    integrity_drops_df: Optional[pd.DataFrame] = None,
    universe_audit_df: Optional[pd.DataFrame] = None,
    watchlist_symbols: Optional[set] = None,
) -> None:
    return console_ui_core.render_analysis_output(
        stock_results=stock_results,
        portfolio_mgr=portfolio_mgr,
        selected_syms=selected_syms,
        etf_options=etf_options,
        update_last_run_cfg=update_last_run_cfg,
        data_mgr=data_mgr,
        config=config,
        logger=logger,
        build_multiscope_status_map=ranking_core.build_multiscope_status_map,
        sort_portfolio_items_by_rank=ranking_core.sort_portfolio_items_by_rank,
        _format_percent_value=ranking_core.format_percent_value,
        _format_percent_bar=ranking_core.format_percent_bar,
        _risk_bucket=ranking_core.risk_bucket,
        _shorten_text=ranking_core.shorten_text,
        build_yahoo_quote_url=build_yahoo_quote_url_func,
        build_etf_rsl_summary=summary_core.build_etf_rsl_summary,
        build_sector_rsl_summary=summary_core.build_sector_rsl_summary,
        industry_summary_df=industry_summary_df,
        cluster_summary_df=cluster_summary_df,
        save_excel_report_safely=save_excel_report_safely,
        save_dataframe_safely=save_dataframe_safely_func,
        save_json_config=save_json_config,
        build_console_symbols=build_console_symbols,
        suggest_portfolio_candidates=suggest_portfolio_candidates,
        market_regime=market_regime,
        integrity_drops_df=integrity_drops_df,
        universe_audit_df=universe_audit_df,
        watchlist_symbols=watchlist_symbols,
    )


def show_ticker_history_interactive(yf_module: Any = yf) -> None:
    ticker = input("\n[?] Yahoo Ticker (z.B. AAPL, SAP.DE): ").strip().upper()
    if not ticker:
        return
    days_in = input("[?] Anzahl Tage (Standard 130, ENTER): ").strip()
    try:
        days = int(days_in) if days_in else 130
    except ValueError:
        days = 130
    print(f"Lade Daten fuer {ticker}...")
    try:
        period = "2y" if days > 250 else "1y"
        ticker_obj = yf_module.Ticker(ticker)
        df = ticker_obj.history(period=period, actions=True, auto_adjust=False)
        if df.empty:
            print(f"\033[91mFehler: Keine Daten fuer {ticker} gefunden.\033[0m")
            return
        df = df.tail(days)
        print("\n" + "=" * 115)
        print(f" HISTORIE: {ticker} (letzte {len(df)} Handelstage)")
        print("=" * 115)
        print(f"{'Datum':<12} | {'Open':>9} | {'High':>9} | {'Low':>9} | {'Close':>9} | {'Adj Close':>9} | {'Volume':>12} | {'Dividende'}")
        print("-" * 115)
        for dt, row in df.iterrows():
            div = row.get("Dividends", 0.0)
            div_str = f"\033[92m{div:>10.3f} \U0001F4B0\033[0m" if div > 0 else f"{'-':>10}"
            color = "\033[93m" if div > 0 else ""
            reset = "\033[0m" if div > 0 else ""
            adj_c = row.get("Adj Close", row["Close"])
            print(f"{color}{dt.strftime('%Y-%m-%d'):<12} | {row['Open']:>9.2f} | {row['High']:>9.2f} | {row['Low']:>9.2f} | {row['Close']:>9.2f} | {adj_c:>9.2f} | {int(row['Volume']):>12,d} | {div_str}{reset}")
        print("=" * 115)
        print(f"Info: {len(df)} Zeilen angezeigt. Dividenden-Tage sind markiert.")
    except Exception as exc:
        print(f"\033[91mFehler beim Abrufen der Historie: {exc}\033[0m")


def auto_adjust_delays(
    config: Dict[str, Any], 
    load_json_config: Callable[..., Any], 
    save_json_config: Callable[[str, Any], None],
    logger: Any
) -> None:
    try:
        stats = load_json_config(config["run_stats_file"])
        last_hits = int(stats.get("last_rate_limit_hits", -1))
        if last_hits == -1:
            return
        min_s = float(config.get("batch_sleep_min_s", 0.5))
        max_s = float(config.get("batch_sleep_max_s", 1.5))
        info_s = float(config.get("info_fetch_delay_s", 0.7))
        if last_hits == 0:
            factor = 0.95
            min_s = max(0.05, min_s * factor)
            max_s = max(0.1, max_s * factor)
            info_s = max(0.05, info_s * factor)
            logger.info(f"AUTO-OPTIMIZE: Keine Rate-Limits im letzten Lauf. Delays werden verkuerzt (Faktor {factor}).")
        elif last_hits > 10:
            factor = 1.10
            min_s = min(2.0, min_s * factor)
            max_s = min(3.0, max_s * factor)
            info_s = min(2.5, info_s * factor)
            logger.info(f"AUTO-OPTIMIZE: {last_hits} Rate-Limits im letzten Lauf. Delays werden erhoeht (Faktor {factor}).")
        config["batch_sleep_min_s"], config["batch_sleep_max_s"], config["info_fetch_delay_s"] = min_s, max_s, info_s
        
        # Persistente Speicherung der optimierten Werte in user_settings.json
        user_settings = load_json_config(config["user_settings_file"])
        if isinstance(user_settings, dict):
            user_settings["batch_sleep_min_s"] = round(min_s, 3)
            user_settings["batch_sleep_max_s"] = round(max_s, 3)
            user_settings["info_fetch_delay_s"] = round(info_s, 3)
            save_json_config(config["user_settings_file"], user_settings)
            logger.info("AUTO-OPTIMIZE: Optimierte Werte wurden in den Benutzereinstellungen gespeichert.")

    except Exception:
        pass


def initialize_run_settings(
    data_mgr: MarketDataManager,
    config: Dict[str, Any],
    logger: Any,
    load_json_config: Callable[..., Any],
    save_json_config: Callable[[str, Any], None],
    currency_rates: Dict[str, Any],
) -> None:
    logger.info("\n--- INITIALISIERUNG ANALYSE-LAUF ---")
    auto_adjust_delays(config, load_json_config, save_json_config, logger)
    update_live_currency_rates(currency_rates, logger)
    if os.path.exists(config["history_cache_file"]):
        try:
            age = cache_age_hours(config["history_cache_file"])
            if age is not None and age < config["cache_duration_hours"]:
                logger.info(f"Kursdaten-Cache ist aktuell ({age:.1f}h alt).")
        except Exception:
            pass


def show_main_menu(has_snapshot: bool) -> str:
    print(f"\n\033[96m\033[1m{'-'*20} HAUPTMENUE {'-'*20}\033[0m")
    if has_snapshot:
        print("\033[94m [1]\033[0m \U0001F4C4 Letzten Datenstand neu anzeigen (Snapshot)")
    else:
        print(" [1] \033[90mLetzten Datenstand neu anzeigen (nicht verfuegbar)\033[0m")
    print("\033[92m [2]\033[0m \U0001F504 Neuen Lauf starten (Download & Analyse)")
    print("\033[93m [3]\033[0m \u2699\ufe0f  Einstellungen / Strategie-Anpassung")
    print("\033[95m [4]\033[0m \U0001F4C8 Historische Kursdaten anzeigen")
    print("\033[94m [5]\033[0m \U0001F4E5 Fundamentaldaten-Download (Market Cap, etc.)")
    print("\033[95m [6]\033[0m \U0001F50D Ad-hoc Analyse (Gezielte Ticker)")
    print("\033[96m [7]\033[0m \u2139\ufe0f  Hilfe: Integritaets-Fehler verstehen")
    print("\033[93m [8]\033[0m \u2b07\ufe0f  Original iShares CSV downloaden")
    print("\033[91m [0]\033[0m \u2716  Beenden")
    return input("Auswahl [2]: ").strip()


def prepare_ticker_universe(
    selected_syms: List[str],
    etf_options: Dict[str, Any],
    config: Dict[str, Any],
    logger: Any,
    make_progress_fn: Callable[..., Any],
    download_ishares_csv: Callable[..., pd.DataFrame],
) -> pd.DataFrame:
    etf_selection = [symbol for symbol in selected_syms if symbol in etf_options]
    wants_xetra = "XETRA" in [symbol.upper() for symbol in selected_syms]
    wants_fra = "FRA" in [symbol.upper() for symbol in selected_syms]
    wants_fd = "FDB" in [symbol.upper() for symbol in selected_syms]
    master_df = pd.DataFrame()
    if etf_selection:
        # Wir laden das Universum, behalten aber für das Audit die Roh-Daten im Hinterkopf
        master_df, total_raw = data_pipeline_core.load_selected_etf_universe(
            selected_syms=etf_selection,
            etf_options=etf_options,
            config=config,
            logger=logger,
            download_ishares_csv=download_ishares_csv,
            normalize_sector_name=normalize_sector_name,
            print_fn=print,
            progress_fn=make_progress_fn,
        )
    
    df = master_df
    if (wants_xetra or wants_fra) and config.get("exchange_scan_enabled", True):
        logger.info(f"Exchange-Integration gestartet (XETRA: {wants_xetra}, FRA: {wants_fra})")
        exchange_df = data_pipeline_core.load_exchange_universe(config, logger, normalize_sector_name)
        if not exchange_df.empty:
            if wants_xetra and not wants_fra:
                exchange_df = exchange_df[exchange_df["Listing_Source"] == "XETRA"]
            elif wants_fra and not wants_xetra:
                exchange_df = exchange_df[exchange_df["Listing_Source"] == "FRA"]
            
            # Zusammenführen und Duplikate zwischen ETF und Exchange entfernen
            df = pd.concat([df, exchange_df], ignore_index=True)

    if wants_fd:
        location_suffix_map = final_support_core.load_json_config(config.get("location_suffix_map_file", ""))
        exchange_suffix_map = final_support_core.load_json_config(config.get("exchange_suffix_map_file", ""))
        unsupported_exchanges = final_support_core.load_json_config(
            config.get("unsupported_exchanges_file", ""),
            is_list=True,
        )
        fd_df = financedatabase_universe_core.build_financedatabase_universe(
            existing_df=df,
            config=config,
            logger_obj=logger,
            location_suffix_map=location_suffix_map if isinstance(location_suffix_map, dict) else {},
            exchange_suffix_map=exchange_suffix_map if isinstance(exchange_suffix_map, dict) else {},
            unsupported_exchanges=unsupported_exchanges if isinstance(unsupported_exchanges, list) else [],
            normalize_sector_name=normalize_sector_name,
        )
        if not fd_df.empty:
            df = pd.concat([df, fd_df], ignore_index=True)

    if not df.empty:
        # Diese Logik muss IMMER laufen, nicht nur beim Exchange-Scan!
        # Spaltennamen normalisieren (Schutz gegen Isin/ISIN etc.)
        df.columns = ["Ticker" if str(c).upper() == "TICKER" else "ISIN" if str(c).upper() == "ISIN" else c for c in df.columns]

        # NEU: Namen- und Land-Enrichment VOR der Deduplizierung durchführen, 
        # damit Namen für das Gruppieren einheitlich sind (z.B. "Adyen N.V." vs "Adyen NV")
        for cache_key, file_key in [("etf_names_cache_file", "Name"), ("country_cache_file", "Land")]:
            cache_path = config.get(cache_key)
            if cache_path and os.path.exists(cache_path):
                cache_data = final_support_core.load_json_config(cache_path)
                if isinstance(cache_data, dict):
                    lookup = {str(key).upper(): value for key, value in cache_data.items()}
                    df[file_key] = df.apply(lambda row: lookup.get(str(row["Ticker"]).upper(), row.get(file_key, "")), axis=1)

        # ISIN-Enrichment aus Cache, um iShares (keine ISIN) mit Exchange (mit ISIN) zu matchen
        info_cache_path = config.get("ticker_info_cache_file")
        if info_cache_path and os.path.exists(info_cache_path):
            try:
                info_cache = final_support_core.load_json_config(info_cache_path)
                if isinstance(info_cache, dict):
                    # Ticker -> ISIN Mapping aus dem Yahoo-Info-Cache aufbauen
                    ticker_to_isin = {str(k).upper(): v.get("isin") for k, v in info_cache.items() if v.get("isin")}
                    
                    def _enrich_isin(row):
                        current_isin = str(row.get("ISIN", "")).strip().upper()
                        if len(current_isin) >= 10 and current_isin not in ("NAN", "NONE", "NULL", "0"):
                            return current_isin
                        ticker = str(row.get("Ticker", "")).strip().upper()
                        return ticker_to_isin.get(ticker, current_isin)
                    
                    if "ISIN" in df.columns:
                        df["ISIN"] = df.apply(_enrich_isin, axis=1)
            except Exception as e:
                logger.warning(f"ISIN-Enrichment im Universum fehlgeschlagen: {e}")

        # Erweiterte Prio-Logik fuer die Zusammenfuehrung:
        # Quelle (ETF > XETRA > FRA) kombiniert mit Ticker-Qualitaet (Native > US > .DE).
        def _calc_combined_prio(row):
            # Quellen-Priorität: ETF=1, XETRA=10, FRA=100
            source_val = 200
            if str(row.get("Source_ETF", "")).strip():
                source_val = 1
            elif row.get("Listing_Source") == "XETRA":
                source_val = 10
            elif row.get("Listing_Source") == "FRA":
                source_val = 100
            
            # Ticker-Qualität innerhalb der Quelle (z.B. Native Endung bevorzugen)
            ticker_val = data_pipeline_core._get_ticker_priority(row.get("Ticker"))
            return (source_val, ticker_val)

        # Identifikatoren bereinigen für korrektes Grouping
        if "ISIN" in df.columns:
            df["ISIN"] = df["ISIN"].astype(str).str.strip().str.upper().replace(["NAN", "NONE"], "")
        if "Ticker" in df.columns:
            df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()

        df["_prio"] = df.apply(_calc_combined_prio, axis=1)
        
        # Identifikator bestimmen: ISIN bevorzugt (jetzt dank Enrichment auch bei iShares),
        # sonst Ticker. Verhindert falsche Merges von leeren ISIN-Feldern.
        def _get_dedup_id(row):
            isin = str(row.get("ISIN", "")).strip().upper()
            if len(isin) >= 10 and isin not in ("NAN", "NONE", "NULL", "0"):
                return isin
            # NEU: Wenn ISIN fehlt, nutze den normalisierten Namen als Gruppen-ID
            name_key = normalize_name_for_dedup(row.get("Name", ""))
            if len(name_key) > 4:
                return f"NAME_{name_key}"
            return str(row.get("Ticker", "")).strip().upper()

        df["_dedup_id"] = df.apply(_get_dedup_id, axis=1)
        dedup_col = "_dedup_id"
        
        # Quellen und Börsenplätze vor der Deduplizierung sichern
        grouped = df.groupby(dedup_col)
        source_map = grouped["Source_ETF"].apply(final_support_core.merge_tokens).to_dict()
        listing_map = grouped["Listing_Source"].apply(final_support_core.merge_tokens).to_dict()
        # Namen und Länder ebenfalls konsolidieren
        name_map = grouped["Name"].first().to_dict() if "Name" in df.columns else {}
        land_map = grouped["Land"].first().to_dict() if "Land" in df.columns else {}
        
        # Jetzt deduplizieren (behält den Ticker mit der besten Priorität, z.B. US-Original)
        df = df.sort_values("_prio").drop_duplicates(subset=[dedup_col], keep="first").copy()
        
        # Gemergte Informationen wieder hinzufügen
        df["Source_ETF"] = df[dedup_col].map(source_map)
        df["Listing_Source"] = df[dedup_col].map(listing_map)
        if name_map:
            df["Name"] = df[dedup_col].map(name_map)
        if land_map:
            df["Land"] = df[dedup_col].map(land_map)
        
        df.drop(columns=["_prio"], errors="ignore", inplace=True)

    logger.info(f"Ticker-Universum Bereinigung beendet. Pool-Groesse: {len(df)}")
    return df


def save_home_market_rsl_audit(
    results: List[Any],
    config: Dict[str, Any],
    location_suffix_map: Dict[str, str],
    save_dataframe_safely_func: Callable[..., None],
) -> pd.DataFrame:
    audit_df = rsl_integrity_core.build_home_market_rsl_audit(results, location_suffix_map)
    shortlist_df = rsl_integrity_core.build_home_market_rsl_review_shortlist(
        audit_df,
        top_rank=int(config.get("home_market_rsl_review_top_rank", 300) or 300),
    )
    save_dataframe_safely_func(audit_df, config["home_market_rsl_audit_file"], sep=";", index=False, encoding="utf-8-sig")
    save_dataframe_safely_func(shortlist_df, config["home_market_rsl_review_file"], sep=";", index=False, encoding="utf-8-sig")
    return audit_df
