import logging
import os
import re
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from .entity_matching import normalize_name_for_dedup
from .etf_processor import is_plausible_ticker, sanitize_ticker_symbol


logger = logging.getLogger(__name__)

_FD_DEFAULT_COLUMNS = [
    "symbol",
    "name",
    "summary",
    "sector",
    "industry_group",
    "industry",
    "exchange",
    "market",
    "country",
    "currency",
    "isin",
]

_PRIMARY_US_MARKETS = {
    "New York Stock Exchange",
    "NASDAQ Global Select",
    "NASDAQ Global Market",
    "NASDAQ Capital Market",
    "NYSE MKT",
}

_OTC_MARKET_TOKENS = ("OTC", "PINK", "GREY")
_VALID_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null"} else text


def _clean_isin(value: Any) -> str:
    text = _clean_text(value).upper().replace(" ", "")
    return text if _VALID_ISIN_RE.match(text) else ""


def _normalize_ticker_for_match(value: Any) -> str:
    text = _clean_text(value).upper()
    for token in (".", "-", "/", " ", "_"):
        text = text.replace(token, "")
    return text


def _normalize_suffix(value: Any) -> str:
    text = _clean_text(value).upper()
    if not text:
        return ""
    return text if text.startswith(".") else f".{text}"


def _build_existing_identity_sets(existing_df: pd.DataFrame) -> Dict[str, Set[str]]:
    if existing_df is None or existing_df.empty:
        return {"ticker": set(), "ticker_norm": set(), "isin": set(), "name": set()}

    ticker_set = {
        _clean_text(value).upper()
        for value in existing_df.get("Ticker", pd.Series(dtype=object)).tolist()
        if _clean_text(value)
    }
    ticker_norm_set = {_normalize_ticker_for_match(value) for value in ticker_set if value}
    isin_set = {
        _clean_isin(value)
        for value in existing_df.get("ISIN", pd.Series(dtype=object)).tolist()
        if _clean_isin(value)
    }
    name_set = {
        normalize_name_for_dedup(value)
        for value in existing_df.get("Name", pd.Series(dtype=object)).tolist()
        if len(normalize_name_for_dedup(value)) > 4
    }
    return {
        "ticker": ticker_set,
        "ticker_norm": ticker_norm_set,
        "isin": isin_set,
        "name": name_set,
    }


def _standardize_fd_frame(
    fd_df: pd.DataFrame,
    normalize_sector_name: Optional[Any] = None,
) -> pd.DataFrame:
    if fd_df is None or fd_df.empty:
        return pd.DataFrame()

    work = fd_df.copy()
    if "symbol" not in work.columns and work.index.name:
        work = work.reset_index().rename(columns={work.index.name: "symbol"})
    elif "symbol" not in work.columns and work.index is not None:
        work = work.reset_index().rename(columns={"index": "symbol"})

    rename_map = {
        "symbol": "Ticker",
        "name": "Name",
        "summary": "Summary",
        "sector": "Sector",
        "industry_group": "Industry_Group",
        "industry": "Industry",
        "market": "Market",
        "country": "Land",
        "currency": "Currency",
        "isin": "ISIN",
    }
    work = work.rename(columns=rename_map)

    if "Exchange" not in work.columns:
        if "Market" in work.columns:
            work["Exchange"] = work["Market"]
        elif "exchange" in work.columns:
            work["Exchange"] = work["exchange"]
        else:
            work["Exchange"] = ""
    elif "Market" in work.columns:
        work["Exchange"] = work["Market"].where(
            work["Market"].astype(str).str.strip().ne(""),
            work["Exchange"],
        )

    for column in [
        "Ticker",
        "Name",
        "Summary",
        "Sector",
        "Industry_Group",
        "Industry",
        "Exchange",
        "Market",
        "Land",
        "Currency",
        "ISIN",
    ]:
        if column not in work.columns:
            work[column] = ""

    work["Ticker"] = work["Ticker"].map(lambda value: sanitize_ticker_symbol(_clean_text(value)))
    work = work[work["Ticker"].map(is_plausible_ticker)].copy()
    work["Ticker"] = work["Ticker"].astype(str).str.upper()
    work["ISIN"] = work["ISIN"].map(_clean_isin)
    work["Name"] = work["Name"].map(_clean_text)
    work["Sector"] = work["Sector"].map(_clean_text)
    work["Industry"] = work["Industry"].map(_clean_text)
    work["Industry_Group"] = work["Industry_Group"].map(_clean_text)
    work["Exchange"] = work["Exchange"].map(_clean_text)
    work["Market"] = work["Market"].map(_clean_text)
    work["Land"] = work["Land"].map(_clean_text)
    work["Currency"] = work["Currency"].map(_clean_text)

    if normalize_sector_name is not None and "Sector" in work.columns:
        work["Sector"] = work["Sector"].map(normalize_sector_name)

    work["Source_ETF"] = ""
    work["Listing_Source"] = "FDB"
    work["Universe_Source"] = "FDB"
    return work


def load_financedatabase_equities(
    config: Dict[str, Any],
    logger_obj: Any,
    normalize_sector_name: Optional[Any] = None,
) -> pd.DataFrame:
    snapshot_file = _clean_text(config.get("financedatabase_snapshot_file"))
    use_live_package = bool(config.get("financedatabase_use_live_package", True))

    if snapshot_file and os.path.exists(snapshot_file):
        _, ext = os.path.splitext(snapshot_file.lower())
        try:
            if ext == ".csv":
                usecols = lambda column: str(column).strip().lower() in set(_FD_DEFAULT_COLUMNS)  # noqa: E731
                fd_df = pd.read_csv(snapshot_file, usecols=usecols)
            elif ext in {".xlsx", ".xls"}:
                fd_df = pd.read_excel(snapshot_file)
            else:
                fd_df = pd.read_csv(snapshot_file)
            logger_obj.info(f"FinanceDatabase Snapshot geladen: {snapshot_file}")
            return _standardize_fd_frame(fd_df, normalize_sector_name=normalize_sector_name)
        except Exception as exc:
            logger_obj.warning(f"FinanceDatabase Snapshot konnte nicht geladen werden ({snapshot_file}): {exc}")

    if not use_live_package:
        return pd.DataFrame()

    try:
        import financedatabase as fd  # type: ignore

        equities = fd.Equities()
        fd_df = equities.select().reset_index().rename(columns={"index": "symbol"})
        logger_obj.info("FinanceDatabase live via Python-Paket geladen.")
        return _standardize_fd_frame(fd_df, normalize_sector_name=normalize_sector_name)
    except Exception as exc:
        logger_obj.warning(f"FinanceDatabase live nicht verfuegbar, FD-Integration wird uebersprungen: {exc}")
        return pd.DataFrame()


def build_financedatabase_universe(
    existing_df: pd.DataFrame,
    config: Dict[str, Any],
    logger_obj: Any,
    location_suffix_map: Dict[str, str],
    exchange_suffix_map: Dict[str, str],
    unsupported_exchanges: List[str],
    normalize_sector_name: Optional[Any] = None,
    fd_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if not bool(config.get("financedatabase_enabled", True)):
        return pd.DataFrame()

    work = (
        _standardize_fd_frame(fd_df, normalize_sector_name=normalize_sector_name)
        if fd_df is not None
        else load_financedatabase_equities(config, logger_obj, normalize_sector_name=normalize_sector_name)
    )
    if work.empty:
        return pd.DataFrame()

    existing_sets = _build_existing_identity_sets(existing_df)
    require_isin = bool(config.get("financedatabase_require_isin", True))
    require_metadata = bool(config.get("financedatabase_require_metadata", True))
    allow_otc = bool(config.get("financedatabase_allow_otc", False))
    max_additions = int(config.get("financedatabase_max_additions", 4000) or 0)
    unsupported_set = {str(item).strip().upper() for item in unsupported_exchanges if str(item).strip()}

    work["_ticker_norm"] = work["Ticker"].map(_normalize_ticker_for_match)
    work["_name_norm"] = work["Name"].map(normalize_name_for_dedup)
    work["_market_upper"] = work["Market"].astype(str).str.upper()
    work["_home_suffix"] = work["Land"].map(lambda value: _normalize_suffix(location_suffix_map.get(value, "")))
    work["_market_suffix"] = work["Market"].map(lambda value: _normalize_suffix(exchange_suffix_map.get(value, "")))
    work["_has_isin"] = work["ISIN"].map(bool)
    work["_has_metadata"] = (
        work["Name"].astype(str).str.len().gt(0)
        & work["Land"].astype(str).str.len().gt(0)
        & work["Sector"].astype(str).str.len().gt(0)
        & work["Industry"].astype(str).str.len().gt(0)
    )
    work["_is_primary"] = ~work["Ticker"].astype(str).str.contains(r"\.", na=False)
    work["_is_home_suffix"] = work.apply(
        lambda row: bool(row["_home_suffix"]) and str(row["Ticker"]).endswith(str(row["_home_suffix"])),
        axis=1,
    )
    work["_is_market_suffix"] = work.apply(
        lambda row: bool(row["_market_suffix"]) and str(row["Ticker"]).endswith(str(row["_market_suffix"])),
        axis=1,
    )
    work["_is_us_primary"] = work["Market"].isin(_PRIMARY_US_MARKETS) & work["_is_primary"]
    work["_is_supported_symbol"] = work["_is_home_suffix"] | work["_is_market_suffix"] | work["_is_us_primary"]
    work["_is_otc"] = work["_market_upper"].map(
        lambda value: any(token in value for token in _OTC_MARKET_TOKENS) or value in unsupported_set
    )
    work["_known_by_ticker"] = work["Ticker"].isin(existing_sets["ticker"])
    work["_known_by_ticker_norm"] = work["_ticker_norm"].isin(existing_sets["ticker_norm"])
    work["_known_by_isin"] = work["ISIN"].isin(existing_sets["isin"]) & work["_has_isin"]
    work["_known_by_name"] = work["_name_norm"].isin(existing_sets["name"]) & work["_name_norm"].astype(str).str.len().gt(4)
    work["_is_known"] = (
        work["_known_by_ticker"]
        | work["_known_by_ticker_norm"]
        | work["_known_by_isin"]
        | work["_known_by_name"]
    )

    keep_mask = work["_is_supported_symbol"] & ~work["_is_known"]
    if require_isin:
        keep_mask &= work["_has_isin"]
    if require_metadata:
        keep_mask &= work["_has_metadata"]
    if not allow_otc:
        keep_mask &= ~work["_is_otc"]

    filtered = work[keep_mask].copy()
    if filtered.empty:
        logger_obj.info("FinanceDatabase: Keine neuen, kompatiblen Kandidaten nach defensiver Filterung.")
        return pd.DataFrame()

    filtered["_fd_score"] = (
        filtered["_has_isin"].astype(int) * 100
        + filtered["_has_metadata"].astype(int) * 20
        + filtered["_is_home_suffix"].astype(int) * 10
        + filtered["_is_us_primary"].astype(int) * 9
        + filtered["_is_market_suffix"].astype(int) * 8
    )
    filtered["_dedup_id"] = filtered.apply(
        lambda row: (
            row["ISIN"]
            if row["ISIN"]
            else f"NAME_{row['_name_norm']}"
            if len(str(row["_name_norm"])) > 4
            else row["Ticker"]
        ),
        axis=1,
    )
    filtered = filtered.sort_values(
        by=["_fd_score", "_is_home_suffix", "_is_us_primary", "_is_market_suffix", "Ticker"],
        ascending=[False, False, False, False, True],
    ).drop_duplicates(subset=["_dedup_id"], keep="first")

    if max_additions > 0:
        filtered = filtered.head(max_additions).copy()

    keep_columns = [
        "Ticker",
        "Name",
        "Summary",
        "Sector",
        "Industry_Group",
        "Industry",
        "Exchange",
        "Market",
        "Land",
        "Currency",
        "ISIN",
        "Source_ETF",
        "Listing_Source",
        "Universe_Source",
    ]
    result = filtered[keep_columns].copy()
    logger_obj.info(
        "FinanceDatabase: %s neue Kandidaten integriert (Quelle=%s, require_isin=%s, require_metadata=%s).",
        len(result),
        _clean_text(config.get("financedatabase_snapshot_file")) or "live-package",
        require_isin,
        require_metadata,
    )
    return result
