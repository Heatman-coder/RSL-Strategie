import logging

import pandas as pd

from core import financedatabase_universe as fd_universe
from core import final_support as final_support_core


def test_parse_etf_selection_input_supports_fdb_without_changing_all_behavior():
    opts = {"IVV": {"name": "iShares Core S&P 500 ETF"}}

    assert final_support_core.parse_etf_selection_input("FDB, FRA", opts) == ["FDB", "FRA"]
    assert final_support_core.parse_etf_selection_input("all", opts) == ["IVV", "XETRA", "FRA"]


def test_build_financedatabase_universe_filters_known_otc_and_missing_isin_rows():
    existing_df = pd.DataFrame(
        [
            {"Ticker": "AAPL", "ISIN": "", "Name": "Apple Inc."},
            {"Ticker": "MSFTALT", "ISIN": "US5949181045", "Name": "Microsoft Corporation"},
        ]
    )
    fd_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market": "New York Stock Exchange",
                "country": "United States",
                "isin": "US0378331005",
            },
            {
                "symbol": "MSFT",
                "name": "Microsoft Corporation",
                "sector": "Technology",
                "industry": "Software",
                "market": "NASDAQ Global Select",
                "country": "United States",
                "isin": "US5949181045",
            },
            {
                "symbol": "SHOP.TO",
                "name": "Shopify Inc.",
                "sector": "Technology",
                "industry": "Software",
                "market": "TSX Toronto Exchange",
                "country": "Canada",
                "isin": "CA82509L1076",
            },
            {
                "symbol": "0700.HK",
                "name": "Tencent Holdings Limited",
                "sector": "Communication Services",
                "industry": "Interactive Media & Services",
                "market": "Hong Kong Stock Exchange",
                "country": "Hong Kong",
                "isin": "KYG875721634",
            },
            {
                "symbol": "ABCD",
                "name": "Alpha Beta Corp.",
                "sector": "Industrials",
                "industry": "Machinery",
                "market": "OTC Bulletin Board",
                "country": "United States",
                "isin": "US0000000001",
            },
            {
                "symbol": "WEIRD.MX",
                "name": "Weird Missing Isin",
                "sector": "Industrials",
                "industry": "Tools",
                "market": "Bolsa Mexicana De Valores",
                "country": "Mexico",
                "isin": "",
            },
            {
                "symbol": "SEDA",
                "name": "Sedana Medical AB",
                "sector": "Health Care",
                "industry": "Health Care Equipment",
                "market": "NASDAQ OMX Stockholm",
                "country": "Sweden",
                "isin": "SE0000000002",
            },
        ]
    )
    config = {
        "financedatabase_enabled": True,
        "financedatabase_require_isin": True,
        "financedatabase_require_metadata": True,
        "financedatabase_allow_otc": False,
        "financedatabase_max_additions": 10,
    }
    location_suffix_map = {
        "Canada": ".TO",
        "Hong Kong": ".HK",
        "Mexico": ".MX",
        "Sweden": ".ST",
    }
    exchange_suffix_map = {
        "TSX Toronto Exchange": ".TO",
        "Hong Kong Stock Exchange": ".HK",
        "Bolsa Mexicana De Valores": ".MX",
        "NASDAQ OMX Stockholm": ".ST",
        "NASDAQ Global Select": "",
        "New York Stock Exchange": "",
    }

    result = fd_universe.build_financedatabase_universe(
        existing_df=existing_df,
        config=config,
        logger_obj=logging.getLogger("test"),
        location_suffix_map=location_suffix_map,
        exchange_suffix_map=exchange_suffix_map,
        unsupported_exchanges=[],
        normalize_sector_name=lambda value: str(value),
        fd_df=fd_df,
    )

    assert set(result["Ticker"].tolist()) == {"SHOP.TO", "0700.HK"}
    assert set(result["Listing_Source"].tolist()) == {"FDB"}
    assert set(result["Source_ETF"].tolist()) == {""}
