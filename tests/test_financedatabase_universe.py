import logging

import pandas as pd
import pytest

from core import app_support
from core import financedatabase_universe as fd_universe
from core import final_support as final_support_core


def test_parse_etf_selection_input_supports_fdb_and_includes_it_in_all():
    opts = {"IVV": {"name": "iShares Core S&P 500 ETF"}}

    assert final_support_core.parse_etf_selection_input("FDB, FRA", opts) == ["FDB", "FRA"]
    assert final_support_core.parse_etf_selection_input("all", opts) == ["IVV", "XETRA", "FRA", "FDB"]


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


def test_audit_financedatabase_universe_reports_rejection_reasons():
    fd_df = pd.DataFrame(
        [
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
                "symbol": "ABCD",
                "name": "Alpha Beta Corp.",
                "sector": "Industrials",
                "industry": "Machinery",
                "market": "OTC Bulletin Board",
                "country": "United States",
                "isin": "US0000000001",
            },
            {
                "symbol": "MISS.MX",
                "name": "Missing Isin S.A.",
                "sector": "Industrials",
                "industry": "Tools",
                "market": "Bolsa Mexicana De Valores",
                "country": "Mexico",
                "isin": "",
            },
        ]
    )
    audit = fd_universe.audit_financedatabase_universe(
        existing_df=pd.DataFrame(columns=["Ticker", "ISIN", "Name"]),
        config={
            "financedatabase_enabled": True,
            "financedatabase_require_isin": True,
            "financedatabase_require_metadata": True,
            "financedatabase_allow_otc": False,
            "financedatabase_max_additions": 10,
        },
        logger_obj=logging.getLogger("test"),
        location_suffix_map={"Canada": ".TO", "Mexico": ".MX"},
        exchange_suffix_map={
            "TSX Toronto Exchange": ".TO",
            "Bolsa Mexicana De Valores": ".MX",
        },
        unsupported_exchanges=[],
        normalize_sector_name=lambda value: str(value),
        fd_df=fd_df,
    )

    assert audit["base_universe_size"] == 0
    assert audit["accepted_size"] == 1
    assert audit["after_universe_size"] == 1
    rejected = audit["rejected_df"]
    reasons = dict(zip(rejected["Ticker"], rejected["rejection_reasons"]))
    assert reasons["ABCD"] == "unsupported_symbol;otc_or_unsupported_market"
    assert reasons["MISS.MX"] == "missing_isin"


def test_prepare_ticker_universe_uses_financedatabase_builder_when_selected(monkeypatch: pytest.MonkeyPatch):
    called = {"fd": False}

    def _load_selected_etf_universe(**kwargs):
        return (
            pd.DataFrame(
                [
                    {
                        "Ticker": "IVVBASE",
                        "Name": "Base Holding",
                        "Sector": "Technology",
                        "Industry": "Software",
                        "Land": "United States",
                        "ISIN": "US0000000002",
                        "Source_ETF": "IVV",
                        "Listing_Source": "",
                    }
                ]
            ),
            1,
        )

    def _fd_builder(**kwargs):
        called["fd"] = True
        existing_df = kwargs["existing_df"]
        assert "IVVBASE" in set(existing_df["Ticker"].tolist())
        return pd.DataFrame(
            [
                {
                    "Ticker": "SHOP.TO",
                    "Name": "Shopify Inc.",
                    "Sector": "Technology",
                    "Industry": "Software",
                    "Land": "Canada",
                    "ISIN": "CA82509L1076",
                    "Source_ETF": "",
                    "Listing_Source": "FDB",
                }
            ]
        )

    monkeypatch.setattr(app_support.data_pipeline_core, "load_selected_etf_universe", _load_selected_etf_universe)
    monkeypatch.setattr(app_support.financedatabase_universe_core, "build_financedatabase_universe", _fd_builder)

    out = app_support.prepare_ticker_universe(
        selected_syms=["IVV", "FDB"],
        etf_options={"IVV": {"name": "iShares Core S&P 500 ETF"}},
        config={
            "exchange_scan_enabled": False,
            "location_suffix_map_file": "",
            "exchange_suffix_map_file": "",
            "unsupported_exchanges_file": "",
            "etf_names_cache_file": "",
            "country_cache_file": "",
            "ticker_info_cache_file": "",
        },
        logger=logging.getLogger("test"),
        make_progress_fn=None,
        download_ishares_csv=lambda *args, **kwargs: pd.DataFrame(),
    )

    assert called["fd"] is True
    assert {"IVVBASE", "SHOP.TO"} <= set(out["Ticker"].tolist())
