"""Massive field mapping, classification, pre-screen and store semantics."""

import asyncio
from datetime import date, datetime, timezone

from app.workers import market_store
from app.workers.indicators import to_dataframe
from app.workers.massive_client import (
    bars_to_fmp_payload,
    map_agg_bar,
    map_grouped_row,
    ms_to_trading_date,
)
from app.workers.screening import (
    classify_ticker,
    dollar_volume,
    enrichment_status_for,
    needs_profile_refresh,
    prescreen_bar,
    prescreen_bars,
)


def _ms(y, m, d, hour=20):
    return int(datetime(y, m, d, hour, tzinfo=timezone.utc).timestamp() * 1000)


GROUPED_ROW = {
    "T": "AAPL",
    "o": 210.0,
    "h": 215.5,
    "l": 209.0,
    "c": 214.2,
    "v": 55_000_000,
    "vw": 213.1,
    "t": _ms(2026, 7, 17),
    "n": 480_000,
}


# --------------------------------------------------------------------------- #
# Field mapping
# --------------------------------------------------------------------------- #

def test_grouped_row_maps_all_fields():
    bar = map_grouped_row(GROUPED_ROW)
    assert bar == {
        "symbol": "AAPL",
        "trading_date": date(2026, 7, 17),
        "open": 210.0,
        "high": 215.5,
        "low": 209.0,
        "close": 214.2,
        "volume": 55_000_000.0,
        "vwap": 213.1,
        "transaction_count": 480_000,
    }


def test_grouped_row_optional_fields_missing():
    row = {k: v for k, v in GROUPED_ROW.items() if k not in ("vw", "n")}
    bar = map_grouped_row(row)
    assert bar["vwap"] is None
    assert bar["transaction_count"] is None


def test_grouped_row_invalid_returns_none():
    assert map_grouped_row({"T": "AAPL"}) is None            # missing OHLCV
    assert map_grouped_row({**GROUPED_ROW, "T": ""}) is None  # missing symbol
    assert map_grouped_row({**GROUPED_ROW, "t": "junk"}) is None


def test_ms_to_trading_date():
    assert ms_to_trading_date(_ms(2026, 7, 17)) == date(2026, 7, 17)
    assert ms_to_trading_date(None) is None


def test_agg_bar_mapping_and_fmp_payload_compat():
    bar = map_agg_bar("MSFT", {k: v for k, v in GROUPED_ROW.items() if k != "T"})
    assert bar["symbol"] == "MSFT"

    payload = bars_to_fmp_payload("MSFT", [bar])
    assert payload["symbol"] == "MSFT"
    assert payload["historical"][0]["date"] == "2026-07-17"
    # The canonical payload must be consumable by the existing pipeline.
    df = to_dataframe(payload)
    assert len(df) == 1
    assert float(df.iloc[0]["close"]) == 214.2


# --------------------------------------------------------------------------- #
# Ticker classification (type/exchange metadata, not suffixes)
# --------------------------------------------------------------------------- #

_ALLOWED_EX = ["XNAS", "XNYS", "XASE"]
_ALLOWED_TYPES = ["CS"]


def _ref(**overrides):
    base = {
        "ticker": "AAPL",
        "active": True,
        "locale": "us",
        "market": "stocks",
        "type": "CS",
        "primary_exchange": "XNAS",
    }
    base.update(overrides)
    return base


def test_classification_accepts_common_stock():
    ok, reason = classify_ticker(_ref(), _ALLOWED_EX, _ALLOWED_TYPES)
    assert ok is True and reason is None


def test_classification_rejections():
    cases = [
        (_ref(active=False), "inactive"),
        (_ref(locale="gb"), "not_us"),
        (_ref(market="otc"), "otc_excluded"),
        (_ref(market="crypto"), "not_stocks_market"),
        (_ref(type="ETF"), "type_not_allowed"),
        (_ref(type="WARRANT"), "type_not_allowed"),
        (_ref(type="PFD"), "type_not_allowed"),
        (_ref(primary_exchange="XLON"), "exchange_not_allowed"),
    ]
    for ticker, expected in cases:
        ok, reason = classify_ticker(ticker, _ALLOWED_EX, _ALLOWED_TYPES)
        assert ok is False and reason == expected


def test_classification_otc_opt_in():
    ok, _ = classify_ticker(
        _ref(market="otc", primary_exchange=""), _ALLOWED_EX, _ALLOWED_TYPES, include_otc=True
    )
    assert ok is True


# --------------------------------------------------------------------------- #
# Local pre-screen
# --------------------------------------------------------------------------- #

def _bar(symbol="AAA", close=50.0, volume=500_000):
    return {"symbol": symbol, "close": close, "volume": volume}


def test_dollar_volume_is_close_times_volume():
    assert dollar_volume(_bar(close=10.0, volume=1000)) == 10_000.0


def test_prescreen_bar_rules():
    assert prescreen_bar(_bar(), 1.0, 100_000, 1_000_000) == (True, None)
    assert prescreen_bar(_bar(close=0.5), 1.0, 100_000, 1_000_000) == (False, "price_below_min")
    assert prescreen_bar(_bar(volume=50_000), 1.0, 100_000, 1_000_000) == (False, "volume_below_min")
    # 2.0 * 200k = 400k dollar volume < 1M
    assert prescreen_bar(_bar(close=2.0, volume=200_000), 1.0, 100_000, 1_000_000) == (
        False, "dollar_volume_below_min",
    )
    assert prescreen_bar({"symbol": "X"}, 1.0, 0, 0) == (False, "invalid_bar")


def test_prescreen_bars_respects_universe_and_counts_reasons():
    bars = [
        _bar("GOOD"),
        _bar("CHEAP", close=0.5),
        _bar("ALIEN"),  # not in eligible universe
    ]
    passed, reasons = prescreen_bars(bars, {"GOOD", "CHEAP"}, 1.0, 100_000, 1_000_000)
    assert passed == ["GOOD"]
    assert reasons == {"price_below_min": 1, "not_in_universe": 1}


# --------------------------------------------------------------------------- #
# Profile cache + enrichment status
# --------------------------------------------------------------------------- #

def test_needs_profile_refresh():
    now = datetime(2026, 7, 20)
    assert needs_profile_refresh(None, now, 7) is True
    assert needs_profile_refresh(datetime(2026, 7, 18), now, 7) is False
    assert needs_profile_refresh(datetime(2026, 7, 1), now, 7) is True


def test_missing_market_cap_is_flagged_not_zeroed():
    assert enrichment_status_for(None) == "missing_market_cap"
    assert enrichment_status_for(5e9) == "enriched"


# --------------------------------------------------------------------------- #
# Idempotent bar ingestion (store layer with a fake connection)
# --------------------------------------------------------------------------- #

class _FakeConn:
    def __init__(self):
        self.executemany_calls = []

    async def executemany(self, sql, args):
        self.executemany_calls.append((sql, list(args)))


def test_bulk_upsert_daily_bars_idempotent_sql(monkeypatch):
    conn = _FakeConn()

    async def fake_get():
        return conn

    async def fake_release(c):
        return None

    monkeypatch.setattr(market_store, "get_db_connection", fake_get)
    monkeypatch.setattr(market_store, "release_db_connection", fake_release)

    bar = map_grouped_row(GROUPED_ROW)
    # Ingest the same bar twice — upsert semantics make this safe/idempotent.
    n1 = asyncio.run(market_store.bulk_upsert_daily_bars([bar]))
    n2 = asyncio.run(market_store.bulk_upsert_daily_bars([bar]))
    assert (n1, n2) == (1, 1)

    sql, args = conn.executemany_calls[0]
    assert "ON CONFLICT (symbol, trading_date) DO UPDATE" in sql
    assert args[0][0] == "AAPL"
    assert args[0][1] == date(2026, 7, 17)
    # Both ingests produced identical parameter tuples (deterministic mapping).
    assert conn.executemany_calls[0][1] == conn.executemany_calls[1][1]


def test_bulk_upsert_empty_is_noop(monkeypatch):
    called = {"n": 0}

    async def fake_get():
        called["n"] += 1
        raise AssertionError("no connection needed for empty input")

    monkeypatch.setattr(market_store, "get_db_connection", fake_get)
    assert asyncio.run(market_store.bulk_upsert_daily_bars([])) == 0
    assert called["n"] == 0
