"""Phase 5.1 — survivor-only 4H trigger support tests.

No live FMP or Supabase. The FMP client is either a fake object (funnel tests)
or a real FMPClient with `_request` monkeypatched (normalization tests).
"""

import asyncio

import numpy as np
import pandas as pd

import app.workers.scanner.funnel as funnel
from app.workers.fmp_client import FMPClient
from app.workers.strategies.wyckoff.strategy import WyckoffMTFStrategy


# --------------------------------------------------------------------------- #
# Builders (deterministic)
# --------------------------------------------------------------------------- #

def _ohlcv(dates, closes, volumes=None, spread=0.5):
    closes = np.asarray(closes, dtype=float)
    n = len(closes)
    vol = np.full(n, 1_000_000.0) if volumes is None else np.asarray(volumes, dtype=float)
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": closes,
            "high": closes + spread,
            "low": closes - spread,
            "close": closes,
            "volume": vol,
        }
    )


def _wyckoff_daily(n=560):
    """Uptrend -> low-ATR consolidation -> volume breakout (SOS)."""
    dates = pd.date_range("2021-01-01", periods=n, freq="B")
    closes = np.empty(n, dtype=float)
    vols = np.full(n, 1_000_000.0)
    trend_end = n - 61
    for i in range(trend_end):
        closes[i] = 50.0 + i * 0.3
    for j, i in enumerate(range(trend_end, n - 1)):
        closes[i] = 200.0 + 6.0 * np.sin(2 * np.pi * j / 30.0)
    closes[n - 1] = 215.0
    vols[n - 1] = 3_000_000.0
    df = _ohlcv(dates, closes, volumes=vols, spread=0.5)
    df.loc[n - 1, "high"] = 215.5
    df.loc[n - 1, "low"] = 210.0
    return df


def _flat_daily(n=560):
    dates = pd.date_range("2021-01-01", periods=n, freq="B")
    return _ohlcv(dates, np.full(n, 100.0))


def _daily_payload(df):
    """Oldest-first frame -> FMP newest-first daily payload."""
    rows = []
    for _, r in df.iloc[::-1].iterrows():
        rows.append(
            {
                "date": pd.to_datetime(r["date"]).strftime("%Y-%m-%d"),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            }
        )
    return {"historical": rows}


def _4h_bars_newest_first(n=20):
    """Rising 4H closes -> last close breaks the prior local high (LONG trigger)."""
    dates = pd.date_range("2023-06-01", periods=n, freq="4h")
    rows = []
    for i in range(n):
        close = 100.0 + i
        rows.append(
            {
                "date": dates[i].strftime("%Y-%m-%d %H:%M:%S"),
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 500_000.0,
            }
        )
    return list(reversed(rows))  # FMP returns newest-first


class _FakeFMP:
    """Tracks daily batch fetches and per-symbol 4H fetches."""

    def __init__(self, daily_payloads, payload_4h=None):
        self._daily = daily_payloads
        self._4h = payload_4h  # None -> empty/unsupported
        self.requested_daily = None
        self.four_hour_calls = []

    async def batch_historical_data(self, symbols, timeseries=350):
        self.requested_daily = list(symbols)
        return {s: self._daily.get(s, {"historical": []}) for s in symbols}

    async def fetch_historical_4h(self, symbol, limit=None):
        self.four_hour_calls.append(symbol)
        if self._4h is None:
            return {"symbol": symbol, "historical": []}
        return {"symbol": symbol, "historical": list(self._4h)}


class _RaisingFMP:
    async def batch_historical_data(self, *a, **k):
        raise AssertionError("daily FMP fetch must not happen here")

    async def fetch_historical_4h(self, *a, **k):
        raise AssertionError("4H FMP fetch must not happen here")


def _async(value):
    async def _f(*a, **k):
        return value
    return _f


def _patch_db(monkeypatch, universe, config):
    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(config))
    monkeypatch.setattr(funnel, "get_universe_tickers", _async(universe))
    monkeypatch.setattr(funnel, "log_pattern_run", _async("run-id"))
    monkeypatch.setattr(funnel, "was_seen_today", _async(False))
    monkeypatch.setattr(funnel, "mark_seen_today", _async(None))


_TICKER = {"market_cap": 5e9, "last_volume": 1e6}


# --------------------------------------------------------------------------- #
# FMP client 4H normalization (mocked _request only)
# --------------------------------------------------------------------------- #

def _client():
    return FMPClient(api_key="test-key", max_concurrent=1)


def test_fmp_4h_normalizes_and_limits(monkeypatch):
    client = _client()
    bars = _4h_bars_newest_first(20)
    called = {}

    async def fake_request(path, params=None):
        called["path"] = path
        return list(bars)

    monkeypatch.setattr(client, "_request", fake_request)

    payload = asyncio.run(client.fetch_historical_4h("AAA", limit=5))
    assert called["path"] == "/historical-chart/4hour/AAA"
    assert payload["symbol"] == "AAA"
    assert len(payload["historical"]) == 5
    # newest-first preserved -> limit keeps the most recent bars.
    assert payload["historical"][0]["close"] == 119.0

    # Same payload shape as daily -> to_dataframe sorts ascending.
    df = funnel.to_dataframe({"historical": payload["historical"]})
    assert list(df["close"]) == sorted(df["close"].tolist())


def test_fmp_4h_error_dict_and_exception_are_safe(monkeypatch):
    client = _client()

    async def error_dict(path, params=None):
        return {"Error Message": "endpoint not available on your plan"}

    monkeypatch.setattr(client, "_request", error_dict)
    payload = asyncio.run(client.fetch_historical_4h("AAA"))
    assert payload["historical"] == []

    async def boom(path, params=None):
        raise RuntimeError("403 forbidden")

    monkeypatch.setattr(client, "_request", boom)
    payload = asyncio.run(client.fetch_historical_4h("AAA"))
    assert payload["historical"] == []


# --------------------------------------------------------------------------- #
# Funnel gating
# --------------------------------------------------------------------------- #

def test_dry_run_never_fetches_4h_even_when_enabled(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=_RaisingFMP(),
            pattern_code="wyckoff_mtf",
            dry_run=True,
            scanner_config={"enable_expensive_stages": True},
        )
    )
    assert summary["dry_run"] is True
    assert summary["telemetry"]["api_call_counts"]["four_hour_fetches"] == 0


def test_disabled_expensive_stages_no_4h_fetch(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()  # enable_4h_trigger False
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)
    monkeypatch.setattr(funnel, "save_signal", _async("id"))

    fake = _FakeFMP({"AAA": _daily_payload(_wyckoff_daily())}, payload_4h=_4h_bars_newest_first())
    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=fake, pattern_code="wyckoff_mtf", dry_run=False)
    )

    assert fake.four_hour_calls == []
    sc = summary["stage_counts"]
    assert sc["watch_count"] == 1  # stays WATCH without 4H
    assert sc["enter_count"] == 0
    assert sc["stage_4_4h_fetched"] == 0


def test_4h_converts_watch_to_enter_for_survivors_only(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(
        monkeypatch,
        [{"symbol": "GOOD", **_TICKER}, {"symbol": "FLAT", **_TICKER}],
        cfg,
    )

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return "sig-id"

    monkeypatch.setattr(funnel, "save_signal", fake_save)

    fake = _FakeFMP(
        {"GOOD": _daily_payload(_wyckoff_daily()), "FLAT": _daily_payload(_flat_daily())},
        payload_4h=_4h_bars_newest_first(),
    )
    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="wyckoff_mtf",
            dry_run=False,
            scanner_config={"enable_expensive_stages": True},
        )
    )

    # 4H fetched ONLY for the WATCH survivor, never the monthly-neutral reject.
    assert fake.four_hour_calls == ["GOOD"]
    sc = summary["stage_counts"]
    assert sc["stage_4_4h_fetched"] == 1
    assert sc["enter_count"] == 1
    assert sc["watch_count"] == 0
    assert sc["reject_count"] == 1
    assert summary["telemetry"]["api_call_counts"]["four_hour_fetches"] == 1

    # The saved ENTER signal carries side/entry/stop for outcome tracking.
    assert len(saved) == 1
    details = saved[0]["details"]
    assert details["side"] == "LONG"
    assert details["entry_price"] is not None
    assert details["stop_price"] is not None
    assert details["setup_type"] == "sos"


def test_4h_unavailable_keeps_watch(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)
    monkeypatch.setattr(funnel, "save_signal", _async("id"))

    fake = _FakeFMP({"AAA": _daily_payload(_wyckoff_daily())}, payload_4h=None)
    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="wyckoff_mtf",
            dry_run=False,
            scanner_config={"enable_expensive_stages": True},
        )
    )

    assert fake.four_hour_calls == ["AAA"]  # attempted, endpoint empty
    sc = summary["stage_counts"]
    assert sc["watch_count"] == 1  # no fake data -> stays WATCH
    assert sc["enter_count"] == 0


def test_no_4h_fetch_for_sma150(monkeypatch):
    from app.workers.strategies.sma150_adapter import Sma150BounceStrategy

    cfg = Sma150BounceStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)
    monkeypatch.setattr(funnel, "save_signal", _async("id"))

    # 250 flat daily bars pass sma150's prefilter; verdict will be AVOID.
    dates = pd.date_range("2022-01-01", periods=250, freq="B")
    daily = _ohlcv(dates, 50.0 + np.arange(250) * 0.1)
    fake = _FakeFMP({"AAA": _daily_payload(daily)}, payload_4h=_4h_bars_newest_first())

    asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="sma150_bounce",
            dry_run=False,
            scanner_config={"enable_expensive_stages": True},
        )
    )
    # sma150 does not declare 4H -> never fetched even with expensive enabled.
    assert fake.four_hour_calls == []


def test_limit_bounds_4h_fetches(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    universe = [{"symbol": f"S{i}", **_TICKER} for i in range(3)]
    _patch_db(monkeypatch, universe, cfg)
    monkeypatch.setattr(funnel, "save_signal", _async("id"))

    payload = _daily_payload(_wyckoff_daily())
    fake = _FakeFMP({t["symbol"]: payload for t in universe}, payload_4h=_4h_bars_newest_first())

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="wyckoff_mtf",
            limit=2,
            dry_run=False,
            scanner_config={"enable_expensive_stages": True},
        )
    )
    # limit bounds daily fetches, and 4H fetches are a subset of those.
    assert len(fake.requested_daily) == 2
    assert len(fake.four_hour_calls) <= 2
    assert summary["telemetry"]["api_call_counts"]["four_hour_fetches"] <= 2
