"""Eligible-universe enforcement + provider-aware telemetry + result visibility.

No live API/DB calls: persistence uses a fake connection, funnel runs use fake
providers/strategies.
"""

import asyncio

import app.workers.persistence as persistence
import app.workers.scanner.funnel as funnel
from app.workers.scanner.funnel import RESULT_SYMBOLS_CAP, build_data_source
from app.workers.strategies.base import StrategyDecision, StrategyResult


# --------------------------------------------------------------------------- #
# 1. Universe query enforces the STORED eligible classification
# --------------------------------------------------------------------------- #

class _FakeConn:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []

    async def fetch(self, query, *params):
        self.queries.append((query, params))
        return self.rows


def _patch_conn(monkeypatch, conn):
    async def fake_get():
        return conn

    async def fake_release(c):
        return None

    monkeypatch.setattr(persistence, "get_db_connection", fake_get)
    monkeypatch.setattr(persistence, "release_db_connection", fake_release)


def test_universe_query_filters_eligible_and_active(monkeypatch):
    """Ineligible securities (warrants/units/ETFs...) never enter Stage 0:
    the query trusts the stored classification, no suffix inference."""
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)

    asyncio.run(persistence.get_universe_tickers())
    query, params = conn.queries[0]
    assert "eligible = true" in query
    assert "is_active = true" in query
    assert "exchange = ANY($1)" in query
    # No suffix-based inference anywhere in the query.
    assert "LIKE" not in query.upper().replace("UNLIKE", "")


def test_universe_query_uses_configured_exchanges(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)
    asyncio.run(persistence.get_universe_tickers())
    _, params = conn.queries[0]
    # MIC codes from settings mapped to the legacy short names stored in DB.
    assert params[0] == ["NASDAQ", "NYSE", "AMEX"]


def test_universe_preserves_null_market_cap(monkeypatch):
    """Eligible-but-unenriched common stock stays in Stage 0 with NULL cap."""
    conn = _FakeConn(rows=[
        {"symbol": "NEWCS", "market_cap": None, "last_volume": 1e6, "exchange": "NASDAQ"},
    ])
    _patch_conn(monkeypatch, conn)

    rows = asyncio.run(persistence.get_universe_tickers())
    assert rows == [
        {"symbol": "NEWCS", "market_cap": None, "last_volume": 1e6, "exchange": "NASDAQ"}
    ]


def test_legacy_candidates_exclude_classified_ineligible(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)
    asyncio.run(persistence.get_candidate_tickers())
    query, _ = conn.queries[0]
    # Excludes eligible=false, tolerates unclassified legacy rows (NULL).
    assert "eligible IS NOT FALSE" in query


def test_eligible_null_cap_rejected_as_market_cap_unknown(monkeypatch):
    """Funnel-level: an eligible CS without market cap is in Stage 0 and gets
    the honest market_cap_unknown rejection (not silently dropped)."""
    async def universe(*a, **k):
        return [{"symbol": "NEWCS", "market_cap": None, "last_volume": 1e6, "exchange": "NASDAQ"}]

    async def cfg(*a, **k):
        return {"min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
                "min_price": 5.0, "score_threshold": 0.5}

    async def noop(*a, **k):
        return None

    monkeypatch.setattr(funnel, "get_universe_tickers", universe)
    monkeypatch.setattr(funnel, "resolve_pattern_config", cfg)
    monkeypatch.setattr(funnel, "create_scan_run", noop)
    monkeypatch.setattr(funnel, "finalize_scan_run", noop)

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=None, pattern_code="sma150_bounce", dry_run=True)
    )
    assert summary["stage_counts"]["stage_0_universe"] == 1
    assert summary["telemetry"]["rejection_reason_counts"] == {"market_cap_unknown": 1}


# --------------------------------------------------------------------------- #
# 2. Provider-aware telemetry
# --------------------------------------------------------------------------- #

def test_build_data_source():
    assert build_data_source("massive", dry_run=False) == "tickers_cache + massive_historical"
    assert build_data_source("fmp", dry_run=False) == "tickers_cache + fmp_historical"
    assert "no historical provider calls" in build_data_source("massive", dry_run=True)


class _NamedFakeProvider:
    def __init__(self, name, payloads):
        self.name = name
        self._payloads = payloads

    async def batch_historical_data(self, symbols, timeseries=350):
        return {s: self._payloads.get(s, {"historical": []}) for s in symbols}


def _run_named_provider_scan(monkeypatch, provider_name):
    async def universe(*a, **k):
        return [{"symbol": "GOOD", "market_cap": 5e9, "last_volume": 1e6, "exchange": "NASDAQ"}]

    async def cfg(*a, **k):
        return {"min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
                "min_price": 5.0, "score_threshold": 0.5}

    async def falsey(*a, **k):
        return False

    async def noop(*a, **k):
        return None

    class _Strategy:
        pattern_code = "sma150_bounce"
        min_daily_bars = 1

        def default_config(self):
            return {}

        def evaluate(self, df, context):
            return StrategyResult(
                decision=StrategyDecision.ENTER,
                symbol=context.symbol,
                pattern_code=context.pattern_code,
                score=0.9,
                reason="ok",
                details={"snapshot_date": "2026-07-17"},
            )

    monkeypatch.setattr(funnel, "get_universe_tickers", universe)
    monkeypatch.setattr(funnel, "resolve_pattern_config", cfg)
    monkeypatch.setattr(funnel, "create_scan_run", noop)
    monkeypatch.setattr(funnel, "finalize_scan_run", noop)
    monkeypatch.setattr(funnel, "was_seen_today", falsey)
    monkeypatch.setattr(funnel, "mark_seen_today", noop)
    monkeypatch.setattr(funnel, "save_signal", noop)
    monkeypatch.setattr(funnel, "get_strategy", lambda pc: _Strategy())
    monkeypatch.setattr(funnel, "cheap_prefilter", lambda df, mp, min_bars=1: None)

    bar = {"historical": [{"date": "2026-07-17", "open": 10, "high": 11, "low": 9,
                           "close": 10.5, "volume": 1e6}]}
    provider = _NamedFakeProvider(provider_name, {"GOOD": bar})
    return asyncio.run(
        funnel.run_funnel_scan(fmp=provider, pattern_code="sma150_bounce", dry_run=False)
    )


def test_massive_scan_telemetry_never_claims_fmp(monkeypatch):
    summary = _run_named_provider_scan(monkeypatch, "massive")
    t = summary["telemetry"]
    assert t["market_data_provider"] == "massive"
    assert t["data_source"] == "tickers_cache + massive_historical"
    assert "fmp" not in t["data_source"]
    assert summary["market_data_provider"] == "massive"


def test_fmp_scan_telemetry_identifies_fmp(monkeypatch):
    summary = _run_named_provider_scan(monkeypatch, "fmp")
    t = summary["telemetry"]
    assert t["market_data_provider"] == "fmp"
    assert t["data_source"] == "tickers_cache + fmp_historical"


def test_dry_run_telemetry_reports_no_provider_calls(monkeypatch):
    async def universe(*a, **k):
        return []

    async def cfg(*a, **k):
        return {"min_liquidity_filters": {}, "min_price": 5.0}

    async def noop(*a, **k):
        return None

    monkeypatch.setattr(funnel, "get_universe_tickers", universe)
    monkeypatch.setattr(funnel, "resolve_pattern_config", cfg)
    monkeypatch.setattr(funnel, "create_scan_run", noop)
    monkeypatch.setattr(funnel, "finalize_scan_run", noop)

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=None, pattern_code="sma150_bounce", dry_run=True)
    )
    t = summary["telemetry"]
    assert t["market_data_provider"] == "none"
    assert "no historical provider calls" in t["data_source"]
    assert t["api_call_counts"]["historical_fetches"] == 0


# --------------------------------------------------------------------------- #
# 3. Bounded result visibility
# --------------------------------------------------------------------------- #

def test_finished_summary_lists_enter_symbols(monkeypatch):
    summary = _run_named_provider_scan(monkeypatch, "massive")
    assert summary["enter_symbols"] == ["GOOD"]
    assert summary["evaluated_symbols"] == ["GOOD"]
    assert summary["watch_symbols"] == []
    # Also present in telemetry (the WS finished event payload).
    assert summary["telemetry"]["enter_symbols"] == ["GOOD"]


def test_result_symbol_lists_are_capped(monkeypatch):
    n = RESULT_SYMBOLS_CAP + 10

    async def universe(*a, **k):
        return [{"symbol": f"S{i:03d}", "market_cap": 5e9, "last_volume": 1e6,
                 "exchange": "NASDAQ"} for i in range(n)]

    async def cfg(*a, **k):
        return {"min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
                "min_price": 5.0}

    async def falsey(*a, **k):
        return False

    async def noop(*a, **k):
        return None

    class _WatchStrategy:
        pattern_code = "sma150_bounce"
        min_daily_bars = 1

        def default_config(self):
            return {}

        def evaluate(self, df, context):
            return StrategyResult(
                decision=StrategyDecision.WATCH,
                symbol=context.symbol,
                pattern_code=context.pattern_code,
                details={"snapshot_date": "2026-07-17"},
            )

    monkeypatch.setattr(funnel, "get_universe_tickers", universe)
    monkeypatch.setattr(funnel, "resolve_pattern_config", cfg)
    monkeypatch.setattr(funnel, "create_scan_run", noop)
    monkeypatch.setattr(funnel, "finalize_scan_run", noop)
    monkeypatch.setattr(funnel, "was_seen_today", falsey)
    monkeypatch.setattr(funnel, "mark_seen_today", noop)
    monkeypatch.setattr(funnel, "save_signal", noop)
    monkeypatch.setattr(funnel, "get_strategy", lambda pc: _WatchStrategy())
    monkeypatch.setattr(funnel, "cheap_prefilter", lambda df, mp, min_bars=1: None)

    bar = {"historical": [{"date": "2026-07-17", "open": 10, "high": 11, "low": 9,
                           "close": 10.5, "volume": 1e6}]}
    provider = _NamedFakeProvider("massive", {f"S{i:03d}": bar for i in range(n)})

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=provider, pattern_code="sma150_bounce", dry_run=False)
    )
    assert summary["stage_counts"]["watch_count"] == n
    assert len(summary["watch_symbols"]) == RESULT_SYMBOLS_CAP
    assert len(summary["evaluated_symbols"]) == RESULT_SYMBOLS_CAP
