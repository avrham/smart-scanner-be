"""Orchestrator tests for run_funnel_scan.

No live FMP or Supabase: DB helpers are monkeypatched and FMP is a fake. Async
entrypoints are driven with asyncio.run so no pytest plugin is required.
"""

import asyncio

import app.workers.scanner.funnel as funnel


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


def _fmp_history(n=210, price=50.0):
    # FMP returns newest-first; to_dataframe re-sorts. Dates descending.
    rows = []
    for i in range(n):
        rows.append(
            {
                "date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price,
                "volume": 1_000_000,
            }
        )
    return {"historical": rows}


class _RaisingFMP:
    """Any call means the funnel touched FMP when it must not have."""

    def __init__(self):
        self.called = False

    async def batch_historical_data(self, *a, **k):
        self.called = True
        raise AssertionError("FMP must not be called in dry_run")


class _FakeFMP:
    def __init__(self, payloads):
        self._payloads = payloads
        self.requested = None

    async def batch_historical_data(self, symbols, timeseries=350):
        self.requested = list(symbols)
        return {s: self._payloads.get(s, {"historical": []}) for s in symbols}


_PATTERN_CONFIG = {
    "min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
    "min_price": 5.0,
    "score_threshold": 0.5,
}

_UNIVERSE = [
    {"symbol": "GOOD1", "market_cap": 5e9, "last_volume": 1e6},
    {"symbol": "GOOD2", "market_cap": 4e9, "last_volume": 1e6},
    {"symbol": "NOMC", "market_cap": None, "last_volume": 1e6},
    {"symbol": "THIN", "market_cap": 5e9, "last_volume": 100},
]


def _patch_common(monkeypatch):
    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(dict(_PATTERN_CONFIG)))
    monkeypatch.setattr(funnel, "get_universe_tickers", _async(list(_UNIVERSE)))
    monkeypatch.setattr(funnel, "log_pattern_run", _async("run-id"))
    monkeypatch.setattr(funnel, "was_seen_today", _async(False))
    monkeypatch.setattr(funnel, "mark_seen_today", _async(None))


def test_dry_run_makes_no_fmp_calls(monkeypatch):
    _patch_common(monkeypatch)
    fake = _RaisingFMP()

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=fake, pattern_code="sma150_bounce", dry_run=True)
    )

    assert fake.called is False
    sc = summary["stage_counts"]
    assert sc["stage_0_universe"] == 4
    assert sc["stage_1_liquidity_passed"] == 2   # GOOD1, GOOD2
    assert sc["stage_2_prefilter_passed"] == 0    # skipped in dry_run
    assert sc["stage_3_evaluated"] == 0
    assert summary["dry_run"] is True
    # rejects recorded for NOMC + THIN
    counts = summary["telemetry"]["rejection_reason_counts"]
    assert counts["market_cap_unknown"] == 1
    assert counts["volume_below_min"] == 1


def test_full_run_counts_and_saves(monkeypatch):
    _patch_common(monkeypatch)

    captured = {}

    def fake_eval(symbol, df, config):
        captured["config"] = config
        if symbol == "GOOD1":
            return {
                "verdict": "ENTER",
                "score": 0.9,
                "reason": "ok",
                "details": {"snapshot_date": "2023-06-01"},
            }
        return {
            "verdict": "AVOID",
            "score": 0.1,
            "reason": "meh",
            "details": {"snapshot_date": "2023-06-01", "rejection_reason": "score_below_threshold"},
        }

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs["symbol"])
        return "sig-id"

    monkeypatch.setattr(funnel, "evaluate_sma150_bounce", fake_eval)
    monkeypatch.setattr(funnel, "save_signal", fake_save)

    fake_fmp = _FakeFMP({"GOOD1": _fmp_history(), "GOOD2": _fmp_history()})

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake_fmp, pattern_code="sma150_bounce", dry_run=False
        )
    )

    sc = summary["stage_counts"]
    # Only liquidity survivors are fetched (bounded set).
    assert set(fake_fmp.requested) == {"GOOD1", "GOOD2"}
    assert summary["telemetry"]["api_call_counts"]["historical_fetches"] == 2
    assert sc["stage_2_prefilter_passed"] == 2
    assert sc["stage_3_evaluated"] == 2
    assert sc["enter_count"] == 1
    assert sc["reject_count"] == 1
    assert saved == ["GOOD1"]  # only ENTER saved (DEBUG_SAVE_AVOID off)
    # The resolved DB pattern config was passed into evaluation.
    assert captured["config"]["score_threshold"] == 0.5
    # Expensive 4H stage documented as disabled.
    assert any("4H" in n for n in summary["telemetry"]["notes"])


def test_limit_caps_survivors_before_fetch(monkeypatch):
    _patch_common(monkeypatch)
    monkeypatch.setattr(
        funnel, "evaluate_sma150_bounce",
        lambda s, d, c: {"verdict": "AVOID", "score": 0.0, "reason": "x",
                         "details": {"snapshot_date": "2023-06-01", "rejection_reason": "x"}},
    )
    monkeypatch.setattr(funnel, "save_signal", _async("id"))
    fake_fmp = _FakeFMP({"GOOD1": _fmp_history(), "GOOD2": _fmp_history()})

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake_fmp, pattern_code="sma150_bounce", limit=1, dry_run=False
        )
    )
    # Only 1 survivor fetched due to limit, even though 2 passed liquidity.
    assert len(fake_fmp.requested) == 1
    assert summary["telemetry"]["api_call_counts"]["historical_fetches"] == 1
