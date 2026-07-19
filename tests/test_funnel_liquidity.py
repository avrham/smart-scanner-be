"""Stage 1 liquidity classification + filtering (real values only)."""

from app.workers.scanner.funnel import (
    RejectionTracker,
    apply_liquidity_filter,
    classify_liquidity,
)


MC = 200_000_000
VOL = 200_000


def test_passes_when_above_thresholds():
    t = {"symbol": "AAA", "market_cap": 5e9, "last_volume": 1e6}
    assert classify_liquidity(t, MC, VOL) is None


def test_market_cap_unknown():
    t = {"symbol": "AAA", "market_cap": None, "last_volume": 1e6}
    assert classify_liquidity(t, MC, VOL) == "market_cap_unknown"


def test_market_cap_below_min():
    t = {"symbol": "AAA", "market_cap": 1e6, "last_volume": 1e6}
    assert classify_liquidity(t, MC, VOL) == "market_cap_below_min"


def test_volume_unknown_rejected_by_default():
    t = {"symbol": "AAA", "market_cap": 5e9, "last_volume": None}
    assert classify_liquidity(t, MC, VOL) == "volume_unknown"


def test_volume_unknown_allowed_when_configured():
    t = {"symbol": "AAA", "market_cap": 5e9, "last_volume": None}
    assert classify_liquidity(t, MC, VOL, allow_unknown_volume=True) is None


def test_volume_below_min():
    t = {"symbol": "AAA", "market_cap": 5e9, "last_volume": 100}
    assert classify_liquidity(t, MC, VOL) == "volume_below_min"


def test_apply_liquidity_filter_counts_and_survivors():
    tickers = [
        {"symbol": "GOOD", "market_cap": 5e9, "last_volume": 1e6},
        {"symbol": "NOMC", "market_cap": None, "last_volume": 1e6},
        {"symbol": "SMALL", "market_cap": 1e6, "last_volume": 1e6},
        {"symbol": "NOVOL", "market_cap": 5e9, "last_volume": None},
        {"symbol": "THIN", "market_cap": 5e9, "last_volume": 100},
    ]
    tracker = RejectionTracker(sample_limit=25)
    survivors = apply_liquidity_filter(tickers, MC, VOL, False, tracker)

    assert [s["symbol"] for s in survivors] == ["GOOD"]
    counts = tracker.counts
    assert counts["market_cap_unknown"] == 1
    assert counts["market_cap_below_min"] == 1
    assert counts["volume_unknown"] == 1
    assert counts["volume_below_min"] == 1
