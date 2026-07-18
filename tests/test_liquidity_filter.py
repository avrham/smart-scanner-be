"""Liquidity filtering uses REAL volume and enforces thresholds (B9/B10)."""

from app.workers.tickers import filter_by_liquidity


def _historical(volume, close, n=20):
    return {"historical": [{"volume": volume, "close": close} for _ in range(n)]}


def test_passes_with_real_liquid_data():
    passed, reason = filter_by_liquidity(_historical(500_000, 50.0))
    assert passed is True
    assert reason is None


def test_rejects_low_volume():
    passed, reason = filter_by_liquidity(_historical(1_000, 50.0))
    assert passed is False
    assert reason == "avg_volume_below_min"


def test_rejects_low_price():
    passed, reason = filter_by_liquidity(_historical(500_000, 0.5))
    assert passed is False
    assert reason == "price_below_min"


def test_rejects_missing_history():
    passed, reason = filter_by_liquidity({})
    assert passed is False
    assert reason == "no_historical_data"


def test_unknown_volume_is_not_fabricated():
    # Volume is None for every bar. The filter must report it as unknown and
    # reject, NOT invent a value from price/market cap.
    data = {"historical": [{"volume": None, "close": 50.0} for _ in range(20)]}
    passed, reason = filter_by_liquidity(data)
    assert passed is False
    assert reason == "volume_unknown"
