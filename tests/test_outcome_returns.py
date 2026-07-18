"""Forward return math: LONG/SHORT, buy&hold, insufficient future bars."""

import pytest

from app.workers.outcomes.calculator import (
    compute_buy_hold_returns,
    compute_forward_returns,
    signed_return_pct,
)


def test_signed_return_long_and_short():
    assert signed_return_pct(100, 110, "LONG") == pytest.approx(10.0)
    assert signed_return_pct(100, 110, "SHORT") == pytest.approx(-10.0)
    assert signed_return_pct(100, 90, "LONG") == pytest.approx(-10.0)
    assert signed_return_pct(100, 90, "SHORT") == pytest.approx(10.0)


def test_signed_return_rejects_bad_inputs():
    with pytest.raises(ValueError):
        signed_return_pct(100, 110, "SIDEWAYS")
    with pytest.raises(ValueError):
        signed_return_pct(0, 110, "LONG")


def test_forward_returns_long():
    closes = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
    out = compute_forward_returns(100, closes, "LONG", windows=[1, 3, 5, 10])
    assert out[1] == pytest.approx(1.0)
    assert out[3] == pytest.approx(3.0)
    assert out[5] == pytest.approx(5.0)
    assert out[10] == pytest.approx(10.0)


def test_forward_returns_short_is_inverse():
    closes = [99, 98, 97]
    out = compute_forward_returns(100, closes, "SHORT", windows=[1, 3])
    assert out[1] == pytest.approx(1.0)
    assert out[3] == pytest.approx(3.0)


def test_insufficient_future_bars_yields_none():
    closes = [101, 102]  # only 2 forward bars
    out = compute_forward_returns(100, closes, "LONG", windows=[1, 3, 5])
    assert out[1] == pytest.approx(1.0)
    assert out[3] is None
    assert out[5] is None


def test_no_future_bars_all_none():
    out = compute_forward_returns(100, [], "LONG", windows=[1, 3, 5])
    assert all(v is None for v in out.values())


def test_buy_hold_is_always_long():
    closes = [90, 80]
    # For a SHORT signal the buy&hold baseline is still the LONG hold return.
    out = compute_buy_hold_returns(100, closes, windows=[1, 2])
    assert out[1] == pytest.approx(-10.0)
    assert out[2] == pytest.approx(-20.0)
