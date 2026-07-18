"""Max favorable / adverse excursion for LONG and SHORT."""

import pytest

from app.workers.outcomes.calculator import compute_mfe_mae


def test_mfe_mae_long():
    entry = 100.0
    highs = [102, 105, 101]
    lows = [99, 97, 100]
    mfe, mae = compute_mfe_mae(entry, highs, lows, "LONG")
    assert mfe == pytest.approx(5.0)   # best high +5%
    assert mae == pytest.approx(-3.0)  # worst low -3%


def test_mfe_mae_short_is_mirrored():
    entry = 100.0
    highs = [102, 105, 101]
    lows = [99, 97, 100]
    mfe, mae = compute_mfe_mae(entry, highs, lows, "SHORT")
    # Favorable for a short is price falling -> uses lows.
    assert mfe == pytest.approx(3.0)   # low 97 => +3% in favor
    assert mae == pytest.approx(-5.0)  # high 105 => -5% against


def test_mfe_mae_respects_window():
    entry = 100.0
    highs = [110, 120, 130]
    lows = [95, 90, 85]
    mfe, mae = compute_mfe_mae(entry, highs, lows, "LONG", window=1)
    assert mfe == pytest.approx(10.0)  # only first bar considered
    assert mae == pytest.approx(-5.0)


def test_mfe_mae_no_bars():
    assert compute_mfe_mae(100.0, [], [], "LONG") == (None, None)
