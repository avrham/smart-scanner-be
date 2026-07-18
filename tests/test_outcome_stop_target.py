"""Stop / target hit detection and simplified R."""

import pytest

from app.workers.outcomes.calculator import (
    compute_simulated_r,
    compute_stop_target_hits,
)


def test_long_stop_and_target_hits():
    hit_stop, hit_target = compute_stop_target_hits(
        entry_price=100,
        stop_price=98,
        target_price=104,
        forward_highs=[102, 105],
        forward_lows=[99, 97],
        side="LONG",
    )
    assert hit_stop is True    # low 97 <= 98
    assert hit_target is True  # high 105 >= 104


def test_long_no_hits():
    hit_stop, hit_target = compute_stop_target_hits(
        entry_price=100,
        stop_price=95,
        target_price=110,
        forward_highs=[101, 102],
        forward_lows=[99, 98],
        side="LONG",
    )
    assert hit_stop is False
    assert hit_target is False


def test_short_hits_are_mirrored():
    hit_stop, hit_target = compute_stop_target_hits(
        entry_price=100,
        stop_price=103,   # stop above for a short
        target_price=96,  # target below for a short
        forward_highs=[104, 101],
        forward_lows=[98, 95],
        side="SHORT",
    )
    assert hit_stop is True    # high 104 >= 103
    assert hit_target is True  # low 95 <= 96


def test_none_when_levels_undefined():
    hit_stop, hit_target = compute_stop_target_hits(
        entry_price=100,
        stop_price=None,
        target_price=None,
        forward_highs=[102],
        forward_lows=[98],
        side="LONG",
    )
    assert hit_stop is None
    assert hit_target is None


def test_simulated_r():
    # risk = |100-98|/100 = 2%; end return +4% => R = 2.0
    assert compute_simulated_r(100, 98, 4.0, "LONG") == pytest.approx(2.0)
    # a loss to the stop distance => R = -1
    assert compute_simulated_r(100, 98, -2.0, "LONG") == pytest.approx(-1.0)


def test_simulated_r_none_without_stop():
    assert compute_simulated_r(100, None, 4.0, "LONG") is None
    assert compute_simulated_r(100, 98, None, "LONG") is None
