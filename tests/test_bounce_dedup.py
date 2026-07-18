"""Deduplicated bounce counting (Phase 1)."""

from app.workers.patterns.sma150 import find_historical_bounces
from tests.helpers import make_df_with_sma


TOL = 3.0
WINDOW = 10
MIN_REBOUND = 5.0


def _count(df):
    return len(find_historical_bounces(df, "sma_150", TOL, WINDOW, MIN_REBOUND))


def test_consecutive_days_in_band_count_as_one_bounce():
    # 5 consecutive in-band days (close 101, sma 100 => 1% distance), then a
    # rebound whose high reaches ~108 (>5% above the touch price of 101).
    closes = [101, 101, 101, 101, 101] + [103.5, 105, 106, 107, 108] + [108] * 5
    highs = [101, 101, 101, 101, 101] + [103.5, 105, 106, 107, 108] + [108] * 5
    df = make_df_with_sma(closes, highs)
    assert _count(df) == 1


def test_separate_touches_after_separation_count_as_multiple():
    # Touch A (in-band days 0-2) -> out-of-band rebound -> Touch B (in-band
    # days 9-11) -> out-of-band rebound. Two DISTINCT events.
    closes = (
        [101, 101, 101]          # touch A
        + [104, 106, 107, 108, 108, 108]  # out of band, rebound A
        + [101, 101, 101]        # touch B (separated)
        + [104, 106, 107, 108, 108, 108]  # out of band, rebound B
    )
    df = make_df_with_sma(closes, highs=closes)
    assert _count(df) == 2


def test_no_touch_returns_zero():
    # Price always ~20% away from the SMA => never in band.
    closes = [120] * 40
    df = make_df_with_sma(closes, highs=[126] * 40)
    assert _count(df) == 0


def test_proximity_threshold_changes_count():
    # A single touch sitting 4% above the SMA (close 104, sma 100), then a
    # rebound. With tol=3 it is out of band (0); with tol=5 it is in band (1).
    closes = [104, 104, 104] + [107, 109, 110, 111, 112, 112, 112, 112, 112, 112]
    df = make_df_with_sma(closes, highs=closes)
    strict = len(find_historical_bounces(df, "sma_150", 3.0, WINDOW, MIN_REBOUND))
    loose = len(find_historical_bounces(df, "sma_150", 5.0, WINDOW, MIN_REBOUND))
    assert strict == 0
    assert loose == 1
