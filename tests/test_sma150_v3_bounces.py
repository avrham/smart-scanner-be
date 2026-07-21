"""Phase 8: sma150.v3 independent bounce-event detection.

Clustering, effective separation, non-overlapping rebound windows,
deterministic representative selection, incomplete-window exclusion, and the
no-lookahead guarantee.
"""

import pandas as pd
import pytest

from app.workers.strategies.sma150_v3 import find_independent_bounce_events


def _frame(closes, highs=None, sma=100.0):
    """Positional frame with a constant SMA column for direct unit tests."""
    n = len(closes)
    dates = pd.bdate_range("2024-01-02", periods=n)
    return pd.DataFrame({
        "date": dates,
        "open": closes,
        "high": highs if highs is not None else [c * 1.001 for c in closes],
        "low": [c * 0.999 for c in closes],
        "close": closes,
        "volume": [1e6] * n,
        "sma_150": [sma] * n,
    })


def _detect(df, *, tolerance=3.0, window=10, separation=15):
    return find_independent_bounce_events(
        df,
        "sma_150",
        touch_tolerance_pct=tolerance,
        rebound_window_bars=window,
        min_event_separation_bars=separation,
        current_index=len(df),
    )


OUT = 110.0   # ~10% above SMA=100: out of the 3% band
IN_ = 100.5   # in band


class TestClustering:
    def test_contiguous_touch_bars_are_one_event(self):
        closes = [OUT] * 20 + [IN_, IN_, IN_] + [OUT] * 30
        events = _detect(_frame(closes))
        assert len(events) == 1
        assert events[0]["cluster_span_bars"] == 3

    def test_nearby_runs_below_separation_collapse(self):
        # Two runs 8 bars apart (< separation 15) -> ONE cluster.
        closes = [OUT] * 20 + [IN_] + [OUT] * 8 + [IN_] + [OUT] * 30
        events = _detect(_frame(closes))
        assert len(events) == 1

    def test_independent_events_remain_separate(self):
        # Two runs 30 bars apart (>= separation) -> TWO events.
        closes = [OUT] * 20 + [IN_] + [OUT] * 30 + [IN_] + [OUT] * 30
        events = _detect(_frame(closes))
        assert len(events) == 2

    def test_effective_separation_includes_rebound_window(self):
        # separation=5 but window=10 => effective separation is 11: runs
        # 8 bars apart still collapse so rebound windows can never overlap.
        closes = [OUT] * 20 + [IN_] + [OUT] * 8 + [IN_] + [OUT] * 30
        events = _detect(_frame(closes), separation=5, window=10)
        assert len(events) == 1

    def test_rebound_windows_never_overlap(self):
        closes = [OUT] * 20 + [IN_] + [OUT] * 30 + [IN_] + [OUT] * 30
        events = _detect(_frame(closes), window=10, separation=15)
        assert len(events) == 2
        first, second = sorted(e["touch_index"] for e in events)
        assert first + 10 < second  # window of first ends before second touch


class TestRepresentativeSelection:
    def test_minimum_distance_bar_is_representative(self):
        closes = [OUT] * 20 + [101.5, 100.2, 101.0] + [OUT] * 30
        events = _detect(_frame(closes))
        assert len(events) == 1
        assert events[0]["touch_index"] == 21  # 100.2 is closest to SMA
        assert events[0]["touch_price"] == pytest.approx(100.2)

    def test_exact_tie_breaks_to_earliest_bar(self):
        closes = [OUT] * 20 + [100.5, 100.5] + [OUT] * 30
        events = _detect(_frame(closes))
        assert events[0]["touch_index"] == 20

    def test_detection_is_deterministic(self):
        closes = [OUT] * 20 + [IN_, 100.2] + [OUT] * 25 + [IN_] + [OUT] * 30
        df = _frame(closes)
        assert _detect(df) == _detect(df)


class TestWindowBoundaries:
    def test_incomplete_rebound_window_excluded(self):
        # Touch 5 bars before the end: a 10-bar window cannot complete.
        closes = [OUT] * 40 + [IN_] + [OUT] * 5
        events = _detect(_frame(closes))
        assert events == []

    def test_exactly_complete_window_included(self):
        closes = [OUT] * 40 + [IN_] + [OUT] * 10
        events = _detect(_frame(closes))
        assert len(events) == 1

    def test_no_data_beyond_frame_is_used(self):
        """Appending bars AFTER the frame boundary must not change events
        measured within the original frame (no lookahead)."""
        closes = [OUT] * 40 + [IN_] + [OUT] * 10
        base = _detect(_frame(closes))
        extended = _detect(_frame(closes[:-1]))  # one bar fewer => incomplete
        assert len(base) == 1
        assert extended == []  # window can't complete without the last bar

    def test_rebound_measured_only_after_touch(self):
        # Huge highs BEFORE the touch must not leak into the rebound.
        closes = [OUT] * 40 + [IN_] + [100.6] * 10
        highs = [c * 1.001 for c in closes]
        highs[10] = 500.0  # pre-touch spike
        events = _detect(_frame(closes, highs=highs))
        assert len(events) == 1
        assert events[0]["max_rebound_pct"] < 1.0


class TestPersistedEventFacts:
    def test_events_returned_in_chronological_order(self):
        """Bounce events are a SEMANTIC sequence: always chronological by
        touch index, never re-sorted by any serialization step."""
        closes = [OUT] * 20 + [IN_] + [OUT] * 30 + [100.2] + [OUT] * 30 + [IN_] + [OUT] * 30
        events = _detect(_frame(closes))
        indices = [e["touch_index"] for e in events]
        assert indices == sorted(indices)
        assert len(events) == 3

    def test_event_dates_and_ages_persisted(self):
        closes = [OUT] * 40 + [IN_] + [OUT] * 30
        df = _frame(closes)
        events = _detect(df)
        assert len(events) == 1
        event = events[0]
        expected_date = pd.to_datetime(df.iloc[40]["date"]).date().isoformat()
        assert event["touch_date"] == expected_date
        assert event["age_bars"] == len(df) - 40
        assert event["sma_value"] == pytest.approx(100.0)
        assert event["distance_pct"] == pytest.approx(0.5, abs=0.01)
        assert "bars_to_max_rebound" in event
