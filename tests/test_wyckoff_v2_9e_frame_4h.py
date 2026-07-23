"""Phase 9E2: canonical completed 4H frame (four_hour_frame.v1)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from app.workers.shadow.frames_4h import (
    FOUR_HOUR_FRAME_CONTRACT_VERSION,
    FOUR_HOUR_FRAME_HARD_CAP_BARS,
    FourHourFrameRejection,
    build_four_hour_frame,
    compute_four_hour_frame_hash,
)


EVAL = datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc)


def _bar(start: datetime, *, o=50.0, h=51.0, l=49.0, c=50.5, v=1000.0):
    return {"start_utc": start, "open": o, "high": h, "low": l,
            "close": c, "volume": v}


def _payload(bars: List[Dict[str, Any]], provider="massive"):
    return {
        "bars": bars,
        "provider": provider,
        "requested_start": "2026-06-15",
        "requested_end": "2026-07-15",
    }


def _series(n: int, *, end: datetime, step_hours: int = 4):
    """n bars whose LAST bar starts at `end` (oldest first)."""
    return [
        _bar(end - timedelta(hours=step_hours * i))
        for i in range(n - 1, -1, -1)
    ]


class TestCompletionSemantics:
    def test_completed_bar_cutoff_is_start_plus_duration(self):
        included = _bar(EVAL - timedelta(hours=4))        # end == EVAL
        excluded = _bar(EVAL - timedelta(hours=3))        # end 1h after EVAL
        frame = build_four_hour_frame(
            "LONGX", _payload([included, excluded]), evaluation_time_utc=EVAL,
        )
        assert frame.bar_count == 1
        assert frame.excluded_incomplete_count == 1
        assert frame.last_end_utc == EVAL.isoformat()

    def test_future_bars_excluded(self):
        frame = build_four_hour_frame(
            "LONGX",
            _payload([
                _bar(EVAL - timedelta(hours=8)),
                _bar(EVAL + timedelta(hours=4)),
            ]),
            evaluation_time_utc=EVAL,
        )
        assert frame.bar_count == 1
        assert frame.excluded_incomplete_count == 1

    def test_all_incomplete_rejects_honestly(self):
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame(
                "LONGX", _payload([_bar(EVAL)]), evaluation_time_utc=EVAL,
            )
        assert err.value.reason_code == "no_completed_bars"

    def test_naive_evaluation_time_rejects(self):
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame(
                "LONGX", _payload([_bar(EVAL - timedelta(hours=8))]),
                evaluation_time_utc=EVAL.replace(tzinfo=None),
            )
        assert err.value.reason_code == "naive_evaluation_time"


class TestAsOfAlignment:
    def test_bars_after_as_of_session_excluded(self):
        # Bar ends 2026-07-15 16:00 UTC -> NY session 2026-07-15 (kept);
        # bar ends 2026-07-16 16:00 UTC -> NY session 2026-07-16 (cut).
        kept = _bar(datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc))
        cut = _bar(datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc))
        frame = build_four_hour_frame(
            "LONGX", _payload([kept, cut]),
            evaluation_time_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
            as_of_session_date=date(2026, 7, 15),
        )
        assert frame.bar_count == 1
        assert frame.excluded_after_as_of_count == 1
        assert frame.as_of_session_date == "2026-07-15"

    def test_session_dates_use_exchange_timezone_not_utc(self):
        # Start 20:00 UTC -> end 2026-07-16 00:00 UTC, which is still
        # 20:00 EDT on 2026-07-15: the NY session is 07-15, not 07-16.
        bar = _bar(datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc))
        frame = build_four_hour_frame(
            "LONGX", _payload([bar]),
            evaluation_time_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
            as_of_session_date=date(2026, 7, 15),
        )
        assert frame.bar_count == 1
        assert frame.sessions_covered == ["2026-07-15"]

    def test_daylight_saving_shift_is_handled_by_zoneinfo(self):
        # Same 21:00 UTC wall-clock end maps to 17:00 EDT (summer) but
        # 16:00 EST (winter) — both resolve to the correct NY session date.
        summer = _bar(datetime(2026, 7, 10, 17, 0, tzinfo=timezone.utc))
        winter = _bar(datetime(2026, 12, 10, 17, 0, tzinfo=timezone.utc))
        f1 = build_four_hour_frame(
            "LONGX", _payload([summer]),
            evaluation_time_utc=datetime(2026, 12, 31, tzinfo=timezone.utc),
        )
        f2 = build_four_hour_frame(
            "LONGX", _payload([winter]),
            evaluation_time_utc=datetime(2026, 12, 31, tzinfo=timezone.utc),
        )
        assert f1.sessions_covered == ["2026-07-10"]
        assert f2.sessions_covered == ["2026-12-10"]

    def test_holiday_or_shortened_sessions_are_never_fabricated(self):
        # Observed coverage only: a session with no bars simply does not
        # appear; no expected calendar is synthesized.
        bars = [
            _bar(datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)),
            # 2026-07-03 market holiday: no bars.
            _bar(datetime(2026, 7, 6, 12, 0, tzinfo=timezone.utc)),
        ]
        frame = build_four_hour_frame(
            "LONGX", _payload(bars),
            evaluation_time_utc=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        assert frame.sessions_covered == ["2026-07-02", "2026-07-06"]

    def test_staleness_measured_in_daily_sessions(self):
        bars = [_bar(datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc))]
        frame = build_four_hour_frame(
            "LONGX", _payload(bars),
            evaluation_time_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
            as_of_session_date=date(2026, 7, 15),
            daily_session_dates=[
                date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15),
            ],
        )
        assert frame.staleness_sessions == 2

    def test_staleness_none_without_daily_calendar(self):
        bars = [_bar(datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc))]
        frame = build_four_hour_frame(
            "LONGX", _payload(bars),
            evaluation_time_utc=datetime(2026, 7, 20, tzinfo=timezone.utc),
        )
        assert frame.staleness_sessions is None


class TestRejections:
    def test_empty_payload_rejects(self):
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame("LONGX", _payload([]), evaluation_time_utc=EVAL)
        assert err.value.reason_code == "no_data"

    def test_duplicate_bar_start_rejects(self):
        start = EVAL - timedelta(hours=8)
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame(
                "LONGX",
                _payload([_bar(start, c=50.0), _bar(start, c=50.6)]),
                evaluation_time_utc=EVAL,
            )
        assert err.value.reason_code == "duplicate_bar_start"

    def test_naive_bar_timestamp_rejects(self):
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame(
                "LONGX",
                _payload([_bar((EVAL - timedelta(hours=8)).replace(tzinfo=None))]),
                evaluation_time_utc=EVAL,
            )
        assert err.value.reason_code == "naive_timestamp"

    @pytest.mark.parametrize("mutation,reason", [
        ({"open": None}, "malformed_ohlc"),
        ({"high": float("nan")}, "malformed_ohlc"),
        ({"close": -1.0}, "malformed_ohlc"),
        ({"low": 60.0}, "ohlc_envelope"),
        ({"volume": -5.0}, "malformed_volume"),
    ])
    def test_malformed_bars_reject(self, mutation, reason):
        bar = _bar(EVAL - timedelta(hours=8))
        bar.update(mutation)
        with pytest.raises(FourHourFrameRejection) as err:
            build_four_hour_frame(
                "LONGX", _payload([bar]), evaluation_time_utc=EVAL,
            )
        assert err.value.reason_code == reason

    def test_missing_volume_is_allowed(self):
        bar = _bar(EVAL - timedelta(hours=8))
        bar["volume"] = None
        frame = build_four_hour_frame(
            "LONGX", _payload([bar]), evaluation_time_utc=EVAL,
        )
        assert frame.bars[0]["volume"] is None


class TestFingerprintAndFrame:
    def test_deterministic_fingerprint(self):
        bars = _series(12, end=EVAL - timedelta(hours=4))
        f1 = build_four_hour_frame(
            "LONGX", _payload(bars), evaluation_time_utc=EVAL,
        )
        f2 = build_four_hour_frame(
            "LONGX", _payload(list(reversed(bars))), evaluation_time_utc=EVAL,
        )
        assert f1.frame_hash == f2.frame_hash
        assert f1.contract_version == FOUR_HOUR_FRAME_CONTRACT_VERSION

    def test_changed_input_changes_fingerprint(self):
        bars = _series(12, end=EVAL - timedelta(hours=4))
        f1 = build_four_hour_frame(
            "LONGX", _payload(bars), evaluation_time_utc=EVAL,
        )
        changed = [dict(b) for b in bars]
        changed[-1]["close"] = changed[-1]["close"] + 0.01
        f2 = build_four_hour_frame(
            "LONGX", _payload(changed), evaluation_time_utc=EVAL,
        )
        assert f1.frame_hash != f2.frame_hash

    def test_hash_covers_exactly_the_bars_supplied_to_strategy(self):
        bars = _series(12, end=EVAL - timedelta(hours=4))
        frame = build_four_hour_frame(
            "LONGX", _payload(bars), evaluation_time_utc=EVAL,
        )
        assert frame.frame_hash == compute_four_hour_frame_hash(frame.bars)
        df = frame.dataframe()
        assert len(df) == frame.bar_count
        assert str(df["date"].iloc[-1].tzinfo) in ("UTC", "utc")

    def test_hard_cap_keeps_most_recent(self):
        bars = _series(FOUR_HOUR_FRAME_HARD_CAP_BARS + 10,
                       end=EVAL - timedelta(hours=4))
        frame = build_four_hour_frame(
            "LONGX", _payload(bars), evaluation_time_utc=EVAL,
        )
        assert frame.bar_count == FOUR_HOUR_FRAME_HARD_CAP_BARS
        assert frame.last_start_utc == (
            (EVAL - timedelta(hours=4)).isoformat()
        )

    def test_metadata_is_bounded_and_json_safe(self):
        from app.workers.shadow.serialization import normalize_json_safe

        bars = _series(12, end=EVAL - timedelta(hours=4))
        frame = build_four_hour_frame(
            "LONGX", _payload(bars), evaluation_time_utc=EVAL,
            as_of_session_date=date(2026, 7, 15),
            daily_session_dates=[date(2026, 7, 15)],
        )
        meta = frame.metadata()
        normalize_json_safe(meta)   # must not raise
        assert "bars" not in meta
        assert meta["state"] == "built"
        assert meta["frame_hash"] == frame.frame_hash
        assert meta["session_count"] >= 1
        # No credentials or secrets appear in metadata.
        assert "key" not in str(meta).lower()
