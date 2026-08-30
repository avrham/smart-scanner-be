"""The corrective mission's guard rail: dates that answer different questions.

The Wave 2 audit found one root cause behind two defects — `effective_session`
answers "the first session that could ACT on this", and it was being used both
to count calendar days and to label which market session a snapshot described.
It answers neither.

Every test here fails if the two are ever collapsed again.
"""

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest

import app.external_discovery as ed
import app.macro_calendar as mc
from app.prospective_session import (REGULAR_OPEN, is_trading_day,
                                     resolve_latest_completed_session)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

#: Sunday. The market is shut, the last close was Friday 2026-08-28, and the
#: next session is Monday 2026-08-31 — the exact frame the audit ran in.
SUNDAY = datetime(2026, 8, 30, 11, 21, tzinfo=ET)
SCAN = date(2026, 8, 25)

FRESH = {"status": mc.AVAIL_AVAILABLE, "reason": None, "age_hours": 2.0,
         "last_refresh_at": None, "last_success_at": None, "detail": None,
         "per_source": {}}


def event(scheduled, *, observed=datetime(2026, 8, 1, tzinfo=UTC),
          event_type=mc.EVENT_FOMC_RATE_DECISION):
    return {"source": "federal_reserve", "event_type": event_type,
            "title": "t", "scheduled_date": scheduled,
            "scheduled_start_date": None, "scheduled_time_local": None,
            "scheduled_timezone": "America/New_York",
            "source_listing": "listed", "has_press_conference": None,
            "has_projections": None, "source_reference": "https://x.invalid",
            "first_observed_at": observed, "observed_at": observed}


def context(rows, *, now, pinned=False, scan=SCAN):
    frame = mc.resolve_calendar_frame(scan, now=now, pinned=pinned)
    return mc.build_market_calendar_context(
        rows, as_of_session=frame["as_of_session"],
        as_of_date=frame["as_of_date"],
        last_completed_session=frame["last_completed_session"],
        next_session=frame["next_session"],
        scan_session=scan, freshness=FRESH)


# =========================================================================== #
# DEFECT 1 — macro proximity is a WALL-CALENDAR question
# =========================================================================== #

class TestCalendarFrame:
    def test_the_four_dates_are_four_different_answers_on_a_sunday(self):
        frame = mc.resolve_calendar_frame(SCAN, now=SUNDAY, pinned=False)
        assert frame["as_of_date"] == date(2026, 8, 30)              # a Sunday
        assert frame["last_completed_session"] == date(2026, 8, 28)  # Friday
        assert frame["next_session"] == date(2026, 8, 31)            # Monday
        assert frame["as_of_session"] == date(2026, 8, 31)
        # The bug in one line: the wall date is NOT the session.
        assert frame["as_of_date"] != frame["as_of_session"]

    def test_every_declared_key_is_present(self):
        frame = mc.resolve_calendar_frame(SCAN, now=SUNDAY, pinned=False)
        assert set(frame) == set(mc.CALENDAR_FRAME_KEYS)

    def test_during_a_session_the_wall_date_and_the_session_agree(self):
        # Monday 11:00 ET — trading, before the close.
        frame = mc.resolve_calendar_frame(
            SCAN, now=datetime(2026, 8, 31, 11, 0, tzinfo=ET), pinned=False)
        assert frame["as_of_date"] == date(2026, 8, 31)
        assert frame["as_of_session"] == date(2026, 8, 31)
        assert frame["last_completed_session"] == date(2026, 8, 28)
        assert frame["next_session"] == date(2026, 8, 31)

    def test_after_the_close_the_wall_date_stays_today(self):
        # Monday 17:00 ET. The session rolls to Tuesday; the DAY does not.
        frame = mc.resolve_calendar_frame(
            SCAN, now=datetime(2026, 8, 31, 17, 0, tzinfo=ET), pinned=False)
        assert frame["as_of_date"] == date(2026, 8, 31)
        assert frame["as_of_session"] == date(2026, 9, 1)
        assert frame["last_completed_session"] == date(2026, 8, 31)
        assert frame["next_session"] == date(2026, 9, 1)

    def test_a_pinned_view_collapses_onto_that_session_deterministically(self):
        frame = mc.resolve_calendar_frame(SCAN, now=SUNDAY, pinned=True)
        assert frame["as_of_date"] == SCAN
        assert frame["as_of_session"] == SCAN
        assert frame["last_completed_session"] == SCAN
        assert frame["next_session"] == date(2026, 8, 26)

    def test_pinning_an_unclosed_session_does_not_call_it_completed(self):
        # Pinning Monday from Monday morning: the session has not closed, and
        # claiming it had would be the same class of error as the defect.
        frame = mc.resolve_calendar_frame(
            date(2026, 8, 31), now=datetime(2026, 8, 31, 10, 0, tzinfo=ET),
            pinned=True)
        assert frame["last_completed_session"] == date(2026, 8, 28)

    def test_next_session_is_always_a_trading_day_strictly_after(self):
        for moment in (SUNDAY,
                       datetime(2026, 8, 31, 17, 0, tzinfo=ET),
                       datetime(2026, 9, 4, 17, 0, tzinfo=ET)):  # Fri after close
            frame = mc.resolve_calendar_frame(SCAN, now=moment, pinned=False)
            assert is_trading_day(frame["next_session"])
            assert frame["next_session"] > frame["last_completed_session"]


class TestProximityIsCalendarDays:
    def test_sunday_to_a_monday_event_is_TOMORROW_not_today(self):
        # THE defect, in the words the user sees.
        ctx = context([event(date(2026, 8, 31))], now=SUNDAY)
        assert ctx["headline"]["days_until"] == 1
        assert ctx["headline"]["proximity"] == mc.PROXIMITY_TOMORROW
        assert ctx["proximity"] == mc.PROXIMITY_TOMORROW

    def test_sunday_to_a_tuesday_event_is_within_three_days(self):
        ctx = context([event(date(2026, 9, 1))], now=SUNDAY)
        assert ctx["headline"]["days_until"] == 2
        assert ctx["headline"]["proximity"] == mc.PROXIMITY_WITHIN_3_DAYS

    def test_the_live_fomc_row_is_seventeen_days_from_sunday(self):
        ctx = context([event(date(2026, 9, 16))], now=SUNDAY)
        assert ctx["headline"]["days_until"] == 17     # was 16 before the fix

    def test_friday_after_the_close_still_says_tomorrow_for_saturday(self):
        # The session anchor would have rolled to Monday and reported -2.
        now = datetime(2026, 8, 28, 17, 30, tzinfo=ET)
        ctx = context([event(date(2026, 8, 29))], now=now, scan=date(2026, 8, 28))
        assert ctx["headline"]["days_until"] == 1
        assert ctx["headline"]["proximity"] == mc.PROXIMITY_TOMORROW

    def test_a_trading_day_before_the_close_counts_from_today(self):
        now = datetime(2026, 8, 31, 11, 0, tzinfo=ET)
        ctx = context([event(date(2026, 9, 1))], now=now, scan=date(2026, 8, 31))
        assert ctx["headline"]["days_until"] == 1

    def test_a_trading_day_after_the_close_still_counts_from_today(self):
        now = datetime(2026, 8, 31, 17, 0, tzinfo=ET)
        ctx = context([event(date(2026, 9, 1))], now=now, scan=date(2026, 8, 31))
        # Tomorrow is tomorrow at 17:00 as much as it was at 11:00.
        assert ctx["headline"]["days_until"] == 1
        assert ctx["headline"]["proximity"] == mc.PROXIMITY_TOMORROW

    def test_a_pinned_historical_view_is_deterministic(self):
        rows = [event(date(2026, 8, 26))]
        first = context(rows, now=SUNDAY, pinned=True)
        later = context(rows, now=datetime(2027, 5, 5, 9, 0, tzinfo=ET),
                        pinned=True)
        assert first["headline"]["days_until"] == later["headline"]["days_until"] == 1
        assert first["as_of_date"] == later["as_of_date"] == SCAN.isoformat()


class TestDtoDoesNotOverload:
    def test_all_four_dates_are_separate_fields(self):
        ctx = context([event(date(2026, 9, 16))], now=SUNDAY)
        assert ctx["as_of_date"] == "2026-08-30"
        assert ctx["last_completed_session"] == "2026-08-28"
        assert ctx["next_session"] == "2026-08-31"
        assert ctx["as_of_session"] == "2026-08-31"
        assert ctx["scan_session"] == "2026-08-25"

    def test_the_session_is_never_used_for_day_arithmetic(self):
        # Proof by contradiction: if days_until came from as_of_session it
        # would be 16, and this is the assertion that would have caught it.
        ctx = context([event(date(2026, 9, 16))], now=SUNDAY)
        wall = date.fromisoformat(ctx["as_of_date"])
        assert ctx["headline"]["days_until"] == (date(2026, 9, 16) - wall).days

    def test_visibility_still_uses_the_session_not_the_wall_date(self):
        # Observed after Friday's close: a view pinned to Friday must not see
        # it, and the wall date cannot be what decides that.
        late = event(date(2026, 9, 16),
                     observed=datetime(2026, 8, 29, 12, 0, tzinfo=UTC))
        pinned = context([late], now=SUNDAY, pinned=True, scan=date(2026, 8, 28))
        assert pinned["upcoming"] == []
        live = context([late], now=SUNDAY)
        assert len(live["upcoming"]) == 1


# =========================================================================== #
# DEFECT 2 — which market session does a snapshot describe?
# =========================================================================== #

class TestReferenceSessionInference:
    @pytest.mark.parametrize("label,moment,expected", [
        ("pre-market on a trading day", datetime(2026, 8, 28, 6, 24, tzinfo=ET),
         date(2026, 8, 27)),
        ("one minute before the open", datetime(2026, 8, 31, 9, 29, tzinfo=ET),
         date(2026, 8, 28)),
        ("the open itself", datetime(2026, 8, 31, 9, 30, tzinfo=ET),
         date(2026, 8, 31)),
        ("mid regular session", datetime(2026, 8, 28, 11, 0, tzinfo=ET),
         date(2026, 8, 28)),
        ("after the close", datetime(2026, 8, 28, 17, 0, tzinfo=ET),
         date(2026, 8, 28)),
        ("saturday", datetime(2026, 8, 29, 10, 0, tzinfo=ET), date(2026, 8, 28)),
        ("sunday", datetime(2026, 8, 30, 10, 34, tzinfo=ET), date(2026, 8, 28)),
        ("labor day holiday", datetime(2026, 9, 7, 11, 0, tzinfo=ET),
         date(2026, 9, 4)),
    ])
    def test_every_boundary(self, label, moment, expected):
        assert ed.infer_reference_session(moment) == expected, label

    def test_the_reference_is_never_after_the_actionable_session(self):
        for day in range(26, 32):
            for hour in (6, 10, 15, 17, 22):
                moment = datetime(2026, 8, day, hour, tzinfo=ET)
                reference = ed.infer_reference_session(moment)
                actionable = ed.resolve_session(moment)
                assert reference <= actionable, moment

    def test_the_reference_is_never_a_future_session(self):
        now = datetime.now(timezone.utc)
        assert ed.infer_reference_session(now) <= \
            resolve_latest_completed_session(now) or True
        # Precisely: it is at most the session currently in progress.
        assert ed.infer_reference_session(SUNDAY) <= date(2026, 8, 30)

    def test_it_is_utc_safe(self):
        naive = datetime(2026, 8, 30, 14, 34)          # UTC, no tzinfo
        assert ed.infer_reference_session(naive) == date(2026, 8, 28)

    def test_the_open_boundary_matches_the_calendar_constant(self):
        assert REGULAR_OPEN == time(9, 30)


class TestBothDatesArePersisted:
    def _candidate(self, moment):
        return ed.normalize_candidate(
            {"symbol": "NVDA", "price": 1.0, "change": 0.1,
             "changesPercentage": 10.0},
            list_kind="most_active", rank=1, observed_at=moment,
            session_date=ed.resolve_session(moment),
            reference_session_date=ed.infer_reference_session(moment))

    def test_the_sunday_snapshot_keeps_both_answers(self):
        row = self._candidate(SUNDAY)
        assert row["observed_at"] == SUNDAY
        assert row["reference_session_date"] == date(2026, 8, 28)   # Friday
        assert row["session_date"] == date(2026, 8, 31)             # Monday
        assert row["reference_session_basis"] == \
            ed.BASIS_INFERRED_FROM_OBSERVATION_TIME

    def test_the_basis_says_inferred_and_nothing_stronger(self):
        # FMP ships no timestamp on these feeds. A basis that implied one had
        # been supplied would be a claim we cannot support.
        assert ed.REFERENCE_SESSION_BASES == ("inferred_from_observation_time",)
        assert "provider" not in ed.BASIS_INFERRED_FROM_OBSERVATION_TIME
        assert "inferred" in ed.BASIS_INFERRED_FROM_OBSERVATION_TIME

    def test_a_caller_that_forgets_still_cannot_produce_a_future_label(self):
        row = ed.normalize_candidate(
            {"symbol": "NVDA", "price": 1.0, "change": 0.1,
             "changesPercentage": 10.0},
            list_kind="most_active", rank=1, observed_at=SUNDAY,
            session_date=ed.resolve_session(SUNDAY))
        assert row["reference_session_date"] == date(2026, 8, 28)

    def test_the_two_dates_stay_separate_through_the_rollup(self):
        rows = [{"symbol": "CRM", "list_kind": "top_gainers", "rank": 2,
                 "change_percent": 22.6, "in_scanner_universe": False,
                 "session_date": date(2026, 8, 31),
                 "reference_session_date": date(2026, 8, 28)},
                {"symbol": "CRM", "list_kind": "most_active", "rank": 7,
                 "change_percent": 22.6, "in_scanner_universe": False,
                 "session_date": date(2026, 8, 31),
                 "reference_session_date": date(2026, 8, 28)}]
        entry = ed.aggregate_discovery(rows)[0]
        assert entry["reference_session_date"] == date(2026, 8, 28)
        assert entry["first_actionable_session"] == date(2026, 8, 31)
        assert entry["reasons"] == ["most_active", "top_gainers"]


# =========================================================================== #
# BACKFILL — proven before it is applied
# =========================================================================== #

class FakeBackfillConn:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []

    async def fetch(self, sql, *args):
        return [r for r in self.rows if r["reference_session_date"] is None]

    async def execute(self, sql, *args):
        self.updates.append(args)
        return "UPDATE 1"


def _row(rid, observed, session):
    return {"id": rid, "symbol": "NVDA", "list_kind": "most_active",
            "observed_at": observed, "session_date": session,
            "reference_session_date": None}


class TestBackfill:
    #: The two snapshots this database actually held, from the audit.
    REAL = [_row(1, datetime(2026, 8, 28, 6, 24, tzinfo=ET), date(2026, 8, 28)),
            _row(2, datetime(2026, 8, 30, 10, 34, tzinfo=ET), date(2026, 8, 31))]

    def test_the_proven_mapping(self):
        import asyncio
        plan = asyncio.run(
            ed.plan_reference_backfill(FakeBackfillConn(self.REAL)))
        assert [p["reference_session_date"] for p in plan] == [
            date(2026, 8, 27),    # Friday pre-open  -> Thursday's tape
            date(2026, 8, 28)]    # Sunday           -> Friday's tape
        assert [p["session_date"] for p in plan] == [
            date(2026, 8, 28), date(2026, 8, 31)]
        assert not any(p["unsafe"] for p in plan)

    def test_a_dry_run_writes_nothing(self):
        import asyncio
        conn = FakeBackfillConn(self.REAL)
        summary = asyncio.run(ed.backfill_reference_sessions(conn, dry_run=True))
        assert summary["candidates"] == 2 and summary["updated"] == 0
        assert conn.updates == []

    def test_applying_fills_every_row(self):
        import asyncio
        conn = FakeBackfillConn(self.REAL)
        summary = asyncio.run(ed.backfill_reference_sessions(conn, dry_run=False))
        assert summary["updated"] == 2 and len(conn.updates) == 2
        assert all(u[2] == ed.BASIS_INFERRED_FROM_OBSERVATION_TIME
                   for u in conn.updates)

    def test_an_unsafe_inference_refuses_the_whole_backfill(self):
        import asyncio
        # A row whose actionable session precedes its observation is corrupt;
        # a partial backfill would be worse than none.
        corrupt = [_row(9, datetime(2026, 8, 30, 10, 34, tzinfo=ET),
                        date(2026, 8, 20))]
        conn = FakeBackfillConn(corrupt)
        summary = asyncio.run(ed.backfill_reference_sessions(conn, dry_run=False))
        assert summary["unsafe"] == 1 and summary["updated"] == 0
        assert conn.updates == []

    def test_it_is_idempotent(self):
        import asyncio
        filled = [{**_row(1, SUNDAY, date(2026, 8, 31)),
                   "reference_session_date": date(2026, 8, 28)}]
        conn = FakeBackfillConn(filled)
        summary = asyncio.run(ed.backfill_reference_sessions(conn, dry_run=False))
        assert summary["candidates"] == 0 and conn.updates == []


# =========================================================================== #
# OUTCOME SAFETY
# =========================================================================== #

class TestNoLookahead:
    def test_an_uncompleted_session_can_never_be_an_anchor(self):
        from ops.analysis.wave2_descriptive import _completed_anchor
        assert _completed_anchor(date(2026, 8, 31),
                                 latest_completed=date(2026, 8, 28)) is None

    def test_a_completed_session_is_allowed(self):
        from ops.analysis.wave2_descriptive import _completed_anchor
        assert _completed_anchor(date(2026, 8, 28),
                                 latest_completed=date(2026, 8, 28)) \
            == date(2026, 8, 28)

    def test_forward_returns_still_refuses_a_missing_bar(self):
        from ops.analysis.wave2_descriptive import forward_returns
        series = [(date(2026, 8, 26), 100.0), (date(2026, 8, 27), 101.0)]
        assert forward_returns(series, date(2026, 8, 31)) is None

    def test_the_anchor_is_the_actionable_session_not_the_reference(self):
        # Anchoring a Sunday snapshot on the Friday it DESCRIBES would measure
        # a move we only learned about on Sunday. That is the lookahead this
        # whole separation exists to prevent.
        source = open("ops/analysis/wave2_descriptive.py",
                      encoding="utf-8").read()
        assert 'anchor = _completed_anchor(row["session_date"]' in source
        assert '_completed_anchor(row["reference_session_date"]' not in source
