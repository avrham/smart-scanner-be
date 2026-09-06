"""The research outcome contract, proven where it is pure.

Everything here runs against real functions and a small in-memory fake
connection — no docker, no network. The parts that are genuinely SQL (the
uniqueness, the CHECKs, the freeze trigger, the migration chain) are proven in
`test_research_outcomes_integration.py` against a real Postgres, because a fake
`execute()` will agree with anything and P0 taught this project exactly what
that costs.

The R-numbers in the test names are the brief's required cases.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

import app.research_funnel as rf
import app.research_outcomes as ro
from app.prospective_session import (is_trading_day,
                                     nth_trading_session_after,
                                     trading_sessions_between)

UTC = timezone.utc


def bar(d: date, close: float, *, high=None, low=None, open_=None) -> ro.Bar:
    return ro.Bar(d, open_ if open_ is not None else close,
                  high if high is not None else close,
                  low if low is not None else close, close)


# --------------------------------------------------------------------------- #
# the market calendar underneath every horizon
# --------------------------------------------------------------------------- #

class TestMarketHorizonCalendar:
    """R02 / R03 / R18 / R21 — sessions, not calendar days."""

    def test_r02_three_sessions_from_a_thursday_crosses_the_weekend(self):
        # Thu 2026-08-27 -> Fri 28, Mon 31, Tue Sep 1.
        assert nth_trading_session_after(date(2026, 8, 27), 1) == date(2026, 8, 28)
        assert nth_trading_session_after(date(2026, 8, 27), 3) == date(2026, 9, 1)
        # Calendar arithmetic would have said 2026-08-30, a Sunday.
        assert not is_trading_day(date(2026, 8, 30))

    def test_r03_labor_day_is_skipped(self):
        # 2026-09-07 is the first Monday of September: Labor Day.
        assert not is_trading_day(date(2026, 9, 7))
        # Fri 2026-09-04 + 1 session is Tue 2026-09-08, not Mon the 7th.
        assert nth_trading_session_after(date(2026, 9, 4), 1) == date(2026, 9, 8)

    def test_r21_good_friday_and_christmas_are_not_sessions(self):
        # Good Friday 2026-04-03, Christmas 2026-12-25 (a Friday).
        assert not is_trading_day(date(2026, 4, 3))
        assert not is_trading_day(date(2026, 12, 25))
        assert nth_trading_session_after(date(2026, 4, 2), 1) == date(2026, 4, 6)

    def test_r21_dst_transitions_do_not_move_a_session(self):
        # US DST ends 2026-11-01. The sessions either side are ordinary ones and
        # the horizon must not gain or lose a day because the clocks changed.
        assert nth_trading_session_after(date(2026, 10, 30), 1) == date(2026, 11, 2)
        assert nth_trading_session_after(date(2026, 10, 30), 5) == date(2026, 11, 6)

    def test_every_horizon_is_strictly_forward_and_ordered(self):
        s = date(2026, 8, 28)
        previous = s
        for horizon in ro.HORIZONS:
            resolved = ro.horizon_session_for(s, horizon)
            assert resolved > previous
            previous = resolved

    def test_twenty_sessions_is_about_a_calendar_month(self):
        s = date(2026, 8, 28)
        assert ro.horizon_session_for(s, 20) == date(2026, 9, 28)

    def test_trading_sessions_between_counts_sessions_not_days(self):
        # Fri -> Mon is ONE session even though it is three calendar days.
        assert trading_sessions_between(date(2026, 8, 28), date(2026, 8, 31)) == 1
        assert trading_sessions_between(date(2026, 8, 28), date(2026, 8, 28)) == 0

    def test_horizon_below_one_is_refused(self):
        with pytest.raises(ValueError):
            nth_trading_session_after(date(2026, 8, 28), 0)

    def test_r18_a_missing_research_session_does_not_shift_a_horizon(self):
        """No research run happened for 2026-09-02. The horizon grid is the
        MARKET's, not ours, so a scan on 2026-09-01 still measures 1D at
        2026-09-02 — and that is a real trading session whether or not the
        lifecycle woke up."""
        assert is_trading_day(date(2026, 9, 2))
        assert ro.horizon_session_for(date(2026, 9, 1), 1) == date(2026, 9, 2)
        assert ro.horizon_session_for(date(2026, 9, 1), 3) == date(2026, 9, 4)


# --------------------------------------------------------------------------- #
# the measurement itself
# --------------------------------------------------------------------------- #

class TestMeasurement:

    def test_r01_simple_positive_return(self):
        result = ro.measure(entry=bar(date(2026, 8, 28), 100.0),
                            exit_bar=bar(date(2026, 8, 31), 110.0),
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 8, 31), 505.0),
                            window=[bar(date(2026, 8, 31), 110.0)],
                            horizon_sessions=1)
        assert result["status"] == ro.STATUS_MEASURED
        assert result["symbol_return_pct"] == pytest.approx(10.0)
        assert result["benchmark_return_pct"] == pytest.approx(1.0)
        assert result["excess_return_pct"] == pytest.approx(9.0)

    def test_a_negative_return_is_measured_exactly_as_a_positive_one(self):
        result = ro.measure(entry=bar(date(2026, 8, 28), 100.0),
                            exit_bar=bar(date(2026, 8, 31), 92.0),
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 8, 31), 510.0),
                            window=[bar(date(2026, 8, 31), 92.0)],
                            horizon_sessions=1)
        assert result["symbol_return_pct"] == pytest.approx(-8.0)
        assert result["excess_return_pct"] == pytest.approx(-10.0)

    def test_r06_the_reference_is_the_scan_session_close(self):
        """The entry price is the close of the scan's OWN session — the last
        bar the scan could see — not the next session's open and not a later
        trigger."""
        entry = bar(date(2026, 8, 28), 100.0, open_=90.0, high=101.0, low=89.0)
        result = ro.measure(entry=entry,
                            exit_bar=bar(date(2026, 8, 31), 100.0),
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 8, 31), 500.0),
                            window=[bar(date(2026, 8, 31), 100.0)],
                            horizon_sessions=1)
        assert result["entry_close"] == 100.0
        # A zero return, because close-to-close is zero. Using the entry OPEN
        # would have produced +11.1% out of nothing.
        assert result["symbol_return_pct"] == pytest.approx(0.0)

    def test_r07_benchmark_uses_the_same_start_and_end_sessions(self):
        """The benchmark leg is computed from the same two dates. A benchmark
        that had moved on while the symbol had not is the 2026-08-31 defect,
        and here it cannot arise: there is nowhere to put a third date."""
        result = ro.measure(entry=bar(date(2026, 8, 28), 50.0),
                            exit_bar=bar(date(2026, 9, 1), 55.0),
                            bench_entry=bar(date(2026, 8, 28), 600.0),
                            bench_exit=bar(date(2026, 9, 1), 612.0),
                            window=[bar(date(2026, 8, 31), 52.0),
                                    bar(date(2026, 9, 1), 55.0)],
                            horizon_sessions=2)
        assert result["symbol_return_pct"] == pytest.approx(10.0)
        assert result["benchmark_return_pct"] == pytest.approx(2.0)
        assert result["excess_return_pct"] == pytest.approx(8.0)

    @pytest.mark.parametrize("drop,expected", [
        ("entry", ro.REASON_MISSING_SYMBOL_ENTRY),
        ("exit", ro.REASON_MISSING_SYMBOL_EXIT),
        ("bench_entry", ro.REASON_MISSING_BENCHMARK_ENTRY),
        ("bench_exit", ro.REASON_MISSING_BENCHMARK_EXIT),
    ])
    def test_r08_r09_any_missing_endpoint_waits_and_writes_no_number(
            self, drop, expected):
        bars = {"entry": bar(date(2026, 8, 28), 100.0),
                "exit": bar(date(2026, 8, 31), 110.0),
                "bench_entry": bar(date(2026, 8, 28), 500.0),
                "bench_exit": bar(date(2026, 8, 31), 505.0)}
        bars[drop] = None
        result = ro.measure(entry=bars["entry"], exit_bar=bars["exit"],
                            bench_entry=bars["bench_entry"],
                            bench_exit=bars["bench_exit"],
                            window=[], horizon_sessions=1)
        assert result["status"] == ro.STATUS_WAITING_FOR_DATA
        assert result["status_reason"] == expected
        # Not a zero, not a partial row: NO number at all.
        for key in ("symbol_return_pct", "benchmark_return_pct",
                    "excess_return_pct", "entry_close", "exit_close"):
            assert key not in result

    def test_a_non_positive_reference_price_is_never_divided_by(self):
        result = ro.measure(entry=bar(date(2026, 8, 28), 0.0),
                            exit_bar=bar(date(2026, 8, 31), 10.0),
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 8, 31), 505.0),
                            window=[], horizon_sessions=1)
        assert result["status"] == ro.STATUS_WAITING_FOR_DATA
        assert result["status_reason"] == ro.REASON_NON_POSITIVE_PRICE

    def test_mfe_mae_over_a_complete_window(self):
        window = [bar(date(2026, 8, 31), 104.0, high=112.0, low=97.0),
                  bar(date(2026, 9, 1), 108.0, high=109.0, low=94.0),
                  bar(date(2026, 9, 2), 110.0, high=111.0, low=105.0)]
        result = ro.measure(entry=bar(date(2026, 8, 28), 100.0),
                            exit_bar=window[-1],
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 9, 2), 500.0),
                            window=window, horizon_sessions=3)
        assert result["mfe_pct"] == pytest.approx(12.0)   # high 112 vs 100
        assert result["mae_pct"] == pytest.approx(-6.0)   # low 94 vs 100
        assert result["excursion_basis"] == ro.EXCURSION_BASIS_DAILY
        assert result["window_sessions_expected"] == 3
        assert result["window_sessions_present"] == 3

    def test_an_incomplete_window_yields_no_excursion_but_still_a_return(self):
        """The return needs two closes; the excursion needs every session. A
        missing INTERIOR bar must not discard a fact we hold."""
        window = [bar(date(2026, 9, 2), 110.0, high=111.0, low=105.0)]
        result = ro.measure(entry=bar(date(2026, 8, 28), 100.0),
                            exit_bar=window[-1],
                            bench_entry=bar(date(2026, 8, 28), 500.0),
                            bench_exit=bar(date(2026, 9, 2), 500.0),
                            window=window, horizon_sessions=3)
        assert result["status"] == ro.STATUS_MEASURED
        assert result["symbol_return_pct"] == pytest.approx(10.0)
        assert result["mfe_pct"] is None and result["mae_pct"] is None
        assert result["excursion_basis"] == ro.EXCURSION_BASIS_INCOMPLETE
        assert result["window_sessions_present"] == 1

    def test_bars_hash_is_deterministic_and_moves_with_a_corrected_close(self):
        args = dict(entry=bar(date(2026, 8, 28), 100.0),
                    exit_bar=bar(date(2026, 8, 31), 110.0),
                    bench_entry=bar(date(2026, 8, 28), 500.0),
                    bench_exit=bar(date(2026, 8, 31), 505.0),
                    window=[bar(date(2026, 8, 31), 110.0)],
                    horizon_sessions=1)
        first = ro.measure(**args)["bars_hash"]
        assert first == ro.measure(**args)["bars_hash"]
        corrected = dict(args, exit_bar=bar(date(2026, 8, 31), 110.01),
                         window=[bar(date(2026, 8, 31), 110.01)])
        assert ro.measure(**corrected)["bars_hash"] != first

    def test_window_order_does_not_change_the_hash(self):
        window = [bar(date(2026, 9, 1), 108.0), bar(date(2026, 8, 31), 104.0)]
        args = dict(entry=bar(date(2026, 8, 28), 100.0),
                    exit_bar=bar(date(2026, 9, 1), 108.0),
                    bench_entry=bar(date(2026, 8, 28), 500.0),
                    bench_exit=bar(date(2026, 9, 1), 500.0),
                    horizon_sessions=2)
        assert (ro.measure(window=window, **args)["bars_hash"]
                == ro.measure(window=list(reversed(window)), **args)["bars_hash"])


# --------------------------------------------------------------------------- #
# planning
# --------------------------------------------------------------------------- #

def scan_row(symbol="AAL", session=date(2026, 8, 28), *, candidate=True,
             config_hash="cfg-1", scanned_at=None, scan_id=None):
    """A `research_scan_results` row shaped exactly as the SELECT returns it."""
    base = {
        "id": scan_id or f"{symbol}-{session.isoformat()}",
        "symbol": symbol, "scan_session": session,
        "scanned_at": scanned_at or datetime(2026, 8, 28, 22, 30, tzinfo=UTC),
        "strategy_code": "wyckoff_mtf", "strategy_version": "v2",
        "config_hash": config_hash, "verdict": "WATCH",
        "reason_code": "spring_confirmed", "rejection_reason": None,
    }
    if candidate:
        base.update({"structure_state": "recognized", "setup_state": "valid",
                     "benchmark_relative": "outperforming"})
    else:
        base.update({"structure_state": "unknown", "setup_state": "unknown",
                     "benchmark_relative": "underperforming"})
    return base


class TestPlanning:

    def test_a_scan_owes_exactly_one_observation_per_horizon(self):
        plans = ro.plan_observations(scan_row())
        assert [p["horizon_sessions"] for p in plans] == list(ro.HORIZONS)
        assert [p["horizon_label"] for p in plans] == ["1D", "3D", "5D", "10D", "20D"]
        assert all(p["horizon_session"] > p["scan_session"] for p in plans)

    def test_r12_a_candidate_scan_is_planned_as_a_candidate(self):
        plans = ro.plan_observations(scan_row(candidate=True))
        assert {p["scan_classification"] for p in plans} == {
            rf.LIFECYCLE_RESEARCH_CANDIDATE}

    def test_r13_a_non_candidate_scan_is_planned_too(self):
        """O1: the screen is the thing under test, and a test with only
        positives is a description."""
        plans = ro.plan_observations(scan_row(candidate=False))
        assert {p["scan_classification"] for p in plans} == {
            rf.LIFECYCLE_SCANNED_NOT_CANDIDATE}
        assert len(plans) == len(ro.HORIZONS)

    def test_a_hard_rejected_scan_is_not_a_candidate(self):
        row = scan_row(candidate=True)
        row["rejection_reason"] = "price_below_minimum"
        plans = ro.plan_observations(row)
        assert {p["scan_classification"] for p in plans} == {
            rf.LIFECYCLE_SCANNED_NOT_CANDIDATE}

    def test_a_scan_with_no_evidence_at_all_is_classification_pending(self):
        row = scan_row()
        for key in ("rejection_reason", "structure_state", "setup_state",
                    "benchmark_relative"):
            row[key] = None
        plans = ro.plan_observations(row)
        assert {p["scan_classification"] for p in plans} == {
            rf.LIFECYCLE_CLASSIFICATION_PENDING}

    def test_classification_uses_the_funnel_s_own_function(self):
        """One definition of `research_candidate`, not two. If this ever
        diverges, the lifecycle funnel and the outcome ledger would disagree
        about the same scan."""
        row = scan_row(candidate=True)
        assert (ro.plan_observations(row)[0]["scan_classification"]
                == rf.scan_classification(row))

    def test_r15_config_hash_is_snapshot_onto_every_observation(self):
        a = ro.plan_observations(scan_row(config_hash="cfg-A"))
        b = ro.plan_observations(scan_row(symbol="NU", config_hash="cfg-B"))
        assert {p["config_hash"] for p in a} == {"cfg-A"}
        assert {p["config_hash"] for p in b} == {"cfg-B"}

    def test_r14_two_sessions_of_one_symbol_produce_independent_horizons(self):
        first = ro.plan_observations(scan_row("AAL", date(2026, 8, 28)))
        second = ro.plan_observations(scan_row("AAL", date(2026, 9, 1)))
        assert first[0]["horizon_session"] != second[0]["horizon_session"]
        assert first[0]["scan_id"] != second[0]["scan_id"]
        # Same symbol, same horizon, different scan: two different observations.
        assert len({(p["scan_id"], p["horizon_sessions"])
                    for p in first + second}) == 2 * len(ro.HORIZONS)

    def test_the_attribution_snapshot_carries_the_scan_s_own_words(self):
        plan = ro.plan_observations(scan_row(candidate=True))[0]
        assert plan["scan_verdict"] == "WATCH"
        assert plan["scan_setup_state"] == "valid"
        assert plan["scan_structure_state"] == "recognized"
        assert plan["scan_reason_code"] == "spring_confirmed"
        assert plan["scan_benchmark_relative"] == "outperforming"
        assert plan["scan_scanned_at"] is not None
        assert plan["benchmark_symbol"] == "SPY"

    def test_the_versions_travel_on_the_row(self):
        plan = ro.plan_observations(scan_row())[0]
        assert plan["contract_version"] == "research_scan_outcome.v1"
        assert plan["calculation_version"] == "outcome.v1"
        assert plan["market_calendar_version"] == "us_market_calendar.v1"


# --------------------------------------------------------------------------- #
# a small, honest fake connection for the flow-level cases
# --------------------------------------------------------------------------- #

class FakeConn:
    """Enough Postgres to exercise the engine's control flow.

    Deliberately NOT a general fake: it recognises the three shapes this module
    actually issues and raises on anything else, so a query that changes shape
    fails loudly here instead of quietly agreeing. The real SQL is proven
    against real Postgres in the integration file.
    """

    def __init__(self, bars=None, observations=None, scans=None):
        # {(symbol, date): (open, high, low, close)}
        self.bars = dict(bars or {})
        self.observations = list(observations or [])
        self.scans = list(scans or [])
        self.measured_writes = []
        self.pending_writes = []
        self.revision_writes = []
        self.plan_inserts = []

    # -- helpers ----------------------------------------------------------- #
    def _row(self, symbol, day):
        v = self.bars.get((symbol, day))
        if v is None:
            return None
        o, h, low, c = v
        return {"trading_date": day, "open": o, "high": h, "low": low,
                "close": c}

    # -- asyncpg surface ---------------------------------------------------- #
    async def fetchrow(self, sql, *args):
        if "FROM public.daily_bars" in sql and "trading_date = $2" in sql:
            return self._row(args[0], args[1])
        if sql.strip().startswith("INSERT INTO public.research_scan_outcomes"):
            key = (args[0], args[3])
            if key in {(p[0], p[3]) for p in self.plan_inserts}:
                return None
            self.plan_inserts.append(args)
            return {"id": f"obs-{args[0]}-{args[3]}"}
        if sql.strip().startswith("UPDATE public.research_scan_outcomes SET\n    status = $2, status_reason = $3,\n    entry_close"):
            self.measured_writes.append(args)
            return {"id": args[0]}
        raise AssertionError(f"unexpected fetchrow: {sql[:90]!r}")

    async def fetch(self, sql, *args):
        if "FROM public.daily_bars" in sql and "trading_date > $2" in sql:
            symbol, start, end = args
            return [self._row(symbol, d) for (s, d) in sorted(self.bars)
                    if s == symbol and start < d <= end]
        if "FROM public.research_scan_results s" in sql:
            return list(self.scans)[:args[0]]
        if "FROM public.research_scan_outcomes" in sql:
            return list(self.observations)
        raise AssertionError(f"unexpected fetch: {sql[:90]!r}")

    async def execute(self, sql, *args):
        if "attempt_count = attempt_count + 1" in sql and "revision_notes" in sql:
            self.revision_writes.append(args)
            return "UPDATE 1"
        if "attempt_count = attempt_count + 1" in sql:
            self.pending_writes.append(args)
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute: {sql[:90]!r}")


def observation(symbol="AAL", scan_session=date(2026, 8, 28), horizon=1, *,
                status=ro.STATUS_NOT_YET_ELIGIBLE, bars_hash=None,
                symbol_return_pct=None, excess_return_pct=None,
                horizon_session=None):
    return {
        "id": f"obs-{symbol}-{scan_session}-{horizon}",
        "scan_id": f"{symbol}-{scan_session.isoformat()}",
        "symbol": symbol, "scan_session": scan_session,
        "horizon_sessions": horizon, "horizon_label": f"{horizon}D",
        "horizon_session": horizon_session or ro.horizon_session_for(
            scan_session, horizon),
        "benchmark_symbol": "SPY", "status": status,
        "bars_hash": bars_hash, "symbol_return_pct": symbol_return_pct,
        "excess_return_pct": excess_return_pct,
    }


class TestMeasurementFlow:

    def test_r05_a_horizon_that_has_not_completed_is_never_measured(self):
        """R05 / R16 — the 10D and 20D horizons of a recent scan stay pending
        and no future bar is consulted, even when future bars exist."""
        obs = observation(horizon=10, scan_session=date(2026, 9, 1))
        conn = FakeConn(bars={("AAL", date(2026, 9, 1)): (1, 1, 1, 100.0),
                              ("AAL", obs["horizon_session"]): (1, 1, 1, 200.0),
                              ("SPY", date(2026, 9, 1)): (1, 1, 1, 500.0),
                              ("SPY", obs["horizon_session"]): (1, 1, 1, 500.0)})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert result["status"] == ro.STATUS_NOT_YET_ELIGIBLE
        assert result["status_reason"] == ro.REASON_HORIZON_NOT_COMPLETE
        assert conn.measured_writes == [] and conn.pending_writes == []

    def test_r08_a_missing_symbol_exit_bar_waits(self):
        obs = observation(horizon=1)
        conn = FakeConn(bars={("AAL", date(2026, 8, 28)): (1, 1, 1, 100.0),
                              ("SPY", date(2026, 8, 28)): (1, 1, 1, 500.0),
                              ("SPY", date(2026, 8, 31)): (1, 1, 1, 505.0)})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert result["status"] == ro.STATUS_WAITING_FOR_DATA
        assert result["status_reason"] == ro.REASON_MISSING_SYMBOL_EXIT
        assert conn.measured_writes == []
        assert conn.pending_writes[0][1] == ro.STATUS_WAITING_FOR_DATA

    def test_r09_a_missing_benchmark_bar_waits_even_with_a_full_symbol(self):
        obs = observation(horizon=1)
        conn = FakeConn(bars={("AAL", date(2026, 8, 28)): (1, 1, 1, 100.0),
                              ("AAL", date(2026, 8, 31)): (1, 1, 1, 130.0),
                              ("SPY", date(2026, 8, 28)): (1, 1, 1, 500.0)})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert result["status"] == ro.STATUS_WAITING_FOR_DATA
        assert result["status_reason"] == ro.REASON_MISSING_BENCHMARK_EXIT
        assert conn.measured_writes == []

    def test_r10_a_later_bar_arrival_measures_exactly_once(self):
        """Pass 1 waits. The bar arrives. Pass 2 measures. Pass 3 finds it
        already measured and only rechecks."""
        obs = observation(horizon=1)
        conn = FakeConn(bars={("AAL", date(2026, 8, 28)): (1, 1, 1, 100.0),
                              ("SPY", date(2026, 8, 28)): (1, 1, 1, 500.0),
                              ("SPY", date(2026, 8, 31)): (1, 1, 1, 505.0)})
        first = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert first["status"] == ro.STATUS_WAITING_FOR_DATA

        conn.bars[("AAL", date(2026, 8, 31))] = (1, 1, 1, 110.0)
        obs["status"] = ro.STATUS_WAITING_FOR_DATA
        second = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert second["status"] == ro.STATUS_MEASURED
        assert len(conn.measured_writes) == 1

        obs.update({"status": ro.STATUS_MEASURED,
                    "bars_hash": second["bars_hash"]})
        third = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert third["unchanged"] is True
        # Still exactly one measurement write, ever.
        assert len(conn.measured_writes) == 1
        assert conn.revision_writes == []

    def test_r17_a_corrected_bar_is_recorded_and_never_applied(self):
        obs = observation(horizon=1, status=ro.STATUS_MEASURED,
                          bars_hash="a-hash-from-the-original-bars",
                          symbol_return_pct=10.0, excess_return_pct=9.0)
        conn = FakeConn(bars={("AAL", date(2026, 8, 28)): (1, 1, 1, 100.0),
                              ("AAL", date(2026, 8, 31)): (1, 1, 1, 111.0),
                              ("SPY", date(2026, 8, 28)): (1, 1, 1, 500.0),
                              ("SPY", date(2026, 8, 31)): (1, 1, 1, 505.0)})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert result["revision_detected"] is True
        assert result["revision_kind"] == "bars_revised"
        # The measurement path was never entered.
        assert conn.measured_writes == []
        assert len(conn.revision_writes) == 1

    def test_bars_that_vanish_under_a_measured_row_are_recorded_too(self):
        obs = observation(horizon=1, status=ro.STATUS_MEASURED,
                          bars_hash="frozen", symbol_return_pct=10.0)
        conn = FakeConn(bars={("AAL", date(2026, 8, 28)): (1, 1, 1, 100.0),
                              ("SPY", date(2026, 8, 28)): (1, 1, 1, 500.0)})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=date(2026, 9, 4)))
        assert result["revision_kind"] == "bars_no_longer_available"
        assert conn.measured_writes == []

    def test_a_long_wait_becomes_terminal_and_says_why(self):
        """O13's fourth status. Reached only after a grace measured in
        SESSIONS, deliberately several multiples of the pool's refresh cycle."""
        scan_session = date(2026, 1, 5)
        obs = observation(horizon=1, scan_session=scan_session)
        far_future = ro.horizon_session_for(scan_session, 1)
        for _ in range(ro.MISSING_DATA_GRACE_SESSIONS + 2):
            far_future = ro.horizon_session_for(far_future, 1)
        conn = FakeConn(bars={})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=far_future))
        assert result["status"] == ro.STATUS_FAILED_TERMINAL
        assert result["status_reason"] == ro.REASON_GRACE_EXCEEDED
        assert conn.pending_writes[0][1] == ro.STATUS_FAILED_TERMINAL

    def test_just_inside_the_grace_is_still_waiting_not_terminal(self):
        scan_session = date(2026, 1, 5)
        obs = observation(horizon=1, scan_session=scan_session)
        cursor = obs["horizon_session"]
        for _ in range(ro.MISSING_DATA_GRACE_SESSIONS - 1):
            cursor = ro.horizon_session_for(cursor, 1)
        conn = FakeConn(bars={})
        result = asyncio.run(ro.measure_observation(
            conn, obs, as_of_session=cursor))
        assert result["status"] == ro.STATUS_WAITING_FOR_DATA


class TestBoundedness:
    """R23 — a backfill must be bounded, and must say when it stopped short."""

    def test_the_schedule_template_can_lower_a_bound_but_never_raise_it(self):
        import app.jobs.research_outcomes as jro
        payload = jro.task_payload_from_template(
            {"scan_limit": 10, "observation_limit": 25}, run_key="k")
        assert payload["scan_limit"] == 10
        assert payload["observation_limit"] == 25

        greedy = jro.task_payload_from_template(
            {"scan_limit": 10 ** 6, "observation_limit": 10 ** 6}, run_key="k")
        assert greedy["scan_limit"] == ro.DEFAULT_SCAN_LIMIT
        assert greedy["observation_limit"] == ro.DEFAULT_OBSERVATION_LIMIT

    def test_a_nonsense_template_falls_back_to_the_defaults(self):
        import app.jobs.research_outcomes as jro
        payload = jro.task_payload_from_template(
            {"scan_limit": "lots", "observation_limit": None}, run_key="k")
        assert payload["scan_limit"] == ro.DEFAULT_SCAN_LIMIT
        assert payload["observation_limit"] == ro.DEFAULT_OBSERVATION_LIMIT

    def test_a_schedule_can_never_ask_to_reread_frozen_evidence(self):
        import app.jobs.research_outcomes as jro
        payload = jro.task_payload_from_template({"include_settled": True},
                                                 run_key="k")
        assert payload["include_settled"] is False

    def test_a_run_that_stops_at_its_bound_says_so(self):
        conn = FakeConn(
            scans=[scan_row(f"SYM{i}") for i in range(3)],
            observations=[observation(f"SYM{i}", horizon=1) for i in range(3)])
        summary = asyncio.run(ro.run_maturation(
            conn, now=datetime(2026, 9, 4, 23, tzinfo=UTC),
            scan_limit=3, observation_limit=3))
        assert summary["truncated_by_limit"] is True


class TestRunKeys:
    """R11 — a duplicate job execution must not become a second run."""

    def test_one_occurrence_yields_one_run_key(self):
        import app.jobs.research_outcomes as jro
        first = jro.run_key_for_occurrence(
            schedule_code="SMART-SCANNER-RESEARCH-OUTCOMES",
            schedule_version=1, occurrence_iso="2026-09-04T23:30:00+00:00")
        second = jro.run_key_for_occurrence(
            schedule_code="SMART-SCANNER-RESEARCH-OUTCOMES",
            schedule_version=1, occurrence_iso="2026-09-04T23:30:00+00:00")
        assert first == second

    def test_tomorrow_is_a_different_run(self):
        import app.jobs.research_outcomes as jro
        assert jro.run_key_for_occurrence(
            schedule_code="S", schedule_version=1,
            occurrence_iso="2026-09-04T23:30:00+00:00") != \
            jro.run_key_for_occurrence(
                schedule_code="S", schedule_version=1,
                occurrence_iso="2026-09-07T23:30:00+00:00")

    def test_a_manual_run_can_never_collide_with_a_scheduled_occurrence(self):
        import app.jobs.research_outcomes as jro
        manual = jro.manual_run_key(label="backfill",
                                    now=datetime(2026, 9, 6, 12, tzinfo=UTC))
        assert manual.startswith("roc:manual:")
        scheduled = jro.run_key_for_occurrence(
            schedule_code="S", schedule_version=1,
            occurrence_iso="2026-09-04T23:30:00+00:00")
        assert manual != scheduled
        assert not scheduled.startswith("roc:manual:")
