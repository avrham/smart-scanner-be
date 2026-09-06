"""Real-Postgres validation of the research outcome ledger.

WHY THIS FILE EXISTS
--------------------
`test_research_outcomes_unit.py` proves the calendar, the arithmetic and the
control flow. Four things in this change are SQL and cannot be proven that way,
and a fake `execute()` would happily agree with all four:

  * migration 031 applies at the END of the real chain (001 .. 031);
  * `(scan_id, horizon_sessions)` and `(symbol, scan_session, horizon_sessions)`
    really do make a duplicate observation impossible;
  * the CHECK constraints really do refuse a pending row that carries a number
    and a horizon that is not in the future of its scan;
  * `research_scan_outcomes_freeze` really does REJECT an update to a measured
    outcome — which is the whole of the O8 policy. A trigger that exists and
    does not fire is worse than no trigger, because it is believed.

And one thing is an END-TO-END question that only real rows can answer: does
the engine, run against a real store through its real entry point, produce
numbers that an independent recomputation from the raw bars agrees with?

Uses the same docker-postgres harness as the other *_integration tests.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.research_funnel as rf
import app.research_outcomes as ro
from app.prospective_session import is_trading_day, nth_trading_session_after

PG_IMAGE = "postgres:16-alpine"
DBNAME = "rscoutdb"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "012_wyckoff_mtf_v2", "013_wyckoff_v2_shadow_arms",
              "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes",
              "017_prospective_campaign_registration",
              "018_durable_job_queue", "019_catalyst_events",
              "020_company_news", "021_sec_material_events",
              "022_external_signals", "023_external_discovery",
              "024_market_calendar_and_analyst",
              "025_discovery_reference_session",
              "026_research_symbols", "027_research_admission",
              "028_source_state_scope", "029_research_lifecycle_runs",
              "030_research_session_correctness",
              "031_research_scan_outcomes",
              "032_research_outcome_freeze_attribution"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UTC = timezone.utc

#: A scan session and the sessions after it, all real 2026 trading days.
#: Fri 2026-08-28 -> Mon 31 (1D) -> Wed Sep 2 (3D) -> Fri Sep 4 (5D).
SCAN_SESSION = date(2026, 8, 28)
S1 = date(2026, 8, 31)
S3 = date(2026, 9, 2)
S5 = date(2026, 9, 4)

#: "Now": after the 2026-09-04 close, so 5D is the newest completed session and
#: 10D / 20D have not happened.
NOW = datetime(2026, 9, 4, 21, 0, tzinfo=UTC)


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(),
                                reason="docker/pg image unavailable")


def _sh(a, inp=None, t=240):
    return subprocess.run(a, input=inp, capture_output=True, text=True, timeout=t)


def _psql(cid, sql, *, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", "postgres", "-d", DBNAME]
    return _sh(args, inp=(open(path).read() if path else sql))


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres",
               "-P", PG_IMAGE]).stdout.strip()
    assert cid
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready", "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        hp = int(_sh(["docker", "port", cid, "5432/tcp"]).stdout.splitlines()[0]
                 .rsplit(":", 1)[1])
        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                    f"CREATE DATABASE {DBNAME};"]).returncode == 0
        # R20: the WHOLE chain, in order, ending at 031. A migration that only
        # applies to a hand-made subset of the schema is not a migration.
        for m in MIGRATIONS:
            r = _psql(cid, None,
                      path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-800:]}"
        yield {"cid": cid,
               "dsn": f"postgresql://postgres:postgres@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "kill", cid])


# --------------------------------------------------------------------------- #
# fixtures for the store
# --------------------------------------------------------------------------- #

async def _reset(conn):
    await conn.execute(
        "TRUNCATE public.research_scan_outcomes, public.research_outcome_runs, "
        "public.research_scan_results, public.research_symbols, "
        "public.daily_bars, public.job_tasks, public.job_runs "
        "RESTART IDENTITY CASCADE")


async def _add_symbol(conn, symbol):
    await conn.execute(
        "INSERT INTO public.research_symbols (symbol, discovery_source,"
        " discovery_reasons, first_observed_at, latest_observed_at,"
        " first_reference_session, latest_reference_session,"
        " first_actionable_session, state, admission_state) "
        "VALUES ($1,'fmp',ARRAY['most_active'],NOW(),NOW(),"
        " $2,$2,$2,'research_scanned','eligible_for_history') "
        "ON CONFLICT (symbol) DO NOTHING",
        symbol, SCAN_SESSION)


async def _add_scan(conn, symbol, session=SCAN_SESSION, *, candidate=True,
                    config_hash="cfg-1", scanned_at=None):
    await _add_symbol(conn, symbol)
    fields = ({"structure_state": "recognized", "setup_state": "valid",
               "benchmark_relative": "outperforming", "rejection_reason": None}
              if candidate else
              {"structure_state": "unknown", "setup_state": "unknown",
               "benchmark_relative": "underperforming",
               "rejection_reason": "price_below_minimum"})
    row = await conn.fetchrow(
        "INSERT INTO public.research_scan_results (symbol, scan_session,"
        " scanned_at, contract_version, strategy_code, strategy_version,"
        " config_hash, verdict, structure_state, setup_state, reason_code,"
        " rejection_reason, benchmark_relative, benchmark_symbol) "
        "VALUES ($1,$2,$3,'research_scan.v1','wyckoff_mtf','v2',$4,'WATCH',"
        " $5,$6,'spring_confirmed',$7,$8,'SPY') "
        "ON CONFLICT (symbol, scan_session) DO UPDATE SET scanned_at=EXCLUDED.scanned_at "
        "RETURNING id",
        symbol, session,
        scanned_at or datetime(2026, 8, 28, 22, 30, tzinfo=UTC), config_hash,
        fields["structure_state"], fields["setup_state"],
        fields["rejection_reason"], fields["benchmark_relative"])
    return row["id"]


async def _add_bar(conn, symbol, day, close, *, high=None, low=None,
                   open_=None):
    await conn.execute(
        "INSERT INTO public.daily_bars (symbol, trading_date, open, high, low,"
        " close, volume, source) VALUES ($1,$2,$3,$4,$5,$6,1000,'test') "
        "ON CONFLICT (symbol, trading_date) DO UPDATE SET "
        " open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,"
        " close=EXCLUDED.close",
        symbol, day, open_ if open_ is not None else close,
        high if high is not None else close,
        low if low is not None else close, close)


async def _seed_benchmark(conn, closes):
    for day, close in closes.items():
        await _add_bar(conn, "SPY", day, close)


#: ONE event loop for the whole of a test, because an asyncpg connection is
#: bound to the loop that created it. The project has no pytest-asyncio, and
#: its other integration files wrap each test in a single `async def go()`;
#: this file has far more statements per test, so the loop is hoisted into the
#: fixture and `run(...)` drives it. Same guarantee, less nesting.
_LOOP = None


def run(coro):
    """Drive one coroutine on the fixture's loop."""
    assert _LOOP is not None, "run() used outside the `conn` fixture"
    return _LOOP.run_until_complete(coro)


@pytest.fixture()
def conn(pg):
    """A fresh, truncated database per test, on a dedicated event loop."""
    global _LOOP
    _LOOP = asyncio.new_event_loop()
    c = _LOOP.run_until_complete(asyncpg.connect(pg["dsn"]))
    _LOOP.run_until_complete(_reset(c))
    try:
        yield c
    finally:
        try:
            _LOOP.run_until_complete(c.close())
        finally:
            _LOOP.close()
            _LOOP = None


# --------------------------------------------------------------------------- #
# 1. the schema is the guarantee
# --------------------------------------------------------------------------- #

class TestSchemaGuarantees:

    def test_r20_the_full_migration_chain_applied(self, conn):
        for rel in ("research_scan_outcomes", "research_outcome_runs"):
            assert run(conn.fetchval(
                "SELECT to_regclass($1)", f"public.{rel}")) is not None
        # RLS on, as on every table this project adds.
        assert run(conn.fetchval(
            "SELECT relrowsecurity FROM pg_class "
            "WHERE oid='public.research_scan_outcomes'::regclass")) is True
        # And the schedule row exists, DISABLED and PAUSED.
        row = run(conn.fetchrow(
            "SELECT enabled, paused, job_type, payload_template FROM job_schedules "
            "WHERE schedule_code='SMART-SCANNER-RESEARCH-OUTCOMES'"))
        assert row is not None
        assert row["enabled"] is False and row["paused"] is True
        tmpl = json.loads(row["payload_template"]) \
            if isinstance(row["payload_template"], str) else row["payload_template"]
        assert tmpl["scheduler_owner"] == "research_lifecycle"
        assert tmpl["queue"] == "research_lifecycle"

    def test_the_migration_is_idempotent(self, pg):
        """Re-applying 031 must succeed — AND 032 must be re-applied after it.

        This is not test bookkeeping, it is the ordering rule for this pair.
        031 and 032 both `CREATE OR REPLACE` the same trigger function, so
        replaying 031 on a database that already has 032 silently reinstates
        031's narrower guard. That is true of any replay of an older migration
        that replaces a function, and it is why a replay must always continue
        forward through the rest of the chain rather than stopping at the file
        somebody meant to re-run. The audit found this by replaying 031 here
        and watching thirteen 032 assertions stop holding.
        """
        for name in ("031_research_scan_outcomes",
                     "032_research_outcome_freeze_attribution"):
            r = _psql(pg["cid"], None, path=os.path.join(
                REPO, "app", "db", "migrations", f"{name}.sql"))
            assert r.returncode == 0, f"{name}: {r.stderr[-800:]}"

    def test_the_lifecycle_schedule_was_not_touched(self, conn):
        """The validated 18:30 ET research lifecycle schedule must be exactly
        as migration 029 left it."""
        row = run(conn.fetchrow(
            "SELECT market_close_delay_minutes, job_type FROM job_schedules "
            "WHERE schedule_code='SMART-SCANNER-RESEARCH-LIFECYCLE'"))
        assert row["market_close_delay_minutes"] == 150
        assert row["job_type"] == "smart_scanner_research_lifecycle.v1"

    def test_r19_a_duplicate_observation_is_impossible(self, conn):
        scan_id = run(_add_scan(conn, "AAL"))
        plan = ro.plan_observations({
            "id": scan_id, "symbol": "AAL", "scan_session": SCAN_SESSION,
            "scanned_at": datetime(2026, 8, 28, 22, 30, tzinfo=UTC),
            "strategy_code": "wyckoff_mtf", "strategy_version": "v2",
            "config_hash": "cfg-1", "verdict": "WATCH",
            "structure_state": "recognized", "setup_state": "valid",
            "reason_code": "spring_confirmed", "rejection_reason": None,
            "benchmark_relative": "outperforming"})[0]
        args = [plan[c] for c in ro._PLAN_COLUMNS]
        assert run(conn.fetchrow(ro.INSERT_PLAN_SQL, *args)) is not None
        # Same identity again: absorbed, not duplicated.
        assert run(conn.fetchrow(ro.INSERT_PLAN_SQL, *args)) is None
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_scan_outcomes")) == 1

        # The NATURAL key is a second, independent guarantee: a different
        # scan_id claiming the same (symbol, session, horizon) is refused.
        other = run(_add_scan(conn, "NU"))
        args2 = list(args)
        args2[ro._PLAN_COLUMNS.index("scan_id")] = other
        with pytest.raises(asyncpg.UniqueViolationError):
            run(conn.fetchrow(ro.INSERT_PLAN_SQL, *args2))

    def test_a_horizon_that_is_not_in_the_future_is_refused(self, conn):
        scan_id = run(_add_scan(conn, "AAL"))
        with pytest.raises(asyncpg.CheckViolationError):
            run(conn.execute(
                "INSERT INTO public.research_scan_outcomes (scan_id, symbol,"
                " scan_session, horizon_sessions, horizon_label,"
                " horizon_session, contract_version, calculation_version,"
                " market_calendar_version, strategy_code, strategy_version,"
                " config_hash, scan_classification, scan_scanned_at,"
                " benchmark_symbol) "
                "VALUES ($1,'AAL',$2,1,'1D',$2,'c','o','m','s','v','h',"
                " 'research_candidate',NOW(),'SPY')", scan_id, SCAN_SESSION))

    def test_a_pending_row_can_never_carry_a_number(self, conn):
        """The schema-level statement of O4: no reader can pick up a return
        that was written before its horizon completed, because a non-measured
        row physically cannot hold one."""
        scan_id = run(_add_scan(conn, "AAL"))
        with pytest.raises(asyncpg.CheckViolationError):
            run(conn.execute(
                "INSERT INTO public.research_scan_outcomes (scan_id, symbol,"
                " scan_session, horizon_sessions, horizon_label,"
                " horizon_session, contract_version, calculation_version,"
                " market_calendar_version, strategy_code, strategy_version,"
                " config_hash, scan_classification, scan_scanned_at,"
                " benchmark_symbol, status, symbol_return_pct) "
                "VALUES ($1,'AAL',$2,1,'1D',$3,'c','o','m','s','v','h',"
                " 'research_candidate',NOW(),'SPY','waiting_for_data',9.9)",
                scan_id, SCAN_SESSION, S1))

    def test_a_measured_row_can_never_be_incomplete(self, conn):
        scan_id = run(_add_scan(conn, "AAL"))
        with pytest.raises(asyncpg.CheckViolationError):
            run(conn.execute(
                "INSERT INTO public.research_scan_outcomes (scan_id, symbol,"
                " scan_session, horizon_sessions, horizon_label,"
                " horizon_session, contract_version, calculation_version,"
                " market_calendar_version, strategy_code, strategy_version,"
                " config_hash, scan_classification, scan_scanned_at,"
                " benchmark_symbol, status, measured_at) "
                "VALUES ($1,'AAL',$2,1,'1D',$3,'c','o','m','s','v','h',"
                " 'research_candidate',NOW(),'SPY','measured',NOW())",
                scan_id, SCAN_SESSION, S1))


# --------------------------------------------------------------------------- #
# 2. end to end, through the real entry point
# --------------------------------------------------------------------------- #

def _seed_measurable(conn):
    """One candidate and one non-candidate scanned on 2026-08-28, with real
    forward bars through 2026-09-04 and a benchmark that moved."""
    run(_add_scan(conn, "AAL", candidate=True, config_hash="cfg-A"))
    run(_add_scan(conn, "NU", candidate=False, config_hash="cfg-B"))
    # AAL: 100 -> 110 (1D) -> 105 (3D) -> 120 (5D). A high of 130 on Sep 3 and
    # a low of 90 on Sep 1 exercise MFE/MAE.
    for day, close, hi, lo in [(SCAN_SESSION, 100.0, 101.0, 99.0),
                               (S1, 110.0, 112.0, 108.0),
                               (date(2026, 9, 1), 95.0, 111.0, 90.0),
                               (S3, 105.0, 106.0, 94.0),
                               (date(2026, 9, 3), 118.0, 130.0, 104.0),
                               (S5, 120.0, 121.0, 117.0)]:
        run(_add_bar(conn, "AAL", day, close, high=hi, low=lo))
    # NU: 50 -> 49.5 (1D), a loser, and the control-like half of the sample.
    for day, close in [(SCAN_SESSION, 50.0), (S1, 49.5),
                       (date(2026, 9, 1), 49.0), (S3, 48.0),
                       (date(2026, 9, 3), 47.5), (S5, 46.0)]:
        run(_add_bar(conn, "NU", day, close))
    run(_seed_benchmark(conn, {SCAN_SESSION: 500.0, S1: 505.0,
                               date(2026, 9, 1): 502.0, S3: 510.0,
                               date(2026, 9, 3): 508.0, S5: 515.0}))


class TestEndToEnd:

    def test_r01_r04_r12_r13_a_full_pass_measures_what_is_eligible(self, conn):
        _seed_measurable(conn)
        summary = run(ro.run_outcome_maturation(conn, run_key="it-1", now=NOW))

        assert summary["status"] == ro.STATUS_COMPLETED
        assert summary["as_of_session"] == S5.isoformat()
        assert summary["scans_considered"] == 2
        assert summary["observations_planned"] == 2 * len(ro.HORIZONS)
        # 1D / 3D / 5D are eligible for both scans; 10D / 20D are not.
        assert summary["observations_due"] == 6
        assert summary["measured"] == 6
        assert summary["waiting_for_data"] == 0

        rows = {(r["symbol"], r["horizon_label"]): dict(r) for r in run(conn.fetch(
            "SELECT * FROM public.research_scan_outcomes ORDER BY symbol, "
            "horizon_sessions"))}
        assert len(rows) == 10

        # R12 the candidate is measured...
        aal_1d = rows[("AAL", "1D")]
        assert aal_1d["scan_classification"] == rf.LIFECYCLE_RESEARCH_CANDIDATE
        assert aal_1d["status"] == ro.STATUS_MEASURED
        assert float(aal_1d["symbol_return_pct"]) == pytest.approx(10.0)
        assert float(aal_1d["benchmark_return_pct"]) == pytest.approx(1.0)
        assert float(aal_1d["excess_return_pct"]) == pytest.approx(9.0)
        # ...and R13 the non-candidate is measured just the same.
        nu_1d = rows[("NU", "1D")]
        assert nu_1d["scan_classification"] == rf.LIFECYCLE_SCANNED_NOT_CANDIDATE
        assert nu_1d["status"] == ro.STATUS_MEASURED
        assert float(nu_1d["symbol_return_pct"]) == pytest.approx(-1.0)
        assert float(nu_1d["excess_return_pct"]) == pytest.approx(-2.0)

        # R02: the 3D horizon crossed a weekend and landed on a real session.
        assert rows[("AAL", "3D")]["horizon_session"] == S3
        assert float(rows[("AAL", "3D")]["symbol_return_pct"]) == pytest.approx(5.0)
        # R04: 5D.
        assert rows[("AAL", "5D")]["horizon_session"] == S5
        assert float(rows[("AAL", "5D")]["symbol_return_pct"]) == pytest.approx(20.0)
        assert float(rows[("AAL", "5D")]["excess_return_pct"]) == pytest.approx(17.0)

        # R05: the long horizons are pending and hold no number at all.
        for label in ("10D", "20D"):
            row = rows[("AAL", label)]
            assert row["status"] == ro.STATUS_NOT_YET_ELIGIBLE
            assert row["symbol_return_pct"] is None
            assert row["measured_at"] is None
            assert row["horizon_session"] > S5

        # R15: the config hash is attributable per scan.
        assert rows[("AAL", "1D")]["config_hash"] == "cfg-A"
        assert rows[("NU", "1D")]["config_hash"] == "cfg-B"

    def test_mfe_mae_come_from_the_real_window(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="it-mfe", now=NOW))
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=5"))
        # Highest high after the scan is 130 on Sep 3; lowest low is 90 on Sep 1.
        assert float(row["mfe_pct"]) == pytest.approx(30.0)
        assert float(row["mae_pct"]) == pytest.approx(-10.0)
        assert row["excursion_basis"] == ro.EXCURSION_BASIS_DAILY
        assert row["window_sessions_expected"] == 5
        assert row["window_sessions_present"] == 5

    def test_an_independent_recomputation_agrees(self, conn):
        """The Phase 12 acceptance test, run against real rows: recompute every
        measured outcome straight from `daily_bars` and require equality."""
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="it-recompute", now=NOW))
        rows = run(conn.fetch(
            "SELECT * FROM public.research_scan_outcomes WHERE status='measured'"))
        assert rows
        for row in rows:
            entry = run(conn.fetchval(
                "SELECT close FROM public.daily_bars WHERE symbol=$1 AND "
                "trading_date=$2", row["symbol"], row["scan_session"]))
            exit_ = run(conn.fetchval(
                "SELECT close FROM public.daily_bars WHERE symbol=$1 AND "
                "trading_date=$2", row["symbol"], row["horizon_session"]))
            b_entry = run(conn.fetchval(
                "SELECT close FROM public.daily_bars WHERE symbol='SPY' AND "
                "trading_date=$1", row["scan_session"]))
            b_exit = run(conn.fetchval(
                "SELECT close FROM public.daily_bars WHERE symbol='SPY' AND "
                "trading_date=$1", row["horizon_session"]))
            expected_symbol = (float(exit_) - float(entry)) / float(entry) * 100.0
            expected_bench = (float(b_exit) - float(b_entry)) / float(b_entry) * 100.0
            assert float(row["symbol_return_pct"]) == pytest.approx(expected_symbol)
            assert float(row["benchmark_return_pct"]) == pytest.approx(expected_bench)
            assert float(row["excess_return_pct"]) == pytest.approx(
                expected_symbol - expected_bench)
            # And the horizon really is the Nth trading session, not the Nth bar.
            assert row["horizon_session"] == nth_trading_session_after(
                row["scan_session"], row["horizon_sessions"])

    def test_r11_a_second_run_creates_no_duplicate_and_changes_nothing(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="it-a", now=NOW))
        before = run(conn.fetch(
            "SELECT id, status, symbol_return_pct, measured_at, attempt_count "
            "FROM public.research_scan_outcomes ORDER BY id"))
        second = run(ro.run_outcome_maturation(conn, run_key="it-b", now=NOW))
        after = run(conn.fetch(
            "SELECT id, status, symbol_return_pct, measured_at, attempt_count "
            "FROM public.research_scan_outcomes ORDER BY id"))
        assert len(before) == len(after) == 10
        assert second["measured"] == 0          # nothing left to measure
        for a, b in zip(before, after):
            assert a["id"] == b["id"]
            assert a["status"] == b["status"]
            assert a["symbol_return_pct"] == b["symbol_return_pct"]
            assert a["measured_at"] == b["measured_at"]

    def test_r08_r10_a_late_bar_is_measured_exactly_once(self, conn):
        """No exit bar -> waiting. The bar arrives -> measured. A third pass
        changes nothing."""
        run(_add_scan(conn, "AAL"))
        run(_add_bar(conn, "AAL", SCAN_SESSION, 100.0))
        run(_seed_benchmark(conn, {SCAN_SESSION: 500.0, S1: 505.0}))

        first = run(ro.run_outcome_maturation(conn, run_key="late-1",
                                              now=datetime(2026, 8, 31, 21,
                                                           tzinfo=UTC)))
        assert first["measured"] == 0 and first["waiting_for_data"] == 1
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["status"] == ro.STATUS_WAITING_FOR_DATA
        assert row["status_reason"] == ro.REASON_MISSING_SYMBOL_EXIT
        assert row["attempt_count"] == 1

        run(_add_bar(conn, "AAL", S1, 110.0))
        second = run(ro.run_outcome_maturation(conn, run_key="late-2",
                                               now=datetime(2026, 8, 31, 21,
                                                            tzinfo=UTC)))
        assert second["measured"] == 1
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["status"] == ro.STATUS_MEASURED
        measured_at = row["measured_at"]
        assert float(row["symbol_return_pct"]) == pytest.approx(10.0)

        third = run(ro.run_outcome_maturation(conn, run_key="late-3",
                                              now=datetime(2026, 8, 31, 21,
                                                           tzinfo=UTC)))
        assert third["measured"] == 0
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["measured_at"] == measured_at

    def test_r09_a_missing_benchmark_bar_blocks_the_whole_observation(self, conn):
        """Even with a complete symbol series. O5 has no partial answer."""
        run(_add_scan(conn, "AAL"))
        run(_add_bar(conn, "AAL", SCAN_SESSION, 100.0))
        run(_add_bar(conn, "AAL", S1, 110.0))
        run(_seed_benchmark(conn, {SCAN_SESSION: 500.0}))
        summary = run(ro.run_outcome_maturation(
            conn, run_key="nobench", now=datetime(2026, 8, 31, 21, tzinfo=UTC)))
        assert summary["measured"] == 0
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["status"] == ro.STATUS_WAITING_FOR_DATA
        assert row["status_reason"] == ro.REASON_MISSING_BENCHMARK_EXIT
        assert row["symbol_return_pct"] is None

    def test_r14_two_scans_of_one_symbol_stay_independent(self, conn):
        run(_add_scan(conn, "AAL", session=SCAN_SESSION))
        run(_add_scan(conn, "AAL", session=S1))
        for day, close in [(SCAN_SESSION, 100.0), (S1, 110.0),
                           (date(2026, 9, 1), 121.0)]:
            run(_add_bar(conn, "AAL", day, close))
        run(_seed_benchmark(conn, {SCAN_SESSION: 500.0, S1: 500.0,
                                   date(2026, 9, 1): 500.0}))
        run(ro.run_outcome_maturation(conn, run_key="two-scans",
                                      now=datetime(2026, 9, 1, 21, tzinfo=UTC)))
        rows = {r["scan_session"]: dict(r) for r in run(conn.fetch(
            "SELECT * FROM public.research_scan_outcomes "
            "WHERE horizon_sessions=1 AND status='measured'"))}
        assert set(rows) == {SCAN_SESSION, S1}
        assert rows[SCAN_SESSION]["horizon_session"] == S1
        assert rows[S1]["horizon_session"] == date(2026, 9, 1)
        assert float(rows[SCAN_SESSION]["symbol_return_pct"]) == pytest.approx(10.0)
        assert float(rows[S1]["symbol_return_pct"]) == pytest.approx(10.0)
        assert rows[SCAN_SESSION]["scan_id"] != rows[S1]["scan_id"]

    def test_r16_a_future_bar_never_reaches_an_earlier_measurement(self, conn):
        """The store holds bars for sessions AFTER the pass's `as_of`. The 3D
        and 5D horizons must stay untouched, and the 1D number must be the 1D
        number."""
        _seed_measurable(conn)
        summary = run(ro.run_outcome_maturation(
            conn, run_key="nolookahead",
            now=datetime(2026, 8, 31, 21, tzinfo=UTC)))
        assert summary["as_of_session"] == S1.isoformat()
        assert summary["measured"] == 2          # the two 1D observations only
        rows = {(r["symbol"], r["horizon_label"]): dict(r) for r in run(conn.fetch(
            "SELECT * FROM public.research_scan_outcomes"))}
        assert float(rows[("AAL", "1D")]["symbol_return_pct"]) == pytest.approx(10.0)
        for label in ("3D", "5D", "10D", "20D"):
            assert rows[("AAL", label)]["status"] == ro.STATUS_NOT_YET_ELIGIBLE
            assert rows[("AAL", label)]["symbol_return_pct"] is None


# --------------------------------------------------------------------------- #
# 3. immutability, in the database
# --------------------------------------------------------------------------- #

class TestImmutability:

    def test_r17_the_database_refuses_to_rewrite_a_measured_outcome(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="frozen", now=NOW))
        oid = run(conn.fetchval(
            "SELECT id FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1 AND status='measured'"))
        for sql in (
            "UPDATE public.research_scan_outcomes SET symbol_return_pct=999 WHERE id=$1",
            "UPDATE public.research_scan_outcomes SET excess_return_pct=0 WHERE id=$1",
            "UPDATE public.research_scan_outcomes SET status='waiting_for_data',"
            " measured_at=NULL, entry_close=NULL, exit_close=NULL,"
            " benchmark_entry_close=NULL, benchmark_exit_close=NULL,"
            " symbol_return_pct=NULL, benchmark_return_pct=NULL,"
            " excess_return_pct=NULL, mfe_pct=NULL, mae_pct=NULL,"
            " bars_hash=NULL WHERE id=$1",
            "UPDATE public.research_scan_outcomes SET horizon_session=$2 WHERE id=$1",
        ):
            with pytest.raises(asyncpg.PostgresError):
                if "$2" in sql:
                    run(conn.execute(sql, oid, S5))
                else:
                    run(conn.execute(sql, oid))
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE id=$1", oid))
        assert float(row["symbol_return_pct"]) == pytest.approx(10.0)

    def test_r17_a_corrected_bar_is_noted_beside_the_frozen_number(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="rev-1", now=NOW))
        frozen = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1"))

        # The provider corrects the 2026-08-31 close.
        run(_add_bar(conn, "AAL", S1, 111.0, high=112.0, low=108.0))
        summary = run(ro.run_outcome_maturation(conn, run_key="rev-2", now=NOW,
                                                include_settled=True))
        assert summary["revisions_detected"] >= 1

        after = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE id=$1",
            frozen["id"]))
        # The label did not move.
        assert float(after["symbol_return_pct"]) == pytest.approx(10.0)
        assert after["bars_hash"] == frozen["bars_hash"]
        # And the disagreement is recorded.
        assert after["revision_detected"] is True
        notes = json.loads(after["revision_notes"]) \
            if isinstance(after["revision_notes"], str) else after["revision_notes"]
        assert notes and notes[0]["kind"] == "bars_revised"
        assert notes[0]["recomputed_symbol_return_pct"] == pytest.approx(11.0)

    def test_revision_notes_are_bounded(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="bounded-1", now=NOW))
        oid = run(conn.fetchval(
            "SELECT id FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1"))
        note = json.dumps([{"kind": "synthetic"}])
        for _ in range(ro.MAX_REVISION_NOTES + 5):
            run(conn.execute(ro.RECORD_REVISION_SQL, oid, note,
                             ro.MAX_REVISION_NOTES, datetime.now(UTC)))
        notes = run(conn.fetchval(
            "SELECT jsonb_array_length(revision_notes) FROM "
            "public.research_scan_outcomes WHERE id=$1", oid))
        assert notes == ro.MAX_REVISION_NOTES


# --------------------------------------------------------------------------- #
# 4. the durable job: dispatch, crash-reconcile, isolation
# --------------------------------------------------------------------------- #

class TestDurableJob:

    def test_dispatch_is_idempotent_and_lands_on_the_research_queue(self, conn):
        import app.jobs.research_outcomes as jro
        payload = jro.task_payload_from_template({}, run_key="rk-1")
        first = run(jro.enqueue_research_outcomes(conn, run_key="rk-1",
                                                  payload=payload))
        second = run(jro.enqueue_research_outcomes(conn, run_key="rk-1",
                                                   payload=payload))
        assert first["status"] == "queued"
        assert second["status"] == "already_queued"
        assert run(conn.fetchval(
            "SELECT count(*) FROM job_runs WHERE job_type=$1",
            jro.RESEARCH_OUTCOMES_JOB_TYPE)) == 1
        task = run(conn.fetchrow(
            "SELECT queue_name, task_type, max_attempts FROM job_tasks"))
        assert task["queue_name"] == "research_lifecycle"
        assert task["task_type"] == jro.RESEARCH_OUTCOMES_TASK
        assert task["max_attempts"] == jro.RESEARCH_OUTCOMES_MAX_ATTEMPTS

    def test_the_handler_is_the_same_program_as_the_service(self, conn):
        from app.jobs.handlers.research_outcomes_worker import (
            execute_research_outcomes)
        _seed_measurable(conn)
        result = run(execute_research_outcomes(
            conn, {"run_key": "handler-1", "scan_limit": 200,
                   "observation_limit": 400, "mode": "backfill"}))
        assert result["ok"] is True
        assert result["result"]["measured"] == 6
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_scan_outcomes "
            "WHERE status='measured'")) == 6

    def test_a_payload_without_a_run_key_is_terminal(self, conn):
        from app.jobs.handlers.research_outcomes_worker import (
            execute_research_outcomes)
        result = run(execute_research_outcomes(conn, {}))
        assert result["ok"] is False
        assert result["error"]["class"] == "terminal"
        assert result["error"]["code"] == "missing_run_key"

    def test_r22_a_completed_run_is_durable_output_and_is_not_repeated(self, conn):
        from app.jobs.handlers.research_outcomes_worker import (
            probe_research_outcomes_durable_output)
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="crash-1", now=NOW))
        probed = run(probe_research_outcomes_durable_output(
            conn, {"run_key": "crash-1"}))
        assert probed is not None
        assert probed["ok"] is True
        assert probed["result"]["measured"] == 6
        assert probed["result"]["reconciled_from_durable_output"] is True

    def test_r22_a_running_or_failed_run_is_not_durable_output(self, conn):
        from app.jobs.handlers.research_outcomes_worker import (
            probe_research_outcomes_durable_output)
        run(conn.execute(
            "INSERT INTO public.research_outcome_runs (run_key,"
            " contract_version, status, mode) VALUES ($1,$2,$3,'scheduled')",
            "still-going", ro.RESEARCH_OUTCOME_CONTRACT_VERSION, "running"))
        run(conn.execute(
            "INSERT INTO public.research_outcome_runs (run_key,"
            " contract_version, status, mode) VALUES ($1,$2,$3,'scheduled')",
            "it-broke", ro.RESEARCH_OUTCOME_CONTRACT_VERSION, "failed"))
        for key in ("still-going", "it-broke", "never-happened"):
            assert run(probe_research_outcomes_durable_output(
                conn, {"run_key": key})) is None

    def test_r22_a_failure_is_recorded_and_the_exception_still_escapes(self, conn):
        """The run row is written in a `finally`, so a worker that dies leaves
        the evidence — and a recorded failure is never durable output."""
        run(_add_scan(conn, "AAL"))
        original = ro.run_maturation

        async def boom(*a, **k):
            raise RuntimeError("simulated worker death")

        ro.run_maturation = boom
        try:
            with pytest.raises(RuntimeError):
                run(ro.run_outcome_maturation(conn, run_key="boom", now=NOW))
        finally:
            ro.run_maturation = original
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_outcome_runs WHERE run_key='boom'"))
        assert row["status"] == "failed"
        assert row["failure_summary"] == "RuntimeError"
        assert row["completed_at"] is not None

    def test_r24_an_outcome_failure_cannot_touch_the_lifecycle(self, conn):
        """The isolation claim, stated as an experiment: a lifecycle run row
        exists; every outcome attempt fails; the lifecycle row is unchanged and
        no lifecycle task was created or altered."""
        run(conn.execute(
            "INSERT INTO public.research_lifecycle_runs (run_key,"
            " contract_version, status, target_session, research_candidates) "
            "VALUES ('lifecycle-untouched','smart_scanner_research_lifecycle.v1',"
            " 'completed', $1, 4)", SCAN_SESSION))
        before = run(conn.fetchrow(
            "SELECT * FROM public.research_lifecycle_runs "
            "WHERE run_key='lifecycle-untouched'"))

        from app.jobs.handlers.research_outcomes_worker import (
            execute_research_outcomes)
        original = ro.run_maturation

        async def boom(*a, **k):
            raise RuntimeError("simulated outcome failure")

        ro.run_maturation = boom
        try:
            for i in range(3):
                result = run(execute_research_outcomes(
                    conn, {"run_key": f"fail-{i}"}))
                assert result["ok"] is False
                # RETRYABLE, not terminal: transport faults resolve, and the
                # queue owns the attempt budget.
                assert result["error"]["class"] == "retryable"
        finally:
            ro.run_maturation = original

        after = run(conn.fetchrow(
            "SELECT * FROM public.research_lifecycle_runs "
            "WHERE run_key='lifecycle-untouched'"))
        assert dict(before) == dict(after)
        assert run(conn.fetchval(
            "SELECT count(*) FROM job_tasks WHERE task_type LIKE "
            "'smart_scanner_research_lifecycle%'")) == 0


# --------------------------------------------------------------------------- #
# 5. the dry run, the bounds, and the readable status
# --------------------------------------------------------------------------- #

class TestDryRunAndStatus:

    def test_a_dry_run_reports_and_writes_nothing(self, conn):
        _seed_measurable(conn)
        report = run(ro.run_outcome_maturation(conn, run_key="dry", now=NOW,
                                               dry_run=True))
        assert report["status"] == ro.STATUS_DRY_RUN
        assert report["observations_would_plan"] == 10
        assert report["eligible"] == 6
        assert report["not_yet_eligible"] == 4
        assert report["measurable"] == 6
        assert report["by_horizon"]["1D"]["measurable"] == 2
        assert report["by_horizon"]["20D"]["not_yet_eligible"] == 2
        # Nothing at all was written — not a plan row, not a run row.
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_scan_outcomes")) == 0
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_outcome_runs")) == 0

    def test_a_dry_run_predicts_the_real_run(self, conn):
        _seed_measurable(conn)
        dry = run(ro.run_outcome_maturation(conn, run_key="dry2", now=NOW,
                                            dry_run=True))
        real = run(ro.run_outcome_maturation(conn, run_key="real2", now=NOW))
        assert dry["measurable"] == real["measured"]
        assert dry["eligible"] == real["observations_due"]

    def test_a_dry_run_counts_missing_data_honestly(self, conn):
        run(_add_scan(conn, "AAL"))
        run(_add_bar(conn, "AAL", SCAN_SESSION, 100.0))
        run(_seed_benchmark(conn, {SCAN_SESSION: 500.0, S1: 505.0, S3: 510.0,
                                   S5: 515.0}))
        report = run(ro.run_outcome_maturation(conn, run_key="dry3", now=NOW,
                                               dry_run=True))
        assert report["eligible"] == 3
        assert report["measurable"] == 0
        assert report["missing_data"] == 3
        assert report["missing_data_reasons"][ro.REASON_MISSING_SYMBOL_EXIT] == 3

    def test_r23_a_bounded_run_stops_and_says_so(self, conn):
        for i in range(4):
            run(_add_scan(conn, f"SYM{i}"))
            run(_add_bar(conn, f"SYM{i}", SCAN_SESSION, 100.0))
            run(_add_bar(conn, f"SYM{i}", S1, 110.0))
        run(_seed_benchmark(conn, {SCAN_SESSION: 500.0, S1: 505.0}))
        summary = run(ro.run_outcome_maturation(
            conn, run_key="bounded", now=datetime(2026, 8, 31, 21, tzinfo=UTC),
            scan_limit=2, observation_limit=2))
        assert summary["scans_considered"] == 2
        assert summary["observations_planned"] == 10
        assert summary["observations_due"] == 2
        assert summary["truncated_by_limit"] is True
        # The rest is still there to do, and a later run does it.
        second = run(ro.run_outcome_maturation(
            conn, run_key="bounded-2", now=datetime(2026, 8, 31, 21, tzinfo=UTC),
            scan_limit=200, observation_limit=400))
        assert second["measured"] == 2
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_scan_outcomes")) == 20

    def test_o13_every_scan_horizon_has_a_queryable_status(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="status", now=NOW))
        report = run(ro.outcome_status(conn))
        assert report["observations"] == 10
        assert report["by_status"][ro.STATUS_MEASURED] == 6
        assert report["by_status"][ro.STATUS_NOT_YET_ELIGIBLE] == 4
        assert report["by_status"][ro.STATUS_WAITING_FOR_DATA] == 0
        # Both classifications are represented — the control-like half is there.
        classifications = {c["classification"] for c in report["cells"]}
        assert classifications == {rf.LIFECYCLE_RESEARCH_CANDIDATE,
                                   rf.LIFECYCLE_SCANNED_NOT_CANDIDATE}
        # Aggregates appear only where there is something to aggregate.
        for cell in report["cells"]:
            if cell["status"] != ro.STATUS_MEASURED:
                assert "mean_excess_return_pct" not in cell

    def test_o2_a_rescan_cannot_rewrite_an_earlier_outcome_s_meaning(self, conn):
        """The scan row is UPSERTed in place by `research_scan.py`. The outcome
        keeps a SNAPSHOT, so a symbol re-scanned into a different verdict does
        not retroactively change what an already-measured horizon was about."""
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="snap-1", now=NOW))
        before = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1"))
        assert before["scan_classification"] == rf.LIFECYCLE_RESEARCH_CANDIDATE

        # The same (symbol, scan_session) is re-scanned and now hard-rejected.
        run(conn.execute(
            "UPDATE public.research_scan_results SET rejection_reason="
            "'price_below_minimum', setup_state='invalid', "
            "structure_state='unknown', benchmark_relative='underperforming', "
            "scanned_at=NOW() WHERE symbol='AAL' AND scan_session=$1",
            SCAN_SESSION))
        run(ro.run_outcome_maturation(conn, run_key="snap-2", now=NOW,
                                      include_settled=True))

        after = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1"))
        assert after["scan_classification"] == rf.LIFECYCLE_RESEARCH_CANDIDATE
        assert after["scan_setup_state"] == "valid"
        assert float(after["symbol_return_pct"]) == pytest.approx(10.0)


# --------------------------------------------------------------------------- #
# 6. acceptance audit (2026-09-06) — the repairs, against real Postgres
# --------------------------------------------------------------------------- #

class TestFreezeGuardsTheAttribution:
    """Migration 032. The audit found ten attribution columns outside the
    guarded set: a measured row's benchmark, strategy, verdict, evidence and
    revision-tell could all be edited while its numbers stayed put."""

    def _measured_id(self, conn):
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="freeze32", now=NOW))
        return run(conn.fetchval(
            "SELECT id FROM public.research_scan_outcomes "
            "WHERE symbol='AAL' AND horizon_sessions=1 AND status='measured'"))

    @pytest.mark.parametrize("column,value", [
        ("benchmark_symbol", "QQQ"),
        ("strategy_code", "something_else"),
        ("strategy_version", "v9"),
        ("config_hash", "a-different-config"),
        ("scan_classification", "scanned_not_candidate"),
        ("scan_verdict", "AVOID"),
        ("scan_structure_state", "unknown"),
        ("scan_setup_state", "invalid"),
        ("scan_reason_code", "rewritten"),
        ("scan_rejection_reason", "price_below_minimum"),
        ("scan_benchmark_relative", "underperforming"),
        ("contract_version", "research_scan_outcome.v2"),
        ("calculation_version", "outcome.v2"),
        ("market_calendar_version", "some_other_calendar.v1"),
    ])
    def test_no_meaning_column_can_be_rewritten_on_a_measured_row(
            self, conn, column, value):
        oid = self._measured_id(conn)
        with pytest.raises(asyncpg.PostgresError):
            run(conn.execute(
                f"UPDATE public.research_scan_outcomes SET {column}=$2 "
                "WHERE id=$1", oid, value))

    def test_the_revision_tell_itself_cannot_be_edited(self, conn):
        """`scan_scanned_at` is what detects a re-scan. A detector that can be
        edited detects nothing."""
        oid = self._measured_id(conn)
        with pytest.raises(asyncpg.PostgresError):
            run(conn.execute(
                "UPDATE public.research_scan_outcomes SET scan_scanned_at=NOW() "
                "WHERE id=$1", oid))

    def test_recording_that_we_looked_again_is_still_permitted(self, conn):
        oid = self._measured_id(conn)
        run(conn.execute(
            "UPDATE public.research_scan_outcomes SET revision_detected=TRUE,"
            " attempt_count=attempt_count+1, last_attempt_at=NOW() WHERE id=$1",
            oid))
        row = run(conn.fetchrow(
            "SELECT revision_detected, attempt_count, symbol_return_pct "
            "FROM public.research_scan_outcomes WHERE id=$1", oid))
        assert row["revision_detected"] is True
        assert float(row["symbol_return_pct"]) == pytest.approx(10.0)

    def test_a_pending_row_is_still_freely_updatable(self, conn):
        """The widening must not freeze rows that have not been measured."""
        _seed_measurable(conn)
        run(ro.run_outcome_maturation(conn, run_key="freeze32-pending", now=NOW))
        oid = run(conn.fetchval(
            "SELECT id FROM public.research_scan_outcomes "
            "WHERE status='not_yet_eligible' LIMIT 1"))
        run(conn.execute(
            "UPDATE public.research_scan_outcomes SET scan_verdict='AVOID',"
            " benchmark_symbol='QQQ' WHERE id=$1", oid))
        assert run(conn.fetchval(
            "SELECT scan_verdict FROM public.research_scan_outcomes WHERE id=$1",
            oid)) == "AVOID"


class TestPlanningAtomicity:
    """A partial plan must never let two scan revisions share one scan."""

    def test_a_failed_plan_leaves_no_partial_row(self, conn):
        """The transaction is proven by making the FIFTH insert fail: if the
        five inserts were independent statements, four rows would survive.

        The failure is injected through a thin PROXY rather than by patching
        the connection — asyncpg's `Connection.fetchrow` is a read-only
        attribute, and a proxy also keeps the real transaction semantics the
        test is actually about.
        """
        run(_add_scan(conn, "AAL"))

        class FailingFifthInsert:
            """Forwards everything; fails the fifth planning INSERT."""

            def __init__(self, inner):
                self._inner = inner
                self._inserts = 0

            def __getattr__(self, name):
                return getattr(self._inner, name)

            async def fetchrow(self, sql, *args):
                if sql is ro.INSERT_PLAN_SQL:
                    self._inserts += 1
                    if self._inserts == 5:
                        raise RuntimeError("simulated worker death mid-plan")
                return await self._inner.fetchrow(sql, *args)

        proxy = FailingFifthInsert(conn)
        with pytest.raises(RuntimeError):
            run(ro.plan_missing_observations(proxy))

        # All five, or none. Never four.
        assert run(conn.fetchval(
            "SELECT count(*) FROM public.research_scan_outcomes")) == 0

    def test_a_replan_after_a_rollback_writes_one_consistent_revision(self, conn):
        """And the scan really can change underneath: the re-plan must produce
        five rows that agree, not a mixture."""
        run(_add_scan(conn, "AAL", candidate=True))
        run(conn.execute(
            "UPDATE public.research_scan_results SET rejection_reason="
            "'price_below_minimum', setup_state='invalid',"
            " structure_state='unknown', benchmark_relative='underperforming'"
            " WHERE symbol='AAL'"))
        run(ro.plan_missing_observations(conn))
        rows = run(conn.fetch(
            "SELECT scan_classification, scan_setup_state, config_hash "
            "FROM public.research_scan_outcomes WHERE symbol='AAL'"))
        assert len(rows) == 5
        assert len({(r["scan_classification"], r["scan_setup_state"],
                     r["config_hash"]) for r in rows}) == 1
        assert rows[0]["scan_classification"] == rf.LIFECYCLE_SCANNED_NOT_CANDIDATE


class TestTerminalIsRecoverable:
    """CONCERN B — a `failed_terminal` row is not a dead end if data arrives."""

    def test_a_terminal_observation_can_still_be_measured_on_a_recheck(self, conn):
        scan_session = date(2026, 1, 5)
        run(_add_symbol(conn, "AAL"))
        run(conn.execute(
            "INSERT INTO public.research_scan_results (symbol, scan_session,"
            " scanned_at, contract_version, strategy_code, strategy_version,"
            " config_hash, verdict, structure_state, setup_state,"
            " benchmark_relative, benchmark_symbol) "
            "VALUES ('AAL',$1,NOW(),'research_scan.v1','wyckoff_mtf','v2','c',"
            " 'WATCH','recognized','valid','outperforming','SPY')", scan_session))
        # Far enough past the horizon that the 60-session grace is exhausted.
        far = ro.horizon_session_for(scan_session, 1)
        for _ in range(ro.MISSING_DATA_GRACE_SESSIONS + 2):
            far = ro.horizon_session_for(far, 1)
        late = datetime.combine(far, datetime.min.time(),
                                tzinfo=UTC) + timedelta(hours=21)
        run(ro.run_outcome_maturation(conn, run_key="grace-1", now=late))
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["status"] == ro.STATUS_FAILED_TERMINAL
        assert row["status_reason"] == ro.REASON_GRACE_EXCEEDED

        # The bars finally arrive. An operator recheck measures it.
        entry, exit_ = scan_session, ro.horizon_session_for(scan_session, 1)
        run(_add_bar(conn, "AAL", entry, 100.0))
        run(_add_bar(conn, "AAL", exit_, 110.0))
        run(_seed_benchmark(conn, {entry: 500.0, exit_: 505.0}))
        run(ro.run_outcome_maturation(conn, run_key="grace-2", now=late,
                                      include_settled=True))
        row = run(conn.fetchrow(
            "SELECT * FROM public.research_scan_outcomes WHERE horizon_sessions=1"))
        assert row["status"] == ro.STATUS_MEASURED
        assert float(row["symbol_return_pct"]) == pytest.approx(10.0)
