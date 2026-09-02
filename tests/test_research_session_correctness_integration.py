"""Real-Postgres validation of the session-correctness work, and the
multi-session fairness proof.

WHY THIS FILE EXISTS
--------------------
Checkpoint 1's 39 regression tests are pure Python against fakes. Three things
in that change are SQL and cannot be proven that way:

  * migration 030's widened CHECK actually accepts `scan_stale`;
  * `FUNNEL_ROW_SQL`'s LEFT JOIN on `(symbol, scan_session)` returns what
    `lifecycle_state` expects;
  * `WARMUP_SELECT_SQL`'s new `$2::date` freshness-top-up branch selects the
    rows it claims to.

And one thing is a SCHEDULING question that a hand-written approximation could
too easily answer the way its author expected: whether stale-ready freshness
work can starve cold history warmup forever. That is simulated here across
successive target sessions using the real `select_warmup_batch` against real
rows, so the answer comes from the production query and the production
ordering rather than from a re-implementation of them.

Uses the same docker-postgres harness as the other *_integration tests.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from datetime import date, datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.research_funnel as rf
import app.research_ingest as ri
import app.research_universe as ru

PG_IMAGE = "postgres:16-alpine"
DBNAME = "rscdb"
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
              "030_research_session_correctness"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UTC = timezone.utc
S = date(2026, 8, 31)


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(),
                                reason="docker/pg image unavailable")


def _sh(a, inp=None, t=180):
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
        for m in MIGRATIONS:
            r = _psql(cid, None,
                      path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-500:]}"
        yield {"cid": cid,
               "dsn": f"postgresql://postgres:postgres@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "kill", cid])


async def _reset(conn):
    await conn.execute("TRUNCATE public.research_lifecycle_run_symbols, "
                       "public.research_scan_results, public.research_symbols, "
                       "public.daily_bars RESTART IDENTITY CASCADE")


async def _add_symbol(conn, symbol, *, state, bars=0, latest=None,
                      admission="eligible_for_history", attempts=0,
                      cooldown=None, rank=100, scanned_at=None,
                      observed=None, last_attempt=None):
    await conn.execute(
        "INSERT INTO public.research_symbols "
        "(symbol, discovery_source, discovery_reasons, "
        " discovery_observation_count, first_observed_at, latest_observed_at, "
        " first_reference_session, latest_reference_session, "
        " first_actionable_session, best_rank, state, "
        " history_daily_bars, history_latest_session, admission_state, "
        " warmup_attempts, warmup_cooldown_until, research_scanned_at, "
        " warmup_last_attempt_at, licensing_visibility) "
        "VALUES ($1,'fmp',ARRAY['most_active'],1,$11,$11,$2,$2,$2,$3,$4,$5,$6,"
        "        $7,$8,$9,$10,$12,'internal_research_only')",
        symbol, S, rank, state, bars, latest, admission, attempts, cooldown,
        scanned_at, observed or datetime(2026, 8, 1, tzinfo=UTC), last_attempt)


# =========================================================================== #
# migration 030 + the two new statements, against a real schema
# =========================================================================== #

class TestSchemaAndStatements:

    def test_migration_030_accepts_scan_stale_and_keeps_the_old_vocabulary(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                run_id = await conn.fetchval(
                    "INSERT INTO public.research_lifecycle_runs "
                    "(run_key, contract_version, status) "
                    "VALUES ('rlc:it:1','v1','completed') RETURNING id")
                # the NEW value must be accepted
                await conn.execute(
                    "INSERT INTO public.research_lifecycle_run_symbols "
                    "(run_id, symbol, lifecycle_state, admission_tier) "
                    "VALUES ($1,'STALE','scan_stale','eligible_for_history')",
                    run_id)
                # and every pre-existing one must still be
                for st in rf.LIFECYCLE_STATES:
                    await conn.execute(
                        "INSERT INTO public.research_lifecycle_run_symbols "
                        "(run_id, symbol, lifecycle_state, admission_tier) "
                        "VALUES ($1,$2,$3,'eligible_for_history')",
                        run_id, f"SYM_{st}", st)
                n = await conn.fetchval(
                    "SELECT count(*) FROM public.research_lifecycle_run_symbols "
                    "WHERE run_id=$1", run_id)
                assert n == len(rf.LIFECYCLE_STATES) + 1
                # and an invented one must still be refused
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "INSERT INTO public.research_lifecycle_run_symbols "
                        "(run_id, symbol, lifecycle_state, admission_tier) "
                        "VALUES ($1,'NOPE','not_a_state','eligible_for_history')",
                        run_id)
            finally:
                await conn.close()
        asyncio.run(go())

    def test_funnel_row_sql_joins_the_session_and_drives_the_partition(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, "CUR", state=ru.STATE_RESEARCH_SCANNED,
                                  bars=500, latest=S, scanned_at=datetime.now(UTC))
                await _add_symbol(conn, "OLD", state=ru.STATE_RESEARCH_SCANNED,
                                  bars=500, latest=S, scanned_at=datetime.now(UTC))
                await _add_symbol(conn, "FRESHREADY", state=ru.STATE_RESEARCH_READY,
                                  bars=500, latest=S)
                # CUR scanned FOR S with affirmative evidence; OLD only for S-3.
                for sym, sess in (("CUR", S), ("OLD", S - timedelta(days=3))):
                    await conn.execute(
                        "INSERT INTO public.research_scan_results "
                        "(symbol, scan_session, scanned_at, contract_version, "
                        " strategy_code, strategy_version, config_hash, "
                        " bars_evaluated, verdict, structure_state, setup_state, "
                        " benchmark_relative, licensing_visibility) "
                        "VALUES ($1,$2,NOW(),'v1','wyckoff_mtf_v2','2','h',500,"
                        "        'WATCH','recognized','valid','outperforming',"
                        "        'internal_research_only')", sym, sess)

                rows = [dict(r) for r in await conn.fetch(rf.FUNNEL_ROW_SQL, S)]
                by = {r["symbol"]: rf.lifecycle_state(r) for r in rows}
                assert by["CUR"] == rf.LIFECYCLE_RESEARCH_CANDIDATE
                assert by["OLD"] == rf.LIFECYCLE_SCAN_STALE
                assert by["FRESHREADY"] == rf.LIFECYCLE_SCAN_PENDING

                summary = rf.summarise(rows, provider_calls_used=0,
                                       provider_calls_avoided=0)
                assert summary["research_candidates"] == 1
                assert summary["scanned"] == 1
                assert summary["conservation"]["ok"] is True
            finally:
                await conn.close()
        asyncio.run(go())

    def test_warmup_select_sql_freshness_branch_runs_on_real_postgres(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, "STALE", state=ru.STATE_RESEARCH_SCANNED,
                                  bars=500, latest=S - timedelta(days=3))
                await _add_symbol(conn, "CURRENT", state=ru.STATE_RESEARCH_SCANNED,
                                  bars=500, latest=S)
                await _add_symbol(conn, "COLD", state=ru.STATE_HISTORY_REQUIRED)

                batch = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 1, tzinfo=UTC))
                picked = {r["symbol"] for r in batch}
                assert "STALE" in picked      # lagging -> top-up
                assert "COLD" in picked       # never had history
                assert "CURRENT" not in picked  # already at S: nothing to do

                # and with no session, the freshness branch is inert
                none_batch = await ri.select_warmup_batch(
                    conn, limit=5, target_session=None,
                    now=datetime(2026, 9, 1, tzinfo=UTC))
                assert {r["symbol"] for r in none_batch} == {"COLD"}
            finally:
                await conn.close()
        asyncio.run(go())

    def test_a_cooled_maturing_symbol_is_skipped_without_consuming_a_slot(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                far = datetime(2027, 1, 1, tzinfo=UTC)
                await _add_symbol(conn, "MATURING", state=ru.STATE_HISTORY_WARMING,
                                  bars=452, latest=S, attempts=2, cooldown=far)
                for i in range(3):
                    await _add_symbol(conn, f"COLD{i}", state=ru.STATE_HISTORY_REQUIRED)
                batch = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 1, tzinfo=UTC))
                names = {r["symbol"] for r in batch}
                assert "MATURING" not in names
                assert names == {"COLD0", "COLD1", "COLD2"}
            finally:
                await conn.close()
        asyncio.run(go())


# =========================================================================== #
# F1-F8 — multi-session fairness, driven by the production selector
# =========================================================================== #

async def _simulate(conn, *, sessions, warm_limit=5, start=S):
    """Run `sessions` successive lifecycles through the REAL selector.

    Each session advances the target date by one day, which is what makes every
    already-current symbol stale again — the exact condition that could let
    freshness maintenance monopolise the batch forever. A selected symbol is
    then warmed the way a successful warm would leave it: bars current to the
    target session.
    """
    log = []
    for i in range(sessions):
        target = start + timedelta(days=i)
        now = datetime(target.year, target.month, target.day,
                       12, 0, tzinfo=UTC) + timedelta(days=1)
        stale_backlog = await conn.fetchval(
            "SELECT count(*) FROM public.research_symbols "
            "WHERE state IN ('research_ready','research_scanned') "
            "  AND (history_latest_session IS NULL OR history_latest_session < $1)",
            target)
        cold_backlog = await conn.fetchval(
            "SELECT count(*) FROM public.research_symbols "
            "WHERE state IN ('discovered','history_required','history_warming')")
        batch = await ri.select_warmup_batch(conn, limit=warm_limit,
                                             target_session=target, now=now)
        picked = [r["symbol"] for r in batch]
        for r in batch:
            # a successful warm brings the symbol current, and a cold symbol
            # that now holds history becomes ready
            await conn.execute(
                "UPDATE public.research_symbols "
                "SET history_latest_session=$2, history_daily_bars=500, "
                "    warmup_last_attempt_at=$3, "
                "    state=CASE WHEN state IN ('discovered','history_required') "
                "               THEN 'research_ready' ELSE state END, "
                "    updated_at=NOW() WHERE symbol=$1",
                r["symbol"], target, now)
        log.append({"session": target, "stale_backlog": stale_backlog,
                    "cold_backlog": cold_backlog, "picked": picked})
    return log


class TestFairness:

    def test_f1_steady_state_does_not_starve_cold_symbols(self, pg):
        """The reproduction the review asked for: 5 stale-ready + 6 cold, and
        every new session re-stales all five."""
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(5):
                    await _add_symbol(conn, f"READY{i}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                for i in range(6):
                    await _add_symbol(conn, f"COLD{i}",
                                      state=ru.STATE_HISTORY_REQUIRED)
                log = await _simulate(conn, sessions=6)
                served_cold = {s for e in log for s in e["picked"]
                               if s.startswith("COLD")}
                served_ready = {s for e in log for s in e["picked"]
                                if s.startswith("READY")}
                assert len(served_cold) == 6, (
                    f"cold symbols starved: only {sorted(served_cold)} served\n"
                    + "\n".join(str(e) for e in log))
                assert served_ready, "freshness work must also progress"
                remaining_cold = await conn.fetchval(
                    "SELECT count(*) FROM public.research_symbols "
                    "WHERE state IN ('discovered','history_required')")
                assert remaining_cold == 0
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f2_ready_population_larger_than_warm_limit_still_serves_cold(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(12):
                    await _add_symbol(conn, f"R{i:02d}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                for i in range(6):
                    await _add_symbol(conn, f"C{i:02d}",
                                      state=ru.STATE_HISTORY_REQUIRED)
                log = await _simulate(conn, sessions=10)
                served_cold = {s for e in log for s in e["picked"]
                               if s.startswith("C")}
                served_ready = {s for e in log for s in e["picked"]
                                if s.startswith("R")}
                assert len(served_cold) == 6, (
                    f"starved: {sorted(served_cold)}\n"
                    + "\n".join(str(e) for e in log))
                # no ready symbol may be permanently skipped by stable ordering
                assert len(served_ready) == 12, sorted(served_ready)
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f3_cold_only_backlog_uses_all_capacity(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(8):
                    await _add_symbol(conn, f"C{i}", state=ru.STATE_HISTORY_REQUIRED)
                log = await _simulate(conn, sessions=2)
                # Five in the first run, and by the end of the second every
                # cold symbol has been served. The second batch is still full
                # because the five warmed in run 1 are stale again by run 2 —
                # correct behaviour, and exactly what the fair queue is for:
                # the three never-served symbols still come first.
                assert len(log[0]["picked"]) == 5
                assert {f"C{i}" for i in range(8)} <= {
                    s for e in log for s in e["picked"]}
                assert all(len(e["picked"]) <= 5 for e in log)
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f4_freshness_only_backlog_uses_all_capacity(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(7):
                    await _add_symbol(conn, f"R{i}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                log = await _simulate(conn, sessions=1)
                assert len(log[0]["picked"]) == 5
                assert all(p.startswith("R") for p in log[0]["picked"])
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f5_a_matured_symbol_competes_once_its_recheck_arrives(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                recheck = datetime(2026, 9, 3, tzinfo=UTC)
                await _add_symbol(conn, "MATURING", state=ru.STATE_HISTORY_WARMING,
                                  bars=499, latest=S, attempts=2, cooldown=recheck)
                before = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 1, tzinfo=UTC))
                assert "MATURING" not in {r["symbol"] for r in before}
                after = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 4, tzinfo=UTC))
                assert "MATURING" in {r["symbol"] for r in after}
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f6_a_retryable_failure_in_one_class_does_not_block_the_other(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                cooled = datetime(2026, 9, 1, 13, 0, tzinfo=UTC)
                await _add_symbol(conn, "FAILING", state=ru.STATE_HISTORY_WARMING,
                                  bars=300, latest=S, attempts=1, cooldown=cooled)
                for i in range(3):
                    await _add_symbol(conn, f"R{i}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                batch = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 1, 12, 0, tzinfo=UTC))
                names = {r["symbol"] for r in batch}
                assert "FAILING" not in names          # parked, not blocking
                assert {"R0", "R1", "R2"} <= names     # others proceed
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f7_more_refreshable_symbols_than_scan_capacity_still_converges(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(9):
                    await _add_symbol(conn, f"R{i}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                log = await _simulate(conn, sessions=4)
                served = {s for e in log for s in e["picked"]}
                assert len(served) == 9, sorted(served)
                for e in log:
                    assert len(e["picked"]) <= 5
            finally:
                await conn.close()
        asyncio.run(go())

    def test_f8_batch_never_exceeds_the_bounded_limit(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                for i in range(10):
                    await _add_symbol(conn, f"R{i}",
                                      state=ru.STATE_RESEARCH_SCANNED, bars=500,
                                      latest=S - timedelta(days=1),
                                      scanned_at=datetime.now(UTC),
                                      last_attempt=datetime(2026, 8, 30,
                                                            tzinfo=UTC))
                for i in range(10):
                    await _add_symbol(conn, f"C{i}", state=ru.STATE_HISTORY_REQUIRED)
                log = await _simulate(conn, sessions=5)
                for e in log:
                    assert len(e["picked"]) <= ru.MAX_WARMUP_SYMBOLS_PER_RUN
                    assert len(set(e["picked"])) == len(e["picked"])
            finally:
                await conn.close()
        asyncio.run(go())


# =========================================================================== #
# S14 / BLOCKER 1 — the maturity write, against the REAL CHECK constraints
#
# This is the coverage gap that broke the 2026-09-02 staging validation. The
# unit fakes record SQL without enforcing constraints, and the tests above
# exercise `select_warmup_batch` (the READ path) but never `warm_symbol`'s
# UPDATE. So `warmup_last_error_class = 'maturing'` — a value
# `research_symbols_error_class_ck` does not admit — reached staging and
# crashed the lifecycle the first time a warmed symbol came back still
# immature (VISN, 159 bars).
# =========================================================================== #

class _Provider:
    """Minimal provider stub. `get_daily_bars` returns a LIST of already
    canonical bar dicts — the shape the real provider returns and the one
    `normalize_daily_bars` consumes. Getting this wrong makes the test prove
    the error path instead of the success path."""
    name = "stub"

    def __init__(self, bars):
        self._bars = bars

    async def get_daily_bars(self, symbol, frm, to):
        return [dict(b, symbol=symbol) for b in self._bars]


def _hist(n, end=date(2026, 9, 1)):
    """`n` canonical daily bars ending at `end`."""
    return [{"trading_date": end - timedelta(days=i), "open": 10.0,
             "high": 11.0, "low": 9.5, "close": 10.5, "volume": 1_000_000.0}
            for i in range(n)]


class TestMaturityWriteAgainstRealConstraints:

    def _warm(self, pg, symbol, *, bars_returned, state=ru.STATE_HISTORY_REQUIRED,
              attempts=0, now=None):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, symbol, state=state, attempts=attempts)
                res = await ri.warm_symbol(
                    conn, _Provider(_hist(bars_returned)), symbol,
                    now=now or datetime(2026, 9, 2, 13, 0, tzinfo=UTC))
                row = dict(await conn.fetchrow(
                    "SELECT state, warmup_attempts, warmup_last_error_code,"
                    " warmup_last_error_class, warmup_cooldown_until"
                    " FROM public.research_symbols WHERE symbol=$1", symbol))
                counts = (await ri.bar_counts(conn, [symbol])).get(symbol, {})
                return res, row, counts
            finally:
                await conn.close()
        return asyncio.run(go())

    def test_s14_maturity_parking_survives_the_real_check_constraint(self, pg):
        """The exact staging failure: a symbol too young for readiness, parked
        for calendar maturity. The UPDATE must succeed."""
        res, row, counts = self._warm(pg, "YOUNG", bars_returned=200)  # ~7 months
        # 1. it did not raise, and the row persisted
        assert res["error_class"] is None
        # 2. class is NULL — waiting is not an error class
        assert row["warmup_last_error_class"] is None
        # 3. the descriptive code still explains the wait
        assert row["warmup_last_error_code"] == "awaiting_history_maturity"
        # 4. the cooldown carries future eligibility, on a month boundary
        assert row["warmup_cooldown_until"] is not None
        assert row["warmup_cooldown_until"].day == 1
        # 5. calendar waiting consumed no attempt budget
        assert row["warmup_attempts"] == 0
        # 6. and it lands in a state warmup can still reach
        state = ru.classify_history_state(
            daily_bars=counts["bars"], month_groups=counts["month_groups"],
            week_groups=counts["week_groups"], symbol="YOUNG",
            attempts=row["warmup_attempts"],
            last_error_class=row["warmup_last_error_class"],
            last_error_code=row["warmup_last_error_code"])
        assert state == ru.STATE_HISTORY_WARMING
        assert state not in ru.TERMINAL_STATES

    def test_s14_parked_symbol_is_not_reselected_before_its_maturity_date(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, "YOUNG", state=ru.STATE_HISTORY_REQUIRED)
                await ri.warm_symbol(conn, _Provider(_hist(200)), "YOUNG",
                                     now=datetime(2026, 9, 2, 13, 0, tzinfo=UTC))
                recheck = await conn.fetchval(
                    "SELECT warmup_cooldown_until FROM public.research_symbols"
                    " WHERE symbol=$1", "YOUNG")
                before = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=datetime(2026, 9, 3, tzinfo=UTC))
                after = await ri.select_warmup_batch(
                    conn, limit=5, target_session=S,
                    now=recheck + timedelta(days=1))
                return {r["symbol"] for r in before}, {r["symbol"] for r in after}
            finally:
                await conn.close()
        before, after = asyncio.run(go())
        assert "YOUNG" not in before, "parked symbol must not be re-warmed early"
        assert "YOUNG" in after, "and must become eligible once matured"

    def test_s14_a_symbol_the_provider_barely_carries_stays_terminal(self, pg):
        """Below the usable floor is a genuine terminal answer, and `terminal`
        is in the constrained vocabulary."""
        res, row, _ = self._warm(pg, "THIN", bars_returned=20)
        assert row["warmup_last_error_class"] == "terminal"
        assert row["warmup_last_error_code"] == "insufficient_provider_history"
        assert row["warmup_cooldown_until"] is None

    def test_s14_a_fully_matured_symbol_records_no_error_at_all(self, pg):
        res, row, counts = self._warm(pg, "GROWN", bars_returned=900)
        assert row["warmup_last_error_class"] is None
        assert row["warmup_last_error_code"] is None
        assert row["warmup_cooldown_until"] is None
        assert row["warmup_attempts"] == 1      # a real attempt WAS spent
        state = ru.classify_history_state(
            daily_bars=counts["bars"], month_groups=counts["month_groups"],
            week_groups=counts["week_groups"], symbol="GROWN",
            attempts=row["warmup_attempts"],
            last_error_class=None, last_error_code=None)
        assert state == ru.STATE_RESEARCH_READY

    def test_s14_the_constrained_vocabulary_still_holds(self, pg):
        """Every class the code can persist must satisfy the CHECK, and an
        invented one must still be refused."""
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, "V", state=ru.STATE_HISTORY_REQUIRED)
                for klass in ("retryable", "terminal", "operator_error", None):
                    await conn.execute(
                        "UPDATE public.research_symbols SET"
                        " warmup_last_error_class=$2 WHERE symbol=$1", "V", klass)
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "UPDATE public.research_symbols SET"
                        " warmup_last_error_class='maturing' WHERE symbol=$1", "V")
            finally:
                await conn.close()
        asyncio.run(go())

    def test_s14_a_provider_failure_is_still_classified_normally(self, pg):
        """A real provider error must keep its retryable/terminal class."""
        class Boom:
            name = "stub"
            async def get_daily_history(self, *a, **k):
                raise TimeoutError("provider timeout")
            async def get_daily_bars(self, *a, **k):
                raise TimeoutError("provider timeout")
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await _reset(conn)
                await _add_symbol(conn, "FAILS", state=ru.STATE_HISTORY_REQUIRED)
                res = await ri.warm_symbol(conn, Boom(), "FAILS",
                                           now=datetime(2026, 9, 2, tzinfo=UTC))
                row = dict(await conn.fetchrow(
                    "SELECT warmup_last_error_code, warmup_last_error_class,"
                    " warmup_cooldown_until FROM public.research_symbols"
                    " WHERE symbol=$1", "FAILS"))
                return res, row
            finally:
                await conn.close()
        res, row = asyncio.run(go())
        assert row["warmup_last_error_class"] in ("retryable", "terminal",
                                                  "operator_error")
        assert row["warmup_last_error_code"] is not None


# =========================================================================== #
# D6-D10 / T15 — HEALTHY_WAITING vs TERMINAL_BLOCKED, from persisted rows
#
# Since the lifecycle defers and re-enters, `blocked_stale_core_history` covers
# both "healthy, the queue will bring it back" and "stopped, an operator must
# act". A monitor that cannot tell them apart pages every morning or never.
# These prove the distinction is decidable from persisted state alone.
# =========================================================================== #

import app.research_runs as rr


async def _mk_run(conn, run_key, *, run_status, target=date(2026, 9, 2)):
    return await conn.fetchval(
        "INSERT INTO public.research_lifecycle_runs "
        "(run_key, contract_version, status, target_session) "
        "VALUES ($1,'v1',$2,$3) RETURNING id", run_key, run_status, target)


async def _mk_task(conn, run_key, *, status, attempt, max_attempts=4,
                   available_at=None):
    available_at = available_at or datetime.now(UTC)   # NOT NULL in the schema
    job_id = await conn.fetchval(
        "INSERT INTO public.job_runs (job_type, job_contract_version,"
        " queue_name, idempotency_key, status, requested_by) "
        "VALUES ('smart_scanner_research_lifecycle.v1','v1','research_lifecycle',"
        "        $1,'running','scheduler') RETURNING id", f"rlcjob:{run_key}")
    await conn.execute(
        "INSERT INTO public.job_tasks (job_id, queue_name, task_type,"
        " task_contract_version, task_key, ordinal, payload, payload_hash,"
        " idempotency_key, status, priority, max_attempts, attempt_count,"
        " available_at) "
        "VALUES ($1,'research_lifecycle','smart_scanner_research_lifecycle_run.v1',"
        "        'v1','lifecycle',0,'{}'::jsonb,'h',$2,$3,100,$4,$5,$6)",
        job_id, f"rlctask:{run_key}", status, max_attempts, attempt, available_at)


class TestOperationalHealthPredicate:

    def _health(self, pg, setup):
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await conn.execute(
                    "TRUNCATE public.research_lifecycle_run_symbols,"
                    " public.research_lifecycle_runs CASCADE")
                await conn.execute("DELETE FROM public.job_tasks")
                await conn.execute("DELETE FROM public.job_events")
                await conn.execute("DELETE FROM public.job_runs")
                await setup(conn)
                return await rr.run_health(conn)
            finally:
                await conn.close()
        return asyncio.run(go())

    def test_d6_a_deferred_run_reads_as_healthy_waiting(self, pg):
        """The exact 2026-09-02 shape under the new architecture: blocked on
        stale core, task retryable, a future attempt scheduled."""
        async def setup(conn):
            await _mk_run(conn, "rlc:sch:wait", run_status="blocked_stale_core_history")
            await _mk_task(conn, "rlc:sch:wait", status="retryable", attempt=1,
                           available_at=datetime.now(UTC) + timedelta(minutes=30))
        rows = self._health(pg, setup)
        assert len(rows) == 1
        assert rows[0]["health"] == rr.HEALTH_HEALTHY_WAITING
        assert rows[0]["run_status"] == "blocked_stale_core_history"

    def test_d7_a_terminal_prerequisite_failure_reads_as_terminal_blocked(self, pg):
        """Same run status, but the task failed — nothing will come back."""
        async def setup(conn):
            await _mk_run(conn, "rlc:sch:dead", run_status="blocked_stale_core_history")
            await _mk_task(conn, "rlc:sch:dead", status="failed", attempt=2)
        rows = self._health(pg, setup)
        assert rows[0]["health"] == rr.HEALTH_TERMINAL_BLOCKED

    def test_d8_an_exhausted_attempt_budget_reads_as_terminal_blocked(self, pg):
        """Retryable in name, but no attempts left is not a future attempt."""
        async def setup(conn):
            await _mk_run(conn, "rlc:sch:spent", run_status="blocked_stale_core_history")
            await _mk_task(conn, "rlc:sch:spent", status="retryable", attempt=4,
                           max_attempts=4,
                           available_at=datetime.now(UTC) + timedelta(minutes=30))
        rows = self._health(pg, setup)
        assert rows[0]["health"] == rr.HEALTH_TERMINAL_BLOCKED

    def test_d9_a_continued_run_reads_as_completed_with_no_residual_wait(self, pg):
        async def setup(conn):
            await _mk_run(conn, "rlc:sch:done", run_status="completed")
            await _mk_task(conn, "rlc:sch:done", status="succeeded", attempt=3)
        rows = self._health(pg, setup)
        assert rows[0]["health"] == rr.HEALTH_COMPLETED
        assert rows[0]["task_status"] == "succeeded"

    def test_d10_a_deferred_task_survives_reclaim_without_a_second_run(self, pg):
        """Crash/reclaim: the task returns to retryable and is re-claimable;
        the run keeps ONE identity."""
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await conn.execute(
                    "TRUNCATE public.research_lifecycle_run_symbols,"
                    " public.research_lifecycle_runs CASCADE")
                await conn.execute("DELETE FROM public.job_tasks")
                await conn.execute("DELETE FROM public.job_events")
                await conn.execute("DELETE FROM public.job_runs")
                await _mk_run(conn, "rlc:sch:crash",
                              run_status="blocked_stale_core_history")
                await _mk_task(conn, "rlc:sch:crash", status="retryable",
                               attempt=1,
                               available_at=datetime.now(UTC) - timedelta(minutes=1))
                # re-open the SAME run, as a re-entry does
                first = await rr.start_run(conn, run_key="rlc:sch:crash",
                                           target_session=date(2026, 9, 2))
                second = await rr.start_run(conn, run_key="rlc:sch:crash",
                                            target_session=date(2026, 9, 3))
                n = await conn.fetchval(
                    "SELECT count(*) FROM public.research_lifecycle_runs")
                health = await rr.run_health(conn)
                return first, second, n, health
            finally:
                await conn.close()
        first, second, n, health = asyncio.run(go())
        assert n == 1, "re-entry must not create a second run"
        assert first["id"] == second["id"]
        # and the pin holds even though the caller passed a later session
        assert second["target_session"] == date(2026, 9, 2)
        assert health[0]["health"] == rr.HEALTH_HEALTHY_WAITING

    def test_d4_the_run_budget_is_not_reset_by_re_entry(self, pg):
        """T13: start_run reports what the run already spent, so the next
        attempt spends the remainder rather than a fresh ceiling."""
        async def go():
            conn = await asyncpg.connect(pg["dsn"])
            try:
                await conn.execute(
                    "TRUNCATE public.research_lifecycle_run_symbols,"
                    " public.research_lifecycle_runs CASCADE")
                await _mk_run(conn, "rlc:sch:budget",
                              run_status="blocked_stale_core_history")
                await conn.execute(
                    "UPDATE public.research_lifecycle_runs"
                    " SET provider_calls_used=9 WHERE run_key='rlc:sch:budget'")
                return await rr.start_run(conn, run_key="rlc:sch:budget",
                                          target_session=date(2026, 9, 2))
            finally:
                await conn.close()
        run = asyncio.run(go())
        assert run["provider_calls_used"] == 9
        remaining = max(0, ru.MAX_PROVIDER_REQUESTS_PER_RUN
                        - run["provider_calls_used"])
        assert remaining == 3, "re-entry must spend the remainder, not 12 again"
