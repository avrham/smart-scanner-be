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
