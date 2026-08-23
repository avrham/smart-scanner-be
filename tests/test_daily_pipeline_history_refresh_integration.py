"""Live-failure regression for the durable HISTORY-REFRESH stage (Root Cause A).

Reproduces the exact live failure on real Docker Postgres with a MOCKED provider
(no network, no real key): a scheduled v2 occurrence whose frozen universe has
STALE daily history. Before the fix the history stage recorded BLOCKED (an
operator HTTP call) and the driver deferred forever, then terminally failed.
After the fix the driver enqueues ONE durable history-refresh child job, WAITS
(in_progress → occurrence_in_progress, never BLOCKED, never premature terminal),
the history-refresh worker runs the provider-backed refresh, the driver re-checks
readiness and advances history_refresh → campaign → outcome → audit → succeeded,
and the outer scheduled marker succeeds. Also covers: idempotent replay (no
duplicate provider job), a terminal history-child failure (truthful stage
failure), and crash reconcile via the probe.

Runs the whole flow on the DB OWNER pool (proves the DRIVER + HISTORY LOGIC
end-to-end); the least-privilege history_warmer/pipeline_driver ROLES + queue
isolation are verified separately by the ops/sql verifiers.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

from app.jobs import contracts as C
from app.jobs import daily_pipeline as DP
from app.jobs import history_refresh as HR
from app.jobs import queue as Q
from app.jobs import scheduler as SCHED

from test_prospective_campaign_integration import (  # noqa: E402
    prospective, pg, _no_external_network, _db, _gen_daily, _gen_4h, _psql, _sh,
    _docker_ready, DBNAME,
)
from test_daily_pipeline_driver_integration import _seed_universe, _insert_pipeline_schedule, DRV_SYMS  # noqa: E402

pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")


def _provider_for(symbols, end_date):
    """A FakeProvider returning the SAME _gen_daily series the universe was
    seeded with (so a gap-fill restores exactly the deleted sessions and
    readiness returns to ready). Never touches the network."""
    from tests.support.fake_provider import FakeProvider
    daily = {}
    for s in symbols:
        daily[s.upper()] = [
            {"symbol": s, "trading_date": r[1], "open": r[2], "high": r[3],
             "low": r[4], "close": r[5], "volume": r[6]}
            for r in _gen_daily(s, end_date)
        ]
    return FakeProvider(daily=daily)


def _make_daily_stale(pg, *, cutoff):
    """Delete daily bars AFTER cutoff so the latest local daily session lags the
    target → the history stage must refresh before the campaign can run."""
    _psql(pg["cid"], f"DELETE FROM daily_bars WHERE symbol = ANY(ARRAY['" +
          "','".join(DRV_SYMS) + f"']) AND trading_date > DATE '{cutoff.isoformat()}';")


def _reset(pg):
    """Isolate each test on the shared module-scoped DB: clear job-queue, shadow,
    history, and registration rows so re-seeding the fixed ZZDRIVE universe +
    TEST-DAILY-PIPELINE schedule never collides across tests."""
    _psql(pg["cid"],
          "TRUNCATE job_task_attempts, job_events, job_tasks, job_runs, job_workers, "
          "job_schedules, job_dependencies, prospective_campaign_registrations, "
          "strategy_shadow_evaluations, strategy_shadow_pair_outcomes, "
          "strategy_shadow_run_pairs, strategy_shadow_pairs, strategy_shadow_outcome_runs, "
          "strategy_shadow_runs, market_bars_4h, daily_bars, history_warmup_runs, "
          "history_warmup_universe_symbols, history_warmup_universes "
          "RESTART IDENTITY CASCADE;")


class TestDailyPipelineHistoryRefresh:
    def test_stale_history_auto_refreshes_then_pipeline_succeeds(self, prospective, monkeypatch):
        from app.config import settings
        import app.routers.admin as admin
        import app.prospective_session as psess
        from app.jobs.handlers.daily_pipeline_driver import drive_pipeline_advance
        from app.jobs.handlers.history_refresh_worker import execute_history_refresh_symbol
        from app.jobs.handlers.prospective import evaluate_prospective_symbol
        from app.jobs.prospective_enqueue import sync_prospective_campaign

        pg = prospective
        _reset(pg)
        uid, uhash = _seed_universe(pg)
        _insert_pipeline_schedule(pg, uid, uhash)
        fixed = datetime.now(timezone.utc).date() - timedelta(days=1)
        # deterministic session everywhere target is resolved
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: fixed)
        monkeypatch.setattr(psess, "resolve_snapshot",
                            lambda now: {"snapshot_session_date": fixed.isoformat(),
                                         "snapshot_cutoff_at": (datetime.combine(fixed, datetime.min.time(), timezone.utc)
                                                                + timedelta(hours=20)).isoformat(),
                                         "market_calendar_version": "us_market_calendar.v1"})
        monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
        monkeypatch.setattr(settings, "PROSPECTIVE_ALLOWED_EXPERIMENT_CODE", "wyckoff_v2_vs_baseline", raising=False)
        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
        monkeypatch.setattr(settings, "DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS", 0, raising=False)
        # no provider cooldown between symbols in the test
        monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0, raising=False)
        owner_dsn = f"postgresql://postgres:postgres@127.0.0.1:{pg['hp']}/{DBNAME}"
        monkeypatch.setattr(settings, "PROSPECTIVE_DATABASE_URL", owner_dsn)
        # provider is injected ONLY into the history-refresh service seam
        provider = _provider_for(DRV_SYMS, fixed)
        monkeypatch.setattr(admin, "_resolve_history_warmup_provider", lambda: provider)

        # make daily stale (leave 4H fresh so only a daily refresh is needed)
        _make_daily_stale(pg, cutoff=fixed - timedelta(days=8))

        async def _process_history_tasks():
            from app.workers.persistence import get_db_connection, release_db_connection
            processed = 0
            while True:
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=HR.HISTORY_REFRESH_QUEUE,
                                                worker_id="hrw", lease_seconds=900)
                finally:
                    await release_db_connection(conn)
                if t is None:
                    break
                p = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                res = await execute_history_refresh_symbol(p)
                assert res["ok"] is True, res
                conn = await get_db_connection()
                try:
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="hrw",
                                                    result_summary=res["result"])
                finally:
                    await release_db_connection(conn)
                processed += 1
            return processed

        async def _process_campaign_tasks():
            from app.workers.persistence import get_db_connection, release_db_connection
            for _ in range(len(DRV_SYMS)):
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_QUEUE, worker_id="evw", lease_seconds=900)
                    assert t is not None
                    p = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                finally:
                    await release_db_connection(conn)
                res = await evaluate_prospective_symbol(p)
                assert res["ok"] is True, res
                conn = await get_db_connection()
                try:
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="evw", result_summary=res["result"])
                    await sync_prospective_campaign(conn, t["job_id"])
                finally:
                    await release_db_connection(conn)

        async def drive():
            import app.deps as deps
            from app.workers.persistence import get_db_connection, release_db_connection
            await deps.init_db_pool()
            try:
                tick = await SCHED.run_scheduler_tick(worker_id="drv", now=datetime.now(timezone.utc))
                assert tick["enqueued"] == 1, tick
                conn = await get_db_connection()
                try:
                    trow = await conn.fetchrow(
                        "SELECT payload FROM job_tasks WHERE task_type=$1", DP.DAILY_PIPELINE_ADVANCE_TASK)
                    payload = json.loads(trow["payload"]) if isinstance(trow["payload"], str) else trow["payload"]
                finally:
                    await release_db_connection(conn)

                # ---- advance #1: stale history -> enqueue child job, WAIT (not blocked/terminal)
                r1 = await drive_pipeline_advance(payload)
                assert r1["ok"] is False and r1["error_class"] == C.ERR_RETRYABLE
                assert r1["safe_error_code"] == "occurrence_in_progress", r1

                conn = await get_db_connection()
                try:
                    occ = await conn.fetchrow(
                        "SELECT * FROM job_runs WHERE job_type=$1 AND queue_name=$2 "
                        "AND result_summary IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                        DP.PIPELINE_JOB_TYPE, DP.PIPELINE_QUEUE)
                    view = DP.build_status_view(occ)
                    # THE third-check fix: waiting on async child = in_progress, NOT blocked.
                    assert view["current_stage"] == DP.STAGE_HISTORY_REFRESH
                    assert view["stage_states"][DP.STAGE_HISTORY_REFRESH] == DP.STAGE_STATE_IN_PROGRESS
                    assert view["blocked_stage"] is None and view["terminal_failure_stage"] is None
                    hjid = view["history_job_id"]
                    assert hjid, view
                    # exactly ONE history-refresh job + one task per symbol on its own queue
                    hjob = await conn.fetchrow("SELECT status, queue_name FROM job_runs WHERE id=$1", hjid)
                    assert hjob["queue_name"] == HR.HISTORY_REFRESH_QUEUE
                    ntasks = await conn.fetchval(
                        "SELECT COUNT(*) FROM job_tasks WHERE job_id=$1 AND task_type=$2",
                        hjid, HR.HISTORY_REFRESH_TASK)
                    assert ntasks == len(DRV_SYMS)
                finally:
                    await release_db_connection(conn)

                # ---- the history-refresh worker runs the provider-backed refresh
                processed = await _process_history_tasks()
                assert processed == len(DRV_SYMS)
                assert provider.call_count() >= 1  # provider WAS used (by the worker only)
                conn = await get_db_connection()
                try:
                    hstatus = await HR.history_refresh_job_status(conn, hjid)
                    assert hstatus == C.JOB_SUCCEEDED, hstatus
                    # the refresh advanced every symbol's latest daily session past
                    # the stale cutoff (readiness freshness is the authoritative gate,
                    # asserted via history_refresh COMPLETED on advance #2 below).
                    min_latest = await conn.fetchval(
                        "SELECT MIN(m) FROM (SELECT symbol, MAX(trading_date) m FROM daily_bars "
                        "WHERE symbol = ANY($1) GROUP BY symbol) x", DRV_SYMS)
                    assert min_latest is not None and min_latest > (fixed - timedelta(days=8))
                finally:
                    await release_db_connection(conn)

                # ---- advance #2: history now ready -> COMPLETED -> campaign enqueued -> WAIT
                r2 = await drive_pipeline_advance(payload)
                assert r2["ok"] is False and r2["safe_error_code"] == "occurrence_in_progress", r2
                conn = await get_db_connection()
                try:
                    occ = await conn.fetchrow("SELECT * FROM job_runs WHERE id=$1", occ["id"])
                    view = DP.build_status_view(occ)
                    assert view["stage_states"][DP.STAGE_HISTORY_REFRESH] == DP.STAGE_STATE_COMPLETED
                    assert view["current_stage"] == DP.STAGE_PROSPECTIVE_CAMPAIGN
                finally:
                    await release_db_connection(conn)

                await _process_campaign_tasks()

                # ---- advance #3: campaign done -> outcome -> audit -> occurrence SUCCEEDED
                r3 = await drive_pipeline_advance(payload)
                assert r3["ok"] is True and r3["result"]["occurrence_status"] == "succeeded", r3

                # ---- replay idempotent: no duplicate history job, no duplicate campaign
                conn = await get_db_connection()
                try:
                    assert await conn.fetchval(
                        "SELECT COUNT(*) FROM job_runs WHERE job_type=$1", HR.HISTORY_REFRESH_JOB_TYPE) == 1
                    assert await conn.fetchval(
                        "SELECT COUNT(*) FROM prospective_campaign_registrations WHERE universe_id=$1", uid) == 1
                    # a re-enqueue recognizes the same job (no new provider batch)
                    again = await HR.enqueue_history_incremental_refresh(
                        conn, universe_id=uid, universe_hash=uhash, symbols=DRV_SYMS,
                        resolved_session_date=fixed.isoformat(), requested_by="test")
                    assert again["status"] in ("already_applied", "already_queued")
                    assert await conn.fetchval(
                        "SELECT COUNT(*) FROM job_runs WHERE job_type=$1", HR.HISTORY_REFRESH_JOB_TYPE) == 1
                finally:
                    await release_db_connection(conn)
            finally:
                import app.deps as _deps
                await _deps.close_db_pool()

        asyncio.run(drive())

    def test_history_child_terminal_failure_fails_stage_truthfully(self, prospective, monkeypatch):
        """A provider auth/terminal error fails the history child job, and the
        pipeline records a TRUTHFUL terminal failure — not an endless BLOCKED."""
        from app.config import settings
        import app.routers.admin as admin
        import app.prospective_session as psess
        from app.jobs.handlers.daily_pipeline_driver import drive_pipeline_advance
        from app.jobs.handlers.history_refresh_worker import execute_history_refresh_symbol
        from tests.support.fake_provider import auth_error

        pg = prospective
        _reset(pg)
        uid, uhash = _seed_universe(pg)
        _insert_pipeline_schedule(pg, uid, uhash)
        fixed = datetime.now(timezone.utc).date() - timedelta(days=1)
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: fixed)
        monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
        monkeypatch.setattr(settings, "DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS", 0, raising=False)
        monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0, raising=False)
        owner_dsn = f"postgresql://postgres:postgres@127.0.0.1:{pg['hp']}/{DBNAME}"
        monkeypatch.setattr(settings, "PROSPECTIVE_DATABASE_URL", owner_dsn)
        from tests.support.fake_provider import FakeProvider
        monkeypatch.setattr(admin, "_resolve_history_warmup_provider",
                            lambda: FakeProvider(daily_error=auth_error()))
        _make_daily_stale(pg, cutoff=fixed - timedelta(days=8))

        async def drive():
            import app.deps as deps
            from app.workers.persistence import get_db_connection, release_db_connection
            await deps.init_db_pool()
            try:
                await SCHED.run_scheduler_tick(worker_id="drv", now=datetime.now(timezone.utc))
                conn = await get_db_connection()
                try:
                    trow = await conn.fetchrow(
                        "SELECT payload FROM job_tasks WHERE task_type=$1", DP.DAILY_PIPELINE_ADVANCE_TASK)
                    payload = json.loads(trow["payload"]) if isinstance(trow["payload"], str) else trow["payload"]
                finally:
                    await release_db_connection(conn)

                await drive_pipeline_advance(payload)  # enqueue child job
                # run the child task: provider auth error -> operator/terminal -> task failed
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=HR.HISTORY_REFRESH_QUEUE, worker_id="hrw", lease_seconds=900)
                    p = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                finally:
                    await release_db_connection(conn)
                res = await execute_history_refresh_symbol(p)
                assert res["ok"] is False and res["error_class"] in (C.ERR_OPERATOR, C.ERR_TERMINAL), res
                conn = await get_db_connection()
                try:
                    await Q.settle_task_failure(conn, task_id=t["id"], worker_id="hrw",
                                                safe_error_code=res["safe_error_code"],
                                                error_class=res["error_class"], backoff_seconds_value=None)
                    # fail any sibling tasks too so the whole job is terminal
                    sibs = await conn.fetch(
                        "SELECT id FROM job_tasks WHERE job_id=(SELECT job_id FROM job_tasks WHERE id=$1) "
                        "AND status IN ('queued','retryable')", t["id"])
                    for s in sibs:
                        st = await Q.claim_next_task(conn, queue_name=HR.HISTORY_REFRESH_QUEUE, worker_id="hrw", lease_seconds=900)
                        if st is None:
                            break
                        rr = await execute_history_refresh_symbol(
                            json.loads(st["payload"]) if isinstance(st["payload"], str) else st["payload"])
                        await Q.settle_task_failure(conn, task_id=st["id"], worker_id="hrw",
                                                    safe_error_code=rr["safe_error_code"],
                                                    error_class=rr["error_class"], backoff_seconds_value=None)
                    jrow = await conn.fetchrow(
                        "SELECT id, status FROM job_runs WHERE job_type=$1", HR.HISTORY_REFRESH_JOB_TYPE)
                    assert jrow["status"] == C.JOB_FAILED, dict(jrow)
                finally:
                    await release_db_connection(conn)

                # next advance: history child failed -> stage TERMINAL_FAILURE (truthful)
                r = await drive_pipeline_advance(payload)
                assert r["ok"] is False and r["error_class"] == C.ERR_TERMINAL, r
                conn = await get_db_connection()
                try:
                    occ = await conn.fetchrow(
                        "SELECT * FROM job_runs WHERE job_type=$1 AND queue_name=$2 "
                        "AND result_summary IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                        DP.PIPELINE_JOB_TYPE, DP.PIPELINE_QUEUE)
                    view = DP.build_status_view(occ)
                    assert view["terminal_failure_stage"] == DP.STAGE_HISTORY_REFRESH
                    assert view["occurrence_status"] == "failed"
                finally:
                    await release_db_connection(conn)
            finally:
                import app.deps as _deps
                await _deps.close_db_pool()

        asyncio.run(drive())

    def test_probe_recognizes_current_symbol_for_crash_reconcile(self, prospective, monkeypatch):
        """The probe reconciles a crash-after-persist: a symbol that is now
        current returns a success; a still-stale symbol returns None (retry)."""
        from app.config import settings
        import app.prospective_session as psess
        from app.jobs.handlers.history_refresh_worker import probe_history_refresh_durable_output

        pg = prospective
        _reset(pg)
        uid, uhash = _seed_universe(pg)
        fixed = datetime.now(timezone.utc).date() - timedelta(days=1)
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: fixed)
        owner_dsn = f"postgresql://postgres:postgres@127.0.0.1:{pg['hp']}/{DBNAME}"

        async def go():
            conn = await asyncpg.connect(owner_dsn)
            try:
                # current symbol (seed leaves daily through `fixed`) -> reconciled
                ok = await probe_history_refresh_durable_output(conn, {"symbol": DRV_SYMS[0]})
                assert ok is not None and ok["status"] == "reconciled_current"
                # now make it stale -> probe returns None (not yet complete)
                await conn.execute(
                    "DELETE FROM daily_bars WHERE symbol=$1 AND trading_date > $2",
                    DRV_SYMS[0], fixed - timedelta(days=3))
                none = await probe_history_refresh_durable_output(conn, {"symbol": DRV_SYMS[0]})
                assert none is None
            finally:
                await conn.close()

        asyncio.run(go())
