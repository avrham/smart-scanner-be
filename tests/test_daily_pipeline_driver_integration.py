"""Integration for the durable daily-pipeline DRIVER (scheduler -> occurrence ->
handler -> advance_daily_pipeline_service -> occurrence succeeded), fully
automatic — no HTTP self-call, no fly ssh, no WORKER_TOKEN.

Reuses the campaign-integration harness (real Docker PG, migrations 001-018,
prospective_runner role, frozen universe + daily + 4H so readiness is ready).
Proves: the scheduler materialises exactly ONE occurrence marker on the driver
queue + ONE driver task with the frozen-universe payload (idempotent on a
duplicate fire); the driver handler advances the occurrence, DEFERS (retryable)
while the async campaign job runs, then resumes to a succeeded occurrence; a
same-session replay is idempotent (no duplicate campaign/pairs/evaluations); and
the probe reconciles a crash after success.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

from app.jobs import contracts as C
from app.jobs import daily_pipeline as DP
from app.jobs import queue as Q
from app.jobs import scheduler as SCHED

from test_prospective_campaign_integration import (  # noqa: E402
    prospective, pg, _no_external_network, _db, _gen_daily, _gen_4h, _psql, _sh,
    _docker_ready, DBNAME, PROS, PROS_PW,
)
from app.history_warmup_execute import compute_universe_hash  # noqa: E402

pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")

DRV_SYMS = ["ZZDV1", "ZZDV2"]
DRV_CODE = "ZZDRIVE"


def _seed_universe(pg):
    cid = pg["cid"]
    _psql(cid, "INSERT INTO history_warmup_universes(universe_code,universe_version,universe_hash,"
          f"config_hash,status,symbol_count,frozen_at) VALUES('{DRV_CODE}',1,'pending','cfg','draft',{len(DRV_SYMS)},NULL);")
    uid = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
               "-c", f"SELECT id FROM history_warmup_universes WHERE universe_code='{DRV_CODE}';"]).stdout.strip()
    for i, s in enumerate(DRV_SYMS):
        _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{uid}','{s}',{i});")
    uhash = compute_universe_hash(universe_code=DRV_CODE, universe_version=1, symbols_in_ordinal_order=DRV_SYMS)
    _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{uid}';")
    yday = datetime.now(timezone.utc).date() - timedelta(days=1)
    for s in DRV_SYMS:
        vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, yday))
        _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals};")
        f4 = _gen_4h(s, datetime.now(timezone.utc) - timedelta(hours=2))
        v4 = ",".join(
            f"('{r[0]}','{r[1].isoformat()}','{r[2].isoformat()}','{r[3]}',{r[4]},{r[5]},{r[6]},{r[7]},{r[8]},true,true,'sha256:dv{i}')"
            for i, r in enumerate(f4))
        _psql(cid, f"INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES {v4};")
    return uid, uhash


def _insert_pipeline_schedule(pg, uid, uhash):
    cid = pg["cid"]
    payload = json.dumps({"contract_version": DP.PIPELINE_CONTRACT_VERSION_V2,
                          "universe_id": uid, "universe_hash": uhash})
    sid = str(uuid.uuid4())
    r = _psql(cid, "INSERT INTO job_schedules(id,schedule_code,schedule_version,schedule_type,timezone,"
              "market_close_delay_minutes,job_type,job_contract_version,enabled,paused,payload_template,"
              "idempotency_scope,next_run_at) VALUES("
              f"'{sid}','TEST-DAILY-PIPELINE',1,'market_daily','America/New_York',30,"
              f"'{DP.PIPELINE_JOB_TYPE}','{DP.PIPELINE_CONTRACT_VERSION_V2}',TRUE,FALSE,"
              f"'{payload}'::jsonb,'occurrence', NOW() - INTERVAL '1 minute');")
    assert r.returncode == 0, f"schedule insert failed: {r.stderr[-400:]}"
    return sid


class TestDailyPipelineDriver:
    def test_scheduler_materializes_driver_task_and_driver_advances_to_succeeded(self, prospective, monkeypatch):
        from app.config import settings
        from app.jobs.handlers.daily_pipeline_driver import (
            drive_pipeline_advance, probe_daily_pipeline_advance_durable_output)
        from app.jobs.handlers.prospective import evaluate_prospective_symbol
        from app.jobs.prospective_enqueue import sync_prospective_campaign
        import app.prospective_session as psess

        pg = prospective
        uid, uhash = _seed_universe(pg)
        _insert_pipeline_schedule(pg, uid, uhash)
        # deterministic session for the occurrence identity
        fixed = datetime.now(timezone.utc).date() - timedelta(days=1)
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: fixed)
        monkeypatch.setattr(psess, "resolve_snapshot",
                            lambda now: {"snapshot_session_date": fixed.isoformat(),
                                         "snapshot_cutoff_at": (datetime.combine(fixed, datetime.min.time(), timezone.utc)
                                                                + timedelta(hours=20)).isoformat(),
                                         "market_calendar_version": "us_market_calendar.v1"})
        monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
        monkeypatch.setattr(settings, "PROSPECTIVE_ALLOWED_EXPERIMENT_CODE", "wyckoff_v2_vs_baseline", raising=False)
        monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
        # defer immediately on a waiting stage instead of holding the task
        monkeypatch.setattr(settings, "DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS", 0, raising=False)

        # Drive the whole logic on the DB OWNER pool so a single pool can run the
        # scheduler tick (job_schedules), the driver advance, the evaluation
        # writes and task claim/complete. This proves the DRIVER + SCHEDULER
        # LOGIC end-to-end; the least-privilege driver ROLE is verified separately
        # by ops/sql/verify_pipeline_driver.sql and the live current_user check.
        owner_dsn = f"postgresql://postgres:postgres@127.0.0.1:{pg['hp']}/{DBNAME}"
        monkeypatch.setattr(settings, "PROSPECTIVE_DATABASE_URL", owner_dsn)

        async def _process_campaign_tasks():
            """Claim+complete the campaign evaluation tasks — the async child job."""
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
            await deps.init_db_pool()  # runner-role global pool (PROSPECTIVE_DATABASE_URL)
            try:
                # ---- scheduler fires -> marker on driver queue + ONE driver task
                tick = await SCHED.run_scheduler_tick(worker_id="drv-test", now=datetime.now(timezone.utc))
                assert tick["leader"] is True and tick["enqueued"] == 1, tick
                conn = await get_db_connection()
                try:
                    markers = await conn.fetch("SELECT id, queue_name FROM job_runs WHERE job_type=$1", DP.PIPELINE_JOB_TYPE)
                    assert len(markers) == 1 and markers[0]["queue_name"] == DP.DAILY_PIPELINE_DRIVER_QUEUE
                    tasks = await conn.fetch("SELECT payload FROM job_tasks WHERE task_type=$1 AND queue_name=$2",
                                             DP.DAILY_PIPELINE_ADVANCE_TASK, DP.DAILY_PIPELINE_DRIVER_QUEUE)
                    assert len(tasks) == 1
                    payload = json.loads(tasks[0]["payload"]) if isinstance(tasks[0]["payload"], str) else tasks[0]["payload"]
                    assert payload["universe_id"] == uid
                    assert payload["pipeline_contract_version"] == DP.PIPELINE_CONTRACT_VERSION_V2

                    # duplicate materialisation for the SAME occurrence is idempotent:
                    # a second _create_scheduled_job for the identical occurrence
                    # instant creates neither a second marker nor a second driver task.
                    from app.jobs import identity as ident
                    sched = dict(await conn.fetchrow(
                        "SELECT * FROM job_schedules WHERE schedule_code='TEST-DAILY-PIPELINE'"))
                    occ = datetime(2027, 1, 4, 20, 30, tzinfo=timezone.utc)
                    okey = ident.schedule_occurrence_idempotency_key(
                        schedule_code="TEST-DAILY-PIPELINE", schedule_version=1, occurrence_iso=occ.isoformat())
                    a = await SCHED._create_scheduled_job(conn, sched, occ)
                    b = await SCHED._create_scheduled_job(conn, sched, occ)
                    assert a is not None and b is None
                    assert await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE idempotency_key=$1", okey) == 1
                    assert await conn.fetchval("SELECT COUNT(*) FROM job_tasks WHERE idempotency_key=$1", "dpadv:" + okey) == 1
                finally:
                    await release_db_connection(conn)

                # ---- driver advances: history_refresh + campaign enqueue, then DEFERS
                r1 = await drive_pipeline_advance(payload)
                assert r1["ok"] is False and r1["error_class"] == C.ERR_RETRYABLE
                assert r1["safe_error_code"] == "occurrence_in_progress", r1

                await _process_campaign_tasks()  # the async child job completes

                # ---- driver resumes: campaign completed -> outcome deferred -> audit -> succeeded
                r2 = await drive_pipeline_advance(payload)
                assert r2["ok"] is True, r2
                assert r2["result"]["occurrence_status"] == "succeeded"

                # ---- replay idempotent: no new campaign/pairs/evaluations
                conn = await get_db_connection()
                try:
                    run = await conn.fetchrow(
                        "SELECT campaign_run_id FROM prospective_campaign_registrations WHERE universe_id=$1", uid)
                    camps = await conn.fetchval(
                        "SELECT COUNT(*) FROM prospective_campaign_registrations WHERE universe_id=$1", uid)
                    pairs = await conn.fetchval(
                        "SELECT COUNT(*) FROM strategy_shadow_run_pairs WHERE run_id=$1", run["campaign_run_id"])
                    evals = await conn.fetchval(
                        "SELECT COUNT(*) FROM strategy_shadow_evaluations e JOIN strategy_shadow_run_pairs rp "
                        "ON rp.pair_id=e.pair_id WHERE rp.run_id=$1", run["campaign_run_id"])
                finally:
                    await release_db_connection(conn)
                assert camps == 1 and pairs == len(DRV_SYMS) and evals == 2 * len(DRV_SYMS)

                r3 = await drive_pipeline_advance(payload)  # occurrence already succeeded
                assert r3["ok"] is True, r3
                conn = await get_db_connection()
                try:
                    assert await conn.fetchval(
                        "SELECT COUNT(*) FROM prospective_campaign_registrations WHERE universe_id=$1", uid) == 1
                    probed = await probe_daily_pipeline_advance_durable_output(conn, payload)
                    assert probed is not None and probed["occurrence_status"] == "succeeded"
                finally:
                    await release_db_connection(conn)
            finally:
                import app.deps as _deps
                await _deps.close_db_pool()

        asyncio.run(drive())
