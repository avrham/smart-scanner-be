"""Real-Postgres integration for the durable job queue + prospective worker.

Isolated Docker PostgreSQL (never Supabase/Massive): migrations 001-018, the
prospective_runner + prospective_worker + job_audit_reader roles and RLS. Covers
(Part 23): migration 018 + constraints, bounded payload, unique idempotency,
SKIP LOCKED claiming by two workers with no double-claim, lease renewal/expiry,
crash-before/during/after-persist recovery, retry backoff, max-attempt terminal,
cancellation, operator retry, enqueue + replay, task reconciliation replay, a
full campaign completion, no duplicate pairs, exactly two arms per pair, no
outcomes, scheduler leader election, duplicate-occurrence prevention,
market-daily calc, paused/disabled schedules, worker heartbeat, stale detection,
RLS + privilege boundaries, no provider construction, no external network,
future-bar / incomplete-4H exclusion.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import socket
import subprocess
import time
import uuid
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
from datetime import date, datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.prospective_campaign as pc
from app.jobs import contracts as C
from app.jobs import queue as Q
from app.jobs import identity as ident

PG_IMAGE = "postgres:16-alpine"
DBNAME = "jobsdb"
SU = "postgres"
PROS_PW = "prospw_local_only"
WORK_PW = "workpw_local_only"
AUD_PW = "audpw_local_only"
PROS = "smart_scanner_prospective_runner"
WORK = "smart_scanner_prospective_worker"
AUD = "smart_scanner_job_audit_reader"
MIGRATIONS = ["001_initial_schema", "002_phase1_sma150_config", "003_phase2_signal_outcomes",
              "004_phase5_wyckoff_mtf_config", "005_massive_provider", "006_market_data_jobs",
              "007_scan_signal_provenance", "008_sma150_v3", "009_watch_outcome_coverage",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes", "017_prospective_campaign_registration",
              "018_durable_job_queue"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEGACY_RLS = ["strategy_shadow_evaluations", "strategy_shadow_pairs", "strategy_shadow_pair_outcomes",
              "strategy_shadow_run_pairs", "strategy_shadow_runs", "strategy_shadow_outcome_runs",
              "daily_bars", "patterns", "pattern_configs"]
SYMS = ["ZZAA", "ZZBB"]


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE], capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")


def _sh(a, inp=None, t=120):
    return subprocess.run(a, input=inp, capture_output=True, text=True, timeout=t)


def _psql(cid, sql, *, variables=None, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    return _sh(args, inp=(open(path).read() if path else sql))


def _gen_daily(sym, end_date, n=800):
    rows = []
    for i in range(n):
        d = end_date - timedelta(days=(n - 1 - i))
        base = 100.0 + i * 0.05 + 12.0 * math.sin(i / 25.0)
        o = round(base, 2); c = round(base + math.sin(i / 7.0), 2)
        hi = round(max(o, c) + 1.5 + abs(math.sin(i / 5.0)), 2)
        lo = round(min(o, c) - 1.5 - abs(math.cos(i / 6.0)), 2)
        rows.append((sym, d, o, hi, lo, c, float(1_000_000 + (i % 11) * 25_000)))
    return rows


def _gen_4h(sym, end_dt, n=120):
    rows = []
    for i in range(n):
        start = end_dt - timedelta(hours=4 * (n - i))
        end = start + timedelta(hours=4)
        base = 100.0 + i * 0.05
        o = round(base, 2); c = round(base + 0.3, 2)
        hi = round(max(o, c) + 0.8, 2); lo = round(min(o, c) - 0.8, 2)
        rows.append((sym, start, end, end.astimezone(timezone.utc).date(), o, hi, lo, c, 500000.0))
    return rows


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres", "-P", PG_IMAGE]).stdout.strip()
    assert cid
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready", "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        hp = int(_sh(["docker", "port", cid, "5432/tcp"]).stdout.splitlines()[0].rsplit(":", 1)[1])
        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c", f"CREATE DATABASE {DBNAME};"]).returncode == 0
        for m in MIGRATIONS:
            r = _psql(cid, None, path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        _psql(cid, "DO $$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY['" +
              "','".join(LEGACY_RLS) + "'] LOOP EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY',t); END LOOP; END $$;")
        # runner role + its RLS
        assert _psql(cid, None, variables={"prospective_password": PROS_PW, "db_name": DBNAME},
                     path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner.sql")).returncode == 0
        assert _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner_rls_policies.sql")).returncode == 0
        # job-queue roles + RLS
        r = _psql(cid, None, variables={"worker_password": WORK_PW, "enqueuer_password": "e_pw",
                                        "audit_reader_password": AUD_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_job_queue_roles.sql"))
        assert r.returncode == 0, f"roles: {r.stderr[-500:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_job_queue_rls_policies.sql"))
        assert r.returncode == 0, f"rls: {r.stderr[-600:]}"

        # frozen universe + local history + lookahead trap
        today = datetime.now(timezone.utc).date()
        session = today - timedelta(days=1)
        yday = today - timedelta(days=1)
        _psql(cid, f"INSERT INTO history_warmup_universes(universe_code,universe_version,"
              f"universe_hash,config_hash,status,symbol_count,frozen_at) VALUES('ZZJOB',1,"
              f"'pending','cfg','draft',{len(SYMS)},NULL);")
        uid = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
                   "-c", "SELECT id FROM history_warmup_universes WHERE universe_code='ZZJOB';"]).stdout.strip()
        for i, s in enumerate(SYMS):
            _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{uid}','{s}',{i});")
        from app.history_warmup_execute import compute_universe_hash
        uhash = compute_universe_hash(universe_code="ZZJOB", universe_version=1, symbols_in_ordinal_order=SYMS)
        _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{uid}';")
        for s in SYMS:
            vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, yday))
            _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals};")
            fut = today + timedelta(days=3)
            _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES ('{s}','{fut}',999,1000,998,999.5,1);")
            f4 = _gen_4h(s, datetime.now(timezone.utc) - timedelta(hours=2))
            v4 = ",".join(f"('{r[0]}','{r[1].isoformat()}','{r[2].isoformat()}','{r[3]}',{r[4]},{r[5]},{r[6]},{r[7]},{r[8]},true,true,'sha256:fp{i}')" for i, r in enumerate(f4))
            _psql(cid, f"INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES {v4};")
            fs = datetime.now(timezone.utc) + timedelta(days=2)
            _psql(cid, f"INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES ('{s}','{fs.isoformat()}','{(fs+timedelta(hours=4)).isoformat()}','{fs.date()}',999,1000,998,999.5,1,true,true,'sha256:future');")

        # a 'registered' registration bound to the frozen universe
        session_cutoff = datetime.combine(session, datetime.min.time(), timezone.utc) + timedelta(hours=20)
        reg_ident = pc.registration_identity(experiment_code="wyckoff_v2_vs_baseline",
                                             universe_id=str(uid), universe_hash=uhash,
                                             history_config_hash="sha256:cfg",
                                             snapshot_session_date=session.isoformat())
        reg_id = str(uuid.uuid4())
        _psql(cid,
              "INSERT INTO prospective_campaign_registrations(id,experiment_code,experiment_contract_version,"
              "universe_id,universe_code,universe_version,universe_hash,history_config_hash,"
              "history_readiness_manifest_hash,candidate_strategy_code,candidate_strategy_version,"
              "candidate_signal_definition,candidate_allow_enter,control_strategy_code,control_strategy_version,"
              "snapshot_session_date,snapshot_cutoff_at,market_calendar_version,registration_identity,status) "
              f"VALUES('{reg_id}','wyckoff_v2_vs_baseline','wyckoff_v2_prospective_experiment.v1','{uid}','ZZJOB',1,"
              f"'{uhash}','sha256:cfg','sha256:manifest','wyckoff_mtf_v2','wyckoff_mtf.v2',"
              f"'pre_rollout_enter_eligible.v1',FALSE,'sma150_bounce','sma150.v2','{session.isoformat()}',"
              f"'{session_cutoff.isoformat()}','us_market_calendar.v1','{reg_ident}','registered');")

        base = f"127.0.0.1:{hp}/{DBNAME}?sslmode=disable"
        yield {"cid": cid, "hp": hp, "universe_id": uid, "universe_hash": uhash,
               "session": session.isoformat(), "reg_id": reg_id, "reg_ident": reg_ident,
               "su_dsn": f"postgresql://postgres:postgres@{base}",
               "runner_dsn": f"postgresql://{PROS}:{PROS_PW}@{base}",
               "worker_dsn": f"postgresql://{WORK}:{WORK_PW}@{base}",
               "audit_dsn": f"postgresql://{AUD}:{AUD_PW}@{base}"}
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(autouse=True)
def _no_external_network(monkeypatch):
    real = socket.socket.connect
    def guard(self, address):
        host = address[0] if isinstance(address, tuple) else str(address)
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"external network access blocked: {host}")
        return real(self, address)
    monkeypatch.setattr(socket.socket, "connect", guard)


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _uq(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def _seed_synthetic_job(conn, *, n_tasks, queue="test", task_type="synthetic_test_task.v1"):
    job_id = await conn.fetchval(
        "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status,"
        "total_task_count,queued_task_count) VALUES ('synthetic','synthetic.v1',$1,$2,'queued',$3,$3) "
        "RETURNING id", queue, f"job:{uuid.uuid4()}", n_tasks)
    for i in range(n_tasks):
        payload = {"mode": "succeed", "echo": i}
        await conn.execute(
            "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,ordinal,"
            "payload,payload_hash,idempotency_key,status,max_attempts) "
            "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',3)",
            job_id, queue, task_type, f"t{i}", i, json.dumps(payload),
            ident.payload_hash(payload), f"task:{uuid.uuid4()}")
    await Q.recompute_job_counters(conn, job_id)
    return job_id


# ============================ queue mechanics ==============================
class TestQueueMechanics:
    def test_migration_018_tables_and_template_present(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                tabs = await conn.fetch(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'job_%'")
                names = {r["tablename"] for r in tabs}
                assert {"job_runs", "job_tasks", "job_task_attempts", "job_events",
                        "job_workers", "job_schedules", "job_dependencies"} <= names
                tmpl = await conn.fetchrow(
                    "SELECT enabled, schedule_type FROM job_schedules WHERE schedule_code='SMART-SCANNER-DAILY-PIPELINE'")
                assert tmpl is not None and tmpl["enabled"] is False and tmpl["schedule_type"] == "market_daily"
            finally:
                await conn.close()
        _run(go())

    def test_bounded_payload_and_unique_idempotency(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await conn.fetchval(
                    "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status) "
                    "VALUES ('x','x.v1','test',$1,'queued') RETURNING id", f"job:{uuid.uuid4()}")
                big = json.dumps({"blob": "x" * 9000})
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,"
                        "ordinal,payload,payload_hash,idempotency_key,status) "
                        "VALUES ($1,'test','t.v1','t.v1','k',0,$2::jsonb,'h',$3,'queued')",
                        job, big, f"task:{uuid.uuid4()}")
                key = f"job:{uuid.uuid4()}"
                await conn.execute(
                    "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status) "
                    "VALUES ('x','x.v1','test',$1,'queued')", key)
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status) "
                        "VALUES ('x','x.v1','test',$1,'queued')", key)
            finally:
                await conn.close()
        _run(go())

    def test_skip_locked_no_double_claim(self, pg):
        async def go():
            q = _uq()
            setup = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await _seed_synthetic_job(setup, n_tasks=2, queue=q)
            finally:
                await setup.close()
            c1 = await asyncpg.connect(pg["su_dsn"])
            c2 = await asyncpg.connect(pg["su_dsn"])
            try:
                t1, t2 = await asyncio.gather(
                    Q.claim_next_task(c1, queue_name=q, worker_id="w1", lease_seconds=900),
                    Q.claim_next_task(c2, queue_name=q, worker_id="w2", lease_seconds=900))
                assert t1 and t2 and t1["id"] != t2["id"]  # two workers, two distinct tasks
                # a third claim finds nothing (only 2 tasks)
                t3 = await Q.claim_next_task(c1, queue_name=q, worker_id="w1", lease_seconds=900)
                assert t3 is None
            finally:
                await c1.close(); await c2.close()
            return job
        _run(go())

    def test_lease_renew_and_expiry_reconcile_to_retryable(self, pg):
        async def go():
            q = _uq()
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await _seed_synthetic_job(conn, n_tasks=1, queue=q)
                t = await Q.claim_next_task(conn, queue_name=q, worker_id="w1", lease_seconds=1)
                assert await Q.renew_lease(conn, task_id=t["id"], worker_id="w1", lease_seconds=900)
                assert not await Q.renew_lease(conn, task_id=t["id"], worker_id="someone_else", lease_seconds=900)
                # force expiry then reconcile
                await conn.execute("UPDATE job_tasks SET lease_expires_at=NOW()-interval '1 second' WHERE id=$1", t["id"])
                expired = await Q.find_expired_lease_tasks(conn, queue_name=q)
                assert any(str(e["id"]) == str(t["id"]) for e in expired)
                await Q.reconcile_task_to_retryable(conn, task_id=t["id"])
                row = await conn.fetchrow("SELECT status FROM job_tasks WHERE id=$1", t["id"])
                assert row["status"] == "retryable"
            finally:
                await conn.close()
        _run(go())

    def test_retry_backoff_then_terminal(self, pg):
        async def go():
            q = _uq()
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await _seed_synthetic_job(conn, n_tasks=1, queue=q)
                # attempt 1 → retryable w/ backoff 60
                t = await Q.claim_next_task(conn, queue_name=q, worker_id="w1", lease_seconds=900)
                await Q.settle_task_failure(conn, task_id=t["id"], worker_id="w1",
                                            safe_error_code="e", error_class=C.ERR_RETRYABLE,
                                            backoff_seconds_value=C.backoff_seconds(1))
                r = await conn.fetchrow("SELECT status, available_at, attempt_count FROM job_tasks WHERE id=$1", t["id"])
                assert r["status"] == "retryable" and r["available_at"] > datetime.now(timezone.utc)
                # make it available; attempt 2 → retryable; attempt 3 → failed (terminal)
                await conn.execute("UPDATE job_tasks SET available_at=NOW() WHERE id=$1", t["id"])
                t = await Q.claim_next_task(conn, queue_name=q, worker_id="w1", lease_seconds=900)
                await Q.settle_task_failure(conn, task_id=t["id"], worker_id="w1", safe_error_code="e",
                                            error_class=C.ERR_RETRYABLE, backoff_seconds_value=C.backoff_seconds(2))
                await conn.execute("UPDATE job_tasks SET available_at=NOW() WHERE id=$1", t["id"])
                t = await Q.claim_next_task(conn, queue_name=q, worker_id="w1", lease_seconds=900)
                await Q.settle_task_failure(conn, task_id=t["id"], worker_id="w1", safe_error_code="e",
                                            error_class=C.ERR_RETRYABLE, backoff_seconds_value=C.backoff_seconds(3))
                r = await conn.fetchrow("SELECT status, attempt_count FROM job_tasks WHERE id=$1", t["id"])
                assert r["status"] == "failed" and r["attempt_count"] == 3
                j = await conn.fetchrow("SELECT status, failed_task_count FROM job_runs WHERE id=$1", job)
                assert j["status"] == "failed" and j["failed_task_count"] == 1
            finally:
                await conn.close()
        _run(go())

    def test_cancellation_and_operator_retry(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await _seed_synthetic_job(conn, n_tasks=3)
                res = await Q.request_cancel(conn, job_id=job, requested_by="op")
                assert res["found"] and res["cancelled_pending"] == 3
                j = await conn.fetchrow("SELECT status, cancelled_task_count FROM job_runs WHERE id=$1", job)
                assert j["status"] == "cancelled" and j["cancelled_task_count"] == 3
                # operator retry only touches operator-eligible FAILED tasks (none here)
                res2 = await Q.retry_failed_tasks(conn, job_id=job)
                assert res2["retried"] == 0
            finally:
                await conn.close()
        _run(go())

    def test_25_task_completion_marks_job_succeeded(self, pg):
        async def go():
            q = _uq()
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job = await _seed_synthetic_job(conn, n_tasks=25, queue=q)
                for _ in range(25):
                    t = await Q.claim_next_task(conn, queue_name=q, worker_id="w1", lease_seconds=900)
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="w1",
                                                    result_summary={"ok": True})
                j = await conn.fetchrow("SELECT status, succeeded_task_count FROM job_runs WHERE id=$1", job)
                assert j["status"] == "succeeded" and j["succeeded_task_count"] == 25
            finally:
                await conn.close()
        _run(go())


# ======================= child-process crash detection =====================
class TestChildProcessCrash:
    def test_synthetic_crash_breaks_pool(self):
        from app.jobs.handlers.synthetic import run_synthetic_task
        ex = ProcessPoolExecutor(max_workers=1)
        try:
            fut = ex.submit(run_synthetic_task, {"mode": "crash"})
            with pytest.raises((BrokenExecutor, Exception)):
                fut.result(timeout=30)
        finally:
            ex.shutdown(wait=False, cancel_futures=True)

    def test_synthetic_success_and_failure_modes(self):
        from app.jobs.handlers.synthetic import run_synthetic_task
        ex = ProcessPoolExecutor(max_workers=1)
        try:
            assert ex.submit(run_synthetic_task, {"mode": "succeed", "echo": 5}).result(30)["ok"] is True
            assert ex.submit(run_synthetic_task, {"mode": "fail_terminal"}).result(30)["error_class"] == "terminal"
        finally:
            ex.shutdown(wait=False, cancel_futures=True)


# =========================== scheduler foundation ==========================
class TestScheduler:
    def test_leader_lock_is_exclusive(self, pg):
        async def go():
            from app.config import settings
            key = int(settings.JOB_SCHEDULER_ADVISORY_LOCK_KEY)
            c1 = await asyncpg.connect(pg["su_dsn"])
            c2 = await asyncpg.connect(pg["su_dsn"])
            try:
                assert await c1.fetchval("SELECT pg_try_advisory_lock($1)", key) is True
                assert await c2.fetchval("SELECT pg_try_advisory_lock($1)", key) is False
                await c1.fetchval("SELECT pg_advisory_unlock($1)", key)
            finally:
                await c1.close(); await c2.close()
        _run(go())

    def test_disabled_and_paused_schedules_are_not_due(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                # the seeded daily template is disabled → never selected
                due = await conn.fetch(
                    "SELECT * FROM job_schedules WHERE enabled=TRUE AND paused=FALSE "
                    "AND (next_run_at IS NULL OR next_run_at <= NOW())")
                codes = {r["schedule_code"] for r in due}
                assert "SMART-SCANNER-DAILY-PIPELINE" not in codes
            finally:
                await conn.close()
        _run(go())

    def test_duplicate_occurrence_prevented(self, pg):
        async def go():
            from app.jobs.scheduler import _create_scheduled_job
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                sid = await conn.fetchval(
                    "INSERT INTO job_schedules (schedule_code,schedule_version,schedule_type,timezone,"
                    "cron_expression,job_type,job_contract_version,enabled) "
                    "VALUES ('T-CRON',1,'cron','America/New_York','30 14 * * *','synthetic','synthetic.v1',TRUE) "
                    "RETURNING id")
                sched = dict(await conn.fetchrow("SELECT * FROM job_schedules WHERE id=$1", sid))
                occ = datetime(2026, 7, 29, 18, 30, tzinfo=timezone.utc)
                j1 = await _create_scheduled_job(conn, sched, occ)
                j2 = await _create_scheduled_job(conn, sched, occ)  # same occurrence → deduped
                assert j1 is not None and j2 is None
            finally:
                await conn.close()
        _run(go())

    def test_market_daily_preview_uses_completed_session(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                sid = await conn.fetchval(
                    "INSERT INTO job_schedules (schedule_code,schedule_version,schedule_type,timezone,"
                    "market_close_delay_minutes,job_type,job_contract_version,enabled) "
                    "VALUES ('T-MKT',1,'market_daily','America/New_York',30,'synthetic','synthetic.v1',FALSE) "
                    "RETURNING id")
                from app.jobs.scheduler import preview_occurrences
                from app.prospective_session import is_trading_day
                from zoneinfo import ZoneInfo
                sched = dict(await conn.fetchrow("SELECT * FROM job_schedules WHERE id=$1", sid))
                occ = preview_occurrences(sched, datetime.now(timezone.utc), count=3)
                assert len(occ) == 3
                for iso in occ:
                    d = datetime.fromisoformat(iso).astimezone(ZoneInfo("America/New_York")).date()
                    assert is_trading_day(d)
            finally:
                await conn.close()
        _run(go())


# ======================= worker heartbeat / RLS ============================
class TestWorkerAndRls:
    def test_worker_heartbeat_and_stale(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                wid = f"prospective-test-{uuid.uuid4().hex[:8]}"
                await conn.execute(
                    "INSERT INTO job_workers (worker_id,worker_type,queue_names,status) "
                    "VALUES ($1,'prospective',ARRAY['prospective'],'idle')", wid)
                await Q.worker_heartbeat(conn, worker_id=wid, status="busy")
                fresh = await conn.fetchrow(
                    "SELECT (NOW()-last_heartbeat_at) < interval '5 seconds' AS ok, status FROM job_workers WHERE worker_id=$1", wid)
                assert fresh["ok"] and fresh["status"] == "busy"
                await conn.execute("UPDATE job_workers SET last_heartbeat_at=NOW()-interval '200 seconds' WHERE worker_id=$1", wid)
                stale = await conn.fetchval(
                    "SELECT (NOW()-last_heartbeat_at) > interval '90 seconds' FROM job_workers WHERE worker_id=$1", wid)
                assert stale is True
            finally:
                await conn.close()
        _run(go())

    def test_rls_worker_cannot_write_bars_or_outcomes(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["worker_dsn"])
            try:
                cur = await conn.fetchval("SELECT current_user")
                assert cur == WORK
                # can claim/select queue
                await conn.fetch("SELECT id FROM job_tasks LIMIT 1")
                # cannot write bars / outcomes
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                                       "VALUES ('XX','2020-01-01',1,1,1,1,1)")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("INSERT INTO strategy_shadow_pair_outcomes(pair_id,outcome_status) "
                                       "VALUES (gen_random_uuid(),'x')")
            finally:
                await conn.close()
        _run(go())

    def test_rls_audit_reader_is_read_only(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["audit_dsn"])
            try:
                assert await conn.fetchval("SELECT current_user") == AUD
                await conn.fetch("SELECT id FROM job_runs LIMIT 1")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status) "
                        "VALUES ('x','x.v1','test',$1,'queued')", f"job:{uuid.uuid4()}")
            finally:
                await conn.close()
        _run(go())


# =================== prospective enqueue + worker handler ==================
class TestProspectiveQueue:
    def test_enqueue_replay_process_and_reconcile(self, pg, monkeypatch):
        from app.config import settings
        import app.deps as deps
        from app.jobs.prospective_enqueue import enqueue_prospective_campaign, sync_prospective_campaign
        from app.jobs.handlers.prospective import evaluate_prospective_symbol, probe_prospective_durable_output

        async def go():
            # ---- enqueue as the RUNNER role (RLS-scoped) ----
            runner = await asyncpg.connect(pg["runner_dsn"])
            try:
                r1 = await enqueue_prospective_campaign(runner, registration_id=pg["reg_id"],
                                                        registration_identity=pg["reg_ident"],
                                                        requested_by="test")
                assert r1["status"] == "queued" and r1["total_task_count"] == 2
                job_id = r1["job_id"]
                # exact replay → already_queued, same job, no new tasks
                r2 = await enqueue_prospective_campaign(runner, registration_id=pg["reg_id"],
                                                        registration_identity=pg["reg_ident"],
                                                        requested_by="test")
                assert r2["status"] == "already_queued" and r2["job_id"] == job_id
                tasks = await runner.fetch(
                    "SELECT payload, ordinal FROM job_tasks WHERE job_id=$1 ORDER BY ordinal", job_id)
                assert len(tasks) == 2
            finally:
                await runner.close()

            # ---- process each task as the WORKER role via the global pool ----
            monkeypatch.setattr(settings, "JOB_WORKER_ENABLED", True, raising=False)
            monkeypatch.setattr(settings, "JOB_WORKER_DATABASE_URL", pg["worker_dsn"], raising=False)
            monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", False)
            monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
            monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", False)
            monkeypatch.setattr(settings, "HISTORY_WARMUP_ONLY_MODE", False)
            await deps.close_db_pool()
            await deps.init_db_pool()
            from app.workers.persistence import get_db_connection, release_db_connection
            try:
                for _ in range(2):
                    conn = await get_db_connection()
                    try:
                        t = await Q.claim_next_task(conn, queue_name="prospective", worker_id="wtest", lease_seconds=900)
                        assert t is not None
                        payload = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                    finally:
                        await release_db_connection(conn)
                    res = await evaluate_prospective_symbol(payload)
                    assert res["ok"] is True, res
                    conn = await get_db_connection()
                    try:
                        await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="wtest",
                                                        result_summary=res["result"])
                        await sync_prospective_campaign(conn, t["job_id"])
                    finally:
                        await release_db_connection(conn)

                # ---- verify durable output: 2 pairs, 4 evals, 0 outcomes ----
                conn = await get_db_connection()
                try:
                    reg = await conn.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", pg["reg_id"])
                    assert reg["status"] == "completed"
                    run_id = reg["campaign_run_id"]
                    pairs = await conn.fetchval(
                        "SELECT count(DISTINCT p.id) FROM strategy_shadow_run_pairs rp "
                        "JOIN strategy_shadow_pairs p ON p.id=rp.pair_id WHERE rp.run_id=$1", run_id)
                    cand = await conn.fetchval(
                        "SELECT count(*) FROM strategy_shadow_run_pairs rp JOIN strategy_shadow_evaluations e "
                        "ON e.pair_id=rp.pair_id WHERE rp.run_id=$1 AND e.arm_code=$2", run_id, pc.CANDIDATE_ARM_CODE)
                    ctrl = await conn.fetchval(
                        "SELECT count(*) FROM strategy_shadow_run_pairs rp JOIN strategy_shadow_evaluations e "
                        "ON e.pair_id=rp.pair_id WHERE rp.run_id=$1 AND e.arm_code=$2", run_id, pc.CONTROL_ARM_CODE)
                    outcomes = await conn.fetchval("SELECT count(*) FROM strategy_shadow_pair_outcomes")
                    assert pairs == 2 and cand == 2 and ctrl == 2 and outcomes == 0
                    job = await conn.fetchrow("SELECT status, succeeded_task_count FROM job_runs WHERE registration_id=$1", pg["reg_id"])
                    assert job["status"] == "succeeded" and job["succeeded_task_count"] == 2
                    # future-bar exclusion: no pair frame extends into the future
                    fut = await conn.fetchval(
                        "SELECT count(*) FROM strategy_shadow_pairs WHERE frame_last_date > $1",
                        date.fromisoformat(pg["session"]))
                    assert fut == 0
                finally:
                    await release_db_connection(conn)

                # ---- probe reconcile: build a payload and confirm complete ----
                conn = await get_db_connection()
                try:
                    trow = await conn.fetchrow("SELECT payload FROM job_tasks WHERE queue_name='prospective' ORDER BY ordinal LIMIT 1")
                    payload = json.loads(trow["payload"]) if isinstance(trow["payload"], str) else trow["payload"]
                    probed = await probe_prospective_durable_output(conn, payload)
                    assert probed is not None and probed["reconciled"] is True
                    assert probed["candidate"]["arm"] == pc.CANDIDATE_ARM_CODE
                finally:
                    await release_db_connection(conn)
            finally:
                await deps.close_db_pool()

        _run(go())


class TestDailyPipelineOrchestrator:
    """smart_scanner_daily_pipeline.v1 — occurrence identity, idempotent
    create-or-resume, stage advancement, and the operator status view. Uses
    the same job_runs table as everything else (no second queue system)."""

    def test_ensure_occurrence_is_idempotent_and_resumable(self, pg):
        from app.jobs.daily_pipeline import (ensure_pipeline_occurrence, get_pipeline_occurrence,
                                             latest_pipeline_occurrence, record_stage_result,
                                             build_status_view, current_stage,
                                             STAGE_HISTORY_REFRESH, STAGE_PROSPECTIVE_CAMPAIGN,
                                             STAGE_OUTCOME_MATURATION, STAGE_AUDIT_REPORT,
                                             STAGE_DONE, STAGE_STATE_COMPLETED,
                                             STAGE_STATE_IN_PROGRESS, STAGE_STATE_TERMINAL_FAILURE)

        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                occ1 = await ensure_pipeline_occurrence(
                    conn, schedule_code="SMART-SCANNER-DAILY-PIPELINE", schedule_version=1,
                    resolved_session_date="2026-08-03", frozen_universe_hash=pg["universe_hash"],
                    universe_id=pg["universe_id"])
                assert current_stage(occ1) == STAGE_HISTORY_REFRESH

                # a second ensure-call for the SAME identity resumes the SAME row
                occ2 = await ensure_pipeline_occurrence(
                    conn, schedule_code="SMART-SCANNER-DAILY-PIPELINE", schedule_version=1,
                    resolved_session_date="2026-08-03", frozen_universe_hash=pg["universe_hash"],
                    universe_id=pg["universe_id"])
                assert occ1["id"] == occ2["id"]
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE job_type='smart_scanner_daily_pipeline'") == 1

                # advance stage 1 -> completed; occurrence should move to stage 2
                occ = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_HISTORY_REFRESH,
                                                result={"state": STAGE_STATE_COMPLETED,
                                                        "symbols_refreshed": 25})
                assert current_stage(occ) == STAGE_PROSPECTIVE_CAMPAIGN

                # in-progress write on the CURRENT stage keeps it in place (resumable)
                occ = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_PROSPECTIVE_CAMPAIGN,
                                                result={"state": STAGE_STATE_IN_PROGRESS,
                                                        "campaign_registration_id": "reg-1",
                                                        "campaign_job_id": "job-1"})
                assert current_stage(occ) == STAGE_PROSPECTIVE_CAMPAIGN
                assert occ["status"] == "running"
                view = build_status_view(occ)
                assert view["campaign_registration_id"] == "reg-1"
                assert view["campaign_job_id"] == "job-1"

                # a stale write against an already-passed stage is a no-op (never
                # rewinds or corrupts a later stage's state)
                stale = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_HISTORY_REFRESH,
                                                  result={"state": STAGE_STATE_COMPLETED})
                assert current_stage(stale) == STAGE_PROSPECTIVE_CAMPAIGN

                # complete remaining stages -> occurrence succeeds
                occ = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_PROSPECTIVE_CAMPAIGN,
                                                result={"state": STAGE_STATE_COMPLETED})
                occ = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_OUTCOME_MATURATION,
                                                result={"state": STAGE_STATE_COMPLETED})
                occ = await record_stage_result(conn, str(occ1["id"]), stage=STAGE_AUDIT_REPORT,
                                                result={"state": STAGE_STATE_COMPLETED})
                assert current_stage(occ) == STAGE_DONE
                assert occ["status"] == "succeeded"
                assert occ["finished_at"] is not None
                view = build_status_view(occ)
                assert view["completed_stages"] == [STAGE_HISTORY_REFRESH, STAGE_PROSPECTIVE_CAMPAIGN,
                                                    STAGE_OUTCOME_MATURATION, STAGE_AUDIT_REPORT]
                assert view["terminal_failure_stage"] is None

                got = await get_pipeline_occurrence(conn, str(occ1["id"]))
                assert got["id"] == occ1["id"]
                latest = await latest_pipeline_occurrence(conn)
                assert latest["id"] == occ1["id"]

                # a DIFFERENT resolved session is a genuinely new occurrence
                occ3 = await ensure_pipeline_occurrence(
                    conn, schedule_code="SMART-SCANNER-DAILY-PIPELINE", schedule_version=1,
                    resolved_session_date="2026-08-04", frozen_universe_hash=pg["universe_hash"],
                    universe_id=pg["universe_id"])
                assert occ3["id"] != occ1["id"]
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE job_type='smart_scanner_daily_pipeline'") == 2
            finally:
                await conn.close()

        _run(go())

    def test_terminal_stage_failure_halts_and_never_advances(self, pg):
        from app.jobs.daily_pipeline import (ensure_pipeline_occurrence, record_stage_result,
                                             current_stage, build_status_view,
                                             STAGE_HISTORY_REFRESH, STAGE_PROSPECTIVE_CAMPAIGN,
                                             STAGE_STATE_TERMINAL_FAILURE)

        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                occ = await ensure_pipeline_occurrence(
                    conn, schedule_code="SMART-SCANNER-DAILY-PIPELINE", schedule_version=1,
                    resolved_session_date="2026-08-05", frozen_universe_hash=pg["universe_hash"],
                    universe_id=pg["universe_id"])
                occ = await record_stage_result(conn, str(occ["id"]), stage=STAGE_HISTORY_REFRESH,
                                                result={"state": STAGE_STATE_TERMINAL_FAILURE,
                                                        "reason": "provider_rate_limited_permanently"})
                assert occ["status"] == "failed"
                assert current_stage(occ) == STAGE_HISTORY_REFRESH  # never silently advances
                view = build_status_view(occ)
                assert view["terminal_failure_stage"] == STAGE_HISTORY_REFRESH
                assert view["blocked_stage"] is None
                assert view["completed_stages"] == []
            finally:
                await conn.close()

        _run(go())
