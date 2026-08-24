"""Durable successor recovery for a failed history-refresh job (Root Cause #2 pt2).

Real Docker Postgres (migrations 001-018). Seeds a FAILED generation-0 history
job matching the live shape (some symbols succeeded, the rest retry-exhausted
`retryable`), then drives ``enqueue_history_incremental_refresh`` and proves the
bounded successor model:

  C. a distinct generation-1 successor is created for ONLY the not-succeeded
     symbols; the predecessor job/task/attempt rows are never mutated;
  D. replay/concurrency returns the SAME successor (no duplicate);
  E. a non-recoverable (terminal/operator) failed task blocks auto-recovery;
  F. the recovery generation is capped (a failed successor is not re-recovered).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid

import pytest

asyncpg = pytest.importorskip("asyncpg")

from app.jobs import history_refresh as HR
from app.jobs import identity as ident

PG_IMAGE = "postgres:16-alpine"
DBNAME = "hrrec"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS = ["001_initial_schema", "002_phase1_sma150_config", "003_phase2_signal_outcomes",
              "004_phase5_wyckoff_mtf_config", "005_massive_provider", "006_market_data_jobs",
              "007_scan_signal_provenance", "008_sma150_v3", "009_watch_outcome_coverage",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes", "017_prospective_campaign_registration",
              "018_durable_job_queue"]
CONTRACT = HR.HISTORY_REFRESH_CONTRACT_VERSION_V2


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE], capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")


def _sh(a, inp=None, t=120):
    return subprocess.run(a, input=inp, capture_output=True, text=True, timeout=t)


@pytest.fixture(scope="module")
def dsn():
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
            with open(os.path.join(REPO, "app", "db", "migrations", f"{m}.sql")) as fh:
                r = _sh(["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
                         "-d", DBNAME, "-f", "-"], inp=fh.read())
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        yield f"postgresql://postgres:postgres@127.0.0.1:{hp}/{DBNAME}"
    finally:
        _sh(["docker", "stop", cid])


async def _seed_failed_job(conn, *, uhash, session, symbols, succeeded, fail_class="retryable",
                           generation=0):
    """Insert a FAILED history-refresh job (generation) with one task per symbol;
    `succeeded` symbols succeed, the rest fail with `fail_class`, attempt=3."""
    jkey = HR._job_key(uhash, session, CONTRACT, generation)
    job_id = await conn.fetchval(
        "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status,"
        "total_task_count,queued_task_count,result_summary) "
        "VALUES ($1,$2,$3,$4,'failed',$5,0,$6::jsonb) RETURNING id",
        HR.HISTORY_REFRESH_JOB_TYPE, HR.HISTORY_REFRESH_JOB_CONTRACT, HR.HISTORY_REFRESH_QUEUE,
        jkey, len(symbols), json.dumps({"recovery_generation": generation}))
    for i, s in enumerate(symbols):
        ok = s in succeeded
        tkey = ident.history_refresh_task_idempotency_key(
            universe_hash=uhash, resolved_session_date=session, symbol=s,
            contract_version=CONTRACT, recovery_generation=generation)
        await conn.execute(
            "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,ordinal,"
            "payload,payload_hash,idempotency_key,status,attempt_count,max_attempts,error_class,safe_error_code) "
            "VALUES ($1,$2,$3,$3,$4,$5,'{}'::jsonb,$6,$7,$8,$9,3,$10,$11)",
            job_id, HR.HISTORY_REFRESH_QUEUE, HR.HISTORY_REFRESH_TASK, s, i,
            f"h{i}", tkey, ("succeeded" if ok else "failed"),
            (1 if ok else 3), (None if ok else fail_class),
            (None if ok else "history_refresh_http_409"))
    await conn.execute("UPDATE job_runs SET succeeded_task_count=$2, failed_task_count=$3 WHERE id=$1",
                       job_id, len(succeeded), len(symbols) - len(succeeded))
    return str(job_id), jkey


def _run(coro):
    return asyncio.run(coro)


class TestHistoryRefreshRecovery:
    def test_C_successor_generation_1_created_for_remaining_symbols(self, dsn):
        async def go():
            conn = await asyncpg.connect(dsn)
            try:
                uhash, session = "UHC", "2026-08-20"
                syms = ["S1", "S2", "S3", "S4", "S5"]
                pred_id, pred_key = await _seed_failed_job(
                    conn, uhash=uhash, session=session, symbols=syms, succeeded={"S1", "S2"})
                pred_attempts_before = await conn.fetch(
                    "SELECT id, status, attempt_count, error_class FROM job_tasks WHERE job_id=$1 ORDER BY ordinal",
                    pred_id)

                enq = await HR.enqueue_history_incremental_refresh(
                    conn, universe_id="u", universe_hash=uhash, symbols=syms,
                    resolved_session_date=session, requested_by="test")

                assert enq["recoverable"] is True and enq["status"] == "queued"
                assert enq["recovery_generation"] == 1
                assert enq["predecessor_history_job_id"] == pred_id
                succ_id = enq["job_id"]
                assert succ_id != pred_id
                # DISTINCT successor job key (generation-scoped)
                succ_key = await conn.fetchval("SELECT idempotency_key FROM job_runs WHERE id=$1", succ_id)
                assert succ_key == HR._job_key(uhash, session, CONTRACT, 1) != pred_key
                # successor tasks: ONLY the not-succeeded symbols, DISTINCT keys
                succ_tasks = await conn.fetch(
                    "SELECT task_key, idempotency_key FROM job_tasks WHERE job_id=$1 ORDER BY ordinal", succ_id)
                assert sorted(t["task_key"] for t in succ_tasks) == ["S3", "S4", "S5"]
                for t in succ_tasks:
                    assert t["idempotency_key"] == ident.history_refresh_task_idempotency_key(
                        universe_hash=uhash, resolved_session_date=session, symbol=t["task_key"],
                        contract_version=CONTRACT, recovery_generation=1)
                    # never collides with a predecessor task key
                    assert t["idempotency_key"] != ident.history_refresh_task_idempotency_key(
                        universe_hash=uhash, resolved_session_date=session, symbol=t["task_key"],
                        contract_version=CONTRACT, recovery_generation=0)
                # lineage recorded on the successor
                summ = await conn.fetchval("SELECT result_summary FROM job_runs WHERE id=$1", succ_id)
                summ = json.loads(summ) if isinstance(summ, str) else summ
                assert summ["recovery_generation"] == 1 and summ["predecessor_history_job_id"] == pred_id

                # PREDECESSOR EVIDENCE IMMUTABLE: still failed, same tasks/attempts
                pred_after = await conn.fetchrow("SELECT status FROM job_runs WHERE id=$1", pred_id)
                assert pred_after["status"] == "failed"
                pred_attempts_after = await conn.fetch(
                    "SELECT id, status, attempt_count, error_class FROM job_tasks WHERE job_id=$1 ORDER BY ordinal",
                    pred_id)
                assert [dict(r) for r in pred_attempts_after] == [dict(r) for r in pred_attempts_before]
            finally:
                await conn.close()
        _run(go())

    def test_D_replay_returns_same_successor_no_duplicate(self, dsn):
        async def go():
            conn = await asyncpg.connect(dsn)
            try:
                uhash, session = "UHD", "2026-08-20"
                syms = ["S1", "S2", "S3"]
                await _seed_failed_job(conn, uhash=uhash, session=session, symbols=syms, succeeded={"S1"})
                a = await HR.enqueue_history_incremental_refresh(
                    conn, universe_id="u", universe_hash=uhash, symbols=syms,
                    resolved_session_date=session, requested_by="test")
                b = await HR.enqueue_history_incremental_refresh(
                    conn, universe_id="u", universe_hash=uhash, symbols=syms,
                    resolved_session_date=session, requested_by="test")
                assert a["job_id"] == b["job_id"]
                assert a["status"] == "queued" and b["status"] == "already_queued"
                # exactly ONE generation-1 job for this logical identity
                n = await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE idempotency_key=$1",
                    HR._job_key(uhash, session, CONTRACT, 1))
                assert n == 1
                # successor tasks not duplicated
                nt = await conn.fetchval("SELECT count(*) FROM job_tasks WHERE job_id=$1", a["job_id"])
                assert nt == 2  # S2, S3
            finally:
                await conn.close()
        _run(go())

    def test_E_non_recoverable_failure_blocks_successor(self, dsn):
        async def go():
            conn = await asyncpg.connect(dsn)
            try:
                uhash, session = "UHE", "2026-08-20"
                syms = ["S1", "S2", "S3"]
                pred_id, _ = await _seed_failed_job(
                    conn, uhash=uhash, session=session, symbols=syms, succeeded={"S1"},
                    fail_class="terminal")   # a terminal/operator failure is NOT auto-recovered
                enq = await HR.enqueue_history_incremental_refresh(
                    conn, universe_id="u", universe_hash=uhash, symbols=syms,
                    resolved_session_date=session, requested_by="test")
                assert enq["recoverable"] is False and enq["status"] == "not_recoverable"
                assert enq["job_id"] == pred_id            # points at the predecessor, no successor
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE idempotency_key=$1",
                    HR._job_key(uhash, session, CONTRACT, 1)) == 0
            finally:
                await conn.close()
        _run(go())

    def test_F_recovery_generation_capped_at_one(self, dsn):
        async def go():
            conn = await asyncpg.connect(dsn)
            try:
                uhash, session = "UHF", "2026-08-20"
                syms = ["S1", "S2", "S3"]
                # gen0 failed → gen1 successor, then gen1 ALSO failed
                await _seed_failed_job(conn, uhash=uhash, session=session, symbols=syms, succeeded={"S1"})
                await _seed_failed_job(conn, uhash=uhash, session=session, symbols=["S2", "S3"],
                                       succeeded=set(), generation=1)
                enq = await HR.enqueue_history_incremental_refresh(
                    conn, universe_id="u", universe_hash=uhash, symbols=syms,
                    resolved_session_date=session, requested_by="test")
                assert enq["recoverable"] is False and enq["status"] == "recovery_exhausted"
                assert enq["recovery_generation"] == 1
                # NO generation-2 job created
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE idempotency_key=$1",
                    HR._job_key(uhash, session, CONTRACT, 2)) == 0
            finally:
                await conn.close()
        _run(go())
