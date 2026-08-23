"""Regression for the history-refresh worker role contract + RLS convergence.

The smart_scanner_history_warmer role is REUSED by the history-warmup HTTP app
and the new durable history-refresh worker. This proves:

  * verify_history_refresh_worker.sql passes on the correctly-provisioned role;
  * behaviorally under SET ROLE the warmer may INSERT/SELECT a
    history_incremental_refresh queue row and is RLS-DENIED prospective /
    daily_pipeline / arbitrary queues (rolled back — no persistent rows);
  * a stale/wrong warmer qscope predicate FAILS the verifier;
    create_history_refresh_worker_grants.sql CONVERGES it; the verifier then
    passes; re-running the grants is idempotent.
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest

PG_IMAGE = "postgres:16-alpine"
DBNAME = "hrwrls"
WARMER_PW = "warmerpw_local_only"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS = ["001_initial_schema", "002_phase1_sma150_config", "003_phase2_signal_outcomes",
              "004_phase5_wyckoff_mtf_config", "005_massive_provider", "006_market_data_jobs",
              "007_scan_signal_provenance", "008_sma150_v3", "009_watch_outcome_coverage",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes", "017_prospective_campaign_registration",
              "018_durable_job_queue"]
WARMER = "smart_scanner_history_warmer"


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE], capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")


def _sh(a, inp=None, t=120):
    return subprocess.run(a, input=inp, capture_output=True, text=True, timeout=t)


def _exec(cid, sql):
    return _sh(["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DBNAME, "-c", sql])


def _query(cid, sql):
    return _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME, "-c", sql]).stdout.strip()


def _apply_file(cid, rel, variables=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    args += ["-f", "-"]
    with open(os.path.join(REPO, rel)) as fh:
        return _sh(args, inp=fh.read())


def _qscope_queues(cid, table):
    return _query(cid,
        "SELECT string_agg(q, ',' ORDER BY q) FROM (SELECT DISTINCT "
        "(regexp_matches(qual, '''([^'']+)''','g'))[1] q FROM pg_policies "
        f"WHERE schemaname='public' AND tablename='{table}' "
        f"AND policyname='{WARMER}_qscope') s;")


def _set_stale_warmer_policy(cid, wrong_queue="prospective"):
    for tbl in ("job_runs", "job_tasks"):
        _exec(cid, f"DROP POLICY IF EXISTS {WARMER}_qscope ON public.{tbl};")
        r = _exec(cid,
            f"CREATE POLICY {WARMER}_qscope ON public.{tbl} AS PERMISSIVE FOR ALL TO {WARMER} "
            f"USING (queue_name IN ('{wrong_queue}')) WITH CHECK (queue_name IN ('{wrong_queue}'));")
        assert r.returncode == 0, r.stderr


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres", "-P", PG_IMAGE]).stdout.strip()
    assert cid
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready", "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c", f"CREATE DATABASE {DBNAME};"]).returncode == 0
        for m in MIGRATIONS:
            with open(os.path.join(REPO, "app", "db", "migrations", f"{m}.sql")) as fh:
                r = _sh(["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
                         "-d", DBNAME, "-f", "-"], inp=fh.read())
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        r = _apply_file(cid, "ops/sql/create_shadow_history_warmer.sql",
                        variables={"warmer_password": WARMER_PW, "db_name": DBNAME})
        assert r.returncode == 0, f"warmer role: {r.stderr[-500:]}"
        r = _apply_file(cid, "ops/sql/create_shadow_history_warmer_rls_policies.sql")
        assert r.returncode == 0, f"warmer rls: {r.stderr[-500:]}"
        r = _apply_file(cid, "ops/sql/create_history_refresh_worker_grants.sql")
        assert r.returncode == 0, f"worker grants: {r.stderr[-500:]}"
        yield {"cid": cid}
    finally:
        _sh(["docker", "stop", cid])


class TestHistoryRefreshWorkerRls:
    def test_verifier_passes_on_correct_role(self, pg):
        v = _apply_file(pg["cid"], "ops/sql/verify_history_refresh_worker.sql")
        assert v.returncode == 0, v.stderr[-600:]
        assert _qscope_queues(pg["cid"], "job_runs") == "history_incremental_refresh"
        assert _qscope_queues(pg["cid"], "job_tasks") == "history_incremental_refresh"

    def test_behavioral_queue_isolation_under_set_role(self, pg):
        cid = pg["cid"]
        _apply_file(cid, "ops/sql/create_history_refresh_worker_grants.sql")  # ensure correct scope
        # allowed queue → INSERT then SELECT-back succeed under the warmer role
        # (rolled back). The SELECT-back proves the USING clause is visible too.
        allowed = _exec(cid,
            f"BEGIN; SET ROLE {WARMER}; "
            "INSERT INTO job_runs(job_type,job_contract_version,queue_name,idempotency_key,status) "
            "VALUES ('t','t.v1','history_incremental_refresh','hrw-probe-allow','queued'); "
            "SELECT count(*) FROM job_runs WHERE idempotency_key='hrw-probe-allow'; "
            "RESET ROLE; ROLLBACK;")
        assert allowed.returncode == 0, allowed.stderr[-400:]
        # forbidden queues → RLS-denied
        for q in ("prospective", "daily_pipeline", "__forbidden_queue__"):
            denied = _exec(cid,
                f"BEGIN; SET ROLE {WARMER}; "
                "INSERT INTO job_runs(job_type,job_contract_version,queue_name,idempotency_key,status) "
                f"VALUES ('t','t.v1','{q}','hrw-probe-deny','queued'); RESET ROLE; ROLLBACK;")
            assert denied.returncode != 0, f"{q} INSERT must be RLS-denied"
            assert "row-level security" in (denied.stderr + denied.stdout).lower()
        # no probe rows persisted
        assert _query(cid, "SELECT count(*) FROM job_runs WHERE idempotency_key LIKE 'hrw-probe-%';") == "0"

    def test_stale_policy_fails_verifier_then_grants_converge(self, pg):
        cid = pg["cid"]
        # simulate a wrong/stale warmer qscope predicate
        _set_stale_warmer_policy(cid, wrong_queue="prospective")
        assert _qscope_queues(cid, "job_runs") == "prospective"
        v_before = _apply_file(cid, "ops/sql/verify_history_refresh_worker.sql")
        assert v_before.returncode != 0, "verifier must reject a stale warmer qscope"
        assert "exactly" in (v_before.stderr + v_before.stdout)

        # grants script converges the policy back to history_incremental_refresh
        r = _apply_file(cid, "ops/sql/create_history_refresh_worker_grants.sql")
        assert r.returncode == 0, r.stderr[-500:]
        assert _qscope_queues(cid, "job_runs") == "history_incremental_refresh"
        assert _qscope_queues(cid, "job_tasks") == "history_incremental_refresh"

        v_after = _apply_file(cid, "ops/sql/verify_history_refresh_worker.sql")
        assert v_after.returncode == 0, v_after.stderr[-600:]

        # idempotent re-run
        r2 = _apply_file(cid, "ops/sql/create_history_refresh_worker_grants.sql")
        assert r2.returncode == 0, r2.stderr[-500:]
        assert _apply_file(cid, "ops/sql/verify_history_refresh_worker.sql").returncode == 0
