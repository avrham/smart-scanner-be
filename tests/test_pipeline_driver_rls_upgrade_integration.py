"""Regression for the pipeline-driver RLS live-UPGRADE defect.

The qscope policy was created CREATE-if-missing, so a live DB carrying the OLD
four-queue predicate would NOT converge to the new five-queue predicate on
re-run — leaving the driver RLS-blocked from the new 'history_incremental_refresh'
queue while the verifier still reported green. This proves:

  * a fresh apply yields exactly the five intended queues (incl history refresh);
  * simulating the OLD four-queue policy then re-running the script CONVERGES it;
  * the script is idempotent;
  * verify_pipeline_driver.sql FAILS on the stale four-queue policy and PASSES
    after the upgrade (effective queue-scope check, not just grants);
  * behaviorally under SET ROLE the driver may INSERT a history_incremental_refresh
    row and is DENIED a forbidden queue (rolled back — no persistent rows).
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest

PG_IMAGE = "postgres:16-alpine"
DBNAME = "drvrls"
DRV_PW = "drvpw_local_only"
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS = ["001_initial_schema", "002_phase1_sma150_config", "003_phase2_signal_outcomes",
              "004_phase5_wyckoff_mtf_config", "005_massive_provider", "006_market_data_jobs",
              "007_scan_signal_provenance", "008_sma150_v3", "009_watch_outcome_coverage",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes", "017_prospective_campaign_registration",
              "018_durable_job_queue"]
EXPECTED_FIVE = "daily_pipeline,daily_pipeline_driver,history_incremental_refresh,prospective,prospective_outcomes"
OLD_FOUR = ("daily_pipeline_driver", "daily_pipeline", "prospective", "prospective_outcomes")


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
        "AND policyname='smart_scanner_pipeline_driver_qscope') s;")


def _set_old_four_queue_policy(cid):
    """Recreate the qscope policy with the OLD four-queue predicate on both
    tables, reproducing the live pre-upgrade state."""
    qlist = "(" + ",".join(f"'{q}'" for q in OLD_FOUR) + ")"
    for tbl in ("job_runs", "job_tasks"):
        _exec(cid, f"DROP POLICY IF EXISTS smart_scanner_pipeline_driver_qscope ON public.{tbl};")
        r = _exec(cid,
            f"CREATE POLICY smart_scanner_pipeline_driver_qscope ON public.{tbl} "
            f"AS PERMISSIVE FOR ALL TO smart_scanner_pipeline_driver "
            f"USING (queue_name IN {qlist}) WITH CHECK (queue_name IN {qlist});")
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
        # the least-privilege driver role (grants + role flags)
        r = _apply_file(cid, "ops/sql/create_pipeline_driver.sql",
                        variables={"pipeline_driver_password": DRV_PW, "db_name": DBNAME})
        assert r.returncode == 0, f"role: {r.stderr[-500:]}"
        yield {"cid": cid}
    finally:
        _sh(["docker", "stop", cid])


class TestPipelineDriverRlsUpgrade:
    def test_fresh_apply_has_exactly_five_queues(self, pg):
        cid = pg["cid"]
        r = _apply_file(cid, "ops/sql/create_pipeline_driver_rls_policies.sql")
        assert r.returncode == 0, r.stderr[-500:]
        for tbl in ("job_runs", "job_tasks"):
            assert _qscope_queues(cid, tbl) == EXPECTED_FIVE, (tbl, _qscope_queues(cid, tbl))

    def test_old_four_queue_policy_upgrades_and_is_idempotent(self, pg):
        cid = pg["cid"]
        # reproduce the live OLD state: a pre-existing four-queue qscope policy
        _set_old_four_queue_policy(cid)
        assert "history_incremental_refresh" not in _qscope_queues(cid, "job_runs")

        # the strengthened verifier must FAIL on the stale four-queue policy
        v_before = _apply_file(cid, "ops/sql/verify_pipeline_driver.sql")
        assert v_before.returncode != 0, "verifier should reject the stale four-queue policy"
        assert "exactly" in (v_before.stderr + v_before.stdout)

        # run the NEW RLS script → must CONVERGE (not a no-op CREATE-if-missing)
        r = _apply_file(cid, "ops/sql/create_pipeline_driver_rls_policies.sql")
        assert r.returncode == 0, r.stderr[-500:]
        for tbl in ("job_runs", "job_tasks"):
            assert _qscope_queues(cid, tbl) == EXPECTED_FIVE, (tbl, _qscope_queues(cid, tbl))

        # verifier now PASSES (effective five-queue scope incl history refresh)
        v_after = _apply_file(cid, "ops/sql/verify_pipeline_driver.sql")
        assert v_after.returncode == 0, v_after.stderr[-500:]

        # idempotent: a second apply keeps exactly five queues, still green
        r2 = _apply_file(cid, "ops/sql/create_pipeline_driver_rls_policies.sql")
        assert r2.returncode == 0, r2.stderr[-500:]
        assert _qscope_queues(cid, "job_runs") == EXPECTED_FIVE
        assert _apply_file(cid, "ops/sql/verify_pipeline_driver.sql").returncode == 0

    def test_behavioral_queue_isolation_under_set_role(self, pg):
        cid = pg["cid"]
        _apply_file(cid, "ops/sql/create_pipeline_driver_rls_policies.sql")  # ensure five-queue
        # allowed queue → INSERT succeeds under the driver role (rolled back).
        allowed = _exec(cid,
            "BEGIN; SET ROLE smart_scanner_pipeline_driver; "
            "INSERT INTO job_runs(job_type,job_contract_version,queue_name,idempotency_key,status) "
            "VALUES ('t','t.v1','history_incremental_refresh','rls-probe-allow','queued'); "
            "RESET ROLE; ROLLBACK;")
        assert allowed.returncode == 0, allowed.stderr[-400:]
        # forbidden queue → RLS WITH CHECK denies it (transaction errors, rolled back).
        denied = _exec(cid,
            "BEGIN; SET ROLE smart_scanner_pipeline_driver; "
            "INSERT INTO job_runs(job_type,job_contract_version,queue_name,idempotency_key,status) "
            "VALUES ('t','t.v1','__forbidden_queue__','rls-probe-deny','queued'); "
            "RESET ROLE; ROLLBACK;")
        assert denied.returncode != 0, "forbidden-queue INSERT must be RLS-denied"
        assert "row-level security" in (denied.stderr + denied.stdout).lower()
        # no probe rows persisted (both transactions rolled back)
        assert _query(cid, "SELECT count(*) FROM job_runs WHERE idempotency_key LIKE 'rls-probe-%';") == "0"
