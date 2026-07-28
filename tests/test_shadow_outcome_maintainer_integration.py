"""Real PostgreSQL enforcement for smart_scanner_outcome_maintainer.

Isolated local PostgreSQL (never Supabase/Massive): real migrations, the
committed maintainer role + RLS policy scripts, RLS enabled on the 8 relations,
and representative fixtures. Proves, as the maintainer role, that grants + RLS
allow ONLY campaign-linked outcome writes for the allowed experiment and block
everything else, that DELETE/TRUNCATE/DDL/daily_bars-writes fail, that duplicate
outcomes are prevented, that the maintenance access-check reports ready, and
that the audit reader remains unchanged. Skips without Docker.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

# Production maintenance-run selection query (mirrors admin._latest_maintenance_run).
_LATEST_MAINT_SQL = (
    "SELECT id, status, created_at, started_at, finished_at, updated_at "
    "FROM strategy_shadow_outcome_runs "
    "WHERE requested_selector -> 'maintenance' IS NOT NULL "
    "ORDER BY COALESCE(finished_at, updated_at, started_at, created_at) DESC LIMIT 1")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "auditdb"
MAINT_PW = "maintpw_local_only_not_secret"
AUDIT_PW = "auditpw_local_only_not_secret"
MAINT_ROLE = "smart_scanner_outcome_maintainer"
AUDIT_ROLE = "smart_scanner_audit_reader"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "013_wyckoff_v2_shadow_arms"]
REL8 = ["public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
        "public.strategy_shadow_pair_outcomes", "public.strategy_shadow_run_pairs",
        "public.strategy_shadow_runs", "public.daily_bars", "public.patterns",
        "public.pattern_configs"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _u(n): return f"{n:08d}-0000-0000-0000-000000000000"


def _valid_block(cid, exp):
    return ("'{\"campaign\":{\"campaign_contract_version\":\"shadow_campaign.v1\","
            f"\"campaign_id\":\"{cid}\",\"experiment_code\":\"{exp}\","
            "\"chunk_index\":0,\"chunk_count\":1,\"as_of_date\":\"2026-06-01\"}}'::jsonb")


CAMP_PAIR = _u(9001)     # campaign-linked, wyckoff_v2_vs_baseline
NOCAMP_PAIR = _u(9002)   # wyckoff_v2_vs_baseline, no campaign telemetry
OTHER_PAIR = _u(9003)    # other_experiment
DONE_PAIR = _u(9004)     # campaign-linked, already-complete outcome


def _fixtures():
    s = []
    runs = [(_u(8001), "wyckoff_v2_vs_baseline", _valid_block("camp-1", "wyckoff_v2_vs_baseline")),
            (_u(8002), "wyckoff_v2_vs_baseline", "NULL"),
            (_u(8003), "other_experiment", _valid_block("camp-x", "other_experiment")),
            (_u(8004), "wyckoff_v2_vs_baseline", _valid_block("camp-1", "wyckoff_v2_vs_baseline"))]
    for rid, exp, tel in runs:
        s.append("INSERT INTO public.strategy_shadow_runs"
                 "(id,experiment_code,experiment_version,status,provider,telemetry,started_at) "
                 f"VALUES ('{rid}','{exp}','wyckoff_v2_shadow.v2','completed','massive',{tel},NOW());")
    pairs = [(CAMP_PAIR, "wyckoff_v2_vs_baseline", "AAA", _u(8001)),
             (NOCAMP_PAIR, "wyckoff_v2_vs_baseline", "BBB", _u(8002)),
             (OTHER_PAIR, "other_experiment", "CCC", _u(8003)),
             (DONE_PAIR, "wyckoff_v2_vs_baseline", "DDD", _u(8004))]
    for pid, exp, sym, rid in pairs:
        s.append("INSERT INTO public.strategy_shadow_pairs"
                 "(id,origin_run_id,experiment_code,experiment_version,symbol,timeframe,provider,"
                 "snapshot_date,market_data_as_of,frame_snapshot_version,frame_hash,frame_bar_count,"
                 "frame_first_date,frame_last_date,frame_snapshot,pair_fingerprint,pair_fingerprint_version) "
                 f"VALUES ('{pid}','{rid}','{exp}','wyckoff_v2_shadow.v2','{sym}','1d','massive',"
                 "'2026-06-01','2026-06-01T00:00:00Z','daily_ohlcv_snapshot.v1','fh-'||"
                 f"'{pid}',1,'2026-06-01','2026-06-01','[]'::jsonb,'fp-{pid}','shadow_pair_fingerprint.v1');")
        s.append("INSERT INTO public.strategy_shadow_run_pairs(run_id,pair_id,created_new_pair) "
                 f"VALUES ('{rid}','{pid}',true);")
    # a pre-existing COMPLETE outcome on DONE_PAIR (to test not-overwrite/dup).
    s.append("INSERT INTO public.strategy_shadow_pair_outcomes"
             "(id,pair_id,outcome_fingerprint,outcome_fingerprint_version,calculation_version,"
             "outcome_coverage_version,forward_frame_version,reference_price_role,"
             "available_forward_bars,outcome_status) "
             f"VALUES ('{_u(7001)}','{DONE_PAIR}','ofp-done','shadow_pair_outcome_fingerprint.v1',"
             "'outcome.v1','shadow_pair_outcomes.v1','shadow_forward_bars.v1',"
             "'paired_decision_observation',20,'complete');")
    return "\n".join(s)


def _outcome_insert(pid, oid):
    return ("INSERT INTO public.strategy_shadow_pair_outcomes"
            "(id,pair_id,outcome_fingerprint,outcome_fingerprint_version,calculation_version,"
            "outcome_coverage_version,forward_frame_version,reference_price_role,"
            "available_forward_bars,outcome_status) "
            f"VALUES ('{oid}','{pid}','ofp-{oid}','shadow_pair_outcome_fingerprint.v1',"
            "'outcome.v1','shadow_pair_outcomes.v1','shadow_forward_bars.v1',"
            "'paired_decision_observation',0,'pending_forward_bars')")


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")


def _sh(a, inp=None, t=120):
    return subprocess.run(a, input=inp, capture_output=True, text=True, timeout=t)


def _psql(cid, sql, *, role="postgres", variables=None, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", role, "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    return _sh(args, inp=(open(path).read() if path else sql))


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
            assert r.returncode == 0, f"{m}: {r.stderr[-300:]}"
        assert _psql(cid, _fixtures()).returncode == 0, "fixtures"
        # roles
        r = _psql(cid, None, variables={"maint_password": MAINT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_outcome_maintainer.sql"))
        assert r.returncode == 0, f"maint role: {r.stderr[-400:]}"
        r = _psql(cid, None, variables={"audit_password": AUDIT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_audit_reader.sql"))
        assert r.returncode == 0, f"audit role: {r.stderr[-400:]}"
        # enable RLS on the 8 relations
        assert _psql(cid, "\n".join(f"ALTER TABLE {r} ENABLE ROW LEVEL SECURITY;" for r in REL8)).returncode == 0
        # policies (audit first, then maintainer — prove maintainer script leaves audit alone)
        assert _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_shadow_audit_rls_policies.sql")).returncode == 0
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_shadow_outcome_maintainer_rls_policies.sql"))
        assert r.returncode == 0, f"maint policies: {r.stderr[-500:]}"
        yield {"cid": cid,
               "maint_dsn": f"postgresql://{MAINT_ROLE}:{MAINT_PW}@127.0.0.1:{hp}/{DBNAME}",
               "audit_dsn": f"postgresql://{AUDIT_ROLE}:{AUDIT_PW}@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "stop", cid])


async def _connect(dsn):
    return await asyncpg.connect(dsn)


class TestPolicyScriptsRerunnable:
    def test_maint_policies_rerunnable(self, pg):
        r = _psql(pg["cid"], None, path=os.path.join(
            REPO, "ops", "sql", "create_shadow_outcome_maintainer_rls_policies.sql"))
        assert r.returncode == 0, r.stderr[-400:]

    def test_verify_script_passes(self, pg):
        r = _psql(pg["cid"], None, role=MAINT_ROLE,
                  path=os.path.join(REPO, "ops", "sql", "verify_shadow_outcome_maintainer.sql"))
        assert r.returncode == 0, r.stderr[-400:]


class TestGrantsAndRls:
    def test_reads_succeed(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                assert await c.fetchval("SELECT current_user") == MAINT_ROLE
                for rel in REL8:
                    await c.fetchval(f"SELECT count(*) FROM {rel}")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_campaign_write_allowed(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                await c.execute(_outcome_insert(CAMP_PAIR, _u(7101)))
                assert await c.fetchval(
                    "SELECT count(*) FROM strategy_shadow_pair_outcomes WHERE pair_id=$1",
                    __import__("uuid").UUID(CAMP_PAIR)) == 1
                # UPDATE the campaign row succeeds
                await c.execute(
                    "UPDATE strategy_shadow_pair_outcomes SET available_forward_bars=1 "
                    "WHERE pair_id=$1", __import__("uuid").UUID(CAMP_PAIR))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_non_campaign_write_denied(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(_outcome_insert(NOCAMP_PAIR, _u(7102)))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_other_experiment_write_denied(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(_outcome_insert(OTHER_PAIR, _u(7103)))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_duplicate_outcome_prevented(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                # DONE_PAIR already has a complete outcome (unique pair_id).
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(_outcome_insert(DONE_PAIR, _u(7104)))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_delete_truncate_ddl_and_daily_bars_denied(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                for stmt in (
                    "DELETE FROM strategy_shadow_pair_outcomes",
                    "TRUNCATE strategy_shadow_pair_outcomes",
                    "CREATE TABLE public.evil (x int)",
                    "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                    "VALUES('Z','2020-01-01',1,1,1,1,1)",
                    "UPDATE daily_bars SET close = 2",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await c.execute(stmt)
            finally:
                await c.close()
        asyncio.run(drive())

    def test_outcome_run_write_allowed(self, pg):
        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                await c.execute(
                    "INSERT INTO strategy_shadow_outcome_runs(id,status,provider,started_at,created_at,updated_at) "
                    "VALUES($1,'running','massive',NOW(),NOW(),NOW())",
                    __import__("uuid").UUID(_u(7201)))
                await c.execute(
                    "UPDATE strategy_shadow_outcome_runs SET status='completed' WHERE id=$1",
                    __import__("uuid").UUID(_u(7201)))
            finally:
                await c.close()
        asyncio.run(drive())


class TestAccessCheckAndAuditUnchanged:
    def test_maintenance_access_check_ready(self, pg):
        from app.maintenance_access import run_maintenance_access_check

        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                async with c.transaction():
                    return await run_maintenance_access_check(
                        c, expected_role=MAINT_ROLE,
                        connection_mode="maintenance_explicit", provider="massive",
                        provider_credential_configured=True, scheduler_enabled=False,
                        maintenance_only_mode=True, max_batch_size=10,
                        mutation_route_count=1,
                        locked_cohort_hash="sha256:LOCK",
                        current_cohort_lock_hash="sha256:LOCK",
                        cohort_pair_count=0)
            finally:
                await c.close()
        r = asyncio.run(drive())
        assert r["ready_for_maintenance_execution"] is True, r["reasons"]

    def test_audit_reader_still_ready(self, pg):
        from app.audit_access import run_access_check

        async def drive():
            c = await _connect(pg["audit_dsn"])
            try:
                return await run_access_check(
                    c, expected_role=AUDIT_ROLE, require_expected_role=True,
                    connection_mode="audit_explicit")
            finally:
                await c.close()
        r = asyncio.run(drive())
        assert r["ready_for_closeout_audit"] is True, r["reasons"]


def _reset_runs(pg):
    """Clear outcome-run rows between cooldown tests (as postgres — the
    maintainer intentionally has no DELETE privilege)."""
    r = _psql(pg["cid"], "DELETE FROM public.strategy_shadow_outcome_runs;")
    assert r.returncode == 0, r.stderr[-300:]


async def _insert_run(c, *, maintenance, status="completed", finished_at=None,
                      run_id=None):
    """Insert an outcome-run row as the connected role. `maintenance=True` tags
    it with a bounded maintenance marker in requested_selector (as the execute
    route does); `maintenance=False` writes a generic run (no marker)."""
    rid = run_id or uuid.uuid4()
    selector = {"pair_ids": [], "include_recalc": False}
    if maintenance:
        selector["maintenance"] = {
            "contract_version": "shadow_maintenance_execute.v2",
            "mode": "normal", "batch_identity": "batch:test", "pair_count": 0}
    await c.execute(
        "INSERT INTO public.strategy_shadow_outcome_runs "
        "(id,status,requested_selector,provider,started_at,created_at,updated_at,finished_at) "
        "VALUES ($1,$2,$3::jsonb,'massive',NOW(),NOW(),NOW(),$4)",
        rid, status, json.dumps(selector), finished_at)
    return rid


class TestMaintenanceCooldownPersistence:
    """Real-Postgres proof the cooldown reads persisted maintenance runs, is
    scoped to maintenance runs only, and survives a fresh connection."""

    def test_maintenance_run_selected_generic_ignored(self, pg):
        from app.maintenance_cooldown import compute_cooldown
        _reset_runs(pg)

        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                # a generic (non-maintenance) run must NOT be selected
                await _insert_run(c, maintenance=False, status="completed",
                                  finished_at=datetime.now(timezone.utc))
                # with only a generic run present, no cooldown applies
                none_row = await c.fetchrow(_LATEST_MAINT_SQL)
                assert none_row is None
                # add a maintenance run finished 4s ago -> selected + blocks
                fin = datetime.now(timezone.utc) - timedelta(seconds=4)
                mid = await _insert_run(c, maintenance=True, status="completed",
                                        finished_at=fin)
                row = await c.fetchrow(_LATEST_MAINT_SQL)
                assert row is not None and row["id"] == mid
                cd = compute_cooldown(dict(row), min_interval_seconds=75,
                                      now=datetime.now(timezone.utc))
                assert cd["execution_allowed_by_cooldown"] is False
                assert cd["cooldown_remaining_seconds"] > 0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_generic_run_alone_allows_execution(self, pg):
        from app.maintenance_cooldown import compute_cooldown
        _reset_runs(pg)

        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                await _insert_run(c, maintenance=False, status="completed",
                                  finished_at=datetime.now(timezone.utc))
                row = await c.fetchrow(_LATEST_MAINT_SQL)
                cd = compute_cooldown(dict(row) if row else None,
                                      min_interval_seconds=75,
                                      now=datetime.now(timezone.utc))
                assert cd["execution_allowed_by_cooldown"] is True
            finally:
                await c.close()
        asyncio.run(drive())

    def test_failed_run_establishes_cooldown(self, pg):
        from app.maintenance_cooldown import compute_cooldown
        _reset_runs(pg)

        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                fin = datetime.now(timezone.utc) - timedelta(seconds=5)
                await _insert_run(c, maintenance=True, status="failed",
                                  finished_at=fin)
                row = await c.fetchrow(_LATEST_MAINT_SQL)
                assert row["status"] == "failed"
                cd = compute_cooldown(dict(row), min_interval_seconds=75,
                                      now=datetime.now(timezone.utc))
                assert cd["execution_allowed_by_cooldown"] is False
            finally:
                await c.close()
        asyncio.run(drive())

    def test_latest_maintenance_run_wins(self, pg):
        _reset_runs(pg)

        async def drive():
            c = await _connect(pg["maint_dsn"])
            try:
                old = await _insert_run(
                    c, maintenance=True, status="completed",
                    finished_at=datetime.now(timezone.utc) - timedelta(seconds=600))
                new = await _insert_run(
                    c, maintenance=True, status="completed",
                    finished_at=datetime.now(timezone.utc) - timedelta(seconds=3))
                row = await c.fetchrow(_LATEST_MAINT_SQL)
                assert row["id"] == new and row["id"] != old
            finally:
                await c.close()
        asyncio.run(drive())

    def test_cooldown_survives_new_connection(self, pg):
        """Insert on one connection/instance; a brand-new connection sees the
        persisted run and still enforces the cooldown (never process memory)."""
        from app.maintenance_cooldown import compute_cooldown
        _reset_runs(pg)

        async def drive():
            c1 = await _connect(pg["maint_dsn"])
            try:
                fin = datetime.now(timezone.utc) - timedelta(seconds=6)
                await _insert_run(c1, maintenance=True, status="completed",
                                  finished_at=fin)
            finally:
                await c1.close()
            c2 = await _connect(pg["maint_dsn"])  # fresh connection == new instance
            try:
                row = await c2.fetchrow(_LATEST_MAINT_SQL)
                assert row is not None
                cd = compute_cooldown(dict(row), min_interval_seconds=75,
                                      now=datetime.now(timezone.utc))
                assert cd["execution_allowed_by_cooldown"] is False
                assert cd["cooldown_remaining_seconds"] > 0
            finally:
                await c2.close()
        asyncio.run(drive())
