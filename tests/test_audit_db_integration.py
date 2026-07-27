"""Real PostgreSQL integration: least-privilege audit role + RLS visibility.

Spins up an ISOLATED local PostgreSQL container (never Supabase, never the
internet beyond the already-present image), applies the real table-creating
migrations, creates `smart_scanner_audit_reader` via the committed SQL template,
and — crucially — reproduces the LIVE drift by ENABLING Row Level Security on
all eight relations. It then proves, against real PostgreSQL:

  * privilege enforcement: the role can SELECT but cannot write or run DDL;
  * RLS state A (enabled, no policy): access-check NOT ready
    (rls_select_policy_missing) and closeout fails closed (409), even though
    the SELECT grant is present;
  * RLS state B (intended policies): access-check ready and closeout completes;
  * states C/D/E/F: wrong-role, conditional, restrictive, owner all NOT ready;
  * the committed policy + verify SQL are executable and rerunnable.

Skips cleanly when Docker / the postgres image is unavailable.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "auditdb"
AUDIT_PW = "auditpw_local_only_not_secret"
AUDIT_ROLE = "smart_scanner_audit_reader"
INTENDED_POLICY = "smart_scanner_audit_reader_select"
MIGRATIONS = [
    "001_initial_schema", "005_massive_provider",
    "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
    "013_wyckoff_v2_shadow_arms",
]
REL8 = [
    "public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
    "public.strategy_shadow_pair_outcomes", "public.strategy_shadow_run_pairs",
    "public.strategy_shadow_runs", "public.daily_bars", "public.patterns",
    "public.pattern_configs",
]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FIXTURES_SQL = """
INSERT INTO public.patterns(code, name, is_enabled) VALUES
  ('wyckoff_mtf_v2', 'Wyckoff MTF v2', false),
  ('sma150_bounce', 'SMA150 Bounce', true)
ON CONFLICT (code) DO NOTHING;
INSERT INTO public.pattern_configs(pattern_code, key, value) VALUES
  ('wyckoff_mtf_v2', 'min_price', '5.0'::jsonb)
ON CONFLICT (pattern_code, key) DO NOTHING;
INSERT INTO public.daily_bars(symbol, trading_date, open, high, low, close, volume, source)
VALUES ('SPY','2026-05-04',1,1,1,1,1000,'test'),
       ('SPY','2026-05-05',1,1,1,1,1000,'test')
ON CONFLICT (symbol, trading_date) DO NOTHING;
"""


def _docker_ready() -> bool:
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_ready(), reason="docker or postgres:16-alpine image unavailable"
)


def _sh(args, inp=None, timeout=90):
    return subprocess.run(args, input=inp, capture_output=True, text=True,
                          timeout=timeout)


def _psql(cid, sql, *, role="postgres", variables=None, path=None):
    """Run SQL in the container's DB as `role` (local trust auth, no password)."""
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", role, "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    body = open(path).read() if path else sql
    return _sh(args, inp=body)


def _drop_all_policies(cid):
    _psql(cid, """
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT schemaname, tablename, policyname FROM pg_policies
           WHERE schemaname='public' AND ('public.'||tablename) = ANY($REL$)
  LOOP EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I',
                      r.policyname, r.schemaname, r.tablename); END LOOP;
END $$;""".replace("$REL$", "ARRAY[%s]" % ",".join("'%s'" % r for r in REL8)))


def _apply_committed_policies(cid):
    return _psql(cid, None,
                 path=os.path.join(REPO, "ops", "sql",
                                   "create_shadow_audit_rls_policies.sql"))


def _policy_on_all(cid, template):
    return _psql(cid, "\n".join(template.format(rel=r) for r in REL8))


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres",
               "-P", PG_IMAGE]).stdout.strip()
    assert cid, "failed to start postgres container"
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready", "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        port_out = _sh(["docker", "port", cid, "5432/tcp"]).stdout.strip()
        host_port = int(port_out.splitlines()[0].rsplit(":", 1)[1])

        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                    f"CREATE DATABASE {DBNAME};"]).returncode == 0
        for m in MIGRATIONS:
            r = _psql(cid, None,
                      path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        assert _psql(cid, FIXTURES_SQL).returncode == 0
        r = _psql(cid, None, variables={"audit_password": AUDIT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_audit_reader.sql"))
        assert r.returncode == 0, f"role script: {r.stderr[-400:]}"

        # Reproduce the LIVE drift: enable RLS on all 8 relations.
        assert _policy_on_all(cid, "ALTER TABLE {rel} ENABLE ROW LEVEL SECURITY;").returncode == 0
        # A throwaway role for the wrong-role state test.
        _psql(cid, "DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='other_reader') "
                   "THEN CREATE ROLE other_reader; END IF; END $$;")

        yield {
            "cid": cid,
            "audit_dsn": (f"postgresql://{AUDIT_ROLE}:{AUDIT_PW}"
                          f"@127.0.0.1:{host_port}/{DBNAME}"),
            "super_dsn": (f"postgresql://postgres:postgres"
                          f"@127.0.0.1:{host_port}/{DBNAME}"),
        }
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(autouse=True)
def _baseline_state_a(pg):
    """Every test starts from State A (RLS enabled, NO policies)."""
    _drop_all_policies(pg["cid"])
    yield
    _drop_all_policies(pg["cid"])


async def _connect(dsn):
    return await asyncpg.connect(dsn)


def _access_check(dsn, expected=AUDIT_ROLE):
    from app.audit_access import run_access_check

    async def drive():
        conn = await _connect(dsn)
        try:
            return await run_access_check(
                conn, expected_role=expected, require_expected_role=True,
                connection_mode="audit_explicit")
        finally:
            await conn.close()
    return asyncio.run(drive())


class TestRolePrivileges:
    def test_role_connects_read_only(self, pg):
        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                assert await conn.fetchval("SELECT current_user") == AUDIT_ROLE
                assert await conn.fetchval("SHOW default_transaction_read_only") == "on"
            finally:
                await conn.close()
        asyncio.run(drive())

    def test_select_grant_present_writes_and_ddl_denied(self, pg):
        from app.audit_access import REQUIRED_RELATIONS

        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                for rel in REQUIRED_RELATIONS:
                    await conn.fetchval(f"SELECT count(*) FROM {rel}")  # grant works
                for stmt in (
                    "INSERT INTO public.daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES('Z','2020-01-01',1,1,1,1,1)",
                    "UPDATE public.daily_bars SET close = 2",
                    "DELETE FROM public.daily_bars",
                    "TRUNCATE public.daily_bars",
                    "CREATE TABLE public.evil (x int)",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await conn.execute(stmt)
                await conn.execute("SET default_transaction_read_only = off")
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await conn.execute(
                        "INSERT INTO public.daily_bars(symbol,trading_date,open,high,low,close,volume) "
                        "VALUES('Z','2020-01-02',1,1,1,1,1)")
            finally:
                await conn.close()
        asyncio.run(drive())


class TestAccessCheckRlsStates:
    def test_state_a_no_policy_not_ready(self, pg):
        r = _access_check(pg["audit_dsn"])
        assert r["ready_for_closeout_audit"] is False
        assert any("rls_select_policy_missing" in x for x in r["reasons"])
        # SELECT grant is still present — proving grant != visibility.
        assert all(rel["can_select"] for rel in r["required_relations"])
        assert all(rel["rls_enabled"] for rel in r["required_relations"])

    def test_state_b_intended_policies_ready(self, pg):
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        r = _access_check(pg["audit_dsn"])
        assert r["ready_for_closeout_audit"] is True, r["reasons"]
        assert all(rel["rls_ready"] for rel in r["required_relations"])
        assert all(rel["full_row_select_policy_present"]
                   for rel in r["required_relations"])

    def test_state_c_wrong_role_not_ready(self, pg):
        _policy_on_all(pg["cid"],
                       f"CREATE POLICY {INTENDED_POLICY} ON {{rel}} AS PERMISSIVE "
                       "FOR SELECT TO other_reader USING (true);")
        r = _access_check(pg["audit_dsn"])
        assert r["ready_for_closeout_audit"] is False
        assert any("rls_select_policy_missing" in x for x in r["reasons"])

    def test_state_d_conditional_not_ready(self, pg):
        _policy_on_all(pg["cid"],
                       f"CREATE POLICY {INTENDED_POLICY} ON {{rel}} AS PERMISSIVE "
                       f"FOR SELECT TO {AUDIT_ROLE} USING (false);")
        r = _access_check(pg["audit_dsn"])
        assert r["ready_for_closeout_audit"] is False
        assert any("rls_full_row_visibility_unproven" in x for x in r["reasons"])

    def test_state_e_restrictive_not_ready(self, pg):
        _policy_on_all(pg["cid"],
                       f"CREATE POLICY {INTENDED_POLICY} ON {{rel}} AS PERMISSIVE "
                       f"FOR SELECT TO {AUDIT_ROLE} USING (true);")
        _policy_on_all(pg["cid"],
                       f"CREATE POLICY test_restrictive ON {{rel}} AS RESTRICTIVE "
                       f"FOR SELECT TO {AUDIT_ROLE} USING (false);")
        r = _access_check(pg["audit_dsn"])
        assert r["ready_for_closeout_audit"] is False
        assert any("rls_restrictive_policy_present" in x for x in r["reasons"])

    def test_state_f_owner_not_ready(self, pg):
        # Even with the intended policies present, running as the owner/superuser
        # must be rejected on identity — owner bypass never masks readiness.
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        r = _access_check(pg["super_dsn"])
        assert r["ready_for_closeout_audit"] is False
        assert "database_identity_mismatch" in r["reasons"]


class TestSqlScriptsExecutable:
    def test_rls_enabled_on_all_relations(self, pg):
        async def drive():
            conn = await _connect(pg["super_dsn"])
            try:
                rows = await conn.fetch(
                    "SELECT c.relname, c.relrowsecurity FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname='public' AND ('public.'||c.relname) = ANY($1::text[])",
                    REL8)
            finally:
                await conn.close()
            return rows
        rows = asyncio.run(drive())
        assert len(rows) == 8 and all(r["relrowsecurity"] for r in rows)

    def test_policy_script_rerunnable(self, pg):
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        assert _apply_committed_policies(pg["cid"]).returncode == 0  # idempotent

    def test_policy_script_fails_on_mismatch(self, pg):
        # A policy of the intended NAME but a different (conditional) definition
        # must make the committed script fail rather than silently replace it.
        _policy_on_all(pg["cid"],
                       f"CREATE POLICY {INTENDED_POLICY} ON {{rel}} AS PERMISSIVE "
                       f"FOR SELECT TO {AUDIT_ROLE} USING (false);")
        assert _apply_committed_policies(pg["cid"]).returncode != 0

    def test_verify_script_passes_when_ready(self, pg):
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        r = _psql(pg["cid"], None, role=AUDIT_ROLE,
                  path=os.path.join(REPO, "ops", "sql",
                                    "verify_shadow_audit_reader.sql"))
        assert r.returncode == 0, r.stderr[-400:]

    def test_verify_script_fails_without_policy(self, pg):
        # State A: verify must exit non-zero (RAISE) — no full-row policy.
        r = _psql(pg["cid"], None, role=AUDIT_ROLE,
                  path=os.path.join(REPO, "ops", "sql",
                                    "verify_shadow_audit_reader.sql"))
        assert r.returncode != 0


class TestCloseoutUnderRls:
    async def _call_closeout(self, dsn, expected_role=AUDIT_ROLE):
        from httpx import ASGITransport, AsyncClient
        from main import app
        from app.config import settings
        from app.deps import get_worker_token, close_db_pool

        saved = (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
                 settings.AUDIT_EXPECTED_DB_ROLE)
        settings.AUDIT_ONLY_MODE = True
        settings.AUDIT_DATABASE_URL = dsn
        settings.AUDIT_EXPECTED_DB_ROLE = expected_role
        app.dependency_overrides[get_worker_token] = lambda: "t"
        await close_db_pool()
        try:
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as ac:
                resp = await ac.get(
                    "/api/admin/shadow-cohort/closeout",
                    params={"experiment_code": "wyckoff_v2_vs_baseline"})
            return resp.status_code, resp.json()
        finally:
            await close_db_pool()
            app.dependency_overrides.pop(get_worker_token, None)
            (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
             settings.AUDIT_EXPECTED_DB_ROLE) = saved

    def test_closeout_409_state_a(self, pg):
        status, body = asyncio.run(self._call_closeout(pg["audit_dsn"]))
        assert status == 409
        assert body["detail"]["error"] == "closeout_not_ready"

    def test_closeout_200_with_policies(self, pg):
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        status, body = asyncio.run(self._call_closeout(pg["audit_dsn"]))
        assert status == 200, body
        assert body["total_evaluations"] == 0
        assert body["closeout_contract_version"] == "shadow_cohort_closeout.v1"

    def test_closeout_409_superuser(self, pg):
        assert _apply_committed_policies(pg["cid"]).returncode == 0
        status, body = asyncio.run(self._call_closeout(pg["super_dsn"]))
        assert status == 409
