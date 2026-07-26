"""Real PostgreSQL integration: least-privilege audit role end-to-end.

Spins up an ISOLATED local PostgreSQL container (never Supabase, never the
internet beyond the already-present image), applies the real table-creating
migrations, creates `smart_scanner_audit_reader` via the committed SQL template,
and proves ACTUAL PostgreSQL enforcement — the role can SELECT but cannot write
or run DDL — plus that the access-check reports ready and the closeout endpoint
completes under the least-privilege role (and fails closed under a broader one).

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
MIGRATIONS = [
    "001_initial_schema", "005_massive_provider",
    "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
    "013_wyckoff_v2_shadow_arms",
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


def _psql(cid, db, sql, variables=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", "postgres", "-d", db]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    return _sh(args, inp=sql)


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

        assert _psql(cid, "postgres", f"CREATE DATABASE {DBNAME};").returncode == 0
        for m in MIGRATIONS:
            with open(os.path.join(REPO, "app", "db", "migrations", f"{m}.sql")) as f:
                r = _psql(cid, DBNAME, f.read())
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        assert _psql(cid, DBNAME, FIXTURES_SQL).returncode == 0

        with open(os.path.join(REPO, "ops", "sql", "create_shadow_audit_reader.sql")) as f:
            role_sql = f.read()
        r = _psql(cid, DBNAME, role_sql,
                  variables={"audit_password": AUDIT_PW, "db_name": DBNAME})
        assert r.returncode == 0, f"role script: {r.stderr[-400:]}"

        yield {
            "cid": cid,
            "port": host_port,
            "audit_dsn": (f"postgresql://{AUDIT_ROLE}:{AUDIT_PW}"
                          f"@127.0.0.1:{host_port}/{DBNAME}"),
            "super_dsn": (f"postgresql://postgres:postgres"
                          f"@127.0.0.1:{host_port}/{DBNAME}"),
            "role_sql": role_sql,
        }
    finally:
        _sh(["docker", "stop", cid])


async def _connect(dsn):
    return await asyncpg.connect(dsn)


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

    def test_select_succeeds_on_all_required_tables(self, pg):
        from app.audit_access import REQUIRED_RELATIONS

        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                for rel in REQUIRED_RELATIONS:
                    await conn.fetchval(f"SELECT count(*) FROM {rel}")
            finally:
                await conn.close()
        asyncio.run(drive())

    def test_writes_and_ddl_denied(self, pg):
        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                for stmt in (
                    "INSERT INTO public.daily_bars(symbol,trading_date,open,high,low,close,volume) "
                    "VALUES('Z','2020-01-01',1,1,1,1,1)",
                    "UPDATE public.daily_bars SET close = 2",
                    "DELETE FROM public.daily_bars",
                    "TRUNCATE public.daily_bars",
                    "CREATE TABLE public.evil (x int)",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await conn.execute(stmt)
            finally:
                await conn.close()
        asyncio.run(drive())

    def test_insert_denied_by_grant_even_when_read_write(self, pg):
        # Prove GRANT-level least privilege, not only the read-only default:
        # switch the session to read-write, INSERT still fails on privilege.
        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                await conn.execute("SET default_transaction_read_only = off")
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await conn.execute(
                        "INSERT INTO public.daily_bars"
                        "(symbol,trading_date,open,high,low,close,volume) "
                        "VALUES('Z','2020-01-02',1,1,1,1,1)")
            finally:
                await conn.close()
        asyncio.run(drive())


class TestAccessCheckReal:
    def test_ready_under_audit_role(self, pg):
        from app.audit_access import run_access_check

        async def drive():
            conn = await _connect(pg["audit_dsn"])
            try:
                return await run_access_check(
                    conn, expected_role=AUDIT_ROLE,
                    require_expected_role=True, connection_mode="audit_explicit")
            finally:
                await conn.close()
        result = asyncio.run(drive())
        assert result["ready_for_closeout_audit"] is True, result["reasons"]
        assert result["database_identity"] == AUDIT_ROLE
        assert result["default_transaction_read_only"] is True
        assert result["unexpected_write_privileges"] == []

    def test_not_ready_under_superuser(self, pg):
        from app.audit_access import run_access_check

        async def drive():
            conn = await _connect(pg["super_dsn"])
            try:
                return await run_access_check(
                    conn, expected_role=AUDIT_ROLE,
                    require_expected_role=True, connection_mode="audit_explicit")
            finally:
                await conn.close()
        result = asyncio.run(drive())
        assert result["ready_for_closeout_audit"] is False
        # superuser: elevated attributes + identity mismatch + writable
        assert result["elevated_role_attributes"]
        assert "database_identity_mismatch" in result["reasons"]


class TestSqlScriptExecutable:
    def test_role_script_is_rerunnable(self, pg):
        r = _psql(pg["cid"], DBNAME, pg["role_sql"],
                  variables={"audit_password": AUDIT_PW, "db_name": DBNAME})
        assert r.returncode == 0, r.stderr[-400:]

    def test_rls_off_on_required_relations(self, pg):
        async def drive():
            conn = await _connect(pg["super_dsn"])
            try:
                rows = await conn.fetch(
                    """
                    SELECT c.relname, c.relrowsecurity
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname='public' AND c.relname = ANY($1::text[])
                    """,
                    ["strategy_shadow_evaluations", "strategy_shadow_pairs",
                     "strategy_shadow_pair_outcomes", "strategy_shadow_run_pairs",
                     "strategy_shadow_runs", "daily_bars", "patterns",
                     "pattern_configs"],
                )
            finally:
                await conn.close()
            return rows
        rows = asyncio.run(drive())
        assert len(rows) == 8
        assert all(r["relrowsecurity"] is False for r in rows)


class TestCloseoutUnderAuditRole:
    """The real closeout endpoint runs end-to-end via the least-privilege role.

    Driven with httpx.AsyncClient so pool creation + the request + pool close
    all happen on ONE event loop (TestClient + a separate asyncio.run would
    close a loop-bound asyncpg pool from the wrong loop).
    """

    async def _call_closeout(self, dsn, expected_role):
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
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport,
                                   base_url="http://test") as ac:
                resp = await ac.get(
                    "/api/admin/shadow-cohort/closeout",
                    params={"experiment_code": "wyckoff_v2_vs_baseline"},
                )
            return resp.status_code, resp.json()
        finally:
            await close_db_pool()
            app.dependency_overrides.pop(get_worker_token, None)
            (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
             settings.AUDIT_EXPECTED_DB_ROLE) = saved

    def test_closeout_completes_under_audit_role(self, pg):
        status, body = asyncio.run(
            self._call_closeout(pg["audit_dsn"], AUDIT_ROLE))
        assert status == 200, body
        # empty shadow tables -> a valid zero-record closeout report, proving
        # every real closeout query executes under the least-privilege role.
        assert body["total_evaluations"] == 0
        assert body["closeout_contract_version"] == "shadow_cohort_closeout.v1"

    def test_closeout_fail_closed_under_superuser(self, pg):
        status, body = asyncio.run(
            self._call_closeout(pg["super_dsn"], AUDIT_ROLE))
        assert status == 409
        assert body["detail"]["error"] == "closeout_not_ready"
