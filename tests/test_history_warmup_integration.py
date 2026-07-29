"""Real-Postgres enforcement for the history-warmer role + 4H persistence.

Isolated local Postgres (never Supabase/Massive): real migrations incl. 014, the
committed history-warmer role + RLS scripts. Proves the warmer can INSERT/UPDATE
market_bars_4h + history_warmup_runs, CANNOT DELETE bars, CANNOT write
campaigns/evaluations/pair-outcomes; the outcome maintainer cannot write 4H bars;
the audit reader can SELECT but not write; writes persist across a new connection
and the readiness-manifest hash is reconnect-stable. Skips without Docker.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "warmupdb"
WARMER_PW = "warmerpw_local_only_not_secret"
MAINT_PW = "maintpw_local_only_not_secret"
AUDIT_PW = "auditpw_local_only_not_secret"
WARMER = "smart_scanner_history_warmer"
MAINT = "smart_scanner_outcome_maintainer"
AUDIT = "smart_scanner_audit_reader"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h",
              "015_history_warmup_run_items"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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


_INS_4H = (
    "INSERT INTO public.market_bars_4h"
    "(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,"
    "is_completed,is_regular_session,content_fingerprint) VALUES"
    "('{sym}','2026-06-11 13:30:00+00','2026-06-11 17:30:00+00','2026-06-11',"
    "1,2,0.5,1.5,100,true,true,'{fp}')")


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
        # roles
        for script, var, pw in (
            ("create_shadow_history_warmer.sql", "warmer_password", WARMER_PW),
            ("create_shadow_outcome_maintainer.sql", "maint_password", MAINT_PW),
            ("create_shadow_audit_reader.sql", "audit_password", AUDIT_PW),
        ):
            r = _psql(cid, None, variables={var: pw, "db_name": DBNAME},
                      path=os.path.join(REPO, "ops", "sql", script))
            assert r.returncode == 0, f"{script}: {r.stderr[-400:]}"
        # warmer RLS policies (also grants audit reader SELECT on the new tables)
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql",
                                               "create_shadow_history_warmer_rls_policies.sql"))
        assert r.returncode == 0, f"warmer rls: {r.stderr[-500:]}"
        yield {"cid": cid, "hp": hp,
               "warmer": f"postgresql://{WARMER}:{WARMER_PW}@127.0.0.1:{hp}/{DBNAME}",
               "maint": f"postgresql://{MAINT}:{MAINT_PW}@127.0.0.1:{hp}/{DBNAME}",
               "audit": f"postgresql://{AUDIT}:{AUDIT_PW}@127.0.0.1:{hp}/{DBNAME}",
               "owner": f"postgresql://postgres:postgres@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "stop", cid])


async def _c(dsn):
    return await asyncpg.connect(dsn)


class TestWarmerGrantsAndRls:
    def test_warmer_can_write_4h_and_runs(self, pg):
        async def drive():
            c = await _c(pg["warmer"])
            try:
                assert await c.fetchval("SELECT current_user") == WARMER
                await c.execute(_INS_4H.format(sym="AAAP", fp="fp1"))
                await c.execute("UPDATE public.market_bars_4h SET volume=200 WHERE symbol='AAAP'")
                await c.execute(
                    "INSERT INTO public.history_warmup_runs(status,requested_symbol_count) "
                    "VALUES('planned',1)")
                assert await c.fetchval("SELECT count(*) FROM market_bars_4h WHERE symbol='AAAP'") == 1
            finally:
                await c.close()
        asyncio.run(drive())

    def test_warmer_cannot_delete_4h(self, pg):
        async def drive():
            c = await _c(pg["warmer"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("DELETE FROM public.market_bars_4h")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_warmer_cannot_write_campaigns_evals_outcomes(self, pg):
        async def drive():
            c = await _c(pg["warmer"])
            try:
                for stmt in (
                    "INSERT INTO strategy_shadow_runs(id,experiment_code,experiment_version,status,provider,started_at) "
                    "VALUES(gen_random_uuid(),'x','v','completed','massive',NOW())",
                    "INSERT INTO strategy_shadow_evaluations(id,pair_id,arm_code,strategy_code,verdict) "
                    "VALUES(gen_random_uuid(),gen_random_uuid(),'candidate_wyckoff_v2','wyckoff_mtf_v2','AVOID')",
                    "INSERT INTO strategy_shadow_pair_outcomes(id,pair_id,outcome_status) "
                    "VALUES(gen_random_uuid(),gen_random_uuid(),'complete')",
                    "CREATE TABLE public.evil(x int)",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await c.execute(stmt)
            finally:
                await c.close()
        asyncio.run(drive())

    def test_warmer_daily_bars_functional_under_rls(self, pg):
        """Under the production posture (RLS ENABLED on daily_bars) the warmer's
        daily_bars grant is inert without a policy. The warmer RLS script adds
        SELECT/INSERT/UPDATE policies (no DELETE) so the already-granted daily
        operations work; DELETE stays denied."""
        # Owner enables RLS on daily_bars, then re-runs the (rerunnable) warmer
        # RLS policy script — which now provisions the warmer daily_bars policies.
        r = _psql(pg["cid"], "ALTER TABLE public.daily_bars ENABLE ROW LEVEL SECURITY;")
        assert r.returncode == 0, r.stderr[-300:]
        r = _psql(pg["cid"], None, path=os.path.join(
            REPO, "ops", "sql", "create_shadow_history_warmer_rls_policies.sql"))
        assert r.returncode == 0, r.stderr[-400:]

        async def drive():
            c = await _c(pg["warmer"])
            try:
                tr = c.transaction(); await tr.start()
                try:
                    await c.execute(
                        "INSERT INTO public.daily_bars(symbol,trading_date,open,high,low,"
                        "close,volume) VALUES('ZZDLYRLS','2026-06-11',1,2,0.5,1.5,100)")
                    assert await c.fetchval(
                        "SELECT count(*) FROM public.daily_bars WHERE symbol='ZZDLYRLS'") == 1
                    await c.execute(
                        "UPDATE public.daily_bars SET volume=200 WHERE symbol='ZZDLYRLS'")
                finally:
                    await tr.rollback()
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("DELETE FROM public.daily_bars")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_outcome_maintainer_cannot_write_4h(self, pg):
        async def drive():
            c = await _c(pg["maint"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(_INS_4H.format(sym="BBBP", fp="fp2"))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_audit_reader_select_but_not_write(self, pg):
        async def drive():
            c = await _c(pg["audit"])
            try:
                await c.fetchval("SELECT count(*) FROM market_bars_4h")  # SELECT ok
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(_INS_4H.format(sym="CCCP", fp="fp3"))
            finally:
                await c.close()
        asyncio.run(drive())

    def test_persist_across_new_connection_and_stable_hash(self, pg):
        from app.prospective_readiness import build_prospective_readiness_v2
        from datetime import datetime, timezone
        sql4h = ("SELECT symbol, COUNT(*) FILTER (WHERE is_completed)::int AS completed_4h_bars, "
                 "MIN(bar_end) FILTER (WHERE is_completed) AS oldest_4h, "
                 "MAX(bar_end) FILTER (WHERE is_completed) AS latest_4h "
                 "FROM market_bars_4h WHERE symbol = ANY($1::text[]) GROUP BY symbol")
        now = datetime(2026, 6, 11, 20, 0, 0, tzinfo=timezone.utc)

        async def one():
            c = await _c(pg["warmer"])
            try:
                fh = [dict(r) for r in await c.fetch(sql4h, ["AAAP"])]
                return build_prospective_readiness_v2(
                    ["AAAP"], [{"symbol": "AAAP", "daily_bars": 0, "month_groups": 0,
                                "week_groups": 0, "oldest": None, "latest": None}],
                    fh, now=now)["four_hour_manifest_hash"]
            finally:
                await c.close()
        h1 = asyncio.run(one())
        h2 = asyncio.run(one())  # brand-new connection
        assert h1 == h2
