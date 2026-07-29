"""Real-Postgres integration for the bounded history-warmup EXECUTE path.

Isolated local Docker Postgres (never Supabase/Massive): real migrations incl.
014+015, the history-warmer role + RLS, a deterministic FAKE provider injected
via app.routers.admin._resolve_history_warmup_provider. Exercises the endpoint
handlers directly on a real warmer asyncpg connection: full warm, exact provider
call count, persistence, readiness flip, idempotent replay, provider correction,
retryable→retry→success, terminal (no infinite retry), cooldown, stale-manifest,
stale-next-batch, advisory-lock conflict, unauthorized symbol, batch>1, invalid
bar, crash recovery, and role privilege denials. Skips without Docker.

A network guard fails any attempt to open a non-loopback socket.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from datetime import datetime, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "hwxdb"
WARMER_PW = "warmerpw_local_only_not_secret"
MAINT_PW = "maintpw_local_only_not_secret"
AUDIT_PW = "auditpw_local_only_not_secret"
WARMER = "smart_scanner_history_warmer"
MAINT = "smart_scanner_outcome_maintainer"
AUDIT = "smart_scanner_audit_reader"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "012_wyckoff_mtf_v2", "013_wyckoff_v2_shadow_arms",
              "014_market_bars_4h", "015_history_warmup_run_items"]
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


def _psql(cid, sql, *, variables=None, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DBNAME]
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
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        # production RLS posture on daily_bars (so the warmer needs its policy)
        assert _psql(cid, "ALTER TABLE public.daily_bars ENABLE ROW LEVEL SECURITY;").returncode == 0
        for script, var, pw in (
            ("create_shadow_history_warmer.sql", "warmer_password", WARMER_PW),
            ("create_shadow_outcome_maintainer.sql", "maint_password", MAINT_PW),
            ("create_shadow_audit_reader.sql", "audit_password", AUDIT_PW),
        ):
            r = _psql(cid, None, variables={var: pw, "db_name": DBNAME},
                      path=os.path.join(REPO, "ops", "sql", script))
            assert r.returncode == 0, f"{script}: {r.stderr[-400:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql",
                                               "create_shadow_history_warmer_rls_policies.sql"))
        assert r.returncode == 0, f"warmer rls: {r.stderr[-500:]}"
        yield {"cid": cid, "hp": hp,
               "warmer": f"postgresql://{WARMER}:{WARMER_PW}@127.0.0.1:{hp}/{DBNAME}",
               "maint": f"postgresql://{MAINT}:{MAINT_PW}@127.0.0.1:{hp}/{DBNAME}",
               "audit": f"postgresql://{AUDIT}:{AUDIT_PW}@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "stop", cid])


# --------------------------------------------------------------------------- #
@pytest.fixture(autouse=True)
def _clean_db(pg):
    """Isolate tests: truncate warmup + bar tables (owner) before each test so
    the GLOBAL persisted cooldown / run history never couples tests."""
    _psql(pg["cid"], "TRUNCATE history_warmup_run_items, history_warmup_runs, "
                     "market_bars_4h, daily_bars CASCADE;")
    yield


@pytest.fixture(autouse=True)
def _no_external_network(monkeypatch):
    """Fail any attempt to open a NON-loopback socket during these tests."""
    real_connect = socket.socket.connect

    def guard(self, address):
        host = address[0] if isinstance(address, tuple) else str(address)
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"external network access blocked: {host}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guard)


@pytest.fixture(autouse=True)
def _warmup_settings(monkeypatch):
    import app.routers.admin as admin
    from app.config import settings
    monkeypatch.setattr(settings, "HISTORY_WARMUP_ONLY_MODE", True)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", False)
    monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH", 1)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS", 0)
    # provider label 'fake' bypasses the Massive 60s cooldown floor so tests can
    # set an exact/zero interval; the real provider is never constructed.
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fake")
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0)


def _inject(monkeypatch, provider):
    import app.routers.admin as admin
    monkeypatch.setattr(admin, "_resolve_history_warmup_provider", lambda: provider)


async def _conn(dsn):
    return await asyncpg.connect(dsn)


async def _preflight(conn, symbols):
    from app.routers.admin import history_warmup_preflight
    return await history_warmup_preflight(_="t", db=conn, symbols=symbols)


async def _execute(conn, body):
    from app.routers.admin import history_warmup_execute
    return await history_warmup_execute(_="t", db=conn, body=body)


def _body_from(pf, mode="normal"):
    nb = pf["next_batch"]
    body = {"contract_version": "history_warmup_execute.v1", "mode": mode,
            "universe_hash": pf["universe_hash"], "config_hash": pf["config_hash"],
            "next_batch_hash": nb["next_batch_hash"], "symbols": list(nb["symbols"]),
            "limit": len(nb["symbols"])}
    if mode == "normal":
        body["readiness_manifest_hash"] = pf["combined_readiness_manifest_hash"]
    else:
        body["retry_plan_hash"] = pf["retry_plan_hash"]
    return body


def _ready_provider(symbol, now):
    from tests.support.fake_provider import FakeProvider, make_ready_daily, make_ready_4h
    return FakeProvider(daily={symbol: make_ready_daily(symbol, today=now.date())},
                        intraday={symbol: make_ready_4h(symbol, now=now)})


NOWFN = lambda: datetime.now(timezone.utc)  # noqa: E731


class TestExecuteHappyPath:
    def test_full_warm_flip_ready_call_count_persistence(self, pg, monkeypatch):
        sym = "ZWARM1"
        prov = _ready_provider(sym, NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                assert pf["next_batch"]["available"] and pf["next_batch"]["symbols"] == [sym]
                before_ready = pf["readiness"]["symbols"][0]["both_ready"]
                res = await _execute(c, _body_from(pf))
                assert res["status"] == "executed", res
                # (2) exact provider call count: 1 daily + 1 4H
                assert prov.call_count() == 2
                assert res["provider_request_count"] == 2
                # (3) persistence
                assert res["daily"]["inserted"] > 200
                assert res["four_hour"]["inserted"] == 12
                assert res["four_hour"]["completed_count"] == 12
                # (4) readiness flip not-ready -> ready
                assert before_ready is False
                assert res["readiness_after"]["both_ready"] is True
                assert res["readiness_after"]["four_hour"] == "ready"
                # DB reflects it
                n4 = await c.fetchval("SELECT count(*) FROM market_bars_4h WHERE symbol=$1", sym)
                assert n4 == 12
            finally:
                await c.close()
        asyncio.run(drive())

    def test_identical_replay_already_applied_no_provider(self, pg, monkeypatch):
        sym = "ZWARM2"
        prov = _ready_provider(sym, NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                body = _body_from(pf)
                r1 = await _execute(c, body)
                assert r1["status"] == "executed"
                calls_after_first = prov.call_count()
                # (5)(6) identical replay -> already_applied, NO provider call, no new run
                r2 = await _execute(c, body)
                assert r2["status"] == "already_applied"
                assert r2["run_id"] == r1["run_id"]
                assert prov.call_count() == calls_after_first  # provider NOT called
                runs = await c.fetchval("SELECT count(*) FROM history_warmup_runs "
                                        "WHERE requested_symbols::text LIKE $1", f"%{sym}%")
                assert runs == 1
            finally:
                await c.close()
        asyncio.run(drive())

    def test_provider_correction_updates_rows(self, pg, monkeypatch):
        from tests.support.fake_provider import FakeProvider, make_ready_daily, make_ready_4h
        sym = "ZWARM3"
        now = NOWFN()
        p1 = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date(), close=1.5)},
                          intraday={sym: make_ready_4h(sym, now=now, close=1.5)})
        _inject(monkeypatch, p1)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                await _execute(c, _body_from(pf))
                fp1 = await c.fetchval("SELECT content_fingerprint FROM market_bars_4h "
                                       "WHERE symbol=$1 ORDER BY bar_start LIMIT 1", sym)
                # corrected provider data (same identities, different close) via a
                # fresh preflight (symbol now ready -> re-warm is a NEW identity)
                p2 = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date(), close=1.5)},
                                  intraday={sym: make_ready_4h(sym, now=now, close=1.87)})
                _inject(monkeypatch, p2)
                # force a re-warm: readiness is ready, so next_batch is unavailable.
                # Drive the correction directly through the canonical upsert to
                # prove in-place correction (row count stable, fingerprint changes).
                from app.history_warmup_execute import normalize_4h_bars, upsert_4h_bars
                rows = normalize_4h_bars(make_ready_4h(sym, now=now, close=1.87),
                                         symbol=sym, now=now)
                tel = await upsert_4h_bars(c, rows)
                assert tel["updated"] >= 1 and tel["inserted"] == 0
                fp2 = await c.fetchval("SELECT content_fingerprint FROM market_bars_4h "
                                       "WHERE symbol=$1 ORDER BY bar_start LIMIT 1", sym)
                assert fp1 != fp2  # corrected in place
                n = await c.fetchval("SELECT count(*) FROM market_bars_4h WHERE symbol=$1", sym)
                assert n == 12  # no duplicate rows
            finally:
                await c.close()
        asyncio.run(drive())


class TestRetryAndTerminal:
    def test_retryable_error_creates_one_retry_item_then_retry_succeeds(self, pg, monkeypatch):
        from tests.support.fake_provider import (
            FakeProvider, make_ready_daily, make_ready_4h, rate_limited_error)
        sym = "ZRETRY1"
        now = NOWFN()
        # 4H fetch fails retryably (daily succeeds first)
        failing = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date())},
                               intraday_error=rate_limited_error())
        _inject(monkeypatch, failing)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                res = await _execute(c, _body_from(pf))
                assert res["status"] == "failed"
                assert res["error"]["code"] == "provider_rate_limited"
                assert res["error"]["retryable"] is True
                # (8) one retryable run item recorded
                item = await c.fetchrow("SELECT status, error_code, error_class, retryable "
                                        "FROM history_warmup_run_items WHERE symbol=$1", sym)
                assert item["status"] == "failed" and item["retryable"] is True
                assert item["error_class"] == "retryable"
                # preflight now exposes a retry plan with this symbol (retry-first)
                pf2 = await _preflight(c, sym)
                assert pf2["retryable_symbols"] == [sym]
                assert pf2["next_batch"]["mode"] == "retry"
                assert pf2["next_batch"]["symbols"] == [sym]
                # daily WAS persisted on the failed attempt (crash after daily,
                # before 4H) -> record the count to prove the retry does not
                # duplicate daily bars.
                daily_after_fail = await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol=$1", sym)
                assert daily_after_fail > 200
                # (9) retry succeeds -> item retryable cleared for the universe
                ok = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date())},
                                  intraday={sym: make_ready_4h(sym, now=now)})
                _inject(monkeypatch, ok)
                res2 = await _execute(c, _body_from(pf2, mode="retry"))
                assert res2["status"] == "executed"
                # case (c): after daily before 4H -> retry re-fetches daily with
                # NO duplicate rows (idempotent upsert), 4H now present.
                assert await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol=$1", sym) == daily_after_fail
                assert await c.fetchval(
                    "SELECT count(*) FROM market_bars_4h WHERE symbol=$1", sym) == 12
                pf3 = await _preflight(c, sym)
                assert pf3["retryable_symbols"] == []
                assert pf3["readiness"]["symbols"][0]["both_ready"] is True
            finally:
                await c.close()
        asyncio.run(drive())

    def test_terminal_error_no_infinite_retry(self, pg, monkeypatch):
        from tests.support.fake_provider import FakeProvider, make_ready_daily, make_invalid_4h
        sym = "ZTERM1"
        now = NOWFN()
        prov = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date())},
                            intraday={sym: make_invalid_4h(sym, now=now)})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                res = await _execute(c, _body_from(pf))
                # (17) invalid provider bar rejected safely; (10) terminal
                assert res["status"] == "failed"
                assert res["error"]["code"] == "provider_invalid_payload"
                assert res["error"]["retryable"] is False
                pf2 = await _preflight(c, sym)
                assert pf2["terminal_symbols"] == [sym]
                assert pf2["retryable_symbols"] == []          # never retryable
                assert pf2["next_batch"]["available"] is False  # blocks progression
                assert pf2["next_batch"]["reason"] == "only_terminal_symbols_remain"
            finally:
                await c.close()
        asyncio.run(drive())


class TestGates:
    def test_active_cooldown_rejects_before_provider(self, pg, monkeypatch):
        from app.config import settings
        sym = "ZCOOL1"
        prov = _ready_provider(sym, NOWFN())
        _inject(monkeypatch, prov)
        monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 120)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                await _execute(c, _body_from(pf))       # first run sets cooldown
                calls = prov.call_count()
                # a DIFFERENT fresh symbol batch now must be cooldown-blocked
                sym2 = "ZCOOL2"
                prov2 = _ready_provider(sym2, NOWFN())
                _inject(monkeypatch, prov2)
                pf2 = await _preflight(c, sym2)
                assert pf2["execution_allowed_by_cooldown"] is False
                from fastapi import HTTPException
                with pytest.raises(HTTPException) as e:
                    await _execute(c, _body_from(pf2))
                assert e.value.status_code == 409
                assert e.value.detail["error"] == "provider_cooldown_active"
                assert prov2.call_count() == 0            # provider never constructed/called
            finally:
                await c.close()
        asyncio.run(drive())

    def test_stale_manifest_and_next_batch_rejected(self, pg, monkeypatch):
        from fastapi import HTTPException
        sym = "ZSTALE1"
        prov = _ready_provider(sym, NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                stale = _body_from(pf); stale["readiness_manifest_hash"] = "sha256:stale"
                with pytest.raises(HTTPException) as e:
                    await _execute(c, stale)
                assert e.value.detail["reason"] == "stale_manifest"
                stale2 = _body_from(pf); stale2["next_batch_hash"] = "sha256:stale"
                with pytest.raises(HTTPException) as e2:
                    await _execute(c, stale2)
                assert e2.value.detail["reason"] == "stale_next_batch"
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_unauthorized_symbol_and_batch_gt_1_rejected(self, pg, monkeypatch):
        from fastapi import HTTPException
        sym = "ZAUTH1"
        _inject(monkeypatch, _ready_provider(sym, NOWFN()))

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                prov = _ready_provider(sym, NOWFN())
                _inject(monkeypatch, prov)
                pf = await _preflight(c, sym)
                # a symbol that is NOT the server-selected batch is rejected before
                # any provider call (the server recomputes over the requested set).
                b = _body_from(pf); b["symbols"] = ["ZOTHER"]; b["limit"] = 1
                with pytest.raises(HTTPException) as e:
                    await _execute(c, b)
                assert e.value.detail["reason"] in (
                    "stale_universe_hash", "symbols_not_server_selected_batch")
                # batch size > 1 is rejected outright (server cap = 1)
                b2 = _body_from(pf); b2["symbols"] = [sym, "ZX"]; b2["limit"] = 2
                with pytest.raises(HTTPException) as e2:
                    await _execute(c, b2)
                assert e2.value.detail["error"] == "batch_size_out_of_range"
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_advisory_lock_conflict(self, pg, monkeypatch):
        from fastapi import HTTPException
        from app.history_warmup_execute import HISTORY_WARMUP_ADVISORY_LOCK_KEY
        sym = "ZLOCK1"
        prov = _ready_provider(sym, NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            holder = await _conn(pg["warmer"])
            c = await _conn(pg["warmer"])
            try:
                got = await holder.fetchval("SELECT pg_try_advisory_lock($1)",
                                            HISTORY_WARMUP_ADVISORY_LOCK_KEY)
                assert got
                pf = await _preflight(c, sym)
                with pytest.raises(HTTPException) as e:
                    await _execute(c, _body_from(pf))
                assert e.value.status_code == 409
                assert e.value.detail["error"] == "history_warmup_execution_locked"
                assert prov.call_count() == 0  # no provider/write after lock reject
            finally:
                await holder.fetchval("SELECT pg_advisory_unlock($1)",
                                      HISTORY_WARMUP_ADVISORY_LOCK_KEY)
                await holder.close()
                await c.close()
        asyncio.run(drive())


class TestCrashRecovery:
    def test_crash_after_run_marker_before_provider_then_redrive(self, pg, monkeypatch):
        """Simulate a PROCESS CRASH at bounded point (a): the durable run marker
        was committed ('running') but the provider was never called and no bars /
        run item exist. Re-driving the SAME identity re-enters idempotently:
        reuses the marker (no new run), completes, and writes exactly one item +
        the bars with no duplication."""
        from tests.support.fake_provider import FakeProvider, make_ready_daily, make_ready_4h
        from app.history_warmup_execute import execution_identity
        import json
        sym = "ZCRASH1"
        now = NOWFN()
        prov = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date())},
                            intraday={sym: make_ready_4h(sym, now=now)})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                pf = await _preflight(c, sym)
                body = _body_from(pf)
                # compute the identity the handler WILL compute, and pre-insert a
                # crashed 'running' marker (warmer may INSERT runs) — no bars/item.
                ident = execution_identity(
                    mode="normal", universe_hash=body["universe_hash"],
                    config_hash=body["config_hash"],
                    plan_hash=body["readiness_manifest_hash"],
                    next_batch_hash=body["next_batch_hash"], symbols=[sym])
                await c.execute(
                    "INSERT INTO history_warmup_runs(mode,status,universe_hash,"
                    "readiness_manifest_hash,requested_symbols,requested_symbol_count,"
                    "idempotency_key,started_at,created_at,updated_at) "
                    "VALUES('normal','running',$1,$2,$3::jsonb,1,$4,NOW(),NOW(),NOW())",
                    body["universe_hash"], body["readiness_manifest_hash"],
                    json.dumps([sym]), ident)
                stuck_id = await c.fetchval(
                    "SELECT id FROM history_warmup_runs WHERE idempotency_key=$1", ident)
                assert prov.call_count() == 0
                # re-drive with the identical body -> recover
                res = await _execute(c, body)
                assert res["status"] == "executed"
                assert res["run_id"] == str(stuck_id)      # reused the marker
                assert prov.call_count() == 2
                # no duplicate run / item / bars
                assert await c.fetchval("SELECT count(*) FROM history_warmup_runs "
                                        "WHERE idempotency_key=$1", ident) == 1
                assert await c.fetchval("SELECT count(*) FROM history_warmup_run_items "
                                        "WHERE symbol=$1", sym) == 1
                assert await c.fetchval("SELECT count(*) FROM market_bars_4h "
                                        "WHERE symbol=$1", sym) == 12
            finally:
                await c.close()
        asyncio.run(drive())


class TestRolePrivileges:
    def test_audit_reader_cannot_execute_writes(self, pg):
        async def drive():
            c = await _conn(pg["audit"])
            try:
                await c.fetchval("SELECT count(*) FROM market_bars_4h")  # SELECT ok
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(
                        "INSERT INTO history_warmup_runs(status,requested_symbol_count) "
                        "VALUES('planned',1)")
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(
                        "INSERT INTO history_warmup_run_items(run_id,symbol,execution_identity) "
                        "VALUES(gen_random_uuid(),'ZZ','x')")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_outcome_maintainer_cannot_write_bars(self, pg):
        async def drive():
            c = await _conn(pg["maint"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute(
                        "INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,"
                        "open,high,low,close,volume,is_completed,is_regular_session,"
                        "content_fingerprint) VALUES('ZZ','2026-06-11 13:30:00+00',"
                        "'2026-06-11 17:30:00+00','2026-06-11',1,2,0.5,1.5,1,true,true,'fp')")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_history_warmer_cannot_write_campaign_or_outcomes(self, pg):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                for stmt in (
                    "INSERT INTO strategy_shadow_runs(id,experiment_code,experiment_version,status) "
                    "VALUES(gen_random_uuid(),'x','v','running')",
                    "INSERT INTO strategy_shadow_pair_outcomes(id,pair_id,outcome_fingerprint,"
                    "outcome_fingerprint_version,calculation_version,outcome_coverage_version,"
                    "forward_frame_version,reference_price_role) VALUES(gen_random_uuid(),"
                    "gen_random_uuid(),'fp','v','v','v','v','paired_decision_observation')",
                    "DELETE FROM market_bars_4h",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await c.execute(stmt)
            finally:
                await c.close()
        asyncio.run(drive())
