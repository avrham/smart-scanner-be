"""Real-Postgres integration for crash-safe warmup execute v2 + frozen universes.

Isolated local Docker Postgres (never Supabase/Massive): migrations incl.
014/015/016, the history-warmer role + RLS + universe triggers, a deterministic
FAKE provider injected via app.routers.admin._resolve_history_warmup_provider.
Drives the endpoint handlers directly on a real warmer asyncpg connection. A
network guard fails any non-loopback socket. Skips without Docker.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "hwx2db"
WARMER_PW = "warmerpw_local_only_not_secret"
MAINT_PW = "maintpw_local_only_not_secret"
AUDIT_PW = "auditpw_local_only_not_secret"
WARMER = "smart_scanner_history_warmer"
MAINT = "smart_scanner_outcome_maintainer"
AUDIT = "smart_scanner_audit_reader"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "012_wyckoff_mtf_v2", "013_wyckoff_v2_shadow_arms",
              "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes"]
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
               "audit": f"postgresql://{AUDIT}:{AUDIT_PW}@127.0.0.1:{hp}/{DBNAME}",
               "owner": f"postgresql://postgres:postgres@127.0.0.1:{hp}/{DBNAME}"}
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(autouse=True)
def _clean_db(pg):
    _psql(pg["cid"], "TRUNCATE history_warmup_run_items, history_warmup_runs, "
                     "history_warmup_universe_symbols, market_bars_4h, daily_bars CASCADE; "
                     "DELETE FROM history_warmup_universes;")
    yield


@pytest.fixture(autouse=True)
def _no_external_network(monkeypatch):
    real_connect = socket.socket.connect

    def guard(self, address):
        host = address[0] if isinstance(address, tuple) else str(address)
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"external network access blocked: {host}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guard)


@pytest.fixture(autouse=True)
def _warmup_settings(monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "HISTORY_WARMUP_ONLY_MODE", True)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", False)
    monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH", 1)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS", 0)
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fake")  # bypasses 60s Massive floor
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_EXECUTION_LEASE_SECONDS", 120)


def _inject(monkeypatch, provider):
    import app.routers.admin as admin
    monkeypatch.setattr(admin, "_resolve_history_warmup_provider", lambda: provider)


async def _conn(dsn):
    return await asyncpg.connect(dsn)

NOWFN = lambda: datetime.now(timezone.utc)  # noqa: E731


async def _create_universe(conn, code, symbols, freeze=True):
    from app.routers.admin import history_warmup_create_universe
    return await history_warmup_create_universe(_="t", db=conn, body={
        "contract_version": "history_warmup_universe_create.v1",
        "universe_code": code, "universe_version": 1, "symbols": symbols, "freeze": freeze})


async def _pf(conn, universe_id):
    from app.routers.admin import history_warmup_preflight
    return await history_warmup_preflight(_="t", db=conn, universe_id=universe_id)


async def _execute(conn, body):
    from app.routers.admin import history_warmup_execute
    return await history_warmup_execute(_="t", db=conn, body=body)


def _body(pf, mode="normal"):
    nb = pf["next_batch"]
    body = {"contract_version": "history_warmup_execute.v2", "mode": mode,
            "universe_id": pf["universe_id"], "universe_hash": pf["universe_hash"],
            "config_hash": pf["config_hash"], "next_batch_hash": nb["next_batch_hash"],
            "symbols": list(nb["symbols"]), "limit": len(nb["symbols"])}
    if mode == "normal":
        body["readiness_manifest_hash"] = pf["combined_readiness_manifest_hash"]
    else:
        body["retry_plan_hash"] = pf["retry_plan_hash"]
    return body


def _ready_provider(symbols, now):
    from tests.support.fake_provider import FakeProvider, make_ready_daily, make_ready_4h
    if isinstance(symbols, str):
        symbols = [symbols]
    return FakeProvider(daily={s: make_ready_daily(s, today=now.date()) for s in symbols},
                        intraday={s: make_ready_4h(s, now=now) for s in symbols})


# =========================================================================== #
class TestUniverseCreateAndPreflightV3:
    def test_create_freeze_and_preflight_loads_members(self, pg, monkeypatch):
        _inject(monkeypatch, _ready_provider(["AAA", "BBB", "CCC"], NOWFN()))

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UONE", ["ccc", "AAA", "bbb", "AAA"])
                assert u["status"] == "frozen" and u["symbol_count"] == 3
                assert u["duplicates_removed"] == 1 and u["symbols"] == ["CCC", "AAA", "BBB"]
                assert u["universe_hash"].startswith("sha256:")
                pf = await _pf(c, u["universe_id"])
                assert pf["contract_version"] == "history_warmup_preflight.v3"
                assert pf["symbol_count"] == 3 and pf["universe_id"] == u["universe_id"]
                assert pf["universe_hash"] == u["universe_hash"]
                # all three loaded from DB, none ready yet
                assert set(pf["normal_pending_symbols"]) == {"AAA", "BBB", "CCC"}
                # server-selected next batch = first pending (sorted), one symbol
                assert pf["next_batch"]["symbols"] == ["AAA"]
            finally:
                await c.close()
        asyncio.run(drive())


class TestExecuteHappyAndReplay:
    def test_full_warm_call_count_persistence_flip(self, pg, monkeypatch):
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UWARM", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                assert pf["readiness"]["symbols"][0]["both_ready"] is False
                res = await _execute(c, _body(pf))
                assert res["status"] == "executed"
                assert prov.call_count() == 2 and res["provider_request_count"] == 2
                assert res["daily"]["inserted"] > 200 and res["four_hour"]["inserted"] == 12
                assert res["readiness_after"]["both_ready"] is True
                assert res["universe_id"] == u["universe_id"]
                # provider activity recorded -> completed
                st = await c.fetchval("SELECT provider_activity_state FROM history_warmup_runs "
                                      "WHERE id=$1", res["run_id"])
                assert st == "completed"
            finally:
                await c.close()
        asyncio.run(drive())

    def test_replay_already_applied_bypasses_cooldown(self, pg, monkeypatch):
        from app.config import settings
        prov = _ready_provider(["AAA", "BBB"], NOWFN())
        _inject(monkeypatch, prov)
        monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 120)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UREPLAY", ["AAA", "BBB"])
                pf = await _pf(c, u["universe_id"])
                body = _body(pf)                       # AAA
                r1 = await _execute(c, body)
                assert r1["status"] == "executed"
                calls = prov.call_count()
                # (Part 12) identical replay during ACTIVE cooldown -> already_applied 200
                r2 = await _execute(c, body)
                assert r2["status"] == "already_applied" and r2["run_id"] == r1["run_id"]
                assert prov.call_count() == calls       # no provider
                # (Part 12) a FRESH next-symbol batch during cooldown -> 409 cooldown
                pf2 = await _pf(c, u["universe_id"])
                assert pf2["next_batch"]["symbols"] == ["BBB"]
                assert pf2["execution_allowed_by_cooldown"] is False
                from fastapi import HTTPException
                with pytest.raises(HTTPException) as e:
                    await _execute(c, _body(pf2))
                assert e.value.status_code == 409
                assert e.value.detail["error"] == "provider_cooldown_active"
                assert prov.call_count() == calls       # still no new provider call
            finally:
                await c.close()
        asyncio.run(drive())


class TestInProgressReplay:
    def test_active_lease_identical_returns_in_progress(self, pg, monkeypatch):
        from fastapi import HTTPException
        from app.history_warmup_execute import execution_identity
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UPROG", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                body = _body(pf)
                ident = execution_identity(
                    mode="normal", universe_id=u["universe_id"], universe_hash=body["universe_hash"],
                    config_hash=body["config_hash"], plan_hash=body["readiness_manifest_hash"],
                    next_batch_hash=body["next_batch_hash"], symbols=["AAA"])
                # a live run holds an ACTIVE lease
                await c.execute(
                    "INSERT INTO history_warmup_runs(mode,status,universe_id,requested_symbols,"
                    "requested_symbol_count,idempotency_key,provider_activity_state,"
                    "execution_lease_expires_at,started_at,created_at,updated_at) "
                    "VALUES('normal','running',$1,$2::jsonb,1,$3,'started',NOW()+interval '90 seconds',"
                    "NOW(),NOW(),NOW())", u["universe_id"], json.dumps(["AAA"]), ident)
                with pytest.raises(HTTPException) as e:
                    await _execute(c, body)
                assert e.value.status_code == 409
                assert e.value.detail["error"] == "history_warmup_execution_in_progress"
                assert "lease_expires_at" in e.value.detail
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())


class TestProviderConstructionNegative:
    """Part 20: provider must NOT be constructed/called for any of these."""
    def test_no_provider_for_rejections(self, pg, monkeypatch):
        from fastapi import HTTPException
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UNEG", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                good = _body(pf)
                # stale manifest
                b = dict(good); b["readiness_manifest_hash"] = "sha256:stale"
                with pytest.raises(HTTPException):
                    await _execute(c, b)
                # stale next batch
                b = dict(good); b["next_batch_hash"] = "sha256:stale"
                with pytest.raises(HTTPException):
                    await _execute(c, b)
                # unknown universe
                b = dict(good); b["universe_id"] = "00000000-0000-0000-0000-000000000000"
                with pytest.raises(HTTPException) as e:
                    await _execute(c, b)
                assert e.value.status_code == 404
                # symbol not in universe
                b = dict(good); b["symbols"] = ["ZZZ"]
                with pytest.raises(HTTPException) as e2:
                    await _execute(c, b)
                assert e2.value.detail["error"] == "symbol_not_in_universe"
                # batch size > 1
                b = dict(good); b["symbols"] = ["AAA", "BBB"]; b["limit"] = 2
                with pytest.raises(HTTPException):
                    await _execute(c, b)
                assert prov.call_count() == 0           # NEVER constructed/called
            finally:
                await c.close()
        asyncio.run(drive())

    def test_no_provider_for_non_frozen_universe(self, pg, monkeypatch):
        from fastapi import HTTPException
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                # draft (not frozen) universe
                u = await _create_universe(c, "UDRAFT", ["AAA"], freeze=False)
                body = {"contract_version": "history_warmup_execute.v2", "mode": "normal",
                        "universe_id": u["universe_id"], "universe_hash": "sha256:x",
                        "config_hash": "sha256:x", "readiness_manifest_hash": "sha256:x",
                        "next_batch_hash": "sha256:x", "symbols": ["AAA"], "limit": 1}
                with pytest.raises(HTTPException) as e:
                    await _execute(c, body)
                assert e.value.status_code == 409 and e.value.detail["error"] == "universe_not_frozen"
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())


# --------------------------------------------------------------------------- #
async def _stuck_run(c, identity, universe_id, symbols, *, activity_state,
                     activity_started_ago=None, lease_ago_seconds=10):
    """Insert an ABANDONED (lease-expired) running run with the given identity."""
    started = (f"NOW() - interval '{activity_started_ago} seconds'"
               if activity_started_ago is not None else "NULL")
    await c.execute(
        f"INSERT INTO history_warmup_runs(mode,status,universe_id,requested_symbols,"
        f"requested_symbol_count,idempotency_key,provider_activity_state,"
        f"provider_activity_started_at,last_provider_activity_at,execution_lease_expires_at,"
        f"started_at,created_at,updated_at) VALUES('normal','running',$1,$2::jsonb,"
        f"{len(symbols)},$3,$4,{started},{started},NOW()-interval '{lease_ago_seconds} seconds',"
        f"NOW(),NOW(),NOW())", universe_id, json.dumps(symbols), identity, activity_state)
    return await c.fetchval("SELECT id FROM history_warmup_runs WHERE idempotency_key=$1", identity)


def _ident(u_id, body, symbols):
    from app.history_warmup_execute import execution_identity
    return execution_identity(mode="normal", universe_id=u_id, universe_hash=body["universe_hash"],
                              config_hash=body["config_hash"], plan_hash=body["readiness_manifest_hash"],
                              next_batch_hash=body["next_batch_hash"], symbols=symbols)


class TestCrashRecovery:
    def test_A_crash_before_provider_activity_no_cooldown_redrive(self, pg, monkeypatch):
        """Crash after run marker, BEFORE provider activity (state='none'):
        no provider cooldown, expired lease permits safe re-drive, one item."""
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UCA", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                body = _body(pf)
                ident = _ident(u["universe_id"], body, ["AAA"])
                run_id = await _stuck_run(c, ident, u["universe_id"], ["AAA"],
                                          activity_state="none")
                # cooldown NOT established by a pre-provider crash
                pf2 = await _pf(c, u["universe_id"])
                assert pf2["execution_allowed_by_cooldown"] is True
                res = await _execute(c, body)           # re-drive
                assert res["status"] == "executed" and res["run_id"] == str(run_id)
                assert prov.call_count() == 2
                assert await c.fetchval("SELECT count(*) FROM history_warmup_run_items "
                                        "WHERE symbol='AAA'") == 1
                assert await c.fetchval("SELECT count(*) FROM market_bars_4h WHERE symbol='AAA'") == 12
            finally:
                await c.close()
        asyncio.run(drive())

    def test_B_crash_after_activity_marker_cooldown_fail_closed(self, pg, monkeypatch):
        """Crash after provider-activity marker (state='started'), before any real
        call: cooldown activates FAIL-CLOSED even though no call occurred; a fresh
        different-symbol batch is blocked."""
        from fastapi import HTTPException
        from app.config import settings
        monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 120)
        prov = _ready_provider(["AAA", "BBB"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UCB", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                body = _body(pf)                        # AAA
                ident = _ident(u["universe_id"], body, ["AAA"])
                # crash after the provider-activity marker (state='started') but
                # before any real call — no bars persisted, lease expired.
                await _stuck_run(c, ident, u["universe_id"], ["AAA"],
                                 activity_state="started", activity_started_ago=1)
                # fail-closed: the activity marker ALONE establishes cooldown even
                # though no provider call actually completed.
                pf2 = await _pf(c, u["universe_id"])
                assert pf2["execution_allowed_by_cooldown"] is False
                # re-driving the same abandoned identity is blocked by cooldown
                # BEFORE any provider call (conservative / fail-closed).
                with pytest.raises(HTTPException) as e:
                    await _execute(c, body)
                assert e.value.detail["error"] == "provider_cooldown_active"
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_CDE_crash_after_call_cooldown_then_redrive_no_dup(self, pg, monkeypatch):
        """Crash after daily call, before 4H (state='started', activity recent):
        provider cooldown active; after it expires re-drive completes 4H with no
        duplicate daily bars."""
        from tests.support.fake_provider import FakeProvider, make_ready_daily, make_ready_4h
        from app.config import settings
        sym = "AAA"
        now = NOWFN()
        # pre-persist daily as if the crash happened after daily upsert
        prov = FakeProvider(daily={sym: make_ready_daily(sym, today=now.date())},
                            intraday={sym: make_ready_4h(sym, now=now)})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UCE", [sym])
                pf = await _pf(c, u["universe_id"])
                body = _body(pf)
                ident = _ident(u["universe_id"], body, [sym])
                # simulate: daily already persisted by the crashed attempt
                from app.history_warmup_execute import normalize_daily_bars, upsert_daily_bars
                await upsert_daily_bars(c, normalize_daily_bars(
                    make_ready_daily(sym, today=now.date()), now=now), source="fake")
                daily_before = await c.fetchval("SELECT count(*) FROM daily_bars WHERE symbol=$1", sym)
                run_id = await _stuck_run(c, ident, u["universe_id"], [sym],
                                          activity_state="started", activity_started_ago=1)
                # cooldown active (provider activity happened)
                monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 120)
                from fastapi import HTTPException
                with pytest.raises(HTTPException) as e:
                    await _execute(c, body)
                assert e.value.detail["error"] == "provider_cooldown_active"
                # after cooldown clears (interval 0) re-drive completes, no dup daily
                monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0)
                res = await _execute(c, body)
                assert res["status"] == "executed" and res["run_id"] == str(run_id)
                assert await c.fetchval("SELECT count(*) FROM daily_bars WHERE symbol=$1", sym) == daily_before
                assert await c.fetchval("SELECT count(*) FROM market_bars_4h WHERE symbol=$1", sym) == 12
            finally:
                await c.close()
        asyncio.run(drive())

    def test_F_crash_after_persist_reconciled_without_provider(self, pg, monkeypatch):
        """Crash after ALL bars persisted, before finalization: an expired running
        run whose persisted local bars satisfy the exact requested readiness is
        finalized reconciled_complete WITHOUT a second provider call."""
        sym = "AAA"
        now = NOWFN()
        prov = _ready_provider([sym], now)
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UCF", [sym])
                # first, genuinely warm the symbol to ready (completed run, diff identity)
                pf = await _pf(c, u["universe_id"])
                await _execute(c, _body(pf))
                calls_after_warm = prov.call_count()
                assert (await _pf(c, u["universe_id"]))["readiness"]["symbols"][0]["both_ready"] is True
                # now a DIFFERENT abandoned run (state started) exists for the same
                # ready symbol -> reconcile from persisted bars, no provider call
                fake_body = {"universe_hash": "sha256:x", "config_hash": "sha256:x",
                             "readiness_manifest_hash": "sha256:x", "next_batch_hash": "sha256:x"}
                ident = _ident(u["universe_id"], fake_body, [sym])
                run_id = await _stuck_run(c, ident, u["universe_id"], [sym],
                                          activity_state="started", activity_started_ago=5)
                exec_body = {"contract_version": "history_warmup_execute.v2", "mode": "normal",
                             "universe_id": u["universe_id"], "symbols": [sym], "limit": 1, **fake_body}
                res = await _execute(c, exec_body)
                assert res["status"] == "reconciled_complete" and res["run_id"] == str(run_id)
                assert prov.call_count() == calls_after_warm    # NO extra provider call
                row = await c.fetchrow("SELECT status, reconciled FROM history_warmup_runs WHERE id=$1", run_id)
                assert row["status"] == "completed" and row["reconciled"] is True
            finally:
                await c.close()
        asyncio.run(drive())


class TestMultiSymbolProgression:
    def test_three_symbol_progression(self, pg, monkeypatch):
        prov = _ready_provider(["AAA", "BBB", "CCC"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UMULTI", ["AAA", "BBB", "CCC"])
                pf1 = await _pf(c, u["universe_id"])
                assert pf1["symbol_count"] == 3
                first_hash = pf1["combined_readiness_manifest_hash"]
                assert pf1["next_batch"]["symbols"] == ["AAA"]        # sorted first
                r1 = await _execute(c, _body(pf1))
                assert r1["status"] == "executed"
                # exact replay -> already_applied (interval 0 so cooldown clear, but
                # identity match still short-circuits before provider)
                assert (await _execute(c, _body(pf1)))["status"] == "already_applied"
                pf2 = await _pf(c, u["universe_id"])
                assert pf2["next_batch"]["symbols"] == ["BBB"]        # second selected
                assert pf2["combined_readiness_manifest_hash"] != first_hash  # readiness changed
                assert pf2["universe_hash"] == pf1["universe_hash"]   # universe unchanged
                # client cannot submit CCC early (not the server batch)
                from fastapi import HTTPException
                bad = _body(pf2); bad["symbols"] = ["CCC"]
                with pytest.raises(HTTPException) as e:
                    await _execute(c, bad)
                assert e.value.detail["reason"] == "symbols_not_server_selected_batch"
                await _execute(c, _body(pf2))                        # BBB
                pf3 = await _pf(c, u["universe_id"])
                await _execute(c, _body(pf3))                        # CCC
                pf4 = await _pf(c, u["universe_id"])
                assert pf4["normal_complete"] is True
                assert pf4["next_batch"]["available"] is False
            finally:
                await c.close()
        asyncio.run(drive())

    def test_retry_first_within_universe(self, pg, monkeypatch):
        from tests.support.fake_provider import (
            FakeProvider, make_ready_daily, make_ready_4h, rate_limited_error)
        now = NOWFN()
        failing = FakeProvider(daily={"AAA": make_ready_daily("AAA", today=now.date())},
                               intraday_error=rate_limited_error())
        _inject(monkeypatch, failing)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "URETRY", ["AAA", "BBB"])
                pf = await _pf(c, u["universe_id"])
                res = await _execute(c, _body(pf))       # AAA fails retryably
                assert res["status"] == "failed" and res["error"]["retryable"] is True
                pf2 = await _pf(c, u["universe_id"])
                # retry-first: next batch is the retryable symbol in retry mode
                assert pf2["retryable_symbols"] == ["AAA"]
                assert pf2["next_batch"]["mode"] == "retry" and pf2["next_batch"]["symbols"] == ["AAA"]
                ok = FakeProvider(daily={"AAA": make_ready_daily("AAA", today=now.date())},
                                  intraday={"AAA": make_ready_4h("AAA", now=now)})
                _inject(monkeypatch, ok)
                r2 = await _execute(c, _body(pf2, mode="retry"))
                assert r2["status"] == "executed"
                assert (await _pf(c, u["universe_id"]))["retryable_symbols"] == []
            finally:
                await c.close()
        asyncio.run(drive())


class TestFrozenUniverseImmutability:
    def test_frozen_membership_and_status_are_immutable(self, pg, monkeypatch):
        _inject(monkeypatch, _ready_provider(["AAA"], NOWFN()))

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UIMMUT", ["AAA", "BBB"])
                uid = u["universe_id"]
            finally:
                await c.close()
            # DB-level enforcement: even the OWNER cannot mutate frozen membership.
            r = _psql(pg["cid"], f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) "
                                 f"VALUES('{uid}','CCC',9);")
            assert r.returncode != 0 and "immutable" in (r.stderr + r.stdout)
            r = _psql(pg["cid"], f"UPDATE history_warmup_universe_symbols SET symbol='ZZZ' "
                                 f"WHERE universe_id='{uid}';")
            assert r.returncode != 0 and "immutable" in (r.stderr + r.stdout)
            r = _psql(pg["cid"], f"DELETE FROM history_warmup_universe_symbols WHERE universe_id='{uid}';")
            assert r.returncode != 0 and "immutable" in (r.stderr + r.stdout)
            # frozen -> draft transition denied
            r = _psql(pg["cid"], f"UPDATE history_warmup_universes SET status='draft' WHERE id='{uid}';")
            assert r.returncode != 0
        asyncio.run(drive())

    def test_draft_membership_mutable_then_superseded_cannot_execute(self, pg, monkeypatch):
        from fastapi import HTTPException
        _inject(monkeypatch, _ready_provider(["AAA"], NOWFN()))

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UDRAFT2", ["AAA"], freeze=False)
                uid = u["universe_id"]
                # draft membership CAN be extended by the warmer
                await c.execute("INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) "
                                "VALUES($1,'BBB',1)", uid)
                # supersede a (frozen) universe and prove it cannot execute
                u2 = await _create_universe(c, "USUP", ["AAA"])
                await c.close()
                _psql(pg["cid"], f"UPDATE history_warmup_universes SET status='superseded',"
                                 f"superseded_at=NOW() WHERE id='{u2['universe_id']}';")
                c2 = await _conn(pg["warmer"])
                try:
                    body = {"contract_version": "history_warmup_execute.v2", "mode": "normal",
                            "universe_id": u2["universe_id"], "universe_hash": "sha256:x",
                            "config_hash": "sha256:x", "readiness_manifest_hash": "sha256:x",
                            "next_batch_hash": "sha256:x", "symbols": ["AAA"], "limit": 1}
                    with pytest.raises(HTTPException) as e:
                        await _execute(c2, body)
                    assert e.value.detail["error"] == "universe_not_frozen"
                finally:
                    await c2.close()
            finally:
                try:
                    await c.close()
                except Exception:
                    pass
        asyncio.run(drive())


class TestCooldownAcrossFailureTypes:
    def test_cooldown_established_by_success_and_retryable_and_terminal(self, pg, monkeypatch):
        from tests.support.fake_provider import (
            FakeProvider, make_ready_daily, make_ready_4h, make_invalid_4h, rate_limited_error)
        from app.config import settings
        now = NOWFN()

        async def _activity_present(c):
            n = await c.fetchval("SELECT count(*) FROM history_warmup_runs "
                                 "WHERE provider_activity_state <> 'none'")
            return n > 0

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                # success
                _inject(monkeypatch, FakeProvider(daily={"AAA": make_ready_daily("AAA", today=now.date())},
                                                  intraday={"AAA": make_ready_4h("AAA", now=now)}))
                u = await _create_universe(c, "UCS", ["AAA"])
                await _execute(c, _body(await _pf(c, u["universe_id"])))
                assert await _activity_present(c)
                _psql(pg["cid"], "TRUNCATE history_warmup_run_items, history_warmup_runs CASCADE;")
                # retryable
                _inject(monkeypatch, FakeProvider(daily={"BBB": make_ready_daily("BBB", today=now.date())},
                                                  intraday_error=rate_limited_error()))
                u2 = await _create_universe(c, "UCR", ["BBB"])
                await _execute(c, _body(await _pf(c, u2["universe_id"])))
                assert await _activity_present(c)
                _psql(pg["cid"], "TRUNCATE history_warmup_run_items, history_warmup_runs CASCADE;")
                # terminal (invalid bar)
                _inject(monkeypatch, FakeProvider(daily={"CCC": make_ready_daily("CCC", today=now.date())},
                                                  intraday={"CCC": make_invalid_4h("CCC", now=now)}))
                u3 = await _create_universe(c, "UCT", ["CCC"])
                await _execute(c, _body(await _pf(c, u3["universe_id"])))
                assert await _activity_present(c)
            finally:
                await c.close()
        asyncio.run(drive())

    def test_cooldown_not_established_by_pre_provider_rejections(self, pg, monkeypatch):
        from fastapi import HTTPException
        prov = _ready_provider(["AAA"], NOWFN())
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "UCN", ["AAA"])
                pf = await _pf(c, u["universe_id"])
                stale = _body(pf); stale["readiness_manifest_hash"] = "sha256:stale"
                with pytest.raises(HTTPException):
                    await _execute(c, stale)
                # no run created, no provider activity, cooldown still clear
                assert await c.fetchval("SELECT count(*) FROM history_warmup_runs") == 0
                assert (await _pf(c, u["universe_id"]))["execution_allowed_by_cooldown"] is True
                assert prov.call_count() == 0
            finally:
                await c.close()
        asyncio.run(drive())


class TestUniverseRolePrivileges:
    def test_warmer_cannot_delete_universe_or_mutate_frozen(self, pg):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                u = await _create_universe(c, "URLS", ["AAA"])
                uid = u["universe_id"]
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("DELETE FROM history_warmup_universes WHERE id=$1", uid)
                # frozen membership INSERT denied (trigger fires even with grant)
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal)"
                                    " VALUES($1,'ZZZ',9)", uid)
            finally:
                await c.close()
        asyncio.run(drive())

    def test_audit_reader_can_select_universe_not_write(self, pg, monkeypatch):
        async def drive():
            w = await _conn(pg["warmer"])
            try:
                u = await _create_universe(w, "UAUD", ["AAA"])
            finally:
                await w.close()
            c = await _conn(pg["audit"])
            try:
                assert await c.fetchval("SELECT count(*) FROM history_warmup_universes") >= 1
                assert await c.fetchval("SELECT count(*) FROM history_warmup_universe_symbols") >= 1
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("INSERT INTO history_warmup_universes(universe_code,universe_version)"
                                    " VALUES('X',1)")
            finally:
                await c.close()
        asyncio.run(drive())

    def test_outcome_maintainer_cannot_write_universe(self, pg):
        async def drive():
            c = await _conn(pg["maint"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await c.execute("INSERT INTO history_warmup_universes(universe_code,universe_version)"
                                    " VALUES('M',1)")
            finally:
                await c.close()
        asyncio.run(drive())
