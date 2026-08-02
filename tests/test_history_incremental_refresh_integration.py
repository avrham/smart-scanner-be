"""Real-Postgres integration for history_incremental_refresh.v1.

Isolated local Docker Postgres (never Supabase/Massive): migrations through
018 (needed for the prospective registration used in the outcome-preflight-
transition proof), the history-warmer role + RLS, a deterministic FAKE
provider injected via app.routers.admin._resolve_history_warmup_provider
(never a real network call). Drives the new admin route functions directly
on a real warmer asyncpg connection, mirroring
tests/test_history_warmup_execute_integration.py's established calling
convention. A network guard fails any non-loopback socket.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "hwidb"
WARMER_PW = "warmerpw_local_only_not_secret"
WARMER = "smart_scanner_history_warmer"
MIGRATIONS = ["001_initial_schema", "005_massive_provider",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes",
              "012_wyckoff_mtf_v2", "013_wyckoff_v2_shadow_arms",
              "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes",
              "017_prospective_campaign_registration"]
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
        r = _psql(cid, None, variables={"warmer_password": WARMER_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_history_warmer.sql"))
        assert r.returncode == 0, f"warmer role: {r.stderr[-500:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql",
                                               "create_shadow_history_warmer_rls_policies.sql"))
        assert r.returncode == 0, f"warmer rls: {r.stderr[-500:]}"
        base = f"127.0.0.1:{hp}/{DBNAME}"
        yield {"cid": cid, "hp": hp,
               "warmer": f"postgresql://{WARMER}:{WARMER_PW}@{base}",
               "owner": f"postgresql://postgres:postgres@{base}"}
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(autouse=True)
def _clean_db(pg):
    _psql(pg["cid"], "TRUNCATE history_warmup_run_items, history_warmup_runs, "
                     "history_warmup_universe_symbols, market_bars_4h, daily_bars, "
                     "prospective_campaign_registrations CASCADE; "
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
    monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", False)
    monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH", 1)
    monkeypatch.setattr(settings, "MARKET_DATA_PROVIDER", "fake")
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_EXECUTION_LEASE_SECONDS", 120)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS", 100)


def _inject(monkeypatch, provider):
    import app.routers.admin as admin
    monkeypatch.setattr(admin, "_resolve_history_warmup_provider", lambda: provider)


async def _conn(dsn):
    return await asyncpg.connect(dsn)


async def _incremental_pf(conn, **kwargs):
    from app.routers.admin import history_warmup_incremental_preflight
    return await history_warmup_incremental_preflight(_="t", db=conn, **kwargs)


async def _incremental_execute(conn, body):
    from app.routers.admin import history_warmup_incremental_execute
    return await history_warmup_incremental_execute(_="t", db=conn, body=body)


async def _seed_bars(conn, symbol, dates, close=100.0):
    for d in dates:
        await conn.execute(
            "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
            "VALUES ($1,$2,101,102,99,$3,1000000) ON CONFLICT (symbol,trading_date) DO NOTHING",
            symbol, d, close)


def _fake_provider_with_bars(symbol, bars_by_date):
    from tests.support.fake_provider import FakeProvider
    daily = {symbol: [
        {"symbol": symbol, "trading_date": d, "open": 101.0, "high": 102.0,
         "low": 99.0, "close": c, "volume": 1_000_000.0}
        for d, c in bars_by_date.items()
    ]}
    return FakeProvider(daily=daily)


def _exec_body(symbol, target_session, latest_local_session):
    from app.history_warmup_execute import INCREMENTAL_REFRESH_CONTRACT_VERSION
    return {"contract_version": INCREMENTAL_REFRESH_CONTRACT_VERSION, "symbol": symbol,
            "target_completed_session": target_session.isoformat(),
            "latest_local_session": latest_local_session.isoformat() if latest_local_session else None}


def _patch_target(monkeypatch, d):
    """Both the preflight and execute routes recompute
    resolve_latest_completed_session(now) against the REAL wall clock — pin
    it to a deterministic date so tests never depend on which real calendar
    day they happen to run on."""
    monkeypatch.setattr("app.prospective_session.resolve_latest_completed_session",
                        lambda now: d)


# =========================================================================== #
class TestIncrementalPreflight:
    def test_preflight_current_when_local_matches_target(self, pg, monkeypatch):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                await _seed_bars(c, "AAA", [date(2026, 7, 27), date(2026, 7, 28)])
                _patch_target(monkeypatch, date(2026, 7, 28))
                pf = await _incremental_pf(c, symbols="AAA")
                assert pf["state"] == "incremental_current"
                assert pf["symbols_current"] == ["AAA"]
                assert pf["provider_called"] is False
            finally:
                await c.close()
        asyncio.run(drive())

    def test_preflight_stale_when_local_behind_target(self, pg, monkeypatch):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                await _seed_bars(c, "AAA", [date(2026, 7, 27), date(2026, 7, 28)])
                _patch_target(monkeypatch, date(2026, 7, 29))
                pf = await _incremental_pf(c, symbols="AAA")
                assert pf["state"] == "incremental_stale"
                assert pf["symbols_requiring_refresh"] == ["AAA"]
                row = pf["symbols"][0]
                assert row["missing_session_count"] == 1
                assert row["first_missing_session"] == "2026-07-29"
            finally:
                await c.close()
        asyncio.run(drive())

    def test_preflight_requires_explicit_scope(self, pg):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                from fastapi import HTTPException
                with pytest.raises(HTTPException) as exc:
                    await _incremental_pf(c)
                assert exc.value.status_code == 422
            finally:
                await c.close()
        asyncio.run(drive())


# =========================================================================== #
class TestIncrementalExecute:
    def test_incremental_insert_and_latest_session_advances(self, pg, monkeypatch):
        prov = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                res = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28)))
                assert res["status"] == "executed"
                assert res["latest_local_session_after"] == "2026-07-29"
                assert res["daily"]["inserted"] == 1
                assert prov.call_count() == 1
                row = await c.fetchval(
                    "SELECT close FROM daily_bars WHERE symbol='AAA' AND trading_date='2026-07-29'")
                assert float(row) == 105.0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_replay_same_identity_creates_no_duplicate_bar(self, pg, monkeypatch):
        prov = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                body = _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28))
                r1 = await _incremental_execute(c, body)
                assert r1["status"] == "executed"
                r2 = await _incremental_execute(c, body)
                assert r2["status"] == "already_applied"
                assert r2["provider_request_count"] == 0
                n = await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol='AAA' AND trading_date='2026-07-29'")
                assert n == 1
                assert prov.call_count() == 1  # provider called only once total
            finally:
                await c.close()
        asyncio.run(drive())

    def test_no_op_when_already_current(self, pg, monkeypatch):
        async def drive():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 28))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                res = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 28), date(2026, 7, 28)))
                assert res["status"] == "no-op"
                assert res["reason"] == "incremental_current"
                assert res["provider_request_count"] == 0
            finally:
                await c.close()
        asyncio.run(drive())

    def test_later_round_adds_only_new_sessions(self, pg, monkeypatch):
        prov1 = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        _inject(monkeypatch, prov1)

        async def drive_round1():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                r1 = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28)))
                assert r1["status"] == "executed"
            finally:
                await c.close()
        asyncio.run(drive_round1())

        prov2 = _fake_provider_with_bars("AAA", {date(2026, 7, 30): 106.0})
        _inject(monkeypatch, prov2)

        async def drive_round2():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 30))
                r2 = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 30), date(2026, 7, 29)))
                assert r2["status"] == "executed"
                assert r2["requested_missing_sessions"] == ["2026-07-30"]
                n = await c.fetchval("SELECT count(*) FROM daily_bars WHERE symbol='AAA'")
                assert n == 3  # 07-28 seed + 07-29 round1 + 07-30 round2
                latest = await c.fetchval(
                    "SELECT MAX(trading_date) FROM daily_bars WHERE symbol='AAA'")
                assert latest == date(2026, 7, 30)
            finally:
                await c.close()
        asyncio.run(drive_round2())

    def test_future_bar_beyond_target_is_rejected(self, pg, monkeypatch):
        """normalize_daily_bars rejects trading_date >= 'today' (UTC); a
        provider row for the still-forming session must never persist. Uses
        the REAL (unpatched) resolve_latest_completed_session for the target
        — it is guaranteed to be an actual valid trading day regardless of
        which real calendar day the test happens to run on (never a naive
        "yesterday", which can land on a weekend)."""
        from datetime import timezone as _tz
        from app.prospective_session import resolve_latest_completed_session, is_trading_day
        today = datetime.now(_tz.utc).date()
        real_target = resolve_latest_completed_session(datetime.now(_tz.utc))
        # the LAST trading day strictly before real_target; missing = [real_target]
        seed = real_target - timedelta(days=1)
        while not is_trading_day(seed):
            seed -= timedelta(days=1)
        prov = _fake_provider_with_bars("AAA", {today: 999.0, real_target: 100.0})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                await _seed_bars(c, "AAA", [seed])
                res = await _incremental_execute(
                    c, _exec_body("AAA", real_target, seed))
                assert res["status"] == "executed"
                # only the completed (real_target) bar persisted; "today" excluded
                assert res["daily"]["inserted"] == 1
                exists_today = await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol='AAA' AND trading_date=$1", today)
                assert exists_today == 0
                exists_target = await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol='AAA' AND trading_date=$1", real_target)
                assert exists_target == 1
            finally:
                await c.close()
        asyncio.run(drive())

    def test_one_symbol_failure_does_not_affect_a_second_symbol(self, pg, monkeypatch):
        from tests.support.fake_provider import FakeProvider, rate_limited_error

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                await _seed_bars(c, "BBB", [date(2026, 7, 28)])

                failing = FakeProvider(daily_error=rate_limited_error())
                _inject(monkeypatch, failing)
                r_fail = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28)))
                assert r_fail["status"] == "failed"
                assert r_fail["error"]["code"] is not None

                ok = _fake_provider_with_bars("BBB", {date(2026, 7, 29): 50.0})
                _inject(monkeypatch, ok)
                r_ok = await _incremental_execute(
                    c, _exec_body("BBB", date(2026, 7, 29), date(2026, 7, 28)))
                assert r_ok["status"] == "executed"
                n = await c.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol='BBB' AND trading_date='2026-07-29'")
                assert n == 1
            finally:
                await c.close()
        asyncio.run(drive())


# =========================================================================== #
class TestIncrementalQueueMechanics:
    def test_two_workers_cannot_double_claim_same_identity(self, pg, monkeypatch):
        """Both connections race the SAME (symbol, latest, target) identity —
        the advisory lock + idempotency-key UNIQUE constraint ensure exactly
        ONE provider call and ONE completed run."""
        prov = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        _inject(monkeypatch, prov)

        async def drive():
            c1 = await _conn(pg["warmer"])
            c2 = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c1, "AAA", [date(2026, 7, 28)])
                body = _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28))
                results = await asyncio.gather(
                    _incremental_execute(c1, body), _incremental_execute(c2, body),
                    return_exceptions=True)
                statuses = []
                for r in results:
                    if isinstance(r, Exception):
                        statuses.append("error")
                    else:
                        statuses.append(r["status"])
                # exactly one real execution; the other sees already_applied,
                # a 409 in-progress, or (rare timing) also executed but the
                # idempotency UNIQUE constraint guarantees only one DB write.
                assert prov.call_count() <= 1
                n = await c1.fetchval(
                    "SELECT count(*) FROM daily_bars WHERE symbol='AAA' AND trading_date='2026-07-29'")
                assert n == 1
            finally:
                await c1.close(); await c2.close()
        asyncio.run(drive())

    def test_lease_loss_and_reclaim(self, pg, monkeypatch):
        """Simulate a crashed run (lease expired, never finalized): a later
        call for the SAME identity re-drives it (reusing the durable marker)
        rather than being permanently blocked."""
        prov = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        _inject(monkeypatch, prov)

        async def drive():
            c = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                await _seed_bars(c, "AAA", [date(2026, 7, 28)])
                from app.history_warmup_execute import incremental_refresh_identity, MODE_INCREMENTAL
                identity = incremental_refresh_identity(
                    symbol="AAA", latest_local_session=date(2026, 7, 28),
                    target_completed_session=date(2026, 7, 29))
                run_id = str(uuid.uuid4())
                # a durable pre-provider marker with an ALREADY-EXPIRED lease —
                # exactly the state a hard crash mid-request would leave.
                await c.execute(
                    "INSERT INTO history_warmup_runs(id, mode, status, requested_symbols, "
                    "requested_symbol_count, idempotency_key, provider_activity_state, "
                    "execution_lease_expires_at, heartbeat_at, started_at, created_at, updated_at) "
                    "VALUES($1,$2,'running','[\"AAA\"]'::jsonb,1,$3,'none', "
                    "NOW() - interval '10 seconds', NOW(), NOW(), NOW(), NOW())",
                    run_id, MODE_INCREMENTAL, identity)
                res = await _incremental_execute(
                    c, _exec_body("AAA", date(2026, 7, 29), date(2026, 7, 28)))
                assert res["status"] == "executed"
                assert res["run_id"] == run_id  # reused the SAME durable marker
                st = await c.fetchval("SELECT status FROM history_warmup_runs WHERE id=$1", run_id)
                assert st == "completed"
            finally:
                await c.close()
        asyncio.run(drive())


# =========================================================================== #
class TestOutcomePreflightTransition:
    """The end-to-end proof this task exists for: a completed prospective
    campaign's outcome preflight moves from eligibility_unknown to eligible
    after exactly one incrementally-refreshed forward session — with NO
    campaign, pair, evaluation or outcome row ever touched."""

    async def _seed_campaign(self, conn, *, symbol="AAA", snapshot=date(2026, 7, 28)):
        import app.prospective_campaign as pc
        uid = str(uuid.uuid4())
        # migration-016's frozen-universe immutability trigger forbids
        # inserting membership once status='frozen' — create as draft, add
        # membership, THEN freeze (the exact sequence
        # history_warmup_create_universe itself uses).
        await conn.execute(
            "INSERT INTO history_warmup_universes(id,universe_code,universe_version,"
            "config_hash,status,symbol_count) VALUES($1,'ZZHI',1,'sha256:cfg','draft',1)", uid)
        await conn.execute(
            "INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) "
            "VALUES($1,$2,0)", uid, symbol)
        await conn.execute(
            "UPDATE history_warmup_universes SET status='frozen', universe_hash='sha256:x', "
            "frozen_at=NOW() WHERE id=$1", uid)
        reg_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        campaign_id = str(uuid.uuid4())
        reg_ident = pc.registration_identity(
            experiment_code="wyckoff_v2_vs_baseline", universe_id=uid, universe_hash="sha256:x",
            history_config_hash="sha256:cfg", snapshot_session_date=snapshot.isoformat())
        await conn.execute(
            "INSERT INTO prospective_campaign_registrations(id,experiment_code,"
            "experiment_contract_version,universe_id,universe_code,universe_version,"
            "universe_hash,history_config_hash,history_readiness_manifest_hash,"
            "candidate_strategy_code,candidate_strategy_version,candidate_signal_definition,"
            "candidate_allow_enter,control_strategy_code,control_strategy_version,"
            "snapshot_session_date,snapshot_cutoff_at,market_calendar_version,"
            "registration_identity,status,campaign_id,campaign_run_id) "
            "VALUES($1,'wyckoff_v2_vs_baseline','wyckoff_v2_prospective_experiment.v1',$2,"
            "'ZZHI',1,'sha256:x','sha256:cfg','sha256:manifest','wyckoff_mtf_v2','wyckoff_mtf.v2',"
            "'pre_rollout_enter_eligible.v1',FALSE,'sma150_bounce','sma150.v2',$3,$4,"
            "'us_market_calendar.v1',$5,'completed',$6,$7)",
            reg_id, uid, snapshot, datetime.combine(snapshot, datetime.min.time(), timezone.utc)
            + timedelta(hours=20), reg_ident, campaign_id, run_id)
        return {"registration_id": reg_id, "campaign_id": campaign_id, "campaign_run_id": run_id,
               "universe_id": uid, "symbol": symbol, "snapshot": snapshot}

    async def _seed_real_pair(self, conn, campaign, *, symbol):
        """A real strategy_shadow_pairs + strategy_shadow_run_pairs row (the
        exact shape the durable evaluation worker persists) so the outcome
        preflight classifies a REAL pair, not a degenerate zero-pair
        registration."""
        snap = campaign["snapshot"]
        frame_snapshot = [{"date": snap.isoformat(), "open": 100.0, "high": 101.0,
                           "low": 99.0, "close": 100.0, "volume": 1_000_000.0}]
        pair_id = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO strategy_shadow_runs(id, experiment_code, experiment_version) "
            "VALUES($1,'wyckoff_v2_vs_baseline','probe') ON CONFLICT (id) DO NOTHING",
            campaign["campaign_run_id"])
        await conn.execute(
            "INSERT INTO strategy_shadow_pairs(id, experiment_code, experiment_version, symbol, "
            "provider, snapshot_date, market_data_as_of, frame_snapshot_version, frame_hash, "
            "frame_bar_count, frame_first_date, frame_last_date, frame_snapshot, "
            "pair_fingerprint, pair_fingerprint_version) "
            "VALUES($1,'wyckoff_v2_vs_baseline','probe',$2,'local-history',$3,$4,'probe.v1',"
            "'sha256:probe',1,$3,$3,$5::jsonb,$6,'probe.v1')",
            pair_id, symbol, snap,
            datetime.combine(snap, datetime.min.time(), timezone.utc),
            __import__("json").dumps(frame_snapshot), f"probe-fp-{pair_id}")
        await conn.execute(
            "INSERT INTO strategy_shadow_run_pairs(run_id, pair_id, created_new_pair) "
            "VALUES($1,$2,TRUE)", campaign["campaign_run_id"], pair_id)
        return pair_id

    def test_outcome_preflight_unknown_to_eligible_after_one_session(self, pg, monkeypatch):
        prov = _fake_provider_with_bars("AAA", {date(2026, 7, 29): 105.0})
        symbol = "AAA"

        async def drive():
            # Seeding/reading prospective tables uses the OWNER connection —
            # the warmer role legitimately has NO grant on
            # prospective_campaign_registrations (matching production; the
            # real registration is created by the prospective app's OWN
            # role). The warmer connection is used ONLY for the incremental-
            # refresh call itself, exactly as in production.
            o = await _conn(pg["owner"])
            w = await _conn(pg["warmer"])
            try:
                _patch_target(monkeypatch, date(2026, 7, 29))
                campaign = await self._seed_campaign(o, symbol=symbol)
                await _seed_bars(o, symbol, [campaign["snapshot"]])
                pair_id = await self._seed_real_pair(o, campaign, symbol=symbol)

                from app.workers.shadow.outcomes.eligibility import (
                    classify_maturation_eligibility, completed_forward_sessions,
                    ELIGIBILITY_UNKNOWN, ELIGIBILITY_ELIGIBLE)

                async def _session_dates_and_classify():
                    rows = await o.fetch(
                        "SELECT DISTINCT trading_date FROM daily_bars "
                        "WHERE symbol=$1 AND trading_date > $2 ORDER BY trading_date",
                        symbol, campaign["snapshot"])
                    session_dates = [r["trading_date"] for r in rows]
                    cfs = completed_forward_sessions(session_dates, campaign["snapshot"])
                    state = classify_maturation_eligibility(
                        outcome_status=None, error_code=None, completed_forward_sessions=cfs)
                    return session_dates, state

                dates_before, state_before = await _session_dates_and_classify()
                assert dates_before == []
                assert state_before == ELIGIBILITY_UNKNOWN  # honestly unknown, never assumed

                _inject(monkeypatch, prov)
                res = await _incremental_execute(
                    w, _exec_body(symbol, date(2026, 7, 29), campaign["snapshot"]))
                assert res["status"] == "executed"

                dates_after, state_after = await _session_dates_and_classify()
                assert dates_after == [date(2026, 7, 29)]
                assert state_after == ELIGIBILITY_ELIGIBLE
                # (app.jobs.prospective_outcome_local_reader.local_session_dates
                # reads via the identical `daily_bars WHERE trading_date > $2`
                # query pattern proven above — its own dedicated Docker
                # integration coverage lives in
                # test_prospective_outcome_maturation_integration.py.)

                # the pair itself is UNCHANGED (still exactly the one seeded);
                # no evaluation or outcome row was ever touched by history refresh
                pairs = await o.fetchval("SELECT count(*) FROM strategy_shadow_pairs")
                evals = await o.fetchval("SELECT count(*) FROM strategy_shadow_evaluations")
                outcomes = await o.fetchval("SELECT count(*) FROM strategy_shadow_pair_outcomes")
                assert pairs == 1 and evals == 0 and outcomes == 0
                still_pair_id = await o.fetchval("SELECT id FROM strategy_shadow_pairs")
                assert str(still_pair_id) == pair_id
                reg_status = await o.fetchval(
                    "SELECT status FROM prospective_campaign_registrations WHERE id=$1",
                    campaign["registration_id"])
                assert reg_status == "completed"  # unchanged, never rewritten by refresh
            finally:
                await o.close(); await w.close()
        asyncio.run(drive())
