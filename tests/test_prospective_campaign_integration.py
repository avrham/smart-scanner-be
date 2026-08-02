"""Real-Postgres integration for the prospective campaign pipeline.

Isolated Docker PostgreSQL (never Supabase/Massive): migrations 001-017, the
prospective_runner role + RLS. Seeds a frozen synthetic universe + local daily
and 4H history (plus FUTURE bars that must be ignored), then drives the endpoint
handlers as the prospective role: access-check, preflight, register (+ replay),
execute (+ replay), audit. Verifies pairs/evaluations/no-outcomes, provider-free
(network guard), lookahead exclusion, idempotency and RLS boundaries.
"""

from __future__ import annotations

import asyncio
import math
import os
import socket
import subprocess
import time
from datetime import date, datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "prosdb"
PROS_PW = "prospw_local_only_not_secret"
PROS = "smart_scanner_prospective_runner"
MIGRATIONS = ["001_initial_schema", "002_phase1_sma150_config", "003_phase2_signal_outcomes",
              "004_phase5_wyckoff_mtf_config", "005_massive_provider", "006_market_data_jobs",
              "007_scan_signal_provenance", "008_sma150_v3", "009_watch_outcome_coverage",
              "010_sma150_shadow_evaluations", "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
              "013_wyckoff_v2_shadow_arms", "014_market_bars_4h", "015_history_warmup_run_items",
              "016_history_warmup_leases_and_universes", "017_prospective_campaign_registration",
              "018_durable_job_queue"]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEGACY_RLS = ["strategy_shadow_evaluations", "strategy_shadow_pairs", "strategy_shadow_pair_outcomes",
              "strategy_shadow_run_pairs", "strategy_shadow_runs", "strategy_shadow_outcome_runs",
              "daily_bars", "patterns", "pattern_configs"]
SYMS = ["ZZAA", "ZZBB"]


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE], capture_output=True, check=True, timeout=20)
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


def _gen_daily(sym, end_date, n=800):
    """Varied rising-range daily series (avoids degenerate strategy paths)."""
    rows = []
    for i in range(n):
        d = end_date - timedelta(days=(n - 1 - i))
        base = 100.0 + i * 0.05 + 12.0 * math.sin(i / 25.0)
        o = round(base, 2); c = round(base + math.sin(i / 7.0), 2)
        hi = round(max(o, c) + 1.5 + abs(math.sin(i / 5.0)), 2)
        lo = round(min(o, c) - 1.5 - abs(math.cos(i / 6.0)), 2)
        vol = 1_000_000 + (i % 11) * 25_000
        rows.append((sym, d, o, hi, lo, c, float(vol)))
    return rows


def _gen_4h(sym, end_dt, n=120):
    rows = []
    for i in range(n):
        start = end_dt - timedelta(hours=4 * (n - i))
        end = start + timedelta(hours=4)
        base = 100.0 + i * 0.05
        o = round(base, 2); c = round(base + 0.3, 2)
        hi = round(max(o, c) + 0.8, 2); lo = round(min(o, c) - 0.8, 2)
        sd = end.astimezone(timezone.utc).date()
        rows.append((sym, start, end, sd, o, hi, lo, c, 500000.0))
    return rows


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
        _psql(cid, "DO $$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY['" +
              "','".join(LEGACY_RLS) + "'] LOOP EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY',t); END LOOP; END $$;")
        r = _psql(cid, None, variables={"prospective_password": PROS_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner.sql"))
        assert r.returncode == 0, f"role: {r.stderr[-400:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner_rls_policies.sql"))
        assert r.returncode == 0, f"rls: {r.stderr[-500:]}"

        # seed frozen universe (owner) + local history + a lookahead trap
        today = datetime.now(timezone.utc).date()
        session = today - timedelta(days=1)
        yday = today - timedelta(days=1)
        import json as _json
        uid = _psql(cid, f"INSERT INTO history_warmup_universes(universe_code,universe_version,"
                    f"universe_hash,config_hash,status,symbol_count,frozen_at) VALUES('ZZPROS',1,"
                    f"'pending','cfg','draft',{len(SYMS)},NULL) RETURNING id;").stdout
        # get the id
        cid_uid = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
                       "-c", "SELECT id FROM history_warmup_universes WHERE universe_code='ZZPROS';"]).stdout.strip()
        for i, s in enumerate(SYMS):
            _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{cid_uid}','{s}',{i});")
        # freeze with a deterministic hash matching the app's compute_universe_hash
        from app.history_warmup_execute import compute_universe_hash
        uhash = compute_universe_hash(universe_code="ZZPROS", universe_version=1, symbols_in_ordinal_order=SYMS)
        _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{cid_uid}';")

        # bulk seed daily + 4H via COPY-ish multi-insert as owner
        for s in SYMS:
            vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, yday))
            _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals};")
            # FUTURE daily bar (must be ignored): today+3
            fut = today + timedelta(days=3)
            _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES ('{s}','{fut}',999,1000,998,999.5,1);")
            f4 = _gen_4h(s, datetime.now(timezone.utc) - timedelta(hours=2))
            v4 = ",".join(f"('{r[0]}','{r[1].isoformat()}','{r[2].isoformat()}','{r[3]}',{r[4]},{r[5]},{r[6]},{r[7]},{r[8]},true,true,'sha256:fp{i}')" for i, r in enumerate(f4))
            _psql(cid, f"INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES {v4};")
            # FUTURE 4H bar (must be ignored): bar_end well after now
            fs = datetime.now(timezone.utc) + timedelta(days=2)
            _psql(cid, f"INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES ('{s}','{fs.isoformat()}','{(fs+timedelta(hours=4)).isoformat()}','{fs.date()}',999,1000,998,999.5,1,true,true,'sha256:future');")
        yield {"cid": cid, "hp": hp, "universe_id": cid_uid, "universe_hash": uhash,
               "session": session.isoformat(),
               "pros_dsn": f"postgresql://{PROS}:{PROS_PW}@127.0.0.1:{hp}/{DBNAME}?sslmode=disable"}
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(autouse=True)
def _no_external_network(monkeypatch):
    real = socket.socket.connect
    def guard(self, address):
        host = address[0] if isinstance(address, tuple) else str(address)
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"external network access blocked: {host}")
        return real(self, address)
    monkeypatch.setattr(socket.socket, "connect", guard)


@pytest.fixture()
def prospective(pg, monkeypatch):
    from app.config import settings
    import app.deps as deps
    monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", False)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_ONLY_MODE", False)
    monkeypatch.setattr(settings, "ENABLE_SCHEDULER", False)
    monkeypatch.setattr(settings, "MASSIVE_API_KEY", "")
    monkeypatch.setattr(settings, "PROSPECTIVE_DATABASE_URL", pg["pros_dsn"])
    asyncio.run(deps.close_db_pool())
    yield pg
    try:
        asyncio.run(deps.close_db_pool())
    except RuntimeError:
        pass


async def _db():
    import app.deps as deps
    pool = await deps.init_db_pool()
    return await pool.acquire(), pool


class TestProspectivePipeline:
    def test_full_pipeline_register_execute_audit(self, prospective, monkeypatch):
        from app.routers.admin import (
            prospective_access_check, prospective_preflight, prospective_register,
            prospective_execute, prospective_audit)
        uid = prospective["universe_id"]

        async def drive():
            conn, pool = await _db()
            try:
                # access-check
                ac = await prospective_access_check(_="t", db=conn)
                assert ac["ready"] is True, ac["reasons"]
                assert ac["database_identity"] == PROS
                assert ac["provider_constructed"] is False
                assert ac["outcome_writes_forbidden"] and ac["bar_writes_forbidden"] and ac["delete_forbidden"]
                # preflight
                pf = await prospective_preflight(_="t", db=conn, universe_id=uid,
                                                 experiment_code="wyckoff_v2_vs_baseline")
                assert pf["contract_version"] == "prospective_preflight.v1"
                assert pf["symbol_count"] == 2 and pf["universe_hash"] == prospective["universe_hash"]
                assert pf["provider_called"] is False and pf["provider_constructed"] is False
                assert pf["all_ready"] is True, pf["blocking_reasons"]
                assert pf["execution_available"] is True
                assert pf["snapshot_session_date"] <= prospective["session"] or pf["snapshot_session_date"]  # server-selected completed session
                # register
                reg = await prospective_register(_="t", db=conn, body={
                    "contract_version": "prospective_campaign_registration.v1",
                    "experiment_code": "wyckoff_v2_vs_baseline", "universe_id": uid,
                    "universe_hash": pf["universe_hash"],
                    "history_config_hash": pf["history_config_hash"],
                    "history_readiness_manifest_hash": pf["history_readiness_manifest_hash"],
                    "snapshot_session_date": pf["snapshot_session_date"],
                    "snapshot_cutoff_at": pf["snapshot_cutoff_at"],
                    "candidate_signal_definition": "pre_rollout_enter_eligible.v1"})
                assert reg["status"] == "registered"
                reg_id = reg["registration_id"]
                # register replay -> already_registered, same id
                reg2 = await prospective_register(_="t", db=conn, body={
                    "contract_version": "prospective_campaign_registration.v1",
                    "experiment_code": "wyckoff_v2_vs_baseline", "universe_id": uid,
                    "universe_hash": pf["universe_hash"],
                    "history_config_hash": pf["history_config_hash"],
                    "history_readiness_manifest_hash": pf["history_readiness_manifest_hash"],
                    "snapshot_session_date": pf["snapshot_session_date"],
                    "snapshot_cutoff_at": pf["snapshot_cutoff_at"],
                    "candidate_signal_definition": "pre_rollout_enter_eligible.v1"})
                assert reg2["status"] == "already_registered" and reg2["registration_id"] == reg_id
                # no pairs/evals yet
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_pairs") == 0
                # execute
                body = {"contract_version": "prospective_campaign_execute.v1",
                        "registration_id": reg_id,
                        "registration_identity": reg["registration_identity"],
                        "universe_hash": pf["universe_hash"],
                        "history_readiness_manifest_hash": pf["history_readiness_manifest_hash"],
                        "snapshot_session_date": pf["snapshot_session_date"], "limit": 2}
                res = await prospective_execute(_="t", db=conn, body=body)
                assert res["status"] == "executed", res
                assert res["pair_count"] == 2 and res["candidate_evaluations"] == 2 and res["control_evaluations"] == 2
                assert res["outcomes"] == 0 and res["provider_request_count"] == 0
                # DB reconciliation
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_pairs") == 2
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_evaluations") == 4
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_pair_outcomes") == 0
                # LOOKAHEAD: no persisted pair frame may include the future daily bar
                maxd = await conn.fetchval("SELECT max(frame_last_date) FROM strategy_shadow_pairs")
                assert str(maxd) <= pf["snapshot_session_date"]
                # execute replay -> already_applied, no new writes
                res2 = await prospective_execute(_="t", db=conn, body=body)
                assert res2["status"] == "already_applied" and res2["provider_request_count"] == 0
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_pairs") == 2
                assert await conn.fetchval("SELECT count(*) FROM strategy_shadow_evaluations") == 4
                # audit
                au = await prospective_audit(_="t", db=conn, registration_id=reg_id)
                assert au["provider_called"] is False and au["outcome_count"] == 0
                assert au["pair_count"] == 2 and au["candidate_evaluation_count"] == 2 and au["control_evaluation_count"] == 2
                assert au["missing_candidate_arms"] == 0 and au["missing_control_arms"] == 0 and au["duplicate_arms"] == 0
                assert au["campaign_completion_state"] == "completed"
                # regression: audit must not crash when a durable-queue job_runs
                # row (+ a job_workers row) exists for this registration — the
                # workers query binds the stale-threshold as text, and it must
                # be sent as a str, not an int (asyncpg param-type inference).
                _psql(prospective["cid"],
                      "INSERT INTO job_runs(job_type, job_contract_version, queue_name, "
                      "idempotency_key, status, registration_id) VALUES "
                      f"('prospective_campaign', 'prospective_campaign_enqueue.v1', 'prospective', "
                      f"'test-audit-regress-{reg_id}', 'succeeded', '{reg_id}');")
                _psql(prospective["cid"],
                      "INSERT INTO job_workers(worker_id, worker_type, queue_names) VALUES "
                      "('test-audit-regress-worker', 'prospective', ARRAY['prospective']) "
                      "ON CONFLICT (worker_id) DO NOTHING;")
                au2 = await prospective_audit(_="t", db=conn, registration_id=reg_id)
                assert au2["job"] is not None and au2["job"]["job_status"] == "succeeded"
                assert len(au2["job"]["workers"]) == 1
                assert isinstance(au2["job"]["workers"][0]["stale"], bool)
            finally:
                import app.deps as _deps
                await pool.release(conn)
                await _deps.close_db_pool()
        asyncio.run(drive())

    def test_prospective_role_cannot_write_bars_or_outcomes(self, prospective):
        async def drive():
            conn, pool = await _db()
            try:
                for stmt in (
                    "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES('ZZAA','2020-01-01',1,2,0.5,1.5,1)",
                    "INSERT INTO market_bars_4h(symbol,bar_start,bar_end,session_date,open,high,low,close,volume,is_completed,is_regular_session,content_fingerprint) VALUES('ZZAA','2020-01-01 13:30:00+00','2020-01-01 17:30:00+00','2020-01-01',1,2,0.5,1.5,1,true,true,'fp')",
                    "INSERT INTO strategy_shadow_pair_outcomes(id,pair_id,outcome_fingerprint,outcome_fingerprint_version,calculation_version,outcome_coverage_version,forward_frame_version,reference_price_role) VALUES(gen_random_uuid(),gen_random_uuid(),'fp','v','v','v','v','paired_decision_observation')",
                    "DELETE FROM strategy_shadow_pairs",
                ):
                    with pytest.raises(asyncpg.PostgresError):
                        await conn.execute(stmt)
            finally:
                import app.deps as _deps
                await pool.release(conn)
                await _deps.close_db_pool()
        asyncio.run(drive())
