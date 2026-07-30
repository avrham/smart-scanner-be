"""Real-Postgres integration for prospective_outcome_maturation.v1.

Isolated Docker PostgreSQL (never Supabase/Massive): migrations 001-018, the
prospective_runner/prospective_worker/job_audit_reader roles (to seed a REAL
completed 2-symbol campaign via the existing evaluation path) PLUS the new
prospective_outcome_worker role + its RLS. Covers: preflight before/after
local forward history exists, idempotent enqueue/replay, idempotent partial
then complete outcome writes, no-duplicate-outcome-row, two-worker
no-double-claim on the outcome queue, lease loss + recovery, missing-bar
retryable classification, terminal invalid payload, local-history-only (no
provider constructed, no external network), and full RLS/privilege proof
(can write only strategy_shadow_pair_outcomes/strategy_shadow_outcome_runs,
cannot write bars/registrations/campaigns/pairs/evaluations, cannot claim a
'prospective' queue evaluation task).
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import socket
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.prospective_campaign as pc
from app.jobs import contracts as C
from app.jobs import queue as Q
from app.jobs import identity as ident

PG_IMAGE = "postgres:16-alpine"
DBNAME = "outcomedb"
PROS_PW = "prospw_local_only"
WORK_PW = "workpw_local_only"
OUT_PW = "outpw_local_only"
PROS = "smart_scanner_prospective_runner"
WORK = "smart_scanner_prospective_worker"
OUTW = "smart_scanner_prospective_outcome_worker"
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
SYMS = ["ZZOA", "ZZOB"]


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
    rows = []
    for i in range(n):
        d = end_date - timedelta(days=(n - 1 - i))
        base = 100.0 + i * 0.05 + 12.0 * math.sin(i / 25.0)
        o = round(base, 2); c = round(base + math.sin(i / 7.0), 2)
        hi = round(max(o, c) + 1.5 + abs(math.sin(i / 5.0)), 2)
        lo = round(min(o, c) - 1.5 - abs(math.cos(i / 6.0)), 2)
        rows.append((sym, d, o, hi, lo, c, float(1_000_000 + (i % 11) * 25_000)))
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
        assert _psql(cid, None, variables={"prospective_password": PROS_PW, "db_name": DBNAME},
                     path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner.sql")).returncode == 0
        assert _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_shadow_prospective_runner_rls_policies.sql")).returncode == 0
        r = _psql(cid, None, variables={"worker_password": WORK_PW, "enqueuer_password": "e_pw",
                                        "audit_reader_password": "a_pw", "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_job_queue_roles.sql"))
        assert r.returncode == 0, f"queue roles: {r.stderr[-500:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_job_queue_rls_policies.sql"))
        assert r.returncode == 0, f"queue rls: {r.stderr[-600:]}"
        # NEW: the outcome-worker role + its RLS
        r = _psql(cid, None, variables={"outcome_worker_password": OUT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_prospective_outcome_worker.sql"))
        assert r.returncode == 0, f"outcome worker role: {r.stderr[-500:]}"
        r = _psql(cid, None, path=os.path.join(REPO, "ops", "sql", "create_prospective_outcome_worker_rls_policies.sql"))
        assert r.returncode == 0, f"outcome worker rls: {r.stderr[-800:]}"

        today = datetime.now(timezone.utc).date()
        session = today - timedelta(days=1)
        _psql(cid, f"INSERT INTO history_warmup_universes(universe_code,universe_version,"
              f"universe_hash,config_hash,status,symbol_count,frozen_at) VALUES('ZZOUT',1,"
              f"'pending','cfg','draft',{len(SYMS)},NULL);")
        uid = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
                   "-c", "SELECT id FROM history_warmup_universes WHERE universe_code='ZZOUT';"]).stdout.strip()
        for i, s in enumerate(SYMS):
            _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{uid}','{s}',{i});")
        from app.history_warmup_execute import compute_universe_hash
        uhash = compute_universe_hash(universe_code="ZZOUT", universe_version=1, symbols_in_ordinal_order=SYMS)
        _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{uid}';")
        for s in SYMS:
            vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, session))
            _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals};")

        session_cutoff = datetime.combine(session, datetime.min.time(), timezone.utc) + timedelta(hours=20)
        reg_ident = pc.registration_identity(experiment_code="wyckoff_v2_vs_baseline",
                                             universe_id=str(uid), universe_hash=uhash,
                                             history_config_hash="sha256:cfg",
                                             snapshot_session_date=session.isoformat())
        reg_id = str(uuid.uuid4())
        _psql(cid,
              "INSERT INTO prospective_campaign_registrations(id,experiment_code,experiment_contract_version,"
              "universe_id,universe_code,universe_version,universe_hash,history_config_hash,"
              "history_readiness_manifest_hash,candidate_strategy_code,candidate_strategy_version,"
              "candidate_signal_definition,candidate_allow_enter,control_strategy_code,control_strategy_version,"
              "snapshot_session_date,snapshot_cutoff_at,market_calendar_version,registration_identity,status) "
              f"VALUES('{reg_id}','wyckoff_v2_vs_baseline','wyckoff_v2_prospective_experiment.v1','{uid}','ZZOUT',1,"
              f"'{uhash}','sha256:cfg','sha256:manifest','wyckoff_mtf_v2','wyckoff_mtf.v2',"
              f"'pre_rollout_enter_eligible.v1',FALSE,'sma150_bounce','sma150.v2','{session.isoformat()}',"
              f"'{session_cutoff.isoformat()}','us_market_calendar.v1','{reg_ident}','registered');")

        base = f"127.0.0.1:{hp}/{DBNAME}?sslmode=disable"
        yield {"cid": cid, "hp": hp, "universe_id": uid, "universe_hash": uhash,
               "session": session, "reg_id": reg_id, "reg_ident": reg_ident,
               "su_dsn": f"postgresql://postgres:postgres@{base}",
               "runner_dsn": f"postgresql://{PROS}:{PROS_PW}@{base}",
               "worker_dsn": f"postgresql://{WORK}:{WORK_PW}@{base}",
               "outcome_dsn": f"postgresql://{OUTW}:{OUT_PW}@{base}"}
    finally:
        # app.deps._db_pool is a process-global that OUTLIVES this module —
        # _bind_global_pool leaves it pointed at a pool into THIS Docker
        # container. Reset it directly (a graceful async close needs a live
        # event loop, which no fixture teardown here has) so a LATER test
        # module's init_db_pool() creates a fresh pool instead of silently
        # reusing a dead one against a container that's about to stop.
        import app.deps as deps
        deps._db_pool = None
        deps._db_pool_mode = None
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


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _bind_global_pool(monkeypatch, dsn):
    """The local-only readers (mirroring select_pairs_for_outcomes'
    established convention) use the process-global asyncpg pool, not an
    explicit conn — correct in production (one pool per process, same role
    as `Depends(get_db)`). Each test method here runs in its OWN event loop
    (_run creates a fresh one every time), so the pool must be closed and
    freshly recreated ON THAT LOOP before every use — a pool object left
    over from a PRIOR test's now-closed loop would raise
    'Event loop is closed'."""
    from app.config import settings
    import app.deps as deps
    monkeypatch.setattr(settings, "JOB_WORKER_ENABLED", True, raising=False)
    monkeypatch.setattr(settings, "JOB_WORKER_DATABASE_URL", dsn, raising=False)
    monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", False)
    monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)
    monkeypatch.setattr(settings, "MAINTENANCE_ONLY_MODE", False)
    monkeypatch.setattr(settings, "HISTORY_WARMUP_ONLY_MODE", False)
    # A pool left over from a PRIOR test's (now-closed) event loop cannot be
    # gracefully closed on a new loop — asyncpg schedules its close callbacks
    # on the loop it was created on. Discard the stale reference directly
    # (acceptable in tests: the Docker container teardown reclaims sockets)
    # rather than awaiting close_db_pool(), then create a fresh pool bound to
    # the CURRENT (live) loop.
    deps._db_pool = None
    deps._db_pool_mode = None
    await deps.init_db_pool()


async def _complete_campaign(pg, monkeypatch):
    """Drive the EXISTING evaluation path to a real completed 2-pair
    campaign — shared setup for every outcome-maturation test below."""
    from app.jobs.prospective_enqueue import enqueue_prospective_campaign, sync_prospective_campaign
    from app.jobs.handlers.prospective import evaluate_prospective_symbol

    runner = await asyncpg.connect(pg["runner_dsn"])
    try:
        r1 = await enqueue_prospective_campaign(runner, registration_id=pg["reg_id"],
                                                registration_identity=pg["reg_ident"],
                                                requested_by="test")
        assert r1["status"] == "queued"
    finally:
        await runner.close()

    await _bind_global_pool(monkeypatch, pg["worker_dsn"])
    from app.workers.persistence import get_db_connection, release_db_connection
    for _ in range(len(SYMS)):
        conn = await get_db_connection()
        try:
            t = await Q.claim_next_task(conn, queue_name="prospective", worker_id="wtest", lease_seconds=900)
            assert t is not None
            payload = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
        finally:
            await release_db_connection(conn)
        res = await evaluate_prospective_symbol(payload)
        assert res["ok"] is True, res
        conn = await get_db_connection()
        try:
            await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="wtest", result_summary=res["result"])
            await sync_prospective_campaign(conn, t["job_id"])
        finally:
            await release_db_connection(conn)

    conn = await get_db_connection()
    try:
        reg = await conn.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", pg["reg_id"])
        assert reg["status"] == "completed"
        pairs = await conn.fetch(
            "SELECT p.id AS pair_id, p.symbol FROM strategy_shadow_run_pairs rp "
            "JOIN strategy_shadow_pairs p ON p.id=rp.pair_id WHERE rp.run_id=$1 ORDER BY p.symbol",
            reg["campaign_run_id"])
        assert len(pairs) == len(SYMS)
    finally:
        await release_db_connection(conn)
    return {"registration_id": str(reg["id"]), "registration_identity": reg["registration_identity"],
            "campaign_id": str(reg["campaign_id"]), "campaign_run_id": str(reg["campaign_run_id"]),
            "pairs": [{"pair_id": str(r["pair_id"]), "symbol": r["symbol"]} for r in pairs]}


# ============================ preflight + enqueue ===========================
class TestOutcomePreflightAndEnqueue:
    def test_preflight_zero_forward_bars_is_not_yet_eligible(self, pg, monkeypatch):
        async def go():
            campaign = await _complete_campaign(pg, monkeypatch)
            from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight
            conn = await asyncpg.connect(pg["runner_dsn"])
            try:
                pf = await build_outcome_maturity_preflight(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"])
                assert pf["pair_count"] == len(SYMS)
                assert pf["configured_horizons"] == ["1D", "3D", "5D", "10D", "20D"]
                # With ZERO forward bars anywhere, the local session calendar
                # is empty — the classifier honestly reports
                # eligibility_unknown (never assumes "0 sessions have
                # passed"), matching the live 25-symbol campaign's actual
                # zero-forward-history state exactly.
                assert pf["eligibility_unknown_count"] == len(SYMS)
                assert pf["not_yet_eligible_count"] == 0
                assert pf["eligible_count"] == 0
                assert pf["enqueue_available_count"] == 0
                assert pf["provider_called"] is False
            finally:
                await conn.close()
            return campaign
        campaign = _run(go())
        pg["_campaign"] = campaign

    def test_preflight_becomes_eligible_after_one_forward_session(self, pg, monkeypatch):
        async def go():
            campaign = pg.get("_campaign") or await _complete_campaign(pg, monkeypatch)
            await _bind_global_pool(monkeypatch, pg["runner_dsn"])
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                nxt = pg["session"] + timedelta(days=1)
                for s in SYMS:
                    await su.execute(
                        "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                        "VALUES ($1,$2,101,102,99,101.5,1000000)", s, nxt)
            finally:
                await su.close()
            from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight
            conn = await asyncpg.connect(pg["runner_dsn"])
            try:
                pf = await build_outcome_maturity_preflight(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"])
                assert pf["eligible_count"] == len(SYMS)
                assert pf["not_yet_eligible_count"] == 0
                assert pf["history_cutoff_used"] == nxt.isoformat()
            finally:
                await conn.close()
            return campaign
        pg["_campaign"] = _run(go())

    def test_enqueue_creates_one_task_per_eligible_pair_and_replay_is_idempotent(self, pg, monkeypatch):
        async def go():
            campaign = pg["_campaign"]
            await _bind_global_pool(monkeypatch, pg["runner_dsn"])
            from app.jobs.prospective_outcome_enqueue import enqueue_outcome_maturation
            conn = await asyncpg.connect(pg["runner_dsn"])
            try:
                r1 = await enqueue_outcome_maturation(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"], requested_by="test")
                assert r1["status"] == "queued"
                assert r1["total_task_count"] == len(SYMS)
                job_id = r1["job_id"]
                r2 = await enqueue_outcome_maturation(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"], requested_by="test")
                assert r2["status"] == "already_queued" and r2["job_id"] == job_id
                tasks = await conn.fetch("SELECT task_key FROM job_tasks WHERE job_id=$1", job_id)
                assert len(tasks) == len(SYMS)
            finally:
                await conn.close()
            pg["_job_id"] = job_id
            return campaign
        pg["_campaign"] = _run(go())


# ======================= process tasks: partial -> complete =================
class TestOutcomeProcessing:
    def test_process_tasks_partial_then_idempotent_replay_no_duplicate(self, pg, monkeypatch):
        async def go():
            campaign = pg["_campaign"]; job_id = pg["_job_id"]
            from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
            await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
            from app.workers.persistence import get_db_connection, release_db_connection

            processed = []
            for _ in range(len(SYMS)):
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                                worker_id="ow1", lease_seconds=900)
                    assert t is not None
                    payload = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                finally:
                    await release_db_connection(conn)
                res = await evaluate_prospective_outcome(payload)
                assert res["ok"] is True, res
                assert res["result"]["outcome_status"] == "partial"
                assert res["result"]["provider_called"] is False
                conn = await get_db_connection()
                try:
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="ow1",
                                                    result_summary=res["result"])
                finally:
                    await release_db_connection(conn)
                processed.append(payload["pair_id"])

            # idempotent replay: re-run the SAME payload for one pair — status
            # stays partial (same 1 forward bar), NO duplicate outcome row.
            pair_id = processed[0]
            payload = {"registration_id": campaign["registration_id"],
                      "registration_identity": campaign["registration_identity"],
                      "campaign_id": campaign["campaign_id"],
                      "campaign_run_id": campaign["campaign_run_id"],
                      "pair_id": pair_id, "symbol": next(
                          p["symbol"] for p in campaign["pairs"] if p["pair_id"] == pair_id)}
            res2 = await evaluate_prospective_outcome(payload)
            assert res2["ok"] is True and res2["result"]["outcome_status"] == "partial"

            conn = await get_db_connection()
            try:
                n = await conn.fetchval(
                    "SELECT count(*) FROM strategy_shadow_pair_outcomes WHERE pair_id=$1", pair_id)
                assert n == 1
                row = await conn.fetchrow(
                    "SELECT ret_1d, ret_3d, available_forward_bars FROM strategy_shadow_pair_outcomes "
                    "WHERE pair_id=$1", pair_id)
                assert row["ret_1d"] is not None
                assert row["ret_3d"] is None
                assert row["available_forward_bars"] == 1
            finally:
                await release_db_connection(conn)
        _run(go())

    def test_missing_snapshot_bar_protects_partial_evidence_and_classifies_correctly(self, pg, monkeypatch):
        """A missing snapshot-date bar classifies as missing_market_session_data
        (the task's "temporarily missing data" state) — a DATA problem a
        future warmup correction can resolve, distinct from terminal_failure
        (a plain re-run can never fix). Proven as a pure classification
        (independent of any row's current status).

        Live proof of the WRITE-ONCE contract: applied against an ALREADY
        `partial` pair, the merge layer (existing, frozen code — never
        touched here) refuses to regress matured evidence to `error`; the
        failure surfaces only as a bounded revision_notes entry, and
        `outcome_status`/`error_code` stay exactly as they were."""
        from app.workers.shadow.outcomes.eligibility import (
            classify_error_code, ELIGIBILITY_MISSING_SESSION_DATA, ELIGIBILITY_TERMINAL)
        state = classify_error_code("snapshot_bar_missing")
        assert state == ELIGIBILITY_MISSING_SESSION_DATA
        assert state != ELIGIBILITY_TERMINAL

        async def go():
            campaign = pg["_campaign"]
            pair = campaign["pairs"][0]
            from app.workers.persistence import get_db_connection, release_db_connection
            await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
            conn = await get_db_connection()
            try:
                before = await conn.fetchrow(
                    "SELECT outcome_status, error_code, ret_1d FROM strategy_shadow_pair_outcomes "
                    "WHERE pair_id=$1", pair["pair_id"])
                assert before["outcome_status"] == "partial"
            finally:
                await release_db_connection(conn)

            su = await asyncpg.connect(pg["su_dsn"])
            try:
                # capture the EXACT row before deleting, so restoration can't
                # accidentally substitute a different (e.g. forward) bar's
                # price at the snapshot date — that would silently corrupt
                # the frozen reference and permanently trip revision
                # detection on every later recalculation.
                original = await su.fetchrow(
                    "SELECT open, high, low, close, volume FROM daily_bars "
                    "WHERE symbol=$1 AND trading_date=$2", pair["symbol"], pg["session"])
                await su.execute("DELETE FROM daily_bars WHERE symbol=$1 AND trading_date=$2",
                                 pair["symbol"], pg["session"])
            finally:
                await su.close()

            from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
            payload = {"registration_id": campaign["registration_id"],
                      "registration_identity": campaign["registration_identity"],
                      "campaign_id": campaign["campaign_id"],
                      "campaign_run_id": campaign["campaign_run_id"],
                      "pair_id": pair["pair_id"], "symbol": pair["symbol"]}
            res = await evaluate_prospective_outcome(payload)
            assert res["ok"] is True
            # protected: the handler reports the MERGED (unregressed) status
            assert res["result"]["outcome_status"] == "partial"

            conn = await get_db_connection()
            try:
                after = await conn.fetchrow(
                    "SELECT outcome_status, error_code, ret_1d, revision_notes "
                    "FROM strategy_shadow_pair_outcomes WHERE pair_id=$1", pair["pair_id"])
                assert after["outcome_status"] == "partial"
                assert after["error_code"] is None  # never surfaces on a protected row
                assert after["ret_1d"] == before["ret_1d"]  # matured evidence untouched
                import json as _json
                notes = after["revision_notes"]
                notes = _json.loads(notes) if isinstance(notes, str) else notes
                assert any(n.get("reason_code") == "recalculation_error"
                          and n.get("error_code") == "snapshot_bar_missing" for n in (notes or []))
            finally:
                await release_db_connection(conn)

            # restore for the "advance to complete" test that follows, and
            # prove recovery: a normal re-run after restoration is unaffected
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                await su.execute(
                    "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    pair["symbol"], pg["session"], original["open"], original["high"],
                    original["low"], original["close"], original["volume"])
            finally:
                await su.close()
            res2 = await evaluate_prospective_outcome(payload)
            assert res2["ok"] is True
            assert res2["result"]["outcome_status"] == "partial"
        _run(go())

    def test_advancing_to_full_20_sessions_reaches_complete_then_already_applied(self, pg, monkeypatch):
        async def go():
            campaign = pg["_campaign"]
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                for i in range(2, 21):
                    d = pg["session"] + timedelta(days=i)
                    for s in SYMS:
                        await su.execute(
                            "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                            "VALUES ($1,$2,101,102,99,$3,1000000)", s, d, 101.5 + i * 0.1)
            finally:
                await su.close()

            from app.jobs.prospective_outcome_enqueue import enqueue_outcome_maturation, build_outcome_maturity_preflight
            from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
            await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
            from app.workers.persistence import get_db_connection, release_db_connection

            conn = await asyncpg.connect(pg["runner_dsn"])
            try:
                pf = await build_outcome_maturity_preflight(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"])
                assert pf["eligible_count"] == len(SYMS)  # partial rows are re-eligible
                enq = await enqueue_outcome_maturation(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"], requested_by="test2")
                assert enq["status"] == "queued"
                job_id2 = enq["job_id"]
            finally:
                await conn.close()

            for _ in range(len(SYMS)):
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                                worker_id="ow2", lease_seconds=900)
                    assert t is not None
                    payload = json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                finally:
                    await release_db_connection(conn)
                res = await evaluate_prospective_outcome(payload)
                assert res["ok"] is True
                assert res["result"]["outcome_status"] == "complete"
                conn = await get_db_connection()
                try:
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="ow2",
                                                    result_summary=res["result"])
                finally:
                    await release_db_connection(conn)

            # a fresh outcome-maturation enqueue against a fully-matured
            # campaign finds NOTHING eligible (already_applied semantics)
            conn = await asyncpg.connect(pg["runner_dsn"])
            try:
                pf2 = await build_outcome_maturity_preflight(
                    conn, registration_id=campaign["registration_id"],
                    registration_identity=campaign["registration_identity"])
                assert pf2["matured_count"] == len(SYMS)
                assert pf2["enqueue_available_count"] == 0
            finally:
                await conn.close()

            conn = await get_db_connection()
            try:
                for p in campaign["pairs"]:
                    row = await conn.fetchrow(
                        "SELECT outcome_status, ret_1d, ret_3d, ret_5d, ret_10d, ret_20d "
                        "FROM strategy_shadow_pair_outcomes WHERE pair_id=$1", p["pair_id"])
                    assert row["outcome_status"] == "complete"
                    assert all(row[c] is not None for c in ("ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"))
                n_rows = await conn.fetchval(
                    "SELECT count(*) FROM strategy_shadow_pair_outcomes WHERE pair_id = ANY($1::uuid[])",
                    [p["pair_id"] for p in campaign["pairs"]])
                assert n_rows == len(SYMS)  # still exactly one row per pair
            finally:
                await release_db_connection(conn)
        _run(go())


# ============================ queue mechanics on outcomes ===================
class TestOutcomeQueueMechanics:
    def test_two_workers_no_double_claim_on_outcome_queue(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job_id = await conn.fetchval(
                    "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status,"
                    "total_task_count,queued_task_count) VALUES ($1,$2,$3,$4,'queued',2,2) RETURNING id",
                    C.JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
                    C.PROSPECTIVE_OUTCOME_QUEUE, f"jobtest:{uuid.uuid4()}")
                for i in range(2):
                    payload = {"probe": i}
                    await conn.execute(
                        "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,"
                        "ordinal,payload,payload_hash,idempotency_key,status,max_attempts) "
                        "VALUES ($1,$2,$3,$3,$4,$5,$6::jsonb,$7,$8,'queued',3)",
                        job_id, C.PROSPECTIVE_OUTCOME_QUEUE, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
                        f"probe{i}", i, json.dumps(payload), ident.payload_hash(payload), f"probetask:{uuid.uuid4()}")
            finally:
                await conn.close()
            c1 = await asyncpg.connect(pg["outcome_dsn"])
            c2 = await asyncpg.connect(pg["outcome_dsn"])
            try:
                t1, t2 = await asyncio.gather(
                    Q.claim_next_task(c1, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE, worker_id="ow1", lease_seconds=900),
                    Q.claim_next_task(c2, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE, worker_id="ow2", lease_seconds=900))
                assert t1 and t2 and t1["id"] != t2["id"]
                t3 = await Q.claim_next_task(c1, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE, worker_id="ow1", lease_seconds=900)
                assert t3 is None
            finally:
                await c1.close(); await c2.close()
        _run(go())

    def test_lease_expiry_reconciles_to_retryable_and_is_reclaimed(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["su_dsn"])
            try:
                job_id = await conn.fetchval(
                    "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status,"
                    "total_task_count,queued_task_count) VALUES ($1,$2,$3,$4,'queued',1,1) RETURNING id",
                    C.JOB_TYPE_PROSPECTIVE_OUTCOME_MATURATION, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
                    C.PROSPECTIVE_OUTCOME_QUEUE, f"jobtest:{uuid.uuid4()}")
                payload = {"probe": "lease"}
                await conn.execute(
                    "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,"
                    "ordinal,payload,payload_hash,idempotency_key,status,max_attempts) "
                    "VALUES ($1,$2,$3,$3,$4,0,$5::jsonb,$6,$7,'queued',3)",
                    job_id, C.PROSPECTIVE_OUTCOME_QUEUE, C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
                    "leasekey", json.dumps(payload), ident.payload_hash(payload), f"leasetask:{uuid.uuid4()}")
                t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                            worker_id="ow1", lease_seconds=1)
                await conn.execute("UPDATE job_tasks SET lease_expires_at=NOW()-interval '1 second' WHERE id=$1", t["id"])
                expired = await Q.find_expired_lease_tasks(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE)
                assert any(str(e["id"]) == str(t["id"]) for e in expired)
                await Q.reconcile_task_to_retryable(conn, task_id=t["id"])
                row = await conn.fetchrow("SELECT status, available_at FROM job_tasks WHERE id=$1", t["id"])
                assert row["status"] == "retryable"
                await conn.execute("UPDATE job_tasks SET available_at=NOW() WHERE id=$1", t["id"])
                reclaimed = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                                    worker_id="ow2", lease_seconds=900)
                assert reclaimed is not None and str(reclaimed["id"]) == str(t["id"])
                assert reclaimed["attempt_count"] == 2
            finally:
                await conn.close()
        _run(go())


# ============================= error taxonomy ================================
class TestOutcomeErrorTaxonomy:
    def test_terminal_invalid_payload_campaign_mismatch(self, pg, monkeypatch):
        async def go():
            campaign = pg["_campaign"]
            pair = campaign["pairs"][0]
            from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
            await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
            payload = {"registration_id": campaign["registration_id"],
                      "registration_identity": campaign["registration_identity"],
                      "campaign_id": str(uuid.uuid4()),  # wrong campaign_id
                      "campaign_run_id": campaign["campaign_run_id"],
                      "pair_id": pair["pair_id"], "symbol": pair["symbol"]}
            res = await evaluate_prospective_outcome(payload)
            assert res["ok"] is False
            assert res["error_class"] == C.ERR_TERMINAL
            assert res["safe_error_code"] == "invalid_task_payload"
        _run(go())

    def test_terminal_missing_required_field(self):
        async def go():
            from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
            res = await evaluate_prospective_outcome({"registration_id": "r1"})
            assert res["ok"] is False
            assert res["error_class"] == C.ERR_TERMINAL
        _run(go())


# ================================ RLS + privilege ============================
class TestOutcomeWorkerRls:
    def test_outcome_worker_can_write_only_its_two_relations(self, pg):
        async def go():
            conn = await asyncpg.connect(pg["outcome_dsn"])
            try:
                assert await conn.fetchval("SELECT current_user") == OUTW
                await conn.fetch("SELECT id FROM strategy_shadow_pair_outcomes LIMIT 1")
                await conn.fetch("SELECT id FROM daily_bars LIMIT 1")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                        "VALUES ('XX','2020-01-01',1,1,1,1,1)")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "UPDATE prospective_campaign_registrations SET status='executing' WHERE false")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("INSERT INTO strategy_shadow_pairs DEFAULT VALUES")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("INSERT INTO strategy_shadow_evaluations DEFAULT VALUES")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("INSERT INTO strategy_shadow_runs DEFAULT VALUES")
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute("DELETE FROM strategy_shadow_pair_outcomes WHERE false")
            finally:
                await conn.close()
        _run(go())

    def test_outcome_worker_cannot_see_or_claim_prospective_evaluation_tasks(self, pg):
        async def go():
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                job_id = await su.fetchval(
                    "INSERT INTO job_runs (job_type,job_contract_version,queue_name,idempotency_key,status,"
                    "total_task_count,queued_task_count) VALUES ('prospective_campaign',"
                    "'prospective_symbol_evaluation.v1','prospective',$1,'queued',1,1) RETURNING id",
                    f"evaljob:{uuid.uuid4()}")
                payload = {"probe": "eval"}
                task_id = await su.fetchval(
                    "INSERT INTO job_tasks (job_id,queue_name,task_type,task_contract_version,task_key,"
                    "ordinal,payload,payload_hash,idempotency_key,status,max_attempts) "
                    "VALUES ($1,'prospective','prospective_symbol_evaluation.v1',"
                    "'prospective_symbol_evaluation.v1','evalkey',0,$2::jsonb,$3,$4,'queued',3) RETURNING id",
                    job_id, json.dumps(payload), ident.payload_hash(payload), f"evaltask:{uuid.uuid4()}")
            finally:
                await su.close()
            conn = await asyncpg.connect(pg["outcome_dsn"])
            try:
                # RLS-scoped SELECT: the 'prospective' queue evaluation task is invisible
                seen = await conn.fetchval("SELECT count(*) FROM job_tasks WHERE id=$1", task_id)
                assert seen == 0
                # the claim query itself finds nothing on the wrong queue
                claimed = await Q.claim_next_task(conn, queue_name="prospective",
                                                  worker_id="ow-intruder", lease_seconds=900)
                assert claimed is None
            finally:
                await conn.close()
            # its OWN queue is fully visible/claimable (sanity check the RLS
            # predicate isn't accidentally blocking everything)
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                await su.execute(
                    "UPDATE job_tasks SET queue_name=$2, task_type=$3, task_contract_version=$3 WHERE id=$1",
                    task_id, C.PROSPECTIVE_OUTCOME_QUEUE, C.PROSPECTIVE_OUTCOME_MATURATION_TASK)
                await su.execute("UPDATE job_runs SET queue_name=$2 WHERE id=$1", job_id, C.PROSPECTIVE_OUTCOME_QUEUE)
            finally:
                await su.close()
            conn = await asyncpg.connect(pg["outcome_dsn"])
            try:
                seen2 = await conn.fetchval("SELECT count(*) FROM job_tasks WHERE id=$1", task_id)
                assert seen2 == 1
            finally:
                await conn.close()
        _run(go())
