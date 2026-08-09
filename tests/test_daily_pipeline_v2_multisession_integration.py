"""Multi-session integration simulation for daily-pipeline v2 (own Docker
container via the shared ``pg`` fixture).

Proves the structural fix: a repeatable daily pipeline where
  * each occurrence's CURRENT campaign (snapshot == that session) is honestly
    DEFERRED (zero completed forward sessions yet) — NOT blocked, NOT faked;
  * PRIOR campaigns mature across occurrences as new local sessions accrue,
    progressing 1D -> 3D -> 5D -> 10D -> 20D by actual trading-session counts;
  * creating a new campaign while a prior campaign already has matured outcomes
    is allowed (the run-scoped no-outcomes guard, not the old global one);
  * replay is idempotent (no duplicate campaigns / pairs / evaluations /
    outcome jobs / outcome rows).

Local PostgreSQL + local-history outcomes only. No provider, no shared Supabase.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.prospective_campaign as pc
from app.jobs import contracts as C
from app.jobs import queue as Q
from app.jobs import daily_pipeline as DP
from app.jobs import daily_pipeline_maturation as DM

from test_prospective_outcome_maturation_integration import (  # noqa: E402
    pg, _no_external_network, _bind_global_pool, _run, _gen_daily, _psql, _sh,
    _docker_ready, DBNAME,
)

pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")

SIM_SYMS = ["ZZS1", "ZZS2"]
SIM_CODE = "ZZSIM"


def _jl(v):
    import json
    return json.loads(v) if isinstance(v, str) else v


async def _ensure_universe(pg):
    from app.history_warmup_execute import compute_universe_hash
    cid = pg["cid"]
    row = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
               "-c", f"SELECT id FROM history_warmup_universes WHERE universe_code='{SIM_CODE}';"]).stdout.strip()
    uhash = compute_universe_hash(universe_code=SIM_CODE, universe_version=1, symbols_in_ordinal_order=SIM_SYMS)
    if not row:
        _psql(cid, "INSERT INTO history_warmup_universes(universe_code,universe_version,universe_hash,"
              f"config_hash,status,symbol_count,frozen_at) VALUES('{SIM_CODE}',1,'pending','cfg','draft',{len(SIM_SYMS)},NULL);")
        row = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
                   "-c", f"SELECT id FROM history_warmup_universes WHERE universe_code='{SIM_CODE}';"]).stdout.strip()
        for i, s in enumerate(SIM_SYMS):
            _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{row}','{s}',{i});")
        _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{row}';")
    return row, uhash


def _insert_bars_upto(pg, upto):
    """Idempotently seed daily bars for both sim symbols through `upto` (800-bar
    depth ending at `upto`); ON CONFLICT DO NOTHING so incremental sessions add
    only the new dates."""
    cid = pg["cid"]
    for s in SIM_SYMS:
        vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, upto))
        _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals} "
                   "ON CONFLICT DO NOTHING;")


async def _register_and_execute(pg, monkeypatch, *, universe_id, universe_hash, snapshot):
    """Register + execute a prospective campaign at `snapshot` via the existing
    evaluation path (direct enqueue + worker handler). Returns campaign dict."""
    from app.jobs.prospective_enqueue import enqueue_prospective_campaign, sync_prospective_campaign
    from app.jobs.handlers.prospective import evaluate_prospective_symbol
    from app.workers.persistence import get_db_connection, release_db_connection
    cid = pg["cid"]
    cutoff = datetime.combine(snapshot, datetime.min.time(), timezone.utc) + timedelta(hours=20)
    reg_ident = pc.registration_identity(experiment_code="wyckoff_v2_vs_baseline", universe_id=str(universe_id),
                                         universe_hash=universe_hash, history_config_hash="sha256:cfg",
                                         snapshot_session_date=snapshot.isoformat())
    reg_id = str(uuid.uuid4())
    _psql(cid,
          "INSERT INTO prospective_campaign_registrations(id,experiment_code,experiment_contract_version,"
          "universe_id,universe_code,universe_version,universe_hash,history_config_hash,"
          "history_readiness_manifest_hash,candidate_strategy_code,candidate_strategy_version,"
          "candidate_signal_definition,candidate_allow_enter,control_strategy_code,control_strategy_version,"
          "snapshot_session_date,snapshot_cutoff_at,market_calendar_version,registration_identity,status) "
          f"VALUES('{reg_id}','wyckoff_v2_vs_baseline','wyckoff_v2_prospective_experiment.v1','{universe_id}','{SIM_CODE}',1,"
          f"'{universe_hash}','sha256:cfg','sha256:manifest','wyckoff_mtf_v2','wyckoff_mtf.v2',"
          f"'pre_rollout_enter_eligible.v1',FALSE,'sma150_bounce','sma150.v2','{snapshot.isoformat()}',"
          f"'{cutoff.isoformat()}','us_market_calendar.v1','{reg_ident}','registered');")
    runner = await asyncpg.connect(pg["runner_dsn"])
    try:
        r1 = await enqueue_prospective_campaign(runner, registration_id=reg_id,
                                                registration_identity=reg_ident, requested_by="sim")
        assert r1["status"] == "queued", r1
        cjob = r1["job_id"]
    finally:
        await runner.close()
    await _bind_global_pool(monkeypatch, pg["worker_dsn"])
    from app.workers.persistence import get_db_connection, release_db_connection
    for _ in range(len(SIM_SYMS)):
        conn = await get_db_connection()
        try:
            t = await Q.claim_next_task(conn, queue_name="prospective", worker_id="simw", lease_seconds=900)
            assert t is not None
            payload = _jl(t["payload"])
        finally:
            await release_db_connection(conn)
        res = await evaluate_prospective_symbol(payload)
        assert res["ok"] is True, res
        conn = await get_db_connection()
        try:
            await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="simw", result_summary=res["result"])
            await sync_prospective_campaign(conn, t["job_id"])
        finally:
            await release_db_connection(conn)
    conn = await get_db_connection()
    try:
        reg = await conn.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", reg_id)
        assert reg["status"] == "completed", dict(reg)
    finally:
        await release_db_connection(conn)
    return {"registration_id": reg_id, "registration_identity": reg_ident,
            "campaign_run_id": str(reg["campaign_run_id"]), "campaign_job_id": str(cjob),
            "snapshot": snapshot}


async def _process_outcome_queue(pg, monkeypatch):
    """Drain + process every currently-claimable prospective_outcomes task via
    the real outcome handler (local-history, provider-free). Returns count."""
    from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
    from app.workers.persistence import get_db_connection, release_db_connection
    await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
    n = 0
    while True:
        conn = await get_db_connection()
        try:
            t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                        worker_id="simow", lease_seconds=900)
            payload = _jl(t["payload"]) if t else None
        finally:
            await release_db_connection(conn)
        if t is None:
            break
        res = await evaluate_prospective_outcome(payload)
        assert res["ok"] is True, res
        assert res["result"]["provider_called"] is False
        conn = await get_db_connection()
        try:
            await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="simow", result_summary=res["result"])
        finally:
            await release_db_connection(conn)
        n += 1
    return n


class TestDailyPipelineV2MultiSession:
    def test_horizon_accumulates_across_sessions_current_deferred(self, pg, monkeypatch):
        from app.config import settings
        from app.routers.admin import _v2_outcome_maturation_stage, _v2_audit_report_stage
        from app.jobs.prospective_outcome_enqueue import build_outcome_maturity_preflight
        from app.workers.persistence import get_db_connection, release_db_connection
        import app.prospective_session as psess

        base = datetime.now(timezone.utc).date() - timedelta(days=90)  # S0 well in the past
        holder = {"target": base}
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: holder["target"])
        monkeypatch.setattr(settings, "PROSPECTIVE_ALLOWED_EXPERIMENT_CODE", "wyckoff_v2_vs_baseline", raising=False)

        async def seed_v2_occurrence(conn, session_date, uhash, uid, current):
            occ = await DP.ensure_pipeline_occurrence(
                conn, schedule_code="SIM-DAILY", schedule_version=1,
                resolved_session_date=str(session_date), frozen_universe_hash=uhash,
                universe_id=uid, pipeline_contract_version=DP.PIPELINE_CONTRACT_VERSION_V2)
            oid = str(occ["id"])
            await DP.record_stage_result(conn, oid, stage=DP.STAGE_HISTORY_REFRESH,
                                         result={"state": DP.STAGE_STATE_COMPLETED})
            await DP.record_stage_result(conn, oid, stage=DP.STAGE_PROSPECTIVE_CAMPAIGN,
                                         result={"state": DP.STAGE_STATE_COMPLETED,
                                                 "campaign_registration_id": current["registration_id"],
                                                 "campaign_job_id": current["campaign_job_id"]})
            occ = await DP.get_pipeline_occurrence(conn, oid)  # fresh row (post-seed)
            return oid, occ

        async def go():
            uid, uhash = await _ensure_universe(pg)
            universe = {"universe_id": uid, "universe_hash": uhash, "symbols": SIM_SYMS}
            _insert_bars_upto(pg, base)
            # PRIOR campaign C0 at S0 (base). No forward bars yet.
            c0 = await _register_and_execute(pg, monkeypatch, universe_id=uid, universe_hash=uhash, snapshot=base)
            c0_run = c0["campaign_run_id"]

            covered_expectations = {1: ["ret_1d"], 3: ["ret_1d", "ret_3d"],
                                    5: ["ret_1d", "ret_3d", "ret_5d"],
                                    10: ["ret_1d", "ret_3d", "ret_5d", "ret_10d"],
                                    20: ["ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"]}
            for k in (1, 3, 5, 10, 20):
                target = base + timedelta(days=k)
                holder["target"] = target
                _insert_bars_upto(pg, target)  # history current through session N=S0+k
                # CURRENT campaign for this session (snapshot == target) — created
                # while C0 may already have matured outcomes (exercises the
                # run-scoped guard; the old global guard would have blocked here).
                await _bind_global_pool(monkeypatch, pg["runner_dsn"])
                cur = await _register_and_execute(pg, monkeypatch, universe_id=uid, universe_hash=uhash, snapshot=target)

                await _bind_global_pool(monkeypatch, pg["runner_dsn"])
                monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
                conn = await get_db_connection()
                try:
                    oid, occ = await seed_v2_occurrence(conn, target, uhash, uid, cur)
                    # v2 outcome stage: current deferred + prior sweep enqueues C0.
                    occ = await _v2_outcome_maturation_stage(conn, occ, oid, universe, datetime.now(timezone.utc))
                    view = DP.build_status_view(occ)
                    cur_mat = view["current_campaign_maturity"]
                    assert cur_mat["status"] == DM.CURRENT_DEFERRED, cur_mat
                    prior = view["prior_maturation"]
                    assert prior["eligible_count"] >= 1, prior  # C0 discovered
                    assert any(e["registration_id"] == c0["registration_id"] for e in prior["eligible"])
                finally:
                    await release_db_connection(conn)

                processed = await _process_outcome_queue(pg, monkeypatch)
                assert processed >= 1

                await _bind_global_pool(monkeypatch, pg["runner_dsn"])
                monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
                conn = await get_db_connection()
                try:
                    occ = await DP.get_pipeline_occurrence(conn, oid)
                    # re-advance outcome stage -> prior job recognized succeeded -> completed
                    occ = await _v2_outcome_maturation_stage(conn, occ, oid, universe, datetime.now(timezone.utc))
                    view = DP.build_status_view(occ)
                    assert view["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_COMPLETED, view
                    # audit -> occurrence succeeded
                    occ = await _v2_audit_report_stage(conn, occ, oid)
                    view = DP.build_status_view(occ)
                    assert view["occurrence_status"] == "succeeded", view

                    # C0 horizon coverage reflects k completed forward sessions.
                    cov = await conn.fetchrow(
                        "SELECT COUNT(*) n, COUNT(ret_1d) c1, COUNT(ret_3d) c3, COUNT(ret_5d) c5, "
                        "COUNT(ret_10d) c10, COUNT(ret_20d) c20, "
                        "COUNT(DISTINCT o.pair_id) dp, MAX(available_forward_bars) fb "
                        "FROM strategy_shadow_pair_outcomes o JOIN strategy_shadow_run_pairs rp "
                        "ON rp.pair_id=o.pair_id WHERE rp.run_id=$1", c0_run)
                    assert cov["n"] == len(SIM_SYMS) == cov["dp"], dict(cov)  # one row per pair, no dup
                    assert cov["fb"] == k
                    want = covered_expectations[k]
                    colmap = {"ret_1d": cov["c1"], "ret_3d": cov["c3"], "ret_5d": cov["c5"],
                              "ret_10d": cov["c10"], "ret_20d": cov["c20"]}
                    for col, v in colmap.items():
                        if col in want:
                            assert v == len(SIM_SYMS), f"k={k} {col} expected covered, got {v}"
                        else:
                            assert v == 0, f"k={k} {col} expected pending, got {v}"

                    # CURRENT campaign for this session has ZERO forward sessions -> deferred,
                    # NO outcome rows fabricated.
                    cur_rows = await conn.fetchval(
                        "SELECT COUNT(*) FROM strategy_shadow_pair_outcomes o JOIN strategy_shadow_run_pairs rp "
                        "ON rp.pair_id=o.pair_id WHERE rp.run_id=$1", cur["campaign_run_id"])
                    assert cur_rows == 0, f"current campaign must have no outcome rows, got {cur_rows}"
                finally:
                    await release_db_connection(conn)

            # ---- replay idempotency on the final occurrence ------------------
            await _bind_global_pool(monkeypatch, pg["runner_dsn"])
            monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
            conn = await get_db_connection()
            try:
                jobs_before = await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE queue_name=$1", C.PROSPECTIVE_OUTCOME_QUEUE)
                out_before = await conn.fetchval("SELECT COUNT(*) FROM strategy_shadow_pair_outcomes")
                # re-run a fresh occurrence for the SAME final session/universe -> same occurrence,
                # C0 already fully matured -> no eligible prior work, current deferred, no new jobs/rows.
                cur2 = cur  # reuse the final current campaign
                occ = await DP.ensure_pipeline_occurrence(
                    conn, schedule_code="SIM-DAILY", schedule_version=1,
                    resolved_session_date=str(base + timedelta(days=20)), frozen_universe_hash=uhash,
                    universe_id=uid, pipeline_contract_version=DP.PIPELINE_CONTRACT_VERSION_V2)
                # the final occurrence already completed; re-advancing its (done) stage is a no-op
                occ2 = await _v2_outcome_maturation_stage(conn, occ, str(occ["id"]), universe, datetime.now(timezone.utc)) \
                    if DP.current_stage(occ) == DP.STAGE_OUTCOME_MATURATION else occ
                jobs_after = await conn.fetchval("SELECT COUNT(*) FROM job_runs WHERE queue_name=$1", C.PROSPECTIVE_OUTCOME_QUEUE)
                out_after = await conn.fetchval("SELECT COUNT(*) FROM strategy_shadow_pair_outcomes")
                assert jobs_after == jobs_before, (jobs_before, jobs_after)
                assert out_after == out_before, (out_before, out_after)
                # C0 fully matured (20D complete)
                st = await conn.fetchval(
                    "SELECT COUNT(*) FROM strategy_shadow_pair_outcomes o JOIN strategy_shadow_run_pairs rp "
                    "ON rp.pair_id=o.pair_id WHERE rp.run_id=$1 AND o.outcome_status='complete'", c0_run)
                assert st == len(SIM_SYMS)
            finally:
                await release_db_connection(conn)

        _run(go())
