"""Real-Postgres integration for the daily-pipeline outcome_maturation stage.

The wired ``admin.daily_pipeline_advance`` outcome stage replaced a hardcoded
BLOCKED stub. This lives in its OWN module (own Docker container via the shared
``pg`` fixture, module-scoped) so it starts from a pristine outcome-queue and a
zero-outcome slate — ``enqueue_prospective_campaign`` enforces a GLOBAL
assert_no_outcomes guard, and ``claim_next_task`` claims by queue, so a shared
container polluted by other outcome tests would make this test claim foreign
tasks. Isolation keeps it deterministic.

Covers the operator-required scenarios:
  * eligibility_unknown (no forward history) -> stage BLOCKED, no job, never faked
  * eligible -> exactly one durable outcome job, scoped to THIS campaign only
  * same-occurrence replay -> recognised, NO duplicate job
  * a succeeded outcome job -> stage completed -> audit_report -> occurrence succeeded
  * exactly one outcome row per pair (no duplicate, provider-free)
  * a second occurrence once pairs are fully matured -> no_eligible_work
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

import app.prospective_campaign as pc
from app.jobs import contracts as C
from app.jobs import queue as Q

# Reuse the sibling module's heavy Docker fixture + helpers (bare-name import:
# pytest's prepend import mode puts tests/ on sys.path, matching this repo's
# existing `from test_wyckoff_v2_9f_cohorts import ...` precedent).
from test_prospective_outcome_maturation_integration import (  # noqa: E402
    pg, _no_external_network, _bind_global_pool, _run, _gen_daily, _psql, _sh,
    _docker_ready, DBNAME,
)

pytestmark = pytest.mark.skipif(not _docker_ready(), reason="docker/pg image unavailable")

WIRE_SYMS = ["ZZWA", "ZZWB"]
WIRE_CODE = "ZZWIRE"


async def _independent_completed_campaign(pg, monkeypatch, *, code, syms, snapshot):
    """Build a self-contained completed campaign (own frozen universe +
    registration + pairs) so the outcome-stage scenarios control forward-session
    eligibility deterministically."""
    from app.history_warmup_execute import compute_universe_hash
    from app.jobs.prospective_enqueue import enqueue_prospective_campaign, sync_prospective_campaign
    from app.jobs.handlers.prospective import evaluate_prospective_symbol
    from app.workers.persistence import get_db_connection, release_db_connection

    cid = pg["cid"]
    _psql(cid, "INSERT INTO history_warmup_universes(universe_code,universe_version,universe_hash,"
          f"config_hash,status,symbol_count,frozen_at) VALUES('{code}',1,'pending','cfg','draft',{len(syms)},NULL);")
    uid = _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres", "-d", DBNAME,
               "-c", f"SELECT id FROM history_warmup_universes WHERE universe_code='{code}';"]).stdout.strip()
    for i, s in enumerate(syms):
        _psql(cid, f"INSERT INTO history_warmup_universe_symbols(universe_id,symbol,ordinal) VALUES('{uid}','{s}',{i});")
    uhash = compute_universe_hash(universe_code=code, universe_version=1, symbols_in_ordinal_order=syms)
    _psql(cid, f"UPDATE history_warmup_universes SET status='frozen', universe_hash='{uhash}', frozen_at=NOW() WHERE id='{uid}';")
    for s in syms:
        vals = ",".join(f"('{r[0]}','{r[1]}',{r[2]},{r[3]},{r[4]},{r[5]},{r[6]})" for r in _gen_daily(s, snapshot))
        _psql(cid, f"INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) VALUES {vals};")
    cutoff = datetime.combine(snapshot, datetime.min.time(), timezone.utc) + timedelta(hours=20)
    reg_ident = pc.registration_identity(experiment_code="wyckoff_v2_vs_baseline", universe_id=str(uid),
                                         universe_hash=uhash, history_config_hash="sha256:cfg",
                                         snapshot_session_date=snapshot.isoformat())
    reg_id = str(uuid.uuid4())
    _psql(cid,
          "INSERT INTO prospective_campaign_registrations(id,experiment_code,experiment_contract_version,"
          "universe_id,universe_code,universe_version,universe_hash,history_config_hash,"
          "history_readiness_manifest_hash,candidate_strategy_code,candidate_strategy_version,"
          "candidate_signal_definition,candidate_allow_enter,control_strategy_code,control_strategy_version,"
          "snapshot_session_date,snapshot_cutoff_at,market_calendar_version,registration_identity,status) "
          f"VALUES('{reg_id}','wyckoff_v2_vs_baseline','wyckoff_v2_prospective_experiment.v1','{uid}','{code}',1,"
          f"'{uhash}','sha256:cfg','sha256:manifest','wyckoff_mtf_v2','wyckoff_mtf.v2',"
          f"'pre_rollout_enter_eligible.v1',FALSE,'sma150_bounce','sma150.v2','{snapshot.isoformat()}',"
          f"'{cutoff.isoformat()}','us_market_calendar.v1','{reg_ident}','registered');")

    runner = await asyncpg.connect(pg["runner_dsn"])
    try:
        r1 = await enqueue_prospective_campaign(runner, registration_id=reg_id,
                                                registration_identity=reg_ident, requested_by="wiretest")
        assert r1["status"] == "queued"
        campaign_job_id = r1["job_id"]
    finally:
        await runner.close()

    await _bind_global_pool(monkeypatch, pg["worker_dsn"])
    for _ in range(len(syms)):
        conn = await get_db_connection()
        try:
            t = await Q.claim_next_task(conn, queue_name="prospective", worker_id="wwire", lease_seconds=900)
            assert t is not None
            payload = _json_load(t["payload"])
        finally:
            await release_db_connection(conn)
        res = await evaluate_prospective_symbol(payload)
        assert res["ok"] is True, res
        conn = await get_db_connection()
        try:
            await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="wwire", result_summary=res["result"])
            await sync_prospective_campaign(conn, t["job_id"])
        finally:
            await release_db_connection(conn)

    conn = await get_db_connection()
    try:
        reg = await conn.fetchrow("SELECT * FROM prospective_campaign_registrations WHERE id=$1", reg_id)
        assert reg["status"] == "completed"
        pairs = await conn.fetch(
            "SELECT p.id AS pair_id, p.symbol FROM strategy_shadow_run_pairs rp "
            "JOIN strategy_shadow_pairs p ON p.id=rp.pair_id WHERE rp.run_id=$1", reg["campaign_run_id"])
        assert len(pairs) == len(syms)
    finally:
        await release_db_connection(conn)
    return {"registration_id": reg_id, "registration_identity": reg_ident, "universe_id": str(uid),
            "universe_hash": uhash, "campaign_run_id": str(reg["campaign_run_id"]),
            "campaign_job_id": str(campaign_job_id), "snapshot": snapshot,
            "pairs": [{"pair_id": str(r["pair_id"]), "symbol": r["symbol"]} for r in pairs]}


def _json_load(v):
    import json
    return json.loads(v) if isinstance(v, str) else v


class TestDailyPipelineOutcomeStageWiring:
    def test_full_lifecycle_and_no_eligible_work(self, pg, monkeypatch):
        from app.config import settings
        from app.routers.admin import daily_pipeline_advance
        from app.jobs import daily_pipeline as DP
        import app.prospective_session as psess
        from app.jobs.handlers.prospective_outcome import evaluate_prospective_outcome
        from app.workers.persistence import get_db_connection, release_db_connection

        today = datetime.now(timezone.utc).date()
        snapshot = today - timedelta(days=60)
        session1 = today - timedelta(days=2)
        session2 = today - timedelta(days=1)
        holder = {"d": session1}
        monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: holder["d"])

        async def seed_occurrence(conn, session_date, camp):
            occ = await DP.ensure_pipeline_occurrence(
                conn, schedule_code="SMART-SCANNER-DAILY-PIPELINE", schedule_version=1,
                resolved_session_date=str(session_date), frozen_universe_hash=camp["universe_hash"],
                universe_id=camp["universe_id"])
            oid = str(occ["id"])
            await DP.record_stage_result(conn, oid, stage=DP.STAGE_HISTORY_REFRESH,
                                         result={"state": DP.STAGE_STATE_COMPLETED})
            await DP.record_stage_result(conn, oid, stage=DP.STAGE_PROSPECTIVE_CAMPAIGN,
                                         result={"state": DP.STAGE_STATE_COMPLETED,
                                                 "campaign_registration_id": camp["registration_id"],
                                                 "campaign_job_id": camp["campaign_job_id"]})
            got = await DP.get_pipeline_occurrence(conn, oid)
            assert DP.current_stage(got) == DP.STAGE_OUTCOME_MATURATION
            return oid

        async def go():
            camp = await _independent_completed_campaign(
                pg, monkeypatch, code=WIRE_CODE, syms=WIRE_SYMS, snapshot=snapshot)
            body = {"contract_version": DP.PIPELINE_CONTRACT_VERSION, "universe_id": camp["universe_id"]}

            # ---- occurrence #1 at the outcome stage, ZERO forward bars --------
            await _bind_global_pool(monkeypatch, pg["runner_dsn"])
            monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
            conn = await get_db_connection()
            try:
                oid1 = await seed_occurrence(conn, session1, camp)
                # (1) unknown eligibility (no forward history) -> BLOCKED, no job
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                assert v["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_BLOCKED
                assert v["occurrence_status"] == "running"
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE queue_name=$1 AND registration_id=$2",
                    C.PROSPECTIVE_OUTCOME_QUEUE, camp["registration_id"]) == 0
            finally:
                await release_db_connection(conn)

            # add 25 forward sessions -> pairs become eligible and fully maturable
            su = await asyncpg.connect(pg["su_dsn"])
            try:
                for k in range(1, 26):
                    d = snapshot + timedelta(days=k)
                    for s in WIRE_SYMS:
                        await su.execute(
                            "INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume) "
                            "VALUES ($1,$2,120,122,119,121,1000000) ON CONFLICT DO NOTHING", s, d)
            finally:
                await su.close()

            # (2) eligible -> ONE durable job, scoped to THIS campaign, in_progress
            conn = await get_db_connection()
            try:
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                assert v["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_IN_PROGRESS
                oj = v["outcome_job_id"]
                assert oj is not None
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE queue_name=$1 AND registration_id=$2",
                    C.PROSPECTIVE_OUTCOME_QUEUE, camp["registration_id"]) == 1
                jr = await conn.fetchrow("SELECT registration_id, total_task_count FROM job_runs WHERE id=$1", oj)
                assert str(jr["registration_id"]) == camp["registration_id"]
                assert jr["total_task_count"] == len(WIRE_SYMS)
                for trow in await conn.fetch("SELECT payload FROM job_tasks WHERE job_id=$1", oj):
                    pl = _json_load(trow["payload"])
                    assert pl["registration_id"] == camp["registration_id"]  # no cross-campaign reuse

                # (3) replay while job still queued -> same job, in_progress, NO dup
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                assert v["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_IN_PROGRESS
                assert v["outcome_job_id"] == oj
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE queue_name=$1 AND registration_id=$2",
                    C.PROSPECTIVE_OUTCOME_QUEUE, camp["registration_id"]) == 1
            finally:
                await release_db_connection(conn)

            # process the outcome tasks to success (real handler, provider-free)
            await _bind_global_pool(monkeypatch, pg["outcome_dsn"])
            for _ in range(len(WIRE_SYMS)):
                conn = await get_db_connection()
                try:
                    t = await Q.claim_next_task(conn, queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
                                                worker_id="owwire", lease_seconds=900)
                    assert t is not None
                    payload = _json_load(t["payload"])
                finally:
                    await release_db_connection(conn)
                res = await evaluate_prospective_outcome(payload)
                assert res["ok"] is True, res
                assert res["result"]["provider_called"] is False
                conn = await get_db_connection()
                try:
                    await Q.complete_task_succeeded(conn, task_id=t["id"], worker_id="owwire",
                                                    result_summary=res["result"])
                finally:
                    await release_db_connection(conn)

            # (4) succeeded outcome job recognised -> stage completed -> audit_report
            await _bind_global_pool(monkeypatch, pg["runner_dsn"])
            monkeypatch.setattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", True)
            conn = await get_db_connection()
            try:
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                assert v["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_COMPLETED
                assert v["current_stage"] == DP.STAGE_AUDIT_REPORT
                assert await conn.fetchval(
                    "SELECT count(*) FROM strategy_shadow_pair_outcomes o "
                    "JOIN strategy_shadow_run_pairs rp ON rp.pair_id=o.pair_id WHERE rp.run_id=$1",
                    camp["campaign_run_id"]) == len(WIRE_SYMS)  # exactly one row per pair

                # (5) audit_report -> completed -> occurrence succeeded
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                assert v["stage_states"][DP.STAGE_AUDIT_REPORT] == DP.STAGE_STATE_COMPLETED
                assert v["occurrence_status"] == "succeeded"
                assert v["current_stage"] == DP.STAGE_DONE
            finally:
                await release_db_connection(conn)

            # ---- occurrence #2: pairs now fully matured -> no_eligible_work -----
            holder["d"] = session2
            conn = await get_db_connection()
            try:
                oid2 = await seed_occurrence(conn, session2, camp)
                assert oid2 != oid1
                v = await daily_pipeline_advance(_="t", db=conn, body=body)
                # known maturity, zero eligible -> honest bounded success (NOT a
                # new job, NOT blocked)
                assert v["stage_states"][DP.STAGE_OUTCOME_MATURATION] == DP.STAGE_STATE_COMPLETED
                assert v["current_stage"] == DP.STAGE_AUDIT_REPORT
                assert await conn.fetchval(
                    "SELECT count(*) FROM job_runs WHERE queue_name=$1 AND registration_id=$2",
                    C.PROSPECTIVE_OUTCOME_QUEUE, camp["registration_id"]) == 1  # no new job
                stage_result = DP.pipeline_summary(
                    await DP.get_pipeline_occurrence(conn, oid2))["stages"][DP.STAGE_OUTCOME_MATURATION]
                assert stage_result.get("outcome_result") == "no_eligible_work"
                assert stage_result.get("outcome_job_id") is None
            finally:
                await release_db_connection(conn)

        _run(go())
