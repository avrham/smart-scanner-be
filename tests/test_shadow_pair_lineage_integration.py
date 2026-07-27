"""Real PostgreSQL integration for the shadow pair-lineage audit.

Isolated local PostgreSQL (never Supabase): real migrations, the committed
audit role + RLS policies (ready state), representative lineage fixtures, and
`GET /api/admin/shadow-cohort/pair-lineage` driven THROUGH the least-privilege
audit role over the real app. Proves the join across pair/run/evaluation/
run-pairs/campaign-telemetry is correct and the deterministic classifier lands
each scenario in the right class, all read-only. Skips without Docker.
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
REL8 = [
    "public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
    "public.strategy_shadow_pair_outcomes", "public.strategy_shadow_run_pairs",
    "public.strategy_shadow_runs", "public.daily_bars", "public.patterns",
    "public.pattern_configs",
]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Pair IDs (stable) mapped to expected classifications.
P = {
    "campaign":  "aa000001-0000-0000-0000-000000000000",
    "sibling":   "aa000002-0000-0000-0000-000000000000",
    "legacy":    "aa000003-0000-0000-0000-000000000000",
    "manual":    "aa000004-0000-0000-0000-000000000000",
    "manual_sib":"aa000005-0000-0000-0000-000000000000",
    "mistag":    "aa000006-0000-0000-0000-000000000000",
    "orphan":    "aa000007-0000-0000-0000-000000000000",
    "ambiguous": "aa000008-0000-0000-0000-000000000000",
}
R = {
    "camp":        ("bb000001-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-24T10:00:00Z", "camp-A"),
    "camp2":       ("bb000002-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-24T10:00:00Z", "camp-B"),
    "origin_nob":  ("bb000003-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-24T09:00:00Z", None),
    "legacy":      ("bb000004-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-23T17:00:00Z", None),
    "manual":      ("bb000005-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-24T12:00:00Z", None),
    "mistag":      ("bb000006-0000-0000-0000-000000000000", "other_experiment",       "completed", "2026-07-24T12:00:00Z", None),
    "orphan":      ("bb000007-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "completed", "2026-07-24T12:00:00Z", None),
    "ambiguous":   ("bb000008-0000-0000-0000-000000000000", "wyckoff_v2_vs_baseline", "failed",    "2026-07-24T12:00:00Z", None),
}


def _run_sql():
    stmts = []
    for key, (rid, exp, status, created, camp) in R.items():
        tel = (f"'{{\"campaign\":{{\"campaign_contract_version\":\"shadow_campaign.v1\","
               f"\"campaign_id\":\"{camp}\",\"experiment_code\":\"{exp}\","
               f"\"chunk_index\":0,\"chunk_count\":1,\"as_of_date\":\"2026-07-23\"}}}}'::jsonb"
               if camp else "NULL")
        stmts.append(
            "INSERT INTO public.strategy_shadow_runs"
            "(id,experiment_code,experiment_version,status,provider,requested_symbols,"
            "started_at,created_at,telemetry) "
            f"VALUES ('{rid}','{exp}','wyckoff_v2_shadow.v2','{status}','massive',"
            f"'[]'::jsonb,'{created}','{created}',{tel});"
        )
    return "\n".join(stmts)


def _pair_sql(pid, symbol, origin_run, exp="wyckoff_v2_vs_baseline"):
    return (
        "INSERT INTO public.strategy_shadow_pairs"
        "(id,origin_run_id,experiment_code,experiment_version,symbol,timeframe,provider,"
        "snapshot_date,market_data_as_of,frame_snapshot_version,frame_hash,frame_bar_count,"
        "frame_first_date,frame_last_date,frame_snapshot,pair_fingerprint,pair_fingerprint_version) "
        f"VALUES ('{pid}','{origin_run}','{exp}','wyckoff_v2_shadow.v2','{symbol}','1d','massive',"
        f"'2026-07-23','2026-07-23T00:00:00Z','daily_ohlcv_snapshot.v1','fh-{pid}',1,"
        f"'2026-07-23','2026-07-23','[]'::jsonb,'fp-{pid}','shadow_pair_fingerprint.v1');"
    )


def _link_sql(run_id, pid):
    return ("INSERT INTO public.strategy_shadow_run_pairs(run_id,pair_id,created_new_pair) "
            f"VALUES ('{run_id}','{pid}',true);")


def _eval_sql(pid, exp="wyckoff_v2_vs_baseline"):
    return (
        "INSERT INTO public.strategy_shadow_evaluations"
        "(id,pair_id,arm_code,strategy_code,strategy_version,decision_policy_version,"
        "config_hash,config_snapshot,verdict,details_snapshot,evaluation_fingerprint,"
        "evaluation_fingerprint_version) "
        f"VALUES ('cc{pid[2:]}','{pid}','candidate_wyckoff_v2','wyckoff_mtf_v2','wyckoff_mtf.v2',"
        f"'wyckoff_mtf.policy.v1','cfg1','{{}}'::jsonb,'AVOID','{{}}'::jsonb,"
        f"'efp-{pid}','shadow_evaluation_fingerprint.v1');"
    )


def _build_fixture_sql():
    s = [_run_sql()]
    rid = {k: v[0] for k, v in R.items()}
    # 1 campaign-linked (origin run has the block)
    s += [_pair_sql(P["campaign"], "AAA", rid["camp"]), _link_sql(rid["camp"], P["campaign"]),
          _eval_sql(P["campaign"])]
    # 2 missing telemetry on origin, but linked to a campaign run (deterministic)
    s += [_pair_sql(P["sibling"], "BBB", rid["origin_nob"]),
          _link_sql(rid["origin_nob"], P["sibling"]),
          _link_sql(rid["camp2"], P["sibling"]),  # sibling link carries campaign block
          _eval_sql(P["sibling"])]
    # 3 legacy pre-campaign
    s += [_pair_sql(P["legacy"], "CCC", rid["legacy"]), _link_sql(rid["legacy"], P["legacy"]),
          _eval_sql(P["legacy"])]
    # 4 manual + 5 manual sibling (same run, proves sibling rollup)
    s += [_pair_sql(P["manual"], "DDD", rid["manual"]), _link_sql(rid["manual"], P["manual"]),
          _eval_sql(P["manual"]),
          _pair_sql(P["manual_sib"], "EEE", rid["manual"]), _link_sql(rid["manual"], P["manual_sib"]),
          _eval_sql(P["manual_sib"])]
    # 6 incorrectly tagged (pair says wyckoff, run says other_experiment)
    s += [_pair_sql(P["mistag"], "FFF", rid["mistag"], exp="wyckoff_v2_vs_baseline"),
          _link_sql(rid["mistag"], P["mistag"]), _eval_sql(P["mistag"])]
    # 7 orphan (no evaluation)
    s += [_pair_sql(P["orphan"], "GGG", rid["orphan"]), _link_sql(rid["orphan"], P["orphan"])]
    # 8 ambiguous (failed run, no campaign block)
    s += [_pair_sql(P["ambiguous"], "HHH", rid["ambiguous"]),
          _link_sql(rid["ambiguous"], P["ambiguous"]), _eval_sql(P["ambiguous"])]
    return "\n".join(s)


def _docker_ready() -> bool:
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_ready(), reason="docker or postgres:16-alpine image unavailable")


def _sh(args, inp=None, timeout=120):
    return subprocess.run(args, input=inp, capture_output=True, text=True, timeout=timeout)


def _psql(cid, sql, *, role="postgres", variables=None, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", role, "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    return _sh(args, inp=(open(path).read() if path else sql))


def _policy_on_all(cid, template):
    return _psql(cid, "\n".join(template.format(rel=r) for r in REL8))


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres",
               "-P", PG_IMAGE]).stdout.strip()
    assert cid
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready", "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        host_port = int(_sh(["docker", "port", cid, "5432/tcp"]).stdout.splitlines()[0].rsplit(":", 1)[1])
        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                    f"CREATE DATABASE {DBNAME};"]).returncode == 0
        for m in MIGRATIONS:
            r = _psql(cid, None, path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        r = _psql(cid, _build_fixture_sql())
        assert r.returncode == 0, f"fixtures: {r.stderr[-800:]}"
        r = _psql(cid, None, variables={"audit_password": AUDIT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_audit_reader.sql"))
        assert r.returncode == 0, f"role: {r.stderr[-400:]}"
        assert _policy_on_all(cid, "ALTER TABLE {rel} ENABLE ROW LEVEL SECURITY;").returncode == 0
        assert _psql(cid, None, path=os.path.join(
            REPO, "ops", "sql", "create_shadow_audit_rls_policies.sql")).returncode == 0
        yield {"cid": cid,
               "audit_dsn": f"postgresql://{AUDIT_ROLE}:{AUDIT_PW}@127.0.0.1:{host_port}/{DBNAME}"}
    finally:
        _sh(["docker", "stop", cid])


async def _call_lineage(dsn, pair_ids):
    from httpx import ASGITransport, AsyncClient
    from main import app
    from app.config import settings
    from app.deps import get_worker_token, close_db_pool

    saved = (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL, settings.AUDIT_EXPECTED_DB_ROLE)
    settings.AUDIT_ONLY_MODE = True
    settings.AUDIT_DATABASE_URL = dsn
    settings.AUDIT_EXPECTED_DB_ROLE = AUDIT_ROLE
    app.dependency_overrides[get_worker_token] = lambda: "t"
    await close_db_pool()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/api/admin/shadow-cohort/pair-lineage",
                                params={"pair_ids": ",".join(pair_ids)})
        return resp.status_code, resp.json()
    finally:
        await close_db_pool()
        app.dependency_overrides.pop(get_worker_token, None)
        (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
         settings.AUDIT_EXPECTED_DB_ROLE) = saved


def _lineage(pg, keys):
    status, body = asyncio.run(_call_lineage(pg["audit_dsn"], [P[k] for k in keys]))
    assert status == 200, body
    return {p["pair_id"]: p for p in body["pairs"]}


class TestLineageClassificationReal:
    def test_all_scenarios_classify_correctly(self, pg):
        out = _lineage(pg, ["campaign", "sibling", "legacy", "manual",
                            "mistag", "orphan", "ambiguous"])
        assert out[P["campaign"]]["classification"] == "legitimate_campaign_record_with_missing_telemetry"
        assert out[P["campaign"]]["assigned_campaign_id"] == "camp-A"
        assert out[P["sibling"]]["classification"] == "legitimate_campaign_record_with_missing_telemetry"
        assert out[P["sibling"]]["assigned_campaign_id"] == "camp-B"
        assert out[P["sibling"]]["deterministic_campaign_assignable"] is True
        assert out[P["legacy"]]["classification"] == "legacy_experiment_run_before_campaign_telemetry"
        assert out[P["manual"]]["classification"] == "manual_non_campaign_shadow_run"
        assert out[P["mistag"]]["classification"] == "incorrectly_tagged_experiment_record"
        assert out[P["orphan"]]["classification"] == "orphan_or_inconsistent_record"
        assert out[P["ambiguous"]]["classification"] == "unverifiable"

    def test_sibling_records_included_for_manual_run(self, pg):
        out = _lineage(pg, ["manual"])
        run = out[P["manual"]]["lineage"]["runs"][0]
        assert run["run_pair_count"] == 2
        assert set(run["run_sibling_symbols"]) == {"DDD", "EEE"}
        assert run["campaign_telemetry_present"] is False

    def test_campaign_block_is_bounded_not_dumped(self, pg):
        out = _lineage(pg, ["campaign"])
        run = out[P["campaign"]]["lineage"]["runs"][0]
        assert run["campaign_id"] == "camp-A"
        assert run["telemetry_keys"] == ["campaign"]

    def test_resolutions_match_classes(self, pg):
        out = _lineage(pg, ["campaign", "legacy", "manual"])
        assert out[P["campaign"]]["recommended_resolution"] == "backfill_campaign_metadata"
        assert out[P["legacy"]]["recommended_resolution"] == "retain_as_legacy_non_campaign_evidence"
        assert out[P["manual"]]["recommended_resolution"] == "exclude_from_campaign_cohort_but_retain_record"


class TestReadOnly:
    def test_access_is_audit_reader_and_no_writes(self, pg):
        async def snapshot():
            conn = await asyncpg.connect(pg["audit_dsn"])
            try:
                who = await conn.fetchval("SELECT current_user")
                runs = await conn.fetchval("SELECT count(*) FROM strategy_shadow_runs")
                pairs = await conn.fetchval("SELECT count(*) FROM strategy_shadow_pairs")
                return who, runs, pairs
            finally:
                await conn.close()
        who, runs_before, pairs_before = asyncio.run(snapshot())
        assert who == AUDIT_ROLE
        _lineage(pg, ["campaign", "manual", "orphan"])
        _, runs_after, pairs_after = asyncio.run(snapshot())
        assert (runs_before, pairs_before) == (runs_after, pairs_after)
