"""Real PostgreSQL integration for the bounded maturation plan.

Spins up an ISOLATED local PostgreSQL container (never Supabase), applies the
real migrations, creates `smart_scanner_audit_reader` + the committed RLS
policies (the ready state), inserts representative shadow fixtures and drives
`GET /api/admin/shadow-cohort/maturation-plan` THROUGH the least-privilege audit
role over the real app.

It proves, against real PostgreSQL:
  * the eligible manifest count is exact and equals the authoritative eligible
    count; eligible and retry entries are separated;
  * pagination has no duplicates or omissions and the manifest hash is stable
    across page sizes;
  * benign cross-campaign duplicate symbol-sessions do NOT block, while
    same-campaign duplicates DO;
  * membership-unverifiable, mismatched strategy identity and terminal failures
    each produce blocking reasons;
  * access happens as `smart_scanner_audit_reader`; no write or DDL occurs.

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
REL8 = [
    "public.strategy_shadow_evaluations", "public.strategy_shadow_pairs",
    "public.strategy_shadow_pair_outcomes", "public.strategy_shadow_run_pairs",
    "public.strategy_shadow_runs", "public.daily_bars", "public.patterns",
    "public.pattern_configs",
]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _u(n: int) -> str:
    return f"{n:08d}-0000-0000-0000-000000000000"


def _build_fixture_sql() -> str:
    """Deterministic fixtures covering every plan scenario.

    Experiments (each queried independently by experiment_code):
      exp_clean    — 6 plain eligible + a benign cross-campaign duplicate pair
                     (2 pairs, camp-1 & camp-2) + 1 matured + 1 not_yet ⇒ 8
                     eligible, safe_to_execute.
      exp_retry    — 1 eligible + 1 retryable failure + 1 terminal failure.
      exp_samecamp — 2 eligible pairs, same symbol/session, SAME campaign.
      exp_unverif  — 1 eligible pair with no campaign telemetry (unverifiable).
      exp_mismatch — 2 eligible pairs with different strategy_version.
    """
    stmts = []
    # SPY trading calendar: weekdays 2026-05-01 .. 2026-06-30 (latest = 06-30).
    stmts.append(
        "INSERT INTO public.daily_bars(symbol,trading_date,open,high,low,close,volume,source) "
        "SELECT 'SPY', d::date,1,1,1,1,1000,'test' "
        "FROM generate_series('2026-05-01'::date,'2026-06-30'::date,'1 day') d "
        "WHERE extract(dow from d) NOT IN (0,6) "
        "ON CONFLICT (symbol,trading_date) DO NOTHING;"
    )

    runs = {
        "runA": ("exp_clean", "camp-1"),
        "runB": ("exp_clean", "camp-2"),
        "runR": ("exp_retry", "camp-r"),
        "runS1": ("exp_samecamp", "camp-s"),
        "runS2": ("exp_samecamp", "camp-s"),
        "runU": ("exp_unverif", None),
        "runM1": ("exp_mismatch", "camp-m"),
        "runM2": ("exp_mismatch", "camp-m"),
    }
    run_ids = {name: _u(1000 + i) for i, name in enumerate(runs)}
    for name, (exp, camp) in runs.items():
        # A full VALID campaign telemetry block (campaign_id + experiment_code +
        # as_of_date), matching the real _campaign_telemetry_block shape.
        tel = (
            "'{\"campaign\":{\"campaign_contract_version\":\"shadow_campaign.v1\","
            f"\"campaign_id\":\"{camp}\",\"experiment_code\":\"{exp}\","
            "\"chunk_index\":0,\"chunk_count\":1,\"as_of_date\":\"2026-06-01\"}}'::jsonb"
            if camp else "NULL")
        stmts.append(
            "INSERT INTO public.strategy_shadow_runs"
            "(id,experiment_code,experiment_version,status,provider,telemetry,started_at) "
            f"VALUES ('{run_ids[name]}','{exp}','wyckoff_v2_shadow.v2','completed',"
            f"'massive',{tel},NOW());"
        )

    # pair spec: (key, exp, symbol, snap, origin_run, strategy_version,
    #             outcome_status, error_code, with_control)
    pairs = []
    for i in range(6):
        pairs.append((f"c{i}", "exp_clean", f"S{i:02d}", "2026-06-01",
                      "runA" if i % 2 == 0 else "runB", "wyckoff_mtf.v2",
                      None, None, False))
    pairs += [
        ("cd1", "exp_clean", "DUPX", "2026-06-02", "runA", "wyckoff_mtf.v2", None, None, False),
        ("cd2", "exp_clean", "DUPX", "2026-06-02", "runB", "wyckoff_mtf.v2", None, None, False),
        ("cm", "exp_clean", "MATU", "2026-06-01", "runA", "wyckoff_mtf.v2", "complete", None, True),
        ("cn", "exp_clean", "NOTY", "2026-06-30", "runA", "wyckoff_mtf.v2", None, None, False),
        ("r0", "exp_retry", "RA", "2026-06-01", "runR", "wyckoff_mtf.v2", None, None, False),
        ("rr", "exp_retry", "RB", "2026-06-01", "runR", "wyckoff_mtf.v2", "error", "forward_fetch_error", True),
        ("rt", "exp_retry", "RC", "2026-06-01", "runR", "wyckoff_mtf.v2", "error", "reference_revision_detected", True),
        ("s1", "exp_samecamp", "SAME", "2026-06-01", "runS1", "wyckoff_mtf.v2", None, None, False),
        ("s2", "exp_samecamp", "SAME", "2026-06-01", "runS2", "wyckoff_mtf.v2", None, None, False),
        ("u1", "exp_unverif", "UNV", "2026-06-01", "runU", "wyckoff_mtf.v2", None, None, False),
        ("m1", "exp_mismatch", "MA", "2026-06-01", "runM1", "wyckoff_mtf.v2", None, None, False),
        ("m2", "exp_mismatch", "MB", "2026-06-01", "runM2", "wyckoff_mtf.v1", None, None, False),
    ]
    pair_ids = {key: _u(2000 + i) for i, (key, *_rest) in enumerate(pairs)}
    eval_seq = 3000
    for key, exp, symbol, snap, origin, sv, status, err, with_control in pairs:
        pid = pair_ids[key]
        rid = run_ids[origin]
        stmts.append(
            "INSERT INTO public.strategy_shadow_pairs"
            "(id,origin_run_id,experiment_code,experiment_version,symbol,timeframe,provider,"
            "snapshot_date,market_data_as_of,frame_snapshot_version,frame_hash,frame_bar_count,"
            "frame_first_date,frame_last_date,frame_snapshot,pair_fingerprint,pair_fingerprint_version) "
            f"VALUES ('{pid}','{rid}','{exp}','wyckoff_v2_shadow.v2','{symbol}','1d','massive',"
            f"'{snap}','{snap}T00:00:00Z','daily_ohlcv_snapshot.v1','fh-{key}',1,"
            f"'{snap}','{snap}','[]'::jsonb,'fp-{key}','shadow_pair_fingerprint.v1');"
        )
        stmts.append(
            "INSERT INTO public.strategy_shadow_run_pairs(run_id,pair_id,created_new_pair) "
            f"VALUES ('{rid}','{pid}',true);"
        )
        # Candidate arm (the evidence record the plan reads).
        stmts.append(
            "INSERT INTO public.strategy_shadow_evaluations"
            "(id,pair_id,arm_code,strategy_code,strategy_version,decision_policy_version,"
            "config_hash,config_snapshot,verdict,details_snapshot,evaluation_fingerprint,"
            "evaluation_fingerprint_version) "
            f"VALUES ('{_u(eval_seq)}','{pid}','candidate_wyckoff_v2','wyckoff_mtf_v2','{sv}',"
            f"'wyckoff_mtf.policy.v1','cfg1','{{}}'::jsonb,'AVOID','{{}}'::jsonb,"
            f"'efp-{key}-cand','shadow_evaluation_fingerprint.v1');"
        )
        eval_seq += 1
        if with_control:
            # Control arm — required for the joined outcome row (error_code).
            stmts.append(
                "INSERT INTO public.strategy_shadow_evaluations"
                "(id,pair_id,arm_code,strategy_code,strategy_version,decision_policy_version,"
                "config_hash,config_snapshot,verdict,details_snapshot,evaluation_fingerprint,"
                "evaluation_fingerprint_version) "
                f"VALUES ('{_u(eval_seq)}','{pid}','control_baseline','sma150_bounce','sma150.v1',"
                f"'sma150.policy.v1','cfg0','{{}}'::jsonb,'AVOID','{{}}'::jsonb,"
                f"'efp-{key}-ctrl','shadow_evaluation_fingerprint.v1');"
            )
            eval_seq += 1
        if status is not None:
            bars = 20 if status == "complete" else 0
            err_sql = f"'{err}'" if err else "NULL"
            stmts.append(
                "INSERT INTO public.strategy_shadow_pair_outcomes"
                "(id,pair_id,outcome_fingerprint,outcome_fingerprint_version,calculation_version,"
                "outcome_coverage_version,forward_frame_version,reference_price_role,"
                "available_forward_bars,outcome_status,error_code) "
                f"VALUES ('{_u(eval_seq)}','{pid}','ofp-{key}',"
                "'shadow_pair_outcome_fingerprint.v1','outcome.v1','shadow_pair_outcomes.v1',"
                f"'shadow_forward_bars.v1','paired_decision_observation',{bars},'{status}',{err_sql});"
            )
            eval_seq += 1

    # ---- extra campaign-scope scenarios (raw SQL) ------------------------- #
    def _valid_block(camp, exp):
        return ("'{\"campaign\":{\"campaign_contract_version\":\"shadow_campaign.v1\","
                f"\"campaign_id\":\"{camp}\",\"experiment_code\":\"{exp}\","
                "\"chunk_index\":0,\"chunk_count\":1,\"as_of_date\":\"2026-06-01\"}}'::jsonb")

    def _run(rid, exp, tel):
        return ("INSERT INTO public.strategy_shadow_runs"
                "(id,experiment_code,experiment_version,status,provider,telemetry,started_at) "
                f"VALUES ('{rid}','{exp}','wyckoff_v2_shadow.v2','completed','massive',{tel},NOW());")

    def _pair(pid, exp, symbol, origin):
        return ("INSERT INTO public.strategy_shadow_pairs"
                "(id,origin_run_id,experiment_code,experiment_version,symbol,timeframe,provider,"
                "snapshot_date,market_data_as_of,frame_snapshot_version,frame_hash,frame_bar_count,"
                "frame_first_date,frame_last_date,frame_snapshot,pair_fingerprint,pair_fingerprint_version) "
                f"VALUES ('{pid}','{origin}','{exp}','wyckoff_v2_shadow.v2','{symbol}','1d','massive',"
                "'2026-06-01','2026-06-01T00:00:00Z','daily_ohlcv_snapshot.v1','fh-'||"
                f"'{pid}',1,'2026-06-01','2026-06-01','[]'::jsonb,'fp-{pid}','shadow_pair_fingerprint.v1');")

    def _link(rid, pid):
        return ("INSERT INTO public.strategy_shadow_run_pairs(run_id,pair_id,created_new_pair) "
                f"VALUES ('{rid}','{pid}',true);")

    def _eval(eid, pid):
        return ("INSERT INTO public.strategy_shadow_evaluations"
                "(id,pair_id,arm_code,strategy_code,strategy_version,decision_policy_version,"
                "config_hash,config_snapshot,verdict,details_snapshot,evaluation_fingerprint,"
                f"evaluation_fingerprint_version) VALUES ('{eid}','{pid}','candidate_wyckoff_v2',"
                "'wyckoff_mtf_v2','wyckoff_mtf.v2','wyckoff_mtf.policy.v1','cfg1','{}'::jsonb,'AVOID',"
                f"'{{}}'::jsonb,'efp-{pid}','shadow_evaluation_fingerprint.v1');")

    rmc1, rmc2 = _u(4001), _u(4002)
    pmc = _u(4101)
    stmts += [
        _run(rmc1, "exp_multi", _valid_block("camp-mc1", "exp_multi")),
        _run(rmc2, "exp_multi", _valid_block("camp-mc2", "exp_multi")),
        _pair(pmc, "exp_multi", "MC", rmc1),
        _link(rmc1, pmc), _link(rmc2, pmc),  # one pair linked to TWO campaigns
        _eval(_u(4201), pmc),
    ]
    rcf = _u(4003)
    pcf = _u(4102)
    stmts += [
        # campaign block whose experiment_code disagrees ⇒ conflicting telemetry
        _run(rcf, "exp_conf", _valid_block("camp-cf", "other_experiment")),
        _pair(pcf, "exp_conf", "CF", rcf),
        _link(rcf, pcf), _eval(_u(4202), pcf),
    ]
    return "\n".join(stmts)


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


def _sh(args, inp=None, timeout=120):
    return subprocess.run(args, input=inp, capture_output=True, text=True, timeout=timeout)


def _psql(cid, sql, *, role="postgres", variables=None, path=None):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", role, "-d", DBNAME]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    body = open(path).read() if path else sql
    return _sh(args, inp=body)


def _policy_on_all(cid, template):
    return _psql(cid, "\n".join(template.format(rel=r) for r in REL8))


def _apply_committed_policies(cid):
    return _psql(cid, None,
                 path=os.path.join(REPO, "ops", "sql",
                                   "create_shadow_audit_rls_policies.sql"))


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

        assert _sh(["docker", "exec", cid, "psql", "-U", "postgres", "-c",
                    f"CREATE DATABASE {DBNAME};"]).returncode == 0
        for m in MIGRATIONS:
            r = _psql(cid, None,
                      path=os.path.join(REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-400:]}"
        r = _psql(cid, _build_fixture_sql())
        assert r.returncode == 0, f"fixtures: {r.stderr[-600:]}"
        r = _psql(cid, None, variables={"audit_password": AUDIT_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql", "create_shadow_audit_reader.sql"))
        assert r.returncode == 0, f"role script: {r.stderr[-400:]}"
        assert _policy_on_all(cid, "ALTER TABLE {rel} ENABLE ROW LEVEL SECURITY;").returncode == 0
        assert _apply_committed_policies(cid).returncode == 0

        yield {
            "cid": cid,
            "audit_dsn": (f"postgresql://{AUDIT_ROLE}:{AUDIT_PW}"
                          f"@127.0.0.1:{host_port}/{DBNAME}"),
        }
    finally:
        _sh(["docker", "stop", cid])


async def _call_plan(dsn, params):
    from httpx import ASGITransport, AsyncClient
    from main import app
    from app.config import settings
    from app.deps import get_worker_token, close_db_pool

    saved = (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
             settings.AUDIT_EXPECTED_DB_ROLE)
    settings.AUDIT_ONLY_MODE = True
    settings.AUDIT_DATABASE_URL = dsn
    settings.AUDIT_EXPECTED_DB_ROLE = AUDIT_ROLE
    app.dependency_overrides[get_worker_token] = lambda: "t"
    await close_db_pool()
    try:
        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://test") as ac:
            resp = await ac.get(
                "/api/admin/shadow-cohort/maturation-plan", params=params)
        return resp.status_code, resp.json()
    finally:
        await close_db_pool()
        app.dependency_overrides.pop(get_worker_token, None)
        (settings.AUDIT_ONLY_MODE, settings.AUDIT_DATABASE_URL,
         settings.AUDIT_EXPECTED_DB_ROLE) = saved


def _plan(pg, exp, *, cohort_scope="campaign", **params):
    params = {"experiment_code": exp, "cohort_scope": cohort_scope, **params}
    return asyncio.run(_call_plan(pg["audit_dsn"], params))


class TestCleanCohort:
    def test_exact_manifest_count_and_safe(self, pg):
        status, body = _plan(pg, "exp_clean", limit=500)
        assert status == 200, body
        assert body["cohort_scope"] == "campaign"
        assert body["manifest_total"] == 8
        assert body["campaign_eligible_unmatured_count"] == 8
        assert body["experiment_eligible_unmatured_count"] == 8
        assert body["excluded_non_campaign_eligible_count"] == 0
        assert body["returned_count"] == 8
        assert body["planning"]["safe_to_execute"] is True, body["planning"]
        assert body["planning"]["blocking_reasons"] == []
        # matured + not_yet are excluded from the manifest.
        symbols = {e["symbol"] for e in body["eligible_manifest"]}
        assert "MATU" not in symbols and "NOTY" not in symbols
        assert body["not_yet_eligible_count"] == 1

    def test_benign_cross_campaign_duplicate_does_not_block(self, pg):
        _, body = _plan(pg, "exp_clean", limit=500)
        groups = body["duplicate_investigation"]["groups"]
        dup = [g for g in groups if g["symbol"] == "DUPX"]
        assert len(dup) == 1
        assert dup[0]["classification"] == "benign_cross_campaign_overlap"
        assert dup[0]["blocks_maturation"] is False

    def test_membership_all_verifiable(self, pg):
        _, body = _plan(pg, "exp_clean", limit=500)
        assert body["membership"]["campaign_membership_unverifiable_count"] == 0
        assert body["membership"]["campaign_membership_verifiable_count"] == 8
        assert set(body["membership"]["pairs_by_campaign"]) == {"camp-1", "camp-2"}

    def test_pagination_no_dupe_or_gap_and_hash_stable(self, pg):
        seen, hashes, offset = [], set(), 0
        while True:
            _, body = _plan(pg, "exp_clean", limit=3, offset=offset)
            seen.extend(e["pair_id"] for e in body["eligible_manifest"])
            hashes.add(body["manifest_hash"])
            if not body["has_more"]:
                break
            offset = body["next_offset"]
        assert len(seen) == 8 and len(set(seen)) == 8
        # one full-page read: same hash as the paginated reads.
        _, full = _plan(pg, "exp_clean", limit=500)
        hashes.add(full["manifest_hash"])
        assert len(hashes) == 1


class TestRetryCohort:
    def test_retry_and_terminal_separated_and_block(self, pg):
        status, body = _plan(pg, "exp_retry", limit=500)
        assert status == 200, body
        assert body["manifest_total"] == 1
        assert body["retryable_failure_count"] == 1
        assert body["terminal_failure_count"] == 1
        retry_ids = {e["pair_id"] for e in body["retry_plan"]["entries"]}
        elig_ids = {e["pair_id"] for e in body["eligible_manifest"]}
        assert not (retry_ids & elig_ids)
        codes = {e["current_error_code"] for e in body["retry_plan"]["entries"]}
        assert "forward_fetch_error" in codes
        assert "reference_revision_detected" in codes
        assert body["planning"]["safe_to_execute"] is False
        assert "terminal_failures_present" in body["planning"]["blocking_reasons"]


class TestUnsafeCohorts:
    def test_same_campaign_duplicate_blocks(self, pg):
        _, body = _plan(pg, "exp_samecamp", limit=500)
        dup = [g for g in body["duplicate_investigation"]["groups"]
               if g["symbol"] == "SAME"]
        assert dup and dup[0]["classification"] == "duplicate_within_same_campaign"
        assert body["planning"]["safe_to_execute"] is False
        assert "blocking_duplicate_group" in body["planning"]["blocking_reasons"]

    def test_strategy_version_mismatch_blocks(self, pg):
        _, body = _plan(pg, "exp_mismatch", limit=500)
        assert body["planning"]["safe_to_execute"] is False
        assert "non_uniform_strategy_version" in body["planning"]["blocking_reasons"]


class TestCampaignScopeSemantics:
    def test_manual_pair_excluded_in_campaign_scope(self, pg):
        _, body = _plan(pg, "exp_unverif", cohort_scope="campaign", limit=500)
        assert body["manifest_total"] == 0
        assert body["excluded_non_campaign_eligible_count"] == 1
        excl = body["excluded_non_campaign_evidence"]["records"]
        assert [r["symbol"] for r in excl] == ["UNV"]
        # excluded manual evidence does not itself block campaign maturation
        assert "membership_unverifiable" not in body["planning"]["blocking_reasons"]
        assert body["membership"]["campaign_membership_unverifiable_count"] == 0

    def test_manual_pair_retained_and_blocks_in_experiment_scope(self, pg):
        _, body = _plan(pg, "exp_unverif", cohort_scope="experiment", limit=500)
        assert body["manifest_total"] == 1
        assert body["excluded_non_campaign_eligible_count"] == 0
        assert body["planning"]["safe_to_execute"] is False
        assert "membership_unverifiable" in body["planning"]["blocking_reasons"]

    def test_scope_hashes_are_distinct(self, pg):
        _, camp = _plan(pg, "exp_clean", cohort_scope="campaign", limit=500)
        _, exp = _plan(pg, "exp_clean", cohort_scope="experiment", limit=500)
        assert camp["manifest_hash"] != exp["manifest_hash"]

    def test_multiple_campaign_links_keep_one_pair(self, pg):
        _, body = _plan(pg, "exp_multi", cohort_scope="campaign", limit=500)
        assert body["manifest_total"] == 1
        entry = body["eligible_manifest"][0]
        assert set(entry["campaign_ids"]) == {"camp-mc1", "camp-mc2"}
        assert body["membership"]["campaign_membership_verifiable_count"] == 1
        assert body["planning"]["safe_to_execute"] is True

    def test_conflicting_campaign_telemetry_blocks(self, pg):
        _, body = _plan(pg, "exp_conf", cohort_scope="campaign", limit=500)
        assert body["manifest_total"] == 0
        assert body["campaign_conflicting_eligible_count"] == 1
        assert body["planning"]["safe_to_execute"] is False
        assert "campaign_membership_conflict" in body["planning"]["blocking_reasons"]

    def test_cohort_scope_required(self, pg):
        status, body = asyncio.run(_call_plan(
            pg["audit_dsn"], {"experiment_code": "exp_clean"}))
        assert status == 422


class TestReadOnlyAndAccess:
    def test_access_is_audit_reader(self, pg):
        async def drive():
            conn = await asyncpg.connect(pg["audit_dsn"])
            try:
                return await conn.fetchval("SELECT current_user")
            finally:
                await conn.close()
        assert asyncio.run(drive()) == AUDIT_ROLE

    def test_no_write_occurred_row_counts_stable(self, pg):
        async def counts():
            conn = await asyncpg.connect(pg["audit_dsn"])
            try:
                pairs = await conn.fetchval("SELECT count(*) FROM strategy_shadow_pairs")
                outs = await conn.fetchval("SELECT count(*) FROM strategy_shadow_pair_outcomes")
                return pairs, outs
            finally:
                await conn.close()
        before = asyncio.run(counts())
        _plan(pg, "exp_clean", limit=500)
        _plan(pg, "exp_retry", limit=500)
        after = asyncio.run(counts())
        assert before == after  # planning never writes
