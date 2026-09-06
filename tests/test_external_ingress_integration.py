"""The AI Edge / TradingView path end to end, against a real PostgreSQL.

WHY A REAL DATABASE, AND WHY THE REAL HTTP ROUTE
------------------------------------------------
The unit suite (tests/test_external_gateway.py) proves the gateway's LOGIC
with a fake connection that reproduces the two UNIQUE constraints. It cannot
prove the thing that actually broke: the deployed revision issued
`ON CONFLICT (source)` against a table whose only unique index had become
`PRIMARY KEY (source, scope)` after migration 028. Every statement in
isolation was correct Python; the SQL was invalid against the live schema, and
no test that stubs the connection could ever have said so.

So this file runs the real FastAPI application, over real HTTP, as the real
least-privilege ingest role, against a schema built from the real migrations,
and asserts each boundary the P1 investigation had to establish by hand:

    public route -> authentication -> delivery row -> normalisation
                 -> external_signals -> session link

The application connects through `EXTERNAL_INGEST_DATABASE_URL` and the real
connection selector, so the role, the RLS policies and the pool settings under
test are the deployed ones rather than a superuser shortcut.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

asyncpg = pytest.importorskip("asyncpg")

PG_IMAGE = "postgres:16-alpine"
DBNAME = "ingressdb"
INGEST_ROLE = "smart_scanner_external_ingest"
INGEST_PW = "ingestpw_local_only"
TOKEN = "integration-ingress-token-local-only"
UNIVERSE_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"
CONTRACT = "smart_scanner_tradingview_signal.v1"

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Through 028: that is the migration which moved `catalyst_source_state` to a
# (source, scope) primary key, and reproducing it here is the whole point.
MIGRATIONS = [
    "001_initial_schema", "002_phase1_sma150_config",
    "003_phase2_signal_outcomes", "004_phase5_wyckoff_mtf_config",
    "005_massive_provider", "006_market_data_jobs",
    "007_scan_signal_provenance", "008_sma150_v3",
    "009_watch_outcome_coverage", "010_sma150_shadow_evaluations",
    "011_shadow_pair_outcomes", "012_wyckoff_mtf_v2",
    "013_wyckoff_v2_shadow_arms", "014_market_bars_4h",
    "015_history_warmup_run_items", "016_history_warmup_leases_and_universes",
    "017_prospective_campaign_registration", "018_durable_job_queue",
    "019_catalyst_events", "020_company_news", "021_sec_material_events",
    "022_external_signals", "023_external_discovery",
    "024_market_calendar_and_analyst", "025_discovery_reference_session",
    "026_research_symbols", "027_research_admission",
    "028_source_state_scope",
]


def _docker_ready():
    try:
        subprocess.run(["docker", "image", "inspect", PG_IMAGE],
                       capture_output=True, check=True, timeout=20)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ready(),
                                reason="docker/pg image unavailable")


def _sh(args, inp=None, t=180):
    return subprocess.run(args, input=inp, capture_output=True, text=True,
                          timeout=t)


def _psql(cid, sql, *, variables=None, path=None, db=DBNAME):
    args = ["docker", "exec", "-i", cid, "psql", "-v", "ON_ERROR_STOP=1",
            "-U", "postgres", "-d", db]
    for k, v in (variables or {}).items():
        args += ["-v", f"{k}={v}"]
    return _sh(args, inp=(open(path).read() if path else sql))


def _scalar(cid, sql):
    return _sh(["docker", "exec", "-i", cid, "psql", "-tA", "-U", "postgres",
                "-d", DBNAME, "-c", sql]).stdout.strip()


@pytest.fixture(scope="module")
def pg():
    cid = _sh(["docker", "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=postgres",
               "-P", PG_IMAGE]).stdout.strip()
    assert cid
    try:
        for _ in range(60):
            if _sh(["docker", "exec", cid, "pg_isready",
                    "-U", "postgres"]).returncode == 0:
                break
            time.sleep(1)
        port = int(_sh(["docker", "port", cid, "5432/tcp"]
                       ).stdout.splitlines()[0].rsplit(":", 1)[1])
        assert _psql(cid, f"CREATE DATABASE {DBNAME};",
                     db="postgres").returncode == 0
        for m in MIGRATIONS:
            r = _psql(cid, None, path=os.path.join(
                REPO, "app", "db", "migrations", f"{m}.sql"))
            assert r.returncode == 0, f"{m}: {r.stderr[-500:]}"

        # The least-privilege ingest role plus its RLS policies — the identity
        # the deployed app actually authenticates as.
        r = _psql(cid, None,
                  variables={"ingest_password": INGEST_PW, "db_name": DBNAME},
                  path=os.path.join(REPO, "ops", "sql",
                                    "create_smart_scanner_external_ingest.sql"))
        assert r.returncode == 0, f"ingest role: {r.stderr[-700:]}"

        # The frozen universe, so a symbol can be classified in-universe vs
        # research-only exactly as it is in production. Drafted first and
        # frozen afterwards: the universe relations carry an immutability
        # trigger, so symbols cannot be added to an already-frozen universe.
        #
        # Every seed statement is asserted. A silently failed seed here leaves
        # an EMPTY universe, which is indistinguishable from "this symbol is
        # not in it" — the test would then pass against a classification that
        # was never exercised.
        assert _psql(cid, "INSERT INTO history_warmup_universes(universe_code,"
                     "universe_version,universe_hash,config_hash,status,"
                     f"symbol_count) VALUES('{UNIVERSE_CODE}',1,'pending',"
                     "'cfg','draft',1);").returncode == 0
        uid = _scalar(cid, "SELECT id FROM history_warmup_universes WHERE "
                      f"universe_code='{UNIVERSE_CODE}';")
        assert uid
        assert _psql(cid, "INSERT INTO history_warmup_universe_symbols("
                     f"universe_id,symbol,ordinal) VALUES('{uid}','AAPL',0);"
                     ).returncode == 0
        assert _psql(cid, "UPDATE history_warmup_universes SET status='frozen',"
                     f"universe_hash='h', frozen_at=NOW() WHERE id='{uid}';"
                     ).returncode == 0
        assert _scalar(cid, "SELECT count(*) FROM "
                       "history_warmup_universe_symbols;") == "1"

        # A scan session for AAPL, so the session-link view has something
        # real to attach an arriving signal to. The view keys off
        # `telemetry->'campaign'->>'as_of_date'`, so that shape is what the
        # seed must produce — not an approximation of it.
        run_id, pair_id = str(uuid.uuid4()), str(uuid.uuid4())
        session_date = datetime.now(timezone.utc).date()
        telemetry = json.dumps(
            {"campaign": {"as_of_date": session_date.isoformat()}})
        assert _psql(cid,
                     "INSERT INTO strategy_shadow_runs(id,experiment_code,"
                     "experiment_version,status,telemetry) VALUES("
                     f"'{run_id}','wyckoff_v2_vs_baseline','wyckoff_v2.v1',"
                     f"'completed','{telemetry}'::jsonb);").returncode == 0
        assert _psql(cid,
                     "INSERT INTO strategy_shadow_pairs(id,origin_run_id,"
                     "experiment_code,experiment_version,symbol,snapshot_date,"
                     "market_data_as_of,frame_snapshot_version,frame_hash,"
                     "frame_bar_count,frame_first_date,frame_last_date,"
                     "frame_snapshot,pair_fingerprint,pair_fingerprint_version)"
                     f" VALUES('{pair_id}','{run_id}','wyckoff_v2_vs_baseline',"
                     f"'wyckoff_v2.v1','AAPL','{session_date}',NOW(),"
                     f"'daily_ohlcv_snapshot.v1','fh',1,'{session_date}',"
                     f"'{session_date}','[]'::jsonb,'pf',"
                     "'shadow_pair_fingerprint.v1');").returncode == 0
        assert _psql(cid,
                     "INSERT INTO strategy_shadow_run_pairs(run_id,pair_id,"
                     f"created_new_pair) VALUES('{run_id}','{pair_id}',TRUE);"
                     ).returncode == 0

        base = f"127.0.0.1:{port}/{DBNAME}?sslmode=disable"
        yield {
            "cid": cid,
            "su_dsn": f"postgresql://postgres:postgres@{base}",
            "ingest_dsn": f"postgresql://{INGEST_ROLE}:{INGEST_PW}@{base}",
            "session_date": session_date,
        }
    finally:
        _sh(["docker", "stop", cid])


@pytest.fixture(scope="module")
def client(pg):
    """The real app, in the real bounded mode, on the real ingest identity.

    Module-scoped and entered as a context manager on purpose. `TestClient`
    opens one event loop per portal, and the asyncpg pool belongs to the loop
    that created it — a per-test client would build a pool in one loop and use
    it from the next. Entering the context also runs the application lifespan,
    so the bounded-mode startup guards (mutual exclusion, no scheduler, a
    credential that must be present) are exercised rather than bypassed.
    """
    from fastapi.testclient import TestClient

    import app.deps as deps
    import app.external_ingress_observability as obs
    from app.config import settings
    from main import app as fastapi_app

    overrides = {
        "EXTERNAL_INGEST_ONLY_MODE": True,
        "EXTERNAL_INGEST_DATABASE_URL": pg["ingest_dsn"],
        "EXTERNAL_INGEST_TOKEN": TOKEN,
        "AUDIT_ONLY_MODE": False,
        "MAINTENANCE_ONLY_MODE": False,
        "HISTORY_WARMUP_ONLY_MODE": False,
        "PROSPECTIVE_CAMPAIGN_ONLY_MODE": False,
        "ENABLE_SCHEDULER": False,
        "JOB_WORKER_ENABLED": False,
    }
    previous = {k: getattr(settings, k, None) for k in overrides}
    for key, value in overrides.items():
        setattr(settings, key, value)
    original_ledger = obs.LEDGER
    obs.LEDGER = obs.RefusalLedger(flush_interval=0.0)
    deps._db_pool = None
    try:
        with TestClient(fastapi_app, raise_server_exceptions=False) as c:
            yield c
    finally:
        deps._db_pool = None
        obs.LEDGER = original_ledger
        for key, value in previous.items():
            setattr(settings, key, value)


def payload(**over):
    """The Open Long template from the runbook, verbatim in shape."""
    body = {
        "contract_version": CONTRACT,
        "source": "ai_edge",
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "timeframe": "240",
        "signal_type": "open_long",
        "direction": "bullish",
        "indicator": "lorentzian_classification",
        "alert_id": "aiedge-open-long",
        "source_timestamp": datetime.now(timezone.utc).isoformat(),
        "bar_time": datetime.now(timezone.utc).isoformat(),
        "price": "231.44",
    }
    body.update(over)
    return {k: v for k, v in body.items() if v is not None}


def post(client, body, *, token=TOKEN, as_text=None):
    url = "/api/external/signals" + (f"?token={token}" if token else "")
    return client.post(url, content=(as_text if as_text is not None
                                     else json.dumps(body)))


def rows(pg, sql):
    out = _scalar(pg["cid"], sql)
    return out


# --------------------------------------------------------------------------- #
# T01-T06  the public contract and the authentication boundary
# --------------------------------------------------------------------------- #
class TestPublicContractOverRealHTTP:
    def test_the_route_exists_and_only_accepts_post(self, client):
        assert client.get("/api/external/signals").status_code == 404
        assert post(client, payload(), token=None).status_code == 401

    def test_a_wrong_credential_is_refused_and_persists_no_delivery(
            self, client, pg):
        before = rows(pg, "SELECT count(*) FROM external_signal_deliveries;")
        assert post(client, payload(), token="wrong").status_code == 401
        assert rows(
            pg, "SELECT count(*) FROM external_signal_deliveries;") == before

    def test_a_wrong_credential_IS_recorded_in_the_refusal_ledger(
            self, client, pg):
        post(client, payload(), token="wrong-and-stale")
        detail = rows(pg, "SELECT detail FROM catalyst_source_state WHERE "
                      "source='external_ingress_refused' AND scope='product';")
        assert "unauthorized_bad_credential" in detail
        assert "wrong-and-stale" not in detail
        assert TOKEN not in detail

    def test_malformed_json_is_a_stable_code_not_a_parser_message(self, client):
        response = post(client, None, as_text="{not json")
        assert response.status_code == 400
        assert response.json()["reason"] == "malformed_json"

    def test_an_unknown_source_is_refused_after_authentication(self, client):
        response = post(client, payload(source="nasdaq_totalview"))
        assert response.status_code == 422
        assert response.json()["reason"] == "unknown_source"

    def test_a_valid_credential_reaches_the_gateway(self, client):
        # Wrong contract version: authentication SUCCEEDED and the content was
        # judged, which is exactly the boundary being proved.
        response = post(client, payload(contract_version="something.v0"))
        assert response.status_code == 422
        assert response.json()["reason"] == "unsupported_contract_version"


# --------------------------------------------------------------------------- #
# T07-T15, T19  the accepted path, all the way through
# --------------------------------------------------------------------------- #
class TestAcceptedPathEndToEnd:
    def test_open_long_becomes_a_normalised_in_universe_signal(
            self, client, pg):
        response = post(client, payload(alert_id=f"ol-{uuid.uuid4().hex[:8]}"))
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "accepted"
        assert body["symbol"] == "AAPL"
        assert body["symbol_scope"] == "scanner_universe"
        assert body["contract_version"] == CONTRACT

        row = rows(pg, "SELECT signal_type_normalized||'|'||"
                   "direction_normalized||'|'||timeframe_normalized||'|'||"
                   "symbol_scope FROM external_signals WHERE id='"
                   + body["signal_id"] + "';")
        assert row == "entry_signal|bullish|4h|scanner_universe"

    def test_open_short_normalises_to_a_bearish_entry(self, client, pg):
        response = post(client, payload(
            signal_type="open_short", direction="bearish",
            alert_id=f"os-{uuid.uuid4().hex[:8]}"))
        assert response.status_code == 200, response.text
        row = rows(pg, "SELECT signal_type_normalized||'|'||"
                   "direction_normalized FROM external_signals WHERE id='"
                   + response.json()["signal_id"] + "';")
        assert row == "entry_signal|bearish"

    def test_the_delivery_row_is_written_and_carries_the_raw_claim(
            self, client, pg):
        alert = f"raw-{uuid.uuid4().hex[:8]}"
        assert post(client, payload(alert_id=alert)).status_code == 200
        stored = rows(pg, "SELECT d.status||'|'||d.signal_count||'|'||"
                      "(d.raw_payload->>'alert_id') FROM "
                      "external_signal_deliveries d JOIN external_signals s "
                      f"ON s.delivery_id = d.id WHERE s.alert_id = '{alert}';")
        assert stored == f"accepted|1|{alert}"

    def test_an_exchange_ticker_id_is_reduced_to_the_symbol(self, client, pg):
        response = post(client, payload(
            symbol="NASDAQ:AAPL", alert_id=f"tid-{uuid.uuid4().hex[:8]}"))
        assert response.status_code == 200, response.text
        assert response.json()["symbol"] == "AAPL"

    def test_a_symbol_outside_the_frozen_universe_is_research_only(
            self, client):
        response = post(client, payload(
            symbol="TSLA", alert_id=f"oo-{uuid.uuid4().hex[:8]}"))
        assert response.status_code == 200, response.text
        assert response.json()["symbol_scope"] == "external_discovery"

    def test_an_unsubstituted_placeholder_is_refused(self, client):
        response = post(client, payload(symbol="{{ticker}}"))
        assert response.status_code == 422
        assert response.json()["reason"] in ("invalid_symbol",
                                             "unsubstituted_placeholder")

    def test_a_timestamp_outside_the_replay_window_is_refused(self, client):
        stale = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        response = post(client, payload(source_timestamp=stale))
        assert response.status_code == 422
        assert response.json()["reason"] == "timestamp_out_of_window"

    def test_a_future_timestamp_is_refused_the_same_way(self, client):
        ahead = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
        response = post(client, payload(source_timestamp=ahead))
        assert response.status_code == 422
        assert response.json()["reason"] == "timestamp_out_of_window"

    def test_the_identical_delivery_twice_is_one_signal(self, client, pg):
        body = payload(alert_id=f"dup-{uuid.uuid4().hex[:8]}")
        first = post(client, body)
        second = post(client, body)
        assert first.status_code == second.status_code == 200
        assert first.json()["status"] == "accepted"
        assert second.json()["status"] == "duplicate"
        count = rows(pg, "SELECT count(*) FROM external_signals WHERE "
                     f"alert_id = '{body['alert_id']}';")
        assert count == "1", "one alert must never become two signals"

    def test_a_repost_with_different_bytes_is_still_one_observation(
            self, client, pg):
        """The same firing re-sent inside a new envelope: a genuine second
        DELIVERY, and deliberately not a second signal."""
        alert = f"reenv-{uuid.uuid4().hex[:8]}"
        stamp = datetime.now(timezone.utc).isoformat()
        first = post(client, payload(alert_id=alert, source_timestamp=stamp))
        second = post(client, payload(alert_id=alert, source_timestamp=stamp,
                                      comment="resent"))
        assert first.json()["status"] == "accepted"
        assert second.json()["status"] == "duplicate"
        assert rows(pg, "SELECT count(*) FROM external_signals WHERE "
                    f"alert_id = '{alert}';") == "1"

    def test_the_signal_links_to_the_open_scan_session(self, client, pg):
        alert = f"link-{uuid.uuid4().hex[:8]}"
        assert post(client, payload(alert_id=alert)).status_code == 200
        linked = rows(pg, "SELECT count(*) FROM external_signal_session_links "
                      "l JOIN external_signals s ON s.id = l.signal_id "
                      f"WHERE s.alert_id = '{alert}';")
        assert int(linked) >= 1, (
            "an arriving signal must attach to a session that had not closed")

    def test_source_freshness_is_recorded_against_the_product_scope(
            self, client, pg):
        assert post(client, payload(
            alert_id=f"fresh-{uuid.uuid4().hex[:8]}")).status_code == 200
        state = rows(pg, "SELECT status||'|'||scope FROM catalyst_source_state "
                     "WHERE source='external_ai_edge';")
        assert state == "ok|product", (
            "the deployed build could not write this row at all: it said "
            "ON CONFLICT (source) against a (source, scope) primary key")

    def test_the_health_endpoint_reports_the_delivery_it_just_took(
            self, client):
        assert post(client, payload(
            alert_id=f"health-{uuid.uuid4().hex[:8]}")).status_code == 200
        body = client.get("/api/external/health").json()
        ai_edge = [s for s in body["sources"] if s["source"] == "ai_edge"][0]
        assert ai_edge["ever_delivered"] is True
        assert ai_edge["last_delivery_at"] is not None


# --------------------------------------------------------------------------- #
# T17  a database failure must be visible, never silent
# --------------------------------------------------------------------------- #
class TestFailureSurfaces:
    def test_a_signal_insert_failure_is_a_503_and_not_a_false_success(
            self, client, monkeypatch):
        import app.external_ingest as ei

        async def _boom(*a, **k):
            raise RuntimeError("insert refused")
        monkeypatch.setattr(ei, "insert_signal", _boom)
        response = post(client, payload(alert_id=f"fail-{uuid.uuid4().hex[:8]}"))
        assert response.status_code == 503
        assert response.json()["reason"] == "ingest_unavailable"

    def test_a_freshness_failure_does_NOT_lose_a_stored_signal(
            self, client, pg, monkeypatch):
        """The regression that motivated this file.

        The signal is already committed by the time freshness is written, so a
        failure there must degrade to a stale row — never to a 503 that tells a
        sender with no retry that a stored delivery failed."""
        import app.external_ingest as ei

        async def _boom(*a, **k):
            raise RuntimeError("no matching ON CONFLICT specification")
        monkeypatch.setattr(ei, "record_source_state", _boom)
        alert = f"state-{uuid.uuid4().hex[:8]}"
        response = post(client, payload(alert_id=alert))
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "accepted"
        assert rows(pg, "SELECT count(*) FROM external_signals WHERE "
                    f"alert_id = '{alert}';") == "1"


# --------------------------------------------------------------------------- #
# T20  the privilege boundary the ingress app runs behind
# --------------------------------------------------------------------------- #
class TestPrivilegeBoundary:
    def test_the_ingest_role_cannot_touch_a_scanner_relation(self, pg):
        import asyncio

        async def check():
            conn = await asyncpg.connect(pg["ingest_dsn"])
            try:
                for sql in ("SELECT count(*) FROM public.strategy_shadow_runs",
                            "SELECT count(*) FROM public.daily_bars",
                            "SELECT count(*) FROM public.patterns"):
                    with pytest.raises(asyncpg.PostgresError):
                        await conn.fetchval(sql)
            finally:
                await conn.close()
        asyncio.run(check())

    def test_the_ingest_role_cannot_delete_a_signal(self, pg):
        import asyncio

        async def check():
            conn = await asyncpg.connect(pg["ingest_dsn"])
            try:
                with pytest.raises(asyncpg.PostgresError):
                    await conn.execute(
                        "DELETE FROM public.external_signals")
            finally:
                await conn.close()
        asyncio.run(check())
