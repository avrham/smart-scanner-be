"""The ingress must be able to say what it turned away.

WHY THIS FILE EXISTS
--------------------
The account owner reported many real AI Edge alerts firing while
`external_signal_deliveries`, `external_signals` and the source-state row were
all empty. Every one of these was consistent with the evidence available at
the time:

    A. TradingView never sent anything
    B. TradingView sent to a path this deployment does not serve
    C. TradingView sent to the right path with the wrong credential

The system could not distinguish them, because a request refused before
parsing was written nowhere and Fly retains logs for minutes. These tests are
the guard on the mechanism that closes that gap, and on the two rules it must
not break while doing so: the WIRE RESPONSE stays undifferentiated, and an
anonymous flood still cannot grow a table.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import app.external_ingest as ei
import app.external_ingress_observability as obs
import app.utils.logging as applog
from app.config import settings
from app.deps import get_db
from main import app as fastapi_app

UTC = timezone.utc
NOW = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
TOKEN = "ingress-token-for-tests-only"


# --------------------------------------------------------------------------- #
# a connection that behaves like the real one for the two statements we use
# --------------------------------------------------------------------------- #
class LedgerConn:
    """Enough of asyncpg to exercise the ledger, and no more.

    Reproduces the ONE property the ledger depends on: the upsert accumulates
    `events_upserted` rather than overwriting it, keyed by (source, scope).
    A stub that overwrote would make a running total look like a per-flush
    count and these tests would pass while the diagnostic lied.
    """

    def __init__(self, *, fail_write=False, fail_read=False):
        self.rows = {}
        self.fail_write = fail_write
        self.fail_read = fail_read
        self.writes = 0

    async def execute(self, sql, *args):
        if "catalyst_source_state" in sql:
            if self.fail_write:
                raise RuntimeError("write refused")
            self.writes += 1
            key = (args[0], args[4])
            row = self.rows.setdefault(
                key, {"events_upserted": 0, "last_refresh_at": None,
                      "detail": None})
            row["events_upserted"] += args[2]
            row["last_refresh_at"] = args[1]
            row["detail"] = args[3]
        return "OK"

    async def fetch(self, sql, *args):
        if self.fail_read:
            raise RuntimeError("read refused")
        if "catalyst_source_state" in sql and "LIKE" in sql:
            prefix = args[0].replace("\\_", "_").rstrip("%")
            return [dict(row, source=key[0])
                    for key, row in self.rows.items()
                    if key[0].startswith(prefix) and key[1] == args[1]]
        return []


@pytest.fixture
def ledger():
    return obs.RefusalLedger(flush_interval=0.0)


# --------------------------------------------------------------------------- #
# the ledger itself
# --------------------------------------------------------------------------- #
class TestRefusalLedger:
    def test_counts_by_reason_and_remembers_the_last_one(self, ledger):
        ledger.record(obs.REASON_NO_CREDENTIAL, transport=obs.TRANSPORT_NONE,
                      now=NOW)
        ledger.record(obs.REASON_NO_CREDENTIAL, transport=obs.TRANSPORT_NONE,
                      now=NOW)
        ledger.record(obs.REASON_BAD_CREDENTIAL, transport=obs.TRANSPORT_QUERY,
                      fingerprint="deadbeef", now=NOW + timedelta(seconds=1))
        snap = ledger.snapshot()
        assert snap["since_boot"] == {obs.REASON_NO_CREDENTIAL: 2,
                                      obs.REASON_BAD_CREDENTIAL: 1}
        assert ledger.pending_by_reason == {obs.REASON_NO_CREDENTIAL: 2,
                                            obs.REASON_BAD_CREDENTIAL: 1}
        assert snap["since_boot_total"] == 3
        assert snap["last_reason"] == obs.REASON_BAD_CREDENTIAL
        assert snap["last_credential_transport"] == obs.TRANSPORT_QUERY
        assert snap["last_supplied_credential_fingerprint"] == "deadbeef"

    def test_reason_map_cannot_grow_without_bound(self, ledger):
        for i in range(obs.MAX_TRACKED_REASONS + 25):
            ledger.record(f"reason_{i}")
        # The cap bounds the DURABLE ROW COUNT too, now that each reason owns
        # a row: an attacker-influenced reason string must not be able to
        # append to `catalyst_source_state` indefinitely.
        assert len(ledger.counts) <= obs.MAX_TRACKED_REASONS + 1
        assert len(ledger.pending_by_reason) <= obs.MAX_TRACKED_REASONS + 1
        assert ledger.counts["other"] >= 25
        assert ledger.total == obs.MAX_TRACKED_REASONS + 25

    def test_detail_is_bounded_and_carries_no_credential(self, ledger):
        ledger.record(obs.REASON_BAD_CREDENTIAL,
                      transport=obs.TRANSPORT_QUERY,
                      fingerprint=obs.credential_fingerprint("super-secret"),
                      now=NOW)
        detail = ledger.detail(obs.REASON_BAD_CREDENTIAL)
        assert len(detail) <= obs.MAX_DETAIL_CHARS
        assert "super-secret" not in detail
        assert obs.credential_fingerprint("super-secret") in detail

    def test_fingerprint_is_truncated_one_way_and_stable(self):
        fp = obs.credential_fingerprint("abc")
        assert fp == obs.credential_fingerprint("abc")
        assert fp != obs.credential_fingerprint("abd")
        assert len(fp) == 8
        assert obs.credential_fingerprint("") is None
        assert obs.credential_fingerprint(None) is None

    def test_throttle_holds_writes_between_flushes(self):
        held = obs.RefusalLedger(flush_interval=60.0)
        held.record("unauthorized")
        assert held.due(monotonic=0.0) is True
        held.take_pending()
        held.mark_flushed(monotonic=0.0)
        held.record("unauthorized")
        assert held.due(monotonic=10.0) is False
        assert held.due(monotonic=61.0) is True

    def test_nothing_pending_is_never_due(self):
        idle = obs.RefusalLedger(flush_interval=0.0)
        assert idle.due() is False


class TestFlush:
    """Driven with `asyncio.run`: this suite has no pytest-asyncio and every
    other async test in the repository is driven the same way."""

    def test_each_reason_accumulates_in_its_own_row(self, ledger):
        conn = LedgerConn()
        ledger.record(obs.REASON_NO_CREDENTIAL, now=NOW)
        assert asyncio.run(
            obs.flush_refusals(conn, ledger=ledger, now=NOW)) == 1
        ledger.record(obs.REASON_NO_CREDENTIAL, now=NOW)
        ledger.record(obs.REASON_ROUTE_NOT_FOUND, now=NOW)
        assert asyncio.run(
            obs.flush_refusals(conn, ledger=ledger, now=NOW)) == 2

        assert len(conn.rows) == 2, "one row per reason, never one per request"
        assert conn.rows[(obs.refusal_state_source(obs.REASON_NO_CREDENTIAL),
                          "product")]["events_upserted"] == 2
        assert conn.rows[(obs.refusal_state_source(obs.REASON_ROUTE_NOT_FOUND),
                          "product")]["events_upserted"] == 1
        assert ledger.pending == 0

    def test_a_second_process_cannot_erase_the_first_ones_reason(self):
        """The bug the live two-machine deployment exposed within a minute.

        Machine B flushing one reason must not overwrite what machine A
        recorded about another — which is exactly what a single shared
        `detail` field did, while the total stayed correct and useless."""
        conn = LedgerConn()
        machine_a = obs.RefusalLedger(flush_interval=0.0)
        machine_b = obs.RefusalLedger(flush_interval=0.0)
        machine_a.record(obs.REASON_BAD_CREDENTIAL, now=NOW)
        machine_a.record(obs.REASON_BAD_CREDENTIAL, now=NOW)
        machine_b.record(obs.REASON_ROUTE_NOT_FOUND, now=NOW)
        asyncio.run(obs.flush_refusals(conn, ledger=machine_a, now=NOW))
        asyncio.run(obs.flush_refusals(conn, ledger=machine_b, now=NOW))

        read = asyncio.run(obs.read_refusals(conn))
        assert read["by_reason"] == {obs.REASON_BAD_CREDENTIAL: 2,
                                     obs.REASON_ROUTE_NOT_FOUND: 1}
        assert read["recorded_total"] == 3

    def test_a_failed_flush_puts_the_counts_back(self, ledger):
        conn = LedgerConn(fail_write=True)
        ledger.record("unauthorized")
        assert asyncio.run(obs.flush_refusals(conn, ledger=ledger)) == 0
        # Restored, so the evidence is not lost — the next flush retries it.
        assert ledger.pending == 1

    def test_read_degrades_to_unavailable_rather_than_raising(self):
        assert asyncio.run(
            obs.read_refusals(LedgerConn(fail_read=True))) == {
                "available": False}

    def test_read_distinguishes_never_refused_from_unreadable(self):
        read = asyncio.run(obs.read_refusals(LedgerConn()))
        assert read["available"] is True
        assert read["by_reason"] == {}
        assert read["recorded_total"] == 0


# --------------------------------------------------------------------------- #
# the credential in the URL must never reach a log line
# --------------------------------------------------------------------------- #
class TestCredentialRedaction:
    """TradingView cannot set a custom header, so `?token=` is the only
    mechanism — which means uvicorn's access line would otherwise print the
    live ingress credential on every genuine delivery."""

    def test_uvicorn_style_request_line_is_redacted(self):
        line = 'POST /api/external/signals?token=s3cr3t HTTP/1.1'
        out = applog.redact_query_credentials(line)
        assert "s3cr3t" not in out
        assert "token=[redacted]" in out
        assert "/api/external/signals" in out, "the path must stay readable"

    @pytest.mark.parametrize("param", ["token", "TOKEN", "api_key", "secret",
                                       "signature", "access_token"])
    def test_every_credential_parameter_name_is_covered(self, param):
        assert "zzz" not in applog.redact_query_credentials(f"/x?{param}=zzz")

    def test_a_parameter_that_merely_starts_with_token_is_left_alone(self):
        assert applog.redact_query_credentials(
            "/x?tokenizer=safe") == "/x?tokenizer=safe"

    def test_the_filter_rewrites_the_record_uvicorn_actually_emits(self):
        record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d',
            ("1.2.3.4:5", "POST", "/api/external/signals?token=s3cr3t", "1.1",
             401),
            None)
        assert obs is not None
        assert applog.CredentialQueryRedactor().filter(record) is True
        assert "s3cr3t" not in record.getMessage()
        assert "token=[redacted]" in record.getMessage()

    def test_a_record_with_no_credential_is_untouched(self):
        record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1,
            '%s - "%s %s HTTP/%s" %d',
            ("1.2.3.4:5", "GET", "/version", "1.1", 200), None)
        before = record.getMessage()
        applog.CredentialQueryRedactor().filter(record)
        assert record.getMessage() == before


#: The Open Long template from docs/external-intelligence-hub-runbook.md §3.1,
#: verbatim in field order. The order is the point: `source` sits behind the
#: contract version, which is what put `"ai_edge"` past the old peek window.
RUNBOOK_OPEN_LONG = (
    '{"contract_version":"smart_scanner_tradingview_signal.v1",'
    '"source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}",'
    '"timeframe":"{{interval}}","signal_type":"open_long",'
    '"direction":"bullish","indicator":"lorentzian_classification",'
    '"alert_id":"aiedge-open-long","source_timestamp":"{{timenow}}",'
    '"bar_time":"{{time}}","price":"{{close}}"}')


class TestTheRateLimitBucketMatchesTheDocumentedTemplate:
    """The limiter picks a bucket from a prefix of the body, before parsing.

    That is the right shape — a cheap check must precede an expensive one — but
    the window has to be wide enough for the payload the runbook actually tells
    people to paste. At 64 bytes it was not, and every AI Edge alert was
    counted against the shared `tradingview` bucket instead of its own.
    """

    def test_the_documented_ai_edge_template_selects_the_ai_edge_bucket(self):
        import app.external_signals as es
        from app.routers.external import SOURCE_HINT_PEEK_BYTES
        peek = RUNBOOK_OPEN_LONG.encode()[:SOURCE_HINT_PEEK_BYTES].decode(
            "utf-8", "replace")
        assert f'"{es.SOURCE_AI_EDGE}"' in peek

    def test_the_old_window_provably_missed_it(self):
        """Kept as the regression's own evidence: 64 bytes stops three bytes
        short of `"ai_edge"` in the documented template."""
        assert '"ai_edge"' not in RUNBOOK_OPEN_LONG.encode()[:64].decode()
        assert RUNBOOK_OPEN_LONG.index('"ai_edge"') == 67

    def test_the_peek_stays_a_bounded_prefix_of_a_bounded_body(self):
        from app.external_adapters import MAX_PAYLOAD_BYTES
        from app.routers.external import SOURCE_HINT_PEEK_BYTES
        assert SOURCE_HINT_PEEK_BYTES < MAX_PAYLOAD_BYTES

    def test_a_generic_tradingview_alert_still_selects_the_shared_bucket(self):
        import app.external_signals as es
        from app.routers.external import SOURCE_HINT_PEEK_BYTES
        body = ('{"contract_version":"smart_scanner_tradingview_signal.v1",'
                '"source":"tradingview","symbol":"AAPL"}')
        peek = body.encode()[:SOURCE_HINT_PEEK_BYTES].decode()
        assert f'"{es.SOURCE_AI_EDGE}"' not in peek


class TestTheLedgerRowsCannotBeMistakenForASource:
    """The refusal rows live in the SHARED freshness table, so the guarantee
    that they cannot contaminate a product verdict has to be mechanical.

    Every product read of `catalyst_source_state` looks a row up by an EXPLICIT
    key derived from a registry source (`source_state_key(...)`), never by
    scanning the `external_` prefix. Names that no registry source can produce
    are therefore inert by construction — and this asserts they stay that way.
    """

    ALL_REASONS = (obs.REASON_NO_CREDENTIAL, obs.REASON_BAD_CREDENTIAL,
                   obs.REASON_ROUTE_NOT_FOUND, "rate_limited",
                   "payload_too_large", "other")

    @pytest.mark.parametrize("reason", ALL_REASONS)
    def test_no_ledger_row_is_a_registry_sources_freshness_row(self, reason):
        import app.external_signals as es
        row = obs.refusal_state_source(reason)
        assert row not in {es.source_state_key(src)
                           for src in es.WEBHOOK_SOURCES}
        assert row not in es.WEBHOOK_SOURCES

    @pytest.mark.parametrize("reason", ALL_REASONS)
    def test_every_ledger_row_stays_inside_the_roles_rls_prefix(self, reason):
        # smart_scanner_external_ingest may only write rows matching
        # `external\_%` (ops/sql/create_smart_scanner_external_ingest.sql). A
        # rename outside that prefix would make every flush fail silently.
        assert obs.refusal_state_source(reason).startswith("external_")

    @pytest.mark.parametrize("reason", ALL_REASONS)
    def test_the_row_name_round_trips_to_the_reason_it_came_from(self, reason):
        # `read_refusals` recovers the reason by stripping the prefix, so the
        # two must agree — otherwise health reports reasons nobody recorded.
        row = obs.refusal_state_source(reason)
        assert row[len(obs.REFUSAL_STATE_PREFIX) + 1:] == reason

    def test_no_ledger_row_is_ever_listed_as_a_source_by_health(self, ingress):
        client, _ = ingress
        client.post("/api/external/signals?token=nope", content=_body())
        client.post("/api/external/signal", content=_body())
        listed = [s["source"] for s in
                  client.get("/api/external/health").json()["sources"]]
        assert not [s for s in listed
                    if s.startswith(obs.REFUSAL_STATE_PREFIX)]


class TestUrlSafeCredential:
    """A credential that cannot survive a query string is an invisible outage:
    curl with a header keeps working while TradingView — which cannot send a
    header at all — silently fails every delivery."""

    @pytest.mark.parametrize("token", [
        "AbC123", "a-b_c.d~e", "0" * 48,
    ])
    def test_a_url_safe_credential_is_reported_safe(self, token):
        assert obs.token_is_url_safe(token) is True

    @pytest.mark.parametrize("token,why", [
        ("abc+def", "+ decodes to a space"),
        ("abc&def", "& ends the parameter"),
        ("abc/def", "/ is ambiguous"),
        ("abc=def", "= is ambiguous"),
        ("abc def", "a literal space"),
        ("abc#def", "# starts a fragment"),
        ("abc%2Fdef", "% starts an escape"),
    ])
    def test_a_url_hostile_credential_is_reported_unsafe(self, token, why):
        assert obs.token_is_url_safe(token) is False, why

    def test_an_absent_credential_is_not_safe(self):
        assert obs.token_is_url_safe("") is False
        assert obs.token_is_url_safe(None) is False


# --------------------------------------------------------------------------- #
# the HTTP surface: the route contract and what a refusal records
# --------------------------------------------------------------------------- #
@pytest.fixture
def ingress(monkeypatch):
    """The real FastAPI app in external-ingest-only mode, with a fake DB."""
    conn = LedgerConn()
    monkeypatch.setattr(settings, "EXTERNAL_INGEST_ONLY_MODE", True,
                        raising=False)
    monkeypatch.setattr(settings, "EXTERNAL_INGEST_TOKEN", TOKEN,
                        raising=False)
    monkeypatch.setattr(obs, "LEDGER", obs.RefusalLedger(flush_interval=0.0))
    fastapi_app.dependency_overrides[get_db] = lambda: conn
    try:
        yield TestClient(fastapi_app, raise_server_exceptions=False), conn
    finally:
        fastapi_app.dependency_overrides.pop(get_db, None)


def _body(**over):
    payload = {
        "contract_version": "smart_scanner_tradingview_signal.v1",
        "source": "ai_edge",
        "symbol": "AAPL",
        "timeframe": "240",
        "signal_type": "open_long",
        "direction": "bullish",
        "indicator": "lorentzian_classification",
    }
    payload.update(over)
    return json.dumps(payload)


class TestPublicRouteContract:
    def test_get_on_the_ingress_is_not_a_route(self, ingress):
        client, _ = ingress
        assert client.get("/api/external/signals").status_code == 404

    def test_post_is_the_only_write(self, ingress):
        client, _ = ingress
        # Unauthorized, not 404 — the route exists and the gate let it through
        # to its own authentication.
        assert client.post("/api/external/signals",
                           content="{}").status_code == 401

    def test_a_trailing_slash_is_not_the_ingress(self, ingress):
        """The exact string TradingView is configured with matters, and a
        trailing slash is a real misconfiguration. It must 404 rather than
        silently redirect a POST."""
        client, _ = ingress
        assert client.post("/api/external/signals/", content="{}",
                           follow_redirects=False).status_code in (307, 404)

    def test_the_scanner_surface_is_unreachable_from_the_ingress_app(
            self, ingress):
        client, _ = ingress
        assert client.get("/api/scanner/symbol?symbol=AAPL").status_code == 404
        assert client.get("/docs").status_code == 404


class TestRefusalsAreRecorded:
    def test_no_credential_is_classified_separately_from_a_wrong_one(
            self, ingress):
        client, _ = ingress
        client.post("/api/external/signals", content=_body())
        client.post("/api/external/signals?token=not-the-token",
                    content=_body())
        counts = obs.LEDGER.snapshot()["since_boot"]
        assert counts[obs.REASON_NO_CREDENTIAL] == 1
        assert counts[obs.REASON_BAD_CREDENTIAL] == 1

    def test_the_wire_response_stays_undifferentiated(self, ingress):
        client, _ = ingress
        without = client.post("/api/external/signals", content=_body())
        wrong = client.post("/api/external/signals?token=nope",
                            content=_body())
        assert without.status_code == wrong.status_code == 401
        assert without.json() == wrong.json() == {"status": "rejected",
                                                  "reason": "unauthorized"}

    def test_the_transport_the_caller_used_is_recorded(self, ingress):
        client, _ = ingress
        client.post("/api/external/signals",
                    headers={"X-Smart-Scanner-Token": "nope"},
                    content=_body())
        assert obs.LEDGER.snapshot()["last_credential_transport"] == \
            obs.TRANSPORT_HEADER
        client.post("/api/external/signals?token=nope", content=_body())
        assert obs.LEDGER.snapshot()["last_credential_transport"] == \
            obs.TRANSPORT_QUERY

    def test_a_wrong_credential_is_fingerprinted_but_never_stored(
            self, ingress):
        client, conn = ingress
        client.post("/api/external/signals?token=owners-stale-token",
                    content=_body())
        snap = obs.LEDGER.snapshot()
        assert snap["last_supplied_credential_fingerprint"] == \
            obs.credential_fingerprint("owners-stale-token")
        blob = json.dumps({str(k): v for k, v in conn.rows.items()},
                          default=str)
        assert "owners-stale-token" not in blob
        assert TOKEN not in blob, "the EXPECTED credential must never appear"

    def test_a_misdirected_post_is_counted(self, ingress):
        client, _ = ingress
        client.post("/api/external/signal", content=_body())      # typo
        assert obs.LEDGER.snapshot()["since_boot"][
            obs.REASON_ROUTE_NOT_FOUND] == 1

    def test_a_get_probe_is_not_counted(self, ingress):
        """The internet scans every public host. Counting that would bury the
        one thing this counter exists to surface."""
        client, _ = ingress
        client.get("/api/admin/anything")
        client.get("/wp-login.php")
        assert obs.REASON_ROUTE_NOT_FOUND not in \
            obs.LEDGER.snapshot()["since_boot"]

    def test_a_flood_writes_one_row_regardless_of_volume(self, ingress):
        client, conn = ingress
        for _ in range(40):
            client.post("/api/external/signals", content=_body())
        assert len(conn.rows) <= 1


class TestHealthReportsRefusals:
    def test_health_publishes_what_was_turned_away(self, ingress):
        client, _ = ingress
        client.post("/api/external/signals?token=nope", content=_body())
        body = client.get("/api/external/health").json()
        refusals = body["ingress_refusals"]
        assert refusals["available"] is True
        assert refusals["recorded_total"] >= 1
        assert refusals["this_process"]["last_reason"] == \
            obs.REASON_BAD_CREDENTIAL

    def test_health_states_whether_the_credential_survives_a_url(
            self, ingress, monkeypatch):
        client, _ = ingress
        assert client.get("/api/external/health").json()[
            "ingress_token_url_safe"] is True
        monkeypatch.setattr(settings, "EXTERNAL_INGEST_TOKEN", "has+plus",
                            raising=False)
        body = client.get("/api/external/health").json()
        assert body["ingress_token_url_safe"] is False
        assert "has+plus" not in json.dumps(body)

    def test_health_never_publishes_the_expected_credential(self, ingress):
        client, _ = ingress
        client.post("/api/external/signals?token=nope", content=_body())
        body = client.get("/api/external/health").text
        assert TOKEN not in body
        assert "nope" not in body
        assert json.loads(body)["ingress_token_configured"] is True

    def test_health_still_answers_when_the_ledger_row_is_unreadable(
            self, monkeypatch):
        """An operator reads this endpoint precisely when something is already
        wrong, so an unreadable ledger must degrade to `available: false`
        rather than take the diagnostic itself down."""
        conn = LedgerConn(fail_read=True)
        monkeypatch.setattr(settings, "EXTERNAL_INGEST_ONLY_MODE", True,
                            raising=False)
        monkeypatch.setattr(settings, "EXTERNAL_INGEST_TOKEN", TOKEN,
                            raising=False)
        monkeypatch.setattr(obs, "LEDGER",
                            obs.RefusalLedger(flush_interval=0.0))
        fastapi_app.dependency_overrides[get_db] = lambda: conn
        try:
            client = TestClient(fastapi_app, raise_server_exceptions=False)
            response = client.get("/api/external/health")
            assert response.status_code == 503
            assert response.json()["database_ready"] is False
        finally:
            fastapi_app.dependency_overrides.pop(get_db, None)
