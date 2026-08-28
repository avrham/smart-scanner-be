"""The webhook gateway: what it accepts, what it refuses, and what it leaks.

This is the only internet-facing write path in the repository, so these tests
are the security boundary rather than a description of it. They run with no
real database: `FakeConn` reproduces exactly the two UNIQUE constraints the
gateway relies on (delivery replay, signal idempotency), because those
constraints ARE the deduplication logic and a test that stubbed them away
would prove nothing.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

import app.external_adapters as ad
import app.external_ingest as ei
import app.external_signals as ex

UTC = timezone.utc
NOW = datetime(2026, 8, 25, 17, 0, tzinfo=UTC)

UNIVERSE = ["AAPL", "MSFT", "NVDA"]


class FakeConn:
    """An in-memory stand-in that enforces the real UNIQUE constraints.

    Deliberately keyed the same way PostgreSQL is — (source, body_fingerprint)
    for deliveries and (source, idempotency_key) for signals — so a change that
    broke deduplication in production would break these tests too.
    """

    def __init__(self, universe=UNIVERSE, universe_fails=False):
        self.universe = list(universe)
        self.universe_fails = universe_fails
        self.deliveries = {}
        self.signals = {}
        self.source_state = []
        self.rows = []

    async def fetch(self, sql, *args):
        if "history_warmup_universe_symbols" in sql:
            if self.universe_fails:
                raise RuntimeError("universe unavailable")
            return [{"symbol": s} for s in self.universe]
        return []

    async def fetchrow(self, sql, *args):
        if "INSERT INTO public.external_signal_deliveries" in sql:
            source, fingerprint = args[0], args[3]
            key = (source, fingerprint)
            if key in self.deliveries:
                return None                      # ON CONFLICT DO NOTHING
            row = {"id": f"delivery-{len(self.deliveries)}",
                   "status": args[5], "reason": args[6],
                   "raw_payload": args[7], "bytes": args[4]}
            self.deliveries[key] = row
            self.rows.append(row)
            return row
        if "INSERT INTO public.external_signals" in sql:
            source, idem = args[0], args[24]
            key = (source, idem)
            if key in self.signals:
                return None                      # ON CONFLICT DO NOTHING
            row = {"id": f"signal-{len(self.signals)}", "symbol": args[3],
                   "scope": args[4], "direction": args[14]}
            self.signals[key] = row
            return row
        return None

    async def execute(self, sql, *args):
        if "catalyst_source_state" in sql:
            self.source_state.append({"source": args[0], "status": args[1]})
        return "OK"


def payload(**overrides):
    body = {
        "contract_version": ex.TRADINGVIEW_CONTRACT_VERSION,
        "source": "ai_edge",
        "symbol": "AAPL",
        "timeframe": "240",
        "signal_type": "classification",
        "direction": "bullish",
        "source_timestamp": NOW.isoformat(),
    }
    body.update(overrides)
    return body


def body_of(**overrides):
    return json.dumps(payload(**overrides)).encode()


def deliver(conn, raw=None, *, now=NOW, **overrides):
    """Drive one delivery to completion.

    Synchronous on purpose: this suite has no pytest-asyncio, and every other
    async test in this repository is driven with `asyncio.run`. Keeping to that
    convention means these tests need no plugin the project does not already
    depend on.
    """
    return asyncio.run(ei.ingest_delivery(
        conn, raw if raw is not None else body_of(**overrides),
        received_at=now))


# --------------------------------------------------------------------------- #
# authentication
# --------------------------------------------------------------------------- #

class TestTokenVerification:
    def test_a_correct_token_passes(self):
        assert ei.verify_ingress_token("s3cret", "s3cret")

    def test_a_wrong_token_fails(self):
        assert not ei.verify_ingress_token("wrong", "s3cret")

    def test_an_unconfigured_server_rejects_everything(self):
        # Fail CLOSED. A deployment that forgot the secret must reject every
        # caller, never accept every caller.
        assert not ei.verify_ingress_token("anything", "")
        assert not ei.verify_ingress_token("anything", None)

    def test_a_missing_token_fails(self):
        assert not ei.verify_ingress_token("", "s3cret")
        assert not ei.verify_ingress_token(None, "s3cret")


# --------------------------------------------------------------------------- #
# schema validation
# --------------------------------------------------------------------------- #

class TestPayloadValidation:
    def test_the_contract_version_is_required(self):
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize({"symbol": "AAPL", "signal_type": "alert"},
                         received_at=NOW)
        assert exc.value.reason == "unsupported_contract_version"

    def test_an_old_contract_version_is_named_in_the_refusal(self):
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(contract_version="something.v0"),
                         received_at=NOW)
        # Both strings are public contract identifiers, so naming the expected
        # one leaks nothing and saves a user from guessing.
        assert ex.TRADINGVIEW_CONTRACT_VERSION in exc.value.detail

    def test_an_unknown_source_is_refused(self):
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(source="definitely_not_a_source"),
                         received_at=NOW)
        assert exc.value.reason == "unknown_source"

    def test_a_missing_signal_type_is_refused(self):
        body = payload()
        del body["signal_type"]
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(body, received_at=NOW)
        assert exc.value.reason == "missing_signal_type"

    @pytest.mark.parametrize("bad", ["", "aapl inc", "TOO-LONG-SYMBOL-NAME-X",
                                     "1AAPL", "../etc/passwd"])
    def test_a_malformed_symbol_is_refused(self, bad):
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(symbol=bad), received_at=NOW)
        assert exc.value.reason == "invalid_symbol"

    def test_an_unsubstituted_placeholder_is_refused_with_a_clear_code(self):
        # The classic user error: pasting the template where TradingView does
        # not expand it. Storing "{{ticker}}" as a symbol would be silently
        # useless, which is the worst possible outcome.
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(symbol="{{ticker}}"), received_at=NOW)
        assert exc.value.reason in ("invalid_symbol", "unsubstituted_placeholder")

    def test_an_exchange_prefixed_ticker_is_accepted(self):
        out = ad.normalize(payload(symbol="NASDAQ:AAPL"), received_at=NOW)
        assert out["symbol"] == "AAPL"


class TestTimestampValidation:
    def test_a_timestamp_far_in_the_past_is_refused(self):
        stale = (NOW - timedelta(hours=4)).isoformat()
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(source_timestamp=stale), received_at=NOW)
        assert exc.value.reason == "timestamp_out_of_window"

    def test_a_timestamp_far_in_the_future_is_refused(self):
        ahead = (NOW + timedelta(hours=4)).isoformat()
        with pytest.raises(ad.PayloadRejected) as exc:
            ad.normalize(payload(source_timestamp=ahead), received_at=NOW)
        assert exc.value.reason == "timestamp_out_of_window"

    def test_small_skew_is_accepted_and_recorded_rather_than_hidden(self):
        skewed = (NOW - timedelta(minutes=2)).isoformat()
        out = ad.normalize(payload(source_timestamp=skewed), received_at=NOW)
        assert out["clock_skew_seconds"] == -120

    def test_a_missing_timestamp_is_allowed(self):
        body = payload()
        del body["source_timestamp"]
        out = ad.normalize(body, received_at=NOW)
        assert out["observed_at"] is None
        # The gate still works: it never depended on the source's clock.
        assert out["effective_at"] == NOW


class TestSizeAndShape:
    def test_an_oversized_body_is_refused_before_it_is_parsed(self):
        huge = b'{"x":"' + b"a" * (ad.MAX_PAYLOAD_BYTES + 100) + b'"}'
        with pytest.raises(ei.IngressRejected) as exc:
            ei.decode_body(huge)
        assert exc.value.reason == "payload_too_large"
        assert exc.value.status_code == 413

    def test_malformed_json_gets_a_stable_code_not_a_parser_message(self):
        with pytest.raises(ei.IngressRejected) as exc:
            ei.decode_body(b"{not json")
        assert exc.value.reason == "malformed_json"

    def test_a_json_array_is_not_a_delivery(self):
        with pytest.raises(ei.IngressRejected) as exc:
            ei.decode_body(b'[{"a":1}]')
        assert exc.value.reason == "payload_not_object"

    def test_metadata_is_bounded(self):
        big = {f"k{i}": "v" * 200 for i in range(50)}
        out = ad.bound_metadata(big)
        assert out.get("_metadata_error") == "too_large"

    def test_secret_shaped_keys_are_redacted_before_persistence(self):
        # The contract says secrets never travel in the body. This is the belt
        # to that braces: a user who pastes a token into their alert message
        # once must not leave it in our database forever.
        cleaned = ad.redact({"token": "abc123", "api_key": "k", "symbol": "AAPL",
                             "nested": {"password": "p", "ok": 1}})
        assert cleaned["token"] == ad.REDACTED
        assert cleaned["api_key"] == ad.REDACTED
        assert cleaned["nested"]["password"] == ad.REDACTED
        assert cleaned["symbol"] == "AAPL"
        assert cleaned["nested"]["ok"] == 1


# --------------------------------------------------------------------------- #
# idempotency and duplicate delivery
# --------------------------------------------------------------------------- #

class TestIdempotency:
    def test_a_valid_delivery_is_accepted_once(self):
        conn = FakeConn()
        result = deliver(conn)
        assert result["status"] == ei.DELIVERY_ACCEPTED
        assert result["signal_id"] is not None
        assert result["symbol"] == "AAPL"

    def test_the_same_bytes_twice_collapse(self):
        conn = FakeConn()
        raw = body_of()
        first = deliver(conn, raw)
        second = deliver(conn, raw)
        assert first["status"] == ei.DELIVERY_ACCEPTED
        assert second["status"] == ei.DELIVERY_DUPLICATE
        assert second["reason"] == "duplicate_delivery"
        assert len(conn.signals) == 1

    def test_a_re_enveloped_duplicate_is_caught_by_idempotency(self):
        # Different bytes (key order changed), same observation. The delivery
        # replay check cannot see it; the semantic idempotency key must.
        conn = FakeConn()
        deliver(conn, json.dumps(payload()).encode())
        reordered = json.dumps(payload(), sort_keys=True).encode()
        second = deliver(conn, reordered)
        if reordered != json.dumps(payload()).encode():
            assert second["status"] == ei.DELIVERY_DUPLICATE
            assert second["reason"] == "duplicate_signal"
        assert len(conn.signals) == 1

    def test_the_same_state_firing_later_is_a_new_observation(self):
        # A repeated same-state alert an hour later is NOT a duplicate: it is
        # the source saying the condition still holds, and suppressing it would
        # lose a real observation.
        conn = FakeConn()
        later = NOW + timedelta(hours=1)
        deliver(conn, now=NOW, source_timestamp=NOW.isoformat())
        second = deliver(conn, now=later,
                               source_timestamp=later.isoformat())
        assert second["status"] == ei.DELIVERY_ACCEPTED
        assert len(conn.signals) == 2

    def test_a_source_supplied_id_wins_over_the_semantic_tuple(self):
        conn = FakeConn()
        later = NOW + timedelta(hours=1)
        deliver(conn, now=NOW, source_signal_id="abc",
                      source_timestamp=NOW.isoformat())
        second = deliver(conn, now=later, source_signal_id="abc",
                               source_timestamp=later.isoformat())
        # Same declared identity: the source is the authority on sameness.
        assert second["status"] == ei.DELIVERY_DUPLICATE
        assert len(conn.signals) == 1

    def test_the_key_falls_back_to_the_fingerprint_without_a_timestamp(self):
        sig = {"source": "ai_edge", "symbol": "AAPL", "observed_at": None}
        key = ei.idempotency_key(sig, fingerprint="deadbeef")
        again = ei.idempotency_key(sig, fingerprint="deadbeef")
        different = ei.idempotency_key(sig, fingerprint="cafe")
        assert key == again and key != different


# --------------------------------------------------------------------------- #
# rejections are recorded, refusals are not
# --------------------------------------------------------------------------- #

class TestDeliveryAudit:
    def test_a_rejected_payload_is_still_recorded_for_diagnosis(self):
        # A gateway that logs only its successes cannot answer "the alert
        # fired, why is nothing showing?" — which is the question that will
        # actually be asked.
        conn = FakeConn()
        result = deliver(conn, symbol="not a symbol")
        assert result["status"] == ei.DELIVERY_REJECTED
        assert result["reason"] == "invalid_symbol"
        assert len(conn.deliveries) == 1
        assert len(conn.signals) == 0

    def test_a_rejection_reason_is_a_code_never_an_exception_string(self):
        conn = FakeConn()
        result = deliver(conn, contract_version="nope.v9")
        assert result["reason"] == "unsupported_contract_version"
        for leak in ("Traceback", "asyncpg", "SELECT", "INSERT", "psql",
                     "postgres", "Exception"):
            assert leak not in json.dumps(result)

    def test_the_stored_raw_payload_is_redacted(self):
        conn = FakeConn()
        deliver(conn, metadata={"token": "leak-me"})
        stored = json.dumps([r["raw_payload"] for r in conn.rows])
        assert "leak-me" not in stored


# --------------------------------------------------------------------------- #
# the frozen-universe boundary (Phase 20)
# --------------------------------------------------------------------------- #

class TestUniverseBoundary:
    def test_a_universe_symbol_is_marked_in_scope(self):
        conn = FakeConn()
        result = deliver(conn, symbol="AAPL")
        assert result["symbol_scope"] == ei.SCOPE_UNIVERSE

    def test_an_outside_symbol_is_accepted_but_quarantined(self):
        # Rejecting it would throw away "what else should we investigate?".
        # Accepting it into the scanner surface would corrupt a live
        # experiment. It is stored, and labelled.
        conn = FakeConn()
        result = deliver(conn, symbol="TSLA")
        assert result["status"] == ei.DELIVERY_ACCEPTED
        assert result["symbol_scope"] == ei.SCOPE_DISCOVERY

    def test_a_universe_lookup_failure_degrades_to_research_only(self):
        # The conservative direction: a signal wrongly marked research-only is
        # invisible; one wrongly marked in-universe enters the experiment.
        conn = FakeConn(universe_fails=True)
        result = deliver(conn, symbol="AAPL")
        assert result["status"] == ei.DELIVERY_ACCEPTED
        assert result["symbol_scope"] == ei.SCOPE_DISCOVERY

    def test_classification_is_a_pure_function(self):
        assert ei.classify_symbol_scope("AAPL", {"AAPL"}) == ei.SCOPE_UNIVERSE
        assert ei.classify_symbol_scope("TSLA", {"AAPL"}) == ei.SCOPE_DISCOVERY
        assert ei.classify_symbol_scope("AAPL", None) == ei.SCOPE_DISCOVERY


# --------------------------------------------------------------------------- #
# rate limiting
# --------------------------------------------------------------------------- #

class TestRateLimiter:
    def test_it_admits_up_to_the_limit_then_refuses(self):
        limiter = ei.SlidingWindowLimiter(3, window_seconds=60)
        assert all(limiter.allow("k", now=100.0) for _ in range(3))
        assert not limiter.allow("k", now=100.0)

    def test_the_window_slides(self):
        limiter = ei.SlidingWindowLimiter(2, window_seconds=60)
        limiter.allow("k", now=100.0)
        limiter.allow("k", now=100.0)
        assert not limiter.allow("k", now=120.0)
        assert limiter.allow("k", now=161.0)

    def test_keys_are_independent(self):
        limiter = ei.SlidingWindowLimiter(1, window_seconds=60)
        assert limiter.allow("a", now=100.0)
        assert limiter.allow("b", now=100.0)
        assert not limiter.allow("a", now=100.0)


# --------------------------------------------------------------------------- #
# the AI Edge normaliser
# --------------------------------------------------------------------------- #

class TestAiEdgeNormalizer:
    @pytest.mark.parametrize("condition,stype,direction", [
        ("Open Long", "entry_signal", "bullish"),
        ("Close Long", "exit_signal", "bearish"),
        ("Open Short", "entry_signal", "bearish"),
        ("Close Short", "exit_signal", "bullish"),
        ("Kernel Bullish Color Change", "trend", "bullish"),
        ("Kernel Bearish Color Change", "trend", "bearish"),
    ])
    def test_the_real_alert_conditions_normalise(self, condition, stype,
                                                 direction):
        body = payload(source="ai_edge", signal_type=condition)
        del body["direction"]
        out = ad.normalize(body, received_at=NOW)
        assert out["signal_type_normalized"] == stype
        assert out["direction_normalized"] == direction

    def test_a_direction_agnostic_condition_is_not_given_a_side(self):
        # `Close Position` closes whatever is open. Assigning it bullish or
        # bearish would invent a claim the indicator never made.
        body = payload(source="ai_edge", signal_type="Close Position")
        del body["direction"]
        out = ad.normalize(body, received_at=NOW)
        assert out["signal_type_normalized"] == "exit_signal"
        assert out["direction_normalized"] == "unknown"

    def test_an_explicit_direction_is_never_overridden(self):
        out = ad.normalize(payload(source="ai_edge", signal_type="Open Long",
                                   direction="bearish"), received_at=NOW)
        assert out["direction_normalized"] == "bearish"

    def test_ai_edge_never_gains_a_confidence(self):
        # Measured fact, not a policy choice: the indicator's vote score is
        # drawn with label.new() and cannot reach an alert message.
        out = ad.normalize(payload(source="ai_edge"), received_at=NOW)
        assert out["confidence"] is None
        assert out["confidence_scale"] is None

    def test_a_confidence_without_a_scale_is_preserved_but_not_promoted(self):
        out = ad.normalize(payload(confidence=7), received_at=NOW)
        assert out["confidence"] is None
        assert out["source_metadata"]["unscaled_confidence"] == 7

    def test_an_unidentified_ai_edge_alert_still_records_its_source(self):
        out = ad.normalize(payload(source="ai_edge"), received_at=NOW)
        assert out["indicator"] == ad.DEFAULT_AI_EDGE_INDICATOR


class TestGenericTradingViewSource:
    def test_a_generic_indicator_needs_no_new_endpoint(self):
        out = ad.normalize(payload(source="tradingview",
                                   signal_type="breakout",
                                   indicator="my_own_script",
                                   indicator_version="v3"), received_at=NOW)
        assert out["source"] == "tradingview"
        assert out["signal_type_normalized"] == "breakout"
        assert out["indicator"] == "my_own_script"
        assert out["indicator_version"] == "v3"

    def test_sources_stay_distinguishable(self):
        a = ad.normalize(payload(source="ai_edge"), received_at=NOW)
        b = ad.normalize(payload(source="tradingview"), received_at=NOW)
        assert a["source"] != b["source"]

    def test_delayed_data_is_surfaced_rather_than_left_in_a_suffix(self):
        out = ad.normalize(payload(exchange="NASDAQ_DLY"), received_at=NOW)
        assert out["source_metadata"]["exchange"] == "NASDAQ_DLY"
        assert out["source_metadata"]["exchange_venue"] == "NASDAQ"
        assert out["source_metadata"]["data_delayed"] is True
