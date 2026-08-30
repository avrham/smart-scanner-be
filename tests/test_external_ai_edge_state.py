"""Connected-and-silent is not blind: the AI Edge semantic hygiene fix.

The gap this closes: `ai_edge` is `live` in the registry — the account owner
armed the alert, the ingress is reachable — and no signal has ever fired,
because the condition simply has not occurred. `evaluate_freshness` reported
that as `unavailable / never_refreshed`, which the UI rendered identically to a
source we cannot see at all. One of those is a working connection; the other is
blindness, and telling a user they are the same thing is a false statement
about our own system.

What must NOT change, and is asserted here: confluence, attention, ordering and
the genuinely-unavailable path.
"""

from datetime import date, datetime, timezone

import app.external_signals as ex

UTC = timezone.utc
NOW = datetime(2026, 8, 30, 17, 0, tzinfo=UTC)
SESSION = date(2026, 8, 28)


class TestFreshnessVerdict:
    def test_live_and_never_delivered_is_connected(self):
        verdict = ex.evaluate_freshness(None, now=NOW, registry_status="live")
        assert verdict["status"] == ex.STATUS_CONNECTED
        assert verdict["reason"] == ex.REASON_AWAITING_FIRST_SIGNAL

    def test_connected_is_not_unavailable(self):
        assert ex.STATUS_CONNECTED != ex.STATUS_UNAVAILABLE
        assert ex.STATUS_CONNECTED in ex.EXTERNAL_STATUSES

    def test_awaiting_setup_still_reports_not_configured(self):
        for registry_status in ("requires_manual_setup",
                                "available_not_integrated"):
            verdict = ex.evaluate_freshness(None, now=NOW,
                                            registry_status=registry_status)
            assert verdict["status"] == ex.STATUS_UNAVAILABLE
            assert verdict["reason"] == ex.REASON_NOT_CONFIGURED

    def test_a_source_we_have_never_heard_of_is_still_never_refreshed(self):
        verdict = ex.evaluate_freshness(None, now=NOW, registry_status=None)
        assert verdict["reason"] == ex.REASON_NEVER_REFRESHED

    def test_a_genuinely_broken_delivery_path_is_still_unavailable(self):
        # A state row EXISTS and is not ok: something delivered once and then
        # broke. That is an outage and must not be dressed up as "connected".
        broken = {"status": "error", "last_refresh_at": NOW,
                  "last_success_at": None, "detail": "ingest_failed"}
        verdict = ex.evaluate_freshness(broken, now=NOW, registry_status="live")
        assert verdict["status"] == ex.STATUS_UNAVAILABLE
        assert verdict["reason"] == ex.REASON_SOURCE_UNAVAILABLE

    def test_a_delivering_source_is_still_available(self):
        healthy = {"status": "ok", "last_refresh_at": NOW,
                   "last_success_at": NOW, "detail": None}
        verdict = ex.evaluate_freshness(healthy, now=NOW,
                                        registry_status="live")
        assert verdict["status"] == ex.STATUS_AVAILABLE

    def test_a_quiet_source_still_goes_stale_eventually(self):
        old = datetime(2026, 8, 1, tzinfo=UTC)
        quiet = {"status": "ok", "last_refresh_at": old,
                 "last_success_at": old, "detail": None}
        assert ex.evaluate_freshness(quiet, now=NOW,
                                     registry_status="live")["status"] \
            == ex.STATUS_STALE


class TestCombine:
    def test_connected_beats_unavailable_for_the_dimension(self):
        combined = ex.combine_freshness({
            "tradingview": {"status": ex.STATUS_UNAVAILABLE,
                            "reason": ex.REASON_NOT_CONFIGURED,
                            "age_hours": None},
            "ai_edge": {"status": ex.STATUS_CONNECTED,
                        "reason": ex.REASON_AWAITING_FIRST_SIGNAL,
                        "age_hours": None}})
        assert combined["status"] == ex.STATUS_CONNECTED

    def test_a_source_with_data_still_wins(self):
        combined = ex.combine_freshness({
            "tradingview": {"status": ex.STATUS_AVAILABLE, "reason": None,
                            "age_hours": 1.0},
            "ai_edge": {"status": ex.STATUS_CONNECTED,
                        "reason": ex.REASON_AWAITING_FIRST_SIGNAL,
                        "age_hours": None}})
        assert combined["status"] == ex.STATUS_AVAILABLE

    def test_per_source_verdicts_survive_the_combine(self):
        combined = ex.combine_freshness({
            "ai_edge": {"status": ex.STATUS_CONNECTED,
                        "reason": ex.REASON_AWAITING_FIRST_SIGNAL,
                        "age_hours": None}})
        assert combined["per_source"]["ai_edge"]["reason"] \
            == ex.REASON_AWAITING_FIRST_SIGNAL


REGISTRY = [
    {"source": "ai_edge", "display_name": "AI Edge", "status": "live",
     "transports": ["webhook"], "supports_signal_events": True,
     "licensing_visibility": "product_display_allowed"},
    {"source": "tradingview", "display_name": "TradingView", "status": "live",
     "transports": ["webhook"], "supports_signal_events": True,
     "licensing_visibility": "product_display_allowed"},
]

CONNECTED = {"status": ex.STATUS_CONNECTED,
             "reason": ex.REASON_AWAITING_FIRST_SIGNAL, "age_hours": None,
             "per_source": {"ai_edge": {
                 "status": ex.STATUS_CONNECTED,
                 "reason": ex.REASON_AWAITING_FIRST_SIGNAL,
                 "last_success_at": None, "age_hours": None}}}


class TestContext:
    def test_the_block_reports_connected_not_unavailable(self):
        ctx = ex.build_external_context([], as_of_session=SESSION,
                                        sources=REGISTRY, freshness=CONNECTED,
                                        attention="high_attention",
                                        scan_session=SESSION)
        assert ctx["status"] == ex.STATUS_CONNECTED
        assert ctx["reason"] == ex.REASON_AWAITING_FIRST_SIGNAL

    def test_no_signal_presence_is_fabricated(self):
        ctx = ex.build_external_context([], as_of_session=SESSION,
                                        sources=REGISTRY, freshness=CONNECTED,
                                        attention="high_attention",
                                        scan_session=SESSION)
        assert ctx["items"] == []
        assert ctx["in_window_count"] == 0
        assert ctx["notable_count"] == 0
        assert ctx["sources_present"] == []
        assert ctx["last_signal_at"] is None

    def test_confluence_is_unchanged_by_the_new_status(self):
        # Deliberate. There is no external reading here, so the confluence
        # answer is the same one an unavailable dimension gives. Letting it
        # fall through to the normal path would turn "we have heard nothing"
        # into `internal_only`, which is a claim about the evidence.
        connected = ex.build_external_context(
            [], as_of_session=SESSION, sources=REGISTRY,
            freshness=CONNECTED, attention="high_attention",
            scan_session=SESSION)
        unavailable = ex.build_external_context(
            [], as_of_session=SESSION, sources=REGISTRY,
            freshness={"status": ex.STATUS_UNAVAILABLE,
                       "reason": ex.REASON_SOURCE_UNAVAILABLE,
                       "age_hours": None, "per_source": {}},
            attention="high_attention", scan_session=SESSION)
        assert connected["confluence"] == unavailable["confluence"]
        assert connected["confluence"] == ex.CONFLUENCE_UNAVAILABLE

    def test_the_row_carries_the_status_so_the_ui_can_word_it(self):
        row = ex.build_row_external(ex.build_external_context(
            [], as_of_session=SESSION, sources=REGISTRY, freshness=CONNECTED,
            attention="developing", scan_session=SESSION))
        assert row["status"] == ex.STATUS_CONNECTED
        assert row["reason"] == ex.REASON_AWAITING_FIRST_SIGNAL
        assert row["notable_count"] == 0
        assert row["latest_source"] is None

    def test_per_source_availability_reaches_the_registry_entry(self):
        ctx = ex.build_external_context([], as_of_session=SESSION,
                                        sources=REGISTRY, freshness=CONNECTED,
                                        scan_session=SESSION)
        entries = {e["source"]: e for e in ctx["sources"]}
        assert entries["ai_edge"]["availability"]["status"] \
            == ex.STATUS_CONNECTED
        # TradingView has no verdict in this freshness map, so it reports None
        # rather than borrowing AI Edge's.
        assert entries["tradingview"]["availability"] is None

    def test_the_empty_context_helper_is_still_unavailable(self):
        ctx = ex.empty_external_context()
        assert ctx["status"] == ex.STATUS_UNAVAILABLE
        assert ctx["confluence"] == ex.CONFLUENCE_UNAVAILABLE


class TestNoRankingImpact:
    def test_the_status_change_touches_no_ordering_input(self):
        row = ex.build_row_external(ex.build_external_context(
            [], as_of_session=SESSION, sources=REGISTRY, freshness=CONNECTED,
            attention="high_attention", scan_session=SESSION))
        # The overview sorts on attention; nothing in this block is read by
        # sv.attention_sort_key, and the compact row carries no score.
        assert "score" not in row and "rank" not in row

    def test_summarize_sources_counts_nothing_for_a_silent_source(self):
        ctx = ex.build_external_context([], as_of_session=SESSION,
                                        sources=REGISTRY, freshness=CONNECTED,
                                        attention="high_attention",
                                        scan_session=SESSION)
        summary = ex.summarize_sources([ctx])
        assert summary["symbols_with_external_signal"] == 0
        assert summary["recent_signal_count"] == 0
        assert summary["agreement_symbol_count"] == 0
