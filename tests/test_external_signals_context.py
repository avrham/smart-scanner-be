"""The pure external-intelligence model: what a session knew, and when.

Every test here runs without a database, without a network and without the
FastAPI app — `app.external_signals` is a deterministic function of stored rows
and one session date, which is what makes the honesty claims checkable.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import app.external_signals as ex

UTC = timezone.utc


def signal(*, effective: str = "2026-08-25T17:00:00+00:00",
           source: str = "ai_edge", symbol: str = "AAPL",
           direction: str = "bullish", signal_type: str = "classification",
           timeframe: str = "4h", confidence=None, confidence_scale=None,
           signal_id: str = "s1", supersedes=None,
           observed: str = None):
    """One persisted row, shaped exactly like the Product API's SELECT."""
    eff = datetime.fromisoformat(effective)
    return {
        "id": signal_id,
        "source": source,
        "symbol": symbol,
        "source_signal_id": None,
        "observed_at": datetime.fromisoformat(observed) if observed else eff,
        "received_at": eff,
        "effective_at": eff,
        "clock_skew_seconds": 0,
        "timeframe": timeframe,
        "timeframe_normalized": ex.normalize_timeframe(timeframe),
        "signal_type": signal_type,
        "signal_type_normalized": ex.normalize_signal_type(signal_type),
        "direction": direction,
        "direction_normalized": ex.normalize_direction(direction),
        "confidence": confidence,
        "confidence_scale": confidence_scale,
        "indicator": "ai_edge",
        "indicator_version": "lc-2.1",
        "alert_id": "alert-1",
        "contract_version": ex.TRADINGVIEW_CONTRACT_VERSION,
        "source_payload_version": None,
        "supersedes_signal_id": supersedes,
    }


FRESH = {"status": ex.STATUS_AVAILABLE, "reason": None,
         "last_refresh_at": None, "last_success_at": None,
         "age_hours": 1.0, "detail": None}
SESSION = date(2026, 8, 25)          # a Tuesday
REGISTRY = [{"source": "ai_edge", "display_name": "AI Edge",
             "status": "live", "transports": ["webhook"],
             "emits_signals": True}]


def build(signals, *, session=SESSION, attention=None, freshness=FRESH):
    return ex.build_external_context(
        signals, as_of_session=session, sources=REGISTRY,
        freshness=freshness, attention=attention)


# --------------------------------------------------------------------------- #
# normalisation preserves the source's own semantics
# --------------------------------------------------------------------------- #

class TestNormalizationKeepsSourceSemantics:
    @pytest.mark.parametrize("raw,expected", [
        ("bullish", "bullish"), ("long", "bullish"), ("buy", "bullish"),
        ("bearish", "bearish"), ("short", "bearish"), ("sell", "bearish"),
        ("flat", "neutral"), ("sideways", "neutral"),
    ])
    def test_known_words_map_to_a_view(self, raw, expected):
        assert ex.normalize_direction(raw) == expected

    def test_an_unrecognised_word_is_unknown_not_neutral(self):
        # The distinction that matters: `neutral` claims the source saw no
        # edge, `unknown` admits we could not read it. Collapsing the second
        # into the first would silently invent a reading.
        assert ex.normalize_direction("kernel_flip_upward") == "unknown"
        assert ex.normalize_direction("") == "unknown"
        assert ex.normalize_direction(None) == "unknown"

    def test_no_buy_or_sell_survives_into_our_vocabulary(self):
        # Execution words must not exist downstream: this system records
        # opinions, it does not place orders.
        assert "buy" not in ex.DIRECTIONS
        assert "sell" not in ex.DIRECTIONS
        assert ex.normalize_direction("buy") == ex.DIRECTION_BULLISH

    @pytest.mark.parametrize("raw,expected", [
        ("240", "4h"), ("60", "1h"), ("D", "1d"), ("1D", "1d"),
        ("W", "1w"), ("15", "15m"), ("4h", "4h"),
    ])
    def test_tradingview_intervals_normalise(self, raw, expected):
        assert ex.normalize_timeframe(raw) == expected

    def test_an_unmappable_timeframe_is_none_never_a_guess(self):
        assert ex.normalize_timeframe("45") is None
        assert ex.normalize_timeframe("banana") is None

    def test_every_normalised_timeframe_satisfies_the_db_constraint(self):
        # A value this function can emit but the CHECK constraint rejects
        # would fail at INSERT time, in production, on a real alert.
        for raw in ("1", "3", "5", "15", "30", "60", "120", "240",
                    "D", "W", "M", "daily", "weekly"):
            out = ex.normalize_timeframe(raw)
            assert out is None or out in ex.NORMALIZED_TIMEFRAMES


# --------------------------------------------------------------------------- #
# the point-in-time gate
# --------------------------------------------------------------------------- #

class TestPointInTime:
    def test_a_signal_arriving_after_the_close_is_invisible(self):
        after = datetime(2026, 8, 25, 20, 30, tzinfo=UTC)   # 16:30 ET
        assert not ex.is_visible_to_session(after, SESSION)

    def test_a_signal_arriving_before_the_close_is_visible(self):
        before = datetime(2026, 8, 25, 17, 0, tzinfo=UTC)   # 13:00 ET
        assert ex.is_visible_to_session(before, SESSION)

    def test_the_gate_uses_arrival_not_the_sources_own_clock(self):
        # THE core honesty property. A source claiming it fired days earlier
        # cannot backdate itself into a session it did not reach in time — that
        # would be lookahead that looks exactly like a finding.
        row = signal(effective="2026-08-26T17:00:00+00:00",   # arrived Wed
                     observed="2026-08-20T13:00:00+00:00")    # claims prev Thu
        assert build([row]) ["in_window_count"] == 0

    def test_a_backdated_claim_is_still_recorded_as_evidence(self):
        # The claim is refused as a GATE, never discarded as a FACT.
        row = signal(effective="2026-08-25T17:00:00+00:00",
                     observed="2026-08-25T16:30:00+00:00")
        item = build([row])["items"][0]
        assert item["observed_at"] is not None
        assert item["effective_at"] is not None
        assert item["observed_at"] != item["effective_at"]


# --------------------------------------------------------------------------- #
# windows
# --------------------------------------------------------------------------- #

class TestProximityWindows:
    @pytest.mark.parametrize("ago,expected", [
        (0, ex.PROXIMITY_THIS_SESSION),
        (1, ex.PROXIMITY_PREVIOUS_SESSION),
        (3, ex.PROXIMITY_RECENT),
        (10, ex.PROXIMITY_OLDER_CONTEXT),
        (11, ex.PROXIMITY_OUT_OF_WINDOW),
        (None, ex.PROXIMITY_OUT_OF_WINDOW),
    ])
    def test_classification(self, ago, expected):
        assert ex.classify_proximity(ago) == expected

    def test_an_old_signal_is_dropped_so_a_badge_cannot_live_forever(self):
        old = signal(effective="2026-06-01T17:00:00+00:00")
        assert build([old])["in_window_count"] == 0

    def test_only_a_directional_recent_signal_is_notable(self):
        assert ex.is_notable(ex.PROXIMITY_THIS_SESSION, direction="bullish")
        assert not ex.is_notable(ex.PROXIMITY_THIS_SESSION, direction="neutral")
        assert not ex.is_notable(ex.PROXIMITY_THIS_SESSION, direction="unknown")
        assert not ex.is_notable(ex.PROXIMITY_OLDER_CONTEXT, direction="bullish")


# --------------------------------------------------------------------------- #
# confidence is never invented
# --------------------------------------------------------------------------- #

class TestConfidenceIsNeverInvented:
    def test_a_source_with_no_confidence_reports_unavailable(self):
        item = build([signal()])["items"][0]
        assert item["confidence"] is None
        assert item["confidence_available"] is False

    def test_a_supplied_confidence_survives_with_its_scale(self):
        item = build([signal(confidence=0.82,
                             confidence_scale="probability_0_1")])["items"][0]
        assert item["confidence"] == pytest.approx(0.82)
        assert item["confidence_scale"] == "probability_0_1"
        assert item["confidence_available"] is True

    def test_no_default_confidence_appears_anywhere_in_the_block(self):
        block = build([signal(), signal(signal_id="s2", direction="bearish")])
        rendered = repr(block)
        assert "0.5" not in rendered


# --------------------------------------------------------------------------- #
# corrections never destroy a prior observation
# --------------------------------------------------------------------------- #

class TestCorrections:
    def test_a_correction_hides_the_row_it_supersedes(self):
        original = signal(signal_id="a", direction="bullish")
        fix = signal(signal_id="b", direction="bearish", supersedes="a",
                     effective="2026-08-25T18:00:00+00:00")
        block = build([original, fix])
        assert block["in_window_count"] == 1
        assert block["items"][0]["direction_normalized"] == "bearish"
        assert block["items"][0]["is_correction"] is True

    def test_a_correction_the_session_could_not_see_changes_nothing(self):
        # A fix that arrived AFTER the close must not retroactively erase what
        # we were actually looking at on the day — that is lookahead wearing
        # the costume of a data correction.
        original = signal(signal_id="a", direction="bullish")
        late_fix = signal(signal_id="b", direction="bearish", supersedes="a",
                          effective="2026-08-26T18:00:00+00:00")
        block = build([original, late_fix])
        assert block["in_window_count"] == 1
        assert block["items"][0]["direction_normalized"] == "bullish"


# --------------------------------------------------------------------------- #
# confluence describes, it never scores
# --------------------------------------------------------------------------- #

class TestConfluenceIsNotAScore:
    def test_internal_interest_plus_external_bullish_is_agreement(self):
        block = build([signal(direction="bullish")], attention="high_attention")
        assert block["confluence"] == ex.CONFLUENCE_AGREEMENT

    def test_internal_interest_plus_external_bearish_is_disagreement(self):
        block = build([signal(direction="bearish")], attention="high_attention")
        assert block["confluence"] == ex.CONFLUENCE_DISAGREEMENT

    def test_conflicting_external_sources_are_mixed(self):
        block = build([signal(signal_id="a", direction="bullish"),
                       signal(signal_id="b", source="tradingview",
                              direction="bearish")],
                      attention="high_attention")
        assert block["confluence"] == ex.CONFLUENCE_MIXED

    def test_external_signal_without_internal_interest(self):
        block = build([signal()], attention="low_attention")
        assert block["confluence"] == ex.CONFLUENCE_EXTERNAL_ONLY

    def test_internal_interest_with_no_external_signal(self):
        assert build([], attention="high_attention")["confluence"] == \
            ex.CONFLUENCE_INTERNAL_ONLY

    def test_neither_side_says_anything(self):
        assert build([], attention="low_attention")["confluence"] == \
            ex.CONFLUENCE_NO_EXTERNAL_SIGNAL

    def test_an_unavailable_source_is_not_silently_read_as_no_signal(self):
        block = build([], freshness={"status": ex.STATUS_UNAVAILABLE,
                                     "reason": ex.REASON_NOT_CONFIGURED,
                                     "age_hours": None})
        assert block["confluence"] == ex.CONFLUENCE_UNAVAILABLE

    def test_confluence_carries_no_number_and_no_ranking(self):
        block = build([signal()], attention="high_attention")
        # The whole point of Phase 12: we are trying to MEASURE whether
        # confluence has value. Publishing a score would answer that question
        # by assertion, and the number would be read as a probability.
        assert isinstance(block["confluence"], str)
        for key in ("score", "confluence_score", "weight", "rank",
                    "probability", "strength"):
            assert key not in block

    def test_a_chatty_source_cannot_outvote_a_quiet_one(self):
        many_bullish = [signal(signal_id=f"b{i}", direction="bullish")
                        for i in range(5)]
        one_bearish = [signal(signal_id="x", source="tradingview",
                              direction="bearish")]
        block = build(many_bullish + one_bearish, attention="high_attention")
        # Five vs one is still "they disagree", not "bullish wins 5-1".
        assert block["confluence"] == ex.CONFLUENCE_MIXED


# --------------------------------------------------------------------------- #
# availability
# --------------------------------------------------------------------------- #

class TestFreshness:
    def test_never_configured_is_distinguished_from_broken(self):
        never = ex.evaluate_freshness(None, now=datetime.now(UTC),
                                      registry_status="requires_manual_setup")
        assert never["reason"] == ex.REASON_NOT_CONFIGURED

        broken = ex.evaluate_freshness(
            {"status": "error", "last_success_at": None},
            now=datetime.now(UTC), registry_status="live")
        assert broken["reason"] == ex.REASON_SOURCE_UNAVAILABLE

    def test_a_quiet_pushed_source_is_not_stale_for_a_week(self):
        # An indicator that has not fired is behaving correctly. Reporting a
        # quiet week as a fault would train the reader to ignore the field.
        now = datetime.now(UTC)
        state = {"status": "ok", "last_success_at": now - timedelta(days=3),
                 "last_refresh_at": now - timedelta(days=3), "detail": None}
        assert ex.evaluate_freshness(state, now=now)["status"] == \
            ex.STATUS_AVAILABLE

    def test_a_genuinely_disconnected_source_eventually_reports_stale(self):
        now = datetime.now(UTC)
        state = {"status": "ok", "last_success_at": now - timedelta(days=9),
                 "last_refresh_at": now - timedelta(days=9), "detail": None}
        assert ex.evaluate_freshness(state, now=now)["status"] == ex.STATUS_STALE

    def test_one_working_source_keeps_the_dimension_available(self):
        # Best-of, not worst-of: a second source nobody has connected must not
        # hide the first source that is actually delivering.
        combined = ex.combine_freshness({
            "ai_edge": {"status": ex.STATUS_AVAILABLE, "age_hours": 2.0},
            "tradingview": {"status": ex.STATUS_UNAVAILABLE,
                            "reason": ex.REASON_NOT_CONFIGURED,
                            "age_hours": None},
        })
        assert combined["status"] == ex.STATUS_AVAILABLE
        assert combined["per_source"]["tradingview"]["reason"] == \
            ex.REASON_NOT_CONFIGURED


# --------------------------------------------------------------------------- #
# the compact list row
# --------------------------------------------------------------------------- #

class TestRowSummary:
    def test_a_quiet_symbol_carries_a_zero_count_not_a_claim(self):
        row = ex.build_row_external(build([]))
        assert row["notable_count"] == 0
        assert row["latest_source"] is None
        # There must be no field a UI could print as "no external signal" on
        # 25 rows — most symbols are quiet on most days, and printing that
        # everywhere would turn an ordinary day into a statement.
        assert row["external_stance"] == "none"

    def test_the_list_row_never_carries_the_signals_themselves(self):
        row = ex.build_row_external(build([signal()]))
        assert "items" not in row
        assert row["notable_count"] == 1
        assert row["latest_source"] == "ai_edge"

    def test_the_overview_summary_counts_rows_and_nothing_else(self):
        contexts = [build([signal()], attention="high_attention"),
                    build([], attention="low_attention")]
        summary = ex.summarize_sources(contexts)
        assert summary["symbols_with_external_signal"] == 1
        assert summary["agreement_symbol_count"] == 1
        assert summary["external_sources_present"] == ["ai_edge"]


# --------------------------------------------------------------------------- #
# total failure degrades this dimension only
# --------------------------------------------------------------------------- #

class TestDegradation:
    def test_an_empty_block_is_well_formed_and_says_unavailable(self):
        block = ex.empty_external_context(reason=ex.REASON_SOURCE_UNAVAILABLE)
        assert block["status"] == ex.STATUS_UNAVAILABLE
        assert block["items"] == []
        assert block["confluence"] == ex.CONFLUENCE_UNAVAILABLE
        # The compact form must survive the same failure without raising.
        assert ex.build_row_external(block)["notable_count"] == 0

    def test_a_missing_session_never_raises(self):
        block = ex.build_external_context(
            [signal()], as_of_session=None, sources=REGISTRY, freshness=FRESH)
        assert block["status"] == ex.STATUS_UNAVAILABLE


# --------------------------------------------------------------------------- #
# the display anchor
# --------------------------------------------------------------------------- #

class TestDisplayAnchor:
    """External signals are PUSHED continuously; the scan is produced on our
    own schedule. When the scan goes stale the two diverge, and the display
    must not report "no external signal" for something that fired an hour ago.
    """

    NOW = datetime(2026, 8, 28, 9, 52, tzinfo=UTC)   # a Friday, before the close

    def test_a_pinned_session_is_never_widened(self):
        # Viewing a past session must show what THAT session could see. Any
        # widening here would be lookahead.
        assert ex.resolve_anchor_session(date(2026, 8, 25), now=self.NOW,
                                         pinned=True) == date(2026, 8, 25)

    def test_the_default_view_anchors_on_the_current_session(self):
        assert ex.resolve_anchor_session(date(2026, 8, 25), now=self.NOW,
                                         pinned=False) == date(2026, 8, 28)

    def test_a_scan_newer_than_now_is_left_alone(self):
        assert ex.resolve_anchor_session(date(2026, 9, 10), now=self.NOW,
                                         pinned=False) == date(2026, 9, 10)

    def test_no_scan_session_means_no_anchor(self):
        assert ex.resolve_anchor_session(None, now=self.NOW, pinned=False) is None

    def test_a_current_signal_is_visible_beside_a_stale_scan(self):
        fresh = signal(effective="2026-08-28T09:52:00+00:00")
        anchor = ex.resolve_anchor_session(date(2026, 8, 25), now=self.NOW,
                                           pinned=False)
        block = ex.build_external_context(
            [fresh], as_of_session=anchor, scan_session=date(2026, 8, 25),
            sources=REGISTRY, freshness=FRESH, attention="high_attention")
        assert block["in_window_count"] == 1
        assert block["items"][0]["proximity"] == ex.PROXIMITY_THIS_SESSION

    def test_the_divergence_is_reported_rather_than_hidden(self):
        # The reader must be able to see that a current signal is sitting
        # beside an older scan — otherwise the UI implies the scanner saw it.
        block = ex.build_external_context(
            [], as_of_session=date(2026, 8, 28), scan_session=date(2026, 8, 25),
            sources=REGISTRY, freshness=FRESH)
        assert block["anchor_is_scan_session"] is False
        assert block["as_of_session"] == "2026-08-28"
        assert block["scan_session"] == "2026-08-25"
        assert ex.build_row_external(block)["anchor_is_scan_session"] is False

    def test_a_fresh_scan_leaves_the_two_identical(self):
        block = ex.build_external_context(
            [], as_of_session=date(2026, 8, 28), scan_session=date(2026, 8, 28),
            sources=REGISTRY, freshness=FRESH)
        assert block["anchor_is_scan_session"] is True

    def test_the_measurement_gate_is_not_affected(self):
        # `is_visible_to_session` is what the outcome-linkage view mirrors. It
        # takes a session and answers strictly; the anchor rule lives above it
        # and must never have loosened it.
        late = datetime(2026, 8, 28, 9, 52, tzinfo=UTC)
        assert not ex.is_visible_to_session(late, date(2026, 8, 25))
