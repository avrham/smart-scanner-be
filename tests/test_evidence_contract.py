"""Phase 8: evidence.v1 contract tests.

Deterministic serialization, state vocabularies, raw/normalized coexistence,
hard-filter separation, unknown preservation, JSON safety, no secret-shaped
data in strategy-produced bundles.
"""

import json

import pytest

from app.workers.strategies.evidence import (
    EVIDENCE_STATES,
    EVIDENCE_VERSION,
    EvidenceBundle,
    EvidenceItem,
    SOURCE_TYPES,
)


def _item(**overrides):
    base = dict(
        code="sma_proximity",
        category="setup",
        source_type="strategy",
        state="pass",
        raw_value=2.31,
        normalized_value=0.23,
        unit="pct",
        threshold=3.0,
        operator="<=",
        required=True,
        timeframe="1d",
        as_of="2026-07-17",
        reason_code=None,
    )
    base.update(overrides)
    return EvidenceItem(**base)


def _bundle(items=None, **overrides):
    base = dict(
        strategy_code="sma150_bounce_v3",
        strategy_version="sma150.v3",
        decision_policy_version="sma150_bounce.policy.v1",
        symbol="TEST",
        verdict="WATCH",
        setup_state="valid",
        trigger_state="missing",
        market_data_as_of="2026-07-17T00:00:00",
        items=items if items is not None else [_item()],
    )
    base.update(overrides)
    return EvidenceBundle(**base)


class TestVocabularies:
    def test_valid_states_accepted(self):
        for state in sorted(EVIDENCE_STATES):
            assert _item(state=state).state == state

    def test_invalid_state_rejected(self):
        with pytest.raises(ValueError, match="invalid evidence state"):
            _item(state="maybe")

    def test_invalid_source_type_rejected(self):
        with pytest.raises(ValueError, match="invalid source_type"):
            _item(source_type="llm")

    def test_source_types_reserve_external_categories(self):
        # Phase 10 readiness: the vocabulary already names external sources.
        for src in ("market_data", "strategy", "external", "fundamental",
                    "event", "risk"):
            assert src in SOURCE_TYPES

    def test_bundle_state_vocabularies_enforced(self):
        with pytest.raises(ValueError, match="invalid verdict"):
            _bundle(verdict="MAYBE")
        with pytest.raises(ValueError, match="invalid setup_state"):
            _bundle(setup_state="almost")
        with pytest.raises(ValueError, match="invalid trigger_state"):
            _bundle(trigger_state="pending")


class TestDeterminism:
    def test_item_order_does_not_change_serialization(self):
        a = _item(code="alpha", category="setup")
        b = _item(code="beta", category="confirmation")
        c = _item(code="gamma", category="confirmation")
        d1 = _bundle(items=[a, b, c]).to_dict()
        d2 = _bundle(items=[c, a, b]).to_dict()
        assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)
        # Sorted by identity key (category first): confirmations before setup.
        assert [i["code"] for i in d1["items"]] == ["beta", "gamma", "alpha"]

    def test_dict_insertion_order_does_not_alter_serialization(self):
        m1 = _item(metadata={"a": 1, "b": 2})
        m2 = _item(metadata={"b": 2, "a": 1})
        d1 = _bundle(items=[m1]).to_dict()
        d2 = _bundle(items=[m2]).to_dict()
        assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)

    def test_missing_data_and_contradictions_sorted(self):
        # Set-like lists: order of construction never alters serialization.
        d1 = _bundle(
            missing_data=["z_field", "a_field"],
            contradictions=["negative_slope", "bearish_close"],
        ).to_dict()
        d2 = _bundle(
            missing_data=["a_field", "z_field"],
            contradictions=["bearish_close", "negative_slope"],
        ).to_dict()
        assert d1["missing_data"] == ["a_field", "z_field"]
        assert d1["contradictions"] == ["bearish_close", "negative_slope"]
        assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)

    def test_repeated_serialization_identical(self):
        bundle = _bundle()
        assert bundle.to_dict() == bundle.to_dict()


class TestSemanticOrderPreservation:
    """Semantic sequences carried inside item raw values/metadata are NEVER
    reordered by serialization — only set-like top-level lists are sorted."""

    def test_chronological_event_list_preserved_not_sorted(self):
        chronological = ["2026-02-01", "2026-04-15", "2026-06-30"]
        item = _item(code="bounce_event_separation",
                     raw_value=list(chronological))
        d = _bundle(items=[item]).to_dict()
        assert d["items"][0]["raw_value"] == chronological

    def test_reversing_chronological_events_alters_serialization(self):
        chronological = ["2026-02-01", "2026-04-15", "2026-06-30"]
        forward = _bundle(items=[
            _item(code="bounce_event_separation", raw_value=list(chronological))
        ]).to_dict()
        reversed_ = _bundle(items=[
            _item(code="bounce_event_separation",
                  raw_value=list(reversed(chronological)))
        ]).to_dict()
        assert json.dumps(forward, sort_keys=True) != json.dumps(
            reversed_, sort_keys=True
        )

    def test_declared_timeframe_sequence_preserved(self):
        d = _bundle(
            timeframe_summary={"required_timeframes": ["1M", "1w", "1d", "4h"]}
        ).to_dict()
        assert d["timeframe_summary"]["required_timeframes"] == [
            "1M", "1w", "1d", "4h"
        ]

    def test_ranking_components_are_named_keys_not_positional(self):
        d = _bundle(ranking_components={"volume_quality": 0.5,
                                        "trend_quality": 0.7}).to_dict()
        assert d["ranking_components"] == {
            "trend_quality": 0.7, "volume_quality": 0.5
        }


class TestItemIdentity:
    def test_two_distinct_timeframe_observations_not_collapsed(self):
        daily = _item(code="sma_slope", timeframe="1d", raw_value=1.2)
        four_hour = _item(code="sma_slope", timeframe="4h", raw_value=-0.4)
        d = _bundle(items=[daily, four_hour]).to_dict()
        assert len(d["items"]) == 2
        assert {i["timeframe"] for i in d["items"]} == {"1d", "4h"}
        # Deterministic order regardless of construction order.
        d_swapped = _bundle(items=[four_hour, daily]).to_dict()
        assert json.dumps(d, sort_keys=True) == json.dumps(
            d_swapped, sort_keys=True
        )

    def test_distinct_as_of_observations_not_collapsed(self):
        first = _item(code="sma_slope", as_of="2026-07-16", raw_value=1.0)
        second = _item(code="sma_slope", as_of="2026-07-17", raw_value=1.1)
        d = _bundle(items=[first, second]).to_dict()
        assert len(d["items"]) == 2

    def test_duplicate_ambiguous_identities_rejected(self):
        dup_a = _item(code="sma_slope", raw_value=1.0)
        dup_b = _item(code="sma_slope", raw_value=2.0)  # same full identity
        with pytest.raises(ValueError, match="duplicate ambiguous evidence"):
            _bundle(items=[dup_a, dup_b])


class TestValues:
    def test_raw_and_normalized_coexist(self):
        item = _item(raw_value=1.07, normalized_value=0.89).to_dict()
        assert item["raw_value"] == 1.07
        assert item["normalized_value"] == 0.89

    def test_normalized_out_of_bounds_rejected(self):
        with pytest.raises(ValueError, match="out of"):
            _item(normalized_value=1.5)
        with pytest.raises(ValueError, match="out of"):
            _item(normalized_value=-0.1)

    def test_unknown_state_never_converted(self):
        item = _item(state="unknown", raw_value=None, normalized_value=None,
                     reason_code="zero_range_bar")
        d = item.to_dict()
        assert d["state"] == "unknown"
        assert d["raw_value"] is None
        assert d["normalized_value"] is None

    def test_ranking_components_bounded(self):
        with pytest.raises(ValueError, match="out of"):
            _bundle(ranking_components={"volume_quality": 1.2})
        ok = _bundle(ranking_components={"volume_quality": 0.89,
                                         "trend_quality": None})
        assert ok.to_dict()["ranking_components"]["trend_quality"] is None

    def test_ranking_score_bounded(self):
        with pytest.raises(ValueError, match="out of"):
            _bundle(ranking_score=2.0)


class TestSeparation:
    def test_hard_filters_visible_and_separate(self):
        hard = _item(code="history_bars", category="data_readiness",
                     source_type="market_data", state="pass", required=True)
        soft = _item(code="volume_quality", category="ranking", state="neutral",
                     required=False)
        d = _bundle(
            items=[hard, soft],
            hard_filter_summary={"history_bars": "pass"},
        ).to_dict()
        assert d["hard_filter_summary"] == {"history_bars": "pass"}
        by_code = {i["code"]: i for i in d["items"]}
        assert by_code["history_bars"]["required"] is True
        assert by_code["volume_quality"]["required"] is False


class TestJsonSafety:
    def test_bundle_is_json_serializable(self):
        text = json.dumps(_bundle().to_dict())
        assert EVIDENCE_VERSION in text

    def test_non_json_safe_raw_value_rejected(self):
        import datetime
        with pytest.raises(ValueError, match="non-JSON-safe"):
            _item(raw_value=datetime.datetime(2026, 1, 1))

    def test_non_json_safe_metadata_rejected(self):
        with pytest.raises(ValueError, match="non-JSON-safe"):
            _item(metadata={"frame": object()})

    def test_no_secret_shaped_data_from_strategy_bundle(self):
        """A real v3 bundle contains no credential-looking keys or values."""
        import re
        from tests.sma150_v3_frames import build_uptrend_frame
        from app.workers.strategies import get_strategy
        from app.workers.strategies.base import StrategyContext

        strategy = get_strategy("sma150_bounce_v3")
        result = strategy.evaluate(
            build_uptrend_frame(),
            StrategyContext(symbol="TEST", pattern_code="sma150_bounce_v3",
                            config=strategy.default_config()),
        )
        text = json.dumps(result.details["evidence"]).lower()
        assert not re.search(
            r"api[_-]?key|token|secret|password|bearer|postgres://", text
        )
