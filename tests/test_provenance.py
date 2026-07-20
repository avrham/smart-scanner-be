"""Phase 7B — provenance building blocks (pure, no DB, no providers).

Covers: canonical config hashing, secret exclusion, evidence snapshot
fidelity + deterministic bounded pruning, market-data as-of extraction, and
version separation.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from app.workers.provenance import (
    DECISION_POLICY_VERSION,
    MANDATORY_EVIDENCE_KEYS,
    MAX_EVIDENCE_BYTES,
    PROVENANCE_VERSION,
    EvidenceTooLargeError,
    build_config_snapshot,
    build_evidence_snapshot,
    build_provenance,
    canonical_json,
    config_hash,
    market_data_as_of_from_df,
    sanitize_config,
)


# --------------------------------------------------------------------------- #
# Config hashing
# --------------------------------------------------------------------------- #

def test_config_hash_independent_of_dict_key_order():
    a = {"score_threshold": 0.5, "min_price": 5.0, "nested": {"x": 1, "y": 2}}
    b = {"nested": {"y": 2, "x": 1}, "min_price": 5.0, "score_threshold": 0.5}
    assert config_hash(a) == config_hash(b)


def test_config_change_produces_different_hash():
    a = {"score_threshold": 0.5, "min_price": 5.0}
    b = {"score_threshold": 0.6, "min_price": 5.0}
    assert config_hash(a) != config_hash(b)


def test_canonical_json_sorted_and_compact():
    s = canonical_json({"b": 1, "a": [3, 1, 2]})
    assert s == '{"a":[3,1,2],"b":1}'  # keys sorted, list order preserved


def test_list_order_is_semantic_and_preserved():
    assert config_hash({"tf": ["1d", "4h"]}) != config_hash({"tf": ["4h", "1d"]})


# --------------------------------------------------------------------------- #
# Secret exclusion
# --------------------------------------------------------------------------- #

def test_secret_shaped_keys_are_excluded():
    cfg = {
        "score_threshold": 0.5,
        "MASSIVE_API_KEY": "sk-123",
        "worker_token": "tok",
        "db_password": "pw",
        "Authorization": "Bearer abc",
        "SUPABASE_DSN": "postgresql://u:p@h/db",
        "nested": {"api_key": "x", "min_price": 5.0},
    }
    clean = sanitize_config(cfg)
    assert clean == {"score_threshold": 0.5, "nested": {"min_price": 5.0}}


def test_secret_shaped_string_values_are_masked():
    cfg = {"base_url": "https://api.example.com?apiKey=secret123"}
    assert sanitize_config(cfg) == {"base_url": "***excluded***"}


def test_snapshot_and_hash_share_sanitization():
    with_secret = {"min_price": 5.0, "api_key": "sk-1"}
    without = {"min_price": 5.0}
    # The secret never influences the hash (it is stripped before hashing).
    assert config_hash(with_secret) == config_hash(without)
    snap = build_config_snapshot(with_secret, {"worker_token": "t", "limit": 10})
    assert "api_key" not in snap["strategy_config"]
    assert "worker_token" not in snap["scanner"]
    assert snap["scanner"]["limit"] == 10


# --------------------------------------------------------------------------- #
# Evidence snapshot
# --------------------------------------------------------------------------- #

def test_evidence_snapshot_copies_existing_deterministic_evidence():
    details = {
        "score_components": {"proximity_to_sma150_pct": 1.2},
        "thresholds_used": {"score_threshold": 0.5},
        "bounces_detail": [{"date": "2026-01-02", "rebound_pct": 6.1}],
        "trend_context": "up",
        "snapshot_date": "2026-01-05",
        "unrelated_noise": "kept out",
        "decision_card": {
            "raw_evidence": {"volume_ratio": 1.4},
            "missing_data": ["4h"],
            "trigger_needed": "4H confirmation",
            "card_version": "decision_card.v1",
        },
    }
    snap, meta = build_evidence_snapshot(details)
    assert snap["score_components"] == {"proximity_to_sma150_pct": 1.2}
    assert snap["thresholds_used"] == {"score_threshold": 0.5}
    assert snap["bounces_detail"][0]["rebound_pct"] == 6.1
    assert snap["trend_context"] == "up"
    assert snap["decision_card_evidence"]["raw_evidence"] == {"volume_ratio": 1.4}
    assert snap["decision_card_evidence"]["missing_data"] == ["4h"]
    # Non-evidence keys are not copied wholesale.
    assert "unrelated_noise" not in snap
    assert meta["evidence_pruned"] is False
    assert meta["evidence_pruned_keys"] == []


def test_evidence_snapshot_does_not_invent_missing_fields():
    snap, _ = build_evidence_snapshot({"snapshot_date": "2026-01-05"})
    assert snap == {"snapshot_date": "2026-01-05"}
    assert "score_components" not in snap
    assert "bounces_detail" not in snap


def _huge_details():
    return {
        "bounces_detail": [{"i": i, "pad": "x" * 100} for i in range(5000)],
        "trend_context": "up",
        "score_components": {"trend": 0.8},
        "thresholds_used": {"score_threshold": 0.5},
        "snapshot_date": "2026-01-05",
    }


def test_evidence_snapshot_is_bounded_and_pruning_is_recorded():
    snap, meta = build_evidence_snapshot(_huge_details())
    size = len(canonical_json(snap).encode("utf-8"))
    assert size <= MAX_EVIDENCE_BYTES
    assert meta["evidence_pruned"] is True
    assert "bounces_detail" in meta["evidence_pruned_keys"]
    assert snap["trend_context"] == "up"  # small optional keys survive
    # Reproducibility facts about the ORIGINAL snapshot are recorded.
    assert meta["evidence_original_size_bytes"] > MAX_EVIDENCE_BYTES
    assert len(meta["evidence_original_sha256"]) == 64


def test_mandatory_evidence_survives_pruning():
    snap, meta = build_evidence_snapshot(_huge_details())
    assert meta["evidence_pruned"] is True
    # Core decision inputs may never be pruned when present.
    assert snap["score_components"] == {"trend": 0.8}
    assert snap["thresholds_used"] == {"score_threshold": 0.5}
    assert snap["snapshot_date"] == "2026-01-05"
    assert not set(meta["evidence_pruned_keys"]) & MANDATORY_EVIDENCE_KEYS


def test_original_hash_computed_before_pruning_distinguishes_pruned_content():
    """Payloads differing only inside a later-pruned optional list keep
    distinct original hashes (identity source), even though the stored
    snapshots become identical."""
    a = dict(_huge_details())
    b = dict(_huge_details())
    b["bounces_detail"] = [{**row, "marker": "B"} for row in b["bounces_detail"]]

    snap_a, meta_a = build_evidence_snapshot(a)
    snap_b, meta_b = build_evidence_snapshot(b)

    assert canonical_json(snap_a) == canonical_json(snap_b)  # stored: identical
    assert meta_a["evidence_original_sha256"] != meta_b["evidence_original_sha256"]


def test_original_hash_is_key_order_independent():
    a = {"score_components": {"trend": 0.8, "volume": 0.5},
         "thresholds_used": {"score_threshold": 0.5}}
    b = {"thresholds_used": {"score_threshold": 0.5},
         "score_components": {"volume": 0.5, "trend": 0.8}}
    _, meta_a = build_evidence_snapshot(a)
    _, meta_b = build_evidence_snapshot(b)
    assert meta_a["evidence_original_sha256"] == meta_b["evidence_original_sha256"]
    assert meta_a["evidence_original_size_bytes"] == meta_b["evidence_original_size_bytes"]


def test_pruning_is_deterministic():
    a_snap, a_meta = build_evidence_snapshot(_huge_details())
    b_snap, b_meta = build_evidence_snapshot(_huge_details())
    assert canonical_json(a_snap) == canonical_json(b_snap)
    assert a_meta == b_meta
    assert a_meta["evidence_pruned_keys"] == b_meta["evidence_pruned_keys"]


def test_oversized_mandatory_evidence_is_rejected():
    """When even the mandatory decision inputs exceed the bound, refuse —
    never silently store a snapshot missing its core decision inputs."""
    details = {"score_components": {"pad": "x" * (70 * 1024)}}
    with pytest.raises(EvidenceTooLargeError):
        build_evidence_snapshot(details)


# --------------------------------------------------------------------------- #
# Market-data as-of
# --------------------------------------------------------------------------- #

def _df(dates):
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1,
    })


def test_as_of_uses_latest_evaluated_bar_not_now():
    df = _df(["2026-07-01", "2026-07-02", "2026-07-03"])
    as_of = market_data_as_of_from_df(df)
    assert as_of == datetime(2026, 7, 3, tzinfo=timezone.utc)
    # Definitely NOT the current time.
    assert abs((datetime.now(timezone.utc) - as_of).days) > 1


def test_as_of_is_timezone_aware_utc():
    as_of = market_data_as_of_from_df(_df(["2026-07-03"]))
    assert as_of.tzinfo is not None
    assert as_of.utcoffset().total_seconds() == 0


def test_as_of_none_for_untrustworthy_data():
    assert market_data_as_of_from_df(None) is None
    assert market_data_as_of_from_df(pd.DataFrame()) is None
    assert market_data_as_of_from_df(pd.DataFrame({"close": [1.0]})) is None


# --------------------------------------------------------------------------- #
# Full record + version separation
# --------------------------------------------------------------------------- #

def _record(**overrides):
    kwargs = dict(
        scan_run_id="7c9f3e1a-0000-0000-0000-000000000001",
        source_path="funnel",
        scanner_mode="funnel",
        provider="massive",
        strategy_code="sma150_bounce",
        strategy_version="sma150.v2",
        strategy_config={"min_price": 5.0},
        scanner_settings={"requested_limit": 10},
        details={"snapshot_date": "2026-07-03"},
        market_data_as_of=datetime(2026, 7, 3, tzinfo=timezone.utc),
    )
    kwargs.update(overrides)
    return build_provenance(**kwargs)


def test_build_provenance_versions_are_separate_concepts():
    rec = _record()
    assert rec["strategy_version"] == "sma150.v2"
    assert rec["decision_policy_version"] == DECISION_POLICY_VERSION == "strategy_decision.v1"
    assert rec["provenance_version"] == PROVENANCE_VERSION == "provenance.v1"
    # None of these identities may collapse into one another.
    assert len({rec["strategy_version"], rec["decision_policy_version"],
                rec["provenance_version"]}) == 3


def test_external_observation_ids_default_empty_never_faked():
    rec = _record()
    assert rec["external_observation_ids"] == []


def test_missing_as_of_stores_null_and_explicit_reason():
    rec = _record(market_data_as_of=None)
    assert rec["market_data_as_of"] is None
    assert "market_data_as_of_missing_reason" in rec["evidence_snapshot"]


def test_provenance_config_hash_matches_snapshot():
    rec = _record()
    assert rec["config_hash"] == config_hash(rec["config_snapshot"])
    # Changing strategy config changes the hash.
    other = _record(strategy_config={"min_price": 6.0})
    assert other["config_hash"] != rec["config_hash"]


def test_provenance_carries_evidence_pruning_metadata():
    rec = _record()
    assert rec["evidence_pruned"] is False
    assert rec["evidence_pruned_keys"] == []
    assert len(rec["evidence_original_sha256"]) == 64
    assert rec["evidence_original_size_bytes"] > 0
