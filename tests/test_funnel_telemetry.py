"""RejectionTracker cap, config summary, and telemetry assembly shape."""

from datetime import datetime

from app.workers.scanner.funnel import (
    RejectionTracker,
    SCANNER_VERSION,
    assemble_telemetry,
    build_config_summary,
)


def test_sample_rejections_are_capped():
    tracker = RejectionTracker(sample_limit=3)
    for i in range(10):
        tracker.add(f"SYM{i}", "liquidity", "volume_below_min")
    assert len(tracker.samples) == 3           # samples capped
    assert tracker.counts["volume_below_min"] == 10  # counts NOT capped


def test_build_config_summary():
    pattern_config = {
        "min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
        "min_price": 5.0,
        "score_threshold": 0.5,
    }
    scanner_config = {"allow_unknown_volume": False, "max_universe_size": 500}
    summary = build_config_summary(pattern_config, scanner_config, limit=10)
    assert summary["min_market_cap"] == 2e8
    assert summary["min_daily_volume"] == 2e5
    assert summary["min_price"] == 5.0
    assert summary["score_threshold"] == 0.5
    assert summary["allow_unknown_volume"] is False
    assert summary["limit"] == 10


def test_assemble_telemetry_shape():
    tracker = RejectionTracker(sample_limit=25)
    tracker.add("AAA", "liquidity", "volume_below_min")
    stage_counts = {
        "stage_0_universe": 100,
        "stage_1_liquidity_passed": 40,
        "stage_2_prefilter_passed": 30,
        "stage_3_evaluated": 30,
        "enter_count": 2,
        "watch_count": 0,
        "reject_count": 28,
    }
    t = assemble_telemetry(
        pattern_code="sma150_bounce",
        scanner_config={"scanner_version": SCANNER_VERSION},
        config_summary={"min_price": 5.0},
        started_at=datetime(2023, 1, 1, 0, 0, 0),
        finished_at=datetime(2023, 1, 1, 0, 0, 5),
        stage_counts=stage_counts,
        tracker=tracker,
        api_call_counts={"historical_fetches": 30},
        dry_run=False,
        extra_notes=["expensive stages (4H) disabled in Phase 3"],
    )
    assert t["scanner_version"] == SCANNER_VERSION
    assert t["pattern_code"] == "sma150_bounce"
    assert t["universe_count"] == 100
    assert t["stage_counts"]["enter_count"] == 2
    assert t["runtime_seconds"] == 5.0
    assert t["rejection_reason_counts"]["volume_below_min"] == 1
    assert len(t["sample_rejections"]) == 1
    assert t["api_call_counts"]["historical_fetches"] == 30
    assert t["dry_run"] is False
    assert "notes" in t
