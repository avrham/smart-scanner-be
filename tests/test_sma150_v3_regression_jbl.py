"""Phase 8: the known JBL-like regression case.

Geometry: price ~2.3% above a DECLINING SMA-150, three v2 bounce detections
(two of them a nearby cluster), strong historical rebounds, volume ratio
~1.07, no deterministic bullish trigger on the last bar.

sma150.v2 classifies this ENTER (its verdict never checks trend, trigger or
a meaningful volume gate). sma150.v3 must classify it WATCH and name the
confirmation gaps: trend, volume and trigger.
"""

import pytest

from app.workers.patterns.sma150 import evaluate_sma150_bounce
from app.workers.strategies import get_strategy
from app.workers.strategies.base import StrategyContext, StrategyDecision
from tests.sma150_v3_frames import build_jbl_like_frame


@pytest.fixture(scope="module")
def jbl_frame():
    return build_jbl_like_frame(end_prox_pct=2.3, vol_ratio=1.07)


def _v3(df):
    strategy = get_strategy("sma150_bounce_v3")
    return strategy.evaluate(
        df, StrategyContext(symbol="JBLX", pattern_code="sma150_bounce_v3",
                            config=strategy.default_config())
    )


def test_fixture_matches_the_reported_characteristics(jbl_frame):
    v2 = evaluate_sma150_bounce("JBLX", jbl_frame, None)
    details = v2["details"]
    assert details["proximity_pct"] < 3.0                # near the SMA
    assert details["bounce_count"] >= 3                  # incl. clustered events
    assert details["avg_rebound_pct"] >= 5.0
    assert 1.0 <= details["vol_ratio"] < 1.20            # ~1.07: weak volume
    # Downtrend as v3 measures it: the SMA-150 slope is clearly negative.
    # (v2's informational trend_context label uses a 20-bar close polyfit
    # that the final pop biases upward — exactly why v2 misses the risk.)
    v3 = _v3(jbl_frame)
    assert v3.details["sma_slope_pct"] < 0


def test_v2_classifies_enter(jbl_frame):
    v2 = evaluate_sma150_bounce("JBLX", jbl_frame, None)
    assert v2["verdict"] == "ENTER"


def test_v3_classifies_watch_not_enter(jbl_frame):
    result = _v3(jbl_frame)
    assert result.decision == StrategyDecision.WATCH
    assert result.details["evidence"]["setup_state"] == "valid"
    assert result.details["evidence"]["trigger_state"] in (
        "missing", "contradicted"
    )


def test_v3_names_trend_volume_and_trigger_gaps(jbl_frame):
    result = _v3(jbl_frame)
    failed = set(result.details["failed_confirmations"])
    # The reason identifies at least trend, volume or trigger gaps —
    # this fixture actually exposes all three.
    assert {"sma_slope", "trigger_volume_ratio", "bullish_trigger"} <= failed
    assert result.details["sma_slope_pct"] < 0
    assert result.details["vol_ratio"] == pytest.approx(1.07, abs=0.02)
    assert "sma_slope_negative" in result.details["evidence"]["contradictions"]


def test_v3_clusters_the_nearby_v2_events(jbl_frame):
    """The two touches 8 bars apart count as SEPARATE bounces for v2 but form
    ONE independent event for v3 (deterministic representative), and all v3
    events respect the effective separation (>= rebound window + 1)."""
    v2 = evaluate_sma150_bounce("JBLX", jbl_frame, None)
    v2_touches = sorted(b["touch_index"] for b in v2["details"]["bounces_detail"])
    # v2 double-counts a clustered interaction: two touches < 15 bars apart.
    assert any(b - a < 15 for a, b in zip(v2_touches, v2_touches[1:]))

    v3 = _v3(jbl_frame)
    v3_touches = sorted(e["touch_index"] for e in v3.details["bounce_events"])
    # v3 events are truly independent: pairwise separation >= effective
    # separation (max(min_event_separation_bars=15, rebound_window+1=11)).
    assert all(b - a >= 15 for a, b in zip(v3_touches, v3_touches[1:]))
    # The clustered pair collapsed: v3 has exactly one event in that region.
    clustered_region = [t for t in v3_touches if v2_touches[-2] - 5 <= t <= v2_touches[-1] + 5]
    assert len(clustered_region) == 1
    assert v3.details["evidence"]["setup_state"] == "valid"
    assert v3.details["bounce_count"] >= 2


def test_v3_score_present_but_did_not_authorize_enter(jbl_frame):
    result = _v3(jbl_frame)
    assert result.score is not None
    assert 0.0 <= result.score <= 1.0
    assert result.decision == StrategyDecision.WATCH
