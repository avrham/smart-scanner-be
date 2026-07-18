"""score_components must be RAW measured values, not score*weight (B2)."""

from app.workers.patterns.sma150 import evaluate_sma150_bounce, DEFAULT_CONFIG
from tests.helpers import make_ohlcv


EXPECTED_KEYS = {
    "proximity_to_sma150_pct",
    "price_vs_sma150_pct",
    "bounce_count_deduped",
    "avg_rebound_pct",
    "volume_ratio",
}


def _evaluate_flat():
    # 320 flat bars => after 150-bar SMA warmup there are still >=150 rows,
    # and the last close sits exactly on the SMA (near_sma True).
    df = make_ohlcv(320, price=100.0)
    return evaluate_sma150_bounce("TEST", df, DEFAULT_CONFIG.copy())


def test_score_components_have_expected_raw_keys():
    result = _evaluate_flat()
    comps = result["details"]["score_components"]
    assert set(comps.keys()) == EXPECTED_KEYS


def test_score_components_are_raw_not_weighted():
    result = _evaluate_flat()
    details = result["details"]
    comps = details["score_components"]
    score = result["score"]

    # Raw proximity equals the measured distance (also exposed as proximity_pct),
    # NOT score * 0.35.
    assert comps["proximity_to_sma150_pct"] == details["proximity_pct"]
    assert comps["bounce_count_deduped"] == float(details["bounce_count"])

    # Guard against regressing to the old score*weight persistence.
    assert comps["proximity_to_sma150_pct"] != round(score * 0.35, 4)
    assert comps["bounce_count_deduped"] != round(score * 0.30, 4)


def test_score_version_present():
    result = _evaluate_flat()
    assert result["details"]["score_version"] == "sma150.v2"


def test_flat_price_has_zero_distance_and_zero_bounces():
    result = _evaluate_flat()
    comps = result["details"]["score_components"]
    assert comps["proximity_to_sma150_pct"] == 0.0
    # A flat series never leaves the band, so there is no distinct rebound event.
    assert comps["bounce_count_deduped"] == 0.0
