"""wyckoff_mtf.v2 — approved Phase 9 version identities and Phase 9A defaults.

These identities are DECLARED here (approved in the Phase 9 audit) but are
NOT registered, persisted or exposed anywhere yet. Registration, the decision
policy and evidence mapping arrive in Phase 9C.

Every behavior-affecting threshold lives in DEFAULT_CONFIG and is
overrideable through an explicit config dictionary — no module-level
constant outside the resolved config may change a calculation. The values
are deterministic engineering defaults, not tuned parameters and not claims
of effectiveness.
"""

from typing import Any, Dict, Optional

from app.workers.strategies.bar_completion import BAR_COMPLETION_POLICY

import math

# ---- Approved Phase 9 identities (declared only; nothing registers them) -- #
STRATEGY_CODE = "wyckoff_mtf_v2"
STRATEGY_FAMILY = "wyckoff_mtf"
STRATEGY_VERSION = "wyckoff_mtf.v2"
DECISION_POLICY_VERSION = "wyckoff_mtf.policy.v1"
RANGE_DETECTION_VERSION = "wyckoff_range.v1"
EVENT_DETECTION_VERSION = "wyckoff_events.v1"
PHASE_CLASSIFICATION_VERSION = "wyckoff_phases.v1"
RANKING_VERSION = "wyckoff_mtf.v2.rank.v1"
COMPLETED_BAR_POLICY = BAR_COMPLETION_POLICY  # "ny_session_close.v1"
EVIDENCE_VERSION = "evidence.v1"

# Phase 9A sub-contract identities (versioned so a future change to the
# readiness or aggregation semantics can never be confused with these).
READINESS_VERSION = "wyckoff_readiness.v1"
AGGREGATION_VERSION = "wyckoff_aggregation.v1"
RANGE_CANDIDATE_VERSION = RANGE_DETECTION_VERSION

# Phase 9B sub-contract identities (declared only; nothing registers them).
HTF_CONTEXT_VERSION = "wyckoff_htf_context.v1"
EFFORT_RESULT_VERSION = "wyckoff_effort_result.v1"
EVENT_CANDIDATE_VERSION = EVENT_DETECTION_VERSION  # wyckoff_events.v1
PHASE_CANDIDATE_VERSION = PHASE_CLASSIFICATION_VERSION  # wyckoff_phases.v1

# ---- Readiness status vocabulary (never represented as a zero score) ------ #
STATUS_READY = "ready"
STATUS_INSUFFICIENT_HISTORY = "insufficient_history"
STATUS_MISSING_VOLUME = "missing_volume"
STATUS_UNCONFIRMED_BAR_COMPLETION = "unconfirmed_bar_completion"
STATUS_UNSUPPORTED_TIMEFRAME = "unsupported_timeframe"
STATUS_UNKNOWN = "unknown"

READINESS_STATUSES = frozenset({
    STATUS_READY,
    STATUS_INSUFFICIENT_HISTORY,
    STATUS_MISSING_VOLUME,
    STATUS_UNCONFIRMED_BAR_COMPLETION,
    STATUS_UNSUPPORTED_TIMEFRAME,
    STATUS_UNKNOWN,
})

# ---- Approved Phase 9A engineering defaults -------------------------------- #
DEFAULT_CONFIG: Dict[str, Any] = {
    # Higher-timeframe context requirements (completed periods only).
    "monthly_sma_window": 20,
    "monthly_slope_lookback": 3,
    "monthly_min_periods": 24,
    "weekly_sma_window": 20,
    "weekly_slope_lookback": 4,
    "weekly_min_periods": 26,
    # Daily measurement windows.
    "atr_window": 14,
    "volume_baseline_window": 20,
    "max_missing_volume_fraction": 0.20,
    # Trading-range candidate generation.
    "range_min_bars": 20,
    "range_max_bars": 120,
    "range_length_step": 5,
    "range_end_lookback_bars": 20,
    "range_end_step": 1,
    # Support/resistance zones (bounded quantile zones, never single bars).
    "support_quantile_low": 0.05,
    "support_quantile_high": 0.15,
    "resistance_quantile_low": 0.85,
    "resistance_quantile_high": 0.95,
    "quantile_interpolation": "linear",
    # Range validity gates.
    "range_min_atr_multiple": 3.0,
    "range_max_atr_multiple": 12.0,
    "min_support_touch_clusters": 2,
    "min_resistance_touch_clusters": 2,
    "min_touch_separation_bars": 3,
    "min_containment_fraction": 0.80,
    "max_breakout_contamination_fraction": 0.20,
    "min_range_volume_coverage": 0.80,
    # Width-stability quality (ranking evidence only, never a gate).
    "range_stability_window_bars": 10,
    "range_stability_step_bars": 5,
    "max_width_coefficient_of_variation": 0.50,
    # History request planning (conservative targets, not proof of coverage).
    "history_request_trading_days_per_month": 23,
    "history_request_trading_days_per_week": 5,
    "history_request_margin_bars": 10,
    "completed_bar_exclusion_margin": 1,
    # Completed-daily-bar policy (shared ny_session_close.v1; versioned and
    # part of the resolved config so session semantics are config-frozen).
    "bar_completion_policy": COMPLETED_BAR_POLICY,
    "exchange_timezone": "America/New_York",
    "session_close_time": "16:00",
    # ---- Phase 9B: HTF context -------------------------------------------- #
    "monthly_slope_reference_pct": 2.0,
    "weekly_slope_reference_pct": 1.5,
    "monthly_structure_window_periods": 4,
    "weekly_structure_window_periods": 6,
    "htf_structure_tolerance_pct": 0.0,
    # ---- Phase 9B: effort-result ------------------------------------------ #
    "event_atr_window": 14,
    "event_volume_baseline_window": 20,
    "event_min_volume_baseline_bars": 15,
    "effort_high_volume_ratio": 1.50,
    "effort_low_volume_ratio": 0.80,
    "result_high_atr_ratio": 1.00,
    "result_low_atr_ratio": 0.35,
    "wide_spread_atr_ratio": 1.20,
    "climax_spread_atr_ratio": 1.50,
    "narrow_spread_atr_ratio": 0.80,
    # ---- Phase 9B: range relationships ------------------------------------ #
    "event_zone_approach_atr_multiple": 0.50,
    "event_pierce_atr_multiple": 0.10,
    "event_breakout_buffer_atr_multiple": 0.05,
    "event_retest_tolerance_atr_multiple": 0.25,
    "event_invalidation_buffer_atr_multiple": 0.10,
    # ---- Phase 9B: close location ----------------------------------------- #
    "accumulation_close_off_low_min": 0.55,
    "distribution_close_off_high_max": 0.45,
    "bullish_close_location_min": 0.65,
    "bearish_close_location_max": 0.35,
    # ---- Phase 9B: sequence / confirmation -------------------------------- #
    "event_confirmation_window_bars": 3,
    "automatic_rally_window_bars": 10,
    "secondary_test_min_separation_bars": 3,
    "secondary_test_max_bars_after_climax": 40,
    "test_max_bars_after_spring": 20,
    "lps_max_bars_after_sos": 20,
    "lpsy_max_bars_after_sow": 20,
    "phase_b_min_range_bars": 30,
    "phase_e_hold_bars": 2,
    # ---- Phase 9B: candidate bounds --------------------------------------- #
    "max_event_candidates_per_code": 10,
    "max_total_event_candidates": 120,
    # ---- Phase 9B: structure ---------------------------------------------- #
    "min_structure_confirmed_event_types": 2,
}

# The exact config subset that participates in the range-candidate identity
# fingerprint (sorted at serialization; key order can never matter).
# NOTE: post_range_bar_count is intentionally NOT part of the fingerprint —
# post-range bars must not change candidate identity (adversarial 9A contract).
RANGE_CONFIG_KEYS = (
    "atr_window",
    "max_breakout_contamination_fraction",
    "max_width_coefficient_of_variation",
    "min_containment_fraction",
    "min_range_volume_coverage",
    "min_resistance_touch_clusters",
    "min_support_touch_clusters",
    "min_touch_separation_bars",
    "quantile_interpolation",
    "range_end_lookback_bars",
    "range_end_step",
    "range_length_step",
    "range_max_atr_multiple",
    "range_max_bars",
    "range_min_atr_multiple",
    "range_min_bars",
    "range_stability_step_bars",
    "range_stability_window_bars",
    "resistance_quantile_high",
    "resistance_quantile_low",
    "support_quantile_high",
    "support_quantile_low",
)

# Operational safety bound on the Cartesian candidate grid. Defaults produce
# at most 21 end offsets × 21 lengths = 441 attempts; this cap rejects
# runaway overrides before they hang the process. Not a tuning parameter.
MAX_CANDIDATE_ATTEMPTS = 10_000

SUPPORTED_QUANTILE_INTERPOLATIONS = frozenset({"linear"})

# Event-candidate identity fingerprint keys (sorted at serialization).
# Confirmation status and confidence are intentionally excluded so maturation
# cannot change candidate_id inside a frozen range/config identity.
EVENT_CONFIG_KEYS = (
    "accumulation_close_off_low_min",
    "automatic_rally_window_bars",
    "bearish_close_location_max",
    "bullish_close_location_min",
    "climax_spread_atr_ratio",
    "distribution_close_off_high_max",
    "effort_high_volume_ratio",
    "effort_low_volume_ratio",
    "event_atr_window",
    "event_breakout_buffer_atr_multiple",
    "event_confirmation_window_bars",
    "event_invalidation_buffer_atr_multiple",
    "event_min_volume_baseline_bars",
    "event_pierce_atr_multiple",
    "event_retest_tolerance_atr_multiple",
    "event_volume_baseline_window",
    "event_zone_approach_atr_multiple",
    "lps_max_bars_after_sos",
    "lpsy_max_bars_after_sow",
    "narrow_spread_atr_ratio",
    "result_high_atr_ratio",
    "result_low_atr_ratio",
    "secondary_test_max_bars_after_climax",
    "secondary_test_min_separation_bars",
    "test_max_bars_after_spring",
    "wide_spread_atr_ratio",
)

# Retention / bounding status order (lower = kept preferentially).
# Do not rely on alphabetical status strings.
EVENT_STATUS_RETENTION_ORDER = (
    "confirmed",
    "candidate",
    "confirmation_pending",
    "unknown",
    "contradicted",
)

# Phase E gate codes (not event detector codes).
GATE_PHASE_E_HOLD_ABOVE_RESISTANCE = "phase_e_hold_above_resistance"
GATE_PHASE_E_HOLD_BELOW_SUPPORT = "phase_e_hold_below_support"


def event_key(family: str, event_code: str) -> str:
    """Canonical family-qualified event grouping key: ``family:event_code``."""
    return f"{family}:{event_code}"


class Phase9AConfigError(ValueError):
    """Deterministic rejection of an invalid Phase 9A/9B config override."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


# Alias for Phase 9B callers; same rejection contract.
Phase9BConfigError = Phase9AConfigError


def default_config() -> Dict[str, Any]:
    """A fresh copy of the Phase 9A/9B default configuration contract."""
    return dict(DEFAULT_CONFIG)


def _require_finite_number(value: Any, name: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError) as exc:
        raise Phase9AConfigError(
            "invalid_range_config", f"{name} is not numeric"
        ) from exc
    if not (f == f) or f == float("inf") or f == float("-inf"):
        raise Phase9AConfigError(
            "invalid_range_config", f"{name} is non-finite"
        )
    return f


def _require_positive_int(config: Dict[str, Any], name: str) -> int:
    v = int(_require_finite_number(config[name], name))
    if v <= 0:
        raise Phase9AConfigError("invalid_event_config", f"{name} <= 0")
    return v


def _require_non_negative(config: Dict[str, Any], name: str) -> float:
    v = _require_finite_number(config[name], name)
    if v < 0:
        raise Phase9AConfigError("invalid_event_config", f"{name} negative")
    return v


def _require_unit_interval(config: Dict[str, Any], name: str) -> float:
    v = _require_finite_number(config[name], name)
    if not (0.0 <= v <= 1.0):
        raise Phase9AConfigError("invalid_event_config", f"{name} outside [0,1]")
    return v


def validate_phase9b_config(config: Dict[str, Any]) -> None:
    """Reject malformed Phase 9B config. Prefer rejection over silent clamping."""
    for name in (
        "monthly_structure_window_periods",
        "weekly_structure_window_periods",
        "event_atr_window",
        "event_volume_baseline_window",
        "event_min_volume_baseline_bars",
        "event_confirmation_window_bars",
        "automatic_rally_window_bars",
        "secondary_test_min_separation_bars",
        "secondary_test_max_bars_after_climax",
        "test_max_bars_after_spring",
        "lps_max_bars_after_sos",
        "lpsy_max_bars_after_sow",
        "phase_b_min_range_bars",
        "phase_e_hold_bars",
        "max_event_candidates_per_code",
        "max_total_event_candidates",
        "min_structure_confirmed_event_types",
    ):
        _require_positive_int(config, name)

    for name in (
        "monthly_slope_reference_pct",
        "weekly_slope_reference_pct",
        "effort_high_volume_ratio",
        "effort_low_volume_ratio",
        "result_high_atr_ratio",
        "result_low_atr_ratio",
        "wide_spread_atr_ratio",
        "climax_spread_atr_ratio",
        "narrow_spread_atr_ratio",
    ):
        v = _require_finite_number(config[name], name)
        if v <= 0:
            raise Phase9AConfigError("invalid_event_config", f"{name} <= 0")

    for name in (
        "htf_structure_tolerance_pct",
        "event_zone_approach_atr_multiple",
        "event_pierce_atr_multiple",
        "event_breakout_buffer_atr_multiple",
        "event_retest_tolerance_atr_multiple",
        "event_invalidation_buffer_atr_multiple",
    ):
        _require_non_negative(config, name)

    for name in (
        "accumulation_close_off_low_min",
        "distribution_close_off_high_max",
        "bullish_close_location_min",
        "bearish_close_location_max",
    ):
        _require_unit_interval(config, name)

    high_vol = _require_finite_number(
        config["effort_high_volume_ratio"], "effort_high_volume_ratio"
    )
    low_vol = _require_finite_number(
        config["effort_low_volume_ratio"], "effort_low_volume_ratio"
    )
    if high_vol < low_vol:
        raise Phase9AConfigError(
            "invalid_event_config",
            "effort_high_volume_ratio below effort_low_volume_ratio",
        )

    high_res = _require_finite_number(
        config["result_high_atr_ratio"], "result_high_atr_ratio"
    )
    low_res = _require_finite_number(
        config["result_low_atr_ratio"], "result_low_atr_ratio"
    )
    if high_res < low_res:
        raise Phase9AConfigError(
            "invalid_event_config",
            "result_high_atr_ratio below result_low_atr_ratio",
        )

    climax = _require_finite_number(
        config["climax_spread_atr_ratio"], "climax_spread_atr_ratio"
    )
    wide = _require_finite_number(
        config["wide_spread_atr_ratio"], "wide_spread_atr_ratio"
    )
    narrow = _require_finite_number(
        config["narrow_spread_atr_ratio"], "narrow_spread_atr_ratio"
    )
    if climax < wide:
        raise Phase9AConfigError(
            "invalid_event_config",
            "climax_spread_atr_ratio below wide_spread_atr_ratio",
        )
    if wide < narrow:
        raise Phase9AConfigError(
            "invalid_event_config",
            "wide_spread_atr_ratio below narrow_spread_atr_ratio",
        )

    min_sep = int(
        _require_finite_number(
            config["secondary_test_min_separation_bars"],
            "secondary_test_min_separation_bars",
        )
    )
    max_after = int(
        _require_finite_number(
            config["secondary_test_max_bars_after_climax"],
            "secondary_test_max_bars_after_climax",
        )
    )
    if max_after < min_sep:
        raise Phase9AConfigError(
            "invalid_event_config",
            "secondary_test_max_bars_after_climax below min separation",
        )

    min_vol_bars = int(
        _require_finite_number(
            config["event_min_volume_baseline_bars"],
            "event_min_volume_baseline_bars",
        )
    )
    vol_window = int(
        _require_finite_number(
            config["event_volume_baseline_window"],
            "event_volume_baseline_window",
        )
    )
    if min_vol_bars > vol_window:
        raise Phase9AConfigError(
            "invalid_event_config",
            "event_min_volume_baseline_bars above event_volume_baseline_window",
        )

    per_code = int(
        _require_finite_number(
            config["max_event_candidates_per_code"],
            "max_event_candidates_per_code",
        )
    )
    total = int(
        _require_finite_number(
            config["max_total_event_candidates"],
            "max_total_event_candidates",
        )
    )
    if total < per_code:
        raise Phase9AConfigError(
            "invalid_event_config",
            "max_total_event_candidates below max_event_candidates_per_code",
        )


def validate_phase9a_config(config: Dict[str, Any]) -> None:
    """Reject malformed config before any candidate generation.

    Prefer rejection over silent clamping. Raises Phase9AConfigError with a
    deterministic reason_code.
    """
    range_min = int(_require_finite_number(config["range_min_bars"], "range_min_bars"))
    range_max = int(_require_finite_number(config["range_max_bars"], "range_max_bars"))
    length_step = int(
        _require_finite_number(config["range_length_step"], "range_length_step")
    )
    end_lookback = int(
        _require_finite_number(
            config["range_end_lookback_bars"], "range_end_lookback_bars"
        )
    )
    end_step = int(
        _require_finite_number(config["range_end_step"], "range_end_step")
    )
    atr_window = int(_require_finite_number(config["atr_window"], "atr_window"))
    vol_window = int(
        _require_finite_number(
            config["volume_baseline_window"], "volume_baseline_window"
        )
    )
    stab_window = int(
        _require_finite_number(
            config["range_stability_window_bars"], "range_stability_window_bars"
        )
    )
    stab_step = int(
        _require_finite_number(
            config["range_stability_step_bars"], "range_stability_step_bars"
        )
    )
    min_sep = int(
        _require_finite_number(
            config["min_touch_separation_bars"], "min_touch_separation_bars"
        )
    )

    if range_min <= 0:
        raise Phase9AConfigError("invalid_range_config", "range_min_bars <= 0")
    if range_max < range_min:
        raise Phase9AConfigError(
            "invalid_range_config", "range_max_bars < range_min_bars"
        )
    if length_step <= 0:
        raise Phase9AConfigError("invalid_range_config", "range_length_step <= 0")
    if end_lookback < 0:
        raise Phase9AConfigError(
            "invalid_range_config", "range_end_lookback_bars < 0"
        )
    if end_step <= 0:
        raise Phase9AConfigError("invalid_range_config", "range_end_step <= 0")
    if atr_window <= 0:
        raise Phase9AConfigError("invalid_range_config", "atr_window <= 0")
    if vol_window <= 0:
        raise Phase9AConfigError(
            "invalid_range_config", "volume_baseline_window <= 0"
        )
    if stab_window <= 0:
        raise Phase9AConfigError(
            "invalid_range_config", "range_stability_window_bars <= 0"
        )
    if stab_step <= 0:
        raise Phase9AConfigError(
            "invalid_range_config", "range_stability_step_bars <= 0"
        )
    if min_sep <= 0:
        raise Phase9AConfigError(
            "invalid_range_config", "min_touch_separation_bars <= 0"
        )

    sq_lo = _require_finite_number(
        config["support_quantile_low"], "support_quantile_low"
    )
    sq_hi = _require_finite_number(
        config["support_quantile_high"], "support_quantile_high"
    )
    rq_lo = _require_finite_number(
        config["resistance_quantile_low"], "resistance_quantile_low"
    )
    rq_hi = _require_finite_number(
        config["resistance_quantile_high"], "resistance_quantile_high"
    )
    for name, q in (
        ("support_quantile_low", sq_lo),
        ("support_quantile_high", sq_hi),
        ("resistance_quantile_low", rq_lo),
        ("resistance_quantile_high", rq_hi),
    ):
        if not (0.0 <= q <= 1.0):
            raise Phase9AConfigError(
                "invalid_quantile_config", f"{name} outside [0,1]"
            )
    if sq_lo > sq_hi:
        raise Phase9AConfigError(
            "invalid_quantile_config",
            "support_quantile_low > support_quantile_high",
        )
    if rq_lo > rq_hi:
        raise Phase9AConfigError(
            "invalid_quantile_config",
            "resistance_quantile_low > resistance_quantile_high",
        )
    # Support zone must sit below resistance zone in quantile space.
    if sq_hi > rq_lo:
        raise Phase9AConfigError(
            "invalid_quantile_config",
            "support-zone quantiles inconsistent with resistance-zone quantiles",
        )

    for name in (
        "max_missing_volume_fraction",
        "min_containment_fraction",
        "max_breakout_contamination_fraction",
        "min_range_volume_coverage",
        "max_width_coefficient_of_variation",
    ):
        v = _require_finite_number(config[name], name)
        if not (0.0 <= v <= 1.0):
            raise Phase9AConfigError(
                "invalid_range_config", f"{name} outside [0,1]"
            )

    min_atr = _require_finite_number(
        config["range_min_atr_multiple"], "range_min_atr_multiple"
    )
    max_atr = _require_finite_number(
        config["range_max_atr_multiple"], "range_max_atr_multiple"
    )
    if min_atr < 0:
        raise Phase9AConfigError(
            "invalid_range_config", "range_min_atr_multiple negative"
        )
    if max_atr < min_atr:
        raise Phase9AConfigError(
            "invalid_range_config",
            "range_max_atr_multiple below range_min_atr_multiple",
        )

    interp = str(config.get("quantile_interpolation", "linear"))
    if interp not in SUPPORTED_QUANTILE_INTERPOLATIONS:
        raise Phase9AConfigError(
            "invalid_quantile_config",
            f"unsupported quantile_interpolation: {interp!r}",
        )

    # Bound the Cartesian candidate grid before generation.
    n_ends = (end_lookback // end_step) + 1
    n_lengths = ((range_max - range_min) // length_step) + 1
    attempts = n_ends * n_lengths
    if attempts > MAX_CANDIDATE_ATTEMPTS:
        raise Phase9AConfigError(
            "candidate_generation_limit_exceeded",
            f"candidate grid {attempts} exceeds hard cap {MAX_CANDIDATE_ATTEMPTS}",
        )


def resolve_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Defaults overlaid with an explicit override dictionary (copied).

    Validates the resolved Phase 9A and Phase 9B config. Does not silently
    clamp operator values.
    """
    config = default_config()
    if overrides:
        config.update(overrides)
    validate_phase9a_config(config)
    validate_phase9b_config(config)
    return config
