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


class Phase9AConfigError(ValueError):
    """Deterministic rejection of an invalid Phase 9A config override."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def default_config() -> Dict[str, Any]:
    """A fresh copy of the Phase 9A default configuration contract."""
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

    Validates the resolved config. Does not silently clamp operator values.
    """
    config = default_config()
    if overrides:
        config.update(overrides)
    validate_phase9a_config(config)
    return config
