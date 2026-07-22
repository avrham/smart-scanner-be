"""wyckoff_mtf.v2 Phase 9A package — readiness, aggregation, range detection.

Phase 9A only. No strategy registration, no decision policy, no events,
no evidence.v1 mapping, no StrategyResult orchestration.
"""

from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import (
    AGGREGATION_VERSION,
    COMPLETED_BAR_POLICY,
    DEFAULT_CONFIG,
    MAX_CANDIDATE_ATTEMPTS,
    RANGE_DETECTION_VERSION,
    READINESS_VERSION,
    STRATEGY_CODE,
    STRATEGY_FAMILY,
    STRATEGY_VERSION,
    Phase9AConfigError,
    default_config,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.ranges import detect_trading_ranges
from app.workers.strategies.wyckoff_v2.readiness import (
    CanonicalDailyError,
    assess_data_readiness,
    derive_history_requirement,
    normalize_canonical_daily,
)

__all__ = [
    "AGGREGATION_VERSION",
    "COMPLETED_BAR_POLICY",
    "CanonicalDailyError",
    "DEFAULT_CONFIG",
    "MAX_CANDIDATE_ATTEMPTS",
    "Phase9AConfigError",
    "RANGE_DETECTION_VERSION",
    "READINESS_VERSION",
    "STRATEGY_CODE",
    "STRATEGY_FAMILY",
    "STRATEGY_VERSION",
    "aggregate_completed_timeframes",
    "assess_data_readiness",
    "default_config",
    "detect_trading_ranges",
    "derive_history_requirement",
    "normalize_canonical_daily",
    "resolve_config",
]
