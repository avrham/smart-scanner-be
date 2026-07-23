"""wyckoff_mtf.v2 Phase 9A/9B/9C1 package.

Phase 9A: readiness, aggregation, range detection.
Phase 9B: HTF context, effort-result, event candidates, structure/phases.
Phase 9C1: 4H trigger, policy, evidence mapping, strategy orchestration.

WyckoffMTFV2Strategy is exported for import but is NOT registered.
"""

from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import (
    AGGREGATION_VERSION,
    COMPLETED_BAR_POLICY,
    DEFAULT_CONFIG,
    DECISION_POLICY_VERSION,
    EFFORT_RESULT_VERSION,
    EVENT_CANDIDATE_VERSION,
    EVENT_DETECTION_VERSION,
    EVENT_STATUS_RETENTION_ORDER,
    EVIDENCE_VERSION,
    FOUR_HOUR_TRIGGER_VERSION,
    HTF_CONTEXT_VERSION,
    INVALIDATION_VERSION,
    MAX_CANDIDATE_ATTEMPTS,
    PHASE_CANDIDATE_VERSION,
    PHASE_CLASSIFICATION_VERSION,
    RANGE_DETECTION_VERSION,
    RANKING_VERSION,
    READINESS_VERSION,
    STRATEGY_CODE,
    STRATEGY_FAMILY,
    STRATEGY_VERSION,
    Phase9AConfigError,
    Phase9BConfigError,
    default_config,
    event_key,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.context_htf import measure_htf_context
from app.workers.strategies.wyckoff_v2.effort_result import (
    EffortResultError,
    measure_effort_result_at_index,
    measure_effort_results,
)
from app.workers.strategies.wyckoff_v2.events import (
    EventDetectionError,
    detect_event_candidates,
)
from app.workers.strategies.wyckoff_v2.phases import (
    PhaseClassificationError,
    classify_phases,
    classify_structure,
)
from app.workers.strategies.wyckoff_v2.policy import (
    PHASE_ORDINAL,
    compute_invalidation,
    compute_ranking,
    evaluate_policy,
)
from app.workers.strategies.wyckoff_v2.ranges import detect_trading_ranges
from app.workers.strategies.wyckoff_v2.readiness import (
    CanonicalDailyError,
    assess_data_readiness,
    derive_history_requirement,
    normalize_canonical_daily,
)
from app.workers.strategies.wyckoff_v2.strategy import WyckoffMTFV2Strategy
from app.workers.strategies.wyckoff_v2.trigger_4h import (
    FourHourTriggerError,
    analyze_4h_trigger,
    normalize_4h_ohlcv,
)

__all__ = [
    "AGGREGATION_VERSION",
    "COMPLETED_BAR_POLICY",
    "CanonicalDailyError",
    "DEFAULT_CONFIG",
    "DECISION_POLICY_VERSION",
    "EFFORT_RESULT_VERSION",
    "EVENT_CANDIDATE_VERSION",
    "EVENT_DETECTION_VERSION",
    "EVENT_STATUS_RETENTION_ORDER",
    "EVIDENCE_VERSION",
    "EventDetectionError",
    "EffortResultError",
    "FOUR_HOUR_TRIGGER_VERSION",
    "FourHourTriggerError",
    "HTF_CONTEXT_VERSION",
    "INVALIDATION_VERSION",
    "MAX_CANDIDATE_ATTEMPTS",
    "PHASE_CANDIDATE_VERSION",
    "PHASE_CLASSIFICATION_VERSION",
    "PHASE_ORDINAL",
    "Phase9AConfigError",
    "Phase9BConfigError",
    "PhaseClassificationError",
    "RANGE_DETECTION_VERSION",
    "RANKING_VERSION",
    "READINESS_VERSION",
    "STRATEGY_CODE",
    "STRATEGY_FAMILY",
    "STRATEGY_VERSION",
    "WyckoffMTFV2Strategy",
    "aggregate_completed_timeframes",
    "analyze_4h_trigger",
    "assess_data_readiness",
    "classify_phases",
    "classify_structure",
    "compute_invalidation",
    "compute_ranking",
    "default_config",
    "detect_event_candidates",
    "detect_trading_ranges",
    "derive_history_requirement",
    "evaluate_policy",
    "event_key",
    "measure_effort_result_at_index",
    "measure_effort_results",
    "measure_htf_context",
    "normalize_4h_ohlcv",
    "normalize_canonical_daily",
    "resolve_config",
]
