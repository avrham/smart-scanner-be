"""Versioned identities, bounds and neutral bands for Phase 8.1B2 outcomes.

Every identity here is a CONTRACT version: changing any semantic (bands,
hash payload shape, coverage rules) requires a NEW version string, never a
silent in-place edit.
"""

# The pure return/excursion math is reused verbatim from outcome.v1; what is
# new in B2 is COVERAGE (one market-path outcome per frozen pair) and the
# canonical forward-frame contract. Each is versioned independently.
CALCULATION_VERSION = "outcome.v1"
OUTCOME_COVERAGE_VERSION = "shadow_pair_outcomes.v1"
OUTCOME_FINGERPRINT_VERSION = "shadow_pair_outcome_fingerprint.v1"
FORWARD_FRAME_VERSION = "shadow_forward_bars.v1"
METRICS_CONTRACT_VERSION = "shadow_pair_resolution_metrics.v1"

# Verdict-neutral reference role: the close of the LAST bar of the frozen B1
# frame_snapshot, identical for ENTER, WATCH and AVOID pairs. An ENTER arm
# may later be INTERPRETED by the metrics layer as the arm that chose
# immediate action, but the shared outcome row is never an executed trade.
REFERENCE_PRICE_ROLE = "paired_decision_observation"

# Bounded forward retrieval: from snapshot_date through
# min(today, snapshot_date + FORWARD_CALENDAR_CAP_DAYS calendar days). Only
# the snapshot-date continuity bar plus the first 20 completed forward bars
# are ever needed; 45 calendar days comfortably covers 20 trading days plus
# weekends and holidays without ever requesting unbounded history.
FORWARD_CALENDAR_CAP_DAYS = 45

# Numeric tolerance for the snapshot-date continuity check: a re-fetched
# close differing from the frozen reference beyond this sets
# reference_revision_detected (the frozen close is NEVER replaced).
REFERENCE_REL_TOL = 1e-9
REFERENCE_ABS_TOL = 1e-8

# Benchmarks fetched from the SAME provider over the SAME bounded range.
BENCHMARK_SYMBOLS = ("SPY", "QQQ")

# Selection bounds for the admin-triggered calculation service.
DEFAULT_CALCULATION_LIMIT = 50
MAX_CALCULATION_LIMIT = 200

# Outcome lifecycle states (see migration 011 CHECK constraint).
STATUS_PENDING = "pending_forward_bars"
STATUS_PARTIAL = "partial"
STATUS_COMPLETE = "complete"
STATUS_ERROR = "error"
OUTCOME_STATUSES = (
    STATUS_PENDING,
    STATUS_PARTIAL,
    STATUS_COMPLETE,
    STATUS_ERROR,
)

# Deterministic rejection codes (bounded, machine-readable, no raw payloads).
REASON_PROVIDER_MISMATCH = "provider_mismatch"
REASON_PROVIDER_RANGE_UNSUPPORTED = "provider_range_unsupported"
# The re-fetched snapshot-date close diverged beyond tolerance: the forward
# price scale is INCOMPATIBLE with the frozen reference (split/revision) —
# no new horizon may be calculated from it. Frozen horizons stay frozen.
REASON_REFERENCE_REVISION = "reference_revision_detected"
# The bounded provider range contains no bar exactly ON snapshot_date:
# reference continuity cannot be confirmed — never substitute the nearest
# date, never move the reference, never calculate from the next bar.
REASON_SNAPSHOT_BAR_MISSING = "snapshot_bar_missing"

# Bounded revision-note storage: notes carry only safe deterministic fields
# (reason_code, horizon, hashes, numeric values, dates) and the list is
# capped so a pathological provider can never grow a row without bound.
MAX_REVISION_NOTES = 40
MAX_REVISION_NOTES_BYTES = 16 * 1024

# Run telemetry / selector JSONB byte bounds (same discipline as B1 runs).
MAX_OUTCOME_TELEMETRY_BYTES = 32 * 1024
MAX_SELECTOR_BYTES = 16 * 1024
MAX_ERROR_TEXT_LEN = 500

# ---------------------------------------------------------------------------
# Neutral resolution bands (shadow_pair_resolution_metrics.v1)
# ---------------------------------------------------------------------------
# METRICS-LAYER constants only: they are NOT strategy thresholds and must
# never affect any verdict. A future band change requires a NEW metrics
# contract version. Values are PERCENT (0.5 == +/-0.5%).
NEUTRAL_BANDS_PCT = {
    1: 0.5,
    3: 0.5,
    5: 1.0,
    10: 1.0,
    20: 1.0,
}

# Disagreements where exactly one arm chose immediate action (ENTER): the
# only categories where per-horizon action-favorability can be classified.
ACTION_DIVERGENT_CATEGORIES = (
    "v2_enter_v3_watch",
    "v2_enter_v3_avoid",
    "v2_watch_v3_enter",
    "v2_avoid_v3_enter",
)

# WATCH-vs-AVOID disagreements: no arm acted, so no arm "winner" can be
# derived — classified as policy_state_disagreement, action_resolvable=false.
POLICY_STATE_CATEGORIES = (
    "v2_watch_v3_avoid",
    "v2_avoid_v3_watch",
)
