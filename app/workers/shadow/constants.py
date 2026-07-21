"""Versioned identities and bounds for Phase 8.1B1 shadow evaluations."""

# Experiment identity (versioned so a future protocol change can never be
# confused with v1 comparisons).
EXPERIMENT_CODE = "sma150_v2_vs_v3"
EXPERIMENT_VERSION = "sma150_shadow.v1"

# Arms: fixed for this experiment. Control is the live v2 strategy; candidate
# is the separately registered v3. Neither is enabled/disabled by the runner.
CONTROL_ARM_CODE = "control_v2"
CANDIDATE_ARM_CODE = "candidate_v3"
CONTROL_PATTERN_CODE = "sma150_bounce"
CANDIDATE_PATTERN_CODE = "sma150_bounce_v3"

# Snapshot / fingerprint algorithm versions.
FRAME_SNAPSHOT_VERSION = "daily_ohlcv_snapshot.v1"
PAIR_FINGERPRINT_VERSION = "shadow_pair_fingerprint.v1"
EVALUATION_FINGERPRINT_VERSION = "shadow_evaluation_fingerprint.v1"

TIMEFRAME = "1d"

# Hard bound on one run's explicit symbol list.
MAX_SHADOW_SYMBOLS = 25

# Canonical history depth is DERIVED per run from both arms' resolved
# configs (see frames.required_history_bars_*): a bar can only participate in
# historical bounce lookback once its SMA is valid, so the full configured
# lookback needs (sma_window - 1) warm-up bars + lookback bars + the current
# evaluated bar — 149 + 365 + 1 = 515 completed bars with defaults. This is
# the documented HARD ceiling on the stored canonical frame regardless of
# config; the derived requirement is capped here, never exceeded. The cap is
# applied before hashing, so the hash always covers exactly the bars both
# arms evaluate.
FRAME_HARD_CAP_BARS = 600
# Extra raw bars requested beyond the derived requirement so the completed
# canonical frame can still reach the target after a partial current-session
# bar is excluded (where the provider has the history). Never fabricates
# missing history.
FRAME_FETCH_MARGIN_BARS = 5

# Snapshot byte bounds. A mandatory snapshot that exceeds its bound REJECTS
# the symbol's pair (a truncated frame could not reproduce the decision).
MAX_FRAME_SNAPSHOT_BYTES = 512 * 1024
MAX_DETAILS_SNAPSHOT_BYTES = 64 * 1024   # same bound as signal evidence
MAX_TELEMETRY_BYTES = 32 * 1024
MAX_ERROR_TEXT_LEN = 500

# Deterministic verdict-combination categories (control first, candidate
# second). Neither label implies improvement or regression.
AGREEMENT_CATEGORIES = (
    "same_enter",
    "same_watch",
    "same_avoid",
)
DISAGREEMENT_CATEGORIES = (
    "v2_enter_v3_watch",
    "v2_enter_v3_avoid",
    "v2_watch_v3_enter",
    "v2_watch_v3_avoid",
    "v2_avoid_v3_enter",
    "v2_avoid_v3_watch",
)
