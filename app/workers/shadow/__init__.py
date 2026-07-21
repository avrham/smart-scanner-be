"""Phase 8.1B1: frozen paired shadow evaluations (sma150.v2 vs sma150.v3).

Admin-triggered, bounded experiment infrastructure. Shadow evaluations are
completely separated from normal signals: they never call save_signal, never
write signals/provenance/outcomes, preserve AVOID decisions, and are never
user-facing candidates.
"""

from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    EVALUATION_FINGERPRINT_VERSION,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    FRAME_SNAPSHOT_VERSION,
    MAX_SHADOW_SYMBOLS,
    PAIR_FINGERPRINT_VERSION,
)
from app.workers.shadow.frames import FrameRejection, build_canonical_frame
from app.workers.shadow.fingerprints import (
    compute_evaluation_fingerprint,
    compute_pair_fingerprint,
    disagreement_category,
)
from app.workers.shadow.runner import run_shadow_comparison

__all__ = [
    "CANDIDATE_ARM_CODE",
    "CONTROL_ARM_CODE",
    "EVALUATION_FINGERPRINT_VERSION",
    "EXPERIMENT_CODE",
    "EXPERIMENT_VERSION",
    "FRAME_SNAPSHOT_VERSION",
    "FrameRejection",
    "MAX_SHADOW_SYMBOLS",
    "PAIR_FINGERPRINT_VERSION",
    "build_canonical_frame",
    "compute_evaluation_fingerprint",
    "compute_pair_fingerprint",
    "disagreement_category",
    "run_shadow_comparison",
]
