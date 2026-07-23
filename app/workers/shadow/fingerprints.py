"""Deterministic pair / evaluation fingerprints for shadow comparisons.

Reuses the Phase 7B canonical-JSON + SHA-256 helpers. Fingerprints identify
one exact comparison input (pair) and one exact arm decision (evaluation);
they deliberately exclude run_id, insertion time, server time, logs and
secrets, so repeated exact comparisons reuse the same immutable rows.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.workers.provenance import canonical_json, _sha256
from app.workers.shadow.constants import (
    EVALUATION_FINGERPRINT_VERSION,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    PAIR_FINGERPRINT_VERSION,
)


def _utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def compute_pair_fingerprint(
    *,
    symbol: str,
    timeframe: str,
    provider: Optional[str],
    frame_hash: str,
    snapshot_date: Any,
    market_data_as_of: Optional[datetime],
    control_identity: Dict[str, Any],
    candidate_identity: Dict[str, Any],
    experiment_code: str = EXPERIMENT_CODE,
    experiment_version: str = EXPERIMENT_VERSION,
    fingerprint_version: str = PAIR_FINGERPRINT_VERSION,
    four_hour: Optional[Dict[str, Any]] = None,
) -> str:
    """SHA-256 identity of one exact comparison input.

    `control_identity` / `candidate_identity` are dicts with strategy_code,
    strategy_version, decision_policy_version and config_hash. A changed
    frame, config, version or policy yields a different fingerprint; run_id
    never enters the payload.

    `four_hour` (Phase 9E4) carries the canonical 4H input identity for
    experiments that supply a 4H frame ({"contract_version", "frame_hash",
    "state"} — frame_hash None with an explicit state when no frame could be
    built). When None (every daily-only experiment, including sma150), the
    payload is BYTE-IDENTICAL to the historical one, so existing sma150
    fingerprints never change.
    """
    payload = {
        "fingerprint_version": fingerprint_version,
        "experiment_code": experiment_code,
        "experiment_version": experiment_version,
        "symbol": symbol,
        "timeframe": timeframe,
        "provider": provider,
        "frame_hash": frame_hash,
        "snapshot_date": str(snapshot_date) if snapshot_date is not None else None,
        "market_data_as_of": _utc_iso(market_data_as_of),
        "control": {
            "strategy_code": control_identity["strategy_code"],
            "strategy_version": control_identity["strategy_version"],
            "decision_policy_version": control_identity["decision_policy_version"],
            "config_hash": control_identity["config_hash"],
        },
        "candidate": {
            "strategy_code": candidate_identity["strategy_code"],
            "strategy_version": candidate_identity["strategy_version"],
            "decision_policy_version": candidate_identity["decision_policy_version"],
            "config_hash": candidate_identity["config_hash"],
        },
    }
    if four_hour is not None:
        payload["four_hour"] = {
            "contract_version": four_hour.get("contract_version"),
            "frame_hash": four_hour.get("frame_hash"),
            "state": four_hour.get("state"),
        }
    return _sha256(canonical_json(payload))


def compute_evaluation_fingerprint(
    *,
    pair_fingerprint: str,
    arm_code: str,
    strategy_code: str,
    strategy_version: str,
    decision_policy_version: str,
    config_hash_value: str,
    verdict: str,
    details_original_sha256: str,
    fingerprint_version: str = EVALUATION_FINGERPRINT_VERSION,
) -> str:
    """SHA-256 identity of one exact arm decision within a pair.

    `details_original_sha256` is the hash of the COMPLETE (pre-pruning)
    canonical details, so decisions that differ only in optional pruned data
    still get distinct identities.
    """
    payload = {
        "fingerprint_version": fingerprint_version,
        "pair_fingerprint": pair_fingerprint,
        "arm_code": arm_code,
        "strategy_code": strategy_code,
        "strategy_version": strategy_version,
        "decision_policy_version": decision_policy_version,
        "config_hash": config_hash_value,
        "verdict": verdict,
        "details_sha256": details_original_sha256,
    }
    return _sha256(canonical_json(payload))


# Historical sma150 arm codes keep their historical category labels; every
# other (Phase 9D experiment) arm code maps to a neutral positional label.
_ARM_CATEGORY_LABELS = {
    "control_v2": "v2",
    "candidate_v3": "v3",
}


def category_label_for_arm(arm_code: str) -> str:
    """Deterministic category label for one persisted arm code.

    'control_v2'/'candidate_v3' keep the historical 'v2'/'v3' labels so
    existing sma150 categories are byte-identical; any other arm code maps to
    its neutral positional role ('control_*' -> 'control',
    'candidate_*' -> 'candidate').
    """
    label = _ARM_CATEGORY_LABELS.get(arm_code)
    if label is not None:
        return label
    if (arm_code or "").startswith("control"):
        return "control"
    if (arm_code or "").startswith("candidate"):
        return "candidate"
    return "unknown"


def disagreement_category(
    control_verdict: str,
    candidate_verdict: str,
    *,
    control_label: str = "v2",
    candidate_label: str = "v3",
) -> str:
    """Deterministic verdict-combination label (control first).

    'same_enter' / 'same_watch' / 'same_avoid' on agreement, otherwise e.g.
    'v2_enter_v3_avoid' (sma150 experiment, historical labels) or
    'control_enter_candidate_avoid' (Phase 9D experiments). A label is never
    an improvement/regression claim.
    """
    c = (control_verdict or "").lower()
    x = (candidate_verdict or "").lower()
    if c == x:
        return f"same_{c}"
    return f"{control_label}_{c}_{candidate_label}_{x}"
