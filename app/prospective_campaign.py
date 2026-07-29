"""PURE identities + contracts for the prospective campaign pipeline.

No provider, no DB, no network. Deterministic registration + execution identities
(idempotency), the advisory-lock key, contract-version constants, and the pinned
candidate/control strategy identities for experiment wyckoff_v2_vs_baseline.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

REGISTRATION_CONTRACT_VERSION = "prospective_campaign_registration.v1"
EXECUTE_CONTRACT_VERSION = "prospective_campaign_execute.v1"
ACCESS_CHECK_CONTRACT_VERSION = "prospective_access_check.v1"
PREFLIGHT_CONTRACT_VERSION = "prospective_preflight.v1"
AUDIT_CONTRACT_VERSION = "prospective_campaign_audit.v1"
EXPERIMENT_CONTRACT_VERSION = "wyckoff_v2_prospective_experiment.v1"

# Pinned identities (server-owned; never client-supplied).
CANDIDATE_STRATEGY_CODE = "wyckoff_mtf_v2"
CANDIDATE_STRATEGY_VERSION = "wyckoff_mtf.v2"
CANDIDATE_SIGNAL_DEFINITION = "pre_rollout_enter_eligible.v1"
CANDIDATE_ALLOW_ENTER = False
CONTROL_STRATEGY_CODE = "sma150_bounce"
CONTROL_STRATEGY_VERSION = "sma150.v2"
CANDIDATE_ARM_CODE = "candidate_wyckoff_v2"
CONTROL_ARM_CODE = "control_baseline"

# Distinct advisory-lock key ('PROS'): only ONE prospective execute at a time.
PROSPECTIVE_ADVISORY_LOCK_KEY = 0x50524F53  # 1347571539


def _sha(obj: Any) -> str:
    import json
    return "sha256:" + hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _candidate_identity() -> Dict[str, Any]:
    return {"strategy_code": CANDIDATE_STRATEGY_CODE,
            "strategy_version": CANDIDATE_STRATEGY_VERSION,
            "signal_definition": CANDIDATE_SIGNAL_DEFINITION,
            "allow_enter": CANDIDATE_ALLOW_ENTER, "arm_code": CANDIDATE_ARM_CODE}


def _control_identity() -> Dict[str, Any]:
    return {"strategy_code": CONTROL_STRATEGY_CODE,
            "strategy_version": CONTROL_STRATEGY_VERSION, "arm_code": CONTROL_ARM_CODE}


def registration_identity(*, experiment_code: str, universe_id: str, universe_hash: str,
                          history_config_hash: str, snapshot_session_date: str) -> str:
    """Deterministic identity preventing a duplicate campaign for the same
    experiment + frozen universe + strategy config + completed snapshot."""
    return "pcr:" + hashlib.sha256("|".join([
        REGISTRATION_CONTRACT_VERSION, EXPERIMENT_CONTRACT_VERSION, str(experiment_code),
        str(universe_id), str(universe_hash), str(history_config_hash),
        str(snapshot_session_date), _sha(_candidate_identity()), _sha(_control_identity()),
    ]).encode()).hexdigest()


def campaign_execution_identity(*, registration_identity_value: str, universe_hash: str,
                                history_readiness_manifest_hash: str,
                                snapshot_session_date: str) -> str:
    """Deterministic execution identity binding the registration + pinned hashes
    + snapshot + candidate/control config."""
    return "pcx:" + hashlib.sha256("|".join([
        EXECUTE_CONTRACT_VERSION, str(registration_identity_value), str(universe_hash),
        str(history_readiness_manifest_hash), str(snapshot_session_date),
        _sha(_candidate_identity()), _sha(_control_identity()),
    ]).encode()).hexdigest()


def candidate_signal_fields(details: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the pre-rollout candidate signal semantics from a wyckoff_mtf_v2
    evaluation `details_snapshot` (already persisted verbatim). Never treats
    final WATCH as the primary signal — the primary signal is
    enter_eligible_without_rollout_gate."""
    d = details or {}
    readiness = (d.get("readiness") or {})
    policy = (d.get("policy") or {})
    trigger = (d.get("four_hour_trigger") or {})
    return {
        "readiness_status": readiness.get("status"),
        "setup_present": bool(policy.get("setup_present") if "setup_present" in policy
                              else d.get("setup_state") not in (None, "absent", "none")),
        "setup_state": d.get("setup_state") or policy.get("setup_state"),
        "trigger_confirmed": bool(policy.get("trigger_confirmed")
                                  or d.get("trigger_state") == "confirmed"),
        "trigger_state": d.get("trigger_state") or policy.get("trigger_state"),
        "enter_eligible_without_rollout_gate": bool(
            policy.get("enter_eligible_without_rollout_gate")),
        "rollout_blocked": bool(policy.get("rollout_blocked")
                                or "enter_disabled_shadow_only" in (policy.get("waiting_reasons") or [])),
        "waiting_reasons": policy.get("waiting_reasons") or [],
        "four_hour_state": trigger.get("state") or trigger.get("trigger_state"),
    }


__all__ = [
    "REGISTRATION_CONTRACT_VERSION", "EXECUTE_CONTRACT_VERSION",
    "ACCESS_CHECK_CONTRACT_VERSION", "PREFLIGHT_CONTRACT_VERSION",
    "AUDIT_CONTRACT_VERSION", "EXPERIMENT_CONTRACT_VERSION",
    "CANDIDATE_STRATEGY_CODE", "CANDIDATE_STRATEGY_VERSION", "CANDIDATE_SIGNAL_DEFINITION",
    "CANDIDATE_ALLOW_ENTER", "CONTROL_STRATEGY_CODE", "CONTROL_STRATEGY_VERSION",
    "CANDIDATE_ARM_CODE", "CONTROL_ARM_CODE", "PROSPECTIVE_ADVISORY_LOCK_KEY",
    "registration_identity", "campaign_execution_identity", "candidate_signal_fields",
]
