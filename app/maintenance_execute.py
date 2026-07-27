"""PURE validation for the one narrow maintenance mutation route.

Given a freshly-recomputed campaign maturation plan (the SAME read the audit
planner produces) and a versioned execute request, this module decides whether
a bounded batch may run — the SERVER, never the operator, determines the exact
batch slice. It never touches the database or a provider; the endpoint layers
concurrency + idempotency + the existing calc service on top.

Locks enforced here:
  * contract version, mode, experiment, cohort scope pinned to the allowed
    values;
  * manifest hash (normal) / retry-plan hash (retry) must match the live plan;
  * plan.safe_to_execute must be true;
  * normal pair_ids must EXACTLY equal the deterministic manifest slice for the
    requested batch_index (same set AND order), 1..MAX pairs, limit == count,
    no duplicates, none excluded, none in the retry plan;
  * retry pair_ids must be exactly the retryable pair(s) in the live retry plan
    (error still forward_fetch_error, membership verifiable), limit == 1.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Tuple


EXECUTE_CONTRACT_VERSION = "shadow_maintenance_execute.v1"
MODE_NORMAL = "normal"
MODE_RETRY = "retry"
RETRYABLE_ERROR_CODE = "forward_fetch_error"

# Single fixed PostgreSQL advisory-lock key: only ONE maintenance execution may
# hold it at a time (single-Machine, single-process assumption documented). It
# is session-scoped on the request connection and always released in `finally`.
MAINTENANCE_ADVISORY_LOCK_KEY = 0x53484144  # 'SHAD' → 1397244228

# Request fields a caller may NEVER supply (broadening / unsafe selectors).
FORBIDDEN_REQUEST_FIELDS = (
    "pending", "symbols", "run_id", "run_ids", "campaign_id", "campaign_ids",
    "run_in_background", "include_recalc", "strategy_code", "strategy_version",
)


class MaintenanceValidationError(ValueError):
    """A bounded, safe validation failure (no secrets, no raw payloads)."""


def _fail(reason: str) -> Dict[str, Any]:
    return {"ok": False, "reason": reason, "validated_pair_ids": [],
            "batch_identity": None}


def safe_batch_identity(
    manifest_hash: str, mode: str, batch_index: Optional[int],
    pair_ids: List[str],
) -> str:
    """Deterministic, secret-free identity for one execution attempt."""
    blob = "|".join([
        str(manifest_hash), str(mode), str(batch_index),
        ",".join(sorted(str(p) for p in pair_ids)),
    ])
    return "batch:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _reject_forbidden(request: Dict[str, Any]) -> Optional[str]:
    present = [f for f in FORBIDDEN_REQUEST_FIELDS if request.get(f) is not None]
    if present:
        return f"forbidden_request_fields:{sorted(present)}"
    return None


def expected_batch_slice(
    manifest_entries: List[Dict[str, Any]], batch_index: int, batch_size: int
) -> List[str]:
    """The exact ordered pair_id slice the server expects for a batch index.

    `manifest_entries` MUST already be in canonical
    snapshot_date_asc_symbol_asc_pair_id_asc order.
    """
    start = batch_index * batch_size
    return [e["pair_id"] for e in manifest_entries[start:start + batch_size]]


def validate_normal(
    plan: Dict[str, Any],
    request: Dict[str, Any],
    *,
    allowed_experiment: str,
    allowed_scope: str,
    max_batch_size: int,
) -> Dict[str, Any]:
    """Validate a normal-mode batch against the live campaign plan."""
    forbidden = _reject_forbidden(request)
    if forbidden:
        return _fail(forbidden)
    if request.get("contract_version") != EXECUTE_CONTRACT_VERSION:
        return _fail("bad_contract_version")
    if request.get("mode") != MODE_NORMAL:
        return _fail("bad_mode")
    if request.get("experiment_code") != allowed_experiment:
        return _fail("experiment_not_allowed")
    if request.get("cohort_scope") != allowed_scope:
        return _fail("cohort_scope_not_allowed")

    planning = plan.get("planning") or {}
    if not planning.get("safe_to_execute"):
        return _fail(f"plan_not_safe:{sorted(planning.get('blocking_reasons') or [])}")
    if plan.get("cohort_scope") != allowed_scope:
        return _fail("plan_scope_mismatch")
    if (plan.get("applied_filters") or {}).get("experiment_code") != allowed_experiment:
        return _fail("plan_experiment_mismatch")

    manifest_hash = plan.get("manifest_hash")
    if request.get("manifest_hash") != manifest_hash:
        return _fail("manifest_hash_mismatch")

    entries = plan.get("eligible_manifest") or []
    manifest_total = plan.get("manifest_total")
    if len(entries) != manifest_total:
        # the endpoint must recompute the FULL manifest (unpaginated)
        return _fail("manifest_not_complete")
    if manifest_total != plan.get("campaign_eligible_unmatured_count"):
        return _fail("manifest_count_mismatch")

    batch_index = request.get("batch_index")
    if not isinstance(batch_index, int) or batch_index < 0:
        return _fail("bad_batch_index")
    total_batches = (manifest_total + max_batch_size - 1) // max_batch_size
    if batch_index >= total_batches:
        return _fail("batch_index_out_of_range")

    pair_ids = request.get("pair_ids")
    if not isinstance(pair_ids, list) or not pair_ids:
        return _fail("pair_ids_required")
    if len(pair_ids) != len(set(pair_ids)):
        return _fail("duplicate_pair_ids")
    if not (1 <= len(pair_ids) <= max_batch_size):
        return _fail("batch_size_out_of_range")
    if request.get("limit") != len(pair_ids):
        return _fail("limit_must_equal_pair_count")

    expected = expected_batch_slice(entries, batch_index, max_batch_size)
    if list(pair_ids) != expected:
        # exact set AND order must match the server-computed slice
        return _fail("pair_ids_not_expected_batch_slice")

    excluded = {r["pair_id"] for r in
                (plan.get("excluded_non_campaign_evidence") or {}).get("records", [])}
    if set(pair_ids) & excluded:
        return _fail("pair_in_excluded_non_campaign")
    retry_ids = {e["pair_id"] for e in
                 (plan.get("retry_plan") or {}).get("entries", [])}
    if set(pair_ids) & retry_ids:
        return _fail("pair_in_retry_plan")

    manifest_ids = {e["pair_id"] for e in entries}
    if not set(pair_ids).issubset(manifest_ids):
        return _fail("pair_not_in_manifest")

    return {
        "ok": True, "reason": None, "validated_pair_ids": list(pair_ids),
        "include_recalc": False,
        "batch_identity": safe_batch_identity(
            manifest_hash, MODE_NORMAL, batch_index, pair_ids),
    }


def validate_retry(
    plan: Dict[str, Any],
    request: Dict[str, Any],
    *,
    allowed_experiment: str,
    allowed_scope: str,
) -> Dict[str, Any]:
    """Validate a retry-mode request against the live retry plan.

    (Contract implemented + tested; retry execution is NOT run in the
    environment-readiness task.)
    """
    forbidden = _reject_forbidden(request)
    if forbidden:
        return _fail(forbidden)
    if request.get("contract_version") != EXECUTE_CONTRACT_VERSION:
        return _fail("bad_contract_version")
    if request.get("mode") != MODE_RETRY:
        return _fail("bad_mode")
    if request.get("experiment_code") != allowed_experiment:
        return _fail("experiment_not_allowed")
    if request.get("cohort_scope") != allowed_scope:
        return _fail("cohort_scope_not_allowed")

    retry_plan = plan.get("retry_plan") or {}
    if request.get("retry_plan_hash") != retry_plan.get("retry_plan_hash"):
        return _fail("retry_plan_hash_mismatch")

    retryable = {
        e["pair_id"]: e for e in retry_plan.get("entries", [])
        if e.get("retryable")
    }
    pair_ids = request.get("pair_ids")
    if not isinstance(pair_ids, list) or len(pair_ids) != 1:
        return _fail("retry_requires_single_pair")
    if request.get("limit") != 1:
        return _fail("retry_limit_must_be_1")
    pid = pair_ids[0]
    entry = retryable.get(pid)
    if entry is None:
        return _fail("pair_not_in_retry_plan")
    if entry.get("current_error_code") != RETRYABLE_ERROR_CODE:
        return _fail("error_not_retryable")
    if not entry.get("requires_include_recalc"):
        return _fail("retry_does_not_require_recalc")
    if entry.get("campaign_membership") != "verifiable":
        return _fail("retry_membership_unverifiable")

    return {
        "ok": True, "reason": None, "validated_pair_ids": [pid],
        "include_recalc": True,  # server-set, never operator-supplied
        "batch_identity": safe_batch_identity(
            retry_plan.get("retry_plan_hash"), MODE_RETRY, None, [pid]),
    }


__all__ = [
    "EXECUTE_CONTRACT_VERSION",
    "MODE_NORMAL",
    "MODE_RETRY",
    "RETRYABLE_ERROR_CODE",
    "MAINTENANCE_ADVISORY_LOCK_KEY",
    "FORBIDDEN_REQUEST_FIELDS",
    "MaintenanceValidationError",
    "safe_batch_identity",
    "expected_batch_slice",
    "validate_normal",
    "validate_retry",
]
