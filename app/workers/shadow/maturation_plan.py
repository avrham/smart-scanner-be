"""Read-only bounded maturation PLAN (shadow_maturation_plan.v1).

PURE aggregation (no I/O) that turns an already-fetched, bounded shadow cohort
into a *deterministic, paginated, hash-stamped manifest* of exactly the pairs a
later bounded maturation run would touch — and proves it is safe to execute.

It exists because the existing closeout report is honest but INSUFFICIENT for
driving a mutation: closeout aggregates are exact, yet its per-pair unresolved
list is a truncated 200-row sample. Before any outcome is calculated we need the
COMPLETE, ordered, de-duplicated set of eligible pair IDs plus a separate,
explicit retry plan for the single retryable failure — with membership proven,
identity uniform, and duplicates classified.

It composes the existing machinery, never a parallel implementation:
  * `classify_maturation_eligibility` / `summarize_eligibility` — the SAME
    trading-session eligibility taxonomy the closeout uses;
  * the same evaluation-record + outcome-row shapes the persistence layer emits.

Hard invariants (any violation ⇒ `safe_to_execute=false`, never a silent pass):
  * the eligible manifest is complete (every eligible pair present, none twice);
  * the eligible-manifest count equals the authoritative eligibility count;
  * the single retryable failure is in the retry plan, NEVER the eligible
    manifest;
  * cohort identity is uniform and matches the requested selector;
  * every eligible pair's campaign membership is recoverable from persisted
    relationships;
  * duplicate (symbol, session) groups are benign cross-campaign overlaps —
    same-campaign / same-run / identity-mismatch / unverifiable duplicates block.

The manifest hash is computed over IMMUTABLE identity only (cohort identity +
per-pair {pair_id, snapshot_date, strategy identity, experiment identity}),
canonicalized so it is independent of page size and row ordering. It is recorded
before maturation and re-checked afterwards.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.shadow.outcomes.eligibility import (
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MISSING_SESSION_DATA,
    ELIGIBILITY_NOT_YET,
    ELIGIBILITY_RETRYABLE,
    ELIGIBILITY_TERMINAL,
    FULL_MATURATION_SESSIONS,
    MIN_MATURATION_SESSIONS,
    classify_maturation_eligibility,
    completed_forward_sessions,
    summarize_eligibility,
)


MATURATION_PLAN_CONTRACT_VERSION = "shadow_maturation_plan.v2"
# v2: the hash preimage now includes the cohort SCOPE, so a campaign manifest and
# an experiment manifest can never share an identity, and the old v1 329-pair
# hash no longer describes any executable manifest.
MANIFEST_HASH_VERSION = "shadow_maturation_manifest_hash.v2"
RETRY_PLAN_CONTRACT_VERSION = "shadow_maturation_retry_plan.v1"
DUPLICATE_AUDIT_CONTRACT_VERSION = "shadow_maturation_duplicate_audit.v1"
# STABLE campaign-cohort membership lock: covers every campaign-linked pair of
# the cohort regardless of outcome status, so normal/retry writes never change
# it. Distinct from the DYNAMIC remaining-work manifest hash (which shrinks as
# pairs mature). next_batch hash binds the two + the exact first remaining slice.
COHORT_LOCK_HASH_VERSION = "shadow_maturation_cohort_lock.v1"
NEXT_BATCH_HASH_VERSION = "shadow_maturation_next_batch.v1"

# Cohort scope (closed vocabulary). `campaign` is the executable maturation
# cohort (campaign-linked records only); `experiment` is the broader read-only
# experiment-evidence view (includes manual/legacy records and stays unsafe for
# campaign maturation while any non-campaign membership exists).
COHORT_SCOPE_CAMPAIGN = "campaign"
COHORT_SCOPE_EXPERIMENT = "experiment"
COHORT_SCOPES = (COHORT_SCOPE_CAMPAIGN, COHORT_SCOPE_EXPERIMENT)

# Per-pair campaign-membership verdicts (derived from persisted run telemetry).
CAMPAIGN_MEMBERSHIP_VERIFIABLE = "verifiable"     # >=1 valid campaign block
CAMPAIGN_MEMBERSHIP_NONE = "none"                 # no campaign telemetry at all
CAMPAIGN_MEMBERSHIP_CONFLICTING = "conflicting"   # block(s) present but invalid

# Deterministic manifest ordering (documented, hash-independent).
MANIFEST_ORDERING = "snapshot_date_asc_symbol_asc_pair_id_asc"

# Bounded pagination. The server maximum is a justified bounded value: the
# whole 329-pair cohort fits in one page, and a hostile caller can never make
# the response grow without bound. There is no unbounded page.
DEFAULT_PAGE_LIMIT = 500
MAX_PAGE_LIMIT = 500

# Bounded per-request maturation batch sizing. Massive Basic is 5 requests/min
# and each pair needs at least one bounded forward fetch (plus shared benchmark
# fetches), while ONE synchronous HTTP request stays open for the whole batch.
# A conservative batch keeps that single request well within provider-throttle
# and proxy/connection timeouts — never the endpoint hard cap of 200, and
# never 50 merely because a report once suggested it.
RECOMMENDED_MATURATION_BATCH_SIZE = 10

# Duplicate (symbol, session) group classifications (closed vocabulary).
DUP_BENIGN_CROSS_CAMPAIGN = "benign_cross_campaign_overlap"
DUP_WITHIN_SAME_CAMPAIGN = "duplicate_within_same_campaign"
DUP_WITHIN_SAME_RUN = "duplicate_within_same_run"
DUP_IDENTITY_MISMATCH = "identity_mismatch"
DUP_UNVERIFIABLE = "unverifiable"
# The duplicate classes that must block a maturation run (a benign cross-
# campaign overlap of two DISTINCT pair IDs is legitimate and never blocks).
BLOCKING_DUPLICATE_CLASSES = frozenset({
    DUP_WITHIN_SAME_CAMPAIGN,
    DUP_WITHIN_SAME_RUN,
    DUP_IDENTITY_MISMATCH,
    DUP_UNVERIFIABLE,
})


def _as_date(value: Any) -> Optional[date]:
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


def _iso(value: Any) -> Optional[str]:
    d = _as_date(value)
    return d.isoformat() if d is not None else (
        str(value) if value is not None else None
    )


def _single_campaign_id(campaign_ids: List[str]) -> Optional[str]:
    """The one recoverable campaign id, or None when absent/ambiguous."""
    ids = [str(c) for c in (campaign_ids or []) if c]
    return ids[0] if len(ids) == 1 else None


def _error_code_by_pair(outcome_rows: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """Map pair_id -> outcome error_code (the code only lives on outcome rows)."""
    out: Dict[str, Optional[str]] = {}
    for row in outcome_rows:
        outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
        pair = row.get("pair") or {}
        pid = str(pair.get("pair_id")) if pair.get("pair_id") else None
        if pid is not None:
            out[pid] = outcome.get("error_code")
    return out


def _pair_view(record: Dict[str, Any], sessions: Optional[int]) -> Dict[str, Any]:
    """Identity + eligibility metadata for one pair (no business price data)."""
    campaign_ids = [str(c) for c in (record.get("campaign_ids") or []) if c]
    return {
        "pair_id": str(record.get("pair_id")) if record.get("pair_id") else None,
        "symbol": record.get("symbol"),
        "snapshot_date": _iso(record.get("snapshot_date")),
        "run_id": str(record.get("run_id")) if record.get("run_id") else None,
        "campaign_ids": campaign_ids,
        "campaign_id": _single_campaign_id(campaign_ids),
        "strategy_code": record.get("strategy_code"),
        "strategy_version": record.get("strategy_version"),
        "experiment_code": record.get("experiment_code"),
        "experiment_version": record.get("experiment_version"),
        "decision_policy_version": record.get("decision_policy_version"),
        "config_hash": record.get("config_hash"),
        "outcome_status": record.get("outcome_status"),
        "completed_forward_sessions": sessions,
        "required_forward_sessions": FULL_MATURATION_SESSIONS,
    }


def _manifest_entry(view: Dict[str, Any], eligibility_class: str) -> Dict[str, Any]:
    return {
        "pair_id": view["pair_id"],
        "symbol": view["symbol"],
        "snapshot_date": view["snapshot_date"],
        "run_id": view["run_id"],
        "campaign_id": view["campaign_id"],
        "campaign_ids": view["campaign_ids"],
        "strategy_code": view["strategy_code"],
        "strategy_version": view["strategy_version"],
        "experiment_code": view["experiment_code"],
        "outcome_status": view["outcome_status"],
        "eligibility_class": eligibility_class,
        "completed_forward_sessions": view["completed_forward_sessions"],
        "required_forward_sessions": view["required_forward_sessions"],
    }


def _sort_key(entry: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(entry.get("snapshot_date") or ""),
        str(entry.get("symbol") or ""),
        str(entry.get("pair_id") or ""),
    )


def classify_campaign_membership(
    campaign_blocks: Optional[List[Any]], cohort_experiment: Optional[str]
) -> Tuple[str, List[str]]:
    """PURE: derive one pair's campaign membership from its linked campaign blocks.

    A campaign block is VALID only when it carries a campaign_id, an as_of_date
    and (when the cohort experiment is known) a matching experiment_code. Returns
    (verdict, sorted valid campaign ids). Multiple valid campaigns is legitimate
    cross-campaign reuse — still `verifiable`, ids reported, pair kept once.
    """
    blocks = campaign_blocks or []
    if not blocks:
        return CAMPAIGN_MEMBERSHIP_NONE, []
    valid: set = set()
    conflict = False
    for b in blocks:
        if not isinstance(b, dict):
            conflict = True
            continue
        cid = b.get("campaign_id")
        ec = b.get("experiment_code")
        aod = b.get("as_of_date")
        if not cid or not aod:
            conflict = True
            continue
        if cohort_experiment and ec and ec != cohort_experiment:
            conflict = True
            continue
        valid.add(str(cid))
    if valid and not conflict:
        return CAMPAIGN_MEMBERSHIP_VERIFIABLE, sorted(valid)
    return CAMPAIGN_MEMBERSHIP_CONFLICTING, sorted(valid)


def compute_cohort_lock_hash(
    cohort_identity: Dict[str, Any], cohort_entries: List[Dict[str, Any]]
) -> str:
    """STABLE hash over the campaign-cohort MEMBERSHIP — outcome-status blind.

    Covers every campaign-linked pair of the cohort (complete, pending,
    retryable — all statuses) by IMMUTABLE identity only, so normal/retry
    outcome writes never change it; adding/removing a campaign pair or changing
    any identity field does. Canonicalized (sorted by pair_id) → page/order
    independent. Excludes outcome/error/eligibility/session/timestamp state.
    """
    canonical = {
        "hash_version": COHORT_LOCK_HASH_VERSION,
        "cohort": {
            "strategy_code": cohort_identity.get("strategy_code"),
            "experiment_code": cohort_identity.get("experiment_code"),
        },
        "entries": sorted(
            (
                {
                    "pair_id": e["pair_id"],
                    "symbol": e["symbol"],
                    "snapshot_date": e["snapshot_date"],
                    "experiment_code": e["experiment_code"],
                    "experiment_version": e.get("experiment_version"),
                    "strategy_code": e["strategy_code"],
                    "strategy_version": e["strategy_version"],
                    "decision_policy_version": e.get("decision_policy_version"),
                    "config_hash": e.get("config_hash"),
                    "campaign_ids": sorted(str(c) for c in (e.get("campaign_ids") or [])),
                }
                for e in cohort_entries
            ),
            key=lambda x: str(x["pair_id"]),
        ),
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def compute_next_batch_hash(
    cohort_lock_hash: str, remaining_manifest_hash: str,
    pair_ids: List[str], batch_size: int, mode: str = "normal",
) -> str:
    """Deterministic identity of the exact next slice: binds the stable cohort
    lock, the dynamic remaining hash, the ordered slice pair_ids, the batch size
    and the mode — NOT a long-lived index into the shrinking manifest."""
    canonical = {
        "hash_version": NEXT_BATCH_HASH_VERSION,
        "cohort_lock_hash": cohort_lock_hash,
        "remaining_manifest_hash": remaining_manifest_hash,
        "mode": mode,
        "batch_size": int(batch_size),
        "pair_ids": [str(p) for p in pair_ids],  # order-significant (the slice)
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def compute_retry_plan_hash(
    cohort_identity: Dict[str, Any], retry_entries: List[Dict[str, Any]]
) -> str:
    """Deterministic hash over the retryable-failure set's immutable identity.

    Canonicalized (sorted by pair_id) so it is page/order independent; includes
    the cohort identity and each retryable pair's {pair_id, snapshot_date,
    current_error_code}. Mutable text/timestamps are never included.
    """
    canonical = {
        "hash_version": MANIFEST_HASH_VERSION,
        "kind": "retry_plan",
        "cohort": {
            "strategy_code": cohort_identity.get("strategy_code"),
            "experiment_code": cohort_identity.get("experiment_code"),
        },
        "entries": sorted(
            (
                {
                    "pair_id": e["pair_id"],
                    "snapshot_date": e["snapshot_date"],
                    "current_error_code": e.get("current_error_code"),
                }
                for e in retry_entries if e.get("retryable")
            ),
            key=lambda x: str(x["pair_id"]),
        ),
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def compute_manifest_hash(
    cohort_identity: Dict[str, Any],
    eligible_entries: List[Dict[str, Any]],
    *,
    scope: str,
) -> str:
    """Deterministic hash over IMMUTABLE identity only.

    Canonicalized (entries sorted by pair_id, keys sorted, no whitespace) so it
    is independent of page size and row ordering. Cohort identity AND the cohort
    SCOPE are part of the preimage, so changing the cohort/scope — or any pair id
    / snapshot / identity — changes the hash; mutable error text and timestamps
    are never included.
    """
    canonical = {
        "hash_version": MANIFEST_HASH_VERSION,
        "scope": scope,
        "cohort": {
            "strategy_code": cohort_identity.get("strategy_code"),
            "experiment_code": cohort_identity.get("experiment_code"),
        },
        "entries": sorted(
            (
                {
                    "pair_id": e["pair_id"],
                    "snapshot_date": e["snapshot_date"],
                    "strategy_code": e["strategy_code"],
                    "strategy_version": e["strategy_version"],
                    "experiment_code": e["experiment_code"],
                    "experiment_version": e.get("experiment_version"),
                }
                for e in eligible_entries
            ),
            key=lambda x: str(x["pair_id"]),
        ),
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def classify_duplicate_group(records: List[Dict[str, Any]]) -> str:
    """Classify one (symbol, session) duplicate group from its records.

    Precedence (most serious first): same run > same campaign > identity
    mismatch > benign cross-campaign overlap > unverifiable. Two records from
    two DIFFERENT legitimate campaigns are a benign overlap; anything that
    cannot be attributed to distinct campaigns, or that disagrees on identity,
    is flagged for investigation and blocks maturation.
    """
    run_ids = [str(r.get("run_id")) for r in records if r.get("run_id")]
    if len(run_ids) != len(set(run_ids)):
        return DUP_WITHIN_SAME_RUN

    campaign_sets = [
        {str(c) for c in (r.get("campaign_ids") or []) if c} for r in records
    ]
    # A campaign id shared by two records ⇒ same-campaign duplicate.
    seen: set = set()
    for cs in campaign_sets:
        if seen & cs:
            return DUP_WITHIN_SAME_CAMPAIGN
        seen |= cs

    identity = {
        (
            r.get("strategy_code"), r.get("strategy_version"),
            r.get("experiment_code"), r.get("experiment_version"),
            r.get("decision_policy_version"), r.get("config_hash"),
        )
        for r in records
    }
    if len(identity) > 1:
        return DUP_IDENTITY_MISMATCH

    # Uniform identity: benign only when EVERY record maps to its own campaign.
    if all(len(cs) >= 1 for cs in campaign_sets):
        return DUP_BENIGN_CROSS_CAMPAIGN
    return DUP_UNVERIFIABLE


def _duplicate_audit(
    records_by_pair_group: Dict[str, List[Dict[str, Any]]],
    focus_keys: Optional[List[str]],
) -> Dict[str, Any]:
    """Build the duplicate-investigation block for every duplicate group.

    `focus_keys` (e.g. the two known AAPL sessions) are always reported even
    when — and especially when — they turn out benign, so the operator sees the
    exact per-record identity attribution.
    """
    groups: List[Dict[str, Any]] = []
    focus = set(focus_keys or [])
    for key in sorted(records_by_pair_group):
        recs = records_by_pair_group[key]
        if len(recs) <= 1 and key not in focus:
            continue
        classification = (
            classify_duplicate_group(recs) if len(recs) > 1 else DUP_UNVERIFIABLE
            if key in focus else None
        )
        if classification is None:
            continue
        symbol, _, snapshot = key.partition("|")
        groups.append({
            "group_key": key,
            "symbol": symbol,
            "snapshot_date": snapshot,
            "record_count": len(recs),
            "classification": classification,
            "blocks_maturation": classification in BLOCKING_DUPLICATE_CLASSES,
            "records": sorted(
                (
                    {
                        "evaluation_id": r.get("evaluation_id"),
                        "pair_id": str(r.get("pair_id")) if r.get("pair_id") else None,
                        "run_id": str(r.get("run_id")) if r.get("run_id") else None,
                        "campaign_ids": [
                            str(c) for c in (r.get("campaign_ids") or []) if c
                        ],
                        "symbol": r.get("symbol"),
                        "snapshot_date": _iso(r.get("snapshot_date")),
                        "strategy_code": r.get("strategy_code"),
                        "strategy_version": r.get("strategy_version"),
                        "experiment_code": r.get("experiment_code"),
                        "config_hash": r.get("config_hash"),
                        "decision_policy_version": r.get("decision_policy_version"),
                        "created_at": (
                            r.get("created_at").isoformat()
                            if hasattr(r.get("created_at"), "isoformat")
                            else (str(r.get("created_at"))
                                  if r.get("created_at") is not None else None)
                        ),
                    }
                    for r in recs
                ),
                key=lambda x: str(x["pair_id"]),
            ),
        })
    return {
        "contract_version": DUPLICATE_AUDIT_CONTRACT_VERSION,
        "duplicate_group_count": len(groups),
        "blocking_duplicate_group_count": sum(
            1 for g in groups if g["blocks_maturation"]
        ),
        "groups": groups,
    }


def build_maturation_plan(
    records: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    *,
    cohort_scope: str,
    applied_filters: Dict[str, Any],
    session_dates: Optional[List[date]] = None,
    latest_completed_session: Optional[date] = None,
    campaign_ids: Optional[List[str]] = None,
    page_limit: int = DEFAULT_PAGE_LIMIT,
    page_offset: int = 0,
    records_possibly_truncated: bool = False,
    duplicate_focus: Optional[List[str]] = None,
    batch_size: int = RECOMMENDED_MATURATION_BATCH_SIZE,
) -> Dict[str, Any]:
    """Build the complete, paginated, hash-stamped maturation plan. PURE.

    `cohort_scope` selects the executable cohort: `campaign` (only campaign-
    linked eligible pairs — the executable maturation manifest) or `experiment`
    (every eligible pair, including manual/legacy evidence — read-only, stays
    unsafe while any non-campaign membership exists).
    """
    if cohort_scope not in COHORT_SCOPES:
        raise ValueError(f"cohort_scope must be one of {list(COHORT_SCOPES)}")
    session_dates = session_dates or []
    page_limit = max(1, min(int(page_limit), MAX_PAGE_LIMIT))
    page_offset = max(0, int(page_offset))
    error_by_pair = _error_code_by_pair(outcome_rows)
    cohort_experiment = applied_filters.get("experiment_code")

    all_eligible: List[Dict[str, Any]] = []      # every eligible pair (annotated)
    retry_entries: List[Dict[str, Any]] = []
    eligibility_inputs: List[Dict[str, Any]] = []
    records_by_group: Dict[str, List[Dict[str, Any]]] = {}
    pair_id_counts: Dict[str, int] = {}
    campaign_id_set = {str(c) for c in (campaign_ids or []) if c}
    # STABLE cohort-lock membership: EVERY campaign-verifiable pair regardless of
    # outcome status, deduped by pair_id (immutable identity only).
    cohort_lock_by_pair: Dict[str, Dict[str, Any]] = {}
    terminal_count = 0
    missing_session_count = 0

    def _membership_of(record: Dict[str, Any]) -> Tuple[str, List[str]]:
        return classify_campaign_membership(
            record.get("campaign_blocks"), cohort_experiment)

    for record in records:
        pid = str(record.get("pair_id")) if record.get("pair_id") else None
        snapshot = _as_date(record.get("snapshot_date"))
        outcome_status = record.get("outcome_status")
        error_code = error_by_pair.get(pid) if outcome_status == "error" else None
        sessions = (
            completed_forward_sessions(
                session_dates, snapshot,
                latest_completed_session=latest_completed_session,
            )
            if snapshot is not None else None
        )
        eligibility_inputs.append({
            "outcome_status": outcome_status,
            "error_code": error_code,
            "completed_forward_sessions": sessions,
        })
        state = classify_maturation_eligibility(
            outcome_status=outcome_status,
            error_code=error_code,
            completed_forward_sessions=sessions,
        )
        view = _pair_view(record, sessions)
        if pid is not None:
            pair_id_counts[pid] = pair_id_counts.get(pid, 0) + 1
        for cid in view["campaign_ids"]:
            campaign_id_set.add(cid)

        key = f"{str(record.get('symbol') or '').upper()}|{_iso(record.get('snapshot_date'))}"
        records_by_group.setdefault(key, []).append(record)

        membership, valid_cids = _membership_of(record)

        # Stable cohort membership: any campaign-verifiable pair, ALL statuses.
        if membership == CAMPAIGN_MEMBERSHIP_VERIFIABLE and pid is not None:
            cohort_lock_by_pair[pid] = {
                "pair_id": view["pair_id"], "symbol": view["symbol"],
                "snapshot_date": view["snapshot_date"],
                "experiment_code": view["experiment_code"],
                "experiment_version": view["experiment_version"],
                "strategy_code": view["strategy_code"],
                "strategy_version": view["strategy_version"],
                "decision_policy_version": view["decision_policy_version"],
                "config_hash": view["config_hash"],
                "campaign_ids": valid_cids,
            }

        if state == ELIGIBILITY_ELIGIBLE:
            entry = _manifest_entry(view, state)
            entry["campaign_membership"] = membership
            entry["campaign_ids"] = valid_cids
            all_eligible.append(entry)
        elif state == ELIGIBILITY_RETRYABLE:
            retry_entries.append({
                "pair_id": view["pair_id"], "symbol": view["symbol"],
                "snapshot_date": view["snapshot_date"],
                "current_error_code": error_code,
                "retryable": True, "requires_include_recalc": True,
                "campaign_membership": membership,
                "campaign_ids": valid_cids,
                "recommended_limit": 1,
            })
        elif state == ELIGIBILITY_TERMINAL:
            terminal_count += 1
            retry_entries.append({
                "pair_id": view["pair_id"], "symbol": view["symbol"],
                "snapshot_date": view["snapshot_date"],
                "current_error_code": error_code,
                "retryable": False, "requires_include_recalc": False,
                "campaign_membership": membership,
                "campaign_ids": valid_cids,
            })
        elif state == ELIGIBILITY_MISSING_SESSION_DATA:
            missing_session_count += 1

    eligibility = summarize_eligibility(eligibility_inputs)
    experiment_eligible_count = eligibility["counts"][ELIGIBILITY_ELIGIBLE]

    # ---- partition eligible pairs by campaign membership ------------------- #
    campaign_verifiable = [e for e in all_eligible
                           if e["campaign_membership"] == CAMPAIGN_MEMBERSHIP_VERIFIABLE]
    campaign_none = [e for e in all_eligible
                     if e["campaign_membership"] == CAMPAIGN_MEMBERSHIP_NONE]
    campaign_conflicting = [e for e in all_eligible
                            if e["campaign_membership"] == CAMPAIGN_MEMBERSHIP_CONFLICTING]

    if cohort_scope == COHORT_SCOPE_CAMPAIGN:
        manifest_entries = list(campaign_verifiable)
        # Manual/legacy (no telemetry) are retained but excluded — never a
        # blocker. Conflicting (campaign-intended but invalid) DO block below.
        excluded_entries = list(campaign_none)
    else:  # experiment scope: the broad read-only view keeps every eligible pair
        manifest_entries = list(all_eligible)
        excluded_entries = []

    manifest_entries.sort(key=_sort_key)
    retry_entries.sort(key=lambda e: (str(e.get("snapshot_date") or ""),
                                      str(e.get("symbol") or ""),
                                      str(e.get("pair_id") or "")))

    def _distinct(field: str) -> List[Any]:
        return sorted({e.get(field) for e in manifest_entries if e.get(field) is not None},
                      key=lambda v: str(v))

    strategy_identity = {
        "strategy_code": applied_filters.get("strategy_code"),
        "experiment_code": applied_filters.get("experiment_code"),
        "strategy_versions": _distinct("strategy_version"),
        "experiment_codes": _distinct("experiment_code"),
    }

    # ---- membership counts WITHIN the manifest ----------------------------- #
    manifest_verifiable = sum(
        1 for e in manifest_entries
        if e["campaign_membership"] == CAMPAIGN_MEMBERSHIP_VERIFIABLE)
    manifest_unverifiable = len(manifest_entries) - manifest_verifiable
    pairs_by_campaign: Dict[str, int] = {}
    for e in manifest_entries:
        if not e["campaign_ids"]:
            pairs_by_campaign["__unverifiable__"] = (
                pairs_by_campaign.get("__unverifiable__", 0) + 1)
        else:
            for cid in e["campaign_ids"]:
                pairs_by_campaign[cid] = pairs_by_campaign.get(cid, 0) + 1

    duplicate_audit = _duplicate_audit(records_by_group, duplicate_focus)

    manifest_total = len(manifest_entries)
    cohort_identity = {
        "strategy_code": applied_filters.get("strategy_code"),
        "experiment_code": applied_filters.get("experiment_code"),
    }
    manifest_hash = compute_manifest_hash(
        cohort_identity, manifest_entries, scope=cohort_scope)

    # ---- STABLE cohort lock + DYNAMIC remaining + deterministic next batch --- #
    cohort_lock_entries = list(cohort_lock_by_pair.values())
    cohort_lock_hash = compute_cohort_lock_hash(cohort_identity, cohort_lock_entries)
    cohort_pair_count = len(cohort_lock_entries)
    # The remaining-work manifest IS this scope's executable manifest (shrinks as
    # pairs mature); its hash is the same value the v1 field exposed.
    remaining_manifest_hash = manifest_hash
    remaining_pair_count = manifest_total
    normal_execution_complete = (remaining_pair_count == 0)
    # The next batch is ALWAYS the first deterministic slice of the CURRENT
    # remaining manifest (never a fixed index into the original shrinking set).
    batch_size = max(1, int(batch_size))
    _next_slice = manifest_entries[:batch_size]
    _next_ids = [e["pair_id"] for e in _next_slice]
    next_batch = {
        "available": bool(_next_ids),
        "pair_count": len(_next_ids),
        "pair_ids": _next_ids,
        "ordering": MANIFEST_ORDERING,
        "next_batch_hash": (
            compute_next_batch_hash(cohort_lock_hash, remaining_manifest_hash,
                                    _next_ids, batch_size, mode="normal")
            if _next_ids else None),
        "remaining_pair_count_before": remaining_pair_count,
    }

    # ---- safety gate (fail-closed) ----------------------------------------- #
    blocking: List[str] = []
    duplicate_pair_ids = sorted(pid for pid, n in pair_id_counts.items() if n > 1)
    if records_possibly_truncated:
        blocking.append("cohort_exceeds_bounded_read")
    if any(e["eligibility_class"] != ELIGIBILITY_ELIGIBLE for e in manifest_entries):
        blocking.append("non_eligible_entry_in_manifest")
    retry_pair_ids = {e["pair_id"] for e in retry_entries}
    manifest_pair_ids = {e["pair_id"] for e in manifest_entries}
    if retry_pair_ids & manifest_pair_ids:
        blocking.append("retry_pair_in_manifest")
    # A duplicate pair id WITHIN the manifest is a real blocker.
    if len(manifest_pair_ids) != len(manifest_entries):
        blocking.append("duplicate_pair_ids")
    if terminal_count > 0:
        blocking.append("terminal_failures_present")
    expected_strategy = applied_filters.get("strategy_code")
    if any(e["strategy_code"] != expected_strategy for e in manifest_entries):
        blocking.append("strategy_code_mismatch")
    if cohort_experiment is not None and any(
        e["experiment_code"] != cohort_experiment for e in manifest_entries
    ):
        blocking.append("experiment_code_mismatch")
    if len(strategy_identity["strategy_versions"]) > 1:
        blocking.append("non_uniform_strategy_version")
    if len(_distinct("experiment_code")) > 1:
        blocking.append("non_uniform_experiment_code")
    if any(g["blocks_maturation"] for g in duplicate_audit["groups"]):
        blocking.append("blocking_duplicate_group")

    if cohort_scope == COHORT_SCOPE_CAMPAIGN:
        # Campaign-intended pairs whose telemetry is invalid/ambiguous block.
        if campaign_conflicting:
            blocking.append("campaign_membership_conflict")
        # By construction the campaign manifest is all-verifiable; assert it.
        if manifest_unverifiable > 0:
            blocking.append("campaign_membership_unverifiable")
    else:
        # Experiment scope stays unsafe for maturation while any eligible pair
        # lacks verifiable campaign membership (preserved v1 contract).
        if manifest_unverifiable > 0:
            blocking.append("membership_unverifiable")

    safe_to_execute = not blocking

    # ---- batch sizing (deterministic; computed from the manifest) ---------- #
    batch_size = RECOMMENDED_MATURATION_BATCH_SIZE
    if manifest_total == 0:
        batch_count = full_batch_count = final_batch_size = 0
    else:
        batch_count = (manifest_total + batch_size - 1) // batch_size
        remainder = manifest_total % batch_size
        if remainder == 0:
            full_batch_count = batch_count
            final_batch_size = batch_size
        else:
            full_batch_count = batch_count - 1
            final_batch_size = remainder

    # ---- page the manifest (hash + totals are page-independent) ------------ #
    page = manifest_entries[page_offset:page_offset + page_limit]
    has_more = (page_offset + len(page)) < manifest_total
    next_offset = (page_offset + len(page)) if has_more else None

    excluded_block = {
        "count": len(excluded_entries),
        "reason": "no_campaign_telemetry",
        "records": [
            {
                "pair_id": e["pair_id"], "symbol": e["symbol"],
                "snapshot_date": e["snapshot_date"],
                "classification": "non_campaign_evidence",
                "reason": "no_campaign_telemetry",
            }
            for e in sorted(excluded_entries, key=_sort_key)
        ],
    }

    return {
        "contract_version": MATURATION_PLAN_CONTRACT_VERSION,
        "cohort_scope": cohort_scope,
        "applied_filters": applied_filters,
        "strategy_identity": strategy_identity,
        "campaign_ids": sorted(campaign_id_set),
        "campaign_count": len(campaign_id_set),
        "total_evaluations_considered": len(records),
        "total_pair_count": len(pair_id_counts),
        # scoped eligibility totals
        "experiment_eligible_unmatured_count": experiment_eligible_count,
        "campaign_eligible_unmatured_count": len(campaign_verifiable),
        # what is actually excluded from THIS scope's manifest (campaign scope
        # excludes the no-telemetry pairs; experiment scope excludes nothing).
        "excluded_non_campaign_eligible_count": len(excluded_entries),
        "non_campaign_eligible_count": len(campaign_none),
        "campaign_conflicting_eligible_count": len(campaign_conflicting),
        # the scope's executable eligible count
        "eligible_unmatured_count": manifest_total,
        "retryable_failure_count": eligibility["counts"][ELIGIBILITY_RETRYABLE],
        "terminal_failure_count": terminal_count,
        "not_yet_eligible_count": eligibility["counts"][ELIGIBILITY_NOT_YET],
        "missing_market_session_data_count": missing_session_count,
        "eligibility": eligibility,
        # pagination
        "manifest_total": manifest_total,
        "returned_count": len(page),
        "limit": page_limit,
        "offset": page_offset,
        "next_offset": next_offset,
        "has_more": has_more,
        "manifest_ordering": MANIFEST_ORDERING,
        "manifest_hash_version": MANIFEST_HASH_VERSION,
        "manifest_hash": manifest_hash,
        # stable cohort-membership lock (outcome-status blind; never changes on
        # a valid outcome write)
        "cohort_lock_hash": cohort_lock_hash,
        "cohort_lock_hash_version": COHORT_LOCK_HASH_VERSION,
        "cohort_pair_count": cohort_pair_count,
        # dynamic remaining-work manifest (shrinks after every successful batch)
        "remaining_manifest_hash": remaining_manifest_hash,
        "remaining_manifest_hash_version": MANIFEST_HASH_VERSION,
        "remaining_pair_count": remaining_pair_count,
        "normal_execution_complete": normal_execution_complete,
        "next_batch": next_batch,
        # collections
        "eligible_manifest": page,
        "excluded_non_campaign_evidence": excluded_block,
        "retry_plan": {
            "contract_version": RETRY_PLAN_CONTRACT_VERSION,
            "retry_plan_hash": compute_retry_plan_hash(cohort_identity, retry_entries),
            "retryable_failure_count": sum(1 for e in retry_entries if e["retryable"]),
            "terminal_failure_count": sum(
                1 for e in retry_entries if not e["retryable"]),
            "entries": retry_entries,
        },
        "duplicate_investigation": duplicate_audit,
        "membership": {
            "campaign_membership_verifiable_count": manifest_verifiable,
            "campaign_membership_unverifiable_count": manifest_unverifiable,
            "pairs_by_campaign": dict(sorted(pairs_by_campaign.items())),
        },
        "planning": {
            "safe_to_execute": safe_to_execute,
            "blocking_reasons": sorted(set(blocking)),
            "recommended_batch_size": batch_size,
            "recommended_batch_count": batch_count,
            "full_batch_count": full_batch_count,
            "final_batch_size": final_batch_size,
            "ordering": MANIFEST_ORDERING,
            "calculation_selector": "explicit_pair_ids",
            "requires_provider": True,
            "requires_write_capable_database_role": True,
            "requires_include_recalc": False,
            "batch_sizing_rationale": {
                "massive_requests_per_minute": 5,
                "min_provider_calls_per_pair": 1,
                "endpoint_hard_cap": 200,
                "one_http_request_open_per_batch": True,
                "note": (
                    "Bounded so one synchronous request stays within provider "
                    "throttle and proxy/connection timeouts; never 50 by habit, "
                    "never the 200 hard cap."
                ),
            },
            "cannot_execute_here": True,
            "cannot_execute_reason": (
                "audit-only staging: SELECT-only database role and no provider "
                "credentials — this endpoint only PLANS, it never matures."
            ),
        },
    }


__all__ = [
    "MATURATION_PLAN_CONTRACT_VERSION",
    "MANIFEST_HASH_VERSION",
    "RETRY_PLAN_CONTRACT_VERSION",
    "DUPLICATE_AUDIT_CONTRACT_VERSION",
    "MANIFEST_ORDERING",
    "DEFAULT_PAGE_LIMIT",
    "MAX_PAGE_LIMIT",
    "RECOMMENDED_MATURATION_BATCH_SIZE",
    "COHORT_SCOPE_CAMPAIGN",
    "COHORT_SCOPE_EXPERIMENT",
    "COHORT_SCOPES",
    "CAMPAIGN_MEMBERSHIP_VERIFIABLE",
    "CAMPAIGN_MEMBERSHIP_NONE",
    "CAMPAIGN_MEMBERSHIP_CONFLICTING",
    "classify_campaign_membership",
    "DUP_BENIGN_CROSS_CAMPAIGN",
    "DUP_WITHIN_SAME_CAMPAIGN",
    "DUP_WITHIN_SAME_RUN",
    "DUP_IDENTITY_MISMATCH",
    "DUP_UNVERIFIABLE",
    "BLOCKING_DUPLICATE_CLASSES",
    "COHORT_LOCK_HASH_VERSION",
    "NEXT_BATCH_HASH_VERSION",
    "compute_manifest_hash",
    "compute_cohort_lock_hash",
    "compute_next_batch_hash",
    "compute_retry_plan_hash",
    "classify_duplicate_group",
    "build_maturation_plan",
]
