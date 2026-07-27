"""Read-only shadow pair lineage audit (shadow_pair_lineage.v1).

Narrowly scoped provenance reconstruction for a bounded set of explicit pair
IDs: for each pair it joins the pair row, its arm evaluations, every run the
pair is linked to (origin + run_pairs links), each run's bounded/redacted
telemetry (campaign block only, never an unrelated JSON dump), the run's sibling
pairs/evaluations, and the outcome status — then deterministically classifies
WHY the pair does or does not carry campaign membership.

It exists to resolve the two membership-unverifiable eligible pairs the
maturation plan surfaced: a pair has empty `campaign_ids` only when NONE of its
linked runs carries a `telemetry.campaign` block. That can mean several
legitimate things (a manual/direct shadow run, a pre-campaign experiment run) or
a real problem (a campaign chunk whose telemetry was lost, a mis-tagged or
orphan record). This module distinguishes them from persisted evidence alone,
never from symbol overlap or date guessing.

Strictly read-only: the reads issue only SELECTs; the classifier is PURE. It
never mutates a row, never calls a provider, never backfills telemetry.

Chronology used by the classifier (all 2026-07-23, from git history):
  * 16:46 experiment_code / experiment registry (commit 0c2ed26)
  * 16:47 manual /strategies/{code}/shadow-run + dry-run (commit e41ca05)
  * 18:22 shadow campaigns + campaign telemetry block (commit 66c8a56)
So a run tagged an experiment_code with NO campaign block created before 18:22
is a legitimate pre-campaign experiment run; after 18:22 (campaigns existed and
always write their block on completion) a COMPLETED non-campaign run is a
deliberate manual/direct run.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


PAIR_LINEAGE_CONTRACT_VERSION = "shadow_pair_lineage.v1"

# Max pair IDs one request may inspect (bounded response).
MAX_LINEAGE_PAIR_IDS = 20
# Bounded caps so a pathological run can never grow the response without bound.
MAX_SIBLING_SYMBOLS = 60
MAX_TELEMETRY_KEYS = 40

# When campaign telemetry began being persisted (commit 66c8a56). A completed
# non-campaign run created at/after this instant was NOT a campaign chunk (a
# completed chunk always writes its block); before it, campaigns did not exist.
CAMPAIGN_TELEMETRY_INTRODUCED_AT = datetime(
    2026, 7, 23, 18, 22, 39, tzinfo=timezone.utc
)

# Classifications (closed vocabulary — step 9).
CLASS_LEGIT_MISSING_TELEMETRY = "legitimate_campaign_record_with_missing_telemetry"
CLASS_LEGACY_PRE_CAMPAIGN = "legacy_experiment_run_before_campaign_telemetry"
CLASS_MANUAL_NON_CAMPAIGN = "manual_non_campaign_shadow_run"
CLASS_INCORRECTLY_TAGGED = "incorrectly_tagged_experiment_record"
CLASS_ORPHAN_INCONSISTENT = "orphan_or_inconsistent_record"
CLASS_UNVERIFIABLE = "unverifiable"

# Resolutions (closed vocabulary — step 10).
RES_BACKFILL = "backfill_campaign_metadata"
RES_RETAIN_LEGACY = "retain_as_legacy_non_campaign_evidence"
RES_EXCLUDE_RETAIN = "exclude_from_campaign_cohort_but_retain_record"
RES_REPAIR_IDENTITY = "repair_inconsistent_identity"
RES_COLLECT_MORE = "do_not_modify_collect_more_evidence"


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _to_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _telemetry_inventory(telemetry: Any) -> Dict[str, Any]:
    """Bounded, redacted view of a run's telemetry.

    Returns the sorted top-level key inventory, the (small, safe) campaign block
    verbatim, and a few safe scalar rollups — never an unrelated JSON dump.
    """
    if not isinstance(telemetry, dict):
        return {
            "campaign_telemetry_present": False,
            "telemetry_keys": [],
            "campaign": None,
            "pair_count": None,
            "as_of_date": None,
        }
    keys = sorted(str(k) for k in telemetry.keys())[:MAX_TELEMETRY_KEYS]
    campaign = telemetry.get("campaign")
    campaign_block = campaign if isinstance(campaign, dict) else None
    return {
        "campaign_telemetry_present": bool(campaign_block),
        "telemetry_keys": keys,
        "campaign": campaign_block,  # already bounded/safe (ids + chunk meta)
        "pair_count": telemetry.get("pair_count"),
        "as_of_date": telemetry.get("as_of_date")
        or (campaign_block or {}).get("as_of_date"),
    }


async def read_pair_lineage(conn, pair_ids: List[str]) -> Dict[str, Any]:
    """Read-only lineage rows for the requested pair IDs (SELECT only).

    Returns raw structured data keyed by pair_id plus a shared runs map. Never
    writes, never constructs a provider. Bounded by MAX_LINEAGE_PAIR_IDS upstream.
    """
    pair_rows = await conn.fetch(
        """
        SELECT id, symbol, snapshot_date, created_at, experiment_code,
               experiment_version, origin_run_id, provider
        FROM strategy_shadow_pairs
        WHERE id = ANY($1::uuid[])
        """,
        pair_ids,
    )
    evals = await conn.fetch(
        """
        SELECT id, pair_id, arm_code, strategy_code, strategy_version,
               decision_policy_version, config_hash, verdict, created_at
        FROM strategy_shadow_evaluations
        WHERE pair_id = ANY($1::uuid[])
        """,
        pair_ids,
    )
    links = await conn.fetch(
        """
        SELECT run_id, pair_id, created_new_pair, linked_at
        FROM strategy_shadow_run_pairs
        WHERE pair_id = ANY($1::uuid[])
        """,
        pair_ids,
    )
    outcomes = await conn.fetch(
        """
        SELECT pair_id, outcome_status
        FROM strategy_shadow_pair_outcomes
        WHERE pair_id = ANY($1::uuid[])
        """,
        pair_ids,
    )

    # The full run set: each pair's origin run + every linked run.
    run_id_set = set()
    for r in pair_rows:
        if r["origin_run_id"]:
            run_id_set.add(str(r["origin_run_id"]))
    for l in links:
        run_id_set.add(str(l["run_id"]))
    run_ids = sorted(run_id_set)

    runs = await conn.fetch(
        """
        SELECT id, experiment_code, experiment_version, status, provider,
               requested_symbols, requested_limit, started_at, finished_at,
               created_at, error_code, telemetry
        FROM strategy_shadow_runs
        WHERE id = ANY($1::uuid[])
        """,
        run_ids,
    ) if run_ids else []

    # Sibling rollups per run (bounded): every pair linked to the run + eval count.
    siblings = await conn.fetch(
        """
        SELECT rp.run_id, p.id AS pair_id, p.symbol, p.snapshot_date,
               (SELECT count(*) FROM strategy_shadow_evaluations e
                WHERE e.pair_id = p.id) AS eval_count
        FROM strategy_shadow_run_pairs rp
        JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
        WHERE rp.run_id = ANY($1::uuid[])
        """,
        run_ids,
    ) if run_ids else []

    return {
        "pair_rows": [dict(r) for r in pair_rows],
        "evals": [dict(r) for r in evals],
        "links": [dict(r) for r in links],
        "outcomes": {str(r["pair_id"]): r["outcome_status"] for r in outcomes},
        "runs": {str(r["id"]): dict(r) for r in runs},
        "siblings": [dict(r) for r in siblings],
    }


def _format_run(run: Dict[str, Any], siblings: List[Dict[str, Any]]) -> Dict[str, Any]:
    # asyncpg returns jsonb columns as JSON strings; decode telemetry (and
    # requested_symbols below) before inspecting. Read-only decode only.
    from app.workers.shadow.persistence import _maybe_json  # local, read-only helper

    inv = _telemetry_inventory(_maybe_json(run.get("telemetry")))
    sib = [s for s in siblings if str(s["run_id"]) == str(run["id"])]
    symbols = sorted({s["symbol"] for s in sib})
    campaign = inv["campaign"] or {}

    return {
        "run_id": str(run["id"]),
        "run_created_at": _iso(run.get("created_at")),
        "run_started_at": _iso(run.get("started_at")),
        "run_finished_at": _iso(run.get("finished_at")),
        "run_status": run.get("status"),
        "run_experiment_code": run.get("experiment_code"),
        "run_experiment_version": run.get("experiment_version"),
        "run_error_code": run.get("error_code"),
        "run_requested_symbols": _maybe_json(run.get("requested_symbols")),
        "run_requested_limit": run.get("requested_limit"),
        "run_pair_count": len(sib),
        "run_evaluation_count": sum(int(s["eval_count"] or 0) for s in sib),
        "run_sibling_symbols": symbols[:MAX_SIBLING_SYMBOLS],
        "run_sibling_symbol_count": len(symbols),
        "run_sibling_symbols_truncated": len(symbols) > MAX_SIBLING_SYMBOLS,
        # telemetry (bounded / redacted)
        "campaign_telemetry_present": inv["campaign_telemetry_present"],
        "telemetry_keys": inv["telemetry_keys"],
        "campaign_id": campaign.get("campaign_id"),
        "campaign_chunk_index": campaign.get("chunk_index"),
        "campaign_chunk_count": campaign.get("chunk_count"),
        "campaign_as_of_date": campaign.get("as_of_date") or inv["as_of_date"],
        "campaign_requested_count": campaign.get("requested_count"),
        "campaign_contract_version": campaign.get("campaign_contract_version"),
    }


def classify_pair_lineage(
    view: Dict[str, Any],
    *,
    campaign_telemetry_introduced_at: datetime = CAMPAIGN_TELEMETRY_INTRODUCED_AT,
) -> Dict[str, Any]:
    """PURE deterministic classification + single resolution for one pair view."""
    runs = view["runs"]
    evals = view["evaluations"]
    evidence: List[str] = []

    # ---- relationship integrity ------------------------------------------- #
    if view.get("pair_exists") is False:
        return {
            "classification": CLASS_ORPHAN_INCONSISTENT,
            "confidence": "high",
            "evidence": ["pair_id not found in strategy_shadow_pairs"],
            "recommended_resolution": RES_COLLECT_MORE,
            "resolution_detail": "pair row absent; nothing to resolve here",
            "deterministic_campaign_assignable": False,
            "assigned_campaign_id": None,
        }
    if not runs:
        evidence.append("pair has no linked run (origin or run_pairs)")
        return _verdict(view, CLASS_ORPHAN_INCONSISTENT, "high", evidence,
                        RES_REPAIR_IDENTITY,
                        "missing pair->run relationship; needs a known correct "
                        "source before any repair", False, None)
    if not evals:
        evidence.append("pair has no evaluation rows")
        return _verdict(view, CLASS_ORPHAN_INCONSISTENT, "high", evidence,
                        RES_REPAIR_IDENTITY,
                        "missing pair->evaluation relationship", False, None)

    # ---- experiment coherence --------------------------------------------- #
    pair_exp = view.get("experiment_code")
    run_exps = {r["run_experiment_code"] for r in runs}
    if len(run_exps | {pair_exp}) > 1:
        evidence.append(
            f"conflicting experiment identity: pair={pair_exp}, runs={sorted(run_exps)}"
        )
        return _verdict(view, CLASS_INCORRECTLY_TAGGED, "medium", evidence,
                        RES_COLLECT_MORE,
                        "experiment identity disagrees across pair/run; a correct "
                        "source is not deterministically known", False, None)

    # ---- campaign membership recoverable? --------------------------------- #
    campaign_runs = [r for r in runs if r["campaign_telemetry_present"]]
    if campaign_runs:
        cids = sorted({r["campaign_id"] for r in campaign_runs if r["campaign_id"]})
        if len(cids) == 1:
            evidence.append(
                f"linked run carries campaign telemetry campaign_id={cids[0]}"
            )
            return _verdict(view, CLASS_LEGIT_MISSING_TELEMETRY, "high", evidence,
                            RES_BACKFILL,
                            f"deterministic single campaign {cids[0]} present on a "
                            "linked run", True, cids[0])
        evidence.append(f"multiple campaign ids on linked runs: {cids}")
        return _verdict(view, CLASS_UNVERIFIABLE, "low", evidence, RES_COLLECT_MORE,
                        "more than one campaign linked; not deterministic",
                        False, None)

    # ---- no campaign block on ANY linked run ------------------------------ #
    statuses = {r["run_status"] for r in runs}
    all_completed = statuses == {"completed"}
    created = [_to_dt(r["run_created_at"]) for r in runs]
    created = [c for c in created if c is not None]
    earliest = min(created) if created else None
    before_campaigns = (
        earliest is not None and earliest < campaign_telemetry_introduced_at
    )
    evidence.append(
        f"no linked run carries a campaign telemetry block "
        f"(run statuses={sorted(statuses)})"
    )

    if not all_completed:
        # A failed/running run could be a campaign chunk whose telemetry never
        # persisted — but nothing here PROVES a single campaign, so stay honest.
        evidence.append(
            "at least one linked run is not 'completed' — telemetry may be "
            "incomplete, but no single campaign is deterministically provable"
        )
        return _verdict(view, CLASS_UNVERIFIABLE, "low", evidence, RES_COLLECT_MORE,
                        "non-completed run without campaign telemetry; cannot "
                        "distinguish a lost-telemetry campaign chunk from a "
                        "manual run", False, None)

    # Completed run(s), coherent experiment identity, no campaign block: a
    # completed campaign chunk ALWAYS writes its block, so this was deliberately
    # non-campaign. Legacy (pre-campaign) vs manual (post-campaign) by time.
    if before_campaigns:
        evidence.append(
            f"earliest linked run created {_iso(earliest)} predates campaign "
            f"telemetry ({_iso(campaign_telemetry_introduced_at)})"
        )
        return _verdict(view, CLASS_LEGACY_PRE_CAMPAIGN, "high", evidence,
                        RES_RETAIN_LEGACY,
                        "valid experiment evidence created before campaigns "
                        "existed; it legitimately has no campaign — the "
                        "maturation-plan contract should model an explicit "
                        "legacy-evidence class, not pretend campaign membership",
                        False, None)
    evidence.append(
        f"completed run(s) tagged {pair_exp} created at/after campaign telemetry "
        "existed but carry no campaign block — the direct/manual shadow-run path "
        "(experiment tagging permitted, no campaign intended)"
    )
    return _verdict(view, CLASS_MANUAL_NON_CAMPAIGN, "high", evidence,
                    RES_EXCLUDE_RETAIN,
                    "valid manual/non-campaign run; exclude from the CAMPAIGN "
                    "cohort via a cohort-definition rule (require a linked run "
                    "with a campaign block) — never delete or alter the record",
                    False, None)


def _verdict(view, classification, confidence, evidence, resolution, detail,
             assignable, campaign_id) -> Dict[str, Any]:
    return {
        "classification": classification,
        "confidence": confidence,
        "evidence": evidence,
        "recommended_resolution": resolution,
        "resolution_detail": detail,
        "deterministic_campaign_assignable": assignable,
        "assigned_campaign_id": campaign_id,
        "backfill_uniqueness_guard": (
            "a future backfill MUST be idempotent on (pair_id) and refuse if the "
            "pair is already linked to a different campaign_id"
            if resolution == RES_BACKFILL else None
        ),
    }


def build_pair_lineage(raw: Dict[str, Any], requested_pair_ids: List[str]) -> Dict[str, Any]:
    """PURE assembly of the versioned lineage response + per-pair classification."""
    pair_rows = {str(p["id"]): p for p in raw["pair_rows"]}
    evals_by_pair: Dict[str, List[Dict[str, Any]]] = {}
    for e in raw["evals"]:
        evals_by_pair.setdefault(str(e["pair_id"]), []).append(e)
    links_by_pair: Dict[str, List[Dict[str, Any]]] = {}
    for l in raw["links"]:
        links_by_pair.setdefault(str(l["pair_id"]), []).append(l)

    pairs_out: List[Dict[str, Any]] = []
    for pid in requested_pair_ids:
        prow = pair_rows.get(pid)
        if prow is None:
            view = {
                "pair_id": pid, "pair_exists": False, "runs": [], "evaluations": [],
                "experiment_code": None,
            }
            pairs_out.append({
                "pair_id": pid, "found": False,
                "lineage": None,
                **classify_pair_lineage(view),
            })
            continue

        # linked run ids = origin + run_pairs links
        run_ids: List[str] = []
        if prow.get("origin_run_id"):
            run_ids.append(str(prow["origin_run_id"]))
        for l in links_by_pair.get(pid, []):
            if str(l["run_id"]) not in run_ids:
                run_ids.append(str(l["run_id"]))

        formatted_runs = [
            _format_run(raw["runs"][rid], raw["siblings"])
            for rid in run_ids if rid in raw["runs"]
        ]
        evaluations = [
            {
                "evaluation_id": str(e["id"]),
                "arm_code": e["arm_code"],
                "strategy_code": e["strategy_code"],
                "strategy_version": e["strategy_version"],
                "decision_policy_version": e["decision_policy_version"],
                "config_hash": e["config_hash"],
                "verdict": e["verdict"],
                "evaluation_created_at": _iso(e["created_at"]),
            }
            for e in evals_by_pair.get(pid, [])
        ]
        view = {
            "pair_id": pid,
            "pair_exists": True,
            "symbol": prow["symbol"],
            "snapshot_date": _iso(prow["snapshot_date"]),
            "pair_created_at": _iso(prow["created_at"]),
            "experiment_code": prow["experiment_code"],
            "experiment_version": prow["experiment_version"],
            "origin_run_id": str(prow["origin_run_id"]) if prow["origin_run_id"] else None,
            "outcome_status": raw["outcomes"].get(pid),
            "runs": formatted_runs,
            "evaluations": evaluations,
        }
        verdict = classify_pair_lineage(view)
        pairs_out.append({
            "pair_id": pid,
            "found": True,
            "lineage": view,
            **verdict,
        })

    return {
        "contract_version": PAIR_LINEAGE_CONTRACT_VERSION,
        "requested_pair_count": len(requested_pair_ids),
        "found_pair_count": sum(1 for p in pairs_out if p["found"]),
        "campaign_telemetry_introduced_at": _iso(CAMPAIGN_TELEMETRY_INTRODUCED_AT),
        "pairs": pairs_out,
    }


__all__ = [
    "PAIR_LINEAGE_CONTRACT_VERSION",
    "MAX_LINEAGE_PAIR_IDS",
    "CAMPAIGN_TELEMETRY_INTRODUCED_AT",
    "CLASS_LEGIT_MISSING_TELEMETRY",
    "CLASS_LEGACY_PRE_CAMPAIGN",
    "CLASS_MANUAL_NON_CAMPAIGN",
    "CLASS_INCORRECTLY_TAGGED",
    "CLASS_ORPHAN_INCONSISTENT",
    "CLASS_UNVERIFIABLE",
    "RES_BACKFILL",
    "RES_RETAIN_LEGACY",
    "RES_EXCLUDE_RETAIN",
    "RES_REPAIR_IDENTITY",
    "RES_COLLECT_MORE",
    "read_pair_lineage",
    "classify_pair_lineage",
    "build_pair_lineage",
]
