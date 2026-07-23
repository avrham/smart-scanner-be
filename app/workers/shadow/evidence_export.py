"""Bounded operator evidence export (Phase 9F7).

Contract: shadow_evidence_export.v1.

Assembles the full evidence package for human review: cohorts, outcome and
benchmark evidence, quality audit, readiness decision, version and failure
distributions, campaign coverage and BOUNDED record references. The
deterministic body is separated from the generation timestamp and hashed
(canonical JSON SHA-256) so identical stored data + identical filters
always produce byte-identical evidence content.

Never contains credentials, tokens, raw secrets, unbounded snapshots or
mutable configuration objects. All functions are pure (no I/O).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.workers.provenance import canonical_json, _sha256
from app.workers.shadow.evidence_cohorts import build_cohorts
from app.workers.shadow.evidence_review import filters_for_response
from app.workers.shadow.serialization import normalize_json_safe


EXPORT_CONTRACT_VERSION = "shadow_evidence_export.v1"

# Hard bound on exported record references.
MAX_EXPORT_RECORD_REFERENCES = 200

_FORBIDDEN_KEY_FRAGMENTS = ("api_key", "password", "token", "secret", "dsn")


def _record_reference(record: Dict[str, Any]) -> Dict[str, Any]:
    """Bounded reference to one frozen evaluation — identifiers only."""
    snapshot = record.get("snapshot_date")
    iso = getattr(snapshot, "isoformat", None)
    return {
        "evaluation_id": record.get("evaluation_id"),
        "pair_id": record.get("pair_id"),
        "symbol": record.get("symbol"),
        "snapshot_date": iso() if callable(iso) else snapshot,
        "verdict": record.get("verdict"),
        "strategy_version": record.get("strategy_version"),
        "config_hash": record.get("config_hash"),
        "outcome_status": record.get("outcome_status"),
    }


def _assert_no_secret_keys(value: Any, path: str = "$") -> None:
    """Defense in depth: refuse to export anything secret-shaped."""
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            for fragment in _FORBIDDEN_KEY_FRAGMENTS:
                if fragment in lowered:
                    raise ValueError(
                        f"refusing to export secret-shaped key at "
                        f"{path}.{key}"
                    )
            _assert_no_secret_keys(item, f"{path}.{key}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            _assert_no_secret_keys(item, f"{path}[{i}]")


def build_evidence_export(
    *,
    filters: Dict[str, Any],
    records: List[Dict[str, Any]],
    outcome_evidence: Dict[str, Any],
    quality_audit: Dict[str, Any],
    readiness: Dict[str, Any],
    campaign_runs: Optional[List[Dict[str, Any]]] = None,
    generated_at: Optional[str] = None,
    max_record_references: int = MAX_EXPORT_RECORD_REFERENCES,
) -> Dict[str, Any]:
    """Assemble the bounded deterministic evidence package.

    The deterministic `evidence` body excludes `generated_at`;
    `content_sha256` is the canonical-JSON hash of that body, so two exports
    over identical stored data and filters are provably identical.
    """
    bound = min(int(max_record_references), MAX_EXPORT_RECORD_REFERENCES)
    if bound < 1:
        raise ValueError("max_record_references must be positive")

    campaigns = campaign_runs or []
    campaign_summary = {
        "campaign_run_count": len(campaigns),
        "campaign_ids": sorted({
            str((run.get("campaign") or {}).get("campaign_id"))
            for run in campaigns
            if (run.get("campaign") or {}).get("campaign_id")
        }),
        "run_status_distribution": {},
    }
    for run in campaigns:
        status = str(run.get("status"))
        campaign_summary["run_status_distribution"][status] = (
            campaign_summary["run_status_distribution"].get(status, 0) + 1
        )
    campaign_summary["run_status_distribution"] = dict(
        sorted(campaign_summary["run_status_distribution"].items())
    )

    cohorts = build_cohorts(records)
    references = [_record_reference(r) for r in records[:bound]]

    body: Dict[str, Any] = {
        "export_contract_version": EXPORT_CONTRACT_VERSION,
        "filters": filters_for_response(filters),
        "readiness": readiness,
        "cohorts": cohorts,
        "outcome_evidence": outcome_evidence,
        "quality_audit": quality_audit,
        "failure_distributions": {
            "rejection_reasons": cohorts["cohorts"]["evaluated"][
                "reason_distribution"
            ],
        },
        "version_distributions": {
            "strategy_versions": cohorts["cohorts"]["evaluated"][
                "strategy_version_distribution"
            ],
            "decision_policy_versions": cohorts["cohorts"]["evaluated"][
                "decision_policy_version_distribution"
            ],
            "config_hashes": cohorts["cohorts"]["evaluated"][
                "config_hash_distribution"
            ],
            "daily_frame_contracts": cohorts["cohorts"]["evaluated"][
                "daily_frame_contract_distribution"
            ],
            "four_hour_frame_contracts": cohorts["cohorts"]["evaluated"][
                "four_hour_frame_contract_distribution"
            ],
        },
        "campaign_coverage": campaign_summary,
        "record_reference_count": len(references),
        "record_references_truncated": len(records) > bound,
        "record_references": references,
        # Explicit rollout-default snapshot: the export itself certifies
        # that nothing was enabled while producing it.
        "rollout_defaults": {
            "patterns.is_enabled": False,
            "allow_enter": False,
            "enable_4h_trigger": False,
            "min_price": 5.0,
        },
        "rollout_mutation_performed": False,
    }

    body = normalize_json_safe(body)
    _assert_no_secret_keys(body)
    return {
        "generated_at": generated_at,
        "content_sha256": _sha256(canonical_json(body)),
        "evidence": body,
    }
