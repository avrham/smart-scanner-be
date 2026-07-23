"""Phase 9F7: bounded deterministic evidence export."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from app.workers.shadow.evidence_export import (
    EXPORT_CONTRACT_VERSION,
    MAX_EXPORT_RECORD_REFERENCES,
    build_evidence_export,
)
from app.workers.shadow.evidence_review import normalize_evidence_filters
from app.workers.shadow.outcome_evidence import build_outcome_evidence
from app.workers.shadow.quality_audit import build_quality_audit
from app.workers.shadow.rollout_readiness import (
    evaluate_rollout_readiness,
    resolve_thresholds,
)

from test_wyckoff_v2_9f_cohorts import evidence_record, trigger_record
from test_wyckoff_v2_9f_outcome_evidence import outcome_row
from test_wyckoff_v2_9f_readiness import LOW_BOUND_OVERRIDES


def _records(n: int = 6) -> List[Dict[str, Any]]:
    return [
        evidence_record(
            symbol=f"S{i:02d}", snapshot="2026-07-16",
            trigger=trigger_record("confirmed", price=50.0) if i == 0
            else trigger_record("missing"),
            has_outcome=(i % 2 == 0),
            outcome_status="complete" if i % 2 == 0 else None,
            campaign_ids=["camp-1"],
        )
        for i in range(n)
    ]


def _export(records=None, generated_at="2026-07-23T10:00:00+00:00", **kwargs):
    records = _records() if records is None else records
    rows = [outcome_row(ret_1d=1.0, status="complete")]
    filters = normalize_evidence_filters()
    audit = build_quality_audit(records, outcome_rows=rows)
    readiness = evaluate_rollout_readiness(
        records, outcome_rows=rows, quality_audit=audit,
        thresholds=resolve_thresholds(LOW_BOUND_OVERRIDES),
    )
    return build_evidence_export(
        filters=filters,
        records=records,
        outcome_evidence=build_outcome_evidence(
            rows, missing_outcome_count=3
        ),
        quality_audit=audit,
        readiness=readiness,
        campaign_runs=[{
            "run_id": "run-1", "status": "completed",
            "campaign": {"campaign_id": "camp-1"},
        }],
        generated_at=generated_at,
        **kwargs,
    )


class TestExportDeterminism:
    def test_identical_data_identical_content(self):
        first = _export(generated_at="2026-07-23T10:00:00+00:00")
        second = _export(generated_at="2026-07-24T18:30:00+00:00")
        # The generation timestamp is separated from the evidence body:
        # identical stored data + filters -> identical content and hash.
        assert first["evidence"] == second["evidence"]
        assert first["content_sha256"] == second["content_sha256"]
        assert first["generated_at"] != second["generated_at"]

    def test_changed_data_changes_hash(self):
        first = _export()
        changed = _records()
        changed[0]["verdict"] = "AVOID"
        second = _export(records=changed)
        assert first["content_sha256"] != second["content_sha256"]

    def test_body_is_json_serializable(self):
        export = _export()
        json.dumps(export["evidence"], allow_nan=False)


class TestExportContent:
    def test_contract_and_sections(self):
        body = _export()["evidence"]
        assert body["export_contract_version"] == EXPORT_CONTRACT_VERSION
        for section in (
            "filters", "readiness", "cohorts", "outcome_evidence",
            "quality_audit", "failure_distributions",
            "version_distributions", "campaign_coverage",
            "record_references", "rollout_defaults",
        ):
            assert section in body, section
        assert body["rollout_mutation_performed"] is False
        assert body["rollout_defaults"]["allow_enter"] is False
        assert body["rollout_defaults"]["patterns.is_enabled"] is False
        assert body["campaign_coverage"]["campaign_ids"] == ["camp-1"]

    def test_record_references_are_bounded(self):
        many = [
            evidence_record(symbol=f"S{i:03d}", snapshot="2026-07-16")
            for i in range(MAX_EXPORT_RECORD_REFERENCES + 40)
        ]
        export = _export(records=many)
        body = export["evidence"]
        assert body["record_reference_count"] == MAX_EXPORT_RECORD_REFERENCES
        assert body["record_references_truncated"] is True
        assert len(body["record_references"]) == MAX_EXPORT_RECORD_REFERENCES
        reference = body["record_references"][0]
        # Bounded identifiers only — never frozen snapshots.
        assert set(reference) == {
            "evaluation_id", "pair_id", "symbol", "snapshot_date",
            "verdict", "strategy_version", "config_hash", "outcome_status",
        }

    def test_reference_bound_can_be_narrowed_never_widened(self):
        export = _export(max_record_references=2)
        assert export["evidence"]["record_reference_count"] == 2
        widened = _export(max_record_references=10_000)
        assert widened["evidence"]["record_reference_count"] <= (
            MAX_EXPORT_RECORD_REFERENCES
        )
        with pytest.raises(ValueError):
            _export(max_record_references=0)


class TestExportSafety:
    def test_no_secret_shaped_keys_anywhere(self):
        body = _export()["evidence"]

        def _walk(value, path="$"):
            if isinstance(value, dict):
                for key, item in value.items():
                    lowered = str(key).lower()
                    for fragment in ("api_key", "password", "secret",
                                     "token", "dsn"):
                        assert fragment not in lowered, (path, key)
                    _walk(item, f"{path}.{key}")
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    _walk(item, f"{path}[{i}]")

        _walk(body)

    def test_record_extras_never_reach_the_export(self):
        # Bounded references strip everything but identifiers: even a
        # poisoned record field cannot flow into the export body.
        records = _records()
        records[0]["api_key_leak"] = "oops"
        text = json.dumps(_export(records=records)["evidence"])
        assert "api_key_leak" not in text
        assert "oops" not in text

    def test_secret_shaped_content_is_refused(self):
        from app.workers.shadow.evidence_export import build_evidence_export
        from app.workers.shadow.evidence_review import (
            normalize_evidence_filters,
        )

        poisoned_audit = build_quality_audit([])
        poisoned_audit["api_key"] = "leak"
        with pytest.raises(ValueError, match="secret-shaped"):
            build_evidence_export(
                filters=normalize_evidence_filters(),
                records=[],
                outcome_evidence=build_outcome_evidence([]),
                quality_audit=poisoned_audit,
                readiness={},
                generated_at="2026-07-23T10:00:00+00:00",
            )

    def test_no_credential_values_in_serialized_export(self):
        text = json.dumps(_export()["evidence"]).lower()
        for fragment in ("supabase", "massive_api", "fmp_api",
                         "worker_token", "postgresql://"):
            assert fragment not in text, fragment
