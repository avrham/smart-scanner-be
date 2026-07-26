"""Historical cohort closeout audit (shadow_cohort_closeout.v1).

Proves the closeout report distinguishes not-yet-eligible outcomes from actual
failures, counts per-horizon/per-status maturation, surfaces the single
forward_fetch_error and provider failures, detects duplicates, and reuses the
existing metrics + quality machinery rather than a parallel implementation.
"""

from __future__ import annotations

import copy
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from app.workers.shadow.cohort_closeout import (
    COHORT_CLOSEOUT_CONTRACT_VERSION,
    build_cohort_closeout_audit,
)

from test_wyckoff_v2_9f_cohorts import evidence_record


def _weekday_calendar(start: date, n: int) -> List[date]:
    out: List[date] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _outcome_row(
    *,
    symbol: str,
    pair_id: str,
    snapshot: str = "2026-05-04",
    status: str = "partial",
    error_code: Optional[str] = None,
    returns: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "pair": {"pair_id": pair_id, "symbol": symbol, "snapshot_date": snapshot},
        "outcome": {
            "outcome_status": status,
            "error_code": error_code,
            "returns": returns or {
                "1D": None, "3D": None, "5D": None, "10D": None, "20D": None,
            },
        },
    }


# A generous old calendar so old snapshots are comfortably eligible.
CALENDAR = _weekday_calendar(date(2026, 5, 1), 60)


def _rec(symbol, pair_id, snapshot="2026-05-04", status=None):
    r = evidence_record(
        symbol=symbol, snapshot=snapshot,
        has_outcome=status is not None, outcome_status=status,
        campaign_ids=["camp-1"],
    )
    r["pair_id"] = pair_id
    return r


class TestCloseoutAggregation:
    def _cohort(self):
        records = [
            _rec("AAAX", "p1", status="complete"),
            _rec("BBBX", "p2", status="partial"),
            _rec("CCCX", "p3", status="error"),
            _rec("DDDX", "p4", status=None),          # missing row, old → eligible
            _rec("EEEX", "p5", "2026-07-24", None),   # recent → not yet eligible
        ]
        outcome_rows = [
            _outcome_row(symbol="AAAX", pair_id="p1", status="complete",
                         returns={"1D": 0.1, "3D": 0.1, "5D": 0.1,
                                  "10D": 0.1, "20D": 0.1}),
            _outcome_row(symbol="BBBX", pair_id="p2", status="partial",
                         returns={"1D": 0.1, "3D": 0.1, "5D": None,
                                  "10D": None, "20D": None}),
            _outcome_row(symbol="CCCX", pair_id="p3", status="error",
                         error_code="forward_fetch_error"),
            _outcome_row(symbol="GGGX", pair_id="p9", status="error",
                         error_code="provider_mismatch"),
        ]
        return records, outcome_rows

    def test_contract_and_totals(self):
        records, rows = self._cohort()
        audit = build_cohort_closeout_audit(
            records, rows, session_dates=CALENDAR,
            latest_completed_session=CALENDAR[-1], campaign_ids=["camp-1"],
        )
        assert audit["closeout_contract_version"] == (
            COHORT_CLOSEOUT_CONTRACT_VERSION
        )
        assert audit["total_evaluations"] == 5
        assert audit["total_outcome_rows"] == 4
        assert audit["campaign_ids"] == ["camp-1"]

    def test_status_and_horizon_grouping(self):
        records, rows = self._cohort()
        audit = build_cohort_closeout_audit(records, rows,
                                            session_dates=CALENDAR)
        assert audit["outcome_status_distribution"] == {
            "complete": 1, "error": 2, "partial": 1,
        }
        # AAAX has all horizons; BBBX has 1D+3D only.
        assert audit["matured_outcomes_by_horizon"] == {
            "1D": 2, "3D": 2, "5D": 1, "10D": 1, "20D": 1,
        }

    def test_eligible_vs_not_yet_eligible(self):
        records, rows = self._cohort()
        audit = build_cohort_closeout_audit(
            records, rows, session_dates=CALENDAR,
            latest_completed_session=CALENDAR[-1],
        )
        # BBBX(partial, old) + DDDX(missing, old) are eligible; EEEX recent is
        # not yet eligible; AAAX matured; CCCX retryable failure.
        assert audit["eligible_not_yet_matured_count"] == 2
        assert audit["not_yet_eligible_count"] == 1
        assert audit["eligibility"]["counts"]["matured"] == 1
        assert audit["eligibility"]["counts"]["retryable_failure"] == 1
        # unresolved = eligible(2) + retryable(1)
        assert audit["unresolved_action_required_count"] == 3

    def test_provider_and_forward_fetch_failures(self):
        records, rows = self._cohort()
        audit = build_cohort_closeout_audit(records, rows,
                                            session_dates=CALENDAR)
        assert audit["forward_fetch_error_count"] == 1
        assert audit["forward_fetch_error_rows"][0]["symbol"] == "CCCX"
        assert audit["provider_failure_count"] == 1
        assert audit["provider_failure_rows"][0]["error_code"] == (
            "provider_mismatch"
        )

    def test_outcome_coverage(self):
        records, rows = self._cohort()
        audit = build_cohort_closeout_audit(records, rows,
                                            session_dates=CALENDAR)
        # 3 of 5 records have an outcome row (AAAX, BBBX, CCCX).
        assert audit["with_outcome_count"] == 3
        assert audit["missing_outcome_count"] == 2
        assert abs(audit["outcome_coverage"] - 0.6) < 1e-9

    def test_no_input_mutation(self):
        records, rows = self._cohort()
        fr, fo = copy.deepcopy(records), copy.deepcopy(rows)
        build_cohort_closeout_audit(records, rows, session_dates=CALENDAR)
        assert records == fr
        assert rows == fo


class TestEligibilityUnknownAndDuplicates:
    def test_missing_calendar_reports_unknown_not_eligible(self):
        records = [_rec("AAAX", "p1", status=None)]
        audit = build_cohort_closeout_audit(records, [], session_dates=[])
        # No trading calendar → eligibility is unknown, never assumed eligible.
        assert audit["eligibility"]["counts"]["eligibility_unknown"] == 1
        assert audit["eligible_not_yet_matured_count"] == 0
        assert audit["not_yet_eligible_count"] == 0

    def test_duplicate_outcome_pair_detected(self):
        records = [_rec("AAAX", "p1", status="complete")]
        rows = [
            _outcome_row(symbol="AAAX", pair_id="p1", status="complete"),
            _outcome_row(symbol="AAAX", pair_id="p1", status="complete"),
        ]
        audit = build_cohort_closeout_audit(records, rows,
                                            session_dates=CALENDAR)
        assert audit["duplicate_outcome_pair_count"] == 1
        assert audit["duplicate_outcome_pairs"] == {"p1": 2}

    def test_reuses_quality_audit_and_metrics(self):
        records = [_rec("AAAX", "p1", status="complete")]
        audit = build_cohort_closeout_audit(records, [],
                                            session_dates=CALENDAR)
        # Composed from the existing machinery, not a parallel implementation.
        assert "quality_audit" in audit
        assert audit["quality_audit"]["contract_version"] == (
            "shadow_evidence_quality.v1"
        )
        assert "decision_metrics" in audit
        assert audit["decision_metrics"]["metrics_contract_version"] == (
            "strategy_shadow_metrics.v2"
        )
