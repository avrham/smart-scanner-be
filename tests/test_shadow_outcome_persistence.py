"""Phase 8.1B2: pure write-once merge / lifecycle for pair outcomes.

No DB I/O — exercises merge_outcome_for_persistence only.
"""

from datetime import date

from app.workers.shadow.outcomes.constants import (
    CALCULATION_VERSION,
    FORWARD_FRAME_VERSION,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
    REFERENCE_PRICE_ROLE,
    STATUS_COMPLETE,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.shadow.outcomes.persistence import merge_outcome_for_persistence


def _base(**overrides):
    record = {
        "pair_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "outcome_fingerprint": "fp-stable",
        "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        "forward_frame_version": FORWARD_FRAME_VERSION,
        "reference_price": 100.0,
        "reference_price_role": REFERENCE_PRICE_ROLE,
        "forward_provider": "massive",
        "forward_data_as_of": date(2026, 6, 16),
        "available_forward_bars": 1,
        "first_forward_date": date(2026, 6, 15),
        "last_forward_date": date(2026, 6, 15),
        "forward_bars_hash": "hash-1",
        "ret_1d": 1.0,
        "ret_3d": None,
        "ret_5d": None,
        "ret_10d": None,
        "ret_20d": None,
        "max_favorable_excursion": 2.0,
        "max_adverse_excursion": -1.0,
        "mfe_mae_bar_count": 1,
        "benchmark_returns": {
            "SPY": {"1D": 0.5, "3D": None, "5D": None, "10D": None, "20D": None},
            "QQQ": {"1D": None, "3D": None, "5D": None, "10D": None, "20D": None},
        },
        "revision_notes": [],
        "reference_revision_detected": False,
        "outcome_status": STATUS_PARTIAL,
        "error_code": None,
        "error_message": None,
    }
    record.update(overrides)
    return record


class TestFirstInsert:
    def test_first_insert_passes_through(self):
        calc = _base()
        merged = merge_outcome_for_persistence(None, calc)
        assert merged["ret_1d"] == 1.0
        assert merged["outcome_fingerprint"] == "fp-stable"
        assert merged["reference_revision_detected"] is False


class TestHorizonFreeze:
    def test_null_horizons_fill(self):
        existing = _base()
        calc = _base(
            available_forward_bars=3,
            forward_bars_hash="hash-3",
            last_forward_date=date(2026, 6, 17),
            ret_1d=1.0,
            ret_3d=2.5,
            mfe_mae_bar_count=3,
            max_favorable_excursion=4.0,
            max_adverse_excursion=-2.0,
            benchmark_returns={
                "SPY": {
                    "1D": 0.5, "3D": 0.8, "5D": None, "10D": None, "20D": None
                },
                "QQQ": {
                    "1D": 0.2, "3D": None, "5D": None, "10D": None, "20D": None
                },
            },
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["ret_1d"] == 1.0
        assert merged["ret_3d"] == 2.5
        assert merged["benchmark_returns"]["SPY"]["3D"] == 0.8
        assert merged["benchmark_returns"]["QQQ"]["1D"] == 0.2
        assert merged["available_forward_bars"] == 3
        assert merged["forward_bars_hash"] == "hash-3"

    def test_non_null_horizons_freeze_on_divergence(self):
        existing = _base(ret_1d=1.0)
        calc = _base(ret_1d=9.9)  # would overwrite if freeze broken
        merged = merge_outcome_for_persistence(
            existing, calc, detected_at="2026-07-01T00:00:00Z"
        )
        assert merged["ret_1d"] == 1.0
        notes = merged["revision_notes"]
        assert any(n["reason_code"] == "horizon_value_divergence" for n in notes)
        note = next(n for n in notes if n["reason_code"] == "horizon_value_divergence")
        assert note["horizon"] == "1D"
        assert note["existing_value"] == 1.0
        assert note["observed_value"] == 9.9
        assert note["detected_at"] == "2026-07-01T00:00:00Z"

    def test_benchmark_horizons_freeze(self):
        existing = _base()
        calc = _base(
            benchmark_returns={
                "SPY": {
                    "1D": 99.0, "3D": 1.1, "5D": None, "10D": None, "20D": None
                },
                "QQQ": {
                    "1D": None, "3D": None, "5D": None, "10D": None, "20D": None
                },
            }
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["benchmark_returns"]["SPY"]["1D"] == 0.5  # frozen
        assert merged["benchmark_returns"]["SPY"]["3D"] == 1.1  # filled
        assert any(
            n["reason_code"] == "benchmark_value_divergence"
            for n in merged["revision_notes"]
        )

    def test_repeated_identical_recalculation_is_idempotent(self):
        existing = _base()
        merged = merge_outcome_for_persistence(existing, _base())
        assert merged["ret_1d"] == 1.0
        assert merged["revision_notes"] == []
        assert merged["forward_bars_hash"] == "hash-1"


class TestMfeMaeLifecycle:
    def test_mfe_mae_updates_only_with_more_bars(self):
        existing = _base(
            mfe_mae_bar_count=3,
            max_favorable_excursion=5.0,
            max_adverse_excursion=-2.0,
            available_forward_bars=3,
        )
        same_or_fewer = _base(
            mfe_mae_bar_count=3,
            max_favorable_excursion=99.0,
            max_adverse_excursion=-99.0,
            available_forward_bars=3,
            forward_bars_hash="hash-same",
        )
        merged = merge_outcome_for_persistence(existing, same_or_fewer)
        assert merged["max_favorable_excursion"] == 5.0
        assert merged["max_adverse_excursion"] == -2.0
        assert merged["mfe_mae_bar_count"] == 3

        more = _base(
            mfe_mae_bar_count=5,
            max_favorable_excursion=7.0,
            max_adverse_excursion=-3.0,
            available_forward_bars=5,
            forward_bars_hash="hash-5",
            ret_5d=1.5,
        )
        merged2 = merge_outcome_for_persistence(existing, more)
        assert merged2["max_favorable_excursion"] == 7.0
        assert merged2["mfe_mae_bar_count"] == 5


class TestStatusMonotonicity:
    def test_complete_row_remains_complete(self):
        existing = _base(
            outcome_status=STATUS_COMPLETE,
            available_forward_bars=20,
            ret_20d=3.0,
            mfe_mae_bar_count=20,
            forward_bars_hash="hash-20",
        )
        calc = _base(
            outcome_status=STATUS_ERROR,
            error_code="noise",
            available_forward_bars=20,
            forward_bars_hash="hash-20",
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["outcome_status"] == STATUS_COMPLETE
        assert merged["error_code"] is None

    def test_error_row_repaired_by_successful_recalc(self):
        existing = _base(
            outcome_status=STATUS_ERROR,
            error_code="provider_mismatch",
            error_message="was mismatched",
            ret_1d=None,
            available_forward_bars=0,
            forward_bars_hash=None,
            mfe_mae_bar_count=None,
            max_favorable_excursion=None,
            max_adverse_excursion=None,
            reference_price=None,
            benchmark_returns=None,
        )
        calc = _base(outcome_status=STATUS_PARTIAL)
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["outcome_status"] == STATUS_PARTIAL
        assert merged["error_code"] is None
        assert merged["ret_1d"] == 1.0

    def test_pending_to_partial_to_complete(self):
        pending = _base(
            outcome_status=STATUS_PENDING,
            available_forward_bars=0,
            ret_1d=None,
            forward_bars_hash=None,
            mfe_mae_bar_count=None,
            max_favorable_excursion=None,
            max_adverse_excursion=None,
        )
        partial = merge_outcome_for_persistence(
            pending, _base(outcome_status=STATUS_PARTIAL)
        )
        assert partial["outcome_status"] == STATUS_PARTIAL
        complete = merge_outcome_for_persistence(
            partial,
            _base(
                outcome_status=STATUS_COMPLETE,
                available_forward_bars=20,
                ret_20d=4.0,
                ret_10d=3.0,
                ret_5d=2.0,
                ret_3d=1.5,
                mfe_mae_bar_count=20,
                forward_bars_hash="hash-20",
            ),
        )
        assert complete["outcome_status"] == STATUS_COMPLETE
        assert complete["ret_1d"] == 1.0  # frozen from partial


class TestRevisionDetection:
    def test_provider_revision_recorded_without_overwrite(self):
        existing = _base(forward_bars_hash="hash-1", available_forward_bars=1)
        calc = _base(
            forward_bars_hash="hash-revised",
            available_forward_bars=1,
            ret_1d=1.0,
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["forward_bars_hash"] == "hash-1"  # frozen
        assert any(
            n["reason_code"] == "forward_bars_revision"
            for n in merged["revision_notes"]
        )

    def test_reference_revision_flag_sticky(self):
        existing = _base(reference_revision_detected=False, reference_price=100.0)
        calc = _base(
            reference_revision_detected=True,
            reference_price=100.0,
            revision_notes=[{
                "reason_code": "reference_close_revision",
                "existing_value": 100.0,
                "observed_value": 101.0,
            }],
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["reference_revision_detected"] is True
        assert merged["reference_price"] == 100.0

    def test_reference_price_never_silently_replaced(self):
        existing = _base(reference_price=100.0)
        calc = _base(reference_price=55.0)
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["reference_price"] == 100.0
        assert merged["reference_revision_detected"] is True

    def test_contract_identity_stable_across_maturation(self):
        existing = _base()
        calc = _base(
            outcome_fingerprint="should-not-win",
            calculation_version="outcome.v999",
            available_forward_bars=5,
            forward_bars_hash="hash-5",
            ret_5d=2.0,
            mfe_mae_bar_count=5,
        )
        merged = merge_outcome_for_persistence(existing, calc)
        assert merged["outcome_fingerprint"] == "fp-stable"
        assert merged["calculation_version"] == CALCULATION_VERSION
        assert merged["forward_provider"] == "massive"
        assert merged["reference_price_role"] == REFERENCE_PRICE_ROLE
