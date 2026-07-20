"""Phase 7B — outcomes freeze the signal version they evaluate.

Pure tests on build_outcome_from_frames (no DB, no providers): new outcomes
copy strategy/policy/config provenance from the signal's provenance row;
legacy signals without provenance keep NULLs (never inferred).
"""

import uuid
from datetime import date

import pandas as pd

from app.workers.outcomes.calculator import CALCULATION_VERSION
from app.workers.outcomes.service import build_outcome_from_frames


def _frame(days=30, start_price=100.0):
    dates = pd.date_range("2026-06-01", periods=days, freq="B")
    prices = [start_price + i for i in range(days)]
    return pd.DataFrame({
        "date": dates,
        "open": prices, "high": [p + 1 for p in prices],
        "low": [p - 1 for p in prices], "close": prices,
        "volume": 1_000_000,
    })


def _signal(provenance=None):
    return {
        "signal_id": str(uuid.uuid4()),
        "symbol": "AAA",
        "pattern_code": "sma150_bounce",
        "snapshot_date": date(2026, 6, 3),
        "created_at": None,
        "details": {},
        **({"provenance": provenance} if provenance is not None else {}),
    }


_PROV = {
    "scan_run_id": str(uuid.uuid4()),
    "strategy_code": "sma150_bounce",
    "strategy_version": "sma150.v2",
    "decision_policy_version": "strategy_decision.v1",
    "config_hash": "cafebabe",
    "provenance_version": "provenance.v1",
}


def test_outcome_copies_signal_provenance():
    record = build_outcome_from_frames(_signal(provenance=_PROV), _frame())
    assert record["scan_run_id"] == _PROV["scan_run_id"]
    assert record["strategy_code"] == "sma150_bounce"
    assert record["strategy_version"] == "sma150.v2"
    assert record["decision_policy_version"] == "strategy_decision.v1"
    assert record["config_hash"] == "cafebabe"
    assert record["provenance_version"] == "provenance.v1"
    # Outcome calculation identity stays a SEPARATE version concept.
    assert record["calculation_version"] == CALCULATION_VERSION
    assert record["calculation_version"] != record["strategy_version"]
    assert record["calculation_version"] != record["decision_policy_version"]


def test_legacy_signal_outcome_keeps_null_provenance():
    record = build_outcome_from_frames(_signal(), _frame())
    assert record["scan_run_id"] is None
    assert record["strategy_code"] is None
    assert record["strategy_version"] is None
    assert record["decision_policy_version"] is None
    assert record["config_hash"] is None
    assert record["provenance_version"] is None
    # The outcome itself still calculates normally.
    assert record["outcome_status"] == "calculated"


def test_outcome_calculation_unchanged_by_provenance_fields():
    with_prov = build_outcome_from_frames(_signal(provenance=_PROV), _frame())
    without = build_outcome_from_frames(_signal(), _frame())
    for key in ("entry_price", "ret_by_window", "max_favorable_excursion",
                "max_adverse_excursion", "outcome_status"):
        assert with_prov[key] == without[key]
