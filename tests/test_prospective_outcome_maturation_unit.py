"""Fast, DB-free unit tests for prospective outcome maturation:
maturity/eligibility boundaries, session-count math, future-bar exclusion,
partial-horizon behavior, local-history-only contract shape, and
candidate/control primary-signal extraction. Every formula/classifier
exercised here is the EXISTING repository code (outcome.v1 /
shadow_maturation_eligibility.v1 / paired-comparison classifiers) — nothing
here reimplements a formula.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.jobs import contracts as C
from app.jobs.contracts import ProspectiveOutcomePayload, TerminalJobError
from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.shadow.outcomes.calculator import (
    build_forward_sequence,
    compute_outcome_values,
    resolve_reference_price,
    status_for_bar_count,
)
from app.workers.shadow.outcomes.constants import (
    STATUS_COMPLETE,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.shadow.outcomes.eligibility import (
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MATURED,
    ELIGIBILITY_NOT_YET,
    ELIGIBILITY_UNKNOWN,
    FULL_MATURATION_SESSIONS,
    MIN_MATURATION_SESSIONS,
    classify_maturation_eligibility,
    completed_forward_sessions,
)


# --------------------------------------------------------------------------
# outcome payload contract
# --------------------------------------------------------------------------
def test_outcome_payload_roundtrip():
    d = {"registration_id": "r1", "registration_identity": "pcr:x",
         "campaign_id": "c1", "campaign_run_id": "run1",
         "pair_id": "p1", "symbol": "aapl"}
    p = ProspectiveOutcomePayload.from_dict(d)
    assert p.symbol == "AAPL"
    assert p.to_dict()["symbol"] == "AAPL"


def test_outcome_payload_missing_field_is_terminal():
    with pytest.raises(TerminalJobError):
        ProspectiveOutcomePayload.from_dict({"registration_id": "r1"})


def test_outcome_task_type_and_queue_distinct_from_evaluation():
    assert C.PROSPECTIVE_OUTCOME_MATURATION_TASK != C.PROSPECTIVE_SYMBOL_EVALUATION_TASK
    assert C.PROSPECTIVE_OUTCOME_QUEUE != C.PROSPECTIVE_QUEUE


# --------------------------------------------------------------------------
# horizons are the EXISTING outcome.v1 constant — never redefined here
# --------------------------------------------------------------------------
def test_horizons_are_the_existing_repo_constant():
    assert list(HOLDING_WINDOWS) == [1, 3, 5, 10, 20]
    assert MIN_MATURATION_SESSIONS == 1
    assert FULL_MATURATION_SESSIONS == 20


# --------------------------------------------------------------------------
# session-count math (retrospective, local-calendar based)
# --------------------------------------------------------------------------
def test_completed_forward_sessions_counts_only_strictly_after_snapshot():
    snap = date(2026, 7, 28)
    dates = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 29), date(2026, 7, 30)]
    assert completed_forward_sessions(dates, snap) == 2


def test_completed_forward_sessions_bounded_by_latest_completed_session():
    snap = date(2026, 7, 28)
    dates = [date(2026, 7, 29), date(2026, 7, 30), date(2026, 7, 31)]
    assert completed_forward_sessions(dates, snap, latest_completed_session=date(2026, 7, 29)) == 1


def test_completed_forward_sessions_empty_calendar_is_none_not_zero():
    assert completed_forward_sessions([], date(2026, 7, 28)) is None


# --------------------------------------------------------------------------
# eligibility boundaries — the EXACT live-campaign shape: 0 sessions locally
# --------------------------------------------------------------------------
def test_eligibility_zero_sessions_is_not_yet_never_a_failure():
    state = classify_maturation_eligibility(
        outcome_status=None, error_code=None, completed_forward_sessions=0)
    assert state == ELIGIBILITY_NOT_YET


def test_eligibility_one_session_is_eligible():
    state = classify_maturation_eligibility(
        outcome_status=None, error_code=None, completed_forward_sessions=1)
    assert state == ELIGIBILITY_ELIGIBLE


def test_eligibility_unknown_calendar_never_assumed_eligible():
    state = classify_maturation_eligibility(
        outcome_status=None, error_code=None, completed_forward_sessions=None)
    assert state == ELIGIBILITY_UNKNOWN


def test_eligibility_complete_status_is_matured_regardless_of_sessions():
    state = classify_maturation_eligibility(
        outcome_status="complete", error_code=None, completed_forward_sessions=0)
    assert state == ELIGIBILITY_MATURED


# --------------------------------------------------------------------------
# future-bar exclusion + partial-horizon behavior (build_forward_sequence /
# status_for_bar_count — existing pure calculator, local-read shaped input)
# --------------------------------------------------------------------------
def _bar(d, close=100.0):
    return {"date": d, "open": close, "high": close + 1, "low": close - 1,
            "close": close, "volume": 1000.0}


def test_build_forward_sequence_excludes_bars_on_or_before_snapshot():
    snap = "2026-07-28"
    bars = [_bar("2026-07-27"), _bar(snap), _bar("2026-07-29"), _bar("2026-07-30")]
    seq = build_forward_sequence(bars, snap, explicit_completed=True)
    assert [b["date"] for b in seq["forward_bars"]] == ["2026-07-29", "2026-07-30"]
    assert seq["snapshot_bar"]["date"] == snap


def test_build_forward_sequence_caps_at_max_window():
    snap = "2026-01-01"
    bars = [_bar(f"2026-01-{d:02d}") for d in range(2, 32)]  # 30 forward bars
    seq = build_forward_sequence(bars, snap, explicit_completed=True, max_window=20)
    assert len(seq["forward_bars"]) == 20


def test_status_for_bar_count_pending_partial_complete_boundaries():
    assert status_for_bar_count(0) == STATUS_PENDING
    assert status_for_bar_count(1) == STATUS_PARTIAL
    assert status_for_bar_count(19) == STATUS_PARTIAL
    assert status_for_bar_count(20) == STATUS_COMPLETE


def test_compute_outcome_values_partial_horizon_stays_none():
    ref = 100.0
    forward = [_bar("2026-07-29", 102.0), _bar("2026-07-30", 101.0)]  # only 2 bars
    values = compute_outcome_values(ref, forward)
    assert values["ret_by_window"][1] is not None  # 1D resolvable
    assert values["ret_by_window"][3] is None       # 3D not yet observable
    assert values["ret_by_window"][20] is None
    assert values["available_forward_bars"] == 2


def test_resolve_reference_price_rejects_frame_mismatch():
    with pytest.raises(Exception):
        resolve_reference_price(
            frame_last_bar={"date": "2026-07-27", "close": 100.0},
            frame_bar_count=501, snapshot_date="2026-07-28",
            frame_last_date="2026-07-28")


def test_resolve_reference_price_accepts_matching_frame():
    price = resolve_reference_price(
        frame_last_bar={"date": "2026-07-28", "close": 340.08},
        frame_bar_count=501, snapshot_date="2026-07-28",
        frame_last_date="2026-07-28")
    assert price == 340.08


# --------------------------------------------------------------------------
# local-history-only reader: no provider import anywhere in its module
# --------------------------------------------------------------------------
def _imported_names(module) -> set:
    """Every name this module binds via import (module-level, code only —
    not docstrings/comments), so a prose mention of "never calls Massive"
    can never produce a false failure."""
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
            names.update(a.name for a in node.names)
    return names


_FORBIDDEN_PROVIDER_IMPORTS = {
    "MassiveClient", "MassiveApiError", "get_market_data_provider",
    "httpx", "requests", "aiohttp", "fmp", "massive",
}


def test_local_reader_module_constructs_no_provider():
    import app.jobs.prospective_outcome_local_reader as reader
    hit = _imported_names(reader) & _FORBIDDEN_PROVIDER_IMPORTS
    assert not hit, f"local reader imports forbidden provider names: {hit}"


def test_outcome_handler_module_constructs_no_provider():
    import app.jobs.handlers.prospective_outcome as handler
    hit = _imported_names(handler) & (_FORBIDDEN_PROVIDER_IMPORTS | {"_fetch_daily_range"})
    assert not hit, f"outcome handler imports forbidden provider names: {hit}"


# --------------------------------------------------------------------------
# candidate / control primary-signal extraction (existing production
# classifiers, fed synthetic-but-real-shaped details_snapshot payloads)
# --------------------------------------------------------------------------
def test_candidate_signal_fields_extraction_matches_persisted_shape():
    from app.prospective_campaign import candidate_signal_fields, CANDIDATE_SIGNAL_DEFINITION
    assert CANDIDATE_SIGNAL_DEFINITION == "pre_rollout_enter_eligible.v1"
    details = {
        "readiness": {"status": "ready"},
        "policy": {"setup_present": True, "trigger_confirmed": False,
                  "enter_eligible_without_rollout_gate": False,
                  "waiting_reasons": ["trigger_not_confirmed"]},
        "four_hour_trigger": {"state": "missing"},
    }
    fields = candidate_signal_fields(details)
    assert fields["setup_present"] is True
    assert fields["trigger_confirmed"] is False
    assert fields["enter_eligible_without_rollout_gate"] is False
    assert fields["waiting_reasons"] == ["trigger_not_confirmed"]


def test_candidate_pre_rollout_eligible_via_production_classifier():
    from app.workers.shadow.strategy_metrics import (
        is_pre_rollout_enter_candidate, is_rollout_blocked, classify_trigger_state,
        TRIGGER_CLASS_CONFIRMED,
    )
    # setup+trigger confirmed, would-enter but for the rollout gate — the exact
    # "trigger_confirmed_rollout_blocked" WATCH shape the campaign is expected
    # to distinguish from a plain unconfirmed-trigger WATCH.
    record = {
        "policy": {"enter_eligible_without_rollout_gate": True, "allow_enter": False},
        "four_hour_trigger": {"state": "confirmed"},
    }
    assert is_pre_rollout_enter_candidate(record) is True
    assert is_rollout_blocked(record) is True
    assert classify_trigger_state(record) == TRIGGER_CLASS_CONFIRMED


def test_candidate_no_policy_record_is_none_never_assumed_false():
    from app.workers.shadow.strategy_metrics import is_pre_rollout_enter_candidate
    assert is_pre_rollout_enter_candidate({}) is None


def test_watch_classification_distinguishes_rollout_blocked_from_unconfirmed():
    from app.paired_comparison import _watch_classification
    rollout_blocked = {"verdict": "WATCH",
                       "policy": {"enter_eligible_without_rollout_gate": True,
                                 "allow_enter": False}}
    assert _watch_classification(rollout_blocked) == "trigger_confirmed_rollout_blocked"

    unconfirmed = {"verdict": "WATCH",
                   "policy": {"setup_state": "valid",
                             "enter_eligible_without_rollout_gate": False,
                             "allow_enter": False}}
    assert _watch_classification(unconfirmed) == "valid_setup_trigger_unconfirmed"


def test_control_primary_signal_is_verdict_based_entry_only():
    from app.paired_comparison import ACTIONABLE_VERDICTS, CONTROL_STRATEGY
    assert CONTROL_STRATEGY == "sma150_bounce"
    # sma150_bounce emits only ENTER/AVOID (no WATCH) — its canonical entry
    # signal collapses to verdict == "ENTER".
    assert "ENTER" in ACTIONABLE_VERDICTS
    ctrl_verdict = "ENTER"
    assert (ctrl_verdict in ACTIONABLE_VERDICTS) is True
    ctrl_verdict = "AVOID"
    assert (ctrl_verdict in ACTIONABLE_VERDICTS) is False


def test_control_strategy_version_pinned_to_v2_not_v3():
    from app.prospective_campaign import CONTROL_STRATEGY_CODE, CONTROL_STRATEGY_VERSION
    assert CONTROL_STRATEGY_CODE == "sma150_bounce"
    assert CONTROL_STRATEGY_VERSION == "sma150.v2"
