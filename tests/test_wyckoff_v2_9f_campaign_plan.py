"""Phase 9F8: deterministic campaign planning without execution."""

from __future__ import annotations

import inspect
from typing import Any, Dict, List

import pytest

from app.workers.shadow.campaign_planning import (
    CAMPAIGN_PLAN_CONTRACT_VERSION,
    MAX_PLAN_SESSIONS,
    MAX_PLAN_SYMBOLS,
    WARNING_MASSIVE_REQUIRED,
    WARNING_MIGRATION_013,
    CampaignPlanError,
    build_campaign_plan,
)
from app.workers.shadow.experiments import UnknownShadowExperimentError


def _plan(**overrides) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = dict(
        experiment_code="wyckoff_v2_vs_baseline",
        candidate_symbols=["bbbx", "AAAX", "cccx", "AAAX"],
        as_of_sessions=["2026-07-17", "2026-07-16"],
        max_symbols_per_campaign=2,
    )
    kwargs.update(overrides)
    return build_campaign_plan(**kwargs)


class TestPlanValidation:
    def test_contract_and_never_executed(self):
        plan = _plan()
        assert plan["plan_contract_version"] == CAMPAIGN_PLAN_CONTRACT_VERSION
        assert plan["executed"] is False

    def test_explicit_bound_required(self):
        with pytest.raises(CampaignPlanError, match="max_symbols_per_campaign"):
            _plan(max_symbols_per_campaign=None)
        with pytest.raises(CampaignPlanError):
            _plan(max_symbols_per_campaign=0)
        with pytest.raises(CampaignPlanError):
            _plan(max_symbols_per_campaign=101)

    def test_no_implicit_universe(self):
        with pytest.raises(CampaignPlanError, match="implicit universe"):
            _plan(candidate_symbols=None)
        with pytest.raises(CampaignPlanError):
            _plan(candidate_symbols=[])
        with pytest.raises(CampaignPlanError):
            _plan(candidate_symbols=[f"S{i}" for i in range(
                MAX_PLAN_SYMBOLS + 1)])

    def test_sessions_bounded_and_validated(self):
        with pytest.raises(CampaignPlanError):
            _plan(as_of_sessions=[])
        with pytest.raises(CampaignPlanError):
            _plan(as_of_sessions=["not a date"])
        with pytest.raises(CampaignPlanError):
            _plan(as_of_sessions=[
                f"2026-01-{i:02d}" for i in range(1, MAX_PLAN_SESSIONS + 2)
            ])

    def test_unknown_experiment_rejects(self):
        with pytest.raises(UnknownShadowExperimentError):
            _plan(experiment_code="nope")

    def test_bad_horizon_rejects(self):
        with pytest.raises(CampaignPlanError):
            _plan(target_horizon="40D")


class TestPlanDeterminism:
    def test_deterministic_sorted_batches(self):
        plan = _plan()
        assert plan["planned_symbols"] == ["AAAX", "BBBX", "CCCX"]
        assert plan["as_of_sessions"] == ["2026-07-16", "2026-07-17"]
        # 3 symbols / 2 per campaign = 2 batches per session x 2 sessions.
        assert plan["expected_campaign_count"] == 4
        assert plan["batches"][0]["symbols"] == ["AAAX", "BBBX"]
        assert plan["batches"][1]["symbols"] == ["CCCX"]
        assert all(
            b["symbol_count"] <= 2 for b in plan["batches"]
        )
        assert _plan() == _plan()

    def test_existing_coverage_reduces_the_plan(self):
        plan = _plan(existing_evaluated_symbols=["aaax"])
        assert plan["already_covered_symbol_count"] == 1
        assert plan["planned_symbols"] == ["BBBX", "CCCX"]
        assert plan["expected_campaign_count"] == 2

    def test_remaining_evidence_gap(self):
        plan = _plan(
            target_unique_symbols=50, existing_unique_symbols=20,
            target_trigger_confirmed=20, existing_trigger_confirmed=25,
            target_matured_outcomes=100, existing_matured_outcomes=40,
        )
        assert plan["remaining_evidence_gap"] == {
            "unique_symbols": 30,
            "trigger_confirmed": 0,
            "matured_outcomes": 60,
        }

    def test_maturation_sessions_follow_target_horizon(self):
        plan = _plan(target_horizon="20D")
        assert plan["required_maturation_trading_sessions"] == 20
        assert plan["maturation_after_session"] == "2026-07-17"
        assert _plan(target_horizon="5D")[
            "required_maturation_trading_sessions"] == 5


class TestPlanPayloads:
    def test_exact_admin_payloads(self):
        plan = _plan()
        payload = plan["campaign_payloads"][0]
        assert payload["method"] == "POST"
        assert payload["path"] == "/api/admin/shadow-campaigns"
        assert payload["body"] == {
            "experiment_code": "wyckoff_v2_vs_baseline",
            "symbols": ["AAAX", "BBBX"],
            "max_symbols": 2,
            "as_of_date": "2026-07-16",
        }
        assert len(plan["campaign_payloads"]) == plan[
            "expected_campaign_count"
        ]

    def test_operational_warnings_present(self):
        plan = _plan()
        assert WARNING_MIGRATION_013 in plan["warnings"]
        assert WARNING_MASSIVE_REQUIRED in plan["warnings"]
        assert "013" in WARNING_MIGRATION_013
        assert "massive" in WARNING_MASSIVE_REQUIRED.lower()


class TestPlanSafety:
    def test_module_never_executes_or_writes(self):
        from app.workers.shadow import campaign_planning

        source = inspect.getsource(campaign_planning)
        for forbidden in (
            "run_shadow_campaign", "run_shadow_comparison",
            "get_market_data_provider", "get_db_connection",
            "INSERT", "UPDATE ", "BackgroundTasks", "add_task",
            "apscheduler",
        ):
            assert forbidden not in source, forbidden

    def test_plan_is_pure(self):
        # Building a plan twice with identical inputs produces identical
        # output and touches nothing global.
        from app.workers.strategies.wyckoff_v2.constants import (
            default_config as v2_default_config,
        )

        before = v2_default_config()
        _plan()
        assert v2_default_config() == before
