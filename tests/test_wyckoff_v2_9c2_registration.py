"""Phase 9C2: controlled registry registration and migration 012 config."""

from __future__ import annotations

import ast
import importlib
import json
import pathlib
import subprocess

import pytest

from app.workers.patterns.config import coerce_config_value, merge_config
from app.workers.strategies.base import StrategyContext, StrategyDecision
from app.workers.strategies.registry import (
    list_strategies,
    register_strategy,
)
from app.workers.strategies.wyckoff_v2.constants import (
    DEFAULT_CONFIG,
    default_config,
)
from app.workers.strategies.wyckoff_v2.strategy import WyckoffMTFV2Strategy


ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "app" / "db" / "migrations"
MIGRATION_012 = MIGRATIONS / "012_wyckoff_mtf_v2.sql"
V2 = ROOT / "app" / "workers" / "strategies" / "wyckoff_v2"
REGISTRY = ROOT / "app" / "workers" / "strategies" / "registry.py"


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def _sql_statements() -> str:
    return "\n".join(
        line
        for line in MIGRATION_012.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )


class TestRegistryRegistration:
    def test_v2_appears_in_list_strategies_after_canonical_init(self):
        codes = list_strategies()
        assert "wyckoff_mtf_v2" in codes
        assert "wyckoff_mtf" in codes

    def test_get_strategy_returns_v2_identity(self):
        from app.workers.strategies.registry import get_strategy

        strategy = get_strategy("wyckoff_mtf_v2")
        assert isinstance(strategy, WyckoffMTFV2Strategy)
        assert strategy.pattern_code == "wyckoff_mtf_v2"
        assert strategy.version == "wyckoff_mtf.v2"
        assert strategy.decision_policy_version == "wyckoff_mtf.policy.v1"

    def test_package_import_alone_does_not_mutate_registry(self):
        before = list(list_strategies())
        importlib.reload(importlib.import_module("app.workers.strategies.wyckoff_v2"))
        after = list(list_strategies())
        assert after == before

    def test_wyckoff_v2_modules_do_not_import_registry(self):
        for name in ("__init__.py", "strategy.py", "policy.py", "constants.py"):
            tree = ast.parse((V2 / name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    mod = node.module or ""
                    assert "registry" not in mod.split("."), name
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        assert "registry" not in alias.name.split("."), name

    def test_duplicate_registration_is_idempotent_replace(self):
        # Existing contract: re-register replaces; defaults guard with `not in`.
        first = WyckoffMTFV2Strategy()
        second = WyckoffMTFV2Strategy()
        register_strategy(first)
        register_strategy(second)
        from app.workers.strategies.registry import get_strategy

        assert get_strategy("wyckoff_mtf_v2") is second
        # Restore a fresh default instance for later tests
        register_strategy(WyckoffMTFV2Strategy())

    def test_registry_defaults_guard_prevents_double_insert_logic(self):
        text = REGISTRY.read_text(encoding="utf-8")
        assert 'if "wyckoff_mtf_v2" not in _REGISTRY:' in text
        assert "WyckoffMTFV2Strategy" in text


class TestMigration012:
    def test_file_exists_exact_name(self):
        assert MIGRATION_012.exists()
        assert not list(MIGRATIONS.glob("013_*"))

    def test_registers_canonical_identifier_disabled(self):
        sql = MIGRATION_012.read_text(encoding="utf-8")
        stmts = _sql_statements()
        assert "'wyckoff_mtf_v2'" in sql
        insert = stmts[stmts.index("INSERT INTO public.patterns") :]
        insert = insert[: insert.index(";")]
        assert "false" in insert.lower()
        assert "wyckoff_mtf_v2" in insert

    def test_idempotent_do_nothing(self):
        stmts = _sql_statements()
        assert "ON CONFLICT (code) DO NOTHING" in stmts
        assert "ON CONFLICT (pattern_code, key) DO NOTHING" in stmts
        assert "DO UPDATE" not in stmts
        for forbidden in ("DROP ", "DELETE ", "TRUNCATE", "ALTER TABLE"):
            assert forbidden not in stmts.upper()

    def test_never_touches_v1_rows(self):
        stmts = _sql_statements()
        assert "('wyckoff_mtf'," not in stmts
        assert "UPDATE public.patterns" not in stmts
        assert "UPDATE public.signals" not in stmts

    def test_rollout_defaults_seeded(self):
        sql = MIGRATION_012.read_text(encoding="utf-8")
        assert "('wyckoff_mtf_v2', 'allow_enter', 'false')" in sql
        assert "('wyckoff_mtf_v2', 'min_price', '5.0')" in sql
        assert "('wyckoff_mtf_v2', 'enable_4h_trigger', 'false')" in sql
        assert "('wyckoff_mtf_v2', 'require_4h_trigger_for_enter', 'true')" in sql

    def test_config_mirrors_default_config_keys(self):
        sql = MIGRATION_012.read_text(encoding="utf-8")
        cfg = default_config()
        for key in cfg:
            assert f"('{key}'" in sql or f", '{key}'," in sql, key
            assert f"('wyckoff_mtf_v2', '{key}'," in sql, key

    def test_seeded_values_roundtrip_through_coerce(self):
        sql = MIGRATION_012.read_text(encoding="utf-8")
        cfg = default_config()
        for key, expected in cfg.items():
            marker = f"('wyckoff_mtf_v2', '{key}', '"
            assert marker in sql, key
            rest = sql.split(marker, 1)[1]
            lit = rest.split("')", 1)[0]
            got = coerce_config_value(lit)
            assert got == expected, (key, expected, got)

    def test_merge_over_defaults_preserves_rollout_gates(self):
        raw = {
            "allow_enter": "false",
            "min_price": "5.0",
            "enable_4h_trigger": "false",
        }
        merged, used_fallback = merge_config(raw, default_config())
        assert used_fallback is False
        assert merged["allow_enter"] is False
        assert merged["min_price"] == 5.0
        assert merged["enable_4h_trigger"] is False


class TestRolloutBehavior:
    def test_code_defaults_disabled_rollout(self):
        cfg = default_config()
        assert cfg["allow_enter"] is False
        assert DEFAULT_CONFIG["allow_enter"] is False
        assert cfg["min_price"] == 5.0
        assert cfg["enable_4h_trigger"] is False

    def test_allow_enter_false_prevents_enter_when_strategy_callable(self):
        """Even with registry access, default allow_enter cannot ENTER."""
        from app.workers.strategies.wyckoff_v2.constants import resolve_config
        from app.workers.strategies.wyckoff_v2.models import (
            FourHourTriggerResult,
            HTFContextResult,
            InvalidationResult,
            PhaseCandidate,
            PhaseClassificationResult,
            PriceZone,
            RangeCandidate,
            ReadinessResult,
            StructureClassificationResult,
        )
        from app.workers.strategies.wyckoff_v2.policy import evaluate_policy

        ready = ReadinessResult(
            readiness_version="wyckoff_readiness.v1",
            ready=True,
            status="ready",
            reason_codes=(),
            latest_bar_completion={"state": "completed"},
            evaluation_time_utc="2024-06-28T20:00:00Z",
            market_data_as_of="2024-06-28",
            desired_history_bars=600,
            requested_history_bars=600,
            available_input_bars=600,
            available_completed_bars=600,
            history_depth_capped=False,
            history_depth_complete=True,
            required_monthly_periods=24,
            available_completed_monthly_periods=30,
            required_weekly_periods=26,
            available_completed_weekly_periods=40,
            required_daily_structure_bars=120,
            usable_volume_bars=600,
            required_volume_bars=100,
            volume_coverage=1.0,
            excluded_partial_daily_bar_date=None,
            missing_fields=(),
        )
        support = PriceZone(lo=98.0, hi=100.0)
        resistance = PriceZone(lo=110.0, hi=112.0)
        rng = RangeCandidate(
            range_candidate_version="wyckoff_range.v1",
            candidate_id="range_1",
            as_of_date="2024-06-28",
            start_date="2024-01-02",
            end_date="2024-03-01",
            start_index=10,
            end_index=49,
            post_range_bar_count=5,
            bar_count=40,
            support_zone=support,
            resistance_zone=resistance,
            support=support.midpoint,
            resistance=resistance.midpoint,
            midpoint=105.0,
            width=14.0,
            atr=2.0,
            width_atr_multiple=7.0,
            support_interactions=(),
            resistance_interactions=(),
            support_touch_cluster_count=2,
            resistance_touch_cluster_count=2,
            containment_fraction=0.9,
            breakout_contamination_fraction=0.05,
            volume_coverage=1.0,
            quality_components={},
            range_quality=0.8,
            valid=True,
            rejection_reasons=(),
        )
        structure = StructureClassificationResult(
            phase_classification_version="wyckoff_phases.v1",
            as_of_date="2024-06-28",
            range_candidate_id="range_1",
            classification="accumulation",
            state="recognized",
            accumulation_event_types=("SC", "Spring"),
            distribution_event_types=(),
            accumulation_candidate_ids=("a1", "a2"),
            distribution_candidate_ids=(),
            accumulation_confirmed_type_count=2,
            distribution_confirmed_type_count=0,
            accumulation_signature_events=("Spring",),
            distribution_signature_events=(),
            contradiction_codes=(),
            reason_codes=(),
        )
        phase_cand = PhaseCandidate(
            phase_candidate_version="wyckoff_phases.v1",
            candidate_id="phase_C",
            structure="accumulation",
            phase="C",
            ordinal=3,
            status="confirmed",
            as_of_date="2024-06-28",
            required_event_codes=("SC",),
            supporting_candidate_ids=("a1", "a2"),
            contradicting_candidate_ids=(),
            missing_event_codes=(),
            required_gate_codes=(),
            passed_gate_codes=(),
            missing_gate_codes=(),
            failed_gate_codes=(),
            sequence_valid=True,
            confidence_components={},
            confidence=0.5,
            reason_codes=(),
        )
        phase = PhaseClassificationResult(
            phase_classification_version="wyckoff_phases.v1",
            as_of_date="2024-06-28",
            structure_classification=structure,
            selected_phase="C",
            selected_phase_status="confirmed",
            phase_state="PHASE_C",
            candidates=(phase_cand,),
            reason_codes=(),
            config_used={},
        )
        htf = HTFContextResult(
            htf_context_version="wyckoff_htf_context.v1",
            as_of_date="2024-06-28",
            monthly_bias="up",
            monthly_sma=100.0,
            monthly_slope_pct=1.0,
            monthly_trend_quality=0.8,
            monthly_window_structure="hh",
            monthly_window_raw={},
            weekly_bias="up",
            weekly_sma=100.0,
            weekly_slope_pct=0.5,
            weekly_trend_quality=0.7,
            weekly_window_structure="hh",
            weekly_window_raw={},
            htf_alignment="aligned_up",
            contradiction_codes=(),
            missing_data=(),
            config_used={},
        )
        trigger = FourHourTriggerResult(
            trigger_version="wyckoff_4h_trigger.v1",
            enabled=True,
            state="confirmed",
            reason_codes=(),
            side="LONG",
            evaluation_time_utc="2024-06-28T20:00:00Z",
            daily_market_data_as_of="2024-06-28",
            available_input_bars=20,
            available_completed_bars=20,
            required_completed_bars=11,
            excluded_incomplete_bar_count=0,
            latest_completed_4h_start="2024-06-28T12:00:00Z",
            latest_completed_4h_end="2024-06-28T16:00:00Z",
            latest_completed_4h_session_date="2024-06-28",
            staleness_sessions=0,
            local_high=101.0,
            local_low=99.0,
            trigger_level=101.0,
            contradiction_level=99.0,
            current_close=105.0,
            trigger_price=105.0,
            triggered=True,
            contradicted=False,
            missing_data=(),
            config_used={},
        )
        inv = InvalidationResult(
            invalidation_version="wyckoff_invalidation.v1",
            rule_code="daily_close_below_support_zone",
            level=97.8,
            source_range_id="range_1",
            source_event_ids=("a1", "a2"),
            zone={"lo": 98.0, "hi": 100.0},
            atr=2.0,
            buffer_atr_multiple=0.1,
            timeframe="1d",
            as_of="2024-06-28",
            reason=None,
            available=True,
        )
        r = evaluate_policy(
            readiness=ready,
            selected_range=rng,
            structure=structure,
            phase_result=phase,
            htf=htf,
            trigger=trigger,
            invalidation=inv,
            last_close=100.0,
            config=resolve_config({"allow_enter": False, "enable_4h_trigger": True}),
        )
        assert r.verdict == "WATCH"
        assert "enter_disabled_shadow_only" in r.waiting_reasons

        strategy = WyckoffMTFV2Strategy()
        result = strategy.evaluate(
            __import__("pandas").DataFrame(),
            StrategyContext(
                symbol="X",
                pattern_code="wyckoff_mtf_v2",
                config=default_config(),
                data_meta={"evaluation_time_utc": "2024-06-28T21:00:00Z"},
            ),
        )
        assert result.decision != StrategyDecision.ENTER

    def test_v1_unchanged(self):
        from app.workers.strategies.registry import get_strategy
        from app.workers.strategies.wyckoff import STRATEGY_VERSION

        v1 = get_strategy("wyckoff_mtf")
        assert v1.pattern_code == "wyckoff_mtf"
        assert v1.version == STRATEGY_VERSION
        assert STRATEGY_VERSION == "wyckoff_mtf.v1"


class TestPhase9C2Boundaries:
    def test_no_funnel_decision_card_scheduler_api_persistence_diff(self):
        # Phase 9C3 may add admin read-only strategy discovery in admin.py.
        # Public listing and execution surfaces must stay unchanged.
        assert _git_diff(
            "app/workers/scanner/funnel.py",
            "app/workers/strategies/decision_card.py",
            "app/workers/persistence.py",
            "app/routers/public.py",
            "app/routers/outcomes.py",
            "app/routers/shadow.py",
            "app/workers/scheduler",
            "app/scheduler",
            "app/jobs",
            "app/workers/outcomes",
            "app/workers/shadow",
            "app/providers",
            "docs/architecture/evidence-engine-roadmap.md",
        ) == ""

    def test_no_provider_db_imports_in_new_migration_path(self):
        # Migration is SQL-only; registry still has no provider/db imports.
        tree = ast.parse(REGISTRY.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert not mod.startswith("app.db")
                assert not mod.startswith("app.providers")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("app.db")
                    assert not alias.name.startswith("app.providers")

    def test_migration_012_does_not_touch_shadow_tables(self):
        sql = MIGRATION_012.read_text(encoding="utf-8").lower()
        assert "strategy_shadow" not in sql
        assert "signal_outcomes" not in sql
