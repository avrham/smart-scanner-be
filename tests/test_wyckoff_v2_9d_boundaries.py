"""Phase 9D7: production safety proof.

Phase 9D adds SHADOW measurement only. These tests prove the production
surfaces — funnel ranking, candidate selection, scheduled scans, scheduler
strategy selection, public listings, decision cards, watches, alerts,
notifications, Wyckoff v1, SMA150, provider selection, registry side
effects and rollout defaults — are untouched.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess

import pytest

from app.workers.strategies.registry import list_strategies
from app.workers.strategies.wyckoff_v2.constants import (
    default_config as v2_default_config,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def _module_imports(rel_path: str) -> set:
    tree = ast.parse((ROOT / rel_path).read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add(node.module or "")
    return names


class TestProductionSurfacesUnmodified:
    def test_funnel_scheduler_cards_watches_untouched(self):
        assert _git_diff(
            "app/workers/scanner/funnel.py",
            "app/workers/scan_runner.py",
            "app/workers/scheduler.py",
            "app/workers/strategies/decision_card.py",
            "app/workers/persistence.py",
            "app/workers/screening.py",
        ) == ""

    def test_wyckoff_v1_and_v2_strategy_code_untouched(self):
        assert _git_diff(
            "app/workers/strategies/wyckoff",
            "app/workers/strategies/wyckoff_v2",
        ) == ""

    def test_sma150_strategies_untouched(self):
        assert _git_diff(
            "app/workers/patterns/sma150.py",
            "app/workers/strategies/sma150_adapter.py",
            "app/workers/strategies/sma150_v3.py",
        ) == ""

    def test_providers_and_registry_untouched(self):
        assert _git_diff(
            "app/providers",
            "app/workers/strategies/registry.py",
        ) == ""

    def test_public_routers_untouched(self):
        assert _git_diff(
            "app/routers/public.py",
            "app/routers/outcomes.py",
            "app/routers/shadow.py",
        ) == ""

    def test_signal_outcome_architecture_untouched(self):
        assert _git_diff("app/workers/outcomes") == ""

    def test_earlier_migrations_untouched(self):
        assert _git_diff(
            "app/db/migrations/001_initial_schema.sql",
            "app/db/migrations/002_phase1_sma150_config.sql",
            "app/db/migrations/003_phase2_signal_outcomes.sql",
            "app/db/migrations/004_phase5_wyckoff_mtf_config.sql",
            "app/db/migrations/005_massive_provider.sql",
            "app/db/migrations/006_market_data_jobs.sql",
            "app/db/migrations/007_scan_signal_provenance.sql",
            "app/db/migrations/008_sma150_v3.sql",
            "app/db/migrations/009_watch_outcome_coverage.sql",
            "app/db/migrations/010_sma150_shadow_evaluations.sql",
            "app/db/migrations/011_shadow_pair_outcomes.sql",
            "app/db/migrations/012_wyckoff_mtf_v2.sql",
        ) == ""


class TestSchedulerBoundaries:
    def test_scheduler_still_hardcodes_sma150_bounce(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(
            encoding="utf-8"
        )
        assert 'pattern_code="sma150_bounce"' in text
        assert "wyckoff_mtf_v2" not in text

    def test_no_automatic_shadow_or_dry_run_scheduling(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(
            encoding="utf-8"
        )
        for forbidden in ("shadow", "dry_run", "dry-run", "experiment"):
            assert forbidden not in text.lower(), forbidden

    def test_funnel_and_scan_runner_never_import_9d_modules(self):
        for rel in (
            "app/workers/scanner/funnel.py",
            "app/workers/scan_runner.py",
            "app/workers/scheduler.py",
        ):
            imports = _module_imports(rel)
            for name in imports:
                assert "shadow" not in name, (rel, name)
                assert "dry_run" not in name, (rel, name)


class TestRegistryAndRolloutBoundaries:
    def test_registered_strategy_set_unchanged(self):
        assert list_strategies() == [
            "sma150_bounce",
            "sma150_bounce_v3",
            "wyckoff_mtf",
            "wyckoff_mtf_v2",
        ]

    def test_package_import_still_has_no_registration_side_effect(self):
        import importlib
        import sys

        from app.workers.strategies import registry

        modules = (
            "app.workers.strategies.wyckoff_v2",
            "app.workers.shadow.experiments",
            "app.workers.strategies.dry_run",
        )
        saved = dict(registry._REGISTRY)
        # Preserve the ORIGINAL module objects: a re-imported copy left in
        # sys.modules would fork class identities (exception isinstance
        # checks across modules would silently break in later tests).
        saved_modules = {name: sys.modules.get(name) for name in modules}
        try:
            registry._REGISTRY.clear()
            sys.modules.pop("app.workers.strategies.wyckoff_v2", None)
            importlib.import_module("app.workers.strategies.wyckoff_v2")
            assert "wyckoff_mtf_v2" not in registry._REGISTRY
            # Phase 9D modules must not register anything either.
            sys.modules.pop("app.workers.shadow.experiments", None)
            importlib.import_module("app.workers.shadow.experiments")
            sys.modules.pop("app.workers.strategies.dry_run", None)
            importlib.import_module("app.workers.strategies.dry_run")
            assert registry._REGISTRY == {}
        finally:
            registry._REGISTRY.clear()
            registry._REGISTRY.update(saved)
            for name, module in saved_modules.items():
                if module is not None:
                    sys.modules[name] = module
                    # Re-importing also rebinds the leaf attribute on the
                    # parent package; restore it so `import a.b.c as x`
                    # resolves the ORIGINAL module everywhere.
                    parent_name, _, leaf = name.rpartition(".")
                    parent = sys.modules.get(parent_name)
                    if parent is not None:
                        setattr(parent, leaf, module)
                else:
                    sys.modules.pop(name, None)

    def test_rollout_defaults_preserved(self):
        cfg = v2_default_config()
        assert cfg["allow_enter"] is False
        assert cfg["enable_4h_trigger"] is False
        assert cfg["min_price"] == 5.0

    def test_migration_012_still_seeds_disabled(self):
        sql = (
            ROOT / "app" / "db" / "migrations" / "012_wyckoff_mtf_v2.sql"
        ).read_text(encoding="utf-8")
        assert "false" in sql.lower()
        assert "wyckoff_mtf_v2" in sql
        # 013 never flips enablement or rollout flags.
        sql13 = "\n".join(
            line
            for line in (
                ROOT / "app" / "db" / "migrations"
                / "013_wyckoff_v2_shadow_arms.sql"
            ).read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "is_enabled" not in sql13
        assert "allow_enter" not in sql13
        assert "patterns" not in sql13


class TestShadowStaysShadow:
    def test_shadow_modules_never_touch_production_persistence(self):
        for rel in (
            "app/workers/shadow/runner.py",
            "app/workers/shadow/experiments.py",
            "app/workers/shadow/strategy_metrics.py",
            "app/workers/strategies/dry_run.py",
        ):
            text = (ROOT / rel).read_text(encoding="utf-8")
            assert "save_signal" not in text, rel
            assert "build_decision_card" not in text, rel
            assert "INSERT INTO signals" not in text, rel

    def test_no_alert_or_notification_subsystem_introduced(self):
        for rel in (
            "app/workers/shadow/runner.py",
            "app/workers/shadow/experiments.py",
            "app/workers/shadow/persistence.py",
            "app/workers/shadow/strategy_metrics.py",
            "app/workers/strategies/dry_run.py",
            "app/routers/admin.py",
        ):
            text = (ROOT / rel).read_text(encoding="utf-8").lower()
            for forbidden in ("send_email", "telegram", "slack",
                              "push_notification", "webhook"):
                assert forbidden not in text, (rel, forbidden)

    def test_shadow_writes_only_shadow_tables(self):
        text = (ROOT / "app" / "workers" / "shadow" / "persistence.py").read_text(
            encoding="utf-8"
        )
        for stmt in ("INSERT INTO", "UPDATE "):
            for line in text.splitlines():
                if stmt in line and "strategy_shadow" not in line:
                    pytest.fail(f"non-shadow write: {line.strip()}")

    def test_dry_run_module_is_read_only(self):
        text = (ROOT / "app" / "workers" / "strategies" / "dry_run.py").read_text(
            encoding="utf-8"
        )
        for verb in ("INSERT", "UPDATE ", "DELETE ", "create_shadow_run",
                     "persist_shadow_pair", "upsert"):
            assert verb not in text, verb

    def test_experiments_registry_keeps_candidate_disabled(self):
        """Declaring the experiment must not enable anything: the registry
        module only references pattern codes, never patterns.is_enabled or
        pattern_configs."""
        text = (
            ROOT / "app" / "workers" / "shadow" / "experiments.py"
        ).read_text(encoding="utf-8")
        assert "is_enabled" not in text
        assert "pattern_configs" not in text
        imports = _module_imports("app/workers/shadow/experiments.py")
        for name in imports:
            assert not name.startswith("app.providers"), name
            assert "persistence" not in name, name


class TestPublicBehaviorUnchanged:
    def test_disabled_wyckoff_v2_still_hidden_from_public_patterns(self):
        from fastapi.testclient import TestClient

        from app.deps import get_db
        from main import app
        from test_wyckoff_v2_9c3_discovery import _v2_configured_db

        db = _v2_configured_db()

        async def _override():
            yield db

        app.dependency_overrides[get_db] = _override
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/api/patterns")
            assert resp.status_code == 200
            codes = [p["code"] for p in resp.json()]
            assert "wyckoff_mtf_v2" not in codes
            assert "sma150_bounce" in codes
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_admin_strategy_discovery_contract_preserved(self):
        """9C3 missing-row representation is untouched by 9D."""
        from app.workers.strategies.discovery import (
            build_discovery_from_sources,
        )

        item = build_discovery_from_sources(
            pattern_code="wyckoff_mtf_v2",
            strategy_version="wyckoff_mtf.v2",
            decision_policy_version="wyckoff_mtf.policy.v1",
            defaults=v2_default_config(),
            pattern_row=None,
            raw_config={},
        )
        assert item.registered is True
        assert item.enabled is None
        assert item.db_configured is False
        assert item.config_status == "missing_pattern_row"
