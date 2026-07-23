"""Phase 9F10: production safety proof.

Phase 9F adds ADVISORY, READ-ONLY evidence review. These tests prove no
9F code path can enable Wyckoff v2, mutate rollout configuration, execute
campaigns from reads, or alter any production surface.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import subprocess

import pytest

from app.workers.strategies.registry import list_strategies
from app.workers.strategies.wyckoff_v2.constants import (
    default_config as v2_default_config,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]

NEW_9F_MODULES = (
    "app/workers/shadow/evidence_review.py",
    "app/workers/shadow/evidence_cohorts.py",
    "app/workers/shadow/outcome_evidence.py",
    "app/workers/shadow/quality_audit.py",
    "app/workers/shadow/rollout_readiness.py",
    "app/workers/shadow/evidence_export.py",
    "app/workers/shadow/campaign_planning.py",
)


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    return result.stdout.strip()


class TestProductionSurfacesUnmodified:
    def test_scheduler_funnel_cards_watches_untouched(self):
        assert _git_diff(
            "app/workers/scanner/funnel.py",
            "app/workers/scan_runner.py",
            "app/workers/scheduler.py",
            "app/workers/strategies/decision_card.py",
            "app/workers/persistence.py",
            "app/workers/screening.py",
        ) == ""

    def test_wyckoff_v1_v2_and_sma150_untouched(self):
        assert _git_diff(
            "app/workers/strategies/wyckoff",
            "app/workers/strategies/wyckoff_v2",
            "app/workers/patterns/sma150.py",
            "app/workers/strategies/sma150_adapter.py",
            "app/workers/strategies/sma150_v3.py",
            "app/workers/strategies/registry.py",
        ) == ""

    def test_providers_public_routers_untouched(self):
        assert _git_diff(
            "app/providers",
            "app/routers/public.py",
            "app/routers/outcomes.py",
            "app/routers/shadow.py",
        ) == ""

    def test_runner_experiments_campaigns_untouched(self):
        # 9F is read-only: the execution layer itself is unmodified.
        assert _git_diff(
            "app/workers/shadow/runner.py",
            "app/workers/shadow/experiments.py",
            "app/workers/shadow/campaigns.py",
            "app/workers/shadow/frames.py",
            "app/workers/shadow/frames_4h.py",
            "app/workers/shadow/fingerprints.py",
        ) == ""

    def test_migration_sequence_ends_at_013_unmodified(self):
        assert _git_diff("app/db/migrations") == ""
        migrations = sorted(
            p.name for p in (ROOT / "app" / "db" / "migrations").glob("*.sql")
        )
        assert migrations[-1] == "013_wyckoff_v2_shadow_arms.sql"
        assert not [m for m in migrations if m.startswith(("014_", "015_"))]


class TestNoActivationPath:
    def test_no_code_path_writes_patterns_or_pattern_configs(self):
        """The strongest rollout guarantee: NOTHING in app/ can flip
        patterns.is_enabled or rewrite pattern_configs — activation is a
        manual operator SQL step by design."""
        result = subprocess.run(
            ["grep", "-rn", "-E",
             "UPDATE +patterns|INSERT +INTO +patterns"
             "|UPDATE +pattern_configs|INSERT +INTO +pattern_configs",
             "app/"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        assert result.stdout.strip() == "", result.stdout

    def test_9f_modules_are_pure_read_only(self):
        for rel in NEW_9F_MODULES:
            text = (ROOT / rel).read_text(encoding="utf-8")
            for forbidden in (
                "INSERT", "UPDATE ", "DELETE ", "save_signal",
                "build_decision_card", "persist_shadow_pair",
                "upsert_pair_outcome", "create_shadow_run",
                "run_shadow_campaign(", "run_shadow_comparison(",
                "get_market_data_provider", "BackgroundTasks",
                "apscheduler",
            ):
                assert forbidden not in text, (rel, forbidden)
            # 'is_enabled' may only ever appear as the FROZEN response
            # snapshot key / docstring phrase "patterns.is_enabled" — never
            # as an assignment or SQL fragment (writes are excluded above
            # and by the repo-wide activation-path grep).
            stripped = text.replace("patterns.is_enabled", "")
            assert "is_enabled" not in stripped, rel

    def test_9f_modules_never_import_providers_or_execution(self):
        for rel in NEW_9F_MODULES:
            tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                for name in names:
                    assert not name.startswith("app.providers"), (rel, name)
                    assert "runner" not in name, (rel, name)
                    assert "fmp" not in name.lower(), (rel, name)
                    assert "massive" not in name.lower(), (rel, name)

    def test_readiness_policy_cannot_return_enabled(self):
        from app.workers.shadow.rollout_readiness import ADVISORY_STATUSES

        assert "enabled" not in ADVISORY_STATUSES
        assert all("enable" not in s for s in ADVISORY_STATUSES)

    def test_readiness_source_never_touches_rollout_keys_mutably(self):
        from app.workers.shadow import rollout_readiness

        source = inspect.getsource(rollout_readiness)
        # The rollout keys appear ONLY inside the frozen response snapshot.
        assert source.count("allow_enter") <= 3
        assert "resolve_pattern_config" not in source
        assert "pattern_configs" not in source


class TestRolloutDefaults:
    def test_defaults_unchanged(self):
        cfg = v2_default_config()
        assert cfg["allow_enter"] is False
        assert cfg["enable_4h_trigger"] is False
        assert cfg["min_price"] == 5.0

    def test_registered_strategy_set_unchanged(self):
        assert list_strategies() == [
            "sma150_bounce",
            "sma150_bounce_v3",
            "wyckoff_mtf",
            "wyckoff_mtf_v2",
        ]

    def test_scheduler_still_hardcodes_sma150_bounce(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(
            encoding="utf-8"
        )
        assert 'pattern_code="sma150_bounce"' in text
        assert "wyckoff" not in text.lower()
        assert "evidence" not in text.lower()

    def test_sma150_pair_fingerprints_still_frozen(self):
        from datetime import datetime, timezone

        from app.workers.provenance import canonical_json, _sha256
        from app.workers.shadow.fingerprints import compute_pair_fingerprint

        control = {
            "strategy_code": "sma150_bounce", "strategy_version": "sma150.v2",
            "decision_policy_version": "strategy_decision.v1",
            "config_hash": "aaa",
        }
        candidate = {
            "strategy_code": "sma150_bounce_v3",
            "strategy_version": "sma150.v3",
            "decision_policy_version": "sma150_bounce.policy.v1",
            "config_hash": "bbb",
        }
        as_of = datetime(2026, 7, 17, tzinfo=timezone.utc)
        actual = compute_pair_fingerprint(
            symbol="JBL", timeframe="1d", provider="fake", frame_hash="fh",
            snapshot_date="2026-07-17", market_data_as_of=as_of,
            control_identity=control, candidate_identity=candidate,
        )
        expected = _sha256(canonical_json({
            "fingerprint_version": "shadow_pair_fingerprint.v1",
            "experiment_code": "sma150_v2_vs_v3",
            "experiment_version": "sma150_shadow.v1",
            "symbol": "JBL", "timeframe": "1d", "provider": "fake",
            "frame_hash": "fh", "snapshot_date": "2026-07-17",
            "market_data_as_of": as_of.isoformat(),
            "control": control, "candidate": candidate,
        }))
        assert actual == expected


class TestNoSideChannels:
    def test_no_alert_or_notification_subsystem(self):
        for rel in NEW_9F_MODULES + ("app/routers/admin.py",):
            text = (ROOT / rel).read_text(encoding="utf-8").lower()
            for forbidden in ("send_email", "telegram", "slack",
                              "push_notification", "webhook"):
                assert forbidden not in text, (rel, forbidden)

    def test_registry_import_side_effect_free(self):
        import importlib
        import sys

        from app.workers.strategies import registry

        modules = tuple(
            rel.replace("app/", "app.").replace("/", ".").removesuffix(".py")
            for rel in NEW_9F_MODULES
        )
        saved = dict(registry._REGISTRY)
        saved_modules = {name: sys.modules.get(name) for name in modules}
        try:
            registry._REGISTRY.clear()
            for name in modules:
                sys.modules.pop(name, None)
                importlib.import_module(name)
            assert registry._REGISTRY == {}
        finally:
            registry._REGISTRY.clear()
            registry._REGISTRY.update(saved)
            for name, module in saved_modules.items():
                if module is not None:
                    sys.modules[name] = module
                    parent_name, _, leaf = name.rpartition(".")
                    parent = sys.modules.get(parent_name)
                    if parent is not None:
                        setattr(parent, leaf, module)
                else:
                    sys.modules.pop(name, None)

    def test_no_env_or_secret_files_tracked(self):
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        tracked = result.stdout.splitlines()
        assert ".env" not in tracked
        for name in tracked:
            assert not name.endswith((".pem", ".key")), name

    def test_runbook_exists_without_secrets(self):
        runbook = ROOT / "docs" / "wyckoff-v2-live-evidence-runbook.md"
        assert runbook.exists()
        text = runbook.read_text(encoding="utf-8")
        assert "allow_enter = false" in text
        assert "013_wyckoff_v2_shadow_arms" in text
        lowered = text.lower()
        for fragment in ("postgresql://", "supabase_db_password",
                         "massive_api_key=", "fmp_api_key="):
            assert fragment not in lowered, fragment


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
            assert "wyckoff_mtf_v2" not in [
                p["code"] for p in resp.json()
            ]
        finally:
            app.dependency_overrides.pop(get_db, None)
