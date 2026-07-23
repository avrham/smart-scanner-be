"""Phase 9E9: production safety proof.

Phase 9E adds shadow MEASUREMENT capability only (canonical 4H frames,
experiment-only trigger analysis, bounded campaigns, protected reads).
These tests prove production behavior is untouched and that every rollout
default survives.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
from datetime import datetime, timezone

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

    def test_registry_and_factory_untouched(self):
        assert _git_diff(
            "app/workers/strategies/registry.py",
            "app/providers/__init__.py",
        ) == ""

    def test_public_routers_untouched(self):
        assert _git_diff(
            "app/routers/public.py",
            "app/routers/outcomes.py",
            "app/routers/shadow.py",
        ) == ""

    def test_outcome_architecture_untouched(self):
        assert _git_diff(
            "app/workers/outcomes",
            "app/workers/shadow/outcomes",
        ) == ""

    def test_all_migrations_untouched_and_no_new_one(self):
        assert _git_diff("app/db/migrations") == ""
        migrations = sorted(
            p.name for p in (ROOT / "app" / "db" / "migrations").glob("*.sql")
        )
        assert migrations[-1] == "013_wyckoff_v2_shadow_arms.sql"
        assert not [m for m in migrations if m.startswith(("014_", "015_"))]


class TestSchedulerBoundaries:
    def test_scheduler_still_hardcodes_sma150_bounce(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(
            encoding="utf-8"
        )
        assert 'pattern_code="sma150_bounce"' in text
        assert "wyckoff_mtf_v2" not in text

    def test_no_automatic_campaign_or_shadow_scheduling(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(
            encoding="utf-8"
        ).lower()
        for forbidden in ("shadow", "campaign", "intraday", "4h"):
            assert forbidden not in text, forbidden

    def test_production_scan_paths_never_import_9e_modules(self):
        for rel in (
            "app/workers/scanner/funnel.py",
            "app/workers/scan_runner.py",
            "app/workers/scheduler.py",
        ):
            tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                for name in names:
                    assert "shadow" not in name, (rel, name)
                    assert "campaign" not in name, (rel, name)
                    assert "frames_4h" not in name, (rel, name)


class TestRolloutDefaults:
    def test_rollout_defaults_preserved(self):
        cfg = v2_default_config()
        assert cfg["allow_enter"] is False
        assert cfg["enable_4h_trigger"] is False
        assert cfg["min_price"] == 5.0

    def test_migration_012_defaults_untouched(self):
        assert _git_diff("app/db/migrations/012_wyckoff_mtf_v2.sql") == ""

    def test_override_is_isolated_to_the_shadow_experiment(self):
        """The experiment-only override never leaks: strategy defaults,
        production resolution and the DB seed all keep enable_4h_trigger
        false; only the frozen shadow arm config carries true."""
        from app.workers.shadow.experiments import WYCKOFF_V2_VS_BASELINE
        from app.workers.strategies.registry import get_strategy

        assert WYCKOFF_V2_VS_BASELINE.candidate_config_overrides == {
            "enable_4h_trigger": True
        }
        assert get_strategy("wyckoff_mtf_v2").default_config()[
            "enable_4h_trigger"
        ] is False
        # allow_enter can never be overridden by ANY experiment.
        for experiment in (
            WYCKOFF_V2_VS_BASELINE,
        ):
            overrides = experiment.candidate_config_overrides or {}
            assert "allow_enter" not in overrides

    def test_registered_strategy_set_unchanged(self):
        assert list_strategies() == [
            "sma150_bounce",
            "sma150_bounce_v3",
            "wyckoff_mtf",
            "wyckoff_mtf_v2",
        ]


class TestSma150FingerprintsUnchanged:
    def test_sma150_pair_fingerprint_payload_frozen(self):
        """The daily-only fingerprint hash is pinned against a manual
        reconstruction of the historical payload — Phase 9E can never move
        existing SMA150 shadow identities."""
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

    def test_sma150_experiment_declaration_unchanged(self):
        from app.workers.shadow.experiments import SMA150_V2_VS_V3

        assert SMA150_V2_VS_V3.experiment_code == "sma150_v2_vs_v3"
        assert SMA150_V2_VS_V3.experiment_version == "sma150_shadow.v1"
        assert SMA150_V2_VS_V3.requires_four_hour_frame is False
        assert SMA150_V2_VS_V3.candidate_config_overrides is None


class TestNoSideChannels:
    def test_no_alert_or_notification_subsystem(self):
        for rel in (
            "app/workers/shadow/campaigns.py",
            "app/workers/shadow/frames_4h.py",
            "app/workers/shadow/runner.py",
            "app/workers/shadow/experiments.py",
            "app/routers/admin.py",
            "app/providers/base.py",
        ):
            text = (ROOT / rel).read_text(encoding="utf-8").lower()
            for forbidden in ("send_email", "telegram", "slack",
                              "push_notification", "webhook"):
                assert forbidden not in text, (rel, forbidden)

    def test_campaign_and_frame_modules_never_touch_production_persistence(
        self,
    ):
        for rel in (
            "app/workers/shadow/campaigns.py",
            "app/workers/shadow/frames_4h.py",
        ):
            text = (ROOT / rel).read_text(encoding="utf-8")
            assert "save_signal" not in text, rel
            assert "build_decision_card" not in text, rel
            assert "INSERT" not in text.upper(), rel
            assert "UPDATE " not in text, rel

    def test_registry_import_side_effect_free(self):
        import importlib
        import sys

        from app.workers.strategies import registry

        modules = (
            "app.workers.shadow.frames_4h",
            "app.workers.shadow.campaigns",
            "app.workers.shadow.experiments",
        )
        saved = dict(registry._REGISTRY)
        # Preserve the ORIGINAL module objects: a re-imported copy left in
        # sys.modules would fork class identities (exception isinstance
        # checks across modules would silently break in later tests).
        saved_modules = {name: sys.modules.get(name) for name in modules}
        try:
            registry._REGISTRY.clear()
            for module in modules:
                sys.modules.pop(module, None)
                importlib.import_module(module)
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

    def test_no_env_or_secret_files_tracked(self):
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        tracked = result.stdout.splitlines()
        assert ".env" not in tracked
        for name in tracked:
            assert not name.endswith((".pem", ".key")), name

    def test_no_credentials_in_frame_or_campaign_metadata(self):
        for rel in (
            "app/workers/shadow/frames_4h.py",
            "app/workers/shadow/campaigns.py",
        ):
            text = (ROOT / rel).read_text(encoding="utf-8").lower()
            assert "api_key" not in text, rel
            assert "settings." not in text, rel


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
        finally:
            app.dependency_overrides.pop(get_db, None)
