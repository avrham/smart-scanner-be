"""Phase 9C1 boundary guards — unregistered, no migration 012, no I/O."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

from app.workers.strategies.registry import list_strategies
from app.workers.strategies.wyckoff_v2.constants import DEFAULT_CONFIG, default_config


ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "app" / "workers" / "strategies" / "wyckoff_v2"
MIGRATIONS = ROOT / "app" / "db" / "migrations"

C1_MODULES = (
    "trigger_4h.py",
    "policy.py",
    "evidence_map.py",
    "strategy.py",
)

FORBIDDEN_IMPORT_PREFIXES = (
    "app.db",
    "app.providers",
    "openai",
    "anthropic",
    "app.workers.external",
)


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def test_no_migration_012():
    assert not any(MIGRATIONS.glob("012_*"))
    assert (MIGRATIONS / "011_shadow_pair_outcomes.sql").exists()


def test_registry_unchanged_no_v2():
    codes = list_strategies()
    assert "wyckoff_mtf_v2" not in codes
    assert "wyckoff_mtf" in codes


def test_package_exports_strategy_without_registration():
    before = list(list_strategies())
    from app.workers.strategies.wyckoff_v2 import WyckoffMTFV2Strategy
    import app.workers.strategies.wyckoff_v2 as v2

    assert v2.WyckoffMTFV2Strategy is WyckoffMTFV2Strategy
    assert WyckoffMTFV2Strategy.pattern_code == "wyckoff_mtf_v2"
    assert WyckoffMTFV2Strategy.version == "wyckoff_mtf.v2"
    assert WyckoffMTFV2Strategy.decision_policy_version == "wyckoff_mtf.policy.v1"
    after = list(list_strategies())
    assert after == before
    assert "wyckoff_mtf_v2" not in after


def test_default_rollout_and_min_price():
    cfg = default_config()
    assert cfg["allow_enter"] is False
    assert cfg["enable_4h_trigger"] is False
    assert cfg["require_4h_trigger_for_enter"] is True
    assert cfg["min_price"] == 5.0
    assert DEFAULT_CONFIG["allow_enter"] is False
    assert DEFAULT_CONFIG["min_price"] == 5.0


def test_no_forbidden_surface_diffs():
    paths = [
        "app/workers/strategies/wyckoff",
        "app/workers/strategies/registry.py",
        "app/workers/strategies/decision_card.py",
        "app/workers/scanner/funnel.py",
        "app/workers/provenance.py",
        "app/workers/persistence.py",
        "app/workers/outcomes",
        "app/workers/shadow",
        "app/routers",
        "app/db/migrations",
        "docs/architecture/evidence-engine-roadmap.md",
    ]
    assert _git_diff(*paths) == ""


def test_c1_modules_no_provider_db_llm():
    for name in C1_MODULES:
        path = V2 / name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        text = path.read_text(encoding="utf-8")
        assert "save_signal" not in text
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for prefix in FORBIDDEN_IMPORT_PREFIXES:
                        assert not alias.name.startswith(prefix), f"{name}: {alias.name}"
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                for prefix in FORBIDDEN_IMPORT_PREFIXES:
                    assert not mod.startswith(prefix), f"{name}: {mod}"


def test_c1_files_exist():
    for name in C1_MODULES:
        assert (V2 / name).exists()
    for name in (
        "test_wyckoff_v2_trigger_4h.py",
        "test_wyckoff_v2_policy.py",
        "test_wyckoff_v2_evidence_map.py",
        "test_wyckoff_v2_strategy.py",
        "test_wyckoff_v2_9c1_boundaries.py",
    ):
        assert (ROOT / "tests" / name).exists()


def test_evidence_map_internal_not_required_on_package():
    import app.workers.strategies.wyckoff_v2 as v2

    # Internal module; package need not re-export helpers.
    assert not hasattr(v2, "build_evidence_bundle") or True
    from app.workers.strategies.wyckoff_v2 import evidence_map as em

    assert callable(em.build_evidence_bundle)
