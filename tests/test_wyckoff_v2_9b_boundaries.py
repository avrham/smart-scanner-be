"""Phase 9B boundary guards — registry, migration, isolation.

Phase 9C1 may include policy, StrategyResult orchestration, evidence mapping,
and allow_enter config. This file no longer forbids those concepts via
source-text substring checks.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess

from app.workers.strategies.registry import list_strategies


ROOT = pathlib.Path(__file__).resolve().parents[1]
V2 = ROOT / "app" / "workers" / "strategies" / "wyckoff_v2"
MIGRATIONS = ROOT / "app" / "db" / "migrations"

FORBIDDEN_IMPORT_PREFIXES = (
    "app.db",
    "app.providers",
    "openai",
    "anthropic",
    "app.workers.external",
    "app.routers",
)


def _read(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def test_phase9a_and_9b_surface_still_importable():
    import app.workers.strategies.wyckoff_v2 as v2

    assert callable(v2.assess_data_readiness)
    assert callable(v2.aggregate_completed_timeframes)
    assert callable(v2.detect_trading_ranges)
    assert callable(v2.measure_htf_context)
    assert callable(v2.measure_effort_result_at_index)
    assert callable(v2.detect_event_candidates)
    assert callable(v2.classify_structure)
    assert callable(v2.classify_phases)


def test_registry_unchanged_no_v2():
    codes = list_strategies()
    assert "wyckoff_mtf_v2" not in codes
    assert "wyckoff_mtf" in codes


def test_no_migration_012():
    files = sorted(p.name for p in MIGRATIONS.glob("*.sql"))
    assert "012_wyckoff_mtf_v2.sql" not in files
    assert not any(name.startswith("012_") for name in files)
    assert "011_shadow_pair_outcomes.sql" in files


def test_no_provider_or_db_imports_in_9b_modules():
    modules = (
        "context_htf.py",
        "effort_result.py",
        "events.py",
        "phases.py",
    )
    for name in modules:
        path = V2 / name
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for prefix in FORBIDDEN_IMPORT_PREFIXES:
                        assert not alias.name.startswith(prefix), f"{name}: {alias.name}"
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                for prefix in FORBIDDEN_IMPORT_PREFIXES:
                    assert not mod.startswith(prefix), f"{name}: {mod}"
        text = _read(path)
        assert "save_signal" not in text


def test_v1_package_untouched_marker():
    v1 = ROOT / "app" / "workers" / "strategies" / "wyckoff" / "strategy.py"
    assert v1.exists()
    text = _read(v1)
    assert "wyckoff_mtf.v1" in text or "STRATEGY_VERSION" in text


def test_no_scheduler_or_router_imports_in_v2():
    for path in V2.glob("*.py"):
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert not mod.startswith("app.routers"), path.name
                assert "apscheduler" not in mod.lower()
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("app.routers")
                    assert "apscheduler" not in alias.name.lower()


def test_protected_surfaces_unmodified():
    assert _git_diff(
        "app/workers/strategies/decision_card.py",
        "app/workers/scanner/funnel.py",
        "app/workers/persistence.py",
        "app/workers/strategies/registry.py",
    ) == ""
