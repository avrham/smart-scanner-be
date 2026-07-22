"""Phase 9B boundary guards — no orchestration, registry, migration, or I/O."""

from __future__ import annotations

import ast
import pathlib

from app.workers.strategies.registry import list_strategies


ROOT = pathlib.Path(__file__).resolve().parents[1]
V2 = ROOT / "app" / "workers" / "strategies" / "wyckoff_v2"
MIGRATIONS = ROOT / "app" / "db" / "migrations"


def _read(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def test_phase9a_surface_still_importable():
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


def test_no_strategy_result_or_policy_or_evidence_mapping():
    forbidden_snippets = (
        "StrategyResult",
        "evidence.v1",
        "allow_enter",
        "decision_card",
        "map_evidence",
    )
    for path in V2.glob("*.py"):
        if path.name == "__init__.py":
            text = _read(path)
            # __init__ may mention versions in comments only — check imports
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    mod = (
                        node.module
                        if isinstance(node, ast.ImportFrom)
                        else ",".join(a.name for a in node.names)
                    )
                    assert mod is None or "decision_card" not in (mod or "")
                    assert mod is None or "evidence" not in (mod or "")
            continue
        text = _read(path)
        for snip in ("class StrategyResult", "def map_to_evidence", "allow_enter="):
            assert snip not in text, f"{path.name} contains {snip}"


def test_no_provider_or_db_imports_in_9b_modules():
    modules = (
        "context_htf.py",
        "effort_result.py",
        "events.py",
        "phases.py",
    )
    banned = (
        "app.db",
        "app.providers",
        "sqlalchemy",
        "openai",
        "anthropic",
        "save_signal",
        "httpx",
        "requests",
    )
    for name in modules:
        text = _read(V2 / name)
        for b in banned:
            assert b not in text, f"{name} references {b}"


def test_v1_package_untouched_marker():
    # v1 strategy module still present and independent
    v1 = ROOT / "app" / "workers" / "strategies" / "wyckoff" / "strategy.py"
    assert v1.exists()
    text = _read(v1)
    assert "wyckoff_mtf.v1" in text or "STRATEGY_VERSION" in text


def test_no_scheduler_or_router_changes_required():
    # Boundary: 9B modules must not import routers/scheduler
    for path in V2.glob("*.py"):
        text = _read(path)
        assert "app.routers" not in text
        assert "apscheduler" not in text.lower()
