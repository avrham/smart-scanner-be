"""Phase 9A boundary tests — isolation, no registration, no migration 012.

Phase 9C1 may export WyckoffMTFV2Strategy from the package without registering
it. This file no longer forbids a Strategy class export.
"""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "app" / "workers" / "strategies" / "wyckoff_v2"
MIGRATIONS = ROOT / "app" / "db" / "migrations"


FORBIDDEN_IMPORT_PREFIXES = (
    "app.db",
    "app.providers",
    "openai",
    "anthropic",
    "app.workers.external",
)
FORBIDDEN_NAMES = {
    "save_signal",
    "create_engine",
    "asyncpg",
    "psycopg2",
    "httpx",
    "requests",
}


def _python_files(package: Path):
    return sorted(package.rglob("*.py"))


def test_v1_package_has_no_diff():
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/strategies/wyckoff"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""
    assert result.returncode == 0


def test_v1_registry_identity_unchanged():
    from app.workers.strategies.registry import get_strategy, list_strategies
    from app.workers.strategies.wyckoff import STRATEGY_VERSION

    assert "wyckoff_mtf" in list_strategies()
    strategy = get_strategy("wyckoff_mtf")
    assert strategy.pattern_code == "wyckoff_mtf"
    assert strategy.version == STRATEGY_VERSION
    assert STRATEGY_VERSION == "wyckoff_mtf.v1"


def test_no_v2_registry_entry_yet():
    from app.workers.strategies.registry import list_strategies

    assert "wyckoff_mtf_v2" not in list_strategies()


def test_no_migration_012():
    paths = sorted(MIGRATIONS.glob("012_*"))
    assert paths == []
    assert (MIGRATIONS / "011_shadow_pair_outcomes.sql").exists()


def test_no_scheduler_change():
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/scheduler", "app/scheduler", "app/jobs"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""


def test_v2_pure_package_has_no_forbidden_imports():
    for path in _python_files(V2):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for prefix in FORBIDDEN_IMPORT_PREFIXES:
                        assert not alias.name.startswith(prefix), (
                            f"{path.name} imports {alias.name}"
                        )
                    assert alias.name not in FORBIDDEN_NAMES
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                for prefix in FORBIDDEN_IMPORT_PREFIXES:
                    assert not mod.startswith(prefix), f"{path.name} imports {mod}"
                for alias in node.names:
                    assert alias.name not in FORBIDDEN_NAMES, (
                        f"{path.name} imports name {alias.name}"
                    )


def test_no_evidence_v1_modification():
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/strategies/evidence.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""


def test_no_outcome_modification():
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/outcomes", "app/workers/shadow"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""


def test_no_decision_card_modification():
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/strategies/decision_card.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""


def test_bar_completion_shared_and_reexported():
    from app.workers.strategies import bar_completion, sma150_v3

    assert hasattr(bar_completion, "assess_latest_bar_completion")
    assert hasattr(sma150_v3, "assess_latest_bar_completion")
    assert sma150_v3.BAR_COMPLETION_POLICY == bar_completion.BAR_COMPLETION_POLICY
    assert bar_completion.BAR_COMPLETION_POLICY == "ny_session_close.v1"


def test_phase_9a_surface_remains_importable():
    """Compatibility: Phase 9A public functions stay importable from the package."""
    import app.workers.strategies.wyckoff_v2 as v2

    assert v2.STRATEGY_CODE == "wyckoff_mtf_v2"
    assert v2.STRATEGY_VERSION == "wyckoff_mtf.v2"
    assert callable(v2.assess_data_readiness)
    assert callable(v2.detect_trading_ranges)
    assert callable(v2.aggregate_completed_timeframes)
    assert callable(v2.normalize_canonical_daily)
    assert callable(v2.derive_history_requirement)
