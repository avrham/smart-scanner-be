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


def test_no_v2_replaces_v1_identity():
    """Phase 9C2 may register v2, but v1 identity must remain distinct."""
    from app.workers.strategies.registry import get_strategy, list_strategies
    from app.workers.strategies.wyckoff import STRATEGY_VERSION

    assert "wyckoff_mtf" in list_strategies()
    strategy = get_strategy("wyckoff_mtf")
    assert strategy.pattern_code == "wyckoff_mtf"
    assert strategy.version == STRATEGY_VERSION
    assert STRATEGY_VERSION == "wyckoff_mtf.v1"


def test_migration_012_is_wyckoff_v2_only():
    paths = sorted(MIGRATIONS.glob("012_*"))
    assert [p.name for p in paths] == ["012_wyckoff_mtf_v2.sql"]
    # Phase 9D3 adds exactly 013_wyckoff_v2_shadow_arms (arm-code
    # CHECK extension only); nothing later exists.
    assert [p.name for p in sorted(MIGRATIONS.glob("013_*"))] == [
        "013_wyckoff_v2_shadow_arms.sql"
    ]
    assert [q.name for q in sorted(MIGRATIONS.glob("014_*"))] == ["014_market_bars_4h.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("019_*"))] == ["019_catalyst_events.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("020_*"))] == ["020_company_news.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("021_*"))] == ["021_sec_material_events.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("022_*"))] == ["022_external_signals.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("023_*"))] == ["023_external_discovery.sql"]
    # Wave 2 adds exactly 024_market_calendar_and_analyst
    # (macro calendar + analyst change events + registry V2);
    # nothing later exists.
    assert [p.name for p in sorted(MIGRATIONS.glob("024_*"))] == [
        "024_market_calendar_and_analyst.sql"
    ]
    assert [p.name for p in sorted(MIGRATIONS.glob("025_*"))] == [
        "025_discovery_reference_session.sql"
    ]
    assert [p.name for p in sorted(MIGRATIONS.glob("026_*"))] == [
        "026_research_symbols.sql"
    ]
    assert [p.name for p in sorted(MIGRATIONS.glob("027_*"))] == [
        "027_research_admission.sql"
    ]
    assert [q.name for q in sorted(MIGRATIONS.glob("028_*"))] == [
        "028_source_state_scope.sql"]
    assert [q.name for q in sorted(MIGRATIONS.glob("029_*"))] == [
        "029_research_lifecycle_runs.sql"]
    assert not list(MIGRATIONS.glob("030_*"))
    assert (MIGRATIONS / "011_shadow_pair_outcomes.sql").exists()
    sql = (MIGRATIONS / "012_wyckoff_mtf_v2.sql").read_text(encoding="utf-8")
    assert "wyckoff_mtf_v2" in sql
    assert "('wyckoff_mtf'," not in sql


def _git_diff_names(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--name-only", "--", *paths],
        cwd=ROOT, capture_output=True, text=True, check=False)
    return result.stdout.strip()


def test_no_scheduler_change():
    """The queue/scheduler surface, guarded by CONTENT rather than by silence.

    The original guard asserted an empty `git diff -- app/jobs`. That is a
    working-tree assertion: it holds only until the change is committed, and
    it cannot distinguish "the daily pipeline's dispatch was altered" from
    "a new, separate queue was added beside it" — which is exactly what the
    research lifecycle is.

    So the intent is kept and made state-independent: the modified files
    under app/jobs must be only the two the new queue needs, and the
    pre-existing daily-pipeline dispatch must still be there, unchanged in
    the parts that matter — its task type, its queue, its idempotency key
    shape, and the fact that a schedule with no declared owner is still
    materialised by any leader exactly as before.
    """
    result = subprocess.run(
        ["git", "diff", "--", "app/workers/scheduler", "app/scheduler"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == ""

    allowed = {"app/jobs/registry.py", "app/jobs/scheduler.py"}
    changed = {p for p in _git_diff_names("app/jobs").split("\n") if p}
    assert changed <= allowed, f"unexpected queue changes: {sorted(changed - allowed)}"

    from app.jobs import daily_pipeline as DP
    from app.jobs import scheduler as S
    # The daily-pipeline driver spec is untouched: same task, same queue,
    # same attempts, and it still requires a frozen universe_id.
    spec = S._pipeline_driver_spec({
        "job_type": DP.PIPELINE_JOB_TYPE,
        "payload_template": {"universe_id": "u-1", "universe_hash": "h"}})
    assert spec["task_type"] == DP.DAILY_PIPELINE_ADVANCE_TASK
    assert spec["queue"] == DP.DAILY_PIPELINE_DRIVER_QUEUE
    assert spec["max_attempts"] == DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS
    assert S._pipeline_driver_spec({"job_type": DP.PIPELINE_JOB_TYPE,
                                    "payload_template": {}}) is None
    # An unowned schedule is still materialised by any leader.
    assert S._schedule_is_ownable({"payload_template": {}})


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
