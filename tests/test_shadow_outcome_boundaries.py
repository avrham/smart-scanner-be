"""Phase 8.1B2 isolation, migration and contract-boundary checks.

Also hosts the optional PostgreSQL integration test (skipped without
TEST_DATABASE_URL): insert a temporary B1-compatible pair, mature one
outcome through real asyncpg codecs, roll back. Never requires providers.
"""

import asyncio
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.workers.shadow.constants import (
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    PAIR_FINGERPRINT_VERSION,
    EVALUATION_FINGERPRINT_VERSION,
)
from app.workers.shadow.outcomes.constants import (
    CALCULATION_VERSION,
    FORWARD_FRAME_VERSION,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
    REFERENCE_PRICE_ROLE,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "app" / "db" / "migrations"
SHADOW_PKG = ROOT / "app" / "workers" / "shadow"
OUTCOMES_PKG = SHADOW_PKG / "outcomes"
MIGRATION_010 = MIGRATIONS_DIR / "010_sma150_shadow_evaluations.sql"
MIGRATION_011 = MIGRATIONS_DIR / "011_shadow_pair_outcomes.sql"


class TestMigrationBoundaries:
    def test_exactly_migration_011_and_012_wyckoff_only(self):
        # Phase 8.1B2 added 011; Phase 9C2 adds exactly 012_wyckoff_mtf_v2.
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("011_*"))] == [
            "011_shadow_pair_outcomes.sql"
        ]
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("012_*"))] == [
            "012_wyckoff_mtf_v2.sql"
        ]
        # Phase 9D3 adds exactly 013_wyckoff_v2_shadow_arms (arm-code
        # CHECK extension only); nothing later exists.
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("013_*"))] == [
            "013_wyckoff_v2_shadow_arms.sql"
        ]
        assert not list(MIGRATIONS_DIR.glob("014_*"))
        sql = (MIGRATIONS_DIR / "012_wyckoff_mtf_v2.sql").read_text(encoding="utf-8")
        assert "strategy_shadow" not in sql.lower()
        assert "wyckoff_mtf_v2" in sql

    def test_migration_011_creates_required_tables(self):
        sql = MIGRATION_011.read_text()
        assert "strategy_shadow_pair_outcomes" in sql
        assert "strategy_shadow_outcome_runs" in sql
        assert "paired_decision_observation" in sql
        assert "pending_forward_bars" in sql
        assert "REFERENCES public.strategy_shadow_pairs" in sql

    def test_migration_010_byte_identical_to_git_head(self):
        # Must not have been rewritten in this phase.
        import subprocess
        result = subprocess.run(
            ["git", "diff", "--", str(MIGRATION_010.relative_to(ROOT))],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0
        assert result.stdout == ""


class TestB1ContractsUnchanged:
    def test_b1_fingerprint_versions_unchanged(self):
        assert PAIR_FINGERPRINT_VERSION == "shadow_pair_fingerprint.v1"
        assert EVALUATION_FINGERPRINT_VERSION == "shadow_evaluation_fingerprint.v1"

    def test_b1_experiment_version_unchanged(self):
        assert EXPERIMENT_CODE == "sma150_v2_vs_v3"
        assert EXPERIMENT_VERSION == "sma150_shadow.v1"

    def test_b2_contract_versions(self):
        assert CALCULATION_VERSION == "outcome.v1"
        assert OUTCOME_COVERAGE_VERSION == "shadow_pair_outcomes.v1"
        assert OUTCOME_FINGERPRINT_VERSION == (
            "shadow_pair_outcome_fingerprint.v1"
        )
        assert FORWARD_FRAME_VERSION == "shadow_forward_bars.v1"
        assert REFERENCE_PRICE_ROLE == "paired_decision_observation"


class TestIsolation:
    def _sources(self):
        sources = {
            f"outcomes/{p.name}": p.read_text()
            for p in sorted(OUTCOMES_PKG.glob("*.py"))
        }
        sources["router_shadow"] = (
            ROOT / "app" / "routers" / "shadow.py"
        ).read_text()
        sources["router_admin"] = (
            ROOT / "app" / "routers" / "admin.py"
        ).read_text()
        return sources

    def test_no_writes_to_signal_tables(self):
        for name, source in self._sources().items():
            assert "save_signal(" not in source, name
            assert "import save_signal" not in source, name
            assert "INSERT INTO signals" not in source, name
            assert "INSERT INTO signal_provenance" not in source, name
            assert "INSERT INTO scan_run_signals" not in source, name
            assert "INSERT INTO signal_outcomes" not in source, name
            assert "UPDATE signals" not in source, name
            assert "UPDATE signal_outcomes" not in source, name
            assert "upsert_signal_outcome" not in source, name
            assert "get_signals_needing_outcomes" not in source, name

    def test_no_b1_pair_mutation(self):
        pers = (OUTCOMES_PKG / "persistence.py").read_text()
        assert "UPDATE strategy_shadow_pairs" not in pers
        assert "UPDATE strategy_shadow_evaluations" not in pers
        assert "INSERT INTO strategy_shadow_pairs" not in pers
        assert "INSERT INTO strategy_shadow_evaluations" not in pers

    def test_scheduler_unchanged(self):
        scheduler = (ROOT / "app" / "workers" / "scheduler.py").read_text().lower()
        assert "shadow" not in scheduler
        assert "shadow_outcome" not in scheduler
        assert "shadow/outcomes" not in scheduler

    def test_v3_remains_disabled_in_migration_008(self):
        sql = (MIGRATIONS_DIR / "008_sma150_v3.sql").read_text()
        assert "is_enabled" in sql
        assert "false" in sql.lower() or "FALSE" in sql

    def test_one_outcome_per_pair_not_per_arm(self):
        sql = MIGRATION_011.read_text()
        assert "pair_id UUID NOT NULL UNIQUE" in sql
        assert "arm_code" not in sql
        create = sql.split(
            "CREATE TABLE IF NOT EXISTS public.strategy_shadow_pair_outcomes"
        )[1].split(");")[0]
        assert "control_return" not in create
        assert "candidate_return" not in create

    def test_no_same_ticker_baseline_or_trade_semantics_in_outcomes_pkg(self):
        import ast

        forbidden_names = {
            "same_ticker", "hit_stop", "hit_target", "simulated_r", "win_rate",
        }
        for path in OUTCOMES_PKG.glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Name) and node.id.lower() in forbidden_names:
                    pytest.fail(f"{path.name} uses forbidden name {node.id}")
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    # Dict keys / emitted API strings must not use win_rate.
                    if node.value == "win_rate":
                        pytest.fail(f"{path.name} emits win_rate string")
                    if node.value in ("hit_stop", "hit_target", "simulated_r"):
                        pytest.fail(f"{path.name} emits {node.value}")

# --------------------------------------------------------------------------- #
# Optional real-PostgreSQL integration (skipped without TEST_DATABASE_URL)
# --------------------------------------------------------------------------- #

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL not set; real-codec integration test skipped",
)
class TestPostgresOutcomeIntegration:
    def test_outcome_insert_mature_and_rollback(self, monkeypatch):
        """Real asyncpg codecs against migrations 010+011, rolled back.

        Never calls providers. Inserts a temporary B1-compatible pair, then
        an outcome that matures from pending -> partial, verifying freeze
        semantics and codecs. Everything rolls back.
        """
        import asyncpg
        import app.workers.shadow.persistence as b1_pers
        import app.workers.shadow.outcomes.persistence as o_pers
        from app.workers.shadow.persistence import (
            create_shadow_run,
            persist_shadow_pair,
        )
        from app.workers.shadow.outcomes.persistence import upsert_pair_outcome
        from app.workers.shadow.constants import (
            CANDIDATE_ARM_CODE,
            CONTROL_ARM_CODE,
            FRAME_SNAPSHOT_VERSION,
        )
        from app.workers.shadow.outcomes.fingerprints import (
            compute_outcome_fingerprint,
        )

        async def scenario():
            conn = await asyncpg.connect(TEST_DATABASE_URL)
            tx = conn.transaction()
            await tx.start()
            try:
                async def get_conn():
                    return conn

                async def release(_conn):
                    return None

                monkeypatch.setattr(b1_pers, "get_db_connection", get_conn)
                monkeypatch.setattr(b1_pers, "release_db_connection", release)
                monkeypatch.setattr(o_pers, "get_db_connection", get_conn)
                monkeypatch.setattr(o_pers, "release_db_connection", release)

                run_id = str(uuid.uuid4())
                await create_shadow_run(
                    run_id, provider="integration",
                    requested_symbols=["ITGB2"], requested_limit=1,
                )
                fp_suffix = str(uuid.uuid4())
                result = await persist_shadow_pair(
                    run_id=run_id,
                    pair={
                        "experiment_code": EXPERIMENT_CODE,
                        "experiment_version": EXPERIMENT_VERSION,
                        "symbol": "ITGB2",
                        "timeframe": "1d",
                        "provider": "integration",
                        "snapshot_date": date(2026, 7, 17),
                        "market_data_as_of": datetime(
                            2026, 7, 17, tzinfo=timezone.utc
                        ),
                        "frame_snapshot_version": FRAME_SNAPSHOT_VERSION,
                        "frame_hash": f"itgb2-{fp_suffix}",
                        "frame_bar_count": 2,
                        "frame_first_date": "2026-07-16",
                        "frame_last_date": "2026-07-17",
                        "frame_snapshot": [
                            {"date": "2026-07-16", "open": 1.0, "high": 2.0,
                             "low": 0.5, "close": 1.5, "volume": 10.0},
                            {"date": "2026-07-17", "open": 1.5, "high": 2.5,
                             "low": 1.0, "close": 2.0, "volume": 12.0},
                        ],
                        "pair_fingerprint": f"itgb2-fp-{fp_suffix}",
                        "pair_fingerprint_version": PAIR_FINGERPRINT_VERSION,
                    },
                    evaluations=[
                        {
                            "arm_code": CONTROL_ARM_CODE,
                            "strategy_code": "sma150",
                            "strategy_version": "sma150.v2",
                            "decision_policy_version": None,
                            "config_hash": f"cfg-v2-{fp_suffix}",
                            "config_snapshot": {"k": 1},
                            "verdict": "AVOID",
                            "score": None,
                            "reason": "test",
                            "rejection_reason": "test",
                            "details_snapshot": {"evidence": None},
                            "evaluation_fingerprint": f"ev-v2-{fp_suffix}",
                            "evaluation_fingerprint_version":
                                EVALUATION_FINGERPRINT_VERSION,
                        },
                        {
                            "arm_code": CANDIDATE_ARM_CODE,
                            "strategy_code": "sma150",
                            "strategy_version": "sma150.v3",
                            "decision_policy_version":
                                "sma150_bounce.policy.v1",
                            "config_hash": f"cfg-v3-{fp_suffix}",
                            "config_snapshot": {"k": 1},
                            "verdict": "AVOID",
                            "score": None,
                            "reason": "test",
                            "rejection_reason": "test",
                            "details_snapshot": {"evidence": {"v": 1}},
                            "evaluation_fingerprint": f"ev-v3-{fp_suffix}",
                            "evaluation_fingerprint_version":
                                EVALUATION_FINGERPRINT_VERSION,
                        },
                    ],
                )
                pair_id = result["pair_id"]
                pair_fp = f"itgb2-fp-{fp_suffix}"
                outcome_fp = compute_outcome_fingerprint(
                    pair_fingerprint=pair_fp,
                    pair_fingerprint_version=PAIR_FINGERPRINT_VERSION,
                )

                first = await upsert_pair_outcome({
                    "pair_id": pair_id,
                    "outcome_fingerprint": outcome_fp,
                    "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
                    "calculation_version": CALCULATION_VERSION,
                    "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
                    "forward_frame_version": FORWARD_FRAME_VERSION,
                    "reference_price": 2.0,
                    "reference_price_role": REFERENCE_PRICE_ROLE,
                    "forward_provider": "integration",
                    "forward_data_as_of": date(2026, 7, 18),
                    "available_forward_bars": 1,
                    "first_forward_date": date(2026, 7, 18),
                    "last_forward_date": date(2026, 7, 18),
                    "forward_bars_hash": "h1",
                    "ret_1d": 1.5,
                    "ret_3d": None,
                    "ret_5d": None,
                    "ret_10d": None,
                    "ret_20d": None,
                    "max_favorable_excursion": 2.0,
                    "max_adverse_excursion": -0.5,
                    "mfe_mae_bar_count": 1,
                    "benchmark_returns": {
                        "SPY": {
                            "1D": 0.1, "3D": None, "5D": None,
                            "10D": None, "20D": None,
                        },
                        "QQQ": {
                            "1D": None, "3D": None, "5D": None,
                            "10D": None, "20D": None,
                        },
                    },
                    "revision_notes": [],
                    "reference_revision_detected": False,
                    "outcome_status": "partial",
                    "error_code": None,
                    "error_message": None,
                })
                assert first["outcome_status"] == "partial"
                assert first["created_new"] is True

                row1 = await conn.fetchrow(
                    "SELECT ret_1d, ret_3d, available_forward_bars, "
                    "outcome_fingerprint, mfe_mae_bar_count "
                    "FROM strategy_shadow_pair_outcomes WHERE pair_id = $1",
                    uuid.UUID(pair_id),
                )
                assert row1["ret_1d"] == pytest.approx(1.5)
                assert row1["ret_3d"] is None
                assert row1["outcome_fingerprint"] == outcome_fp

                # Maturation: fill 3D, attempt to overwrite 1D (must freeze).
                second = await upsert_pair_outcome({
                    "pair_id": pair_id,
                    "outcome_fingerprint": outcome_fp,
                    "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
                    "calculation_version": CALCULATION_VERSION,
                    "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
                    "forward_frame_version": FORWARD_FRAME_VERSION,
                    "reference_price": 2.0,
                    "reference_price_role": REFERENCE_PRICE_ROLE,
                    "forward_provider": "integration",
                    "forward_data_as_of": date(2026, 7, 21),
                    "available_forward_bars": 3,
                    "first_forward_date": date(2026, 7, 18),
                    "last_forward_date": date(2026, 7, 21),
                    "forward_bars_hash": "h3",
                    "ret_1d": 99.0,  # must NOT overwrite
                    "ret_3d": 3.0,
                    "ret_5d": None,
                    "ret_10d": None,
                    "ret_20d": None,
                    "max_favorable_excursion": 4.0,
                    "max_adverse_excursion": -1.0,
                    "mfe_mae_bar_count": 3,
                    "benchmark_returns": {
                        "SPY": {
                            "1D": 0.1, "3D": 0.2, "5D": None,
                            "10D": None, "20D": None,
                        },
                        "QQQ": {
                            "1D": None, "3D": None, "5D": None,
                            "10D": None, "20D": None,
                        },
                    },
                    "revision_notes": [],
                    "reference_revision_detected": False,
                    "outcome_status": "partial",
                    "error_code": None,
                    "error_message": None,
                })
                assert second["created_new"] is False
                assert second["outcome_status"] == "partial"

                row2 = await conn.fetchrow(
                    "SELECT ret_1d, ret_3d, available_forward_bars, "
                    "outcome_fingerprint, mfe_mae_bar_count "
                    "FROM strategy_shadow_pair_outcomes WHERE pair_id = $1",
                    uuid.UUID(pair_id),
                )
                assert row2["ret_1d"] == pytest.approx(1.5)
                assert row2["ret_3d"] == pytest.approx(3.0)
                assert row2["available_forward_bars"] == 3
                assert row2["mfe_mae_bar_count"] == 3
                assert row2["outcome_fingerprint"] == outcome_fp

                # One row only.
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM strategy_shadow_pair_outcomes "
                    "WHERE pair_id = $1",
                    uuid.UUID(pair_id),
                )
                assert count == 1
            finally:
                await tx.rollback()
                await conn.close()

        asyncio.run(scenario())
