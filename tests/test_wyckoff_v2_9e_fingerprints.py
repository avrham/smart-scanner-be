"""Phase 9E4: fingerprints and typed persistence for 4H shadow evidence."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pytest

from app.workers.provenance import canonical_json, _sha256, config_hash
from app.workers.shadow import runner as shadow_runner
from app.workers.shadow.experiments import (
    ShadowExperiment,
    WYCKOFF_V2_VS_BASELINE,
)
from app.workers.shadow.fingerprints import compute_pair_fingerprint
from app.workers.shadow.runner import run_shadow_comparison

from test_shadow_comparison import NOW_UTC, default_configs, store  # noqa: F401
from test_wyckoff_v2_9d_shadow import _long_daily_payload
from test_wyckoff_v2_9e_shadow_mtf import MtfFakeProvider, _intraday_bars


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"


def _run(coro):
    return asyncio.run(coro)


_CONTROL = {
    "strategy_code": "sma150_bounce", "strategy_version": "sma150.v2",
    "decision_policy_version": "strategy_decision.v1", "config_hash": "aaa",
}
_CANDIDATE = {
    "strategy_code": "sma150_bounce_v3", "strategy_version": "sma150.v3",
    "decision_policy_version": "sma150_bounce.policy.v1", "config_hash": "bbb",
}
_KWARGS = dict(
    symbol="JBL", timeframe="1d", provider="fake", frame_hash="fh",
    snapshot_date="2026-07-17",
    market_data_as_of=datetime(2026, 7, 17, tzinfo=timezone.utc),
    control_identity=_CONTROL, candidate_identity=_CANDIDATE,
)


class TestPairFingerprintCompatibility:
    def test_sma150_payload_is_byte_identical_without_four_hour(self):
        """The daily-only fingerprint payload is reconstructed manually:
        omitting `four_hour` (and passing None) yields EXACTLY the
        historical hash — existing SMA150 fingerprints never change."""
        legacy_payload = {
            "fingerprint_version": "shadow_pair_fingerprint.v1",
            "experiment_code": "sma150_v2_vs_v3",
            "experiment_version": "sma150_shadow.v1",
            "symbol": "JBL",
            "timeframe": "1d",
            "provider": "fake",
            "frame_hash": "fh",
            "snapshot_date": "2026-07-17",
            "market_data_as_of": datetime(
                2026, 7, 17, tzinfo=timezone.utc
            ).isoformat(),
            "control": dict(_CONTROL),
            "candidate": dict(_CANDIDATE),
        }
        expected = _sha256(canonical_json(legacy_payload))
        assert compute_pair_fingerprint(**_KWARGS) == expected
        assert compute_pair_fingerprint(**_KWARGS, four_hour=None) == expected

    def test_four_hour_component_changes_identity(self):
        base = compute_pair_fingerprint(**_KWARGS)
        with_4h = compute_pair_fingerprint(**_KWARGS, four_hour={
            "contract_version": "four_hour_frame.v1",
            "frame_hash": "4h-hash-1",
            "state": "built",
        })
        assert with_4h != base

    def test_changed_4h_hash_changes_identity(self):
        f1 = compute_pair_fingerprint(**_KWARGS, four_hour={
            "contract_version": "four_hour_frame.v1",
            "frame_hash": "4h-hash-1", "state": "built",
        })
        f2 = compute_pair_fingerprint(**_KWARGS, four_hour={
            "contract_version": "four_hour_frame.v1",
            "frame_hash": "4h-hash-2", "state": "built",
        })
        assert f1 != f2

    def test_absent_vs_present_4h_frame_differ(self):
        built = compute_pair_fingerprint(**_KWARGS, four_hour={
            "contract_version": "four_hour_frame.v1",
            "frame_hash": "4h-hash-1", "state": "built",
        })
        missing = compute_pair_fingerprint(**_KWARGS, four_hour={
            "contract_version": "four_hour_frame.v1",
            "frame_hash": None, "state": "fetch_error",
        })
        assert built != missing


class TestConfigOverrideMateriality:
    def test_override_enters_config_hash_and_snapshot(
        self, default_configs
    ):
        plain = _run(shadow_runner._resolve_arm(
            "wyckoff_mtf_v2", "candidate_wyckoff_v2",
        ))
        overridden = _run(shadow_runner._resolve_arm(
            "wyckoff_mtf_v2", "candidate_wyckoff_v2",
            config_overrides={"enable_4h_trigger": True},
        ))
        assert plain["config_hash"] != overridden["config_hash"]
        assert plain["config_snapshot"]["enable_4h_trigger"] is False
        assert overridden["config_snapshot"]["enable_4h_trigger"] is True
        assert overridden["config_overrides"] == {"enable_4h_trigger": True}
        assert plain["config_overrides"] == {}
        # The hash is the standard provenance hash of the effective config.
        assert overridden["config_hash"] == config_hash(overridden["config"])

    def test_allow_enter_override_is_forbidden_by_construction(self):
        with pytest.raises(ValueError):
            ShadowExperiment(
                experiment_code="x",
                experiment_version="x.v1",
                control_pattern_code="sma150_bounce",
                candidate_pattern_code="wyckoff_mtf_v2",
                control_arm_code="control_baseline",
                candidate_arm_code="candidate_wyckoff_v2",
                control_category_label="control",
                candidate_category_label="candidate",
                control_history_bars=lambda cfg: 100,
                candidate_history_bars=lambda cfg: 100,
                candidate_config_overrides={"allow_enter": True},
            )


class TestRunnerIdempotencyWith4h:
    def test_identical_mtf_run_is_idempotent(self, store, default_configs):
        bars = _intraday_bars(breakout_close=None)
        payloads = {"LONGX": _long_daily_payload()}
        provider = MtfFakeProvider(payloads, intraday_bars={"LONGX": bars})
        first = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        assert first["telemetry"]["pairs_created"] == 1
        second = _run(run_shadow_comparison(
            MtfFakeProvider(payloads, intraday_bars={"LONGX": bars}),
            ["LONGX"], now_utc=NOW_UTC, experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        assert second["telemetry"]["pairs_created"] == 0
        assert second["telemetry"]["pairs_deduplicated"] == 1
        assert len(store.pairs) == 1

    def test_changed_4h_input_creates_new_pair_identity(
        self, store, default_configs
    ):
        payloads = {"LONGX": _long_daily_payload()}
        bars = _intraday_bars(breakout_close=None)
        _run(run_shadow_comparison(
            MtfFakeProvider(payloads, intraday_bars={"LONGX": bars}),
            ["LONGX"], now_utc=NOW_UTC, experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        changed = [dict(b) for b in bars]
        changed[0] = {**changed[0], "close": changed[0]["close"] + 0.01}
        summary = _run(run_shadow_comparison(
            MtfFakeProvider(payloads, intraday_bars={"LONGX": changed}),
            ["LONGX"], now_utc=NOW_UTC, experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        # Same daily frame, different 4H input -> a NEW immutable pair.
        assert summary["telemetry"]["pairs_created"] == 1
        assert len(store.pairs) == 2


class TestTypedPersistenceWith4h:
    def test_mtf_evaluation_persists_with_exact_driver_types(self, monkeypatch):
        from test_shadow_typed_persistence import StrictTypedConn
        from app.workers.shadow import persistence as shadow_persistence

        db = StrictTypedConn()

        async def get_conn():
            return db

        async def release(_conn):
            return None

        monkeypatch.setattr(shadow_persistence, "get_db_connection", get_conn)
        monkeypatch.setattr(
            shadow_persistence, "release_db_connection", release
        )

        async def fake_resolve(pattern_code, defaults):
            return dict(defaults)

        monkeypatch.setattr(
            shadow_runner, "resolve_pattern_config", fake_resolve
        )

        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=None)},
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert db.forbidden == []
        assert summary["telemetry"]["experiment_version"] == (
            "wyckoff_v2_shadow.v2"
        )

        # The candidate evaluation's frozen JSONB carries the 4H frame
        # metadata AND the strategy's own trigger record — queryable via
        # JSONB paths without any schema change.
        candidate_row = [
            row for row in db.eval_inserts if row[2] == "candidate_wyckoff_v2"
        ][0]
        snapshot = json.loads(candidate_row[12])
        meta = snapshot["_four_hour_frame_meta"]
        assert meta["contract_version"] == "four_hour_frame.v1"
        assert meta["state"] == "built"
        assert isinstance(meta["frame_hash"], str)
        # The config snapshot column shows the frozen override.
        config_snapshot = json.loads(candidate_row[7])
        assert config_snapshot["enable_4h_trigger"] is True
        assert config_snapshot["allow_enter"] is False

    def test_old_rows_without_4h_metadata_remain_readable(self):
        """Backward compatibility: the pair-summary reader is agnostic to
        the optional _four_hour_frame_meta key (old rows simply lack it)."""
        from app.workers.shadow.persistence import _pair_summary

        class Row(dict):
            def __getitem__(self, key):
                return dict.get(self, key)

        row = Row({
            "id": uuid.uuid4(), "origin_run_id": None,
            "experiment_code": "wyckoff_v2_vs_baseline",
            "experiment_version": "wyckoff_v2_shadow.v2",
            "symbol": "LONGX", "timeframe": "1d", "provider": "fake",
            "snapshot_date": "2026-07-17", "market_data_as_of": None,
            "frame_snapshot_version": "daily_ohlcv_snapshot.v1",
            "frame_hash": "fh", "frame_bar_count": 500,
            "control_arm_code": "control_baseline",
            "control_strategy_code": "sma150_bounce",
            "control_strategy_version": "sma150.v2",
            "control_decision_policy_version": "strategy_decision.v1",
            "control_config_hash": "c", "control_verdict": "AVOID",
            "control_score": None, "control_reason": None,
            "control_rejection_reason": None,
            "candidate_arm_code": "candidate_wyckoff_v2",
            "candidate_strategy_code": "wyckoff_mtf_v2",
            "candidate_strategy_version": "wyckoff_mtf.v2",
            "candidate_decision_policy_version": "wyckoff_mtf.policy.v1",
            "candidate_config_hash": "x", "candidate_verdict": "WATCH",
            "candidate_score": None, "candidate_reason": None,
            "candidate_rejection_reason": None,
            "created_at": None,
        })
        summary = _pair_summary(row)
        assert summary["disagreement_category"] == (
            "control_avoid_candidate_watch"
        )


class TestNoNewMigration:
    def test_migration_sequence_stays_at_013(self):
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("013_*"))] == [
            "013_wyckoff_v2_shadow_arms.sql"
        ]
        assert not list(MIGRATIONS_DIR.glob("014_*"))
        assert not list(MIGRATIONS_DIR.glob("015_*"))

    def test_4h_evidence_fits_existing_schema(self):
        """Everything Phase 9E persists rides in EXISTING columns: the 4H
        frame metadata + trigger evidence live inside the bounded
        details_snapshot JSONB; the experiment override lives inside the
        config_snapshot JSONB and config_hash; no migration adds columns."""
        sql_013 = (MIGRATIONS_DIR / "013_wyckoff_v2_shadow_arms.sql").read_text(
            encoding="utf-8"
        )
        executable = "\n".join(
            line for line in sql_013.splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "ADD COLUMN" not in executable.upper()
        assert "CREATE TABLE" not in executable.upper()
