"""Phase 9D2/9D3: Wyckoff MTF v2 through the canonical shadow runner."""

from __future__ import annotations

import asyncio
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from app.workers.provenance import canonical_json
from app.workers.shadow import runner as shadow_runner
from app.workers.shadow.constants import (
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    FRAME_FETCH_MARGIN_BARS,
    FRAME_HARD_CAP_BARS,
    GENERIC_DISAGREEMENT_CATEGORIES,
    MAX_DETAILS_SNAPSHOT_BYTES,
)
from app.workers.shadow.experiments import (
    DEFAULT_EXPERIMENT,
    EXPERIMENTS,
    KNOWN_ARM_CODES,
    SMA150_V2_VS_V3,
    WYCKOFF_V2_VS_BASELINE,
    UnknownShadowExperimentError,
    experiment_for_candidate,
    get_experiment,
)
from app.workers.shadow.fingerprints import (
    category_label_for_arm,
    disagreement_category,
)
from app.workers.shadow.frames import (
    required_history_bars_v2,
    required_history_bars_wyckoff_v2,
    shared_required_history_bars,
)
from app.workers.shadow.runner import run_shadow_comparison

# Reuse the canonical shadow test harness (fixtures import into this module).
from test_shadow_comparison import (  # noqa: F401
    FakeProvider,
    NOW_UTC,
    ShadowStore,
    default_configs,
    frame_to_payload,
    store,
)


MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"
MIGRATION_013 = MIGRATIONS_DIR / "013_wyckoff_v2_shadow_arms.sql"

_CATEGORY_RE = re.compile(
    r"^(same_(enter|watch|avoid)"
    r"|control_(enter|watch|avoid)_candidate_(enter|watch|avoid))$"
)


def _run(coro):
    return asyncio.run(coro)


def _long_daily_payload(bars: int = 600, *, price: float = 50.0) -> Dict[str, Any]:
    dates = pd.bdate_range(end="2026-07-17", periods=bars)
    historical = []
    for i, d in enumerate(dates):
        px = price + (i % 9) * 0.3
        historical.append({
            "date": d.date().isoformat(),
            "open": px,
            "high": px + 0.6,
            "low": px - 0.6,
            "close": px + 0.15,
            "volume": 900_000 + (i % 4) * 25_000,
        })
    return {"historical": historical}


def _wyckoff_run(store_obj, payloads, symbols, run_id=None):
    provider = FakeProvider(payloads)
    summary = _run(run_shadow_comparison(
        provider, symbols,
        run_id=run_id,
        now_utc=NOW_UTC,
        experiment=WYCKOFF_V2_VS_BASELINE,
    ))
    return provider, summary


class TestExperimentRegistry:
    def test_registry_is_closed_and_deterministic(self):
        assert set(EXPERIMENTS) == {
            "sma150_v2_vs_v3", "wyckoff_v2_vs_baseline",
        }
        assert DEFAULT_EXPERIMENT is SMA150_V2_VS_V3
        assert get_experiment("wyckoff_v2_vs_baseline") is WYCKOFF_V2_VS_BASELINE
        with pytest.raises(UnknownShadowExperimentError):
            get_experiment("no_such_experiment")

    def test_default_experiment_identities_unchanged(self):
        assert SMA150_V2_VS_V3.experiment_code == EXPERIMENT_CODE
        assert SMA150_V2_VS_V3.experiment_version == EXPERIMENT_VERSION
        assert SMA150_V2_VS_V3.control_arm_code == "control_v2"
        assert SMA150_V2_VS_V3.candidate_arm_code == "candidate_v3"
        assert SMA150_V2_VS_V3.control_pattern_code == "sma150_bounce"
        assert SMA150_V2_VS_V3.candidate_pattern_code == "sma150_bounce_v3"
        assert SMA150_V2_VS_V3.control_data_meta_extras is None
        assert SMA150_V2_VS_V3.candidate_data_meta_extras is None

    def test_wyckoff_experiment_identity(self):
        exp = WYCKOFF_V2_VS_BASELINE
        assert exp.experiment_code == "wyckoff_v2_vs_baseline"
        assert exp.experiment_version == "wyckoff_v2_shadow.v1"
        assert exp.control_pattern_code == "sma150_bounce"
        assert exp.candidate_pattern_code == "wyckoff_mtf_v2"
        assert exp.control_arm_code == "control_baseline"
        assert exp.candidate_arm_code == "candidate_wyckoff_v2"

    def test_candidate_resolution_is_unique_and_explicit(self):
        candidates = [e.candidate_pattern_code for e in EXPERIMENTS.values()]
        assert len(candidates) == len(set(candidates))
        assert experiment_for_candidate("wyckoff_mtf_v2") is WYCKOFF_V2_VS_BASELINE
        assert experiment_for_candidate("sma150_bounce_v3") is SMA150_V2_VS_V3
        # No implicit pairing for production strategies.
        with pytest.raises(UnknownShadowExperimentError):
            experiment_for_candidate("sma150_bounce")
        with pytest.raises(UnknownShadowExperimentError):
            experiment_for_candidate("wyckoff_mtf")

    def test_wyckoff_history_depth_uses_strategy_derivation(self):
        from app.workers.strategies.registry import get_strategy
        from app.workers.strategies.wyckoff_v2.readiness import (
            derive_history_requirement,
        )

        cfg = get_strategy("wyckoff_mtf_v2").default_config()
        assert required_history_bars_wyckoff_v2(cfg) == (
            derive_history_requirement(cfg)["desired_history_bars"]
        )
        v2cfg = get_strategy("sma150_bounce").default_config()
        depth = shared_required_history_bars(
            v2cfg, cfg,
            control_fn=required_history_bars_v2,
            candidate_fn=required_history_bars_wyckoff_v2,
        )
        assert depth == min(
            max(required_history_bars_v2(v2cfg),
                required_history_bars_wyckoff_v2(cfg)),
            FRAME_HARD_CAP_BARS,
        )


class TestCategoryLabels:
    def test_legacy_labels_preserved(self):
        assert category_label_for_arm("control_v2") == "v2"
        assert category_label_for_arm("candidate_v3") == "v3"
        assert disagreement_category("ENTER", "AVOID") == "v2_enter_v3_avoid"
        assert disagreement_category("WATCH", "WATCH") == "same_watch"

    def test_generic_labels_for_new_arms(self):
        assert category_label_for_arm("control_baseline") == "control"
        assert category_label_for_arm("candidate_wyckoff_v2") == "candidate"
        assert disagreement_category(
            "ENTER", "AVOID",
            control_label="control", candidate_label="candidate",
        ) == "control_enter_candidate_avoid"
        assert sorted(GENERIC_DISAGREEMENT_CATEGORIES) == sorted({
            f"control_{c}_candidate_{x}"
            for c in ("enter", "watch", "avoid")
            for x in ("enter", "watch", "avoid")
            if c != x
        })


class TestWyckoffShadowRun:
    def test_uses_canonical_runner_and_preserves_identities(
        self, store, default_configs
    ):
        payloads = {"LONGX": _long_daily_payload()}
        provider, summary = _wyckoff_run(store, payloads, ["LONGX"])

        assert summary["status"] == "completed"
        assert summary["telemetry"]["experiment_code"] == "wyckoff_v2_vs_baseline"
        assert summary["telemetry"]["experiment_version"] == "wyckoff_v2_shadow.v1"
        assert summary["telemetry"]["pair_count"] == 1

        stored = list(store.pairs.values())[0]
        pair = stored["pair"]
        assert pair["experiment_code"] == "wyckoff_v2_vs_baseline"
        assert pair["experiment_version"] == "wyckoff_v2_shadow.v1"

        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        assert set(by_arm) == {"control_baseline", "candidate_wyckoff_v2"}
        control = by_arm["control_baseline"]
        candidate = by_arm["candidate_wyckoff_v2"]
        assert control["strategy_code"] == "sma150_bounce"
        assert control["strategy_version"] == "sma150.v2"
        assert candidate["strategy_code"] == "wyckoff_mtf_v2"
        assert candidate["strategy_version"] == "wyckoff_mtf.v2"
        assert candidate["decision_policy_version"] == "wyckoff_mtf.policy.v1"
        assert candidate["verdict"] in ("ENTER", "WATCH", "AVOID")

    def test_requested_depth_covers_wyckoff_requirement(
        self, store, default_configs
    ):
        payloads = {"LONGX": _long_daily_payload()}
        provider, summary = _wyckoff_run(store, payloads, ["LONGX"])
        from app.workers.strategies.registry import get_strategy

        v2cfg = get_strategy("sma150_bounce").default_config()
        wcfg = get_strategy("wyckoff_mtf_v2").default_config()
        expected_depth = shared_required_history_bars(
            v2cfg, wcfg,
            control_fn=required_history_bars_v2,
            candidate_fn=required_history_bars_wyckoff_v2,
        )
        assert provider.requested_timeseries == [
            expected_depth + FRAME_FETCH_MARGIN_BARS
        ]
        assert summary["telemetry"]["requested_history_bars"] == expected_depth

    def test_idempotent_repeat_creates_no_duplicate_state(
        self, store, default_configs
    ):
        payloads = {"LONGX": _long_daily_payload()}
        _, first = _wyckoff_run(store, payloads, ["LONGX"])
        assert first["telemetry"]["pairs_created"] == 1
        assert len(store.pairs) == 1

        _, second = _wyckoff_run(store, payloads, ["LONGX"])
        assert second["telemetry"]["pairs_created"] == 0
        assert second["telemetry"]["pairs_deduplicated"] == 1
        assert len(store.pairs) == 1
        # The occurrence link (not a duplicate pair) records the repeat.
        assert len(store.links) == 2

    def test_typed_serialization_is_deterministic(self, store, default_configs):
        payloads = {"LONGX": _long_daily_payload()}
        _wyckoff_run(store, payloads, ["LONGX"])
        stored = list(store.pairs.values())[0]
        fingerprints_first = [
            ev["evaluation_fingerprint"] for ev in stored["evaluations"]
        ]
        details_first = canonical_json(
            [ev["details_snapshot"] for ev in stored["evaluations"]]
        )

        provider = FakeProvider(payloads)
        _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        # Identical inputs deduplicate onto the SAME frozen pair; re-read it.
        stored2 = list(store.pairs.values())[0]
        fingerprints_second = [
            ev["evaluation_fingerprint"] for ev in stored2["evaluations"]
        ]
        details_second = canonical_json(
            [ev["details_snapshot"] for ev in stored2["evaluations"]]
        )
        assert fingerprints_first == fingerprints_second
        assert details_first == details_second

    def test_evidence_stays_bounded(self, store, default_configs):
        payloads = {"LONGX": _long_daily_payload()}
        _wyckoff_run(store, payloads, ["LONGX"])
        stored = list(store.pairs.values())[0]
        for ev in stored["evaluations"]:
            size = len(
                canonical_json(ev["details_snapshot"]).encode("utf-8")
            )
            assert size <= MAX_DETAILS_SNAPSHOT_BYTES + 4096

    def test_rollout_defaults_prevent_candidate_enter(
        self, store, default_configs
    ):
        payloads = {
            "LONGX": _long_daily_payload(),
            "SHRTX": _long_daily_payload(bars=250, price=20.0),
        }
        _, summary = _wyckoff_run(store, payloads, ["LONGX", "SHRTX"])
        assert summary["telemetry"]["candidate_enter_count"] == 0
        for stored in store.pairs.values():
            by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
            candidate = by_arm["candidate_wyckoff_v2"]
            assert candidate["verdict"] != "ENTER"
            policy = candidate["details_snapshot"]["policy"]
            assert policy["allow_enter"] is False
            # Rollout-blocked state is recorded explicitly, never as ENTER.
            assert isinstance(
                policy["enter_eligible_without_rollout_gate"], bool
            )
            if policy["enter_eligible_without_rollout_gate"]:
                assert "enter_disabled_shadow_only" in policy["waiting_reasons"]

    def test_missing_trigger_stays_missing_nothing_fabricated(
        self, store, default_configs
    ):
        payloads = {"LONGX": _long_daily_payload()}
        _wyckoff_run(store, payloads, ["LONGX"])
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        details = by_arm["candidate_wyckoff_v2"]["details_snapshot"]
        # No df_4h was supplied and enable_4h_trigger=false: any trigger
        # record must not carry a confirmed trigger price.
        trigger = details.get("four_hour_trigger")
        if trigger is not None:
            assert trigger.get("trigger_price") is None
            assert trigger.get("state") != "confirmed"
        # wyckoff v2 never fabricates stop/target.
        assert details["thresholds_used"]["enable_4h_trigger"] is False
        assert details["thresholds_used"]["allow_enter"] is False

    def test_category_vocabulary_is_generic(self, store, default_configs):
        payloads = {"LONGX": _long_daily_payload()}
        _, summary = _wyckoff_run(store, payloads, ["LONGX"])
        for category in summary["telemetry"]["verdict_categories"]:
            assert _CATEGORY_RE.match(category), category
        for pair in summary["pairs"]:
            assert _CATEGORY_RE.match(pair["disagreement_category"])

    def test_default_experiment_run_is_unchanged(self, store, default_configs):
        from sma150_v3_frames import build_uptrend_frame

        provider = FakeProvider({
            "ENTRX": frame_to_payload(build_uptrend_frame(trigger=True)),
        })
        summary = _run(run_shadow_comparison(
            provider, ["ENTRX"], now_utc=NOW_UTC,
        ))
        assert summary["telemetry"]["experiment_code"] == "sma150_v2_vs_v3"
        stored = list(store.pairs.values())[0]
        assert {ev["arm_code"] for ev in stored["evaluations"]} == {
            "control_v2", "candidate_v3",
        }
        for category in summary["telemetry"]["verdict_categories"]:
            assert category.startswith(("same_", "v2_"))

    def test_shadow_run_never_calls_production_persistence(
        self, store, default_configs, monkeypatch
    ):
        import app.workers.persistence as persistence_mod
        import app.workers.strategies.decision_card as card_mod

        def _bomb(*args, **kwargs):
            raise AssertionError("production persistence invoked by shadow run")

        monkeypatch.setattr(persistence_mod, "save_signal", _bomb)
        monkeypatch.setattr(card_mod, "build_decision_card", _bomb)
        payloads = {"LONGX": _long_daily_payload()}
        _, summary = _wyckoff_run(store, payloads, ["LONGX"])
        assert summary["status"] == "completed"


class TestTypedPersistenceEndToEnd:
    def test_wyckoff_pair_persists_with_exact_driver_types(self, monkeypatch):
        from datetime import date as _date

        from test_shadow_typed_persistence import (
            StrictTypedConn,
            frame_to_payload as typed_frame_to_payload,  # noqa: F401
        )
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

        provider = FakeProvider({"LONGX": _long_daily_payload()})
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["rejected_counts"] == {}
        assert db.forbidden == []

        args = db.pair_inserts[0]
        assert args[2] == "wyckoff_v2_vs_baseline"
        assert args[3] == "wyckoff_v2_shadow.v1"
        for idx in (7, 12, 13):
            assert type(args[idx]) is _date, idx
        assert args[8].tzinfo == timezone.utc
        assert isinstance(args[0], uuid.UUID)
        arm_codes = {row[2] for row in db.eval_inserts}
        assert arm_codes == {"control_baseline", "candidate_wyckoff_v2"}


class TestMigration013:
    def test_exact_file_and_no_014(self):
        assert MIGRATION_013.exists()
        assert [p.name for p in sorted(MIGRATIONS_DIR.glob("013_*"))] == [
            "013_wyckoff_v2_shadow_arms.sql"
        ]
        assert not list(MIGRATIONS_DIR.glob("014_*"))

    def test_arm_codes_in_sync_with_registry(self):
        sql = MIGRATION_013.read_text(encoding="utf-8")
        quoted = set(re.findall(r"'([a-z0-9_]+)'", sql))
        assert set(KNOWN_ARM_CODES) <= quoted
        # Every arm code the CHECK allows is declared by the registry.
        check_block = sql[sql.index("CHECK (arm_code IN ("):]
        allowed = set(re.findall(r"'([a-z0-9_]+)'", check_block))
        assert allowed == set(KNOWN_ARM_CODES)

    def test_idempotent_and_additive_only(self):
        raw = MIGRATION_013.read_text(encoding="utf-8")
        # Judge executable statements only (comments explain, never execute).
        sql = "\n".join(
            line for line in raw.splitlines()
            if not line.lstrip().startswith("--")
        )
        upper = sql.upper()
        assert "DROP CONSTRAINT IF EXISTS" in sql
        assert upper.index("DROP CONSTRAINT IF EXISTS") < upper.index(
            "ADD CONSTRAINT"
        )
        # Constraint-only migration: no tables, no data, no other objects.
        assert "CREATE TABLE" not in upper
        assert "INSERT" not in upper
        assert "UPDATE" not in upper
        assert "DELETE" not in upper
        assert "DROP TABLE" not in upper
        assert upper.count("ALTER TABLE") == 2
        assert "strategy_shadow_evaluations" in sql
        # It never touches enablement or configuration.
        assert "is_enabled" not in sql
        assert "pattern_configs" not in sql

    def test_earlier_migrations_unmodified(self):
        import subprocess

        result = subprocess.run(
            [
                "git", "diff", "--",
                "app/db/migrations/010_sma150_shadow_evaluations.sql",
                "app/db/migrations/011_shadow_pair_outcomes.sql",
                "app/db/migrations/012_wyckoff_mtf_v2.sql",
            ],
            cwd=str(MIGRATIONS_DIR.parents[2]),
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.stdout.strip() == ""
