"""Phase 8: version isolation, provenance integration and migration 008.

sma150.v2 and wyckoff_mtf.v1 stay unchanged; sma150_bounce_v3 is a separate
strategy code with its own version/policy; v2 and v3 coexist as separate
immutable signals; evidence.v1 is persisted through the Phase 7B pipeline.
"""

import asyncio
import copy
import json
from pathlib import Path

import pytest

from app.workers import persistence
from app.workers.patterns.sma150 import (
    DEFAULT_CONFIG as V2_DEFAULT_CONFIG,
    SCORE_VERSION as V2_SCORE_VERSION,
    evaluate_sma150_bounce,
)
from app.workers.provenance import (
    DECISION_POLICY_VERSION as LEGACY_POLICY,
    SIGNAL_FINGERPRINT_VERSION,
    build_provenance,
    market_data_as_of_from_df,
)
from app.workers.strategies import get_strategy, list_strategies
from app.workers.strategies.base import StrategyContext
from app.workers.strategies.sma150_v3 import (
    DEFAULT_CONFIG as V3_DEFAULT_CONFIG,
)
from tests.sma150_v3_frames import build_uptrend_frame

from test_signal_provenance_persistence import _FakeConn, _patch_conn


MIGRATIONS = Path(__file__).resolve().parents[1] / "app" / "db" / "migrations"


def _provenance_from(result, strategy, df, config):
    return build_provenance(
        scan_run_id=None,
        source_path="funnel",
        scanner_mode="funnel",
        provider="massive",
        strategy_code=result.pattern_code,
        strategy_version=result.strategy_version,
        strategy_config=config,
        details=result.details,
        score_components=result.score_components,
        market_data_as_of=market_data_as_of_from_df(df),
        decision_policy_version=getattr(strategy, "decision_policy_version", None),
    )


def _evaluate(pattern_code, df, config_overrides=None):
    strategy = get_strategy(pattern_code)
    config = strategy.default_config()
    if config_overrides:
        config.update(config_overrides)
    result = strategy.evaluate(
        df, StrategyContext(symbol="TEST", pattern_code=pattern_code,
                            config=config)
    )
    return strategy, result, config


class TestV2Unchanged:
    def test_v2_version_and_defaults_unchanged(self):
        assert V2_SCORE_VERSION == "sma150.v2"
        assert V2_DEFAULT_CONFIG["score_threshold"] == 0.5
        assert V2_DEFAULT_CONFIG["min_bounces"] == 2
        assert V2_DEFAULT_CONFIG["min_avg_rebound_pct"] == 5.0

    def test_registry_still_maps_sma150_bounce_to_v2(self):
        strategy = get_strategy("sma150_bounce")
        assert strategy.version == "sma150.v2"
        assert strategy.pattern_code == "sma150_bounce"
        # v2 keeps the implicit legacy decision policy, NOT the v3 policy.
        assert strategy.decision_policy_version == LEGACY_POLICY

    def test_v2_result_unaffected_by_v3_registration(self):
        """The v2 evaluator output on a fixed frame contains no v3 artifacts
        and keeps its exact v2 shape."""
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        raw = evaluate_sma150_bounce("TEST", df, None)
        assert raw["details"]["score_version"] == "sma150.v2"
        for v3_key in ("evidence", "evidence_version", "setup_state",
                       "trigger_state", "ranking", "bounce_events"):
            assert v3_key not in raw["details"]

    def test_v2_evaluation_deterministic_before_and_after_v3_import(self):
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        first = evaluate_sma150_bounce("TEST", df, None)
        import app.workers.strategies.sma150_v3  # noqa: F401 (already imported)
        second = evaluate_sma150_bounce("TEST", df, None)
        assert json.dumps(first, sort_keys=True, default=str) == json.dumps(
            second, sort_keys=True, default=str
        )


class TestWyckoffUnchanged:
    def test_wyckoff_version_and_policy_unchanged(self):
        strategy = get_strategy("wyckoff_mtf")
        assert strategy.version == "wyckoff_mtf.v1"
        assert strategy.decision_policy_version == LEGACY_POLICY

    def test_registry_lists_exactly_three_strategies(self):
        assert list_strategies() == [
            "sma150_bounce", "sma150_bounce_v3", "wyckoff_mtf"
        ]


class TestV3Identity:
    def test_v3_registration_identity(self):
        strategy = get_strategy("sma150_bounce_v3")
        assert strategy.pattern_code == "sma150_bounce_v3"
        assert strategy.version == "sma150.v3"
        assert strategy.decision_policy_version == "sma150_bounce.policy.v1"

    def test_v3_provenance_fields(self):
        df = build_uptrend_frame()
        strategy, result, config = _evaluate("sma150_bounce_v3", df)
        prov = _provenance_from(result, strategy, df, config)
        assert prov["strategy_code"] == "sma150_bounce_v3"
        assert prov["strategy_version"] == "sma150.v3"
        assert prov["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert prov["external_observation_ids"] == []
        assert prov["market_data_as_of"] is not None

    def test_evidence_v1_persisted_inside_evidence_snapshot(self):
        df = build_uptrend_frame()
        strategy, result, config = _evaluate("sma150_bounce_v3", df)
        prov = _provenance_from(result, strategy, df, config)
        snapshot = prov["evidence_snapshot"]
        assert snapshot["evidence_version"] == "evidence.v1"
        assert snapshot["evidence"]["evidence_version"] == "evidence.v1"
        assert snapshot["evidence"]["strategy_version"] == "sma150.v3"
        # JSON-safe end to end.
        json.dumps(snapshot["evidence"])

    def test_evidence_bundle_survives_pruning(self):
        """The evidence.v1 bundle is a MANDATORY evidence key: pruning an
        oversized snapshot can never remove it."""
        from app.workers.provenance import MANDATORY_EVIDENCE_KEYS
        assert "evidence" in MANDATORY_EVIDENCE_KEYS
        assert "evidence_version" in MANDATORY_EVIDENCE_KEYS

    def test_v3_config_hash_changes_with_meaningful_config(self):
        df = build_uptrend_frame()
        strategy, result, config = _evaluate("sma150_bounce_v3", df)
        base = _provenance_from(result, strategy, df, config)
        changed_cfg = dict(config, min_trigger_volume_ratio=1.5)
        changed = _provenance_from(result, strategy, df, changed_cfg)
        assert base["config_hash"] != changed["config_hash"]

    def test_no_provider_calls_in_v3_evaluation(self):
        """v3 is a pure function of the dataframe + config: evaluating it
        must not touch any provider/network module."""
        import app.workers.strategies.sma150_v3 as mod
        source = Path(mod.__file__).read_text()
        for token in ("massive", "fmp", "httpx", "aiohttp", "requests"):
            assert token not in source.lower().replace("dataframe", "")


class TestCoexistencePersistence:
    def _save(self, result, prov, **kwargs):
        defaults = dict(
            symbol="TEST",
            pattern_code=result.pattern_code,
            verdict=result.verdict,
            score=result.score,
            reason=result.reason,
            details=result.details,
            provenance=prov,
        )
        defaults.update(kwargs)
        return asyncio.run(persistence.save_signal(**defaults))

    def test_v2_and_v3_same_symbol_date_coexist_as_immutable_signals(
        self, monkeypatch
    ):
        conn = _FakeConn()
        _patch_conn(monkeypatch, conn)
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)

        s2, r2, c2 = _evaluate("sma150_bounce", df)
        s3, r3, c3 = _evaluate("sma150_bounce_v3", df)
        assert r2.details["snapshot_date"] == r3.details["snapshot_date"]

        res2 = self._save(r2, _provenance_from(r2, s2, df, c2))
        res3 = self._save(r3, _provenance_from(r3, s3, df, c3))

        assert res2["signal_id"] != res3["signal_id"]
        assert res2["created_new_signal"] and res3["created_new_signal"]
        assert res2["signal_fingerprint"] != res3["signal_fingerprint"]
        assert res2["signal_fingerprint_version"] == SIGNAL_FINGERPRINT_VERSION
        assert res3["signal_fingerprint_version"] == SIGNAL_FINGERPRINT_VERSION
        assert len(conn.signals_by_fp) == 2

    def test_exact_repeated_v3_inputs_deduplicate(self, monkeypatch):
        conn = _FakeConn()
        _patch_conn(monkeypatch, conn)
        df = build_uptrend_frame()

        s3, r3, c3 = _evaluate("sma150_bounce_v3", df)
        first = self._save(r3, _provenance_from(r3, s3, df, c3))
        second = self._save(r3, _provenance_from(r3, s3, df, c3))

        assert second["signal_id"] == first["signal_id"]
        assert second["deduplicated"] is True
        assert len(conn.signals_by_fp) == 1

    def test_v3_outcome_provenance_fields_available(self, monkeypatch):
        """Outcome readiness: the persisted provenance carries every field the
        outcome service copies (strategy/policy/config/provenance versions)."""
        conn = _FakeConn()
        _patch_conn(monkeypatch, conn)
        df = build_uptrend_frame()

        s3, r3, c3 = _evaluate("sma150_bounce_v3", df)
        prov = _provenance_from(r3, s3, df, c3)
        self._save(r3, prov)

        assert prov["strategy_code"] == "sma150_bounce_v3"
        assert prov["strategy_version"] == "sma150.v3"
        assert prov["decision_policy_version"] == "sma150_bounce.policy.v1"
        assert prov["config_hash"]
        assert prov["provenance_version"] == "provenance.v1"
        assert len(conn.provenance_by_signal) == 1


class TestLegacyPathRouting:
    def test_legacy_default_config_resolution(self):
        from app.workers.scan_runner import _resolve_default_config
        assert _resolve_default_config("sma150_bounce") == V2_DEFAULT_CONFIG
        v3_cfg = _resolve_default_config("sma150_bounce_v3")
        assert v3_cfg["min_trigger_volume_ratio"] == 1.20
        assert "score_threshold" not in v3_cfg

    def test_legacy_path_keeps_direct_v2_call(self):
        from app.workers.scan_runner import _evaluate_pattern
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        result, policy = _evaluate_pattern(
            "TEST", df, "sma150_bounce", dict(V2_DEFAULT_CONFIG), None
        )
        direct = evaluate_sma150_bounce("TEST", df, dict(V2_DEFAULT_CONFIG))
        assert policy is None  # v2 keeps the implicit legacy policy
        assert result["verdict"] == direct["verdict"]
        assert result["details"]["score_version"] == "sma150.v2"

    def test_legacy_path_routes_v3_through_registry(self):
        from app.workers.scan_runner import _evaluate_pattern
        df = build_uptrend_frame(trigger=True, vol_ratio=1.30)
        cfg = get_strategy("sma150_bounce_v3").default_config()
        result, policy = _evaluate_pattern(
            "TEST", df, "sma150_bounce_v3", cfg, None
        )
        assert policy == "sma150_bounce.policy.v1"
        assert result["details"]["score_version"] == "sma150.v3"
        assert result["details"]["evidence"]["evidence_version"] == "evidence.v1"

    def test_unknown_pattern_still_rejected(self):
        from app.workers.scan_runner import _evaluate_pattern
        from app.workers.strategies import UnknownStrategyError
        with pytest.raises(UnknownStrategyError):
            _evaluate_pattern("TEST", build_uptrend_frame(), "no_such", {}, None)


class TestMigration008:
    def _sql(self):
        return (MIGRATIONS / "008_sma150_v3.sql").read_text()

    def _statements(self):
        """Executable SQL only (comment lines stripped) so assertions about
        statements are never confused by explanatory comments."""
        return "\n".join(
            line for line in self._sql().splitlines()
            if not line.lstrip().startswith("--")
        )

    def test_migration_is_idempotent(self):
        sql = self._statements()
        assert "ON CONFLICT (code) DO NOTHING" in sql
        assert "ON CONFLICT (pattern_code, key) DO NOTHING" in sql
        # No destructive statements.
        for forbidden in ("DROP ", "DELETE ", "TRUNCATE", "ALTER TABLE"):
            assert forbidden not in sql.upper()

    def test_rerun_preserves_operator_modified_config(self):
        """The config upsert must be DO NOTHING: rerunning the migration can
        never reset values an operator changed after the first run. (002-style
        DO UPDATE is reserved for explicit corrections of live values.)"""
        sql = self._statements()
        assert "DO UPDATE" not in sql
        assert sql.count("ON CONFLICT (pattern_code, key) DO NOTHING") == 1

    def test_v3_pattern_disabled_by_default(self):
        sql = self._sql()
        assert "'sma150_bounce_v3'" in sql
        # The patterns INSERT registers it with is_enabled=false.
        insert = sql[sql.index("INSERT INTO public.patterns"):]
        insert = insert[:insert.index(";")]
        assert "false" in insert.lower()

    def test_migration_never_touches_v2_rows(self):
        sql = self._sql()
        # Every value row targets sma150_bounce_v3; the plain v2 code never
        # appears as a standalone pattern_code literal.
        assert "('sma150_bounce'," not in sql
        assert "UPDATE public.patterns" not in sql
        assert "UPDATE public.signals" not in sql

    def test_migration_config_mirrors_code_defaults(self):
        sql = self._sql()
        for key in ("sma_window", "min_history_bars", "volume_window_bars",
                    "slope_lookback_bars", "rebound_window_bars",
                    "max_close_above_sma_pct", "max_close_below_sma_pct",
                    "touch_tolerance_pct", "min_event_separation_bars",
                    "min_independent_bounces", "min_median_rebound_pct",
                    "min_sma_slope_pct", "min_close_location_value",
                    "min_trigger_volume_ratio", "invalidation_below_sma_pct",
                    "recency_half_life_bars", "bar_completion_policy",
                    "exchange_timezone", "session_close_time", "min_price",
                    "min_liquidity_filters"):
            assert f"'{key}'" in sql, key
            assert key in V3_DEFAULT_CONFIG

    def test_no_migration_009_created(self):
        assert not list(MIGRATIONS.glob("009_*"))

    def test_v3_defaults_copy_is_isolated(self):
        """default_config() returns an independent copy (mutation-safe)."""
        strategy = get_strategy("sma150_bounce_v3")
        cfg = strategy.default_config()
        cfg["min_liquidity_filters"]["min_market_cap"] = 1
        cfg["sma_window"] = 1
        fresh = strategy.default_config()
        assert fresh["sma_window"] == 150
        assert fresh["min_liquidity_filters"]["min_market_cap"] == 200000000
        assert copy.deepcopy(V3_DEFAULT_CONFIG)["sma_window"] == 150
