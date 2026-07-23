"""Phase 9E3: multi-timeframe shadow evaluation (daily + canonical 4H)."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from app.workers.shadow import runner as shadow_runner
from app.workers.shadow.experiments import (
    SMA150_V2_VS_V3,
    WYCKOFF_V2_VS_BASELINE,
    ShadowExperiment,
)
from app.workers.shadow.frames_4h import FOUR_HOUR_FRAME_CONTRACT_VERSION
from app.workers.shadow.runner import run_shadow_comparison
from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyDecision,
    StrategyResult,
)
from app.workers.strategies.registry import _REGISTRY, register_strategy
from app.workers.strategies.wyckoff_v2.constants import (
    default_config as v2_default_config,
)
from app.workers.strategies.wyckoff_v2.trigger_4h import analyze_4h_trigger

from test_shadow_comparison import (  # noqa: F401
    NOW_UTC,
    default_configs,
    store,
)
from test_wyckoff_v2_9d_shadow import _long_daily_payload


LAST_DAILY = date(2026, 7, 17)
LAST_4H_START = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


def _intraday_bars(
    n: int = 12,
    *,
    last_start: datetime = LAST_4H_START,
    breakout_close: Optional[float] = 55.0,
) -> List[Dict[str, Any]]:
    """n 4H bars ending on/before the daily as-of session; the last close
    breaks the prior local high when breakout_close is set."""
    bars = []
    for i in range(n - 1, -1, -1):
        start = last_start - timedelta(hours=4 * i)
        bars.append({
            "start_utc": start,
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
            "volume": 10_000.0,
        })
    if breakout_close is not None:
        bars[-1] = {
            **bars[-1],
            "high": breakout_close + 0.5,
            "close": breakout_close,
        }
    return bars


class MtfFakeProvider:
    """Daily + bounded intraday fake (never a live provider)."""

    name = "fake_provider"
    supports_intraday_history = True

    def __init__(self, daily_payloads, intraday_bars=None,
                 intraday_error: Optional[Exception] = None):
        self.daily_payloads = daily_payloads
        self.intraday_bars = intraday_bars if intraday_bars is not None else {}
        self.intraday_error = intraday_error
        self.daily_calls: List[str] = []
        self.intraday_calls: List[Dict[str, Any]] = []

    async def get_daily_history(self, symbol, timeseries=400):
        self.daily_calls.append(symbol)
        return self.daily_payloads[symbol]

    async def get_intraday_history(self, symbol, *, multiplier, timespan,
                                   start=None, end=None, limit=None):
        self.intraday_calls.append({
            "symbol": symbol, "multiplier": multiplier, "timespan": timespan,
            "start": start, "end": end,
        })
        if self.intraday_error is not None:
            raise self.intraday_error
        return {
            "symbol": symbol,
            "provider": self.name,
            "multiplier": multiplier,
            "timespan": timespan,
            "requested_start": str(start),
            "requested_end": str(end),
            "bars": list(self.intraday_bars.get(symbol, [])),
            "skipped_malformed": 0,
            "dropped_exact_duplicates": 0,
        }


class EchoTriggerStrategy(Strategy):
    """Test-only registered candidate: proves the REAL 4H frame reaches the
    strategy through the canonical runner and that a confirmed trigger is
    measurable with a REAL price. Never registered by production code."""

    pattern_code = "shadow_mtf_echo_test"
    version = "echo.v1"
    decision_policy_version = "echo.policy.v1"
    min_daily_bars = 10

    def default_config(self) -> Dict[str, Any]:
        return dict(v2_default_config())

    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        meta = dict(context.data_meta or {})
        df_4h = meta.get("df_4h")
        trigger = analyze_4h_trigger(
            df_4h,
            side="LONG",
            evaluation_time_utc=meta.get("evaluation_time_utc"),
            daily_frame=df,
            daily_market_data_as_of=str(df["date"].iloc[-1].date()),
            config=context.config,
        )
        return StrategyResult(
            decision=StrategyDecision.WATCH,
            symbol=context.symbol,
            pattern_code=self.pattern_code,
            details={
                "received_df_4h_rows": (
                    None if df_4h is None else int(len(df_4h))
                ),
                "data_meta_keys": sorted(meta.keys()),
                "four_hour_trigger": trigger.to_dict(),
            },
            strategy_version=self.version,
        )


@pytest.fixture
def echo_experiment():
    """Temporary registered echo candidate + declared test experiment."""
    strategy = EchoTriggerStrategy()
    register_strategy(strategy)
    experiment = ShadowExperiment(
        experiment_code="wyckoff_v2_vs_baseline",
        experiment_version="wyckoff_v2_shadow.v2",
        control_pattern_code="sma150_bounce",
        candidate_pattern_code="shadow_mtf_echo_test",
        control_arm_code="control_baseline",
        candidate_arm_code="candidate_wyckoff_v2",
        control_category_label="control",
        candidate_category_label="candidate",
        control_history_bars=SMA150_V2_VS_V3.control_history_bars,
        candidate_history_bars=lambda cfg: 300,
        requires_four_hour_frame=True,
        candidate_config_overrides={"enable_4h_trigger": True},
    )
    try:
        yield experiment
    finally:
        _REGISTRY.pop("shadow_mtf_echo_test", None)


class TestBothFramesSupplied:
    def test_real_confirmed_trigger_is_measurable_through_the_runner(
        self, store, default_configs, echo_experiment
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=55.0)},
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC, experiment=echo_experiment,
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["four_hour_frames_built"] == 1
        assert provider.intraday_calls[0]["multiplier"] == 4
        assert provider.intraday_calls[0]["timespan"] == "hour"

        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        details = by_arm["candidate_wyckoff_v2"]["details_snapshot"]
        # The strategy received the REAL canonical completed 4H frame.
        assert details["received_df_4h_rows"] == 12
        trigger = details["four_hour_trigger"]
        assert trigger["state"] == "confirmed"
        assert trigger["triggered"] is True
        # The trigger price is the REAL last completed 4H close — never
        # fabricated, and stop/target do not exist anywhere in the record.
        assert trigger["trigger_price"] == 55.0
        assert trigger["current_close"] == 55.0
        assert "stop_price" not in trigger and "target_price" not in trigger
        # Telemetry separates the trigger states explicitly.
        assert summary["telemetry"]["candidate_trigger_states"] == {
            "confirmed": 1
        }
        assert summary["telemetry"]["candidate_real_trigger_price_count"] == 1
        # Frozen 4H frame metadata rides with the evaluation.
        meta = details["_four_hour_frame_meta"]
        assert meta["state"] == "built"
        assert meta["contract_version"] == FOUR_HOUR_FRAME_CONTRACT_VERSION
        assert meta["frame_hash"]
        assert meta["bar_count"] == 12

    def test_missing_trigger_remains_missing(
        self, store, default_configs, echo_experiment
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=None)},
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC, experiment=echo_experiment,
        ))
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        trigger = by_arm["candidate_wyckoff_v2"]["details_snapshot"][
            "four_hour_trigger"
        ]
        assert trigger["state"] == "missing"
        assert trigger["trigger_price"] is None
        assert summary["telemetry"]["candidate_trigger_states"] == {
            "missing": 1
        }
        assert summary["telemetry"]["candidate_real_trigger_price_count"] == 0

    def test_insufficient_4h_history_is_separate_from_absent(
        self, store, default_configs, echo_experiment
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={
                "LONGX": _intraday_bars(n=3, breakout_close=None)
            },
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC, experiment=echo_experiment,
        ))
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        trigger = by_arm["candidate_wyckoff_v2"]["details_snapshot"][
            "four_hour_trigger"
        ]
        assert trigger["state"] == "unknown"
        assert "insufficient_4h_history" in trigger["reason_codes"]
        assert summary["telemetry"]["four_hour_frames_built"] == 1

    def test_provider_fetch_error_is_typed_and_never_aborts_the_pair(
        self, store, default_configs, echo_experiment
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_error=RuntimeError("intraday boom"),
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC, experiment=echo_experiment,
        ))
        assert summary["telemetry"]["pair_count"] == 1
        assert summary["telemetry"]["four_hour_fetch_error"] == 1
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        details = by_arm["candidate_wyckoff_v2"]["details_snapshot"]
        assert details["received_df_4h_rows"] is None
        assert details["_four_hour_frame_meta"]["state"] == "fetch_error"
        assert details["_four_hour_frame_meta"]["reason_code"] == (
            "provider_RuntimeError"
        )
        # "intraday boom" text never leaks into frozen metadata.
        assert "boom" not in str(details["_four_hour_frame_meta"])

    def test_frame_rejection_is_typed(
        self, store, default_configs, echo_experiment
    ):
        dup = _intraday_bars(n=2, breakout_close=None)
        dup.append(dict(dup[-1], close=50.9))   # same start, different values
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": dup},
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC, experiment=echo_experiment,
        ))
        assert summary["telemetry"]["four_hour_frame_rejected"] == 1
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        meta = by_arm["candidate_wyckoff_v2"]["details_snapshot"][
            "_four_hour_frame_meta"
        ]
        assert meta["state"] == "frame_rejected"
        assert meta["reason_code"] == "duplicate_bar_start"


class TestWyckoffCandidateMtf:
    def test_wyckoff_candidate_receives_4h_and_stays_rollout_blocked(
        self, store, default_configs
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=55.0)},
        )
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["four_hour_frames_built"] == 1
        # allow_enter=false is untouched: no candidate ENTER is possible.
        assert summary["telemetry"]["candidate_enter_count"] == 0
        stored = list(store.pairs.values())[0]
        by_arm = {ev["arm_code"]: ev for ev in stored["evaluations"]}
        candidate = by_arm["candidate_wyckoff_v2"]
        assert candidate["verdict"] != "ENTER"
        details = candidate["details_snapshot"]
        # The experiment-only override is visible in the frozen config
        # snapshot and thresholds; the stored default stays false.
        assert details["thresholds_used"]["enable_4h_trigger"] is True
        assert details["thresholds_used"]["allow_enter"] is False
        assert candidate["config_snapshot"]["enable_4h_trigger"] is True
        assert candidate["config_snapshot"]["allow_enter"] is False
        assert v2_default_config()["enable_4h_trigger"] is False
        # Override echoed in frozen telemetry.
        assert summary["telemetry"]["candidate_identity"][
            "config_overrides"
        ] == {"enable_4h_trigger": True}
        assert "config_overrides" not in summary["telemetry"][
            "control_identity"
        ]

    def test_production_configuration_never_mutated(
        self, store, monkeypatch
    ):
        """The override applies to an in-memory copy only: the canonical
        resolver's returned dict is not mutated and no DB write happens."""
        resolved_objects = {}

        async def fake_resolve(pattern_code, defaults):
            cfg = dict(defaults)
            resolved_objects[pattern_code] = cfg
            return cfg

        monkeypatch.setattr(
            shadow_runner, "resolve_pattern_config", fake_resolve
        )
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=None)},
        )
        _run(run_shadow_comparison(
            provider, ["LONGX"], now_utc=NOW_UTC,
            experiment=WYCKOFF_V2_VS_BASELINE,
        ))
        # The object the resolver returned was never mutated in place.
        assert resolved_objects["wyckoff_mtf_v2"]["enable_4h_trigger"] is False
        assert v2_default_config()["enable_4h_trigger"] is False
        assert v2_default_config()["allow_enter"] is False


class TestSma150ExperimentUnchanged:
    def test_default_experiment_never_touches_intraday(
        self, store, default_configs
    ):
        from sma150_v3_frames import build_uptrend_frame
        from test_shadow_comparison import frame_to_payload

        provider = MtfFakeProvider(
            {"ENTRX": frame_to_payload(build_uptrend_frame(trigger=True))},
            intraday_bars={"ENTRX": _intraday_bars()},
        )
        summary = _run(run_shadow_comparison(
            provider, ["ENTRX"], now_utc=NOW_UTC,
        ))
        assert summary["status"] == "completed"
        # The sma150 experiment performs ZERO intraday calls and its
        # telemetry carries no 4H keys.
        assert provider.intraday_calls == []
        assert "four_hour_frames_built" not in summary["telemetry"]
        assert "candidate_trigger_states" not in summary["telemetry"]
        assert "config_overrides" not in summary["telemetry"][
            "candidate_identity"
        ]
        stored = list(store.pairs.values())[0]
        for ev in stored["evaluations"]:
            assert "_four_hour_frame_meta" not in ev["details_snapshot"]

    def test_as_of_date_pins_daily_frame_deterministically(
        self, store, default_configs, echo_experiment
    ):
        provider = MtfFakeProvider(
            {"LONGX": _long_daily_payload()},
            intraday_bars={"LONGX": _intraday_bars(breakout_close=None)},
        )
        as_of = date(2026, 7, 10)
        summary = _run(run_shadow_comparison(
            provider, ["LONGX"], experiment=echo_experiment, as_of_date=as_of,
        ))
        assert summary["status"] == "completed"
        assert summary["telemetry"]["as_of_date"] == "2026-07-10"
        stored = list(store.pairs.values())[0]
        # No daily bar after the pinned as-of session enters the frame.
        assert stored["pair"]["frame_last_date"] <= "2026-07-10"
