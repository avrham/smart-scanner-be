"""wyckoff_mtf — deterministic multi-timeframe strategy (Phase 5).

Flow (all deterministic, no subjective interpretation):
    daily -> resample to monthly + weekly
    monthly bias   (LONG/SHORT/NEUTRAL) ; NEUTRAL -> REJECT
    weekly align   (must agree with bias + valid phase) ; else REJECT
    daily setup    (spring/utad/sos/sow/range breakout|breakdown) ; none -> REJECT
    4H trigger     (optional, only if enabled AND data supplied):
                     trigger  -> ENTER (with entry/stop/invalidation)
                     no 4H    -> WATCH  (valid MTF setup awaiting trigger)

4H data is NEVER fetched here. It is only used when the caller injects it via
`StrategyContext.data_meta["df_4h"]` AND `enable_4h_trigger` is true. The funnel
keeps expensive stages disabled, so via the funnel wyckoff yields at most WATCH.

Score is decomposed and explainable (structure_score in 0..1) built from raw
components; there is no opaque 0-100 confidence number.
"""

from typing import Any, Dict, Optional

import pandas as pd

from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.wyckoff import events, structure
from app.workers.timeframes import (
    normalize_ohlcv,
    resample_to_monthly,
    resample_to_weekly,
)


STRATEGY_VERSION = "wyckoff_mtf.v1"

# Structure-score weights (sum to 1.0). The 4H trigger is NOT part of this score;
# it only decides ENTER vs WATCH. This keeps a valid MTF setup at WATCH quality
# even before a trigger exists.
_W_MONTHLY = 0.30
_W_WEEKLY = 0.30
_W_DAILY = 0.30
_W_VOLUME = 0.10

DEFAULT_CONFIG: Dict[str, Any] = {
    # Monthly macro bias
    "monthly_sma_window": 20,
    "monthly_min_bars": 24,
    "monthly_slope_lookback": 3,
    # Weekly alignment
    "weekly_sma_window": 20,
    "weekly_min_bars": 26,
    "weekly_slope_lookback": 4,
    # Daily setup
    "daily_range_lookback": 60,
    "atr_window": 14,
    "min_range_atr_multiple": 3.0,
    "pierce_atr_multiple": 0.10,
    "volume_sma_window": 20,
    "min_breakout_volume_ratio": 1.5,
    # 4H trigger (optional/expensive)
    "trigger_lookback_4h": 10,
    "enable_4h_trigger": False,
    "require_4h_for_enter": True,
    # Gating
    "score_threshold": 0.55,
    "min_price": 5.0,
    # Deep daily history is required to build >=24 monthly bars.
    "min_daily_bars": 540,
    "min_liquidity_filters": {"min_market_cap": 300_000_000, "min_daily_volume": 300_000},
}

_SIDE_MAP = {structure.LONG: StrategySide.LONG, structure.SHORT: StrategySide.SHORT}


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


class WyckoffMTFStrategy(Strategy):
    pattern_code = "wyckoff_mtf"
    version = STRATEGY_VERSION
    required_timeframes = ["1d", "1w", "1M", "4h"]
    min_daily_bars = int(DEFAULT_CONFIG["min_daily_bars"])

    def default_config(self) -> Dict[str, Any]:
        cfg = dict(DEFAULT_CONFIG)
        cfg["min_liquidity_filters"] = dict(DEFAULT_CONFIG["min_liquidity_filters"])
        return cfg

    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        config = context.config if context.config is not None else self.default_config()
        daily = normalize_ohlcv(df)

        min_daily_bars = int(config.get("min_daily_bars", self.min_daily_bars))
        if len(daily) < min_daily_bars:
            return self._reject(
                context, "insufficient_daily_data",
                {"daily_bars": len(daily), "daily_bars_required": min_daily_bars},
            )

        snapshot_date = str(pd.to_datetime(daily["date"].iloc[-1]).date())
        min_price = float(config.get("min_price", 0.0))
        last_close = float(daily["close"].iloc[-1])
        if last_close < min_price:
            return self._reject(context, "price_below_min", {"snapshot_date": snapshot_date})

        monthly = resample_to_monthly(daily)
        weekly = resample_to_weekly(daily)

        bias, m_components = structure.monthly_bias(monthly, config)
        components: Dict[str, Any] = dict(m_components)
        if bias == structure.NEUTRAL:
            return self._reject(context, "monthly_neutral", {"snapshot_date": snapshot_date}, components)

        aligned, phase, w_components = structure.weekly_alignment(weekly, bias, config)
        components.update(w_components)
        if not aligned:
            return self._reject(context, "weekly_not_aligned", {"snapshot_date": snapshot_date}, components)

        d_components = events.detect_daily_setup(daily, bias, config)
        components.update(d_components)
        setup_type = d_components.get("setup_type", events.SETUP_NONE)
        if setup_type == events.SETUP_NONE:
            return self._reject(context, "no_daily_setup", {"snapshot_date": snapshot_date}, components)

        # Volume confirmation (raw, clipped) for the structure score.
        min_vol_ratio = float(config["min_breakout_volume_ratio"])
        vol_ratio = d_components.get("daily_volume_ratio")
        volume_confirmation = (
            _clip01(vol_ratio / min_vol_ratio) if isinstance(vol_ratio, (int, float)) else 0.0
        )
        components["volume_confirmation"] = round(volume_confirmation, 4)

        monthly_q = float(m_components.get("monthly_bias_quality", 0.0))
        weekly_q = float(w_components.get("weekly_alignment_quality", 0.0))
        daily_q = float(d_components.get("daily_setup_quality", 0.0))
        structure_score = round(
            _W_MONTHLY * monthly_q
            + _W_WEEKLY * weekly_q
            + _W_DAILY * daily_q
            + _W_VOLUME * volume_confirmation,
            4,
        )
        components["structure_score"] = structure_score

        score_threshold = float(config["score_threshold"])
        if structure_score < score_threshold:
            return self._reject(
                context, "score_below_threshold",
                {"snapshot_date": snapshot_date}, components,
                score=structure_score, side=_SIDE_MAP[bias], setup_type=setup_type,
            )

        # ---- 4H trigger (optional; never fetched here) --------------------- #
        enable_4h = bool(config.get("enable_4h_trigger", False))
        require_4h = bool(config.get("require_4h_for_enter", True))
        df_4h = (context.data_meta or {}).get("df_4h") if context.data_meta else None

        trigger = None
        if enable_4h and df_4h is not None:
            trigger = events.four_hour_trigger(df_4h, bias, config)
        components["trigger_quality"] = float(trigger["trigger_quality"]) if trigger else 0.0
        components["has_4h"] = df_4h is not None

        entry_price = stop_price = target_price = invalidation = None
        if require_4h:
            if trigger and trigger.get("triggered"):
                decision = StrategyDecision.ENTER
                entry_price = trigger.get("entry_price")
                stop_price = trigger.get("stop_price")
                invalidation = trigger.get("invalidation")
                target_price = trigger.get("target_price")
            else:
                # Valid MTF setup, but no confirmed trigger -> WATCH (not ENTER).
                decision = StrategyDecision.WATCH
        else:
            decision = StrategyDecision.ENTER

        return self._result(
            context,
            decision=decision,
            score=structure_score,
            side=_SIDE_MAP[bias],
            setup_type=setup_type,
            phase=phase,
            bias=bias,
            snapshot_date=snapshot_date,
            components=components,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            invalidation=invalidation,
        )

    # ------------------------------------------------------------------ #
    # Result builders
    # ------------------------------------------------------------------ #

    def _reject(
        self,
        context: StrategyContext,
        rejection_reason: str,
        extra: Optional[Dict[str, Any]] = None,
        components: Optional[Dict[str, Any]] = None,
        score: Optional[float] = None,
        side: StrategySide = StrategySide.UNKNOWN,
        setup_type: Optional[str] = None,
    ) -> StrategyResult:
        components = components or {}
        details: Dict[str, Any] = {
            "symbol": context.symbol,
            "score_version": STRATEGY_VERSION,
            "rejection_reason": rejection_reason,
            "score_components": components,
        }
        if extra:
            details.update(extra)
        return StrategyResult(
            decision=StrategyDecision.REJECT,
            symbol=context.symbol,
            pattern_code=self.pattern_code,
            score=score,
            side=side,
            reason=rejection_reason,
            rejection_reason=rejection_reason,
            details=details,
            score_components=components,
            required_timeframes=self.required_timeframes,
            setup_type=setup_type,
            strategy_version=STRATEGY_VERSION,
        )

    def _result(
        self,
        context: StrategyContext,
        *,
        decision: StrategyDecision,
        score: float,
        side: StrategySide,
        setup_type: str,
        phase: str,
        bias: str,
        snapshot_date: str,
        components: Dict[str, Any],
        entry_price: Optional[float],
        stop_price: Optional[float],
        target_price: Optional[float],
        invalidation: Optional[float],
    ) -> StrategyResult:
        reason = (
            f"{decision.value} {side.value} {setup_type} | "
            f"monthly={bias}, weekly_phase={phase}, score={score}"
        )
        details: Dict[str, Any] = {
            "symbol": context.symbol,
            "snapshot_date": snapshot_date,
            "score_version": STRATEGY_VERSION,
            "side": side.value,
            "setup_type": setup_type,
            "monthly_bias": bias,
            "weekly_phase": phase,
            "weekly_aligned": True,
            "timeframes": {
                "monthly_bars": components.get("monthly_bars"),
                "weekly_bars": components.get("weekly_bars"),
                "daily_bars": None,
                "has_4h": components.get("has_4h", False),
            },
            "score_components": components,
            "rejection_reason": None,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "invalidation": invalidation,
        }
        return StrategyResult(
            decision=decision,
            symbol=context.symbol,
            pattern_code=self.pattern_code,
            score=score,
            side=side,
            reason=reason,
            rejection_reason=None,
            details=details,
            score_components=components,
            required_timeframes=self.required_timeframes,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            invalidation=invalidation,
            setup_type=setup_type,
            strategy_version=STRATEGY_VERSION,
        )
