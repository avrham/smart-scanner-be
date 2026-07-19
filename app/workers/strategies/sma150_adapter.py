"""sma150_bounce wrapped behind the strategy interface (Phase 4).

This adapter does NOT change sma150 logic. It calls the existing
`evaluate_sma150_bounce` and repackages its output as a `StrategyResult` with an
identical verdict/score/reason/details payload.

Side: sma150_bounce is a long-only rebound setup (it scores upward rebounds off
the SMA and only ever persists ENTER). Phase 2 outcome tracking already treats
these signals as LONG by default. We therefore expose `side=LONG` at the
interface level, but we intentionally keep `details` byte-identical to the legacy
evaluator (we do NOT inject side/stop/target into the persisted signal). Any
formal per-strategy direction/stop/target contract is left to future strategies
(e.g. Wyckoff MTF in Phase 5).
"""

from typing import Any, Dict

import pandas as pd

from app.workers.patterns.sma150 import (
    DEFAULT_CONFIG,
    SCORE_VERSION,
    evaluate_sma150_bounce,
)
from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyResult,
    StrategySide,
    decision_from_verdict,
)


class Sma150BounceStrategy(Strategy):
    pattern_code = "sma150_bounce"
    version = SCORE_VERSION  # "sma150.v2"
    required_timeframes = ["1d"]

    def default_config(self) -> Dict[str, Any]:
        """Safe defaults used when no DB config is resolved by the caller."""
        return DEFAULT_CONFIG.copy()

    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        config = context.config if context.config is not None else self.default_config()
        raw = evaluate_sma150_bounce(context.symbol, df, config)

        details = raw.get("details") or {}
        decision = decision_from_verdict(raw.get("verdict"))

        return StrategyResult(
            decision=decision,
            symbol=context.symbol,
            pattern_code=self.pattern_code,
            score=raw.get("score"),
            # Long-only rebound semantics; details stay unchanged (see module docstring).
            side=StrategySide.LONG,
            reason=raw.get("reason"),
            rejection_reason=details.get("rejection_reason"),
            details=details,
            score_components=details.get("score_components", {}),
            required_timeframes=self.required_timeframes,
            # sma150 defines no stop/target/invalidation -> not invented.
            entry_price=None,
            stop_price=None,
            target_price=None,
            invalidation=None,
            setup_type="sma150_bounce",
            strategy_version=self.version,
        )
