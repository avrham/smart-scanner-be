"""The strategy interface (Phase 4).

Small, typed, and practical. A strategy takes an OHLCV DataFrame plus a
`StrategyContext` and returns a `StrategyResult`. The result maps cleanly onto
the existing `signals` persistence and Phase 2 outcome tracking.

Design choices:
  * `StrategyResult.details` is whatever the strategy wants persisted. Adapters
    for existing strategies MUST keep it byte-identical to prior behavior.
  * side/stop/target/invalidation live on the result for future strategies. They
    are NOT invented for strategies that don't define them (they stay None /
    UNKNOWN), so nothing downstream is fabricated.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd


class StrategyDecision(str, Enum):
    """What the strategy concluded for a symbol."""

    ENTER = "ENTER"
    WATCH = "WATCH"
    AVOID = "AVOID"
    REJECT = "REJECT"


class StrategySide(str, Enum):
    """Directional intent. UNKNOWN when a strategy does not define direction."""

    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


# Legacy string verdicts (from evaluate_sma150_bounce) -> decision enum.
_VERDICT_TO_DECISION = {
    "ENTER": StrategyDecision.ENTER,
    "WATCH": StrategyDecision.WATCH,
    "AVOID": StrategyDecision.AVOID,
    "REJECT": StrategyDecision.REJECT,
}


def decision_from_verdict(verdict: str) -> StrategyDecision:
    """Map a legacy verdict string to a StrategyDecision (defaults to AVOID)."""
    return _VERDICT_TO_DECISION.get((verdict or "").upper(), StrategyDecision.AVOID)


@dataclass
class StrategyContext:
    """Inputs a strategy may need beyond the price DataFrame."""

    symbol: str
    pattern_code: str
    config: Dict[str, Any]
    scanner_mode: Optional[str] = None       # "funnel" | "legacy" | None
    scan_run_id: Optional[str] = None
    data_meta: Optional[Dict[str, Any]] = None


@dataclass
class StrategyResult:
    """The single, uniform output of any strategy evaluation."""

    decision: StrategyDecision
    symbol: str
    pattern_code: str
    score: Optional[float] = None
    side: StrategySide = StrategySide.UNKNOWN
    reason: Optional[str] = None
    rejection_reason: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    score_components: Dict[str, Any] = field(default_factory=dict)
    required_timeframes: List[str] = field(default_factory=lambda: ["1d"])
    entry_price: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    invalidation: Optional[float] = None
    setup_type: Optional[str] = None
    strategy_version: Optional[str] = None

    @property
    def verdict(self) -> str:
        """Legacy verdict string for existing persistence (ENTER/AVOID/...)."""
        return self.decision.value

    def is_actionable(self) -> bool:
        """True for decisions that represent a candidate (ENTER/WATCH)."""
        return self.decision in (StrategyDecision.ENTER, StrategyDecision.WATCH)


class Strategy(ABC):
    """Base contract every strategy implements.

    Subclasses set `pattern_code`, `version`, and `required_timeframes`, and
    implement `evaluate`.
    """

    pattern_code: str = ""
    version: str = "unknown"
    required_timeframes: List[str] = ["1d"]

    @abstractmethod
    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        """Evaluate the strategy for one symbol and return a StrategyResult."""
        raise NotImplementedError
