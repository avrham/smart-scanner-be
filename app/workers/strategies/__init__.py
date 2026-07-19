"""Phase 4 (Evidence Engine): strategy interface + registry.

Every strategy is evaluated through one small, typed contract (`Strategy` in
base.py) so the funnel scanner, signal persistence, and Phase 2 outcome tracking
all talk to strategies the same way. This is the seam that later lets Wyckoff MTF
plug in as just another strategy module (Phase 5) without touching the scanner.

Nothing here changes sma150_bounce's behavior: `sma150_adapter.py` wraps the
existing `evaluate_sma150_bounce` and returns a `StrategyResult` with identical
verdict/score/reason/details.
"""

from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.registry import (
    UnknownStrategyError,
    get_strategy,
    list_strategies,
    register_strategy,
)

__all__ = [
    "Strategy",
    "StrategyContext",
    "StrategyDecision",
    "StrategyResult",
    "StrategySide",
    "UnknownStrategyError",
    "get_strategy",
    "list_strategies",
    "register_strategy",
]
