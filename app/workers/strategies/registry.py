"""Strategy registry (Phase 4).

Maps a `pattern_code` to a concrete `Strategy` instance. Intentionally simple:
static registration only (no dynamic plugin loading yet).
"""

import logging
from typing import Dict, List

from app.workers.strategies.base import Strategy


logger = logging.getLogger(__name__)


class UnknownStrategyError(KeyError):
    """Raised when a pattern_code has no registered strategy."""


_REGISTRY: Dict[str, Strategy] = {}


def register_strategy(strategy: Strategy) -> None:
    """Register (or replace) a strategy under its pattern_code."""
    code = strategy.pattern_code
    if not code:
        raise ValueError("Strategy.pattern_code must be a non-empty string")
    if code in _REGISTRY:
        logger.info("Replacing already-registered strategy '%s'", code)
    _REGISTRY[code] = strategy


def get_strategy(pattern_code: str) -> Strategy:
    """Return the strategy for a pattern_code or raise UnknownStrategyError."""
    try:
        return _REGISTRY[pattern_code]
    except KeyError:
        raise UnknownStrategyError(
            f"No strategy registered for pattern_code '{pattern_code}'. "
            f"Registered: {sorted(_REGISTRY.keys())}"
        )


def list_strategies() -> List[str]:
    """Return the sorted list of registered pattern codes."""
    return sorted(_REGISTRY.keys())


def _register_defaults() -> None:
    """Register the built-in strategies. Imported here to avoid import cycles."""
    # Local imports: adapters import base (+ their own logic), never the registry.
    from app.workers.strategies.sma150_adapter import Sma150BounceStrategy
    from app.workers.strategies.sma150_v3 import Sma150BounceV3Strategy
    from app.workers.strategies.wyckoff import WyckoffMTFStrategy

    if "sma150_bounce" not in _REGISTRY:
        register_strategy(Sma150BounceStrategy())
    # Phase 8: sma150.v3 is a SEPARATE strategy code — never an alias/upgrade
    # of sma150_bounce, which stays on sma150.v2 unchanged.
    if "sma150_bounce_v3" not in _REGISTRY:
        register_strategy(Sma150BounceV3Strategy())
    if "wyckoff_mtf" not in _REGISTRY:
        register_strategy(WyckoffMTFStrategy())


_register_defaults()
