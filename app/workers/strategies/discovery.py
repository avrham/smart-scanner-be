"""Read-only strategy discovery for administrative inspection (Phase 9C3).

Joins the in-process strategy registry with `patterns` / `pattern_configs`
without mutating configuration, enabling strategies, or invoking providers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import asyncpg

from app.workers.patterns.config import merge_config
from app.workers.strategies.registry import (
    UnknownStrategyError,
    get_strategy,
    list_strategies,
)


CONFIG_STATUS_CONFIGURED = "configured"
CONFIG_STATUS_MISSING_PATTERN_ROW = "missing_pattern_row"


@dataclass(frozen=True)
class StrategyDiscovery:
    """Internal read model for admin strategy discovery."""

    pattern_code: str
    registered: bool
    enabled: Optional[bool]
    db_configured: bool
    config_status: str
    name: Optional[str]
    description: Optional[str]
    strategy_version: Optional[str]
    decision_policy_version: Optional[str]
    allow_enter: Optional[bool]
    enable_4h_trigger: Optional[bool]
    min_price: Optional[float]
    effective_config: Dict[str, Any]


def _optional_bool(config: Dict[str, Any], key: str) -> Optional[bool]:
    if key not in config:
        return None
    value = config[key]
    if isinstance(value, bool):
        return value
    return None


def _optional_float(config: Dict[str, Any], key: str) -> Optional[float]:
    if key not in config:
        return None
    value = config[key]
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def build_discovery_from_sources(
    *,
    pattern_code: str,
    strategy_version: Optional[str],
    decision_policy_version: Optional[str],
    defaults: Dict[str, Any],
    pattern_row: Optional[Dict[str, Any]],
    raw_config: Dict[str, Any],
) -> StrategyDiscovery:
    """Assemble a discovery record from registry + optional DB sources.

    Pure helper: never writes. A missing pattern row yields enabled=None and
    never treats the strategy as enabled.
    """
    if pattern_row is None:
        merged, _ = merge_config({}, defaults)
        return StrategyDiscovery(
            pattern_code=pattern_code,
            registered=True,
            enabled=None,
            db_configured=False,
            config_status=CONFIG_STATUS_MISSING_PATTERN_ROW,
            name=None,
            description=None,
            strategy_version=strategy_version,
            decision_policy_version=decision_policy_version,
            allow_enter=_optional_bool(merged, "allow_enter"),
            enable_4h_trigger=_optional_bool(merged, "enable_4h_trigger"),
            min_price=_optional_float(merged, "min_price"),
            effective_config=merged,
        )

    merged, _ = merge_config(raw_config or {}, defaults)
    enabled = pattern_row.get("is_enabled")
    if enabled is not None:
        enabled = bool(enabled)
    return StrategyDiscovery(
        pattern_code=pattern_code,
        registered=True,
        enabled=enabled,
        db_configured=True,
        config_status=CONFIG_STATUS_CONFIGURED,
        name=pattern_row.get("name"),
        description=pattern_row.get("description"),
        strategy_version=strategy_version,
        decision_policy_version=decision_policy_version,
        allow_enter=_optional_bool(merged, "allow_enter"),
        enable_4h_trigger=_optional_bool(merged, "enable_4h_trigger"),
        min_price=_optional_float(merged, "min_price"),
        effective_config=merged,
    )


async def _fetch_pattern_row(
    db: asyncpg.Connection, pattern_code: str
) -> Optional[asyncpg.Record]:
    return await db.fetchrow(
        """
        SELECT code, name, description, is_enabled
        FROM patterns
        WHERE code = $1
        """,
        pattern_code,
    )


async def _fetch_raw_config(
    db: asyncpg.Connection, pattern_code: str
) -> Dict[str, Any]:
    rows = await db.fetch(
        """
        SELECT key, value
        FROM pattern_configs
        WHERE pattern_code = $1
        """,
        pattern_code,
    )
    return {row["key"]: row["value"] for row in rows}


async def discover_strategy(
    db: asyncpg.Connection, pattern_code: str
) -> Optional[StrategyDiscovery]:
    """Discover one registered strategy. Returns None if not in the registry."""
    try:
        strategy = get_strategy(pattern_code)
    except UnknownStrategyError:
        return None

    pattern_row = await _fetch_pattern_row(db, pattern_code)
    raw_config: Dict[str, Any] = {}
    row_dict: Optional[Dict[str, Any]] = None
    if pattern_row is not None:
        row_dict = dict(pattern_row)
        raw_config = await _fetch_raw_config(db, pattern_code)

    return build_discovery_from_sources(
        pattern_code=pattern_code,
        strategy_version=getattr(strategy, "version", None),
        decision_policy_version=getattr(strategy, "decision_policy_version", None),
        defaults=strategy.default_config(),
        pattern_row=row_dict,
        raw_config=raw_config,
    )


async def discover_all_strategies(
    db: asyncpg.Connection,
) -> List[StrategyDiscovery]:
    """Discover every canonically registered strategy (sorted by list_strategies)."""
    results: List[StrategyDiscovery] = []
    for code in list_strategies():
        item = await discover_strategy(db, code)
        if item is not None:
            results.append(item)
    return results
