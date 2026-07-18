"""
Pattern configuration resolution.

Bridges DB-stored `pattern_configs` (JSONB values) into the typed config dicts
that pattern evaluators expect. Keeps a pure merge/parse core (unit-testable
without a database) and a thin async resolver that reads from the DB.

Phase 1 (Evidence Engine): fixes B1 - pattern config was never wired into
evaluation. Strategies must not rely on hardcoded thresholds when a DB config
exists; when it is missing we fall back to safe defaults and log it clearly.
"""

import json
import logging
from typing import Any, Dict, Tuple

from app.workers.persistence import get_pattern_config


logger = logging.getLogger(__name__)


def coerce_config_value(value: Any) -> Any:
    """Coerce a single DB config value into a native Python type.

    `pattern_configs.value` is JSONB. Depending on the driver/codec it may
    arrive as a JSON-encoded string (e.g. "150", "5.0", '{"a": 1}') or as an
    already-decoded Python object. We normalize both cases; on failure we
    return the original value untouched (never invent a value).
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


def parse_config_values(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce every value in a raw DB config dict."""
    return {key: coerce_config_value(val) for key, val in (raw or {}).items()}


def merge_config(
    raw: Dict[str, Any], defaults: Dict[str, Any]
) -> Tuple[Dict[str, Any], bool]:
    """Merge parsed DB config over safe defaults.

    Returns (config, used_fallback). `used_fallback` is True when no DB config
    values were available and defaults were used wholesale.
    """
    parsed = parse_config_values(raw)
    if not parsed:
        return dict(defaults), True
    merged = {**defaults, **parsed}
    return merged, False


async def resolve_pattern_config(
    pattern_code: str, defaults: Dict[str, Any]
) -> Dict[str, Any]:
    """Load config for a pattern from the DB and merge over safe defaults.

    If the DB has no config (or the lookup fails), logs the fallback clearly
    and returns a copy of the safe defaults.
    """
    try:
        raw = await get_pattern_config(pattern_code)
    except Exception as exc:  # defensive: never let config loading break a scan
        logger.warning(
            "Config lookup failed for pattern '%s' (%s); using safe defaults",
            pattern_code,
            exc,
        )
        return dict(defaults)

    config, used_fallback = merge_config(raw, defaults)
    if used_fallback:
        logger.warning(
            "No DB config found for pattern '%s'; using safe defaults", pattern_code
        )
    else:
        db_keys = sorted(set(parse_config_values(raw).keys()))
        logger.info(
            "Loaded DB config for pattern '%s' (overrides: %s)",
            pattern_code,
            ", ".join(db_keys),
        )
    return config
