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
from contextlib import contextmanager
from contextvars import ContextVar
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


class ConfigUnavailable(RuntimeError):
    """The canonical configuration could not be resolved, and the caller asked
    to be told rather than quietly given defaults."""


# --------------------------------------------------------------------------- #
# Binding config resolution to a caller's connection.
#
# THE PROBLEM THIS SOLVES
# The default read goes through the global pool. For the FastAPI process that
# is right. For an operator-run process reading an isolated database, it is a
# DIFFERENT database — the read fails, the resolver falls back to safe
# defaults, and the caller gets a plausible configuration from nowhere. The
# first research cohort hit exactly that: its config hash matched the canonical
# experiment's only because staging stores no override for that pattern, so
# both ended up on the same defaults. An accidental equality is not a
# guarantee.
#
# WHY A CONTEXT VARIABLE AND NOT A PARAMETER
# The parameter would have to be threaded through `_resolve_arm` in
# `app/workers/shadow/runner.py`, which several phase-boundary tests assert is
# unmodified — and rightly: that file is the canonical execution layer. A
# context variable keeps the change inside this one resolver, leaves the
# canonical call path byte-identical, and is scoped explicitly at the call site
# by `bound_config_connection`. It is async-safe (each task gets its own copy)
# and defaults to the historical behaviour when nothing is bound.
# --------------------------------------------------------------------------- #
_BOUND_CONNECTION: ContextVar = ContextVar("pattern_config_connection",
                                           default=None)
_REQUIRE_DB: ContextVar = ContextVar("pattern_config_require_db", default=False)

#: The canonical config read, defined once. Identical in shape to the one in
#: `app.workers.persistence.get_pattern_config`; it exists here so a caller can
#: run it on a connection it already holds without that module acquiring its
#: own from the global pool.
PATTERN_CONFIG_SQL = """
SELECT key, value FROM pattern_configs WHERE pattern_code = $1
"""


@contextmanager
def bound_config_connection(conn, *, require_db: bool = False):
    """Resolve pattern configuration on `conn` for the duration of the block.

    `require_db=True` turns the silent safe-defaults fallback into
    `ConfigUnavailable`. A caller that must evaluate with the canonical
    configuration uses it, so "we happened to get the same hash" stops being
    the guarantee and "we resolved the same configuration" starts being it.
    """
    conn_token = _BOUND_CONNECTION.set(conn)
    require_token = _REQUIRE_DB.set(bool(require_db))
    try:
        yield
    finally:
        _BOUND_CONNECTION.reset(conn_token)
        _REQUIRE_DB.reset(require_token)


async def _read_bound_config(conn, pattern_code: str) -> Dict[str, Any]:
    rows = await conn.fetch(PATTERN_CONFIG_SQL, pattern_code)
    return {row["key"]: row["value"] for row in rows}


async def resolve_pattern_config(
    pattern_code: str, defaults: Dict[str, Any], *, conn=None,
    require_db: bool = False,
) -> Dict[str, Any]:
    """Load config for a pattern from the DB and merge over safe defaults.

    If the DB has no config (or the lookup fails), logs the fallback clearly
    and returns a copy of the safe defaults.

    `conn` (or an enclosing `bound_config_connection`) binds the read to a
    connection the caller already holds — without it the global pool is used,
    which for an operator-run process is a DIFFERENT database, and a config
    resolved from the wrong database is indistinguishable from a correct one
    until it is not.

    `require_db=True` makes the fallback an ERROR instead.
    """
    bound = conn if conn is not None else _BOUND_CONNECTION.get()
    require_db = require_db or _REQUIRE_DB.get()
    try:
        raw = (await _read_bound_config(bound, pattern_code) if bound is not None
               else await get_pattern_config(pattern_code))
    except Exception as exc:  # defensive: never let config loading break a scan
        if require_db:
            raise ConfigUnavailable(
                f"canonical config for {pattern_code!r} could not be read"
            ) from exc
        logger.warning(
            "Config lookup failed for pattern '%s' (%s); using safe defaults",
            pattern_code,
            exc,
        )
        return dict(defaults)

    config, used_fallback = merge_config(raw, defaults)
    if used_fallback and require_db:
        # A pattern with NO stored config is a legitimate state — the defaults
        # ARE the canonical configuration then. What must not happen silently
        # is reaching that answer because the database was unreachable, and the
        # branch above has already ruled that out.
        logger.info(
            "Pattern '%s' has no stored config rows; the strategy defaults ARE "
            "the canonical configuration for it", pattern_code)
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
