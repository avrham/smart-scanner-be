"""Shared prospective helpers used by BOTH the enqueue service and the worker
handler — the frozen-universe loader, the pinned candidate/control identity
strings, and the no-outcomes guard. Raises TerminalJobError (not HTTPException)
so the worker classifies failures correctly. No strategy math here.
"""

from __future__ import annotations

from typing import Any, Dict, List

import asyncpg

import app.prospective_campaign as pc
from app.jobs.contracts import TerminalJobError


def candidate_identity_string() -> str:
    """Canonical candidate identity pinned server-side (allow_enter is always
    false — a WATCH is never reinterpreted as an entry)."""
    return (f"{pc.CANDIDATE_STRATEGY_CODE}:{pc.CANDIDATE_STRATEGY_VERSION}:"
            f"{pc.CANDIDATE_SIGNAL_DEFINITION}:allow_enter="
            f"{str(bool(pc.CANDIDATE_ALLOW_ENTER)).lower()}")


def control_identity_string() -> str:
    return f"{pc.CONTROL_STRATEGY_CODE}:{pc.CONTROL_STRATEGY_VERSION}"


async def load_frozen_universe(conn: asyncpg.Connection, universe_id: Any) -> Dict[str, Any]:
    """Load a FROZEN universe + ordered membership and prove the recomputed hash
    equals the pinned hash. Raises TerminalJobError on any drift."""
    from app.history_warmup_execute import compute_universe_hash, UNIVERSE_FROZEN
    urow = await conn.fetchrow(
        "SELECT * FROM history_warmup_universes WHERE id = $1", universe_id)
    if urow is None:
        raise TerminalJobError("unknown_universe", "frozen universe not found")
    rows = await conn.fetch(
        "SELECT symbol, ordinal FROM history_warmup_universe_symbols "
        "WHERE universe_id=$1 ORDER BY ordinal", urow["id"])
    symbols: List[str] = [r["symbol"] for r in rows]
    recomputed = compute_universe_hash(
        universe_code=urow["universe_code"], universe_version=urow["universe_version"],
        symbols_in_ordinal_order=symbols)
    if urow["status"] != UNIVERSE_FROZEN:
        raise TerminalJobError("universe_not_frozen", f"status={urow['status']}")
    if recomputed != urow["universe_hash"]:
        raise TerminalJobError("universe_membership_mismatch",
                               "recomputed universe hash != pinned hash")
    return {
        "universe_id": str(urow["id"]),
        "universe_code": urow["universe_code"],
        "universe_version": urow["universe_version"],
        "status": urow["status"],
        "symbol_count": urow["symbol_count"],
        "symbols": symbols,
        "universe_hash": urow["universe_hash"],
    }


async def assert_no_outcomes(conn: asyncpg.Connection) -> int:
    """This pipeline creates NO forward outcomes. Any pre-existing outcome row is
    a data-integrity hard stop (never expected on the isolated DB)."""
    n = await conn.fetchval("SELECT count(*)::int FROM strategy_shadow_pair_outcomes")
    if int(n or 0) != 0:
        raise TerminalJobError("unexpected_outcomes_present",
                               f"outcome rows exist: {int(n or 0)}")
    return 0


__all__ = [
    "candidate_identity_string", "control_identity_string",
    "load_frozen_universe", "assert_no_outcomes",
]
