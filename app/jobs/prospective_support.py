"""Shared prospective helpers used by BOTH the enqueue service and the worker
handler — the frozen-universe loader, the pinned candidate/control identity
strings, and the no-outcomes guard. Raises TerminalJobError (not HTTPException)
so the worker classifies failures correctly. No strategy math here.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

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
    """DEPRECATED (global variant). The prospective evaluation pipeline creates NO
    forward outcomes, but the dedicated outcome-maturation worker legitimately
    writes outcomes for OTHER (prior) campaigns onto the SAME isolated DB once a
    repeatable daily pipeline runs. A global count therefore false-blocks every
    campaign after the first maturation. Retained only for callers that predate
    the run-scoped guard; new callers must use ``assert_no_outcomes_for_run``."""
    n = await conn.fetchval("SELECT count(*)::int FROM strategy_shadow_pair_outcomes")
    if int(n or 0) != 0:
        raise TerminalJobError("unexpected_outcomes_present",
                               f"outcome rows exist: {int(n or 0)}")
    return 0


async def assert_no_outcomes_for_run(conn: asyncpg.Connection,
                                     campaign_run_id: Optional[Any]) -> int:
    """Run-scoped integrity guard: a campaign's OWN pairs must carry no forward
    outcomes at (fresh) creation/execution time. Outcomes belonging to OTHER
    campaigns — e.g. prior campaigns matured by the outcome worker on the shared
    isolated DB — are EXPECTED and never a problem. A ``None`` run means the
    campaign has not executed yet (no pairs exist), so there is nothing to check.
    """
    if campaign_run_id is None:
        return 0
    n = await conn.fetchval(
        "SELECT count(*)::int FROM strategy_shadow_pair_outcomes o "
        "JOIN strategy_shadow_run_pairs rp ON rp.pair_id = o.pair_id "
        "WHERE rp.run_id = $1", campaign_run_id)
    if int(n or 0) != 0:
        raise TerminalJobError("unexpected_outcomes_present",
                               f"outcome rows exist for campaign run: {int(n or 0)}")
    return 0


__all__ = [
    "candidate_identity_string", "control_identity_string",
    "load_frozen_universe", "assert_no_outcomes", "assert_no_outcomes_for_run",
]
