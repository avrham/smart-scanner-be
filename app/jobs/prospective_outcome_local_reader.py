"""Local-only forward-bar reader for prospective outcome maturation.

Mirrors app/prospective_local_provider.LocalHistoryProvider's convention
(read ONLY from the local daily_bars cache, construct NO provider) but reads
FORWARD from a pair's snapshot_date rather than backward from a cutoff —
LocalHistoryProvider is hard-bounded to trading_date <= snapshot_session_date
by design and cannot serve this direction.

Bar dicts are shaped to feed app.workers.shadow.outcomes.calculator's
existing pure functions (build_forward_sequence / _canonical_bar) UNCHANGED.
Every daily_bars row is, by construction of the history-warmup pipeline,
already a COMPLETED session (normalize_daily_bars only ever stores
trading_date < the warmup day) — so forward reads are marked
explicit_completed=True; there is no still-forming-session ambiguity to
resolve for already-persisted local history.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from app.workers.persistence import get_db_connection, release_db_connection
from app.workers.shadow.outcomes.calculator import MAX_WINDOW


async def read_local_forward_bars(symbol: str, snapshot_date: date,
                                  *, limit: int = MAX_WINDOW) -> List[Dict[str, Any]]:
    """COMPLETED daily bars strictly after ``snapshot_date``, chronological,
    local-only (no provider), capped at ``limit`` (default MAX_WINDOW=20 —
    build_forward_sequence would cap it there anyway, but bounding the SQL
    read keeps this a small, predictable query)."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            "SELECT trading_date, open, high, low, close, volume "
            "FROM daily_bars WHERE symbol = $1 AND trading_date > $2 "
            "ORDER BY trading_date ASC LIMIT $3",
            symbol.upper(), snapshot_date, int(limit))
    finally:
        await release_db_connection(conn)
    return [
        {"date": r["trading_date"].isoformat(), "open": float(r["open"]),
         "high": float(r["high"]), "low": float(r["low"]),
         "close": float(r["close"]), "volume": float(r["volume"])}
        for r in rows
    ]


async def read_local_snapshot_bar(symbol: str, snapshot_date: date) -> Dict[str, Any]:
    """The local bar ON snapshot_date, used only as a continuity/revision
    check against the frozen reference (never part of the forward sequence,
    never a substitute reference)."""
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT trading_date, open, high, low, close, volume "
            "FROM daily_bars WHERE symbol = $1 AND trading_date = $2",
            symbol.upper(), snapshot_date)
    finally:
        await release_db_connection(conn)
    if row is None:
        return None
    return {"date": row["trading_date"].isoformat(), "open": float(row["open"]),
            "high": float(row["high"]), "low": float(row["low"]),
            "close": float(row["close"]), "volume": float(row["volume"])}


async def local_session_dates(symbols: List[str], after: date) -> List[date]:
    """The local trading-session calendar used for maturity classification:
    the UNION of distinct trading_date values present locally, strictly
    after ``after``, across ``symbols``. Unlike the existing SPY-based
    _cohort_trading_calendar (app/routers/admin.py), this campaign's frozen
    universe does not include SPY/QQQ — the campaign symbols themselves
    (warmed together as one universe, sharing identical local date coverage)
    serve as the local session-date reference instead. An empty result
    leaves eligibility honestly ``eligibility_unknown`` (never assumed)."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            "SELECT DISTINCT trading_date FROM daily_bars "
            "WHERE symbol = ANY($1::text[]) AND trading_date > $2 "
            "ORDER BY trading_date ASC",
            [s.upper() for s in symbols], after)
    finally:
        await release_db_connection(conn)
    return [r["trading_date"] for r in rows]


__all__ = ["read_local_forward_bars", "read_local_snapshot_bar", "local_session_dates"]
