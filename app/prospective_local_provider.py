"""Local-history provider shim for prospective campaign evaluation.

Exposes the EXACT duck-typed surface the shadow runner consumes
(`name`, `supports_intraday_history`, `get_daily_history`, `get_intraday_history`)
but serves bars ONLY from the local isolated database (`daily_bars`,
`market_bars_4h`) bounded at/before the pinned snapshot cutoff. It performs NO
network access and constructs NO real provider — a hard lookahead barrier:

  * daily bars: trading_date <= snapshot_session_date;
  * 4H bars: is_completed AND bar_end <= snapshot_cutoff_at.

The strategy math + frame builders are reused unchanged; only the two provider
fetch points are replaced by these local reads.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from app.workers.persistence import get_db_connection, release_db_connection


class LocalHistoryProvider:
    name = "local-history"
    supports_bounded_daily_range = True
    supports_intraday_history = True

    def __init__(self, *, snapshot_session_date: date, snapshot_cutoff_at: datetime):
        self.snapshot_session_date = snapshot_session_date
        self.snapshot_cutoff_at = snapshot_cutoff_at
        # bounded observability (no secrets): counts of local reads performed
        self.daily_reads = 0
        self.intraday_reads = 0

    async def get_daily_history(self, symbol: str, timeseries: int = 400) -> Dict[str, Any]:
        """FMP-shaped daily payload from local daily_bars, <= snapshot session."""
        self.daily_reads += 1
        cap = max(1, int(timeseries)) + 30
        conn = await get_db_connection()
        try:
            rows = await conn.fetch(
                "SELECT trading_date, open, high, low, close, volume "
                "FROM daily_bars WHERE symbol = $1 AND trading_date <= $2 "
                "ORDER BY trading_date DESC LIMIT $3",
                symbol.upper(), self.snapshot_session_date, cap)
        finally:
            await release_db_connection(conn)
        historical = [
            {"date": r["trading_date"].isoformat(), "open": float(r["open"]),
             "high": float(r["high"]), "low": float(r["low"]),
             "close": float(r["close"]), "volume": float(r["volume"])}
            for r in reversed(rows)
        ]
        return {"symbol": symbol.upper(), "historical": historical}

    async def batch_historical_data(self, symbols: List[str], timeseries: int = 350
                                    ) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for s in symbols:
            out[s] = await self.get_daily_history(s, timeseries)
        return out

    async def get_intraday_history(self, symbol: str, *, multiplier: int,
                                   timespan: str, start=None, end=None,
                                   limit: Optional[int] = None) -> Dict[str, Any]:
        """Normalized 4H payload from local market_bars_4h, COMPLETED bars whose
        bar_end <= snapshot cutoff (never a later/forming bar)."""
        self.intraday_reads += 1
        conn = await get_db_connection()
        try:
            rows = await conn.fetch(
                "SELECT bar_start, open, high, low, close, volume "
                "FROM market_bars_4h WHERE symbol = $1 AND is_completed "
                "AND bar_end <= $2 ORDER BY bar_start",
                symbol.upper(), self.snapshot_cutoff_at)
        finally:
            await release_db_connection(conn)
        bars = [
            {"start_utc": r["bar_start"], "open": float(r["open"]),
             "high": float(r["high"]), "low": float(r["low"]),
             "close": float(r["close"]), "volume": float(r["volume"])}
            for r in rows
        ]
        if limit is not None:
            bars = bars[-int(limit):]
        return {"symbol": symbol.upper(), "provider": self.name,
                "multiplier": int(multiplier), "timespan": timespan,
                "requested_start": str(start), "requested_end": str(end),
                "bars": bars, "skipped_malformed": 0, "dropped_exact_duplicates": 0}

    # never used in prospective mode, present for interface completeness
    async def get_daily_bars(self, symbol: str, from_date: str, to_date: str):
        payload = await self.get_daily_history(symbol, timeseries=600)
        return [b for b in payload["historical"] if b["date"] <= to_date]


__all__ = ["LocalHistoryProvider"]
