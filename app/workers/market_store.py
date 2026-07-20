"""DB persistence for provider market data (universe rows + daily bars).

Thin async layer over the extended `tickers` table and the `daily_bars` table
(migration 005). Reuses the pooled-connection helpers so the B11 release
discipline is preserved. All upserts are idempotent.
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

from app.workers.persistence import get_db_connection, release_db_connection


logger = logging.getLogger(__name__)


UPSERT_UNIVERSE_SQL = """
    INSERT INTO tickers (
        symbol, name, exchange, market, locale, primary_exchange, security_type,
        currency, cik, composite_figi, share_class_figi, is_active, eligible,
        provider_updated_at, last_synced_at, updated_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)
    ON CONFLICT (symbol) DO UPDATE SET
        name = COALESCE(EXCLUDED.name, tickers.name),
        exchange = COALESCE(EXCLUDED.exchange, tickers.exchange),
        market = EXCLUDED.market,
        locale = EXCLUDED.locale,
        primary_exchange = EXCLUDED.primary_exchange,
        security_type = EXCLUDED.security_type,
        currency = EXCLUDED.currency,
        cik = EXCLUDED.cik,
        composite_figi = EXCLUDED.composite_figi,
        share_class_figi = EXCLUDED.share_class_figi,
        is_active = EXCLUDED.is_active,
        eligible = EXCLUDED.eligible,
        provider_updated_at = EXCLUDED.provider_updated_at,
        last_synced_at = EXCLUDED.last_synced_at,
        updated_at = EXCLUDED.updated_at
"""

UPSERT_DAILY_BAR_SQL = """
    INSERT INTO daily_bars (
        symbol, trading_date, open, high, low, close, volume, vwap,
        transaction_count, source
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (symbol, trading_date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        vwap = EXCLUDED.vwap,
        transaction_count = EXCLUDED.transaction_count,
        source = EXCLUDED.source
"""


async def bulk_upsert_universe(rows: List[Dict[str, Any]]) -> int:
    """Idempotent upsert of universe reference rows. Returns count written."""
    if not rows:
        return 0
    now = datetime.utcnow()
    args = [
        (
            r["symbol"], r.get("name"), r.get("exchange"), r.get("market"),
            r.get("locale"), r.get("primary_exchange"), r.get("security_type"),
            r.get("currency"), r.get("cik"), r.get("composite_figi"),
            r.get("share_class_figi"), bool(r.get("is_active", True)),
            r.get("eligible"), r.get("provider_updated_at"), now,
        )
        for r in rows
    ]
    conn = await get_db_connection()
    try:
        await conn.executemany(UPSERT_UNIVERSE_SQL, args)
        return len(args)
    finally:
        await release_db_connection(conn)


async def bulk_upsert_daily_bars(bars: List[Dict[str, Any]], source: str = "massive") -> int:
    """Idempotent upsert of canonical daily bars keyed on (symbol, trading_date)."""
    if not bars:
        return 0
    args = [
        (
            b["symbol"], b["trading_date"], b["open"], b["high"], b["low"],
            b["close"], b["volume"], b.get("vwap"), b.get("transaction_count"),
            source,
        )
        for b in bars
    ]
    conn = await get_db_connection()
    try:
        await conn.executemany(UPSERT_DAILY_BAR_SQL, args)
        return len(args)
    finally:
        await release_db_connection(conn)


async def update_last_volumes_from_bars(trading_date: date) -> int:
    """Propagate a day's real volumes into tickers.last_volume (for the funnel)."""
    conn = await get_db_connection()
    try:
        result = await conn.execute(
            """
            UPDATE tickers t
            SET last_volume = b.volume, updated_at = NOW()
            FROM daily_bars b
            WHERE b.symbol = t.symbol AND b.trading_date = $1
            """,
            trading_date,
        )
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError, AttributeError):
            return 0
    finally:
        await release_db_connection(conn)


async def get_bars_for_date(trading_date: date) -> List[Dict[str, Any]]:
    """All stored canonical bars for one trading date."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT symbol, trading_date, open, high, low, close, volume, vwap,
                   transaction_count
            FROM daily_bars WHERE trading_date = $1
            """,
            trading_date,
        )
        return [dict(r) for r in rows]
    finally:
        await release_db_connection(conn)


async def get_eligible_symbols() -> Set[str]:
    """Symbols currently classified as universe-eligible."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            "SELECT symbol FROM tickers WHERE eligible = true AND is_active = true"
        )
        return {r["symbol"] for r in rows}
    finally:
        await release_db_connection(conn)


async def get_ticker_profiles(symbols: List[str]) -> List[Dict[str, Any]]:
    """Profile cache state for the given symbols (for enrichment planning)."""
    if not symbols:
        return []
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT symbol, market_cap, profile_synced_at, enrichment_status
            FROM tickers WHERE symbol = ANY($1)
            """,
            symbols,
        )
        return [dict(r) for r in rows]
    finally:
        await release_db_connection(conn)


async def update_ticker_profile(
    symbol: str,
    market_cap: Optional[float],
    enrichment_status: str,
) -> None:
    """Store enrichment results. Missing market cap stays NULL + status flag."""
    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            UPDATE tickers
            SET market_cap = $2, enrichment_status = $3,
                profile_synced_at = NOW(), updated_at = NOW()
            WHERE symbol = $1
            """,
            symbol,
            market_cap,
            enrichment_status,
        )
    finally:
        await release_db_connection(conn)


async def get_local_daily_bars(symbol: str, limit: int = 600) -> List[Dict[str, Any]]:
    """Most recent locally stored bars for one symbol (ascending order)."""
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT symbol, trading_date, open, high, low, close, volume, vwap,
                   transaction_count
            FROM daily_bars WHERE symbol = $1
            ORDER BY trading_date DESC LIMIT $2
            """,
            symbol,
            limit,
        )
        return [dict(r) for r in reversed(rows)]
    finally:
        await release_db_connection(conn)


async def get_latest_bar_date(symbol: str) -> Optional[date]:
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT MAX(trading_date) AS d FROM daily_bars WHERE symbol = $1", symbol
        )
        return row["d"] if row else None
    finally:
        await release_db_connection(conn)


async def get_ticker_counts() -> Dict[str, int]:
    """Universe counts for coverage reporting (local DB only)."""
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE is_active = true) AS active,
                   COUNT(*) FILTER (WHERE eligible = true AND is_active = true) AS eligible
            FROM tickers
            """
        )
        return {
            "total": row["total"] if row else 0,
            "active": row["active"] if row else 0,
            "eligible": row["eligible"] if row else 0,
        }
    finally:
        await release_db_connection(conn)


async def get_latest_daily_bar_date() -> Optional[date]:
    """Most recent trading date with locally stored bars (any symbol)."""
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow("SELECT MAX(trading_date) AS d FROM daily_bars")
        return row["d"] if row else None
    finally:
        await release_db_connection(conn)


async def get_provider_sync_status() -> Dict[str, Any]:
    """Sync freshness for the health endpoint. Tolerates a missing migration."""
    status: Dict[str, Any] = {
        "latest_universe_sync": None,
        "latest_daily_bar_date": None,
        "universe_size": None,
        "eligible_count": None,
    }
    conn = await get_db_connection()
    try:
        try:
            row = await conn.fetchrow(
                """
                SELECT MAX(last_synced_at) AS latest_sync,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE eligible = true) AS eligible
                FROM tickers
                """
            )
            if row:
                status["latest_universe_sync"] = (
                    row["latest_sync"].isoformat() if row["latest_sync"] else None
                )
                status["universe_size"] = row["total"]
                status["eligible_count"] = row["eligible"]
        except Exception:
            pass  # migration 005 not applied yet
        try:
            row = await conn.fetchrow("SELECT MAX(trading_date) AS d FROM daily_bars")
            if row and row["d"]:
                status["latest_daily_bar_date"] = str(row["d"])
        except Exception:
            pass
        return status
    finally:
        await release_db_connection(conn)
