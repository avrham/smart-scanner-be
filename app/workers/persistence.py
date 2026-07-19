"""
Database persistence layer for Smart Scanner
Handles saving signals, tracking seen tickers, and logging pattern runs
"""

import logging
import asyncpg
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import uuid
import json
import pandas as pd
import numpy as np

from app.deps import init_db_pool


logger = logging.getLogger(__name__)


async def get_db_connection() -> asyncpg.Connection:
    """Acquire a pooled connection.

    IMPORTANT: callers MUST return it via release_db_connection() (not
    conn.close()). See B11 fix below.
    """
    pool = await init_db_pool()
    return await pool.acquire()


async def release_db_connection(conn: asyncpg.Connection) -> None:
    """Release a pooled connection back to the pool.

    Fixes B11: callers previously called conn.close() on pooled connections,
    which destroyed them instead of returning them, degrading/exhausting the
    pool over time. pool.release() returns the connection to the pool.
    """
    if conn is None:
        return
    try:
        pool = await init_db_pool()
        await pool.release(conn)
    except Exception as exc:  # last-resort: don't leak on release failure
        logger.warning("Failed to release DB connection cleanly: %s", exc)
        try:
            await conn.close()
        except Exception:
            pass


def serialize_for_json(obj):
    """Custom JSON serializer for pandas/numpy objects"""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, (pd.Timedelta, np.timedelta64)):
        return str(obj)
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(item) for item in obj]
    return obj


async def save_signal(
    symbol: str,
    pattern_code: str,
    verdict: str,
    score: Optional[float] = None,
    probability: Optional[float] = None,
    reason: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    snapshot_date: Optional[date] = None
) -> str:
    """
    Save a signal to the database
    Returns the signal ID
    """
    conn = await get_db_connection()
    
    try:
        if snapshot_date is None:
            snapshot_date = date.today()
        
        signal_id = uuid.uuid4()
        
        query = """
            INSERT INTO signals (
                id, symbol, pattern_code, verdict, probability, score, 
                reason, details, snapshot_date, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, pattern_code, snapshot_date) 
            DO UPDATE SET
                verdict = EXCLUDED.verdict,
                probability = EXCLUDED.probability,
                score = EXCLUDED.score,
                reason = EXCLUDED.reason,
                details = EXCLUDED.details,
                created_at = EXCLUDED.created_at
            RETURNING id
        """
        
        result = await conn.fetchrow(
            query,
            signal_id,
            symbol,
            pattern_code,
            verdict,
            probability,
            score,
            reason,
            json.dumps(serialize_for_json(details)) if details else None,
            snapshot_date,
            datetime.utcnow()
        )
        
        logger.info(f"Saved {verdict} signal for {symbol} ({pattern_code})")
        return str(result["id"] if result else signal_id)
        
    except Exception as e:
        logger.error(f"Failed to save signal for {symbol}: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def was_seen_today(symbol: str, check_date: date = None) -> bool:
    """Check if symbol was already scanned today"""
    if check_date is None:
        check_date = date.today()
    
    conn = await get_db_connection()
    
    try:
        query = """
            SELECT 1 FROM daily_seen 
            WHERE symbol = $1 AND seen_date = $2
        """
        
        result = await conn.fetchrow(query, symbol, check_date)
        return result is not None
        
    except Exception as e:
        logger.error(f"Failed to check if {symbol} was seen: {e}")
        return False
    finally:
        await release_db_connection(conn)


async def mark_seen_today(symbol: str, check_date: date = None) -> None:
    """Mark symbol as seen today to avoid duplicate scans"""
    if check_date is None:
        check_date = date.today()
    
    conn = await get_db_connection()
    
    try:
        query = """
            INSERT INTO daily_seen (symbol, seen_date)
            VALUES ($1, $2)
            ON CONFLICT (symbol, seen_date) DO NOTHING
        """
        
        await conn.execute(query, symbol, check_date)
        logger.debug(f"Marked {symbol} as seen on {check_date}")
        
    except Exception as e:
        logger.error(f"Failed to mark {symbol} as seen: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def log_pattern_run(
    pattern_code: str,
    scanned_count: int,
    enter_count: int,
    rejected_count: int,
    notes: Optional[str] = None,
    run_started_at: Optional[datetime] = None
) -> str:
    """Log pattern run telemetry"""
    if run_started_at is None:
        run_started_at = datetime.utcnow()
    
    conn = await get_db_connection()
    
    try:
        run_id = uuid.uuid4()
        
        query = """
            INSERT INTO pattern_runs (
                id, pattern_code, run_started_at, scanned_count,
                enter_count, rejected_count, notes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        """
        
        result = await conn.fetchrow(
            query,
            run_id,
            pattern_code,
            run_started_at,
            scanned_count,
            enter_count,
            rejected_count,
            notes
        )
        
        logger.info(
            f"Logged {pattern_code} run: scanned={scanned_count}, "
            f"enter={enter_count}, rejected={rejected_count}"
        )
        
        return str(result["id"])
        
    except Exception as e:
        logger.error(f"Failed to log pattern run: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def get_pattern_config(pattern_code: str) -> Dict[str, Any]:
    """Get configuration for a pattern"""
    conn = await get_db_connection()
    
    try:
        query = """
            SELECT key, value
            FROM pattern_configs
            WHERE pattern_code = $1
        """
        
        configs = await conn.fetch(query, pattern_code)
        
        config = {}
        for row in configs:
            config[row["key"]] = row["value"]
        
        return config
        
    except Exception as e:
        logger.error(f"Failed to get config for {pattern_code}: {e}")
        return {}
    finally:
        await release_db_connection(conn)


async def batch_upsert_tickers(tickers_data: List[Dict]) -> None:
    """Batch insert or update ticker information"""
    if not tickers_data:
        return
        
    conn = await get_db_connection()
    
    try:
        # Create temporary table for batch operations
        await conn.execute("""
            CREATE TEMP TABLE temp_tickers (
                symbol VARCHAR(10) PRIMARY KEY,
                name VARCHAR(255),
                exchange VARCHAR(10),
                market_cap DECIMAL,
                last_volume DECIMAL,
                is_active BOOLEAN,
                updated_at TIMESTAMP
            ) ON COMMIT DROP
        """)
        
        # Insert data into temp table
        for ticker in tickers_data:
            await conn.execute("""
                INSERT INTO temp_tickers (symbol, name, exchange, market_cap, last_volume, is_active, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, 
                ticker['symbol'],
                ticker['name'],
                ticker['exchange'],
                ticker['market_cap'],
                ticker['last_volume'],
                ticker['is_active'],
                datetime.utcnow()
            )
        
        # Upsert from temp table to main table
        await conn.execute("""
            INSERT INTO tickers (symbol, name, exchange, market_cap, last_volume, is_active, updated_at)
            SELECT symbol, name, exchange, market_cap, last_volume, is_active, updated_at
            FROM temp_tickers
            ON CONFLICT (symbol) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, tickers.name),
                exchange = COALESCE(EXCLUDED.exchange, tickers.exchange),
                market_cap = COALESCE(EXCLUDED.market_cap, tickers.market_cap),
                last_volume = COALESCE(EXCLUDED.last_volume, tickers.last_volume),
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
        """)
        
    except Exception as e:
        logger.error(f"Failed to batch upsert tickers: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def upsert_ticker(
    symbol: str,
    name: Optional[str] = None,
    exchange: Optional[str] = None,
    market_cap: Optional[float] = None,
    last_volume: Optional[float] = None,
    is_active: bool = True
) -> None:
    """Insert or update ticker information"""
    conn = await get_db_connection()
    
    try:
        query = """
            INSERT INTO tickers (symbol, name, exchange, market_cap, last_volume, is_active, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (symbol) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, tickers.name),
                exchange = COALESCE(EXCLUDED.exchange, tickers.exchange),
                market_cap = COALESCE(EXCLUDED.market_cap, tickers.market_cap),
                last_volume = COALESCE(EXCLUDED.last_volume, tickers.last_volume),
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at
        """
        
        await conn.execute(
            query,
            symbol,
            name,
            exchange,
            market_cap,
            last_volume,
            is_active,
            datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Failed to upsert ticker {symbol}: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def get_candidate_tickers(
    min_market_cap: float = 200_000_000,
    min_volume: float = 200_000,
    exchanges: list = None
) -> list[str]:
    """Get list of candidate tickers for scanning based on filters"""
    if exchanges is None:
        exchanges = ["NASDAQ", "NYSE"]
    
    conn = await get_db_connection()
    
    try:
        query = """
            SELECT symbol
            FROM tickers
            WHERE is_active = true
              AND market_cap >= $1
              AND last_volume >= $2
              AND exchange = ANY($3)
            ORDER BY market_cap DESC
        """
        
        results = await conn.fetch(query, min_market_cap, min_volume, exchanges)
        return [row["symbol"] for row in results]
        
    except Exception as e:
        logger.error(f"Failed to get candidate tickers: {e}")
        return []
    finally:
        await release_db_connection(conn)


async def get_universe_tickers(
    exchanges: list = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Load raw ticker rows (symbol, market_cap, last_volume) for the funnel.

    Unlike get_candidate_tickers (which pre-filters and returns symbols only),
    this returns the raw cached values INCLUDING NULLs so the funnel can classify
    rejections precisely (market_cap_unknown vs below_min, volume_unknown vs
    below_min). Never fabricates values. Ordered by market cap desc so a bounded
    validation run keeps the most liquid names.
    """
    if exchanges is None:
        exchanges = ["NASDAQ", "NYSE", "AMEX"]

    conn = await get_db_connection()
    try:
        query = """
            SELECT symbol, market_cap, last_volume, exchange
            FROM tickers
            WHERE is_active = true
              AND exchange = ANY($1)
            ORDER BY market_cap DESC NULLS LAST
            LIMIT $2
        """
        rows = await conn.fetch(query, exchanges, limit)
        return [
            {
                "symbol": r["symbol"],
                "market_cap": float(r["market_cap"]) if r["market_cap"] is not None else None,
                "last_volume": float(r["last_volume"]) if r["last_volume"] is not None else None,
                "exchange": r["exchange"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"Failed to load universe tickers: {e}")
        return []
    finally:
        await release_db_connection(conn)


async def cleanup_old_daily_seen(days_to_keep: int = 7) -> int:
    """Clean up old daily_seen entries"""
    conn = await get_db_connection()
    
    try:
        query = """
            DELETE FROM daily_seen
            WHERE seen_date < CURRENT_DATE - INTERVAL '%s days'
        """ % days_to_keep
        
        result = await conn.execute(query)
        
        # Extract number of deleted rows from result
        deleted_count = int(result.split()[-1]) if result else 0
        
        logger.info(f"Cleaned up {deleted_count} old daily_seen entries")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to cleanup daily_seen: {e}")
        return 0
    finally:
        await release_db_connection(conn)


async def get_signals_count_today(pattern_code: str = None) -> Dict[str, int]:
    """Get count of signals generated today"""
    conn = await get_db_connection()
    
    try:
        query = """
            SELECT verdict, COUNT(*) as count
            FROM signals
            WHERE DATE(created_at) = CURRENT_DATE
        """
        
        params = []
        if pattern_code:
            query += " AND pattern_code = $1"
            params.append(pattern_code)
        
        query += " GROUP BY verdict"
        
        results = await conn.fetch(query, *params)
        
        counts = {"ENTER": 0, "AVOID": 0}
        for row in results:
            counts[row["verdict"]] = row["count"]
        
        return counts
        
    except Exception as e:
        logger.error(f"Failed to get signals count: {e}")
        return {"ENTER": 0, "AVOID": 0}
    finally:
        await release_db_connection(conn)
