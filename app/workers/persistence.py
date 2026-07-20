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
    snapshot_date: Optional[date] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Persist an IMMUTABLE signal + provenance + scan occurrence link (7B).

    This is the ONLY code path allowed to INSERT INTO signals. Every new
    signal MUST carry a provenance record (built via
    app.workers.provenance.build_provenance).

    Identity: a SHA-256 signal_fingerprint (algorithm signal_fingerprint.v1,
    persisted in signals.signal_fingerprint_version) over the canonical
    decision inputs (symbol, strategy code+version, decision-policy version,
    config hash, snapshot date, market-data as-of, verdict, ORIGINAL
    pre-pruning evidence hash, sorted external observation ids — NO
    scan_run_id). Semantics:

      * new fingerprint  -> INSERT signal + provenance (origin scan) + link
      * known fingerprint-> reuse the existing signal_id; evidence, provenance
        and the ORIGIN scan_run_id are NEVER overwritten; only a new
        scan_run_signals occurrence link is added
      * all writes share ONE transaction (all or nothing)

    Returns {"signal_id", "created_new_signal", "deduplicated",
    "signal_fingerprint", "signal_fingerprint_version"}.
    """
    if not provenance:
        raise ValueError(
            "save_signal requires a provenance record (Phase 7B); "
            "build it with app.workers.provenance.build_provenance"
        )

    from app.workers.provenance import (
        MAX_EVIDENCE_BYTES,
        SIGNAL_FINGERPRINT_VERSION,
        EvidenceTooLargeError,
        canonical_json,
        compute_signal_fingerprint,
    )

    if snapshot_date is None:
        snapshot_date = date.today()

    evidence_snapshot = provenance["evidence_snapshot"]
    # Defensive re-check: an evidence snapshot above the bound must never be
    # persisted, whatever path produced it.
    if len(canonical_json(evidence_snapshot).encode("utf-8")) > MAX_EVIDENCE_BYTES:
        raise EvidenceTooLargeError(
            f"evidence snapshot exceeds {MAX_EVIDENCE_BYTES} bytes; "
            "refusing to persist signal"
        )

    # Identity hashes the ORIGINAL (pre-pruning) evidence: decisions that
    # differ only in optional evidence later pruned for size must still be
    # distinct immutable signals.
    fingerprint = compute_signal_fingerprint(
        symbol=symbol,
        strategy_code=provenance["strategy_code"],
        strategy_version=provenance["strategy_version"],
        decision_policy_version=provenance["decision_policy_version"],
        config_hash_value=provenance["config_hash"],
        snapshot_date=snapshot_date,
        market_data_as_of=provenance.get("market_data_as_of"),
        verdict=verdict,
        evidence_original_sha256=provenance["evidence_original_sha256"],
        external_observation_ids=provenance.get("external_observation_ids"),
    )
    fingerprint_version = SIGNAL_FINGERPRINT_VERSION

    conn = await get_db_connection()

    try:
        signal_id = uuid.uuid4()

        insert_query = """
            INSERT INTO signals (
                id, symbol, pattern_code, verdict, probability, score, 
                reason, details, snapshot_date, created_at,
                signal_fingerprint, signal_fingerprint_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (signal_fingerprint, signal_fingerprint_version)
                WHERE signal_fingerprint IS NOT NULL
            DO NOTHING
            RETURNING id
        """

        async with conn.transaction():
            result = await conn.fetchrow(
                insert_query,
                signal_id,
                symbol,
                pattern_code,
                verdict,
                probability,
                score,
                reason,
                json.dumps(serialize_for_json(details)) if details else None,
                snapshot_date,
                datetime.utcnow(),
                fingerprint,
                fingerprint_version,
            )

            if result is not None:
                # Brand-new immutable signal: write its origin provenance.
                saved_id = result["id"]
                created_new = True
                await _insert_signal_provenance(conn, saved_id, provenance)
            else:
                # Exact repeated fingerprint: reuse the immutable signal.
                # Nothing about it (row, evidence, provenance, origin scan)
                # is modified — only an occurrence link is added below.
                saved_id = await _existing_signal_for_fingerprint(
                    conn, fingerprint, fingerprint_version, provenance
                )
                created_new = False

            await _link_scan_occurrence(conn, saved_id, provenance, created_new)

        logger.info(
            "Saved %s signal for %s (%s) created_new=%s",
            verdict, symbol, pattern_code, created_new,
        )
        return {
            "signal_id": str(saved_id),
            "created_new_signal": created_new,
            "deduplicated": not created_new,
            "signal_fingerprint": fingerprint,
            "signal_fingerprint_version": fingerprint_version,
        }

    except Exception as e:
        logger.error(f"Failed to save signal for {symbol}: {e}")
        raise
    finally:
        await release_db_connection(conn)


async def _existing_signal_for_fingerprint(
    conn: asyncpg.Connection,
    fingerprint: str,
    fingerprint_version: str,
    provenance: Dict[str, Any],
) -> Any:
    """Resolve the existing signal for a repeated fingerprint.

    Deduplication compares BOTH the fingerprint and its algorithm version:
    a v1 identity can never be satisfied by a hypothetical v2 row. Also
    verifies the persisted provenance identity is compatible with the new
    detection instead of silently replacing anything (the fingerprint already
    encodes these values, so a mismatch means data corruption or a hash
    collision — refuse to proceed).
    """
    row = await conn.fetchrow(
        """
        SELECT s.id, sp.strategy_code, sp.strategy_version,
               sp.decision_policy_version, sp.config_hash
        FROM signals s
        LEFT JOIN signal_provenance sp ON sp.signal_id = s.id
        WHERE s.signal_fingerprint = $1
          AND s.signal_fingerprint_version = $2
        """,
        fingerprint,
        fingerprint_version,
    )
    if row is None:
        raise RuntimeError(
            f"signal fingerprint conflict but no existing row found "
            f"({fingerprint[:12]}...)"
        )
    if row["strategy_code"] is not None:  # provenance row present
        for field in ("strategy_code", "strategy_version",
                      "decision_policy_version", "config_hash"):
            if row[field] != provenance[field]:
                raise ValueError(
                    f"fingerprint reuse with incompatible provenance "
                    f"({field}: stored={row[field]!r} new={provenance[field]!r}); "
                    "refusing to overwrite immutable provenance"
                )
    return row["id"]


async def _insert_signal_provenance(
    conn: asyncpg.Connection,
    signal_id: Any,
    provenance: Dict[str, Any],
) -> None:
    """Insert the 1:1 ORIGIN provenance row for a NEW signal (same transaction
    as the signal write — never call outside save_signal's transaction block).

    Plain INSERT by design: provenance records the origin evaluation and is
    never overwritten by later scans (immutability guarantee).
    """
    scan_run_id = provenance.get("scan_run_id")
    await conn.execute(
        """
        INSERT INTO signal_provenance (
            signal_id, scan_run_id, source_path, scanner_mode, provider,
            strategy_code, strategy_version, decision_policy_version,
            provenance_version, config_hash, config_snapshot,
            market_data_as_of, evidence_snapshot,
            evidence_original_sha256, evidence_original_size_bytes,
            evidence_pruned, evidence_pruned_keys,
            external_observation_ids, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, NOW())
        """,
        signal_id,
        uuid.UUID(str(scan_run_id)) if scan_run_id else None,
        provenance["source_path"],
        provenance.get("scanner_mode"),
        provenance.get("provider"),
        provenance["strategy_code"],
        provenance["strategy_version"],
        provenance["decision_policy_version"],
        provenance["provenance_version"],
        provenance["config_hash"],
        json.dumps(serialize_for_json(provenance["config_snapshot"])),
        provenance.get("market_data_as_of"),
        json.dumps(serialize_for_json(provenance["evidence_snapshot"])),
        provenance.get("evidence_original_sha256"),
        provenance.get("evidence_original_size_bytes"),
        bool(provenance.get("evidence_pruned", False)),
        json.dumps(provenance.get("evidence_pruned_keys") or []),
        json.dumps(provenance.get("external_observation_ids") or []),
    )


async def _link_scan_occurrence(
    conn: asyncpg.Connection,
    signal_id: Any,
    provenance: Dict[str, Any],
    created_new: bool,
) -> None:
    """Record that THIS scan detected the signal (origin or re-detection).

    scan_run_signals is the occurrence ledger; provenance.scan_run_id stays
    the origin. Signals persisted outside a scan context (documented 'manual'
    source path) have no scan_run_id and get no link.
    """
    scan_run_id = provenance.get("scan_run_id")
    if not scan_run_id:
        return
    await conn.execute(
        """
        INSERT INTO scan_run_signals (
            scan_run_id, signal_id, source_path, created_new_signal, linked_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (scan_run_id, signal_id) DO NOTHING
        """,
        uuid.UUID(str(scan_run_id)),
        signal_id,
        provenance["source_path"],
        created_new,
    )


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
    is_active: bool = True,
    eligible: Optional[bool] = None,
) -> None:
    """Insert or update ticker information.

    `eligible=None` preserves any stored classification (COALESCE); passing a
    boolean sets it explicitly (the FMP screener refresh marks its rows True so
    the funnel's `eligible = true` universe filter works on both providers).
    """
    conn = await get_db_connection()
    
    try:
        query = """
            INSERT INTO tickers (symbol, name, exchange, market_cap, last_volume, is_active, eligible, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, tickers.name),
                exchange = COALESCE(EXCLUDED.exchange, tickers.exchange),
                market_cap = COALESCE(EXCLUDED.market_cap, tickers.market_cap),
                last_volume = COALESCE(EXCLUDED.last_volume, tickers.last_volume),
                is_active = EXCLUDED.is_active,
                eligible = COALESCE(EXCLUDED.eligible, tickers.eligible),
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
            eligible,
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
    """Get list of candidate tickers for scanning based on filters.

    Legacy path. Rows the universe sync explicitly classified as ineligible
    (eligible = false: warrants/units/ETFs...) are excluded; rows never
    classified (eligible IS NULL, e.g. pre-005 or FMP-screener rows) are kept
    for backward compatibility.
    """
    if exchanges is None:
        exchanges = ["NASDAQ", "NYSE"]

    conn = await get_db_connection()

    try:
        query = """
            SELECT symbol
            FROM tickers
            WHERE is_active = true
              AND eligible IS NOT FALSE
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


def _default_universe_exchanges() -> List[str]:
    """Configured supported exchanges as legacy short names.

    UNIVERSE_ALLOWED_EXCHANGES holds MIC codes (XNAS/XNYS/XASE); the tickers
    cache stores short names (NASDAQ/NYSE/AMEX). Map via the same table the
    universe sync uses; keep unmapped values verbatim.
    """
    from app.config import settings
    from app.workers.screening import MIC_TO_SHORT

    return [MIC_TO_SHORT.get(e.upper(), e.upper()) for e in settings.UNIVERSE_ALLOWED_EXCHANGES]


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

    Only rows the universe sync classified as `eligible = true` enter Stage 0
    (migration 005): warrants/units/ETFs etc. are excluded by the STORED
    classification, never re-inferred from symbol suffixes. Eligible common
    stocks with market_cap NULL are intentionally kept so the funnel can reject
    them honestly as market_cap_unknown until enrichment covers them.
    """
    if exchanges is None:
        exchanges = _default_universe_exchanges()

    conn = await get_db_connection()
    try:
        query = """
            SELECT symbol, market_cap, last_volume, exchange
            FROM tickers
            WHERE is_active = true
              AND eligible = true
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
