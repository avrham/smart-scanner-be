"""
Maintenance tasks for Smart Scanner
Cleanup and housekeeping operations
"""

import logging
from datetime import datetime, date, timedelta
import asyncpg

from app.workers.persistence import cleanup_old_daily_seen


logger = logging.getLogger(__name__)


async def clear_daily_seen(db: asyncpg.Connection, target_date: date = None) -> int:
    """Clear daily_seen records for a specific date (default: today)"""
    
    try:
        if target_date is None:
            target_date = date.today()
        
        query = """
            DELETE FROM daily_seen
            WHERE seen_date = $1
        """
        
        result = await db.execute(query, target_date)
        
        # Extract number of deleted rows
        deleted_count = int(result.split()[-1]) if result and result.split() else 0
        
        logger.info(f"Cleared {deleted_count} daily_seen entries for {target_date}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to clear daily_seen: {e}")
        return 0


async def cleanup_daily_seen(db: asyncpg.Connection, days_to_keep: int = 7) -> int:
    """Clean up old daily_seen entries"""
    
    try:
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        query = """
            DELETE FROM daily_seen
            WHERE seen_date < $1
        """
        
        result = await db.execute(query, cutoff_date)
        
        # Extract number of deleted rows
        deleted_count = int(result.split()[-1]) if result and result.split() else 0
        
        logger.info(f"Cleaned up {deleted_count} daily_seen entries older than {cutoff_date}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to cleanup daily_seen: {e}")
        return 0


async def cleanup_old_pattern_runs(db: asyncpg.Connection, days_to_keep: int = 90) -> int:
    """Clean up old pattern run entries"""
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        query = """
            DELETE FROM pattern_runs
            WHERE run_started_at < $1
        """
        
        result = await db.execute(query, cutoff_date)
        
        deleted_count = int(result.split()[-1]) if result and result.split() else 0
        
        logger.info(f"Cleaned up {deleted_count} pattern_runs entries older than {cutoff_date}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to cleanup pattern_runs: {e}")
        return 0


async def cleanup_old_signals(db: asyncpg.Connection, days_to_keep: int = 365) -> int:
    """Clean up very old signal entries (keep 1 year)"""
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        query = """
            DELETE FROM signals
            WHERE created_at < $1
        """
        
        result = await db.execute(query, cutoff_date)
        
        deleted_count = int(result.split()[-1]) if result and result.split() else 0
        
        logger.info(f"Cleaned up {deleted_count} old signals older than {cutoff_date}")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Failed to cleanup old signals: {e}")
        return 0


async def update_ticker_activity_status(db: asyncpg.Connection) -> int:
    """Mark tickers as inactive if they haven't been updated recently"""
    
    try:
        # Mark as inactive if not updated in 30 days
        cutoff_date = datetime.utcnow() - timedelta(days=30)
        
        query = """
            UPDATE tickers
            SET is_active = false
            WHERE updated_at < $1 AND is_active = true
        """
        
        result = await db.execute(query, cutoff_date)
        
        updated_count = int(result.split()[-1]) if result and result.split() else 0
        
        logger.info(f"Marked {updated_count} tickers as inactive")
        return updated_count
        
    except Exception as e:
        logger.error(f"Failed to update ticker activity: {e}")
        return 0


async def get_database_stats(db: asyncpg.Connection) -> dict:
    """Get database statistics for monitoring"""
    
    try:
        stats = {}
        
        # Table row counts
        tables = ["signals", "pattern_runs", "daily_seen", "tickers", "patterns"]
        
        for table in tables:
            query = f"SELECT COUNT(*) as count FROM {table}"
            result = await db.fetchrow(query)
            stats[f"{table}_count"] = result["count"] if result else 0
        
        # Recent activity (last 24 hours)
        recent_query = """
            SELECT COUNT(*) as count
            FROM signals
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        result = await db.fetchrow(recent_query)
        stats["signals_last_24h"] = result["count"] if result else 0
        
        # Pattern run stats (last 7 days)
        pattern_stats_query = """
            SELECT pattern_code,
                   COUNT(*) as runs,
                   SUM(scanned_count) as total_scanned,
                   SUM(enter_count) as total_enter,
                   SUM(rejected_count) as total_rejected
            FROM pattern_runs
            WHERE run_started_at >= NOW() - INTERVAL '7 days'
            GROUP BY pattern_code
        """
        pattern_results = await db.fetch(pattern_stats_query)
        
        stats["pattern_stats_7d"] = [
            {
                "pattern_code": row["pattern_code"],
                "runs": row["runs"],
                "total_scanned": row["total_scanned"],
                "total_enter": row["total_enter"],
                "total_rejected": row["total_rejected"]
            }
            for row in pattern_results
        ]
        
        # Database size estimation
        size_query = """
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """
        
        size_results = await db.fetch(size_query)
        stats["table_sizes"] = [
            {
                "table": row["tablename"],
                "size": row["size"],
                "size_bytes": row["size_bytes"]
            }
            for row in size_results
        ]
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        return {"error": str(e)}


async def run_all_maintenance_tasks(db: asyncpg.Connection) -> dict:
    """Run all maintenance tasks and return summary"""
    
    logger.info("Starting comprehensive maintenance tasks")
    
    summary = {
        "started_at": datetime.utcnow().isoformat(),
        "tasks": {}
    }
    
    try:
        # Cleanup daily_seen
        daily_seen_cleaned = await cleanup_daily_seen(db)
        summary["tasks"]["daily_seen_cleanup"] = {
            "success": True,
            "deleted_count": daily_seen_cleaned
        }
        
        # Cleanup old pattern runs
        pattern_runs_cleaned = await cleanup_old_pattern_runs(db)
        summary["tasks"]["pattern_runs_cleanup"] = {
            "success": True,
            "deleted_count": pattern_runs_cleaned
        }
        
        # Update ticker activity
        tickers_updated = await update_ticker_activity_status(db)
        summary["tasks"]["ticker_activity_update"] = {
            "success": True,
            "updated_count": tickers_updated
        }
        
        # Get database stats
        db_stats = await get_database_stats(db)
        summary["database_stats"] = db_stats
        
        summary["completed_at"] = datetime.utcnow().isoformat()
        summary["success"] = True
        
        logger.info("Comprehensive maintenance tasks completed successfully")
        
    except Exception as e:
        logger.error(f"Maintenance tasks failed: {e}")
        summary["error"] = str(e)
        summary["success"] = False
        summary["completed_at"] = datetime.utcnow().isoformat()
    
    return summary
