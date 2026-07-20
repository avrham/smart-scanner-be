"""
Scheduler for automated pattern scanning
Handles cron-like scheduling of scan batches
"""

import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.workers.scan_runner import run_scan_batch, run_maintenance_tasks
from app.providers import get_market_data_provider
from app.config import settings


logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = None


async def scheduled_scan_job():
    """Scheduled scan job that runs at specified times"""
    logger.info("Starting scheduled scan job")
    
    try:
        # Use the configured MarketDataProvider (massive default, fmp fallback).
        provider = get_market_data_provider()
        
        # Run scan batch
        result = await run_scan_batch(
            fmp=provider,
            batch_size=settings.SCAN_BATCH_SIZE,
            pattern_code="sma150_bounce"
        )
        
        if result["success"]:
            logger.info(
                f"Scheduled scan completed: {result['scanned_count']} scanned, "
                f"{result['enter_count']} signals, {result['rejected_count']} rejected"
            )
        else:
            logger.error(f"Scheduled scan failed: {result.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Scheduled scan job failed: {e}")


async def scheduled_maintenance_job():
    """Scheduled maintenance job"""
    logger.info("Starting scheduled maintenance job")
    
    try:
        await run_maintenance_tasks()
        logger.info("Scheduled maintenance completed")
        
    except Exception as e:
        logger.error(f"Scheduled maintenance failed: {e}")


def start_scheduler():
    """Start the background scheduler.

    This in-process APScheduler is the SINGLE authoritative scheduler (B6).
    The external docker curl-loop trigger was removed to avoid duplicate runs.
    Jobs use max_instances=1 + coalesce=True to further guard against overlap.
    """
    global scheduler
    
    if scheduler is not None:
        logger.warning("Scheduler already running; refusing to start a second instance")
        return
    
    logger.info("Starting scheduler (single authoritative source for scans)")
    
    scheduler = AsyncIOScheduler()
    
    # Add scan jobs based on configured times
    for scan_time in settings.SCAN_TIMES:
        hour, minute = scan_time.split(":")
        
        scheduler.add_job(
            scheduled_scan_job,
            trigger=CronTrigger(hour=int(hour), minute=int(minute), timezone="UTC"),
            id=f"scan_{scan_time}",
            name=f"Scan Job {scan_time} UTC",
            max_instances=1,  # Prevent overlapping runs
            coalesce=True     # If missed, run once when possible
        )
        
        logger.info(f"Scheduled scan job for {scan_time} UTC")
    
    # Add daily maintenance job (run at 01:00 UTC)
    scheduler.add_job(
        scheduled_maintenance_job,
        trigger=CronTrigger(hour=1, minute=0, timezone="UTC"),
        id="daily_maintenance",
        name="Daily Maintenance",
        max_instances=1,
        coalesce=True
    )
    
    logger.info("Scheduled daily maintenance for 01:00 UTC")
    
    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started successfully")


def stop_scheduler():
    """Stop the background scheduler"""
    global scheduler
    
    if scheduler is None:
        logger.warning("Scheduler not running")
        return
    
    logger.info("Stopping scheduler")
    scheduler.shutdown(wait=True)
    scheduler = None
    logger.info("Scheduler stopped")


def get_scheduler_status():
    """Get current scheduler status and job information"""
    global scheduler
    
    if scheduler is None:
        return {
            "running": False,
            "jobs": [],
            "next_run": None
        }
    
    jobs = []
    next_run = None
    
    for job in scheduler.get_jobs():
        job_info = {
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        }
        
        jobs.append(job_info)
        
        # Track earliest next run
        if job.next_run_time:
            if next_run is None or job.next_run_time < next_run:
                next_run = job.next_run_time
    
    return {
        "running": True,
        "jobs": jobs,
        "next_run": next_run.isoformat() if next_run else None
    }


async def trigger_manual_scan():
    """Trigger a manual scan job (for testing/admin use)"""
    logger.info("Triggering manual scan")
    
    try:
        await scheduled_scan_job()
        return {"success": True, "message": "Manual scan completed"}
        
    except Exception as e:
        logger.error(f"Manual scan failed: {e}")
        return {"success": False, "error": str(e)}
