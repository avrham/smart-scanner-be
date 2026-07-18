"""
Public API endpoints for Smart Scanner
Read-only endpoints for frontend consumption
"""

from typing import List, Optional
from datetime import datetime, date
from fastapi import APIRouter, Depends, Query
import asyncpg

from app.deps import get_db
from app.models.responses import (
    PatternResponse, SignalResponse, PatternRunResponse
)


router = APIRouter()


@router.get("/patterns", response_model=List[PatternResponse])
async def get_patterns(db: asyncpg.Connection = Depends(get_db)):
    """Get all available patterns with their configurations"""
    
    # Get patterns
    patterns_query = """
        SELECT p.code, p.name, p.description, p.is_enabled, p.created_at
        FROM patterns p
        WHERE p.is_enabled = true
        ORDER BY p.created_at
    """
    patterns = await db.fetch(patterns_query)
    
    result = []
    for pattern in patterns:
        # Get pattern configs
        config_query = """
            SELECT key, value
            FROM pattern_configs 
            WHERE pattern_code = $1
        """
        configs = await db.fetch(config_query, pattern["code"])
        
        config_dict = {config["key"]: config["value"] for config in configs}
        
        result.append({
            "code": pattern["code"],
            "name": pattern["name"],
            "description": pattern["description"],
            "is_enabled": pattern["is_enabled"],
            "created_at": pattern["created_at"],
            "config": config_dict
        })
    
    return result


@router.get("/signals", response_model=List[SignalResponse])
async def get_signals(
    pattern_code: Optional[str] = Query(None, description="Filter by pattern code"),
    limit: int = Query(50, ge=1, le=500, description="Number of signals to return"),
    since: Optional[datetime] = Query(None, description="Return signals created after this time"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get latest ENTER signals (sma150_bounce only shows ENTER signals)"""
    
    query = """
        SELECT s.id, s.symbol, s.pattern_code, s.verdict, s.probability,
               s.score, s.reason, s.details, s.snapshot_date, s.created_at
        FROM signals s
        WHERE s.verdict = 'ENTER'
    """
    
    params = []
    param_count = 0
    
    if pattern_code:
        param_count += 1
        query += f" AND s.pattern_code = ${param_count}"
        params.append(pattern_code)
    
    if since:
        param_count += 1
        query += f" AND s.created_at >= ${param_count}"
        params.append(since)
    
    query += f" ORDER BY s.created_at DESC LIMIT ${param_count + 1}"
    params.append(limit)
    
    signals = await db.fetch(query, *params)
    
    return [
        {
            "id": str(signal["id"]),
            "symbol": signal["symbol"],
            "pattern_code": signal["pattern_code"],
            "verdict": signal["verdict"],
            "probability": float(signal["probability"]) if signal["probability"] else None,
            "score": float(signal["score"]) if signal["score"] else None,
            "reason": signal["reason"],
            "details": signal["details"],
            "snapshot_date": signal["snapshot_date"],
            "created_at": signal["created_at"]
        }
        for signal in signals
    ]


@router.get("/signals/{signal_id}", response_model=SignalResponse)
async def get_signal(
    signal_id: str,
    db: asyncpg.Connection = Depends(get_db)
):
    """Get specific signal details"""
    
    query = """
        SELECT s.id, s.symbol, s.pattern_code, s.verdict, s.probability,
               s.score, s.reason, s.details, s.snapshot_date, s.created_at
        FROM signals s
        WHERE s.id = $1
    """
    
    signal = await db.fetchrow(query, signal_id)
    
    if not signal:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Signal not found")
    
    return {
        "id": str(signal["id"]),
        "symbol": signal["symbol"],
        "pattern_code": signal["pattern_code"],
        "verdict": signal["verdict"],
        "probability": float(signal["probability"]) if signal["probability"] else None,
        "score": float(signal["score"]) if signal["score"] else None,
        "reason": signal["reason"],
        "details": signal["details"],
        "snapshot_date": signal["snapshot_date"],
        "created_at": signal["created_at"]
    }


@router.get("/pattern-runs", response_model=List[PatternRunResponse])
async def get_pattern_runs(
    pattern_code: Optional[str] = Query(None, description="Filter by pattern code"),
    limit: int = Query(20, ge=1, le=100, description="Number of runs to return"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get recent pattern run telemetry"""
    
    query = """
        SELECT pr.id, pr.pattern_code, pr.run_started_at, pr.scanned_count,
               pr.enter_count, pr.rejected_count, pr.notes
        FROM pattern_runs pr
    """
    
    params = []
    if pattern_code:
        query += " WHERE pr.pattern_code = $1"
        params.append(pattern_code)
    
    query += f" ORDER BY pr.run_started_at DESC LIMIT ${len(params) + 1}"
    params.append(limit)
    
    runs = await db.fetch(query, *params)
    
    return [
        {
            "id": str(run["id"]),
            "pattern_code": run["pattern_code"],
            "run_started_at": run["run_started_at"],
            "scanned_count": run["scanned_count"],
            "enter_count": run["enter_count"],
            "rejected_count": run["rejected_count"],
            "notes": run["notes"]
        }
        for run in runs
    ]
