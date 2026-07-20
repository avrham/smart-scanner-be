"""
Public API endpoints for Smart Scanner
Read-only endpoints for frontend consumption
"""

import json
import uuid as uuid_lib
from typing import Any, List, Optional, Tuple
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query
import asyncpg

from app.deps import get_db
from app.models.responses import (
    PatternResponse, SignalResponse, PatternRunResponse
)


router = APIRouter()

# Phase 6: candidate filters. 'ALL' means decision-support candidates
# (ENTER + WATCH) — never debug AVOID rows.
VALID_VERDICTS = {"ENTER", "WATCH", "ALL"}
VALID_SIDES = {"LONG", "SHORT"}


def build_signals_query(
    verdict: str = "ENTER",
    pattern_code: Optional[str] = None,
    side: Optional[str] = None,
    min_score: Optional[float] = None,
    since: Optional[datetime] = None,
    limit: int = 50,
    scan_run_id: Optional[Any] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    config_hash: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    """Build the signals SELECT with filters. Pure — unit-testable without DB.

    Default verdict='ENTER' preserves the pre-Phase-6 endpoint behavior.
    `side` filters on details->>'side' (set by strategies that define direction).

    Phase 7B: the provenance filters compose with the existing filters using
    AND semantics and only add joins when used, so the default query (and
    legacy signals) are unaffected.

    scan_run_id filters through the scan_run_signals OCCURRENCE table — it
    returns every signal that scan detected, including immutable signals
    originally created by an earlier scan (not only signals whose origin
    provenance points at that run).
    """
    where = []
    params: List[Any] = []

    if verdict == "ALL":
        where.append("s.verdict IN ('ENTER', 'WATCH')")
    else:
        params.append(verdict)
        where.append(f"s.verdict = ${len(params)}")

    if pattern_code:
        params.append(pattern_code)
        where.append(f"s.pattern_code = ${len(params)}")

    if side:
        params.append(side)
        where.append(f"s.details->>'side' = ${len(params)}")

    if min_score is not None:
        params.append(min_score)
        where.append(f"s.score >= ${len(params)}")

    if since:
        params.append(since)
        where.append(f"s.created_at >= ${len(params)}")

    join = ""

    if scan_run_id is not None:
        join += " JOIN scan_run_signals srs ON srs.signal_id = s.id"
        params.append(scan_run_id)
        where.append(f"srs.scan_run_id = ${len(params)}")

    provenance_filters = (
        ("strategy_version", strategy_version),
        ("decision_policy_version", decision_policy_version),
        ("config_hash", config_hash),
    )
    if any(v is not None for _, v in provenance_filters):
        join += " JOIN signal_provenance sp ON sp.signal_id = s.id"
    for column, value in provenance_filters:
        if value is not None:
            params.append(value)
            where.append(f"sp.{column} = ${len(params)}")

    params.append(limit)
    query = f"""
        SELECT s.id, s.symbol, s.pattern_code, s.verdict, s.probability,
               s.score, s.reason, s.details, s.snapshot_date, s.created_at
        FROM signals s{join}
        WHERE {' AND '.join(where)}
        ORDER BY s.created_at DESC LIMIT ${len(params)}
    """
    return query, params


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
    verdict: str = Query("ENTER", description="'ENTER' | 'WATCH' | 'ALL' (ENTER+WATCH)"),
    side: Optional[str] = Query(None, description="'LONG' | 'SHORT' (details.side)"),
    min_score: Optional[float] = Query(None, ge=0, le=1, description="Minimum score"),
    limit: int = Query(50, ge=1, le=500, description="Number of signals to return"),
    since: Optional[datetime] = Query(None, description="Return signals created after this time"),
    scan_run_id: Optional[str] = Query(None, description="Filter by canonical scan run (provenance)"),
    strategy_version: Optional[str] = Query(None, description="Filter by exact strategy version (provenance)"),
    decision_policy_version: Optional[str] = Query(None, description="Filter by decision-policy version (provenance)"),
    config_hash: Optional[str] = Query(None, description="Filter by configuration hash (provenance)"),
    db: asyncpg.Connection = Depends(get_db)
):
    """Get candidate signals.

    Default (verdict='ENTER') preserves the original behavior. Phase 6 adds
    WATCH candidates (valid setups awaiting a trigger) and filters for the
    decision UI. AVOID/debug rows are never returned. Phase 7B adds additive
    provenance filters (AND semantics); they only match signals that HAVE a
    provenance row (legacy rows are excluded by these filters by design).
    """
    verdict = (verdict or "ENTER").upper()
    if verdict not in VALID_VERDICTS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid verdict '{verdict}'. Allowed: {sorted(VALID_VERDICTS)}",
        )
    if side:
        side = side.upper()
        if side not in VALID_SIDES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid side '{side}'. Allowed: {sorted(VALID_SIDES)}",
            )

    scan_run_uuid = None
    if scan_run_id:
        try:
            scan_run_uuid = uuid_lib.UUID(scan_run_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail=f"Invalid scan_run_id '{scan_run_id}'")

    query, params = build_signals_query(
        verdict=verdict,
        pattern_code=pattern_code,
        side=side,
        min_score=min_score,
        since=since,
        limit=limit,
        scan_run_id=scan_run_uuid,
        strategy_version=strategy_version,
        decision_policy_version=decision_policy_version,
        config_hash=config_hash,
    )
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


def _as_json(value: Any) -> Any:
    """asyncpg may return JSONB as str depending on codec config."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


@router.get("/signals/{signal_id}/provenance")
async def get_signal_provenance(
    signal_id: str,
    db: asyncpg.Connection = Depends(get_db)
):
    """Complete provenance for one signal (Phase 7B).

    Signals persisted before Phase 7B have no provenance row: they are
    returned successfully with provenance_status='legacy_unlinked' and NULL
    provenance fields — nothing is inferred or fabricated for them.
    """
    try:
        signal_uuid = uuid_lib.UUID(signal_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Signal not found")

    signal = await db.fetchrow(
        "SELECT id, signal_fingerprint, signal_fingerprint_version "
        "FROM signals WHERE id = $1",
        signal_uuid,
    )
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    prov = await db.fetchrow(
        """
        SELECT signal_id, scan_run_id, source_path, scanner_mode, provider,
               strategy_code, strategy_version, decision_policy_version,
               provenance_version, config_hash, config_snapshot,
               market_data_as_of, evidence_snapshot,
               evidence_original_sha256, evidence_original_size_bytes,
               evidence_pruned, evidence_pruned_keys,
               external_observation_ids, created_at
        FROM signal_provenance
        WHERE signal_id = $1
        """,
        signal_uuid,
    )

    if not prov:
        return {
            "provenance_status": "legacy_unlinked",
            "signal_id": str(signal_uuid),
            # Legacy rows keep both identity fields NULL — never fabricated.
            "signal_fingerprint": signal["signal_fingerprint"],
            "signal_fingerprint_version": signal["signal_fingerprint_version"],
            "scan_run_id": None,
            "source_path": None,
            "scanner_mode": None,
            "provider": None,
            "strategy_code": None,
            "strategy_version": None,
            "decision_policy_version": None,
            "provenance_version": None,
            "config_hash": None,
            "config_snapshot": None,
            "market_data_as_of": None,
            "evidence_snapshot": None,
            "external_observation_ids": None,
            "created_at": None,
        }

    return {
        "provenance_status": "linked",
        "signal_id": str(prov["signal_id"]),
        "signal_fingerprint": signal["signal_fingerprint"],
        "signal_fingerprint_version": signal["signal_fingerprint_version"],
        "scan_run_id": str(prov["scan_run_id"]) if prov["scan_run_id"] else None,
        "source_path": prov["source_path"],
        "scanner_mode": prov["scanner_mode"],
        "provider": prov["provider"],
        "strategy_code": prov["strategy_code"],
        "strategy_version": prov["strategy_version"],
        "decision_policy_version": prov["decision_policy_version"],
        "provenance_version": prov["provenance_version"],
        "config_hash": prov["config_hash"],
        "config_snapshot": _as_json(prov["config_snapshot"]),
        "market_data_as_of": prov["market_data_as_of"],
        "evidence_snapshot": _as_json(prov["evidence_snapshot"]),
        "evidence_original_sha256": prov["evidence_original_sha256"],
        "evidence_original_size_bytes": prov["evidence_original_size_bytes"],
        "evidence_pruned": prov["evidence_pruned"],
        "evidence_pruned_keys": _as_json(prov["evidence_pruned_keys"]),
        "external_observation_ids": _as_json(prov["external_observation_ids"]),
        "created_at": prov["created_at"],
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
