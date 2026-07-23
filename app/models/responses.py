"""
Pydantic response models for Smart Scanner API
"""

from typing import Optional, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel


class PatternResponse(BaseModel):
    """Pattern information response"""
    code: str
    name: str
    description: Optional[str] = None
    is_enabled: bool
    created_at: datetime
    config: Dict[str, Any]


class StrategyDiscoveryResponse(BaseModel):
    """Admin read-only strategy discovery (Phase 9C3).

    Distinguishes registry availability (`registered`) from runtime enablement
    (`enabled` from patterns.is_enabled). Missing DB rows yield enabled=null
    and config_status=missing_pattern_row — never treated as enabled.
    """

    pattern_code: str
    registered: bool
    enabled: Optional[bool] = None
    db_configured: bool
    config_status: str
    name: Optional[str] = None
    description: Optional[str] = None
    strategy_version: Optional[str] = None
    decision_policy_version: Optional[str] = None
    allow_enter: Optional[bool] = None
    enable_4h_trigger: Optional[bool] = None
    min_price: Optional[float] = None
    effective_config: Dict[str, Any]


class SignalResponse(BaseModel):
    """Signal response model"""
    id: str
    symbol: str
    pattern_code: str
    verdict: str  # 'ENTER' | 'WATCH' | 'AVOID' (debug only)
    probability: Optional[float] = None
    score: Optional[float] = None
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    snapshot_date: date
    created_at: datetime


class PatternRunResponse(BaseModel):
    """Pattern run telemetry response"""
    id: str
    pattern_code: str
    run_started_at: datetime
    scanned_count: int
    enter_count: int
    rejected_count: int
    notes: Optional[str] = None
