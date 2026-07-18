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


class SignalResponse(BaseModel):
    """Signal response model"""
    id: str
    symbol: str
    pattern_code: str
    verdict: str  # 'ENTER' | 'AVOID'
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
