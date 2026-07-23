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


class StrategyDryRunResponse(BaseModel):
    """Explicit persistence-free strategy dry-run result (Phase 9D1).

    `persisted` is always False. `status` is one of the typed terminal
    dry-run statuses (evaluated / frame_rejected / provider_error /
    strategy_error); non-evaluated statuses carry a bounded
    `error_reason_code` instead of decision fields. Rollout flags reflect
    the resolved configuration — the dry-run never changes them.
    """

    dry_run_contract_version: str
    persisted: bool
    status: str
    error_reason_code: Optional[str] = None
    pattern_code: str
    symbol: str
    provider: Optional[str] = None
    evaluation_time_utc: str
    registered: bool
    enabled: Optional[bool] = None
    db_configured: bool
    config_status: str
    strategy_version: Optional[str] = None
    decision_policy_version: Optional[str] = None
    rollout_flags: Dict[str, Any]
    requested_history_bars: int
    frame: Optional[Dict[str, Any]] = None
    decision: Optional[str] = None
    score: Optional[float] = None
    side: Optional[str] = None
    reason: Optional[str] = None
    rejection_reason: Optional[str] = None
    setup_type: Optional[str] = None
    entry_price: Optional[float] = None
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    invalidation: Optional[float] = None
    trigger: Optional[Dict[str, Any]] = None
    readiness_status: Optional[str] = None
    insufficient_data: Optional[bool] = None
    rollout_blocked: Optional[bool] = None
    enter_eligible_without_rollout_gate: Optional[bool] = None
    evidence: Optional[Dict[str, Any]] = None
    details_snapshot: Optional[Dict[str, Any]] = None
    details_original_sha256: Optional[str] = None


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
