"""PURE server-enforced pacing between provider-backed maintenance batches.

The Massive Basic plan allows only ~5 requests/minute. A single 3-pair batch
consumes roughly five cache-cold provider requests (three pairs + the shared
SPY/QQQ benchmarks), which exhausts the minute budget; a second batch started
seconds later is throttled (HTTP 429) and every pair fails with a retryable
`forward_fetch_error`. The safety signal is therefore the provider's ROLLING
request window, NOT the observed response duration — a cache-warm batch can
finish in ~2s yet still leave the rate-limit window fully spent.

This module is PURE: it never touches the database, a provider or a token. The
endpoint layer supplies the latest persisted maintenance outcome-run row (see
`strategy_shadow_outcome_runs`) and the resolved interval; this module decides
whether enough wall-clock time has elapsed since that run's reference timestamp.
Because the decision is derived from a persisted row, the cooldown survives
process restart, Fly auto-stop, Machine replacement and worker-token rotation —
it is never held only in process memory.

Timestamp precedence (documented, deterministic): finished_at -> updated_at ->
started_at -> created_at. A batch that ended as failed / error-dominated / 429
still has one of these set, so a failed batch establishes cooldown exactly like
a successful one. When a maintenance run is clearly identifiable but somehow
carries no usable timestamp, the safe default is that cooldown IS required — a
malformed row is never treated as permission to run immediately.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional


COOLDOWN_CONTRACT_VERSION = "shadow_maintenance_cooldown.v1"

# Where the pacing state is persisted (surfaced by access-check for operators).
COOLDOWN_PERSISTENCE_SOURCE = "strategy_shadow_outcome_runs"

# Default server-enforced interval between provider-backed maintenance batches.
DEFAULT_MIN_BATCH_INTERVAL_SECONDS = 75
# Hard floor whenever maintenance mode is active on the Massive Basic plan: the
# rolling request window is one minute, so batches must never be paced under it.
MASSIVE_BASIC_FLOOR_SECONDS = 60
# Reasonable maximum (guards against a fat-fingered runtime override).
MAX_MIN_BATCH_INTERVAL_SECONDS = 600

# Reason surfaced in preflight blocking_reasons / execute 409 during cooldown.
COOLDOWN_BLOCKING_REASON = "provider_cooldown_active"
# The same condition detected only after acquiring the advisory lock.
COOLDOWN_UNDER_LOCK_REASON = "provider_cooldown_activated_under_lock"

# Reference-timestamp precedence (most authoritative first).
_TIMESTAMP_PRECEDENCE = ("finished_at", "updated_at", "started_at", "created_at")


def resolve_min_interval_seconds(
    configured: Any,
    *,
    maintenance_only_mode: bool,
    provider: Optional[str],
) -> int:
    """Resolve the effective interval from configuration.

    * default is `DEFAULT_MIN_BATCH_INTERVAL_SECONDS` (applied by the caller /
      config default);
    * clamped to [0, MAX_MIN_BATCH_INTERVAL_SECONDS];
    * floored to `MASSIVE_BASIC_FLOOR_SECONDS` (>= 60) whenever maintenance mode
      is active AND the provider is Massive — a fast cache-warm batch must never
      lower the interval below the provider's rolling window.

    The value is not sensitive and requires no secret treatment.
    """
    try:
        value = int(configured)
    except (TypeError, ValueError):
        value = DEFAULT_MIN_BATCH_INTERVAL_SECONDS
    if value < 0:
        value = 0
    if maintenance_only_mode and (provider or "").strip().lower() == "massive":
        value = max(value, MASSIVE_BASIC_FLOOR_SECONDS)
    return min(value, MAX_MIN_BATCH_INTERVAL_SECONDS)


def _as_utc(value: Any) -> Optional[datetime]:
    """Coerce a persisted timestamp to a tz-aware UTC datetime, or None."""
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def reference_timestamp(
    run: Optional[Mapping[str, Any]],
) -> tuple[Optional[datetime], Optional[str]]:
    """Return (reference_utc, source_field) using the documented precedence."""
    if not run:
        return None, None
    for field in _TIMESTAMP_PRECEDENCE:
        ts = _as_utc(run.get(field))
        if ts is not None:
            return ts, field
    return None, None


def compute_cooldown(
    latest_run: Optional[Mapping[str, Any]],
    *,
    min_interval_seconds: int,
    now: datetime,
) -> Dict[str, Any]:
    """Decide whether a provider-backed batch may start right now.

    `latest_run` is the most recent MAINTENANCE outcome-run row (or None when no
    maintenance run has ever executed). All arithmetic is UTC. The returned
    `cooldown_remaining_seconds` is never negative and is zero once elapsed.
    No pair id or provider payload is ever exposed.
    """
    now_utc = _as_utc(now) or datetime.now(timezone.utc)
    interval = int(min_interval_seconds or 0)

    base: Dict[str, Any] = {
        "cooldown_contract_version": COOLDOWN_CONTRACT_VERSION,
        "min_interval_seconds": max(0, interval),
        "cooldown_persistence_source": COOLDOWN_PERSISTENCE_SOURCE,
        "last_execution_run_id": None,
        "last_execution_status": None,
        "last_execution_finished_at": None,
        "last_execution_timestamp_source": None,
        "next_execution_not_before": None,
        "cooldown_remaining_seconds": 0,
        "cooldown_required": False,
        "execution_allowed_by_cooldown": True,
    }

    # No pacing configured, or no prior maintenance run -> execution allowed.
    if interval <= 0 or not latest_run:
        return base

    run_id = latest_run.get("id")
    status = latest_run.get("status")
    base["last_execution_run_id"] = None if run_id is None else str(run_id)
    base["last_execution_status"] = None if status is None else str(status)
    base["cooldown_required"] = True

    ref, source = reference_timestamp(latest_run)
    if ref is None:
        # Identifiable maintenance run but no usable timestamp: never treat a
        # malformed row as permission to run — hold for the full interval.
        base["cooldown_remaining_seconds"] = interval
        base["execution_allowed_by_cooldown"] = False
        return base

    not_before = ref + timedelta(seconds=interval)
    remaining = (not_before - now_utc).total_seconds()
    remaining = max(0.0, remaining)

    base["last_execution_finished_at"] = ref.isoformat()
    base["last_execution_timestamp_source"] = source
    base["next_execution_not_before"] = not_before.isoformat()
    base["cooldown_remaining_seconds"] = int(math.ceil(remaining))
    base["execution_allowed_by_cooldown"] = remaining <= 0.0
    return base


def retry_after_seconds(cooldown: Mapping[str, Any]) -> int:
    """Whole-second Retry-After value (>= 1) for a cooldown-blocked response."""
    remaining = int(cooldown.get("cooldown_remaining_seconds") or 0)
    return max(1, remaining)


__all__ = [
    "COOLDOWN_CONTRACT_VERSION",
    "COOLDOWN_PERSISTENCE_SOURCE",
    "DEFAULT_MIN_BATCH_INTERVAL_SECONDS",
    "MASSIVE_BASIC_FLOOR_SECONDS",
    "MAX_MIN_BATCH_INTERVAL_SECONDS",
    "COOLDOWN_BLOCKING_REASON",
    "COOLDOWN_UNDER_LOCK_REASON",
    "resolve_min_interval_seconds",
    "reference_timestamp",
    "compute_cooldown",
    "retry_after_seconds",
]
