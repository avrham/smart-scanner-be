"""Explicit operator-triggered strategy dry-run (Phase 9D1).

One registered strategy, one explicit symbol, one deterministic evaluation —
and NOTHING is persisted. The dry-run exists so an authorized operator can
inspect what a strategy (including a database-disabled one such as
wyckoff_mtf_v2) would decide, without creating a signal, watch, alert,
notification, decision card, ranking input or configuration change.

Reused canonical building blocks (never duplicated):
  * registry resolution        - app.workers.strategies.registry.get_strategy
  * config resolution          - Phase 9C3 discovery (merge_config over the
                                 strategy defaults + patterns/pattern_configs
                                 rows read through the injected connection)
  * completed-frame policy     - app.workers.shadow.frames.build_canonical_frame
                                 (ny_session_close.v1, malformed-bar rejection)
  * details bounding           - the shadow 64KB deterministic pruner

Safety contract:
  * no database writes (the injected connection is used read-only by
    discovery);
  * no fallback strategy: an unknown pattern_code raises
    DryRunUnknownStrategyError, it is never substituted;
  * provider failures and frame rejections return a TYPED result with a
    bounded reason code — never a raw payload or stack trace;
  * a database-disabled or unconfigured strategy is still evaluable, but the
    dry-run never enables it, never mutates pattern_configs and never
    changes rollout flags — the resolved config is read as-is.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg

from app.workers.shadow.constants import (
    FRAME_FETCH_MARGIN_BARS,
    FRAME_HARD_CAP_BARS,
)
from app.workers.shadow.frames import (
    FrameRejection,
    build_canonical_frame,
    required_history_bars_v2,
    required_history_bars_v3,
)
from app.workers.strategies.base import Strategy, StrategyContext
from app.workers.strategies.discovery import StrategyDiscovery, discover_strategy
from app.workers.strategies.registry import get_strategy


logger = logging.getLogger(__name__)


DRY_RUN_CONTRACT_VERSION = "strategy_dry_run.v1"

# Terminal dry-run statuses. Every status is a typed, bounded result — the
# dry-run never falls back to another strategy and never guesses data.
STATUS_EVALUATED = "evaluated"
STATUS_FRAME_REJECTED = "frame_rejected"
STATUS_PROVIDER_ERROR = "provider_error"
STATUS_STRATEGY_ERROR = "strategy_error"

DRY_RUN_STATUSES = (
    STATUS_EVALUATED,
    STATUS_FRAME_REJECTED,
    STATUS_PROVIDER_ERROR,
    STATUS_STRATEGY_ERROR,
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")


class DryRunRequestError(ValueError):
    """Invalid dry-run request (malformed symbol / timestamp)."""


class DryRunUnknownStrategyError(KeyError):
    """The pattern_code has no canonically registered strategy."""


def normalize_dry_run_symbol(symbol: Any) -> str:
    """Uppercased, validated ticker. Rejects anything non-ticker-shaped."""
    normalized = str(symbol or "").strip().upper()
    if not normalized or not _SYMBOL_RE.match(normalized):
        raise DryRunRequestError(
            "symbol must be a ticker of 1-12 characters (A-Z, 0-9, '.', '-')"
        )
    return normalized


def parse_evaluation_time(value: Any) -> Optional[datetime]:
    """Optional explicit evaluation timestamp (ISO-8601, normalized to UTC).

    None means 'resolve safely to the current UTC time'. A naive timestamp
    is rejected — guessing a timezone would silently change completed-bar
    decisions.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise DryRunRequestError(
                "evaluation_time_utc must be an ISO-8601 datetime"
            )
    else:
        raise DryRunRequestError(
            "evaluation_time_utc must be an ISO-8601 datetime string"
        )
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise DryRunRequestError(
            "evaluation_time_utc must be timezone-aware (use an explicit offset)"
        )
    return parsed.astimezone(timezone.utc)


def required_daily_history_bars(
    pattern_code: str, config: Dict[str, Any], strategy: Strategy
) -> int:
    """Per-strategy completed-daily-bar requirement for a dry-run fetch.

    Reuses each strategy's own canonical derivation where one exists; the
    result is hard-capped at the documented shadow frame ceiling so a
    runaway config can never trigger an unbounded fetch.
    """
    if pattern_code == "sma150_bounce":
        desired = required_history_bars_v2(config)
    elif pattern_code == "sma150_bounce_v3":
        desired = required_history_bars_v3(config)
    elif pattern_code == "wyckoff_mtf_v2":
        from app.workers.strategies.wyckoff_v2.readiness import (
            derive_history_requirement,
        )

        desired = int(derive_history_requirement(config)["desired_history_bars"])
    elif pattern_code == "wyckoff_mtf":
        # v1 declares its own deep-history gate in config (>=24 monthly bars).
        desired = int(config.get("min_daily_bars", strategy.min_daily_bars)) + 10
    else:
        desired = max(int(getattr(strategy, "min_daily_bars", 200)), 400)
    return min(desired, FRAME_HARD_CAP_BARS)


def _safe_flag(config: Dict[str, Any], key: str) -> Optional[bool]:
    value = config.get(key)
    return value if isinstance(value, bool) else None


def _rollout_block(details: Dict[str, Any]) -> Dict[str, Optional[bool]]:
    """Explicit rollout state extracted from the strategy's OWN policy record.

    Only strategies that persist a policy block (wyckoff_mtf.policy.v1) can
    report it; everything else stays None — never fabricated.
    """
    policy = details.get("policy")
    if not isinstance(policy, dict):
        return {"rollout_blocked": None, "enter_eligible_without_rollout_gate": None}
    eligible = policy.get("enter_eligible_without_rollout_gate")
    allow_enter = policy.get("allow_enter")
    if not isinstance(eligible, bool) or not isinstance(allow_enter, bool):
        return {"rollout_blocked": None, "enter_eligible_without_rollout_gate": None}
    return {
        "rollout_blocked": eligible and not allow_enter,
        "enter_eligible_without_rollout_gate": eligible,
    }


def _readiness_status(details: Dict[str, Any]) -> Optional[str]:
    readiness = details.get("readiness")
    if isinstance(readiness, dict):
        status = readiness.get("status")
        if isinstance(status, str):
            return status
    return None


def _base_result(
    *,
    discovery: StrategyDiscovery,
    symbol: str,
    provider_name: Optional[str],
    evaluation_time_utc: datetime,
    requested_history_bars: int,
) -> Dict[str, Any]:
    config = discovery.effective_config
    return {
        "dry_run_contract_version": DRY_RUN_CONTRACT_VERSION,
        "persisted": False,
        "pattern_code": discovery.pattern_code,
        "symbol": symbol,
        "provider": provider_name,
        "evaluation_time_utc": evaluation_time_utc.isoformat(),
        "registered": discovery.registered,
        "enabled": discovery.enabled,
        "db_configured": discovery.db_configured,
        "config_status": discovery.config_status,
        "strategy_version": discovery.strategy_version,
        "decision_policy_version": discovery.decision_policy_version,
        "rollout_flags": {
            "allow_enter": _safe_flag(config, "allow_enter"),
            "enable_4h_trigger": _safe_flag(config, "enable_4h_trigger"),
            "min_price": discovery.min_price,
        },
        "requested_history_bars": requested_history_bars,
        # Filled by the terminal branch:
        "status": None,
        "error_reason_code": None,
        "frame": None,
        "decision": None,
        "score": None,
        "side": None,
        "reason": None,
        "rejection_reason": None,
        "setup_type": None,
        "entry_price": None,
        "stop_price": None,
        "target_price": None,
        "invalidation": None,
        "trigger": None,
        "readiness_status": None,
        "insufficient_data": None,
        "rollout_blocked": None,
        "enter_eligible_without_rollout_gate": None,
        "evidence": None,
        "details_snapshot": None,
        "details_original_sha256": None,
    }


async def run_strategy_dry_run(
    db: asyncpg.Connection,
    provider: Any,
    *,
    pattern_code: str,
    symbol: Any,
    evaluation_time_utc: Any = None,
) -> Dict[str, Any]:
    """Execute one explicit, persistence-free strategy dry-run.

    Raises DryRunUnknownStrategyError for an unregistered pattern_code and
    DryRunRequestError for malformed inputs; every other failure mode is a
    typed result with a bounded reason code.
    """
    # Local import: the runner's bounding helper lives beside the shadow
    # serializer; importing here keeps module import light and cycle-free.
    from app.workers.shadow.runner import _bound_details
    from app.workers.shadow.serialization import ShadowSerializationError

    normalized_symbol = normalize_dry_run_symbol(symbol)
    eval_time = parse_evaluation_time(evaluation_time_utc) or datetime.now(
        timezone.utc
    )

    discovery = await discover_strategy(db, pattern_code)
    if discovery is None:
        raise DryRunUnknownStrategyError(
            f"No strategy registered for pattern_code '{pattern_code}'"
        )

    strategy = get_strategy(pattern_code)
    config = discovery.effective_config
    requested_bars = required_daily_history_bars(pattern_code, config, strategy)
    provider_name = getattr(provider, "name", None)

    result = _base_result(
        discovery=discovery,
        symbol=normalized_symbol,
        provider_name=provider_name,
        evaluation_time_utc=eval_time,
        requested_history_bars=requested_bars,
    )

    try:
        payload = await provider.get_daily_history(
            normalized_symbol, timeseries=requested_bars + FRAME_FETCH_MARGIN_BARS
        )
    except Exception as exc:
        logger.warning(
            "Dry-run fetch failed for %s/%s: %s",
            pattern_code, normalized_symbol, type(exc).__name__,
        )
        result["status"] = STATUS_PROVIDER_ERROR
        result["error_reason_code"] = f"provider_{type(exc).__name__}"
        return result

    try:
        frame = build_canonical_frame(
            normalized_symbol, payload, max_bars=requested_bars, now_utc=eval_time
        )
    except FrameRejection as rejection:
        result["status"] = STATUS_FRAME_REJECTED
        result["error_reason_code"] = rejection.reason_code
        return result

    result["frame"] = {
        "bar_count": frame.bar_count,
        "first_date": frame.first_date,
        "last_date": frame.last_date,
        "snapshot_date": frame.snapshot_date.isoformat(),
        "requested_history_bars": requested_bars,
        "history_depth_complete": frame.bar_count >= requested_bars,
        "completion": frame.completion,
    }

    # The canonical frame builder PROVED the latest bar completed, so both
    # completion vocabularies are set: sma150.v3 reads latest_bar_completed;
    # wyckoff_mtf.v2 reads explicit_completed / as_of_date.
    context = StrategyContext(
        symbol=normalized_symbol,
        pattern_code=pattern_code,
        config=config,
        scanner_mode="dry_run",
        scan_run_id=None,
        data_meta={
            "latest_bar_completed": True,
            "explicit_completed": True,
            "as_of_date": frame.last_date,
            "evaluation_time_utc": eval_time,
        },
    )

    try:
        evaluation = strategy.evaluate(frame.dataframe(), context)
    except Exception as exc:
        logger.warning(
            "Dry-run evaluation failed for %s/%s: %s",
            pattern_code, normalized_symbol, type(exc).__name__,
        )
        result["status"] = STATUS_STRATEGY_ERROR
        result["error_reason_code"] = f"strategy_{type(exc).__name__}"
        return result

    details = evaluation.details or {}
    try:
        bounded = _bound_details(details)
    except ShadowSerializationError as exc:
        result["status"] = STATUS_STRATEGY_ERROR
        result["error_reason_code"] = f"details_not_json_safe:{exc.reason_code}"
        return result
    except Exception:
        result["status"] = STATUS_STRATEGY_ERROR
        result["error_reason_code"] = "details_bounding_failed"
        return result

    snapshot = bounded["snapshot"]
    rollout = _rollout_block(snapshot if isinstance(snapshot, dict) else {})
    readiness_status = _readiness_status(
        snapshot if isinstance(snapshot, dict) else {}
    )

    result.update({
        "status": STATUS_EVALUATED,
        "decision": evaluation.decision.value,
        "score": evaluation.score,
        "side": evaluation.side.value,
        "reason": evaluation.reason,
        "rejection_reason": evaluation.rejection_reason,
        "setup_type": evaluation.setup_type,
        "entry_price": evaluation.entry_price,
        "stop_price": evaluation.stop_price,
        "target_price": evaluation.target_price,
        "invalidation": evaluation.invalidation,
        "trigger": (
            snapshot.get("four_hour_trigger")
            if isinstance(snapshot, dict) else None
        ),
        "readiness_status": readiness_status,
        "insufficient_data": (
            None if readiness_status is None else readiness_status != "ready"
        ),
        "rollout_blocked": rollout["rollout_blocked"],
        "enter_eligible_without_rollout_gate": rollout[
            "enter_eligible_without_rollout_gate"
        ],
        "evidence": (
            snapshot.get("evidence") if isinstance(snapshot, dict) else None
        ),
        "details_snapshot": snapshot,
        "details_original_sha256": bounded["original_sha256"],
    })
    return result
