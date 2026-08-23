"""Code-level task-handler registry.

The queue framework contains NO task-specific calculations — it only claims a
task, runs the registered handler in a bounded child process, and finalizes.
Concrete work is registered here. Rules:

  * unknown task types fail terminally (never silently skipped);
  * fake/test handlers are only selectable when JOB_ALLOW_TEST_HANDLERS=true
    (never in production);
  * a handler exposes a picklable, module-level ``child_callable`` (runs the CPU
    work in a child process and returns a bounded result dict) and an async
    ``probe_fn`` (parent-side, inspects durable output for crash reconciliation).

Future handlers plug in by adding a HandlerSpec:
  history_universe_warmup / prospective_campaign_registration /
  daily_scanner_run / quality_audit / notification_delivery.
prospective_symbol_evaluation.v1 and prospective_outcome_maturation.v1 are
live-enabled now.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.config import settings
from app.jobs import contracts as C


@dataclass(frozen=True)
class HandlerSpec:
    task_type: str
    queue_name: str
    #: module-level, picklable callable run IN A CHILD PROCESS.
    #: signature: (payload: dict) -> dict with {"ok": bool, ...}
    child_callable: Callable[[Dict[str, Any]], Dict[str, Any]]
    #: parent-side async probe: (conn, payload) -> Optional[result dict]. Returns
    #: a complete result when durable output already exists (crash reconcile),
    #: else None. May be None if the task type has no reconcilable output.
    probe_fn: Optional[Callable[..., Awaitable[Optional[Dict[str, Any]]]]]
    #: default per-task attempt budget.
    max_attempts: int = 3
    #: OPTIONAL per-handler retry backoff schedule (seconds before each retry).
    #: ``schedule[k-1]`` is the delay after the k-th failed attempt; a handler
    #: with ``max_attempts=N`` that wants to actually use all N attempts must
    #: supply a schedule of length ``N-1`` (the N-th failure is always terminal).
    #: When None, the worker falls back to the GLOBAL
    #: ``settings.JOB_RETRY_BACKOFF_SECONDS`` (default [60, 300] → 3 attempts),
    #: preserving the existing bounded two-retry behaviour for every handler that
    #: does not opt in. See app.jobs.worker._finalize_failure and
    #: app.jobs.contracts.backoff_seconds.
    retry_backoff_schedule: Optional[List[int]] = None
    #: whether this handler may be selected in a normal (non-test) deployment.
    production_enabled: bool = False
    is_test_handler: bool = False


_REGISTRY: Dict[str, HandlerSpec] = {}


def register(spec: HandlerSpec) -> HandlerSpec:
    _REGISTRY[spec.task_type] = spec
    return spec


class UnknownTaskType(C.TerminalJobError):
    def __init__(self, task_type: str) -> None:
        super().__init__("unknown_task_type", f"no handler registered for {task_type!r}")


def resolve_handler(task_type: str) -> HandlerSpec:
    """Resolve a handler or fail TERMINALLY. Test handlers are rejected unless
    JOB_ALLOW_TEST_HANDLERS=true (default false → never in production)."""
    spec = _REGISTRY.get(task_type)
    if spec is None:
        raise UnknownTaskType(task_type)
    if spec.is_test_handler and not getattr(settings, "JOB_ALLOW_TEST_HANDLERS", False):
        raise C.TerminalJobError(
            "test_handler_forbidden",
            f"task type {task_type!r} is a test handler and is not enabled")
    if not spec.is_test_handler and not spec.production_enabled:
        raise C.TerminalJobError(
            "handler_not_enabled",
            f"task type {task_type!r} is registered but not production-enabled")
    return spec


def is_registered(task_type: str) -> bool:
    return task_type in _REGISTRY


def registered_task_types() -> Dict[str, HandlerSpec]:
    return dict(_REGISTRY)


def _install_default_handlers() -> None:
    """Register the production + (optional) test handlers. Imported lazily to
    avoid import cycles with the handler modules."""
    from app.jobs.handlers import prospective as _p
    register(HandlerSpec(
        task_type=C.PROSPECTIVE_SYMBOL_EVALUATION_TASK,
        queue_name=C.PROSPECTIVE_QUEUE,
        child_callable=_p.run_prospective_symbol_task,
        probe_fn=_p.probe_prospective_durable_output,
        max_attempts=getattr(settings, "JOB_MAX_ATTEMPTS_DEFAULT", 3),
        production_enabled=True,
    ))
    from app.jobs.handlers import prospective_outcome as _po
    register(HandlerSpec(
        task_type=C.PROSPECTIVE_OUTCOME_MATURATION_TASK,
        queue_name=C.PROSPECTIVE_OUTCOME_QUEUE,
        child_callable=_po.run_prospective_outcome_task,
        probe_fn=_po.probe_prospective_outcome_durable_output,
        max_attempts=getattr(settings, "JOB_MAX_ATTEMPTS_DEFAULT", 3),
        production_enabled=True,
    ))
    from app.jobs.handlers import history_refresh_worker as _hrw
    from app.jobs import history_refresh as _hr
    register(HandlerSpec(
        task_type=_hr.HISTORY_REFRESH_TASK,
        queue_name=_hr.HISTORY_REFRESH_QUEUE,
        child_callable=_hrw.run_history_incremental_refresh_task,
        probe_fn=_hrw.probe_history_refresh_durable_output,
        max_attempts=_hr.HISTORY_REFRESH_MAX_ATTEMPTS,
        # No per-handler schedule → global bounded two-retry, reserved for GENUINE
        # provider errors. The shared history-warmup cooldown/lock 409 is absorbed
        # by a bounded in-task wait in the handler, not by these queue attempts.
        production_enabled=True,
    ))
    from app.jobs.handlers import daily_pipeline_driver as _dpd
    from app.jobs import daily_pipeline as _dp
    register(HandlerSpec(
        task_type=_dp.DAILY_PIPELINE_ADVANCE_TASK,
        queue_name=_dp.DAILY_PIPELINE_DRIVER_QUEUE,
        child_callable=_dpd.run_daily_pipeline_advance_task,
        probe_fn=_dpd.probe_daily_pipeline_advance_durable_output,
        max_attempts=_dp.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS,
        # Own backoff schedule so the driver can actually use its 10-attempt
        # budget across repeated occurrence_in_progress defers (the global
        # 2-entry list would cap it at 3, which is exactly the live failure).
        retry_backoff_schedule=list(settings.DAILY_PIPELINE_DRIVER_BACKOFF_SECONDS),
        production_enabled=True,
    ))
    # A safe, synthetic test handler for controlled retry/crash tests. NEVER
    # selectable unless JOB_ALLOW_TEST_HANDLERS=true; performs no strategy math
    # and touches no real campaign data.
    from app.jobs.handlers import synthetic as _s
    register(HandlerSpec(
        task_type=_s.SYNTHETIC_TASK_TYPE,
        queue_name="test",
        child_callable=_s.run_synthetic_task,
        probe_fn=None,
        max_attempts=3,
        production_enabled=False,
        is_test_handler=True,
    ))


_install_default_handlers()


__all__ = [
    "HandlerSpec", "register", "resolve_handler", "is_registered",
    "registered_task_types", "UnknownTaskType",
]
