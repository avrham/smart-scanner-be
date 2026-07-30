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
from typing import Any, Awaitable, Callable, Dict, Optional

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
