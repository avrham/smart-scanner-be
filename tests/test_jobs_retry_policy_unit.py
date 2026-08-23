"""Unit coverage for the per-handler retry-backoff policy (Root Cause B fix).

The generic worker derives retry backoff from a SCHEDULE. The global schedule
(`settings.JOB_RETRY_BACKOFF_SECONDS`, default [60, 300]) exhausts after two
retries, which terminally fails ANY task at attempt 3 regardless of its
``max_attempts``. That is exactly why the live daily-pipeline driver task
(max_attempts=10) failed on attempt 3. The fix: a handler may declare its own
``retry_backoff_schedule``; handlers that do not opt in keep the global (bounded
two-retry) behaviour unchanged.

These are pure unit checks (no DB / no Docker): the backoff arithmetic, the
handler-registry wiring, and the length invariant that guarantees the driver can
actually use its full attempt budget.
"""

from __future__ import annotations

from app.config import settings
from app.jobs import contracts as C
from app.jobs import daily_pipeline as DP
from app.jobs import registry as R


def test_global_backoff_schedule_exhausts_after_two_retries():
    """The documented global contract: attempt1->60, attempt2->300, then None
    (terminal). This is the pre-existing behaviour every non-opted-in handler
    keeps."""
    sched = list(settings.JOB_RETRY_BACKOFF_SECONDS)
    assert sched == [60, 300]
    assert C.backoff_seconds(1, schedule=sched) == 60
    assert C.backoff_seconds(2, schedule=sched) == 300
    assert C.backoff_seconds(3, schedule=sched) is None   # <- the live-failure point


def test_driver_backoff_schedule_covers_full_attempt_budget():
    """The driver's own schedule must be exactly one shorter than its
    max_attempts so every attempt up to the last has a defined backoff and only
    the final attempt is terminal — no premature terminal, no infinite retry."""
    sched = list(settings.DAILY_PIPELINE_DRIVER_BACKOFF_SECONDS)
    assert len(sched) == DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS - 1 == 9
    # every attempt 1..9 has a real backoff (task stays retryable)
    for attempt in range(1, DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS):
        assert C.backoff_seconds(attempt, schedule=sched) is not None, attempt
    # the max-th attempt has no backoff -> terminal (hard ceiling)
    assert C.backoff_seconds(DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS, schedule=sched) is None


def test_driver_handler_is_wired_to_its_own_schedule():
    """Registry wiring: the driver handler carries the driver schedule so the
    worker uses it instead of the (too-short) global list."""
    R._install_default_handlers()
    spec = R.registered_task_types()[DP.DAILY_PIPELINE_ADVANCE_TASK]
    assert spec.max_attempts == DP.DAILY_PIPELINE_DRIVER_MAX_ATTEMPTS == 10
    assert spec.retry_backoff_schedule == list(settings.DAILY_PIPELINE_DRIVER_BACKOFF_SECONDS)
    assert len(spec.retry_backoff_schedule) == spec.max_attempts - 1


def test_existing_handlers_retain_global_backoff_behaviour():
    """Pre-existing handlers must NOT silently gain 10 retries: they declare no
    per-handler schedule, so the worker falls back to the global bounded list."""
    R._install_default_handlers()
    specs = R.registered_task_types()
    for task_type in (C.PROSPECTIVE_SYMBOL_EVALUATION_TASK, C.PROSPECTIVE_OUTCOME_MATURATION_TASK):
        assert specs[task_type].retry_backoff_schedule is None, task_type


def test_worker_schedule_selection_prefers_handler_then_global():
    """Mirror the exact selection expression in worker._finalize_failure: a
    per-handler schedule wins; otherwise the global schedule applies."""
    def select(spec_schedule):
        return list(spec_schedule) if spec_schedule else list(settings.JOB_RETRY_BACKOFF_SECONDS)

    assert select(None) == [60, 300]
    assert select([1, 2, 3]) == [1, 2, 3]
