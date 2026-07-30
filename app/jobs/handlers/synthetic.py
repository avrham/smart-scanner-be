"""A safe, synthetic test-only handler for controlled retry/crash/lease tests.

NEVER selectable unless JOB_ALLOW_TEST_HANDLERS=true (enforced by the registry).
Performs NO strategy math and touches NO real campaign data — it only echoes /
sleeps / fails / hard-exits based on its payload, so deterministic tests can
exercise the queue's retry, backoff, cancellation and crash-recovery paths
without injecting failure into a real symbol.
"""

from __future__ import annotations

from typing import Any, Dict

SYNTHETIC_TASK_TYPE = "synthetic_test_task.v1"
SYNTHETIC_QUEUE = "test"


def run_synthetic_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Child-process entrypoint. mode ∈ {succeed, fail_retryable, fail_terminal,
    crash, sleep}."""
    mode = str(payload.get("mode", "succeed"))
    if mode == "crash":
        import os
        os._exit(97)  # hard child death — parent sees a broken process pool
    if mode == "sleep":
        import time
        time.sleep(float(payload.get("seconds", 0.5)))
    if mode == "fail_retryable":
        return {"ok": False, "error_class": "retryable",
                "safe_error_code": "synthetic_retryable", "message": "synthetic"}
    if mode == "fail_terminal":
        return {"ok": False, "error_class": "terminal",
                "safe_error_code": "synthetic_terminal", "message": "synthetic"}
    return {"ok": True, "reconciled": False,
            "result": {"echo": payload.get("echo"), "mode": mode}}


def run_pool_isolation_probe(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Child-process probe: report whether this child inherited the parent's
    module-global asyncpg pool. A handler child MUST see ``app.deps._db_pool``
    as None (it owns its pool); a fork-started child inherits the parent's live
    pool object (and its socket FDs) — the exact defect this probe detects."""
    import os
    import app.deps as deps
    return {"pool_inherited": deps._db_pool is not None, "child_pid": os.getpid()}


__all__ = ["SYNTHETIC_TASK_TYPE", "SYNTHETIC_QUEUE", "run_synthetic_task",
           "run_pool_isolation_probe"]
