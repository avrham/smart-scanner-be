"""Unit coverage for the history-refresh in-task cooldown wait (live blocker fix).

The history-warmup service enforces a SHARED execution cooldown / advisory lock,
so a serially-claimed symbol frequently receives a KNOWN transient 409 while the
previous symbol's window is still active. Mapping that 409 to a queue-level
retryable (the old behaviour) made 24/25 tasks burn their attempt budget in the
same retry wave. The handler now absorbs the known cooldown/lock 409 with a
BOUNDED in-task wait and retries within the SAME claim.

These are deterministic unit tests: the service and the DB connection are faked,
and the handler's clock + sleep are driven by a virtual clock (no real sleeping,
no Docker). They exercise the exact control flow, not the SQL.
"""

from __future__ import annotations

import asyncio

import pytest

from fastapi import HTTPException

import app.jobs.handlers.history_refresh_worker as HRW
from app.jobs import contracts as C


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class _FakeConn:
    async def fetchval(self, *a, **k):
        return None  # latest daily / latest 4H unknown → service recomputes anyway


class _ScriptedService:
    """Callable standing in for history_incremental_refresh_execute_service:
    each call yields the next scripted item — raise it if it's an Exception,
    else return it. The last item repeats (for the unbounded-cooldown case)."""
    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    async def __call__(self, conn, *, body, now=None):
        item = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        if isinstance(item, BaseException):
            raise item
        return item


def _cooldown_409(remaining=5, retry_after="5", reason="provider_cooldown_active"):
    return HTTPException(status_code=409,
                         detail={"error": reason,
                                 "cooldown_remaining_seconds": remaining},
                         headers=({"Retry-After": retry_after} if retry_after is not None else None))


def _lock_409():
    return HTTPException(status_code=409, detail={"error": "history_warmup_execution_locked"})


def _ok(status="executed", reqs=1):
    return {"status": status, "run_id": "run-x", "provider_request_count": reqs}


@pytest.fixture()
def wired(monkeypatch):
    """Wire the handler's seams to fakes + a virtual clock. Returns a dict with a
    knob to install a scripted service and inspect sleeps."""
    import app.workers.persistence as persistence
    import app.routers.admin as admin
    import app.prospective_session as psess
    from datetime import date

    async def _get():
        return _FakeConn()

    async def _rel(c):
        return None

    monkeypatch.setattr(persistence, "get_db_connection", _get, raising=True)
    monkeypatch.setattr(persistence, "release_db_connection", _rel, raising=True)
    monkeypatch.setattr(psess, "resolve_latest_completed_session", lambda now: date(2026, 8, 20))

    # virtual clock: _monotonic reads t[0]; _sleep advances it and records waits.
    t = [0.0]
    sleeps = []

    async def _sleep(s):
        sleeps.append(s)
        t[0] += s

    monkeypatch.setattr(HRW, "_monotonic", lambda: t[0])
    monkeypatch.setattr(HRW, "_sleep", _sleep)

    def install(script):
        svc = _ScriptedService(script)
        monkeypatch.setattr(admin, "history_incremental_refresh_execute_service", svc)
        return svc

    return {"install": install, "sleeps": sleeps, "clock": t}


def _run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------------------------- #
# A. sequential shared-cooldown: 409 → wait → recompute → retry → success
# --------------------------------------------------------------------------- #
def test_A_cooldown_409_waits_then_succeeds_in_same_claim(wired):
    svc = wired["install"]([_cooldown_409(remaining=5, retry_after="5"), _ok()])
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is True and res["result"]["status"] == "executed"
    assert svc.calls == 2                       # retried inside the SAME invocation
    assert wired["sleeps"] == [5 + 3]           # Retry-After (5) + default margin (3)
    # NOTE: the return is a success, NOT a queue-level retryable for the cooldown.


# --------------------------------------------------------------------------- #
# B. several symbols progress serially through the shared cooldown
# --------------------------------------------------------------------------- #
def test_B_multiple_symbols_progress_without_exhausting_attempts(wired):
    for sym in ("ZZA", "ZZB", "ZZC"):
        svc = wired["install"]([_cooldown_409(retry_after="4"), _ok()])
        res = _run(HRW.execute_history_refresh_symbol({"symbol": sym}))
        assert res["ok"] is True, (sym, res)
        assert svc.calls == 2                    # each symbol: one wait, one success
    # each symbol consumed ZERO queue retries for the cooldown.


# --------------------------------------------------------------------------- #
# C. the in-task wait is bounded (never spins forever)
# --------------------------------------------------------------------------- #
def test_C_wait_is_bounded_and_defers_when_exhausted(wired, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "HISTORY_REFRESH_TASK_MAX_WAIT_SECONDS", 10, raising=False)
    monkeypatch.setattr(settings, "HISTORY_REFRESH_TASK_COOLDOWN_MARGIN_SECONDS", 0, raising=False)
    svc = wired["install"]([_cooldown_409(retry_after="3")])   # ALWAYS cooling down
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is False and res["error_class"] == C.ERR_RETRYABLE
    assert res["safe_error_code"] == "history_refresh_cooldown_wait_exhausted"
    # bounded: with a 10s ceiling and 3s waits it slept a small, finite number of
    # times then deferred — it did not spin.
    assert sum(wired["sleeps"]) <= 10 and 0 < len(wired["sleeps"]) <= 5


def test_C2_lock_409_without_hint_uses_bounded_poll(wired, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "HISTORY_REFRESH_TASK_MAX_WAIT_SECONDS", 12, raising=False)
    monkeypatch.setattr(settings, "HISTORY_REFRESH_TASK_POLL_SECONDS", 5, raising=False)
    # lock 409 (no Retry-After/remaining) twice, then success
    svc = wired["install"]([_lock_409(), _lock_409(), _ok()])
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is True and svc.calls == 3
    assert wired["sleeps"] == [5, 5]            # fell back to the poll default


# --------------------------------------------------------------------------- #
# D. an unrelated/unrecognized 409 is NOT treated as cooldown (fail-closed)
# --------------------------------------------------------------------------- #
def test_D_unrecognized_409_is_not_swallowed(wired):
    svc = wired["install"]([HTTPException(status_code=409,
                                          detail={"error": "stale_target_completed_session"})])
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is False and res["error_class"] == C.ERR_RETRYABLE
    assert res["safe_error_code"] == "history_refresh_http_409"
    assert svc.calls == 1 and wired["sleeps"] == []   # NO wait, NO retry inside the claim


def test_D2_non_409_4xx_is_terminal(wired):
    svc = wired["install"]([HTTPException(status_code=422, detail={"error": "bad_contract_version"})])
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is False and res["error_class"] == C.ERR_TERMINAL
    assert res["safe_error_code"] == "history_refresh_http_422"
    assert wired["sleeps"] == []


# --------------------------------------------------------------------------- #
# provider failures keep their existing classification (unchanged)
# --------------------------------------------------------------------------- #
def test_provider_failure_classification_unchanged(wired):
    for klass, expect in (("retryable", C.ERR_RETRYABLE), ("operator_error", C.ERR_OPERATOR),
                          ("terminal", C.ERR_TERMINAL)):
        wired["install"]([{"status": "failed", "error": {"code": "provider_x", "class": klass}}])
        res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
        assert res["ok"] is False and res["error_class"] == expect, (klass, res)
        assert res["safe_error_code"] == "provider_x"


def test_no_op_and_already_applied_succeed(wired):
    for status in ("no-op", "already_applied", "executed"):
        wired["install"]([_ok(status=status, reqs=0)])
        res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
        assert res["ok"] is True and res["result"]["status"] == status


# --------------------------------------------------------------------------- #
# F. history handler retry budget NOT broadened (queue attempts unchanged)
# --------------------------------------------------------------------------- #
def test_F_history_handler_retry_budget_unchanged():
    from app.jobs import registry as R
    from app.jobs import history_refresh as HR
    R._install_default_handlers()
    spec = R.registered_task_types()[HR.HISTORY_REFRESH_TASK]
    assert spec.retry_backoff_schedule is None          # still global bounded two-retry
    assert spec.max_attempts == HR.HISTORY_REFRESH_MAX_ATTEMPTS == 3


# --------------------------------------------------------------------------- #
# Root cause #2 PART 1: the EXACT missing live reason is recognized (via the
# authoritative constant) and waits in-task; producer/consumer cannot drift.
# --------------------------------------------------------------------------- #
def test_A2_under_lock_cooldown_reason_is_recognized_and_waits(wired):
    from app.maintenance_cooldown import COOLDOWN_UNDER_LOCK_REASON
    # this is the exact reason the live third attempts received
    assert COOLDOWN_UNDER_LOCK_REASON == "provider_cooldown_activated_under_lock"
    svc = wired["install"]([
        _cooldown_409(remaining=6, retry_after="6", reason=COOLDOWN_UNDER_LOCK_REASON), _ok()])
    res = _run(HRW.execute_history_refresh_symbol({"symbol": "ZZB"}))
    assert res["ok"] is True and svc.calls == 2          # waited in-task, then succeeded
    assert wired["sleeps"] == [6 + 3]                    # NOT a queue-level failure


def test_B_no_reason_string_drift_producer_and_consumer_share_constants():
    """The worker's allowlist IS the authoritative set exported from
    history_warmup_execute, which re-exports the maintenance_cooldown reason
    constants — so a producer reason can never drift out of the consumer set."""
    from app.history_warmup_execute import (
        HISTORY_WARMUP_TRANSIENT_409_REASONS, HISTORY_WARMUP_EXECUTION_IN_PROGRESS_REASON,
        HISTORY_WARMUP_EXECUTION_LOCKED_REASON)
    from app.maintenance_cooldown import COOLDOWN_BLOCKING_REASON, COOLDOWN_UNDER_LOCK_REASON
    # consumer uses the exact authoritative object (identity, not a copy)
    assert HRW._COOLDOWN_409_REASONS is HISTORY_WARMUP_TRANSIENT_409_REASONS
    # every authoritative reason is present via its constant (no literals)
    assert HISTORY_WARMUP_TRANSIENT_409_REASONS == frozenset({
        COOLDOWN_BLOCKING_REASON, COOLDOWN_UNDER_LOCK_REASON,
        HISTORY_WARMUP_EXECUTION_IN_PROGRESS_REASON, HISTORY_WARMUP_EXECUTION_LOCKED_REASON})
    # and the two cooldown reasons are the SAME objects the producer module owns
    import app.history_warmup_execute as H
    assert H.COOLDOWN_BLOCKING_REASON == COOLDOWN_BLOCKING_REASON
    assert H.COOLDOWN_UNDER_LOCK_REASON == COOLDOWN_UNDER_LOCK_REASON
