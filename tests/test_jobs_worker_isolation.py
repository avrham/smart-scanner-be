"""Child-process pool-isolation regression tests (live-worker defect).

Live failure on Fly (Linux, python:3.12): the worker's ProcessPoolExecutor used
the platform-default start method — ``fork`` on Linux — so the handler child
inherited the parent's module-global asyncpg pool (``app.deps._db_pool``) and
its OPEN SOCKET FDs. The child's ``init_db_pool()`` then reused the parent's
live connections; parent and child interleaved on the same PostgreSQL sockets,
desynchronizing the protocol: lease renewal hung forever, the lease expired
mid-run, and the claim loop stalled permanently.

These tests (a) REPRODUCE the inheritance mechanism under an explicit fork
context and (b) prove the worker's executor factory starts children via
``spawn``, where the pool global is None and the child builds its own pool.
"""

from __future__ import annotations

import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import pytest

import app.deps as deps
from app.jobs.handlers.synthetic import run_pool_isolation_probe
from app.jobs.worker import JobWorker, _make_executor


class _PoolSentinel:
    """Stands in for a live asyncpg pool in the parent's module global."""


@pytest.fixture
def parent_pool_sentinel():
    original = deps._db_pool
    deps._db_pool = _PoolSentinel()
    try:
        yield
    finally:
        deps._db_pool = original


@pytest.mark.skipif(sys.platform.startswith("win"), reason="fork unavailable")
def test_fork_child_inherits_parent_pool_global(parent_pool_sentinel):
    """REPRODUCTION: a fork-started child sees the parent's pool global —
    the exact mechanism that corrupted the live worker's connections."""
    ex = ProcessPoolExecutor(max_workers=1,
                             mp_context=multiprocessing.get_context("fork"))
    try:
        report = ex.submit(run_pool_isolation_probe, {}).result(timeout=60)
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    assert report["pool_inherited"] is True
    assert report["child_pid"] != os.getpid()


def test_worker_executor_child_does_not_inherit_pool(parent_pool_sentinel):
    """FIX: a child from the worker's executor factory starts clean — the pool
    global is None in the child even while the parent holds a live pool."""
    ex = _make_executor(1)
    try:
        report = ex.submit(run_pool_isolation_probe, {}).result(timeout=120)
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    assert report["pool_inherited"] is False
    assert report["child_pid"] != os.getpid()


def test_worker_run_and_recycle_use_spawn_context():
    """Both executor construction sites must use the spawn factory."""
    w = JobWorker()
    w._recycle_executor_if_broken()
    try:
        ctx = w.executor._mp_context
        assert "spawn" in type(ctx).__name__.lower()
    finally:
        w.executor.shutdown(wait=False, cancel_futures=True)

    ex = _make_executor(1)
    try:
        assert "spawn" in type(ex._mp_context).__name__.lower()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
