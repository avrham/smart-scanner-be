"""Dedicated non-HTTP job worker — `python -m app.jobs.worker`.

Poll → claim (FOR UPDATE SKIP LOCKED) → run the handler in a BOUNDED CHILD
PROCESS (CPU isolation) while the parent renews the lease + heartbeats → finalize.
Never serves HTTP, never depends on Fly Proxy traffic, survives hard process
death via lease-expiry reconciliation. Concurrency 1 initially.

The parent event loop NEVER runs strategy math — that happens only in the child
process (ProcessPoolExecutor, max_workers=1), so the parent stays responsive to
renew leases, heartbeat, handle shutdown and detect child failure.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
from typing import Any, Dict, List, Optional

from app.build_info import build_provenance
from app.config import settings
from app.jobs import contracts as C
from app.jobs import queue as Q
from app.jobs import registry as R
from app.jobs.prospective_enqueue import sync_prospective_campaign

logger = logging.getLogger("app.jobs.worker")


def _json_payload(raw: Any) -> Dict[str, Any]:
    import json
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (str, bytes)):
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


class JobWorker:
    def __init__(self) -> None:
        self.worker_type = settings.JOB_WORKER_TYPE
        self.queues: List[str] = [q.strip() for q in
                                  str(settings.JOB_WORKER_QUEUES).split(",") if q.strip()]
        self.concurrency = max(1, int(settings.JOB_WORKER_CONCURRENCY))
        self.lease_seconds = int(settings.JOB_TASK_LEASE_SECONDS)
        self.task_hb = int(settings.JOB_TASK_HEARTBEAT_SECONDS)
        self.worker_hb = int(settings.JOB_WORKER_HEARTBEAT_SECONDS)
        self.poll = max(1, int(settings.JOB_QUEUE_POLL_SECONDS))
        self.hostname = socket.gethostname()[:120]
        self.worker_id = f"{self.worker_type}-{self.hostname}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.draining = False
        self.stop = False
        self.executor: Optional[ProcessPoolExecutor] = None
        self._pool = None

    # ----- lifecycle -----
    async def register(self) -> None:
        from app.deps import init_db_pool
        await init_db_pool()
        from app.workers.persistence import get_db_connection, release_db_connection
        conn = await get_db_connection()
        try:
            await conn.execute(
                "INSERT INTO job_workers (worker_id, worker_type, queue_names, deployed_git_sha,"
                " hostname, status, started_at, last_heartbeat_at, draining) "
                "VALUES ($1,$2,$3,$4,$5,'idle',NOW(),NOW(),FALSE) "
                "ON CONFLICT (worker_id) DO UPDATE SET status='idle', last_heartbeat_at=NOW(),"
                " draining=FALSE",
                self.worker_id, self.worker_type, self.queues,
                build_provenance().get("git_sha", "unknown"), self.hostname)
        finally:
            await release_db_connection(conn)
        logger.info("worker registered", extra={"extra_data": {
            "worker_id": self.worker_id, "queues": self.queues,
            "concurrency": self.concurrency}})

    async def mark_stopped(self) -> None:
        from app.workers.persistence import get_db_connection, release_db_connection
        try:
            conn = await get_db_connection()
            try:
                await conn.execute(
                    "UPDATE job_workers SET status='stopped', draining=TRUE,"
                    " current_task_id=NULL, last_heartbeat_at=NOW() WHERE worker_id=$1",
                    self.worker_id)
            finally:
                await release_db_connection(conn)
        except Exception:
            pass

    # ----- background loops -----
    async def heartbeat_loop(self) -> None:
        from app.workers.persistence import get_db_connection, release_db_connection
        while not self.stop:
            try:
                conn = await get_db_connection()
                try:
                    await Q.worker_heartbeat(conn, worker_id=self.worker_id,
                                             draining=self.draining)
                finally:
                    await release_db_connection(conn)
            except Exception:
                pass
            await asyncio.sleep(self.worker_hb)

    async def reconcile_loop(self) -> None:
        """Recover expired-lease tasks (including this worker's own from a prior
        crash and any dead worker's). A task with COMPLETE durable output is
        reconciled to succeeded WITHOUT recompute; otherwise retryable/failed."""
        from app.workers.persistence import get_db_connection, release_db_connection
        while not self.stop:
            await asyncio.sleep(max(5, self.task_hb))
            for queue_name in self.queues:
                try:
                    conn = await get_db_connection()
                    try:
                        expired = await Q.find_expired_lease_tasks(conn, queue_name=queue_name)
                        for task in expired:
                            await self._reconcile_one(conn, task)
                    finally:
                        await release_db_connection(conn)
                except Exception:
                    pass

    async def _reconcile_one(self, conn, task: Dict[str, Any]) -> None:
        payload = _json_payload(task["payload"])
        spec = R.registered_task_types().get(task["task_type"])
        completed = None
        if spec is not None and spec.probe_fn is not None:
            try:
                completed = await spec.probe_fn(conn, payload)
            except Exception:
                completed = None
        if completed is not None:
            await Q.reconcile_task_to_succeeded(conn, task_id=task["id"], result_summary=completed)
        else:
            await Q.reconcile_task_to_retryable(conn, task_id=task["id"])
        if task["job_id"] is not None:
            try:
                await sync_prospective_campaign(conn, task["job_id"])
            except Exception:
                pass

    # ----- scheduler leader (runs inside the worker parent process) -----
    async def scheduler_loop(self) -> None:
        if not settings.JOB_SCHEDULER_ENABLED:
            return
        from app.jobs.scheduler import run_scheduler_tick
        while not self.stop:
            try:
                await run_scheduler_tick(worker_id=self.worker_id)
            except Exception:
                logger.debug("scheduler tick error", exc_info=True)
            await asyncio.sleep(30)

    # ----- main claim loop -----
    async def claim_loop(self) -> None:
        from app.workers.persistence import get_db_connection, release_db_connection
        while not self.stop and not self.draining:
            claimed = False
            for queue_name in self.queues:
                if self.stop or self.draining:
                    break
                conn = await get_db_connection()
                try:
                    task = await Q.claim_next_task(
                        conn, queue_name=queue_name, worker_id=self.worker_id,
                        lease_seconds=self.lease_seconds)
                finally:
                    await release_db_connection(conn)
                if task is not None:
                    claimed = True
                    await self.run_task(task)
                    break
            if not claimed:
                await asyncio.sleep(self.poll)

    async def run_task(self, task: Dict[str, Any]) -> None:
        from app.workers.persistence import get_db_connection, release_db_connection
        task_id = task["id"]
        job_id = task["job_id"]
        attempt = int(task["attempt_count"])
        payload = _json_payload(task["payload"])

        # Resolve handler (unknown/disabled → terminal, no child spawned).
        try:
            spec = R.resolve_handler(task["task_type"])
        except C.JobError as e:
            conn = await get_db_connection()
            try:
                await Q.settle_task_failure(conn, task_id=task_id, worker_id=self.worker_id,
                                            safe_error_code=e.safe_error_code,
                                            error_class=e.error_class, backoff_seconds_value=None)
                await self._sync(conn, job_id)
            finally:
                await release_db_connection(conn)
            return

        # mark running
        conn = await get_db_connection()
        try:
            await Q.mark_running(conn, task_id=task_id, worker_id=self.worker_id)
            await Q.record_event(conn, job_id=job_id, task_id=task_id, event_type="task_running",
                                 metadata={"worker_id": self.worker_id, "attempt": attempt})
        finally:
            await release_db_connection(conn)

        loop = asyncio.get_running_loop()
        started = loop.time()
        crashed = False
        result: Optional[Dict[str, Any]] = None
        assert self.executor is not None
        fut = loop.run_in_executor(self.executor, spec.child_callable, payload)
        lease_lost = False
        while True:
            done, _ = await asyncio.wait({fut}, timeout=self.task_hb)
            if fut in done:
                break
            # renew lease + heartbeat while CPU work runs
            conn = await get_db_connection()
            try:
                ok = await Q.renew_lease(conn, task_id=task_id, worker_id=self.worker_id,
                                         lease_seconds=self.lease_seconds)
                await Q.worker_heartbeat(conn, worker_id=self.worker_id, draining=self.draining)
                if not ok:
                    lease_lost = True
            finally:
                await release_db_connection(conn)
            if lease_lost:
                logger.warning("lease lost mid-run; abandoning finalize",
                               extra={"extra_data": {"task_id": str(task_id)}})
                break
        try:
            result = fut.result()
        except (BrokenExecutor, Exception) as e:  # hard child death
            crashed = True
            logger.warning("child process failed", extra={"extra_data": {
                "task_id": str(task_id), "error": type(e).__name__}})
            self._recycle_executor_if_broken()

        duration_ms = int((loop.time() - started) * 1000)

        if lease_lost:
            # Another owner (reconciler) will finalize; do not clobber.
            return

        conn = await get_db_connection()
        try:
            # ownership guard: a reconciler may have reclaimed us
            still = await conn.fetchval(
                "SELECT 1 FROM job_tasks WHERE id=$1 AND lease_owner=$2 AND status IN ('leased','running')",
                task_id, self.worker_id)
            if not still:
                await self._sync(conn, job_id)
                return
            if crashed or result is None:
                await self._finalize_crash(conn, spec, task_id, job_id, payload, duration_ms)
            elif result.get("ok"):
                await Q.complete_task_succeeded(
                    conn, task_id=task_id, worker_id=self.worker_id,
                    result_summary=_bound(result.get("result") or {}), duration_ms=duration_ms)
            else:
                await self._finalize_failure(conn, spec, task_id, job_id, payload, result,
                                             attempt, duration_ms)
            await self._sync(conn, job_id)
        finally:
            await release_db_connection(conn)

    async def _finalize_crash(self, conn, spec, task_id, job_id, payload, duration_ms) -> None:
        completed = None
        if spec.probe_fn is not None:
            try:
                completed = await spec.probe_fn(conn, payload)
            except Exception:
                completed = None
        if completed is not None:  # crash-after-persist → reconcile succeeded
            await Q.reconcile_task_to_succeeded(conn, task_id=task_id, result_summary=completed)
        else:
            await Q.settle_task_failure(
                conn, task_id=task_id, worker_id=self.worker_id,
                safe_error_code="worker_process_interrupted", error_class=C.ERR_RETRYABLE,
                backoff_seconds_value=0, lease_lost=True, duration_ms=duration_ms)

    async def _finalize_failure(self, conn, spec, task_id, job_id, payload, result,
                                attempt, duration_ms) -> None:
        error_class = result.get("error_class", C.ERR_RETRYABLE)
        safe_code = str(result.get("safe_error_code", "task_failed"))[:80]
        if error_class == C.ERR_RETRYABLE and spec.probe_fn is not None:
            try:
                completed = await spec.probe_fn(conn, payload)
            except Exception:
                completed = None
            if completed is not None:  # already durably complete → succeed
                await Q.reconcile_task_to_succeeded(conn, task_id=task_id, result_summary=completed)
                return
        backoff = C.backoff_seconds(attempt, schedule=list(settings.JOB_RETRY_BACKOFF_SECONDS)) \
            if error_class == C.ERR_RETRYABLE else None
        await Q.settle_task_failure(
            conn, task_id=task_id, worker_id=self.worker_id, safe_error_code=safe_code,
            error_class=error_class, backoff_seconds_value=backoff, duration_ms=duration_ms)

    async def _sync(self, conn, job_id) -> None:
        if job_id is None:
            return
        try:
            await sync_prospective_campaign(conn, job_id)
        except Exception:
            pass

    def _recycle_executor_if_broken(self) -> None:
        try:
            if self.executor is not None:
                self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        self.executor = ProcessPoolExecutor(max_workers=self.concurrency)

    # ----- run -----
    async def run(self) -> None:
        self.executor = ProcessPoolExecutor(max_workers=self.concurrency)
        await self.register()
        self._install_signals()
        bg = [asyncio.create_task(self.heartbeat_loop()),
              asyncio.create_task(self.reconcile_loop()),
              asyncio.create_task(self.scheduler_loop())]
        try:
            await self.claim_loop()
        finally:
            self.stop = True
            for t in bg:
                t.cancel()
            await asyncio.gather(*bg, return_exceptions=True)
            if self.executor is not None:
                self.executor.shutdown(wait=True, cancel_futures=False)
            await self.mark_stopped()
            from app.deps import close_db_pool
            await close_db_pool()

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()

        def _drain(signame: str) -> None:
            logger.info("received %s: draining", signame)
            self.draining = True
            self.stop = True
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _drain, sig.name)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, lambda *_: _drain("signal"))


def _bound(d: Dict[str, Any]) -> Dict[str, Any]:
    """Defensive bound on a result summary (schema also caps it to 8KB)."""
    import json
    s = json.dumps(d, default=str)
    if len(s) <= 7000:
        return d
    return {"truncated": True, "keys": sorted(list(d.keys()))[:20]}


def main() -> None:
    from app.utils.logging import setup_logging
    setup_logging()
    if not settings.JOB_WORKER_ENABLED:
        logger.error("JOB_WORKER_ENABLED is false; refusing to start the worker")
        raise SystemExit(2)
    worker = JobWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
