"""Durable PostgreSQL-backed job queue + dedicated worker.

PostgreSQL is the single source of truth for the queue (task claiming uses
SELECT ... FOR UPDATE SKIP LOCKED). This package is a GENERIC job/task framework
— it contains NO task-specific strategy math. Concrete work lives in typed
handlers registered in ``app.jobs.registry``; the first production handler
(``prospective_symbol_evaluation.v1``) reuses the pure shadow runner unchanged.

Modules:
  * contracts  — versions, statuses, error taxonomy, backoff, typed payloads.
  * identity   — deterministic idempotency-key derivation (sha256 canonical JSON).
  * queue      — the queue service (enqueue, claim, lease, heartbeat, finalize,
                 cancel, retry-failed, reconcile-expired) — no strategy logic.
  * registry   — the task-handler registry (unknown types fail terminally).
  * handlers   — concrete typed handlers (e.g. prospective symbol evaluation).
  * scheduler  — advisory-lock leader + cron/market_daily due evaluation.
  * worker     — `python -m app.jobs.worker`: poll → claim → run-in-child → finalize.
"""
