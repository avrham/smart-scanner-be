# Durable Job Queue + Dedicated Worker — Runbook

A generic, project-wide durable job/task queue backed entirely by PostgreSQL
(the DB is the source of truth) plus a dedicated worker process that never serves
HTTP. Task claiming uses `SELECT ... FOR UPDATE SKIP LOCKED`; CPU-bound strategy
math runs in an isolated child process; leases + append-only attempts guarantee
crash recovery with no permanently-`running` task. Additive only (migration
`app/db/migrations/018_durable_job_queue.sql`) — no existing table touched, no
strategy/outcome/provider logic changed. Apply ONLY to the isolated
non-production database — NEVER to shared Supabase.

## Queue data model (migration 018)
Seven new tables, RLS enabled on every one, all with bounded columns (no
unbounded free text or JSON):
- `job_runs` — one parent job (a unit of enqueued work, e.g. one campaign).
  Holds status, priority, per-status task counters, correlation/registration/
  campaign/schedule refs, `result_summary` (JSONB ≤ 16384 bytes).
  UNIQUE `idempotency_key`.
- `job_tasks` — durable per-item work units claimed by workers. Holds
  `payload` (JSONB ≤ 8192 bytes) + `payload_hash`, status, `priority`,
  `ordinal`, `available_at`, `attempt_count`/`max_attempts`,
  `operator_retry_eligible`, lease fields (`lease_owner`, `lease_acquired_at`,
  `lease_expires_at`, `heartbeat_at`), `error_class`, `result_summary`
  (≤ 8192 bytes). UNIQUE `(job_id, task_key)` and UNIQUE `idempotency_key`.
- `job_task_attempts` — append-only per-attempt audit. UNIQUE
  `(task_id, attempt_number)`; `result_summary` ≤ 8192 bytes.
- `job_events` — bounded state-transition event log (`event_type` ≤ 60,
  `safe_message` ≤ 500, `metadata` JSONB ≤ 4096 bytes).
- `job_workers` — worker identity + heartbeat/health (`worker_id` PK,
  `queue_names[]`, status, `last_heartbeat_at`, `current_task_id`, `draining`).
- `job_schedules` — recurring definitions (`cron` | `market_daily`). UNIQUE
  `(schedule_code, schedule_version)`.
- `job_dependencies` — minimal inter-job ordering (`succeeded` | `finished`).
  Intentionally minimal — NOT a full DAG engine.

Bounded-length checks: `job_type` ≤ 80, `job_contract_version`/
`task_contract_version` ≤ 80, `queue_name` ≤ 40, `safe_error_code` ≤ 80,
`task_key` ≤ 120, `idempotency_key` ≤ 200. JSON size caps: task `payload`
≤ 8192, task/attempt `result_summary` ≤ 8192, job `result_summary` ≤ 16384,
event `metadata` ≤ 4096. `attempt_count ≤ max_attempts + 1`; `max_attempts`
∈ [1,10].

## Job & task states (`app/jobs/contracts.py`)
Job states: `queued`, `running`, `succeeded`, `failed`, `cancel_requested`,
`cancelled`. Terminal: `succeeded`, `failed`, `cancelled`.
Task states: `queued`, `leased`, `running`, `retryable`, `succeeded`, `failed`,
`cancelled`.

## Task claiming (`app/jobs/queue.py` `claim_next_task`)
One SHORT transaction:
```sql
SELECT id FROM job_tasks
 WHERE queue_name = $1
   AND status IN ('queued','retryable')
   AND available_at <= NOW()
 ORDER BY priority DESC, ordinal ASC, created_at ASC
 FOR UPDATE SKIP LOCKED
 LIMIT 1;
```
In the SAME transaction: increment `attempt_count`, set `status='leased'`, set
`lease_owner` + `lease_acquired_at` + `lease_expires_at`, insert the
`job_task_attempts` row, and update the parent job counters. The transaction
COMMITS *before* any CPU work begins. The task handler (CPU work) then runs
OUTSIDE any transaction. `SKIP LOCKED` lets multiple workers claim disjoint
tasks without blocking; the partial claim index
(`queue_name, priority DESC, ordinal ASC, created_at ASC WHERE status IN
('queued','retryable')`) keeps the scan tight.

## Leases & heartbeats
Defaults (`app/config.py`):
- `JOB_TASK_LEASE_SECONDS=900`
- `JOB_WORKER_HEARTBEAT_SECONDS=15`
- `JOB_TASK_HEARTBEAT_SECONDS=30`
- `JOB_QUEUE_POLL_SECONDS=2`
- `JOB_MAX_ATTEMPTS_DEFAULT=3`
- `JOB_WORKER_STALE_SECONDS=90`

While the child runs the CPU work, the parent process renews the task lease
(`queue.renew_lease`). `renew_lease` returns `False` if the lease was lost
(reclaimed by reconciliation) — the worker then ABANDONS finalize and does not
persist a stale result.

Expired-lease reconciliation (for `leased`/`running` tasks past
`lease_expires_at`):
- NO durable output → `retryable` (or `failed` if attempts exhausted).
- COMPLETE durable output (pair + both arms) → reconciled to `succeeded`
  WITHOUT recompute.
- PARTIAL durable output → `retryable` + idempotent recovery on the next
  attempt.

No task stays permanently `running`: the lease TTL guarantees eventual
reclamation.

## Retry policy
Bounded exponential backoff `JOB_RETRY_BACKOFF_SECONDS=[60,300]`:
- after attempt 1 → wait 60s, then `retryable`
- after attempt 2 → wait 300s, then `retryable`
- after attempt 3 → terminal `failed`

Deterministic (jitter is applied only under a controlled test hook). Failure
classes (`error_class`): `retryable`, `terminal`, `operator_error`,
`cancelled`.
- Retryable examples: DB connection interruption, worker process interruption,
  transient persistence conflict, lease lost before durable completion.
- Terminal / operator examples: invalid registration, stale immutable identity,
  history not ready, future-bar leakage, strategy-contract mismatch, invalid
  task payload, auth/config error.

Contract / data-integrity errors are NEVER retried indefinitely — they fail
terminally so a human (operator retry) is required.

## Idempotency (`app/jobs/identity.py`)
Deterministic sha256 keys make exact replay a no-op (enforced by the UNIQUE
constraints on `job_runs.idempotency_key`, `job_tasks.idempotency_key`, and
`job_tasks(job_id, task_key)`):
- `job_idempotency_key(job_type, registration_identity, campaign_execution_identity)`
- task key binds registration identity + campaign execution identity + symbol +
  snapshot identity + candidate identity + control identity
- `schedule_occurrence_idempotency_key(schedule_code, version, occurrence)`

## Attempt history
`job_task_attempts` is append-only: `attempt_number`, `worker_id`, `status`,
`started_at`/`finished_at`, `safe_error_code`, `duration_ms`, `lease_lost`,
`result_summary`. NO raw exception traces and NO secrets are ever stored.

## Events
`job_events` is a bounded state-transition log (`event_type` ≤ 60,
`safe_message` ≤ 500, `metadata` JSONB ≤ 4096 bytes), scoped to a job and/or a
task. Retention (documented, NOT DDL-enforced):
- prune `job_events` > 90 days
- prune `job_task_attempts` > 180 days
- terminal `job_runs`/`job_tasks` may be archived > 180 days

## Cancellation
`POST /api/admin/jobs/{id}/cancel` sets `cancel_requested_at` + status
`cancel_requested`. It stops the queue claiming further tasks for the job,
cancels remaining `queued`/`retryable` tasks, and ALLOWS the currently-running
atomic task to finish (it is never interrupted mid-write). It NEVER deletes
persisted output. Once all tasks are terminal the job settles to `cancelled`.

## Worker lifecycle (`app/jobs/worker.py`)
Run: `python -m app.jobs.worker`. The worker:
- registers a `worker_id` of form `type-hostname-pid-rand` in `job_workers`
- heartbeats into `job_workers` (`JOB_WORKER_HEARTBEAT_SECONDS`)
- polls ONLY the configured queues (`JOB_QUEUE_POLL_SECONDS`)
- claims one task at a time (concurrency 1)
- stops claiming when draining
- maintains task heartbeats while a task runs
- handles `SIGTERM`/`SIGINT` by draining
- recovers leases after a hard death (via expiry reconciliation)
- NEVER serves HTTP and NEVER depends on Fly Proxy
- logs structured, safe state transitions; NEVER logs secrets or evidence

## Task registry (`app/jobs/registry.py`)
`TASK_HANDLERS` maps task type → `HandlerSpec`. Unknown types fail terminally
(`UnknownTaskType`). Test handlers are selectable ONLY when
`JOB_ALLOW_TEST_HANDLERS=true` (never in production). Each handler exposes:
- a picklable, module-level `child_callable` that runs in the child process and
  returns a bounded result dict, and
- an async `probe_fn` used parent-side for the reconcile probe.

Handlers:
- `prospective_symbol_evaluation.v1` — live handler
  (`app/jobs/handlers/prospective.py`), reuses the pure shadow runner unchanged.
- `synthetic_test_task.v1` — synthetic, test-only handler.

## CPU isolation
Each task handler runs in a bounded
`ProcessPoolExecutor(max_workers=JOB_WORKER_CONCURRENCY=1)` child process. The
parent event loop only renews leases/heartbeats, handles shutdown, and detects
child failure (`BrokenProcessPool` → `retryable` after a reconcile probe).
Strategy math NEVER runs on the parent event loop.

## Scheduler leadership (`app/jobs/scheduler.py`)
`run_scheduler_tick` takes
`pg_try_advisory_lock(JOB_SCHEDULER_ADVISORY_LOCK_KEY=0x4A425343)` so exactly
one leader acts per tick. The leader inspects enabled + non-paused due schedules,
creates jobs via a deterministic per-occurrence idempotency key (so an occurrence
is never enqueued twice), advances `next_run_at`, and respects
`disabled`/`paused`. It NEVER runs task logic. It runs inside the worker parent
process.

## Schedule types
- `market_daily`: next occurrence = latest fully completed NYSE session close
  (16:00 America/New_York) + `market_close_delay_minutes`. Holiday & early-close
  aware, reusing `app.prospective_session`
  (`resolve_latest_completed_session` / `session_cutoff_utc` / `is_trading_day`).
- `cron`: a minimal 5-field cron (`min hour dom mon dow`) supporting
  lists/ranges/steps, resolved by a bounded one-year forward search.

## Roles & RLS
Roles created by `ops/sql/create_job_queue_roles.sql`; RLS policies by
`ops/sql/create_job_queue_rls_policies.sql`.
- `smart_scanner_prospective_worker` — claim/update tasks; write attempts,
  events, worker heartbeats; read local history; write prospective
  campaign/pairs/evaluations; advance schedules. NO bar/outcome/warmup writes,
  NO `DELETE`/`TRUNCATE`/DDL.
- `smart_scanner_job_enqueuer` — create jobs + tasks + events, cancel jobs,
  read schedules.
- `smart_scanner_job_audit_reader` — `SELECT`-only.

The existing web role `smart_scanner_prospective_runner` is extended with
bounded queue enqueue/cancel/schedule grants (it drives the management endpoints;
it never claims or executes tasks).

## Adding a new task type
1. Implement a module-level `child_callable(payload) -> {"ok": bool, ...}` that
   REUSES existing pure code (no new strategy math), plus an optional async
   `probe_fn(conn, payload)`.
2. Register a `HandlerSpec` in `app/jobs/registry._install_default_handlers`
   with `production_enabled=True`.
3. Add a typed payload/contract.
4. Add an enqueue service that creates the parent job + tasks ATOMICALLY with
   deterministic idempotency keys.
5. Extend the relevant least-privilege role + RLS grants.
6. Add tests.

## Operator endpoints
All require the worker token; none return unbounded lists.
- `GET /api/admin/jobs` — bounded filter (`job_type`, `status`,
  `created_after`, `limit`, `cursor`)
- `GET /api/admin/jobs/{id}`, `.../tasks`, `.../events`
- `POST /api/admin/jobs/{id}/cancel`
- `POST /api/admin/jobs/{id}/retry-failed` — only `operator_retry_eligible`
  terminal tasks; preserves prior attempts; payload is immutable
- `GET /api/admin/jobs/workers`
- Schedules: `GET`/`POST /api/admin/job-schedules`,
  `PATCH /api/admin/job-schedules/{id}`, `POST .../pause`, `POST .../resume`,
  `GET .../preview`
- Prospective enqueue: `POST /api/admin/prospective/jobs` (contract
  `prospective_campaign_enqueue.v1`)

The web API app serves the enqueue + job/schedule management endpoints; the
worker never serves HTTP.

## Monitoring queries
Queue depth per queue + status:
```sql
SELECT queue_name, status, COUNT(*)
  FROM job_tasks
 GROUP BY queue_name, status
 ORDER BY queue_name, status;
```
Active vs expired leases:
```sql
SELECT status,
       COUNT(*) FILTER (WHERE lease_expires_at >  NOW()) AS active,
       COUNT(*) FILTER (WHERE lease_expires_at <= NOW()) AS expired
  FROM job_tasks
 WHERE status IN ('leased','running')
 GROUP BY status;
```
Stale workers (heartbeat older than `JOB_WORKER_STALE_SECONDS`):
```sql
SELECT worker_id, status, last_heartbeat_at
  FROM job_workers
 WHERE last_heartbeat_at < NOW() - INTERVAL '90 seconds';
```
Last N events for a job:
```sql
SELECT created_at, event_type, safe_message
  FROM job_events
 WHERE job_id = $1
 ORDER BY created_at DESC
 LIMIT 50;
```
Per-job counters: `GET /api/admin/jobs/{id}` (or select the
`*_task_count` columns from `job_runs`).

## Incident recovery
- Worker crash → lease expiry reconciliation: `retryable` (recompute) or
  reconcile-to-`succeeded` when durable output is complete.
- A task stuck `running` forever is impossible — the lease TTL bounds it.
- Duplicate enqueue → `already_queued` (UNIQUE idempotency key).
- Completed replay → `already_applied` (no new writes, no provider).
- Terminal-but-eligible task → operator retry (`retry-failed`) preserving prior
  attempts.
- A job with a terminally-failed task settles `failed`; its registration remains
  recoverable and is NEVER marked completed.

## Data retention
See **Events** above: prune `job_events` > 90 days; prune `job_task_attempts`
> 180 days; terminal `job_runs`/`job_tasks` may be archived > 180 days.
Documented policy, not DDL-enforced.

## Fly deployment
The dedicated worker runs as its own Fly app,
`smart-scanner-be-prospective-worker-staging`, using `fly.worker.toml`:
- NO `[http_service]`
- `[processes] worker = "python -m app.jobs.worker"`
- `shared-cpu-1x` / 1GB, region `fra`, runs continuously (no auto-stop)
- Secrets: `JOB_WORKER_DATABASE_URL` (worker-role DSN) + `WORKER_TOKEN` ONLY.
  NO provider key, NO Supabase credential.

Deploy:
```bash
fly deploy -c fly.worker.toml --build-arg APP_GIT_SHA=$(git rev-parse HEAD)
```
The web API app `smart-scanner-be-prospective-staging` serves the enqueue +
job/schedule management endpoints; the worker never serves HTTP and never
depends on Fly Proxy. On deploy the worker drains on `SIGTERM`: the current child
finishes when possible; otherwise the lease expires and the task retries. A
restarted worker registers a fresh heartbeat and reclaims expired work.

## Cost model
Isolated DB (`shared-cpu-1x`, 1GB volume) + the always-on worker
(`shared-cpu-1x`/1GB, ~$3–4/mo) + the auto-stopping web app — total well under
$15/month. A resize to `shared-cpu-2x`/1GB is authorized ONLY if CPU profiling
shows a meaningful benefit AND the total stays < $15/mo.

## Daily-pipeline orchestrator (`app/jobs/daily_pipeline.py`)
The disabled `SMART-SCANNER-DAILY-PIPELINE` `market_daily` template (seeded
`enabled=false` in migration 018, so the scheduler NEVER enqueues it yet)
documents the intended daily order: history refresh → prospective campaign →
outcome maturation → audit/reporting. A durable ORCHESTRATOR FOUNDATION now
exists for this — it is a state machine, not yet a self-driving process:

- ONE `job_runs` row per occurrence (`job_type='smart_scanner_daily_pipeline'`,
  `queue_name='daily_pipeline'` — a distinct queue NAME in the SAME tables,
  never a second queue system). Stage progress lives in that row's
  `result_summary` JSONB.
- Occurrence identity (`ident.pipeline_occurrence_identity`) is deterministic
  over `schedule_code`, `schedule_version`, `resolved_session_date`,
  `frozen_universe_hash`, `pipeline_contract_version` — a repeated call for
  the same resolved session + universe always resumes the SAME occurrence
  (idempotent `ON CONFLICT (idempotency_key) DO NOTHING`); a later completed
  session, or a genuinely different universe, is a new one.
- `ensure_pipeline_occurrence` / `record_stage_result` / `get_pipeline_occurrence`
  / `latest_pipeline_occurrence` / `build_status_view` implement create-or-
  resume, stage advancement (`pending → in_progress → completed`, or
  `blocked` / `retryable_failure` / `terminal_failure`), and the bounded
  operator status view (`GET /api/admin/daily-pipeline/status`, no secrets).
  A `terminal_failure` freezes `current_stage` at the blocked stage and marks
  the occurrence `failed` — it never silently advances past a failed stage. A
  stale write against an already-passed stage is ignored (never rewinds state).
- Stage EXECUTION (actually calling the history-warmup incremental-refresh
  endpoint, the prospective register/enqueue endpoints, the outcome-
  maturation enqueue endpoint, and the audit endpoints) is intentionally
  NOT performed by this module — `record_stage_result` takes the stage's
  outcome as an argument. This keeps the state machine testable without live
  HTTP/DB-role dependencies, and separates "did stage N succeed" from "how do
  we run stage N", exactly like this repo's existing split between queue
  mechanics and task handlers.

**Not yet wired**: an autonomous driver that calls the stage endpoints and
feeds their results into `record_stage_result` from inside the worker
process. The four component endpoints (history-warmup, prospective register/
enqueue, prospective outcome enqueue, prospective audit) live on THREE
different Fly apps, each behind its own `WORKER_TOKEN` — confirmed
experimentally to be per-app-distinct secrets (a token from one app returns
401 against another). Wiring autonomous cross-app self-execution requires
either a dedicated internal-auth mechanism or per-app token secrets
installed on the worker process; neither has been provisioned, consistent
with this project's existing credential-boundary discipline (worker roles
and their DSNs are also never shared across apps). Until then, an operator
(or an external script with multi-app credentials) drives the pipeline by
calling `ensure_pipeline_occurrence`, running each stage's existing endpoint,
and calling `record_stage_result` with the outcome — the exact same
sequence a future autonomous driver would perform, just invoked externally.

To adopt full autonomy, introduce these as queue handlers (each per the
"Adding a new task type" checklist — reuse existing pure code, no new
strategy math) OR wire an internal stage-executor with the cross-app tokens
above:
- `history_universe_refresh.v1` — refresh the frozen-universe daily/4H history
  (now: `app.history_warmup_execute` incremental-refresh functions, exposed at
  `POST /api/admin/history-warmup/incremental/execute`).
- `prospective_daily_campaign.v1` — enqueue the daily prospective campaign
  (`POST /api/admin/prospective/register` + `.../jobs`).
- `outcome_maturation.v1` — mature forward outcomes for prior snapshots
  (`POST /api/admin/prospective/outcomes/jobs`).
- `daily_quality_audit.v1` — reconcile + assert daily quality invariants
  (`GET /api/admin/prospective/audit`).

Enable the schedule template only after an autonomous driver exists AND the
outcome-worker role/Fly app (see "Roles & RLS" and the prospective-outcome-
worker sections) are live — never before.
