# Daily-Pipeline Driver — Deploy, Readiness Gate & Enable Runbook

Operator runbook for the **durable daily-pipeline driver**:
scheduler → occurrence → `smart_scanner_daily_pipeline_advance.v1` → durable
worker → `advance_daily_pipeline_service` → history refresh → campaign →
outcome (current deferred / prior sweep) → audit → occurrence **succeeded**,
fully automatic with **no HTTP self-call, no `fly ssh`, no operator-held
`WORKER_TOKEN`**.

> **Updated after the first live proof.** The initial live proof FAILED and
> exposed two backend defects, now fixed:
> - **History refresh is now AUTOMATIC (Root Cause A).** When the frozen
>   universe's daily/4H history is stale, the driver enqueues (or recognizes)
>   ONE durable `history_incremental_refresh` child job; a dedicated
>   **history-refresh worker** — the only automated component besides the
>   history-warmup HTTP app that holds the Massive credential — runs the
>   provider-backed refresh per symbol. The pipeline WAITS on that child
>   (stage `in_progress` / `waiting_on: history_refresh_job`, **not** BLOCKED),
>   re-checks readiness, then advances. There is **no** manual
>   `POST /history-warmup/incremental/execute` step in the automated path.
>   This adds a **new deployment step (§1b + §3f)**: the history-refresh worker.
> - **Driver retry budget matches its `max_attempts` (Root Cause B).** The
>   driver task now carries its own backoff schedule so it can defer
>   (`occurrence_in_progress`) across its full 10-attempt budget instead of
>   terminally failing at attempt 3 (the global 2-entry backoff list cap).
>   Existing task types keep their bounded two-retry default.

All steps below require access the automation does not hold and must be run by
the operator. Run them **in order**; do not enable the recurring schedule until
the readiness gate (§4) is fully green.

Prereq once per shell:

```bash
flyctl auth login                       # interactive; browser
export DRIVER_APP=smart-scanner-be-pipeline-driver-staging
export HREFRESH_APP=smart-scanner-be-history-refresh-worker-staging
export GITSHA=$(git rev-parse HEAD)     # the current driver + history-refresh commit
```

---

## 1. Provision the least-privilege driver DB role (idempotent)

Against the **isolated staging Fly Postgres admin** connection (`$ADMIN_DSN`)
and target database (`$DBNAME`, e.g. `warmup`). Apply ALL of these DB steps —
driver role, **driver RLS upgrade**, **history-refresh worker grants** (§1b),
and the **strengthened verifier** — BEFORE deploying either worker (§3, §3f).
The RLS script is **convergent** (DROP+CREATE of the queue-scoped policy), so a
live DB carrying the older four-queue predicate is upgraded to the five-queue
set that now includes `history_incremental_refresh` — a plain re-run would
otherwise leave the driver RLS-blocked from the new history queue.

```bash
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 \
  -v pipeline_driver_password="$DRIVER_DB_PASSWORD" -v db_name="$DBNAME" \
  -f ops/sql/create_pipeline_driver.sql

# CONVERGENT: upgrades an existing (e.g. four-queue) qscope policy in place.
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/create_pipeline_driver_rls_policies.sql

# MUST print all assertions OK and exit 0 — now includes EFFECTIVE queue-scope:
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/verify_pipeline_driver.sql
```

`verify_pipeline_driver.sql` asserts the role flags are all false
(`NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOREPLICATION/NOBYPASSRLS/NOINHERIT`), it
has **no** write on bars/runs/pairs/evaluations/outcomes, it **has** the
required writes (registrations, `job_runs.UPDATE`, `job_tasks.INSERT+UPDATE`),
it holds **no** `DELETE` anywhere, AND — new — that the qscope RLS policy on
`job_runs`/`job_tasks` permits **exactly** the five queues incl
`history_incremental_refresh` (it FAILS on a stale four-queue predicate, so a
non-converged live upgrade is caught here rather than at the next live proof).
If any assertion fails, STOP.

> **Pre-deploy DB gate order (all green BEFORE §3/§3f deploy any worker):**
> 1. `create_pipeline_driver.sql` (create/update the driver role)
> 2. `create_pipeline_driver_rls_policies.sql` (convergent driver RLS)
> 3. `create_history_refresh_worker_grants.sql` (§1b — warmer queue grants/RLS)
> 4. `verify_pipeline_driver.sql` (driver role + effective five-queue scope)
> 5. `verify_history_refresh_worker.sql` (warmer combined contract + effective scope)
> → ONLY THEN deploy the driver (§3) and the history-refresh worker (§3f).
>
> Deploying against a stale/non-converged policy would make the driver's
> history-refresh enqueue (or the warmer's claim) silently RLS-fail at the next
> live proof. The production schedule stays disabled and `PROOF-DAILY-PIPELINE`
> stays paused throughout; the failed-proof rows are audit evidence — never
> modified or deleted.

Build the driver DSN (role `smart_scanner_pipeline_driver`, the password above)
for the next step — this is the ONLY place the password is used:

```bash
export DRIVER_DSN="postgresql://smart_scanner_pipeline_driver:${DRIVER_DB_PASSWORD}@<pg-host>:5432/${DBNAME}?sslmode=require"
```

---

## 1b. Grant the history-refresh worker its durable-queue access (Root Cause A)

The history-refresh worker REUSES the existing least-privilege
`smart_scanner_history_warmer` role (it already writes daily/4H bars +
`history_warmup_runs` and pairs with the Massive credential). It only LACKS
job-queue access; add it, queue-scoped to `history_incremental_refresh` (idempotent):

```bash
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/create_history_refresh_worker_grants.sql
```

Expected tail: `history-refresh worker grants configured.` This grants the
warmer role SELECT/INSERT/UPDATE on `job_runs`/`job_tasks` **RLS-scoped to the
one queue** (it can never claim a prospective/outcome/driver task) plus
attempts/events/workers; NO `job_schedules`/`job_dependencies`, NO `DELETE`.
The qscope policy is **convergent** (DROP+CREATE), same as the driver's. The
driver's own RLS scope already includes `history_incremental_refresh` (it
enqueues + reads the child job) via `create_pipeline_driver_rls_policies.sql`.

Then verify the combined warmer contract (role safety + effective queue scope +
required queue-plane privileges + intact history-warmup data-plane + forbidden
strategy/campaign writes) — MUST exit 0:

```bash
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/verify_history_refresh_worker.sql
```

`verify_history_refresh_worker.sql` asserts the `smart_scanner_history_warmer`
qscope on `job_runs`/`job_tasks` permits **exactly** `history_incremental_refresh`
(effective RLS, not just grants) — so a stale/wrong predicate FAILS here — while
confirming the existing history-warmup data-plane writes remain intact and
strategy/campaign writes stay forbidden. If any assertion fails, STOP.

Build the warmer DSN for the worker secret (§3f) — the warmer password is the
one set when `create_shadow_history_warmer.sql` was applied; never printed:

```bash
read -rs WARMER_DB_PASSWORD; echo; echo "captured ${#WARMER_DB_PASSWORD} chars"
export WARMER_DSN="postgresql://smart_scanner_history_warmer:${WARMER_DB_PASSWORD}@<pg-host>:5432/${DBNAME}?sslmode=require"
```

---

## 2. Create the driver app + secrets (once)

```bash
flyctl apps create "$DRIVER_APP" --org <org>          # skip if it already exists
flyctl secrets set -a "$DRIVER_APP" \
  JOB_WORKER_DATABASE_URL="$DRIVER_DSN"               # the driver-role DSN, a SECRET
# WORKER_TOKEN is a non-secret placeholder already in the toml (non-HTTP worker).
```

`JOB_WORKER_DATABASE_URL` must be the **driver role** DSN — never the
prospective-runner or history-warmer role.

> **Identity is not self-enforced at boot.** `JOB_WORKER_EXPECTED_DB_ROLE`
> (`smart_scanner_pipeline_driver` in the toml) is a **declared/configured
> expectation only** — as of this implementation it is **not** wired to a
> worker boot-time hard gate, so the worker will **not** refuse to start if the
> DSN connects as a different `current_user`. The authoritative identity gate is
> the explicit live verification in §3d + the role/RLS verifier SQL (§1):
> `SELECT current_user`, the elevated-role-flag checks, the visible-queue check,
> the forbidden-write probe, and `verify_pipeline_driver.sql`. Treat those as
> the source of truth; do not rely on `JOB_WORKER_EXPECTED_DB_ROLE` to stop a
> misconfigured DSN.

---

## 3. Deploy the driver, then flip the scheduler leader (order matters)

The driver worker is the **sole** durable-queue scheduler leader. The
evaluation + outcome worker tomls already set `JOB_SCHEDULER_ENABLED='false'`;
redeploy them so no two apps contend for the advisory lock.

```bash
# 3a. Deploy the driver (also becomes scheduler leader).
flyctl deploy -a "$DRIVER_APP" -c fly.pipeline-driver.toml \
  --build-arg APP_GIT_SHA="$GITSHA"

# 3b. Redeploy the eval + outcome workers with the scheduler OFF.
flyctl deploy -a smart-scanner-be-prospective-worker-staging \
  -c fly.worker.toml --build-arg APP_GIT_SHA="$GITSHA"
flyctl deploy -a smart-scanner-be-prospective-outcome-worker-staging \
  -c fly.prospective-outcome-worker.toml --build-arg APP_GIT_SHA="$GITSHA"

# 3c. Confirm the driver machine is up (no fly ssh needed):
flyctl status -a "$DRIVER_APP"
```

Expected: one machine `started` (process group `worker`), no crash loop. The
worker registers a `worker registered` log line on boot; there is **no**
`current_user`/"acquired leadership" boot assertion to grep for — identity and
leadership are verified explicitly in §3d and §3e, not inferred from logs.

---

## 3d. Driver DB identity + queue-isolation verification (authoritative)

This is the real identity gate (see the §2 note). Run it after §3 and before
the readiness gate.

**Role-specific connection — as the driver role itself** (via `flyctl proxy` if
`<pg-host>` is not directly reachable):

```bash
# optional: flyctl proxy 15432:5432 -a "$PG_APP"   # then point DRIVER_DSN host at 127.0.0.1:15432
psql "$DRIVER_DSN"
```

At the driver-role prompt:

```sql
SELECT current_user;
SELECT DISTINCT queue_name FROM job_tasks ORDER BY 1;
-- forbidden-write probe: the driver must NOT be able to write market bars
INSERT INTO daily_bars(symbol,trading_date,open,high,low,close,volume)
  VALUES ('__RLS_PROBE__', DATE '2000-01-03', 1,1,1,1,1);
```

Expected:

```
 current_user  ->  smart_scanner_pipeline_driver
 queue_name    ->  ONLY a subset of:
                   daily_pipeline_driver / daily_pipeline / prospective / prospective_outcomes
 INSERT        ->  ERROR:  permission denied for table daily_bars
```

**STOP CONDITIONS** — abort (and roll back per Rollback) if any hold:
- `current_user` ≠ `smart_scanner_pipeline_driver`;
- **any** visible `queue_name` outside the four listed queues;
- the `daily_bars` INSERT **succeeds** (least-privilege is broken).

**Admin-side worker identity/heartbeat** — separate admin/owner connection
(`$ADMIN_DSN`), kept apart from the role-specific checks above:

```sql
SELECT worker_type, deployed_git_sha, status, last_heartbeat_at
FROM   job_workers
WHERE  worker_type = 'pipeline_driver'
ORDER  BY last_heartbeat_at DESC;
```

Expected: exactly one row, `worker_type = pipeline_driver`, `deployed_git_sha`
equals `$GITSHA`, `status` in `idle`/`busy`, and `last_heartbeat_at` fresh
(within the last ~minute). **STOP CONDITION:** wrong/stale SHA, no row, or a
stale heartbeat → the deployed driver is not the build you verified.

---

## 3f. Deploy the history-refresh worker (Root Cause A)

The driver enqueues history refresh but never runs a provider — this worker
does, and it is the ONLY new component that carries the Massive credential.

```bash
flyctl apps create "$HREFRESH_APP" --org <org>            # skip if it exists
flyctl secrets set -a "$HREFRESH_APP" \
  JOB_WORKER_DATABASE_URL="$WARMER_DSN" \
  MASSIVE_API_KEY="$MASSIVE_API_KEY"                       # values never printed
flyctl deploy -a "$HREFRESH_APP" -c fly.history-refresh-worker.toml \
  --build-arg APP_GIT_SHA="$GITSHA"
flyctl status -a "$HREFRESH_APP"
```

Verify identity + queue isolation exactly as in §3d but for this worker
(connect as `smart_scanner_history_warmer`):

```sql
SELECT current_user;                                        -- smart_scanner_history_warmer
SELECT DISTINCT queue_name FROM job_tasks ORDER BY 1;       -- only history_incremental_refresh visible
```

Admin-side (`$ADMIN_DSN`): a fresh `history_refresh` worker heartbeat on `$GITSHA`:

```sql
SELECT worker_type, deployed_git_sha, status, last_heartbeat_at
FROM   job_workers WHERE worker_type = 'history_refresh';
```

**STOP CONDITIONS:** `current_user` ≠ `smart_scanner_history_warmer`; any
visible `queue_name` other than `history_incremental_refresh`; missing
`MASSIVE_API_KEY` secret (the worker cannot refresh); or no fresh heartbeat.
The Massive key must be present on THIS app and the history-warmup HTTP app
ONLY — never on the driver / prospective / outcome apps (confirm with
`flyctl secrets list` on each).

---

## 3e. Scheduler-leader verification

The scheduler leader tick runs **inside** the driver worker and acquires a
Postgres advisory lock **per tick, releasing it at the end of each tick**.
Because the lock is transient, a `pg_locks` snapshot is **not** a reliable proof
of leadership and must **not** be used as the gate. Verify by configuration +
liveness instead.

**Configuration — exactly one app schedules** (`NORMAL TERMINAL`):

```bash
for A in "$DRIVER_APP" \
         smart-scanner-be-prospective-worker-staging \
         smart-scanner-be-prospective-outcome-worker-staging; do
  echo "== $A =="; flyctl config env -a "$A" 2>/dev/null | grep JOB_SCHEDULER_ENABLED
done
```

Expected:
- driver app (`$DRIVER_APP`) → `JOB_SCHEDULER_ENABLED = true`;
- prospective **evaluation** worker (`smart-scanner-be-prospective-worker-staging`) → `false`;
- prospective **outcome** worker (`smart-scanner-be-prospective-outcome-worker-staging`) → `false`.

**Liveness — one fresh `pipeline_driver` heartbeat** (`$ADMIN_DSN`):

```sql
SELECT worker_type, status, last_heartbeat_at
FROM   job_workers WHERE worker_type = 'pipeline_driver';
```

Expected: exactly one heartbeating `pipeline_driver` worker (the process that
runs the leader tick).

**STOP CONDITION:** more than one app with `JOB_SCHEDULER_ENABLED=true` (two
leaders can contend), or no fresh `pipeline_driver` heartbeat → fix the
tomls/redeploy before proceeding.

---

## 4. Readiness gate — ALL must be green before §5

Run these read-only checks (via `$ADMIN_DSN` or the read-model). Do **not**
enable the recurring schedule unless every line passes.

| # | Gate | Check |
|---|------|-------|
| 1 | Driver role correct | §1 verifier exited 0. |
| 2 | Driver worker healthy | `flyctl status -a "$DRIVER_APP"` → 1 machine `started`, no crash loop. |
| 3 | Driver identity + isolation | §3d green: `current_user = smart_scanner_pipeline_driver`, only the four allowed queues visible, `daily_bars` INSERT denied, fresh `pipeline_driver` row on `$GITSHA`. |
| 4 | Sole scheduler leader | §3e green: only `$DRIVER_APP` has `JOB_SCHEDULER_ENABLED=true`; both prospective workers `false`; exactly one fresh `pipeline_driver` heartbeat. |
| 5 | History-refresh worker healthy | §3f green: `flyctl status -a "$HREFRESH_APP"` → 1 machine `started`; `current_user = smart_scanner_history_warmer`; only `history_incremental_refresh` queue visible; fresh `history_refresh` heartbeat on `$GITSHA`; `MASSIVE_API_KEY` present on this app + history-warmup ONLY. |
| 6 | Frozen universe ready | the production universe is `frozen` with a stable `universe_hash`. |
| 7 | Provider reachable for refresh | the history-refresh worker can reach Massive (the driver itself needs no provider). History does **not** need to be pre-fresh: if stale, the driver auto-enqueues a refresh child and waits. A `history_incremental_refresh` job that lands `failed` (e.g. provider auth) is the truthful blocker to fix here. |
| 8 | 3 green campaigns | `SELECT count(*) FROM prospective_campaign_registrations WHERE status='completed'` ≥ 3 (incl. `9470418d`, see recovery evidence doc). |
| 9 | Live auto-advance proof | §4a below drove a throwaway occurrence to `succeeded` **and** §4a's machine-driven attestation passed. |

### 4a. Live auto-advance proof (throwaway schedule — proves the loop before arming production)

Insert a **disabled-by-default, one-shot** schedule pointing at the frozen
universe with `next_run_at` in the past, let the live driver materialise +
advance it, confirm `succeeded`, then remove it. This proves scheduler →
driver-task → advance → succeeded on the real infra **without** touching the
real `SMART-SCANNER-DAILY-PIPELINE` row.

```sql
-- insert throwaway occurrence (paused=FALSE, enabled=TRUE, distinct schedule_code)
INSERT INTO job_schedules(id, schedule_code, schedule_version, schedule_type, timezone,
  market_close_delay_minutes, job_type, job_contract_version, enabled, paused,
  payload_template, idempotency_scope, next_run_at)
VALUES (gen_random_uuid(), 'PROOF-DAILY-PIPELINE', 1, 'market_daily', 'America/New_York', 30,
  'smart_scanner_daily_pipeline', 'smart_scanner_daily_pipeline.v2', TRUE, FALSE,
  jsonb_build_object('contract_version','smart_scanner_daily_pipeline.v2',
                     'universe_id','<FROZEN_UNIVERSE_ID>',
                     'universe_hash','<FROZEN_UNIVERSE_HASH>'),
  'occurrence', NOW() - INTERVAL '1 minute');
```

Watch (the driver's scheduler tick fires within its interval):

```sql
-- ONE marker + ONE driver task on the driver queue for the proof occurrence
SELECT status, queue_name FROM job_runs
WHERE  job_type='smart_scanner_daily_pipeline' AND schedule_id =
       (SELECT id FROM job_schedules WHERE schedule_code='PROOF-DAILY-PIPELINE');
SELECT status, attempt_count FROM job_tasks
WHERE  task_type='smart_scanner_daily_pipeline_advance.v1';
```

The driver will `history_refresh → campaign` (enqueues `prospective` eval
tasks; eval worker processes them) then, on a later claim, `outcome → audit →
succeeded`.

**Machine-driven attestation — prove the DRIVER advanced it, not an operator.**
Before tearing the proof down, confirm the advance came from the durable worker
(not a human `POST /daily-pipeline/advance` and not `fly ssh`). Admin/owner
connection (`$ADMIN_DSN`):

```sql
-- (a) the durable advance task exists and was claimed/advanced by a pipeline_driver worker
SELECT t.task_type, t.status, t.attempt_count, w.worker_type, w.deployed_git_sha
FROM   job_tasks t
LEFT   JOIN job_workers w
       ON w.current_task_id = t.id OR w.worker_type = 'pipeline_driver'
WHERE  t.task_type = 'smart_scanner_daily_pipeline_advance.v1';

-- (b) scheduler-origin + stage progress recorded against the proof occurrence
SELECT e.event_type, e.safe_message, e.created_at
FROM   job_events e
JOIN   job_runs r ON r.id = e.job_id
JOIN   job_schedules s ON s.id = r.schedule_id
WHERE  s.schedule_code = 'PROOF-DAILY-PIPELINE'
ORDER  BY e.created_at;
```

Expected:
- (a) the `smart_scanner_daily_pipeline_advance.v1` task is present and its work
  is attributable to a `worker_type = pipeline_driver` worker deployed at
  `$GITSHA` — no operator claimed or advanced it;
- (b) the occurrence shows a `job_scheduled` event with
  `safe_message = PROOF-DAILY-PIPELINE` (scheduler origin), followed by stage
  progress — i.e. the state machine advanced under the worker, not a manual call.

Optional log corroboration (`NORMAL TERMINAL`):

```bash
flyctl logs -a "$DRIVER_APP" --no-tail \
  | grep -E "smart_scanner_daily_pipeline_advance|worker registered" | tail
```

**Operator attestation:** for this proof occurrence you issued **no**
`POST /api/admin/daily-pipeline/advance` and **no** `flyctl ssh` — advancement
came solely from the durable pipeline-driver worker.

When the driver task reads `succeeded` and the attestation above holds, tear
down the proof:

```sql
DELETE FROM job_schedules WHERE schedule_code='PROOF-DAILY-PIPELINE';
-- (the proof occurrence job_runs/job_tasks are terminal & harmless; leave for audit.
--  the driver role holds no DELETE, so run this teardown on the admin connection.)
```

If the proof does not reach `succeeded`, or the machine-driven attestation fails
(the advance is attributable to a manual call rather than the pipeline_driver
worker), STOP and diagnose via `flyctl logs -a "$DRIVER_APP"` and the occurrence
`stage_states` — do **not** enable production.

---

## 5. Enable ONLY the production schedule

With the gate green, arm the single recurring schedule — nothing else:

```sql
UPDATE job_schedules
SET    enabled = TRUE,
       paused  = FALSE,
       job_contract_version = 'smart_scanner_daily_pipeline.v2',
       payload_template = jsonb_build_object(
         'contract_version','smart_scanner_daily_pipeline.v2',
         'universe_id','<FROZEN_UNIVERSE_ID>',
         'universe_hash','<FROZEN_UNIVERSE_HASH>')
WHERE  schedule_code = 'SMART-SCANNER-DAILY-PIPELINE';
```

The `payload_template.universe_id` is what makes the scheduler materialise the
**driver task** (not just the inert marker); without it the schedule stays
backward-compatible and never auto-advances.

---

## 6. Post-enable verification

After the next scheduled fire (or set `next_run_at = NOW()` once to force one):

```sql
-- exactly one occurrence per fire, on the driver queue, advancing/terminal
SELECT r.status, r.queue_name, r.created_at
FROM   job_runs r
JOIN   job_schedules s ON s.id = r.schedule_id
WHERE  s.schedule_code='SMART-SCANNER-DAILY-PIPELINE'
ORDER  BY r.created_at DESC LIMIT 3;

-- the driver task advanced (succeeded, or retryable while async work runs)
SELECT status, attempt_count, last_error
FROM   job_tasks WHERE task_type='smart_scanner_daily_pipeline_advance.v1'
ORDER  BY created_at DESC LIMIT 3;
```

Green = a fresh occurrence appears each fire, the driver task advances, and the
occurrence reaches `succeeded` (no duplicate campaign/pairs/evaluations for the
session — same-session replay is idempotent).

---

## Rollback

- **Disarm production**: `UPDATE job_schedules SET paused=TRUE WHERE
  schedule_code='SMART-SCANNER-DAILY-PIPELINE';` — the scheduler stops
  materialising new occurrences immediately; in-flight occurrences finish or
  defer cleanly.
- **Stop the driver entirely**: `flyctl scale count 0 -a "$DRIVER_APP"`. No
  scheduler leader remains; nothing auto-advances. Re-arm by scaling back to 1.
- No data mutation is required to roll back; the driver only enqueues and
  advances durable state.
