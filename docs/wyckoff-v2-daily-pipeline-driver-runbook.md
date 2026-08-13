# Daily-Pipeline Driver — Deploy, Readiness Gate & Enable Runbook

Operator runbook for the **durable daily-pipeline driver** (commit `7fcea64`):
scheduler → occurrence → `smart_scanner_daily_pipeline_advance.v1` → durable
worker → `advance_daily_pipeline_service` → history refresh → campaign →
outcome (current deferred / prior sweep) → audit → occurrence **succeeded**,
fully automatic with **no HTTP self-call, no `fly ssh`, no operator-held
`WORKER_TOKEN`**.

All steps below require access the automation does not hold and must be run by
the operator. Run them **in order**; do not enable the recurring schedule until
the readiness gate (§4) is fully green.

Prereq once per shell:

```bash
flyctl auth login                       # interactive; browser
export DRIVER_APP=smart-scanner-be-pipeline-driver-staging
export GITSHA=$(git rev-parse HEAD)     # 7fcea64… (driver commit)
```

---

## 1. Provision the least-privilege driver DB role (idempotent)

Against the **isolated staging Fly Postgres admin** connection (`$ADMIN_DSN`)
and target database (`$DBNAME`, e.g. `warmup`):

```bash
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 \
  -v pipeline_driver_password="$DRIVER_DB_PASSWORD" -v db_name="$DBNAME" \
  -f ops/sql/create_pipeline_driver.sql

psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/create_pipeline_driver_rls_policies.sql

# MUST print all assertions OK and exit 0:
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f ops/sql/verify_pipeline_driver.sql
```

`verify_pipeline_driver.sql` asserts the role flags are all false
(`NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOREPLICATION/NOBYPASSRLS/NOINHERIT`), it
has **no** write on bars/runs/pairs/evaluations/outcomes, it **has** the
required writes (registrations, `job_runs.UPDATE`, `job_tasks.INSERT+UPDATE`),
and it holds **no** `DELETE` anywhere. If any assertion fails, STOP.

Build the driver DSN (role `smart_scanner_pipeline_driver`, the password above)
for the next step — this is the ONLY place the password is used:

```bash
export DRIVER_DSN="postgresql://smart_scanner_pipeline_driver:${DRIVER_DB_PASSWORD}@<pg-host>:5432/${DBNAME}?sslmode=require"
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
prospective-runner or history-warmer role. `JOB_WORKER_EXPECTED_DB_ROLE` in the
toml makes the worker refuse to boot on any other `current_user`.

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

# 3c. Confirm the driver booted on the RIGHT role and is leader (no fly ssh needed):
flyctl logs -a "$DRIVER_APP" --no-tail | grep -E "current_user|scheduler.*leader|role" | tail
```

Expected: driver logs show `current_user = smart_scanner_pipeline_driver` and
"acquired scheduler leadership"; the other two apps log that the scheduler is
disabled.

---

## 4. Readiness gate — ALL must be green before §5

Run these read-only checks (via `$ADMIN_DSN` or the read-model). Do **not**
enable the recurring schedule unless every line passes.

| # | Gate | Check |
|---|------|-------|
| 1 | Driver role correct | §1 verifier exited 0. |
| 2 | Driver worker healthy | `flyctl status -a "$DRIVER_APP"` → 1 machine `started`, no crash loop. |
| 3 | Sole scheduler leader | driver logs show leadership; eval+outcome logs show scheduler disabled. |
| 4 | Frozen universe ready | the production universe is `frozen` with a stable `universe_hash`. |
| 5 | Daily + 4H fresh | every universe symbol has `daily_bars` through the latest completed session and completed `market_bars_4h` (no stale symbols → history_refresh completes, not blocks). |
| 6 | 3 green campaigns | `SELECT count(*) FROM prospective_campaign_registrations WHERE status='completed'` ≥ 3 (incl. `9470418d`, see recovery evidence doc). |
| 7 | Live auto-advance proof | §4a below drove a throwaway occurrence to `succeeded` end-to-end. |

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
succeeded`. When the driver task reads `succeeded`, tear down the proof:

```sql
DELETE FROM job_schedules WHERE schedule_code='PROOF-DAILY-PIPELINE';
-- (the proof occurrence job_runs/job_tasks are terminal & harmless; leave for audit)
```

If the proof does not reach `succeeded`, STOP and diagnose via
`flyctl logs -a "$DRIVER_APP"` and the occurrence `stage_states` — do **not**
enable production.

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
