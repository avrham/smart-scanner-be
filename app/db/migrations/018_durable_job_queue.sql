-- ===========================================================================
-- 018_durable_job_queue.sql
-- ===========================================================================
-- Additive: a GENERIC, project-wide durable job/task queue backed entirely by
-- PostgreSQL (the queue is the source of truth; task claiming uses
-- SELECT ... FOR UPDATE SKIP LOCKED). Seven new tables, no touch to any existing
-- table, no strategy/outcome/provider logic, no existing row. The first
-- production task type (prospective_symbol_evaluation.v1) reuses the pure shadow
-- runner unchanged; this migration only stores queue state, leases, attempts,
-- events, worker heartbeats and schedules. No role/grant here (ops/sql owns
-- those). Apply ONLY to the isolated non-production database — NEVER to shared
-- Supabase.
--
-- Retention (documented in the task-system runbook; not enforced by DDL):
--   * job_events  — bounded state-transition log; prune > 90 days.
--   * job_task_attempts — append-only audit; prune > 180 days.
--   * job_runs / job_tasks — terminal rows may be archived > 180 days.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- A. job_runs — one parent job (a unit of enqueued work; e.g. one campaign)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type TEXT NOT NULL,
  job_contract_version TEXT NOT NULL,
  queue_name TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued','running','succeeded','failed',
                      'cancel_requested','cancelled')),
  priority INT NOT NULL DEFAULT 100,
  registration_id UUID,
  campaign_id UUID,
  schedule_id UUID,
  correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
  requested_by TEXT,
  total_task_count INT NOT NULL DEFAULT 0,
  queued_task_count INT NOT NULL DEFAULT 0,
  running_task_count INT NOT NULL DEFAULT 0,
  succeeded_task_count INT NOT NULL DEFAULT 0,
  retryable_task_count INT NOT NULL DEFAULT 0,
  failed_task_count INT NOT NULL DEFAULT 0,
  cancelled_task_count INT NOT NULL DEFAULT 0,
  cancel_requested_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  safe_error_code TEXT,
  result_summary JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- bounded job type / contract / idempotency (no unbounded free text)
  CONSTRAINT job_runs_type_len CHECK (char_length(job_type) BETWEEN 1 AND 80),
  CONSTRAINT job_runs_contract_len CHECK (char_length(job_contract_version) BETWEEN 1 AND 80),
  CONSTRAINT job_runs_queue_len CHECK (char_length(queue_name) BETWEEN 1 AND 40),
  CONSTRAINT job_runs_idem_len CHECK (char_length(idempotency_key) BETWEEN 1 AND 200),
  CONSTRAINT job_runs_error_len CHECK (safe_error_code IS NULL OR char_length(safe_error_code) <= 80),
  -- bounded result JSON size (no raw strategy evidence duplication)
  CONSTRAINT job_runs_result_size CHECK (
    result_summary IS NULL OR pg_column_size(result_summary) <= 16384),
  CONSTRAINT job_runs_counts_nonneg CHECK (
    total_task_count >= 0 AND queued_task_count >= 0 AND running_task_count >= 0
    AND succeeded_task_count >= 0 AND retryable_task_count >= 0
    AND failed_task_count >= 0 AND cancelled_task_count >= 0),
  CONSTRAINT job_runs_idempotency_unique UNIQUE (idempotency_key)
);
CREATE INDEX IF NOT EXISTS job_runs_status_idx
  ON public.job_runs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS job_runs_type_idx
  ON public.job_runs (job_type, created_at DESC);
CREATE INDEX IF NOT EXISTS job_runs_registration_idx
  ON public.job_runs (registration_id);

-- ---------------------------------------------------------------------------
-- B. job_tasks — durable per-item work units claimed by workers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID NOT NULL REFERENCES public.job_runs(id),
  queue_name TEXT NOT NULL,
  task_type TEXT NOT NULL,
  task_contract_version TEXT NOT NULL,
  task_key TEXT NOT NULL,
  ordinal INT NOT NULL,
  payload JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued','leased','running','retryable',
                      'succeeded','failed','cancelled')),
  priority INT NOT NULL DEFAULT 100,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempt_count INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  operator_retry_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  lease_owner TEXT,
  lease_acquired_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  safe_error_code TEXT,
  error_class TEXT
    CHECK (error_class IS NULL OR error_class IN
           ('retryable','terminal','operator_error','cancelled')),
  result_summary JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT job_tasks_type_len CHECK (char_length(task_type) BETWEEN 1 AND 80),
  CONSTRAINT job_tasks_contract_len CHECK (char_length(task_contract_version) BETWEEN 1 AND 80),
  CONSTRAINT job_tasks_queue_len CHECK (char_length(queue_name) BETWEEN 1 AND 40),
  CONSTRAINT job_tasks_key_len CHECK (char_length(task_key) BETWEEN 1 AND 120),
  CONSTRAINT job_tasks_idem_len CHECK (char_length(idempotency_key) BETWEEN 1 AND 200),
  CONSTRAINT job_tasks_error_len CHECK (safe_error_code IS NULL OR char_length(safe_error_code) <= 80),
  CONSTRAINT job_tasks_attempts_bounded CHECK (
    attempt_count >= 0 AND max_attempts BETWEEN 1 AND 10
    AND attempt_count <= max_attempts + 1),
  CONSTRAINT job_tasks_ordinal_nonneg CHECK (ordinal >= 0),
  -- bounded payload/result JSON size
  CONSTRAINT job_tasks_payload_size CHECK (pg_column_size(payload) <= 8192),
  CONSTRAINT job_tasks_result_size CHECK (
    result_summary IS NULL OR pg_column_size(result_summary) <= 8192),
  CONSTRAINT job_tasks_job_key_unique UNIQUE (job_id, task_key),
  CONSTRAINT job_tasks_idempotency_unique UNIQUE (idempotency_key)
);
-- The claim query filters queue_name + status + available_at, ordered by
-- priority/ordinal/created_at; this partial index keeps SKIP LOCKED scans tight.
CREATE INDEX IF NOT EXISTS job_tasks_claim_idx
  ON public.job_tasks (queue_name, priority DESC, ordinal ASC, created_at ASC)
  WHERE status IN ('queued','retryable');
CREATE INDEX IF NOT EXISTS job_tasks_job_idx
  ON public.job_tasks (job_id, ordinal ASC);
CREATE INDEX IF NOT EXISTS job_tasks_lease_idx
  ON public.job_tasks (status, lease_expires_at)
  WHERE status IN ('leased','running');

-- ---------------------------------------------------------------------------
-- C. job_task_attempts — append-only per-attempt audit (no traces/secrets)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES public.job_tasks(id),
  attempt_number INT NOT NULL,
  worker_id TEXT,
  status TEXT NOT NULL
    CHECK (status IN ('leased','running','succeeded','retryable',
                      'failed','cancelled')),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  safe_error_code TEXT,
  duration_ms INT,
  lease_lost BOOLEAN NOT NULL DEFAULT FALSE,
  result_summary JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT job_task_attempts_num_pos CHECK (attempt_number >= 1),
  CONSTRAINT job_task_attempts_error_len CHECK (
    safe_error_code IS NULL OR char_length(safe_error_code) <= 80),
  CONSTRAINT job_task_attempts_result_size CHECK (
    result_summary IS NULL OR pg_column_size(result_summary) <= 8192),
  CONSTRAINT job_task_attempts_unique UNIQUE (task_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS job_task_attempts_task_idx
  ON public.job_task_attempts (task_id, attempt_number ASC);

-- ---------------------------------------------------------------------------
-- D. job_events — bounded state-transition event log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID REFERENCES public.job_runs(id),
  task_id UUID REFERENCES public.job_tasks(id),
  event_type TEXT NOT NULL,
  safe_message TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT job_events_type_len CHECK (char_length(event_type) BETWEEN 1 AND 60),
  CONSTRAINT job_events_msg_len CHECK (safe_message IS NULL OR char_length(safe_message) <= 500),
  CONSTRAINT job_events_meta_size CHECK (metadata IS NULL OR pg_column_size(metadata) <= 4096),
  CONSTRAINT job_events_scope CHECK (job_id IS NOT NULL OR task_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS job_events_job_idx
  ON public.job_events (job_id, created_at ASC);
CREATE INDEX IF NOT EXISTS job_events_task_idx
  ON public.job_events (task_id, created_at ASC);

-- ---------------------------------------------------------------------------
-- E. job_workers — worker identity + heartbeat/health
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_workers (
  worker_id TEXT PRIMARY KEY,
  worker_type TEXT NOT NULL,
  queue_names TEXT[] NOT NULL,
  deployed_git_sha TEXT,
  hostname TEXT,
  status TEXT NOT NULL DEFAULT 'starting'
    CHECK (status IN ('starting','idle','busy','draining','stopped')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  current_task_id UUID,
  draining BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB,
  CONSTRAINT job_workers_type_len CHECK (char_length(worker_type) BETWEEN 1 AND 40),
  CONSTRAINT job_workers_meta_size CHECK (metadata IS NULL OR pg_column_size(metadata) <= 4096)
);
CREATE INDEX IF NOT EXISTS job_workers_heartbeat_idx
  ON public.job_workers (status, last_heartbeat_at DESC);

-- ---------------------------------------------------------------------------
-- F. job_schedules — recurring job definitions (cron | market_daily)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_schedules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  schedule_code TEXT NOT NULL,
  schedule_version INT NOT NULL DEFAULT 1,
  schedule_type TEXT NOT NULL CHECK (schedule_type IN ('cron','market_daily')),
  timezone TEXT NOT NULL DEFAULT 'America/New_York',
  cron_expression TEXT,
  market_close_delay_minutes INT,
  job_type TEXT NOT NULL,
  job_contract_version TEXT NOT NULL,
  payload_template JSONB,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  next_run_at TIMESTAMPTZ,
  last_enqueued_at TIMESTAMPTZ,
  last_job_id UUID,
  idempotency_scope TEXT NOT NULL DEFAULT 'occurrence',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT job_schedules_code_len CHECK (char_length(schedule_code) BETWEEN 1 AND 80),
  CONSTRAINT job_schedules_cron_len CHECK (cron_expression IS NULL OR char_length(cron_expression) <= 120),
  CONSTRAINT job_schedules_delay_bounded CHECK (
    market_close_delay_minutes IS NULL
    OR market_close_delay_minutes BETWEEN 0 AND 1440),
  CONSTRAINT job_schedules_template_size CHECK (
    payload_template IS NULL OR pg_column_size(payload_template) <= 8192),
  -- shape rules per schedule_type
  CONSTRAINT job_schedules_type_fields CHECK (
    (schedule_type = 'cron' AND cron_expression IS NOT NULL)
    OR (schedule_type = 'market_daily' AND market_close_delay_minutes IS NOT NULL)),
  CONSTRAINT job_schedules_code_version_unique UNIQUE (schedule_code, schedule_version)
);
CREATE INDEX IF NOT EXISTS job_schedules_due_idx
  ON public.job_schedules (enabled, paused, next_run_at)
  WHERE enabled = TRUE AND paused = FALSE;

-- ---------------------------------------------------------------------------
-- G. job_dependencies — minimal inter-job ordering (NOT a full DAG engine)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.job_dependencies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID NOT NULL REFERENCES public.job_runs(id),
  depends_on_job_id UUID NOT NULL REFERENCES public.job_runs(id),
  dependency_condition TEXT NOT NULL DEFAULT 'succeeded'
    CHECK (dependency_condition IN ('succeeded','finished')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT job_dependencies_not_self CHECK (job_id <> depends_on_job_id),
  CONSTRAINT job_dependencies_unique UNIQUE (job_id, depends_on_job_id)
);

-- ---------------------------------------------------------------------------
-- RLS: enabled on every new table (policies are created by ops/sql per role).
-- ---------------------------------------------------------------------------
ALTER TABLE public.job_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_task_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_workers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.job_dependencies ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Disabled daily-pipeline schedule TEMPLATE (market_daily). Inert: enabled=false
-- so the scheduler NEVER enqueues it in this task. Documents the intended future
-- daily stages in its payload_template. Idempotent seed (safe to re-run).
-- Enable only after the history + outcome handlers adopt this queue framework.
-- ---------------------------------------------------------------------------
INSERT INTO public.job_schedules (
  schedule_code, schedule_version, schedule_type, timezone,
  market_close_delay_minutes, job_type, job_contract_version, payload_template,
  enabled, paused, idempotency_scope)
VALUES (
  'SMART-SCANNER-DAILY-PIPELINE', 1, 'market_daily', 'America/New_York',
  30, 'smart_scanner_daily_pipeline', 'smart_scanner_daily_pipeline.v1',
  '{"documentation":"disabled template — intended future daily stages",
    "stages":["history_universe_refresh.v1","readiness_verification",
              "prospective_daily_campaign.v1","outcome_maturation.v1",
              "daily_quality_audit.v1"],
    "note":"not enabled until history + outcome handlers use the shared queue"}'::jsonb,
  FALSE, FALSE, 'occurrence')
ON CONFLICT (schedule_code, schedule_version) DO NOTHING;
