-- =============================================================================
-- create_history_refresh_worker_grants.sql — durable-queue access for the
-- history-refresh worker, which REUSES the existing least-privilege
-- smart_scanner_history_warmer role (Root Cause A).
--
-- That role already holds exactly the data-plane writes a provider-backed
-- incremental refresh needs (daily_bars, market_bars_4h, history_warmup_runs —
-- see create_shadow_history_warmer.sql) and the provider credential lives ONLY
-- on the history-warmup + history-refresh-worker apps. What it LACKS is any
-- durable job-queue access. This script adds ONLY that, queue-scoped to
-- 'history_incremental_refresh':
--   * job_runs / job_tasks: SELECT/INSERT/UPDATE, RLS-scoped to the one queue so
--     the worker can claim + finalize ONLY its own refresh tasks (it can never
--     see/claim a prospective, outcome, or driver task);
--   * job_task_attempts / job_events / job_workers: full-row (an attempt/event
--     only ever exists for a task the worker already legitimately touched;
--     job_workers holds its own registration/heartbeat).
-- It gets NO job_schedules / job_dependencies access and NO DELETE anywhere.
--
-- Apply (idempotent) AFTER create_shadow_history_warmer.sql:
--   psql "<admin conn>" -v db_name="warmup" -f ops/sql/create_history_refresh_worker_grants.sql
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_history_warmer') THEN
    RAISE EXCEPTION 'role smart_scanner_history_warmer does not exist '
                    '(run create_shadow_history_warmer.sql first)';
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO smart_scanner_history_warmer;

-- ---- queue-plane grants (RLS below scopes job_runs/job_tasks to one queue) ----
GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_history_warmer;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_history_warmer;
GRANT SELECT, INSERT, UPDATE ON public.job_task_attempts TO smart_scanner_history_warmer;
GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_history_warmer;
GRANT SELECT, INSERT, UPDATE ON public.job_workers       TO smart_scanner_history_warmer;

-- Never grant DELETE/TRUNCATE, job_schedules, or job_dependencies to this role.
REVOKE DELETE, TRUNCATE ON public.job_runs          FROM smart_scanner_history_warmer;
REVOKE DELETE, TRUNCATE ON public.job_tasks         FROM smart_scanner_history_warmer;
REVOKE DELETE, TRUNCATE ON public.job_task_attempts FROM smart_scanner_history_warmer;
REVOKE DELETE, TRUNCATE ON public.job_events        FROM smart_scanner_history_warmer;
REVOKE DELETE, TRUNCATE ON public.job_workers       FROM smart_scanner_history_warmer;

-- ---- RLS: queue-scope job_runs/job_tasks to 'history_incremental_refresh' -----
DO $$
DECLARE
  warmer constant text := 'smart_scanner_history_warmer';
  q      constant text := '(''history_incremental_refresh'')';
  t text;
  full_row_rels constant text[] := ARRAY[
    'public.job_task_attempts', 'public.job_events', 'public.job_workers'];
BEGIN
  -- job_runs / job_tasks are queue-scoped (the actual privilege boundary).
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid='public.job_runs'::regclass AND relrowsecurity) THEN
    EXECUTE 'ALTER TABLE public.job_runs ENABLE ROW LEVEL SECURITY';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid='public.job_runs'::regclass
                 AND polname=warmer||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_runs AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
      warmer||'_qscope', warmer, q, q);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid='public.job_tasks'::regclass AND relrowsecurity) THEN
    EXECUTE 'ALTER TABLE public.job_tasks ENABLE ROW LEVEL SECURITY';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid='public.job_tasks'::regclass
                 AND polname=warmer||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_tasks AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
      warmer||'_qscope', warmer, q, q);
  END IF;

  -- attempts/events/workers: full-row (only ever created for a task the worker
  -- already legitimately claimed on the scoped queue).
  FOREACH t IN ARRAY full_row_rels LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=t::regclass AND relrowsecurity) THEN
      EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', t);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=t::regclass AND polname=warmer||'_all') THEN
      EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR ALL TO %I '
                     'USING (true) WITH CHECK (true)', warmer||'_all', t, warmer);
    END IF;
  END LOOP;

  RAISE NOTICE 'history-refresh worker grants ensured (history_warmer queue-scoped to '
               'history_incremental_refresh; no schedules/dependencies; no DELETE).';
END
$$;

\echo 'history-refresh worker grants configured.'
