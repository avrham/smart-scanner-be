-- =============================================================================
-- create_pipeline_driver_rls_policies.sql — RLS for smart_scanner_pipeline_driver
--
-- When RLS is enabled on a relation a grant alone is inert without a policy.
--   * job_runs/job_tasks are QUEUE-SCOPED to exactly the four queues the driver
--     legitimately touches: 'daily_pipeline_driver' (claim its own task),
--     'daily_pipeline' (create/advance the occurrence), 'prospective' (enqueue
--     the campaign job) and 'prospective_outcomes' (enqueue the outcome job).
--     It is structurally unable to see/claim any OTHER queue's rows.
--   * job_task_attempts/job_events/job_workers/job_schedules/job_dependencies
--     stay full-row (an attempt/event only ever exists for a task the driver
--     already legitimately touched).
--   * prospective_campaign_registrations: SELECT/INSERT/UPDATE (register +
--     status), full-row.
--   * the read relations get SELECT-only policies (advance reads them).
-- Idempotent; run after create_pipeline_driver.sql.
-- =============================================================================

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION pg_temp._ensure_policy(
    tbl text, pol text, role text, cmd text) RETURNS void AS $fn$
DECLARE
  rel regclass := to_regclass(tbl);
BEGIN
  IF rel IS NULL THEN RETURN; END IF;
  IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
    IF tbl LIKE 'public.job_%' THEN
      EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
    ELSE
      RETURN;  -- never create an inert policy on a non-RLS legacy relation
    END IF;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = rel AND polname = pol) THEN
    RETURN;
  END IF;
  IF cmd = 'INSERT' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', pol, tbl, role);
  ELSIF cmd = 'ALL' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)', pol, tbl, role);
  ELSE
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR %s TO %I USING (true)', pol, tbl, cmd, role);
  END IF;
END
$fn$ LANGUAGE plpgsql;

DO $$
DECLARE
  driver constant text := 'smart_scanner_pipeline_driver';
  queues constant text := '(''daily_pipeline_driver'',''daily_pipeline'',''prospective'',''prospective_outcomes'',''history_incremental_refresh'')';
  t text;
  full_row_rels constant text[] := ARRAY[
    'public.job_task_attempts', 'public.job_events', 'public.job_workers'];
  read_rels constant text[] := ARRAY[
    'public.daily_bars', 'public.market_bars_4h', 'public.patterns', 'public.pattern_configs',
    'public.strategy_shadow_runs', 'public.strategy_shadow_run_pairs', 'public.strategy_shadow_pairs',
    'public.strategy_shadow_evaluations', 'public.strategy_shadow_pair_outcomes',
    'public.strategy_shadow_outcome_runs', 'public.history_warmup_universes',
    'public.history_warmup_universe_symbols'];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = driver) THEN
    RAISE EXCEPTION 'role % does not exist (run create_pipeline_driver.sql first)', driver;
  END IF;

  -- queue-scoped job_runs/job_tasks (the actual privilege boundary)
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'public.job_runs'::regclass
                 AND polname = driver||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_runs AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
      driver||'_qscope', driver, queues, queues);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'public.job_tasks'::regclass
                 AND polname = driver||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_tasks AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
      driver||'_qscope', driver, queues, queues);
  END IF;

  FOREACH t IN ARRAY full_row_rels LOOP
    PERFORM pg_temp._ensure_policy(t, driver||'_select', driver, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, driver||'_insert', driver, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, driver||'_update', driver, 'UPDATE');
  END LOOP;
  PERFORM pg_temp._ensure_policy('public.job_schedules', driver||'_select', driver, 'SELECT');
  PERFORM pg_temp._ensure_policy('public.job_schedules', driver||'_update', driver, 'UPDATE');
  PERFORM pg_temp._ensure_policy('public.job_dependencies', driver||'_select', driver, 'SELECT');

  -- campaign registrations: register + status (full-row)
  PERFORM pg_temp._ensure_policy('public.prospective_campaign_registrations', driver||'_select', driver, 'SELECT');
  PERFORM pg_temp._ensure_policy('public.prospective_campaign_registrations', driver||'_insert', driver, 'INSERT');
  PERFORM pg_temp._ensure_policy('public.prospective_campaign_registrations', driver||'_update', driver, 'UPDATE');

  -- read relations (SELECT-only; only where RLS is already enabled)
  FOREACH t IN ARRAY read_rels LOOP
    PERFORM pg_temp._ensure_policy(t, driver||'_select', driver, 'SELECT');
  END LOOP;

  RAISE NOTICE 'pipeline-driver RLS ensured (queue-scoped job_runs/job_tasks + registrations + reads).';
END
$$;

\echo 'pipeline-driver RLS policies configured.'
