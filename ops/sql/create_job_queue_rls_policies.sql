-- =============================================================================
-- RLS policies for the Durable Job Queue roles
-- =============================================================================
-- Migration 018 ENABLEs RLS on every job_* table, so a grant is inert without a
-- policy. This owner-run, rerunnable script adds the minimal per-role policies:
--   * worker         : full task lifecycle on the queue + the same data-plane
--                      read/write policies the runner has (so it can evaluate a
--                      symbol) — NO DELETE policy anywhere.
--   * prospective_runner (web API): enqueue/cancel/retry + schedule mgmt + read.
--   * enqueuer       : create jobs/tasks/events, request cancel, read schedules.
--   * audit reader   : SELECT-only on every queue table.
-- Never grants BYPASSRLS, never touches ownership, never adds a DELETE policy.
-- Run as the table owner against the ISOLATED database.
-- =============================================================================

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION pg_temp._ensure_policy(
    tbl text, pol text, role text, cmd text) RETURNS void AS $fn$
DECLARE
  rel regclass := to_regclass(tbl);
BEGIN
  IF rel IS NULL THEN RETURN; END IF;
  IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
    -- Only force RLS on the job_* queue tables (already enabled by 018); for the
    -- pre-existing data tables we respect whatever RLS state they already have.
    IF tbl LIKE 'public.job_%' THEN
      EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
    ELSE
      RETURN;  -- data table without RLS: grants alone govern access
    END IF;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = rel AND polname = pol) THEN
    RETURN;
  END IF;
  IF cmd = 'INSERT' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)',
                   pol, tbl, role);
  ELSIF cmd = 'ALL' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)',
                   pol, tbl, role);
  ELSE
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR %s TO %I USING (true)',
                   pol, tbl, cmd, role);
  END IF;
END
$fn$ LANGUAGE plpgsql;

DO $$
DECLARE
  worker constant text := 'smart_scanner_prospective_worker';
  runner constant text := 'smart_scanner_prospective_runner';
  enq    constant text := 'smart_scanner_job_enqueuer';
  aud    constant text := 'smart_scanner_job_audit_reader';
  t text;
  -- worker data-plane tables (read + write), mirroring the runner's policies
  data_read constant text[] := ARRAY[
    'public.daily_bars','public.market_bars_4h','public.patterns','public.pattern_configs',
    'public.history_warmup_universes','public.history_warmup_universe_symbols',
    'public.strategy_shadow_pair_outcomes','public.strategy_shadow_outcome_runs'];
  data_write constant text[] := ARRAY[
    'public.prospective_campaign_registrations','public.strategy_shadow_runs',
    'public.strategy_shadow_run_pairs','public.strategy_shadow_pairs',
    'public.strategy_shadow_evaluations'];
  queue_rw constant text[] := ARRAY[
    'public.job_runs','public.job_tasks','public.job_task_attempts',
    'public.job_events','public.job_workers'];
  queue_all constant text[] := ARRAY[
    'public.job_runs','public.job_tasks','public.job_task_attempts','public.job_events',
    'public.job_workers','public.job_schedules','public.job_dependencies'];
BEGIN
  -- worker: data-plane
  FOREACH t IN ARRAY data_read LOOP
    PERFORM pg_temp._ensure_policy(t, worker||'_select', worker, 'SELECT');
  END LOOP;
  FOREACH t IN ARRAY data_write LOOP
    PERFORM pg_temp._ensure_policy(t, worker||'_select', worker, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, worker||'_insert', worker, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, worker||'_update', worker, 'UPDATE');
  END LOOP;

  -- worker: queue-plane
  FOREACH t IN ARRAY queue_rw LOOP
    PERFORM pg_temp._ensure_policy(t, worker||'_select', worker, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, worker||'_insert', worker, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, worker||'_update', worker, 'UPDATE');
  END LOOP;
  PERFORM pg_temp._ensure_policy('public.job_schedules', worker||'_select', worker, 'SELECT');
  PERFORM pg_temp._ensure_policy('public.job_schedules', worker||'_update', worker, 'UPDATE');
  PERFORM pg_temp._ensure_policy('public.job_dependencies', worker||'_select', worker, 'SELECT');

  -- prospective_runner (web API): enqueue/cancel/retry + schedule mgmt + reads
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = runner) THEN
    FOREACH t IN ARRAY ARRAY['public.job_runs','public.job_tasks','public.job_events'] LOOP
      PERFORM pg_temp._ensure_policy(t, runner||'_qselect', runner, 'SELECT');
      PERFORM pg_temp._ensure_policy(t, runner||'_qinsert', runner, 'INSERT');
      PERFORM pg_temp._ensure_policy(t, runner||'_qupdate', runner, 'UPDATE');
    END LOOP;
    PERFORM pg_temp._ensure_policy('public.job_task_attempts', runner||'_qselect', runner, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_workers', runner||'_qselect', runner, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_dependencies', runner||'_qselect', runner, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_schedules', runner||'_qselect', runner, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_schedules', runner||'_qinsert', runner, 'INSERT');
    PERFORM pg_temp._ensure_policy('public.job_schedules', runner||'_qupdate', runner, 'UPDATE');
  END IF;

  -- enqueuer
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = enq) THEN
    FOREACH t IN ARRAY ARRAY['public.job_runs','public.job_tasks'] LOOP
      PERFORM pg_temp._ensure_policy(t, enq||'_select', enq, 'SELECT');
      PERFORM pg_temp._ensure_policy(t, enq||'_insert', enq, 'INSERT');
      PERFORM pg_temp._ensure_policy(t, enq||'_update', enq, 'UPDATE');
    END LOOP;
    PERFORM pg_temp._ensure_policy('public.job_events', enq||'_select', enq, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_events', enq||'_insert', enq, 'INSERT');
    PERFORM pg_temp._ensure_policy('public.job_schedules', enq||'_select', enq, 'SELECT');
    PERFORM pg_temp._ensure_policy('public.job_workers', enq||'_select', enq, 'SELECT');
  END IF;

  -- audit reader: SELECT-only across every queue table
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = aud) THEN
    FOREACH t IN ARRAY queue_all LOOP
      PERFORM pg_temp._ensure_policy(t, aud||'_select', aud, 'SELECT');
    END LOOP;
  END IF;
END
$$;

\echo 'durable job-queue RLS policies ensured (worker / runner / enqueuer / audit reader).'
