-- =============================================================================
-- Verify smart_scanner_prospective_outcome_worker is correctly least-privileged.
-- =============================================================================
-- Run CONNECTED AS smart_scanner_prospective_outcome_worker, AFTER applying
-- create_prospective_outcome_worker.sql and
-- create_prospective_outcome_worker_rls_policies.sql. Mostly read-only; the
-- queue-isolation section (6) inserts and then deletes exactly one throwaway
-- fixture row it owns (queue_name='prospective_outcomes') to prove claim
-- behavior, and cleans up after itself. No secrets here.
--   psql "<outcome-worker connection>" -f ops/sql/verify_prospective_outcome_worker.sql
-- =============================================================================

\set ON_ERROR_STOP on

SELECT current_user AS database_identity;

-- 1) Elevated attributes must all be false for this role.
SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
FROM pg_roles WHERE rolname = current_user;

-- 2) Read-only data-plane relations: SELECT present, all writes absent.
WITH req(relation) AS (
  VALUES ('public.prospective_campaign_registrations'),
         ('public.strategy_shadow_runs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_evaluations'),
         ('public.daily_bars')
)
SELECT relation FROM req
WHERE NOT has_table_privilege(relation,'SELECT')
   OR has_table_privilege(relation,'INSERT')
   OR has_table_privilege(relation,'UPDATE')
   OR has_table_privilege(relation,'DELETE')
   OR has_table_privilege(relation,'TRUNCATE');

-- 3) Write relations: SELECT+INSERT+UPDATE present; DELETE/TRUNCATE absent.
WITH req(relation) AS (
  VALUES ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_outcome_runs')
)
SELECT relation FROM req
WHERE NOT (has_table_privilege(relation,'SELECT')
           AND has_table_privilege(relation,'INSERT')
           AND has_table_privilege(relation,'UPDATE'))
   OR has_table_privilege(relation,'DELETE')
   OR has_table_privilege(relation,'TRUNCATE');

-- 4) Queue-plane relations: expected grant shape.
WITH req(relation, want_insert, want_update) AS (
  VALUES ('public.job_runs', true, true),
         ('public.job_tasks', true, true),
         ('public.job_task_attempts', true, true),
         ('public.job_events', true, true),
         ('public.job_workers', true, true),
         ('public.job_schedules', false, true),
         ('public.job_dependencies', false, false)
)
SELECT relation FROM req
WHERE NOT has_table_privilege(relation,'SELECT')
   OR has_table_privilege(relation,'DELETE')
   OR has_table_privilege(relation,'TRUNCATE')
   OR (want_insert AND NOT has_table_privilege(relation,'INSERT'))
   OR (want_update AND NOT has_table_privilege(relation,'UPDATE'))
   OR (NOT want_insert AND has_table_privilege(relation,'INSERT'))
   OR (NOT want_update AND has_table_privilege(relation,'UPDATE'));

-- 5) RLS visibility: this role must see ZERO rows of any pre-existing
--    'prospective' (evaluation) queue row, regardless of how many exist.
--    (Run this only after confirming (4) above returns no rows.)
SELECT count(*) AS visible_prospective_eval_rows
FROM public.job_runs WHERE queue_name = 'prospective';

SELECT count(*) AS visible_prospective_eval_task_rows
FROM public.job_tasks WHERE queue_name = 'prospective';

-- 6) Queue isolation, end to end: this role can create + see + claim + clean
--    up its OWN 'prospective_outcomes' fixture row, entirely inside one
--    transaction so a failure never leaves a stray row behind.
BEGIN;
  INSERT INTO public.job_runs (job_type, job_contract_version, queue_name, idempotency_key, status)
  VALUES ('prospective_outcome_maturation', 'prospective_outcome_maturation.v1',
          'prospective_outcomes', 'verify-fixture-outcome-worker-role', 'queued')
  RETURNING id AS fixture_job_id \gset

  INSERT INTO public.job_tasks (job_id, queue_name, task_type, task_contract_version,
      task_key, ordinal, payload, payload_hash, idempotency_key)
  VALUES (:'fixture_job_id', 'prospective_outcomes', 'prospective_outcome_maturation.v1',
          'prospective_outcome_maturation.v1', 'verify-fixture-task', 0, '{}'::jsonb,
          'sha256:fixture', 'verify-fixture-outcome-worker-task')
  RETURNING id AS fixture_task_id \gset

  -- must be visible and claimable (SKIP LOCKED claim shape, same as the worker)
  SELECT id FROM public.job_tasks
  WHERE id = :'fixture_task_id' AND queue_name = 'prospective_outcomes'
  FOR UPDATE SKIP LOCKED;

  UPDATE public.job_tasks SET status = 'succeeded' WHERE id = :'fixture_task_id';

ROLLBACK;  -- always rollback: this is a fixture probe, never a real row.

\echo 'prospective outcome-worker role verified (privileges + queue isolation).'
