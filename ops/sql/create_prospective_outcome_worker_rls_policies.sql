-- =============================================================================
-- RLS policies for smart_scanner_prospective_outcome_worker
-- =============================================================================
-- Data-plane: full-row SELECT on the 7 read relations; the writable outcome
-- relation (strategy_shadow_pair_outcomes) gets a full-row SELECT PLUS the
-- NARROWEST campaign-scoped INSERT/UPDATE policies, mirroring
-- create_shadow_outcome_maintainer_rls_policies.sql's exact predicate style
-- (experiment_code='wyckoff_v2_vs_baseline' AND a persisted
-- run_pairs -> runs.telemetry.campaign.campaign_id link — membership is NEVER
-- inferred from symbol or date). strategy_shadow_outcome_runs gets
-- unconditional policies (run rows are not pair-scoped).
--
-- Queue-plane: job_runs/job_tasks get a QUEUE-SCOPED policy
-- (queue_name = 'prospective_outcomes') — STRONGER than
-- smart_scanner_prospective_worker's full-row policies, and the actual
-- privilege mechanism behind "must not claim prospective-evaluation tasks":
-- this role is structurally unable to SELECT/UPDATE a 'prospective' queue
-- row even if its app-level JOB_WORKER_QUEUES were ever misconfigured.
-- job_task_attempts/job_events/job_workers/job_schedules/job_dependencies
-- stay full-row, mirroring ops/sql/create_job_queue_rls_policies.sql's
-- pattern (an attempt/event row only ever gets created for a task_id this
-- role already legitimately claimed via the job_tasks policy above it).
--
-- Run as the table owner against the ISOLATED database. Never
-- enables/disables/forces RLS on a pre-existing table's current state
-- (job_* tables already have RLS enabled by migration 018), never grants
-- BYPASSRLS, never touches ownership or another role's policies.
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
      RETURN;
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

-- ---- queue-plane: job_runs/job_tasks are QUEUE-SCOPED (stronger than the
--      evaluation worker's full-row policies) — this is the actual privilege
--      mechanism behind "must not claim prospective-evaluation tasks": even
--      if this role's app-level JOB_WORKER_QUEUES were ever misconfigured,
--      it is structurally unable to SELECT/UPDATE a 'prospective' queue row,
--      so it cannot claim, see or mutate an evaluation task at the DB layer.
--      job_task_attempts/job_events/job_workers/job_schedules/job_dependencies
--      stay full-row (same shape as smart_scanner_prospective_worker) — an
--      attempt/event row only ever gets created for a task_id this role
--      already legitimately claimed via the job_tasks policy above it.
DO $$
DECLARE
  worker constant text := 'smart_scanner_prospective_outcome_worker';
  t text;
  full_row_rels constant text[] := ARRAY[
    'public.job_task_attempts', 'public.job_events', 'public.job_workers'];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = worker) THEN
    RAISE EXCEPTION 'role % does not exist (run create_prospective_outcome_worker.sql first)', worker;
  END IF;

  -- job_runs/job_tasks need a queue_name predicate (not the generic
  -- _ensure_policy full-row USING(true) shape) — created directly, idempotent
  -- via IF NOT EXISTS so a rerun never errors and never silently widens scope.
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'public.job_runs'::regclass
                 AND polname = worker||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_runs AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name = ''prospective_outcomes'') '
      'WITH CHECK (queue_name = ''prospective_outcomes'')',
      worker||'_qscope', worker);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'public.job_tasks'::regclass
                 AND polname = worker||'_qscope') THEN
    EXECUTE format(
      'CREATE POLICY %I ON public.job_tasks AS PERMISSIVE FOR ALL TO %I '
      'USING (queue_name = ''prospective_outcomes'') '
      'WITH CHECK (queue_name = ''prospective_outcomes'')',
      worker||'_qscope', worker);
  END IF;

  FOREACH t IN ARRAY full_row_rels LOOP
    PERFORM pg_temp._ensure_policy(t, worker||'_select', worker, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, worker||'_insert', worker, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, worker||'_update', worker, 'UPDATE');
  END LOOP;
  PERFORM pg_temp._ensure_policy('public.job_schedules', worker||'_select', worker, 'SELECT');
  PERFORM pg_temp._ensure_policy('public.job_schedules', worker||'_update', worker, 'UPDATE');
  PERFORM pg_temp._ensure_policy('public.job_dependencies', worker||'_select', worker, 'SELECT');
END
$$;

-- ---- data-plane: 7 read-only relations + the 2 write relations ----------
DO $$
DECLARE
  t           text;
  rel_oid     regclass;
  rec         record;
  worker_role constant text := 'smart_scanner_prospective_outcome_worker';
  read_rels   constant text[] := ARRAY[
    'public.prospective_campaign_registrations',
    'public.strategy_shadow_runs',
    'public.strategy_shadow_run_pairs',
    'public.strategy_shadow_pairs',
    'public.strategy_shadow_evaluations',
    'public.strategy_shadow_pair_outcomes',
    'public.daily_bars'
  ];
  outcome_rel constant text := 'public.strategy_shadow_pair_outcomes';
  runs_rel    constant text := 'public.strategy_shadow_outcome_runs';
  scope_pred  constant text :=
    'EXISTS (SELECT 1 FROM public.strategy_shadow_pairs p '
    'WHERE p.id = public.strategy_shadow_pair_outcomes.pair_id '
    'AND p.experiment_code = ''wyckoff_v2_vs_baseline'' '
    'AND EXISTS (SELECT 1 FROM public.strategy_shadow_run_pairs rp '
    'JOIN public.strategy_shadow_runs r ON r.id = rp.run_id '
    'WHERE rp.pair_id = p.id '
    'AND (r.telemetry -> ''campaign'' ->> ''campaign_id'') IS NOT NULL))';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = worker_role) THEN
    RAISE EXCEPTION 'role % does not exist (run create_prospective_outcome_worker.sql first)', worker_role;
  END IF;

  -- read SELECT policies (RLS must already be ON for these pre-existing tables)
  FOREACH t IN ARRAY read_rels LOOP
    rel_oid := to_regclass(t);
    IF rel_oid IS NULL THEN
      RAISE EXCEPTION 'required relation % is missing', t;
    END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      RAISE EXCEPTION 'RLS is not enabled on % — refusing to create an inert policy', t;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = worker_role||'_select') THEN
      EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR SELECT TO %I USING (true)',
                     worker_role||'_select', t, worker_role);
      RAISE NOTICE 'created SELECT policy on %', t;
    END IF;
  END LOOP;

  -- outcome INSERT (campaign-scoped WITH CHECK)
  rel_oid := to_regclass(outcome_rel);
  IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                 AND p.polname = worker_role||'_ins') THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (%s)',
                   worker_role||'_ins', outcome_rel, worker_role, scope_pred);
    RAISE NOTICE 'created INSERT policy on %', outcome_rel;
  END IF;

  -- outcome UPDATE (campaign-scoped USING + WITH CHECK)
  IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                 AND p.polname = worker_role||'_upd') THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR UPDATE TO %I USING (%s) WITH CHECK (%s)',
                   worker_role||'_upd', outcome_rel, worker_role, scope_pred, scope_pred);
    RAISE NOTICE 'created UPDATE policy on %', outcome_rel;
  END IF;

  -- run bookkeeping relation: unconditional policies (not pair-scoped)
  rel_oid := to_regclass(runs_rel);
  IF rel_oid IS NULL THEN
    RAISE EXCEPTION 'required relation % is missing', runs_rel;
  END IF;
  IF (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = worker_role||'_runs_select') THEN
      EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR SELECT TO %I USING (true)',
                     worker_role||'_runs_select', runs_rel, worker_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = worker_role||'_runs_ins') THEN
      EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)',
                     worker_role||'_runs_ins', runs_rel, worker_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = worker_role||'_runs_upd') THEN
      EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)',
                     worker_role||'_runs_upd', runs_rel, worker_role);
    END IF;
    RAISE NOTICE 'run-table policies ensured on %', runs_rel;
  ELSE
    RAISE NOTICE 'RLS off on % — grant governs, no policy needed', runs_rel;
  END IF;
END
$$;

\echo 'prospective outcome-worker RLS policies ensured (queue-plane + campaign-scoped outcome writes).'
