-- =============================================================================
-- RLS policies for smart_scanner_prospective_runner
-- =============================================================================
-- When RLS is enabled on a relation, a grant alone is inert without a policy.
-- This owner-run script adds, PER ROLE, the minimal policies:
--   * SELECT-only policy on the read relations;
--   * SELECT + INSERT policy on the insert-only queue relations (job_tasks,
--     job_events — this role only ever enqueues, never claims/mutates);
--   * SELECT + INSERT + UPDATE policies on the campaign write relations,
--     including job_runs (NO DELETE policy for anyone).
-- It never enables/forces RLS elsewhere, never grants BYPASSRLS, never touches
-- ownership or other roles' policies. Rerunnable. Run as the table owner.
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
  role_name constant text := 'smart_scanner_prospective_runner';
  t         text;
  rel       regclass;
  read_rels constant text[] := ARRAY[
    'public.daily_bars','public.market_bars_4h','public.patterns','public.pattern_configs',
    'public.history_warmup_universes','public.history_warmup_universe_symbols',
    'public.strategy_shadow_pair_outcomes','public.strategy_shadow_outcome_runs',
    'public.job_workers'];
  insert_only_rels constant text[] := ARRAY['public.job_tasks','public.job_events'];
  write_rels constant text[] := ARRAY[
    'public.prospective_campaign_registrations','public.strategy_shadow_runs',
    'public.strategy_shadow_run_pairs','public.strategy_shadow_pairs',
    'public.strategy_shadow_evaluations','public.job_runs'];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
    RAISE EXCEPTION 'role % does not exist (run create_shadow_prospective_runner.sql first)', role_name;
  END IF;

  FOREACH t IN ARRAY read_rels LOOP
    rel := to_regclass(t);
    IF rel IS NULL THEN CONTINUE; END IF;
    IF (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
      IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_select') THEN
        EXECUTE format('CREATE POLICY prospective_runner_select ON %s AS PERMISSIVE FOR SELECT TO %I USING (true)', t, role_name);
      END IF;
    END IF;
  END LOOP;

  FOREACH t IN ARRAY insert_only_rels LOOP
    rel := to_regclass(t);
    IF rel IS NULL THEN CONTINUE; END IF;
    IF (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
      IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_select') THEN
        EXECUTE format('CREATE POLICY prospective_runner_select ON %s AS PERMISSIVE FOR SELECT TO %I USING (true)', t, role_name);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_insert') THEN
        EXECUTE format('CREATE POLICY prospective_runner_insert ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', t, role_name);
      END IF;
    END IF;
  END LOOP;

  FOREACH t IN ARRAY write_rels LOOP
    rel := to_regclass(t);
    IF rel IS NULL THEN CONTINUE; END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
      EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', t);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_select') THEN
      EXECUTE format('CREATE POLICY prospective_runner_select ON %s AS PERMISSIVE FOR SELECT TO %I USING (true)', t, role_name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_insert') THEN
      EXECUTE format('CREATE POLICY prospective_runner_insert ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', t, role_name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel AND polname='prospective_runner_update') THEN
      EXECUTE format('CREATE POLICY prospective_runner_update ON %s AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)', t, role_name);
    END IF;
    RAISE NOTICE 'prospective_runner write policies ensured on %', t;
  END LOOP;
END
$$;
