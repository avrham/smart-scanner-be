-- =============================================================================
-- verify_history_refresh_worker.sql — combined-contract verifier for the
-- smart_scanner_history_warmer role, which is REUSED by BOTH the existing
-- history-warmup HTTP app AND the new durable history-refresh worker.
--
-- Run as an admin/owner connection AFTER create_shadow_history_warmer.sql,
-- create_shadow_history_warmer_rls_policies.sql, and
-- create_history_refresh_worker_grants.sql. Raises on any mismatch; prints a
-- success line otherwise.
--
-- It validates the INTENDED COMBINED contract — it does NOT redesign or narrow
-- the role. In particular it REQUIRES the existing history-warmup data-plane
-- capabilities (daily_bars / market_bars_4h / history_warmup_runs writes,
-- readiness reads, universe lifecycle) to remain intact, AND the new durable
-- queue-plane access to be present and queue-scoped to exactly
-- 'history_incremental_refresh'.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) ROLE SAFETY -------------------------------------------------------------
DO $$
DECLARE r pg_roles%ROWTYPE;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname='smart_scanner_history_warmer';
  IF NOT FOUND THEN RAISE EXCEPTION 'role smart_scanner_history_warmer missing'; END IF;
  IF r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication
     OR r.rolbypassrls OR r.rolinherit THEN
    RAISE EXCEPTION 'elevated role flags present (super/createdb/createrole/replication/bypassrls/inherit must all be false)';
  END IF;
  RAISE NOTICE 'history-warmer role safety verified (no elevation).';
END
$$;

-- 2) EFFECTIVE DURABLE-QUEUE SCOPE (RLS, not just grants) ---------------------
-- The qscope policy on job_runs AND job_tasks must permit EXACTLY the single
-- 'history_incremental_refresh' queue (USING + WITH CHECK) and apply to the
-- role. A stale/wrong predicate (any other queue present, or the queue missing)
-- fails here — this is what catches a non-converged live upgrade.
DO $$
DECLARE
  warmer   constant text := 'smart_scanner_history_warmer';
  expected constant text := 'history_incremental_refresh';
  rel text;
  polcount int;
  applies boolean;
  using_set text;
  check_set text;
BEGIN
  FOREACH rel IN ARRAY ARRAY['job_runs','job_tasks'] LOOP
    SELECT count(*) INTO polcount FROM pg_policies
      WHERE schemaname='public' AND tablename=rel AND policyname=warmer||'_qscope';
    IF polcount = 0 THEN
      RAISE EXCEPTION 'qscope policy missing on public.% (expected %)', rel, warmer||'_qscope';
    END IF;
    SELECT (warmer = ANY(roles)) INTO applies FROM pg_policies
      WHERE schemaname='public' AND tablename=rel AND policyname=warmer||'_qscope';
    IF applies IS NOT TRUE THEN
      RAISE EXCEPTION 'qscope policy on public.% does not apply to role %', rel, warmer;
    END IF;
    SELECT string_agg(q, ',' ORDER BY q) INTO using_set FROM (
      SELECT DISTINCT (regexp_matches(qual, '''([^'']+)''', 'g'))[1] AS q
      FROM pg_policies WHERE schemaname='public' AND tablename=rel AND policyname=warmer||'_qscope') s;
    IF using_set IS DISTINCT FROM expected THEN
      RAISE EXCEPTION 'qscope USING on public.% allows [%] but must allow exactly [%]',
        rel, using_set, expected;
    END IF;
    SELECT string_agg(q, ',' ORDER BY q) INTO check_set FROM (
      SELECT DISTINCT (regexp_matches(COALESCE(with_check, qual), '''([^'']+)''', 'g'))[1] AS q
      FROM pg_policies WHERE schemaname='public' AND tablename=rel AND policyname=warmer||'_qscope') s;
    IF check_set IS DISTINCT FROM expected THEN
      RAISE EXCEPTION 'qscope WITH CHECK on public.% allows [%] but must allow exactly [%]',
        rel, check_set, expected;
    END IF;
  END LOOP;
  RAISE NOTICE 'history-warmer effective queue scope verified (exactly history_incremental_refresh).';
END
$$;

-- 3) REQUIRED QUEUE-PLANE PRIVILEGES + 4) FORBIDDEN queue/control-plane -------
DO $$
DECLARE
  warmer constant text := 'smart_scanner_history_warmer';
  t text;
BEGIN
  -- (3) minimum the generic worker needs to claim + finalize its own tasks.
  FOREACH t IN ARRAY ARRAY['public.job_runs','public.job_tasks','public.job_task_attempts',
                           'public.job_events','public.job_workers'] LOOP
    IF NOT (has_table_privilege(warmer, t, 'SELECT')
            AND has_table_privilege(warmer, t, 'INSERT')
            AND has_table_privilege(warmer, t, 'UPDATE')) THEN
      RAISE EXCEPTION 'history-warmer missing SELECT/INSERT/UPDATE on %', t;
    END IF;
    -- (4) never DELETE/TRUNCATE on the queue plane.
    IF has_table_privilege(warmer, t, 'DELETE') OR has_table_privilege(warmer, t, 'TRUNCATE') THEN
      RAISE EXCEPTION 'history-warmer must not hold DELETE/TRUNCATE on %', t;
    END IF;
  END LOOP;
  -- (4) no control-plane access: job_schedules / job_dependencies (no grants in
  -- create_history_refresh_worker_grants.sql — the worker never schedules).
  FOREACH t IN ARRAY ARRAY['public.job_schedules','public.job_dependencies'] LOOP
    IF has_table_privilege(warmer, t, 'SELECT') OR has_table_privilege(warmer, t, 'INSERT')
       OR has_table_privilege(warmer, t, 'UPDATE') OR has_table_privilege(warmer, t, 'DELETE') THEN
      RAISE EXCEPTION 'history-warmer must have NO access to % (control plane)', t;
    END IF;
  END LOOP;
  RAISE NOTICE 'history-warmer queue-plane privileges verified (SIU on 5 tables; no DELETE/TRUNCATE; no schedules/dependencies).';
END
$$;

-- 5) DATA-PLANE CONTRACT (existing history-warmup capabilities intact) --------
DO $$
DECLARE
  warmer constant text := 'smart_scanner_history_warmer';
  t text;
BEGIN
  -- REQUIRED writes (bounded market-data + warmup lifecycle) — must stay intact.
  FOREACH t IN ARRAY ARRAY['public.daily_bars','public.market_bars_4h','public.history_warmup_runs'] LOOP
    IF NOT (has_table_privilege(warmer, t, 'SELECT')
            AND has_table_privilege(warmer, t, 'INSERT')
            AND has_table_privilege(warmer, t, 'UPDATE')) THEN
      RAISE EXCEPTION 'history-warmer missing required SELECT/INSERT/UPDATE on %', t;
    END IF;
    IF has_table_privilege(warmer, t, 'DELETE') OR has_table_privilege(warmer, t, 'TRUNCATE') THEN
      RAISE EXCEPTION 'history-warmer must not hold DELETE/TRUNCATE on %', t;
    END IF;
  END LOOP;
  -- REQUIRED reads (readiness relations).
  FOREACH t IN ARRAY ARRAY['public.patterns','public.pattern_configs'] LOOP
    IF NOT has_table_privilege(warmer, t, 'SELECT') THEN
      RAISE EXCEPTION 'history-warmer missing required SELECT on %', t;
    END IF;
  END LOOP;
  -- Frozen-universe lifecycle (migration 016) when present: universes
  -- SELECT/INSERT/UPDATE; membership SELECT/INSERT only (post-freeze immutable),
  -- never UPDATE/DELETE.
  IF to_regclass('public.history_warmup_universes') IS NOT NULL THEN
    IF NOT (has_table_privilege(warmer, 'public.history_warmup_universes', 'SELECT')
            AND has_table_privilege(warmer, 'public.history_warmup_universes', 'INSERT')
            AND has_table_privilege(warmer, 'public.history_warmup_universes', 'UPDATE')) THEN
      RAISE EXCEPTION 'history-warmer missing SELECT/INSERT/UPDATE on history_warmup_universes';
    END IF;
    IF NOT (has_table_privilege(warmer, 'public.history_warmup_universe_symbols', 'SELECT')
            AND has_table_privilege(warmer, 'public.history_warmup_universe_symbols', 'INSERT')) THEN
      RAISE EXCEPTION 'history-warmer missing SELECT/INSERT on history_warmup_universe_symbols';
    END IF;
    IF has_table_privilege(warmer, 'public.history_warmup_universe_symbols', 'UPDATE')
       OR has_table_privilege(warmer, 'public.history_warmup_universe_symbols', 'DELETE') THEN
      RAISE EXCEPTION 'history-warmer must not UPDATE/DELETE history_warmup_universe_symbols (post-freeze immutable)';
    END IF;
  END IF;

  -- FORBIDDEN strategy/business surfaces — no privilege of any kind.
  FOREACH t IN ARRAY ARRAY['public.strategy_shadow_evaluations','public.strategy_shadow_pairs',
                           'public.strategy_shadow_run_pairs','public.strategy_shadow_runs',
                           'public.strategy_shadow_pair_outcomes'] LOOP
    IF has_table_privilege(warmer, t, 'SELECT') OR has_table_privilege(warmer, t, 'INSERT')
       OR has_table_privilege(warmer, t, 'UPDATE') OR has_table_privilege(warmer, t, 'DELETE') THEN
      RAISE EXCEPTION 'history-warmer must have NO access to % (strategy surface)', t;
    END IF;
  END LOOP;
  IF to_regclass('public.strategy_shadow_outcome_runs') IS NOT NULL THEN
    IF has_table_privilege(warmer, 'public.strategy_shadow_outcome_runs', 'SELECT')
       OR has_table_privilege(warmer, 'public.strategy_shadow_outcome_runs', 'INSERT')
       OR has_table_privilege(warmer, 'public.strategy_shadow_outcome_runs', 'UPDATE')
       OR has_table_privilege(warmer, 'public.strategy_shadow_outcome_runs', 'DELETE') THEN
      RAISE EXCEPTION 'history-warmer must have NO access to strategy_shadow_outcome_runs';
    END IF;
  END IF;
  -- Campaign registrations are the DRIVER's write surface, never the warmer's.
  IF to_regclass('public.prospective_campaign_registrations') IS NOT NULL THEN
    IF has_table_privilege(warmer, 'public.prospective_campaign_registrations', 'INSERT')
       OR has_table_privilege(warmer, 'public.prospective_campaign_registrations', 'UPDATE')
       OR has_table_privilege(warmer, 'public.prospective_campaign_registrations', 'DELETE') THEN
      RAISE EXCEPTION 'history-warmer must not write prospective_campaign_registrations';
    END IF;
  END IF;
  RAISE NOTICE 'history-warmer data-plane contract verified (warmup writes intact; strategy/campaign writes forbidden).';
END
$$;

\echo 'history-refresh worker role verified.'
