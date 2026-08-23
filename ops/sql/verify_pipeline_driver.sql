-- =============================================================================
-- verify_pipeline_driver.sql — least-privilege + queue-isolation verification
-- for smart_scanner_pipeline_driver. Run as an admin/owner connection AFTER
-- create_pipeline_driver.sql + create_pipeline_driver_rls_policies.sql.
-- Raises an exception on any mismatch; prints a success line otherwise.
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
  r pg_roles%ROWTYPE;
BEGIN
  SELECT * INTO r FROM pg_roles WHERE rolname = 'smart_scanner_pipeline_driver';
  IF NOT FOUND THEN RAISE EXCEPTION 'role missing'; END IF;
  IF r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls OR r.rolinherit THEN
    RAISE EXCEPTION 'elevated role flags present (super/createdb/createrole/replication/bypassrls/inherit must all be false)';
  END IF;

  -- must NOT hold write privileges on bars / strategy runs-pairs-evals / outcomes
  IF has_table_privilege('smart_scanner_pipeline_driver', 'public.daily_bars', 'INSERT')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.market_bars_4h', 'UPDATE')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.strategy_shadow_pair_outcomes', 'INSERT')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.strategy_shadow_evaluations', 'INSERT')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.strategy_shadow_pairs', 'INSERT')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.strategy_shadow_runs', 'INSERT') THEN
    RAISE EXCEPTION 'driver must not hold bar/run/pair/eval/outcome write privileges';
  END IF;

  -- MUST hold the writes it needs
  IF NOT has_table_privilege('smart_scanner_pipeline_driver', 'public.prospective_campaign_registrations', 'INSERT')
     OR NOT has_table_privilege('smart_scanner_pipeline_driver', 'public.job_runs', 'UPDATE')
     OR NOT has_table_privilege('smart_scanner_pipeline_driver', 'public.job_tasks', 'UPDATE')
     OR NOT has_table_privilege('smart_scanner_pipeline_driver', 'public.job_tasks', 'INSERT') THEN
    RAISE EXCEPTION 'driver missing a required write privilege (registrations / job_runs.UPDATE / job_tasks.INSERT+UPDATE)';
  END IF;

  -- must NOT hold DELETE anywhere it writes
  IF has_table_privilege('smart_scanner_pipeline_driver', 'public.job_tasks', 'DELETE')
     OR has_table_privilege('smart_scanner_pipeline_driver', 'public.prospective_campaign_registrations', 'DELETE') THEN
    RAISE EXCEPTION 'driver must not hold DELETE';
  END IF;

  RAISE NOTICE 'pipeline-driver role verified (privileges + no elevation + no forbidden writes).';
END
$$;

-- ---------------------------------------------------------------------------
-- EFFECTIVE queue-scope verification (not just grants). Proves the qscope RLS
-- policy on job_runs AND job_tasks permits EXACTLY the intended five queues —
-- including history_incremental_refresh — so a stale/old predicate (e.g. the
-- previous four-queue definition that a CREATE-if-missing upgrade would leave
-- behind) FAILS here instead of silently passing. Asserted from the policy
-- definition (deterministic; no SET ROLE, no probe rows).
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  driver   constant text := 'smart_scanner_pipeline_driver';
  expected constant text := 'daily_pipeline,daily_pipeline_driver,'
                            'history_incremental_refresh,prospective,prospective_outcomes';
  rel text;
  polcount int;
  applies boolean;
  using_set text;
  check_set text;
BEGIN
  FOREACH rel IN ARRAY ARRAY['job_runs','job_tasks'] LOOP
    SELECT count(*) INTO polcount FROM pg_policies
      WHERE schemaname='public' AND tablename=rel AND policyname=driver||'_qscope';
    IF polcount = 0 THEN
      RAISE EXCEPTION 'qscope policy missing on public.% (expected %)', rel, driver||'_qscope';
    END IF;
    SELECT (driver = ANY(roles)) INTO applies FROM pg_policies
      WHERE schemaname='public' AND tablename=rel AND policyname=driver||'_qscope';
    IF applies IS NOT TRUE THEN
      RAISE EXCEPTION 'qscope policy on public.% does not apply to role %', rel, driver;
    END IF;
    -- distinct + sorted queue literals in the USING (qual) expression
    SELECT string_agg(q, ',' ORDER BY q) INTO using_set FROM (
      SELECT DISTINCT (regexp_matches(qual, '''([^'']+)''', 'g'))[1] AS q
      FROM pg_policies WHERE schemaname='public' AND tablename=rel AND policyname=driver||'_qscope') s;
    IF using_set IS DISTINCT FROM expected THEN
      RAISE EXCEPTION 'qscope USING on public.% allows [%] but must allow exactly [%]',
        rel, using_set, expected;
    END IF;
    -- and the WITH CHECK (insert/update) expression must match the same set
    SELECT string_agg(q, ',' ORDER BY q) INTO check_set FROM (
      SELECT DISTINCT (regexp_matches(COALESCE(with_check, qual), '''([^'']+)''', 'g'))[1] AS q
      FROM pg_policies WHERE schemaname='public' AND tablename=rel AND policyname=driver||'_qscope') s;
    IF check_set IS DISTINCT FROM expected THEN
      RAISE EXCEPTION 'qscope WITH CHECK on public.% allows [%] but must allow exactly [%]',
        rel, check_set, expected;
    END IF;
  END LOOP;
  RAISE NOTICE 'pipeline-driver effective queue scope verified (exactly 5 queues incl history_incremental_refresh).';
END
$$;

\echo 'pipeline-driver role verified.'
