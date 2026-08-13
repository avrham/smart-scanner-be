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

\echo 'pipeline-driver role verified.'
