-- =============================================================================
-- create_pipeline_driver.sql — least-privilege role for the daily-pipeline
-- DRIVER worker (smart_scanner_daily_pipeline_advance.v1).
--
-- This role runs ONLY the durable pipeline-advance handler, whose orchestration
-- primitive is advance_daily_pipeline_service. It therefore needs exactly:
--   * SELECT on the read relations advance touches (universe, bars, pairs,
--     evaluations, outcomes, registrations);
--   * INSERT/UPDATE on prospective_campaign_registrations (register + status);
--   * full queue-plane task lifecycle (SELECT/INSERT/UPDATE) so it can CLAIM its
--     own driver task AND ENQUEUE the campaign job (queue 'prospective') and the
--     outcome-maturation job (queue 'prospective_outcomes'), and create/advance
--     the daily-pipeline occurrence (queue 'daily_pipeline').
-- It NEVER writes market bars, strategy runs/pairs/evaluations, or outcome rows
-- (those belong to the history-warmer, evaluation worker, and outcome worker
-- respectively). RLS (create_pipeline_driver_rls_policies.sql) additionally
-- limits its queue-plane visibility to the four queues above.
--
-- Apply:
--   psql "<admin conn>" -v pipeline_driver_password="..." -v db_name="warmup" \
--     -f ops/sql/create_pipeline_driver.sql
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_pipeline_driver') THEN
    CREATE ROLE smart_scanner_pipeline_driver
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

\if :{?pipeline_driver_password}
  ALTER ROLE smart_scanner_pipeline_driver PASSWORD :'pipeline_driver_password';
\endif

-- bounded session guards (a stuck query/txn can never hang the queue)
ALTER ROLE smart_scanner_pipeline_driver SET statement_timeout = '55s';
ALTER ROLE smart_scanner_pipeline_driver SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_pipeline_driver SET lock_timeout = '10s';

\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_pipeline_driver;
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_pipeline_driver;

-- ---- data-plane: SELECT-only reads (everything advance READS) -------------
GRANT SELECT ON public.daily_bars                     TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.market_bars_4h                 TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.patterns                       TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.pattern_configs                TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_runs           TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_run_pairs      TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_pairs          TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_evaluations    TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_pair_outcomes  TO smart_scanner_pipeline_driver;
GRANT SELECT ON public.strategy_shadow_outcome_runs   TO smart_scanner_pipeline_driver;

DO $$
BEGIN
  IF to_regclass('public.history_warmup_universes') IS NOT NULL THEN
    GRANT SELECT ON public.history_warmup_universes        TO smart_scanner_pipeline_driver;
    GRANT SELECT ON public.history_warmup_universe_symbols TO smart_scanner_pipeline_driver;
  END IF;
END
$$;

-- ---- data-plane: the ONLY campaign write relation (register + status) -----
GRANT SELECT, INSERT, UPDATE ON public.prospective_campaign_registrations TO smart_scanner_pipeline_driver;

-- ---- queue-plane: full task lifecycle (claim own task + enqueue children +
--      create/advance the occurrence). RLS caps which queues are visible. -----
GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_pipeline_driver;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_pipeline_driver;
GRANT SELECT, INSERT, UPDATE ON public.job_task_attempts TO smart_scanner_pipeline_driver;
GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_pipeline_driver;
GRANT SELECT, INSERT, UPDATE ON public.job_workers       TO smart_scanner_pipeline_driver;
GRANT SELECT, UPDATE          ON public.job_schedules    TO smart_scanner_pipeline_driver;
GRANT SELECT                  ON public.job_dependencies TO smart_scanner_pipeline_driver;

-- ---- defensive REVOKEs: this role NEVER writes market bars, strategy
--      runs/pairs/evaluations, or outcome rows (other workers own those) ------
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.daily_bars                    FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.market_bars_4h                FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_runs          FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_run_pairs     FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_pairs         FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_evaluations   FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_pair_outcomes FROM smart_scanner_pipeline_driver;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_outcome_runs  FROM smart_scanner_pipeline_driver;
-- NO DELETE / TRUNCATE anywhere for this role (none granted above).

\echo 'pipeline-driver role configured (reads + registrations write + queue-plane lifecycle; no bar/run/pair/eval/outcome writes).'
