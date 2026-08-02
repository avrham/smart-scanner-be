-- =============================================================================
-- Least-privilege PostgreSQL role for the Prospective Campaign Environment
-- =============================================================================
-- Used ONLY by the bounded prospective campaign execute path
-- (POST /api/admin/prospective/execute). It runs the reused shadow runner over
-- LOCAL bars (no provider). Access matrix:
--   SELECT  : daily_bars, market_bars_4h, patterns, pattern_configs,
--             history_warmup_universes, history_warmup_universe_symbols,
--             strategy_shadow_* (read for dedup/audit),
--             prospective_campaign_registrations,
--             job_runs, job_tasks, job_events (also write — see below),
--             job_workers (read-only; for the /prospective/audit job block
--             and the durable-queue enqueue paths; migration 018)
--   INSERT/UPDATE : prospective_campaign_registrations, strategy_shadow_runs,
--             strategy_shadow_run_pairs, strategy_shadow_pairs,
--             strategy_shadow_evaluations, job_runs (INSERT+UPDATE — the
--             prospective/outcome ENQUEUE paths create+recompute their own
--             parent job), job_tasks (INSERT — per-symbol/per-pair task
--             creation), job_events (INSERT — append-only event log)
--   NONE (write) : daily_bars, market_bars_4h, strategy_shadow_pair_outcomes,
--             strategy_shadow_outcome_runs, history_warmup_* ;
--             NO DELETE / TRUNCATE / DDL / TRIGGER anywhere; no role admin.
--
-- Apply against the ISOLATED non-production database only:
--   psql "<admin conn>" -v prospective_password="$(openssl rand -base64 24)" \
--        -v db_name="warmup" -f ops/sql/create_shadow_prospective_runner.sql
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_prospective_runner') THEN
    CREATE ROLE smart_scanner_prospective_runner
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

\if :{?prospective_password}
  ALTER ROLE smart_scanner_prospective_runner PASSWORD :'prospective_password';
  \echo 'prospective_runner password set from -v prospective_password'
\else
  \echo 'NOTE: prospective_password not supplied; role password left unchanged.'
\endif

ALTER ROLE smart_scanner_prospective_runner SET default_transaction_read_only = off;
ALTER ROLE smart_scanner_prospective_runner SET statement_timeout = '110s';
ALTER ROLE smart_scanner_prospective_runner SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE smart_scanner_prospective_runner SET lock_timeout = '10s';

\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_prospective_runner;
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_prospective_runner;

-- SELECT (read) relations
GRANT SELECT ON public.daily_bars                     TO smart_scanner_prospective_runner;
GRANT SELECT ON public.market_bars_4h                 TO smart_scanner_prospective_runner;
GRANT SELECT ON public.patterns                       TO smart_scanner_prospective_runner;
GRANT SELECT ON public.pattern_configs                TO smart_scanner_prospective_runner;
GRANT SELECT ON public.strategy_shadow_pair_outcomes  TO smart_scanner_prospective_runner;
GRANT SELECT ON public.strategy_shadow_outcome_runs   TO smart_scanner_prospective_runner;

DO $$
BEGIN
  IF to_regclass('public.history_warmup_universes') IS NOT NULL THEN
    GRANT SELECT ON public.history_warmup_universes        TO smart_scanner_prospective_runner;
    GRANT SELECT ON public.history_warmup_universe_symbols TO smart_scanner_prospective_runner;
  END IF;
  IF to_regclass('public.prospective_campaign_registrations') IS NOT NULL THEN
    GRANT SELECT, INSERT, UPDATE ON public.prospective_campaign_registrations
      TO smart_scanner_prospective_runner;
  END IF;
  -- durable-queue access (migration 018); guarded since older/isolated
  -- setups may predate it. job_runs/job_tasks/job_events are WRITABLE here
  -- because the prospective + daily-pipeline ENQUEUE paths run under this
  -- role and create their own parent job/tasks/events directly (traced from
  -- app.jobs.prospective_enqueue.enqueue_prospective_campaign,
  -- app.jobs.prospective_outcome_enqueue.enqueue_outcome_maturation, and
  -- app.jobs.daily_pipeline). job_workers stays read-only: this role never
  -- registers as a worker.
  IF to_regclass('public.job_runs') IS NOT NULL THEN
    GRANT SELECT, INSERT, UPDATE ON public.job_runs  TO smart_scanner_prospective_runner;
    GRANT SELECT, INSERT        ON public.job_tasks  TO smart_scanner_prospective_runner;
    GRANT SELECT, INSERT        ON public.job_events TO smart_scanner_prospective_runner;
    GRANT SELECT                ON public.job_workers TO smart_scanner_prospective_runner;
    -- Defensive: this role only ever enqueues (INSERT) tasks; it never
    -- claims, runs, or mutates an existing task's lease/status (that is the
    -- WORKER role's job).
    REVOKE UPDATE, DELETE, TRUNCATE ON public.job_tasks  FROM smart_scanner_prospective_runner;
    REVOKE UPDATE, DELETE, TRUNCATE ON public.job_events FROM smart_scanner_prospective_runner;
    REVOKE DELETE, TRUNCATE          ON public.job_runs  FROM smart_scanner_prospective_runner;
  END IF;
END
$$;

-- SELECT + INSERT + UPDATE on the campaign write relations (NO DELETE/TRUNCATE).
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_runs       TO smart_scanner_prospective_runner;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_run_pairs  TO smart_scanner_prospective_runner;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_pairs      TO smart_scanner_prospective_runner;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_evaluations TO smart_scanner_prospective_runner;

-- Defensive: NEVER any write on bars or outcomes.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.daily_bars                    FROM smart_scanner_prospective_runner;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.market_bars_4h                FROM smart_scanner_prospective_runner;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_pair_outcomes FROM smart_scanner_prospective_runner;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_outcome_runs  FROM smart_scanner_prospective_runner;

\echo 'smart_scanner_prospective_runner role configured (least-privilege).'
