-- =============================================================================
-- Least-privilege roles for the Durable Job Queue (migration 018)
-- =============================================================================
-- Creates three roles and extends the existing prospective API role with
-- explicitly bounded queue permissions. Apply ONLY to the ISOLATED database.
--
--   smart_scanner_prospective_worker  (the dedicated worker Machine)
--     claim/update queue tasks, write attempts/events/heartbeats, read local
--     history, write prospective campaign/pairs/evaluations, read+update the
--     registration, advance schedules. NO bar writes, NO outcome writes, NO
--     warmup writes, NO DELETE/TRUNCATE/DDL anywhere.
--
--   smart_scanner_job_enqueuer  (dedicated enqueue alt; least-privilege doc)
--     create jobs + tasks, request cancellation, read schedules/workers. NO
--     task claiming semantics, NO pair/evaluation execution, NO bar access.
--
--   smart_scanner_job_audit_reader  (read-only observability)
--     SELECT on every queue/job/schedule/worker table. NO writes anywhere.
--
--   smart_scanner_prospective_runner (existing web API role) is granted the
--     bounded queue permissions it needs to ENQUEUE + cancel/retry + manage
--     schedules + read job state (it never claims/executes tasks).
--
-- Apply:
--   psql "<admin conn>" \
--     -v worker_password="..." -v enqueuer_password="..." \
--     -v audit_reader_password="..." -v db_name="warmup" \
--     -f ops/sql/create_job_queue_roles.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- ---- role creation ---------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_prospective_worker') THEN
    CREATE ROLE smart_scanner_prospective_worker
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_job_enqueuer') THEN
    CREATE ROLE smart_scanner_job_enqueuer
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_job_audit_reader') THEN
    CREATE ROLE smart_scanner_job_audit_reader
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

\if :{?worker_password}
  ALTER ROLE smart_scanner_prospective_worker PASSWORD :'worker_password';
\endif
\if :{?enqueuer_password}
  ALTER ROLE smart_scanner_job_enqueuer PASSWORD :'enqueuer_password';
\endif
\if :{?audit_reader_password}
  ALTER ROLE smart_scanner_job_audit_reader PASSWORD :'audit_reader_password';
\endif

-- bounded session guards (a stuck query/txn can never hang the queue)
ALTER ROLE smart_scanner_prospective_worker SET statement_timeout = '110s';
ALTER ROLE smart_scanner_prospective_worker SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE smart_scanner_prospective_worker SET lock_timeout = '10s';
ALTER ROLE smart_scanner_job_enqueuer SET statement_timeout = '30s';
ALTER ROLE smart_scanner_job_enqueuer SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_job_audit_reader SET default_transaction_read_only = on;
ALTER ROLE smart_scanner_job_audit_reader SET statement_timeout = '30s';

\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_prospective_worker;
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_job_enqueuer;
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_job_audit_reader;
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_prospective_worker;
GRANT USAGE ON SCHEMA public TO smart_scanner_job_enqueuer;
GRANT USAGE ON SCHEMA public TO smart_scanner_job_audit_reader;

-- ---- worker: data-plane grants (same least-privilege as the runner) --------
GRANT SELECT ON public.daily_bars                     TO smart_scanner_prospective_worker;
GRANT SELECT ON public.market_bars_4h                 TO smart_scanner_prospective_worker;
GRANT SELECT ON public.patterns                       TO smart_scanner_prospective_worker;
GRANT SELECT ON public.pattern_configs                TO smart_scanner_prospective_worker;
GRANT SELECT ON public.strategy_shadow_pair_outcomes  TO smart_scanner_prospective_worker;
GRANT SELECT ON public.strategy_shadow_outcome_runs   TO smart_scanner_prospective_worker;
DO $$
BEGIN
  IF to_regclass('public.history_warmup_universes') IS NOT NULL THEN
    GRANT SELECT ON public.history_warmup_universes        TO smart_scanner_prospective_worker;
    GRANT SELECT ON public.history_warmup_universe_symbols TO smart_scanner_prospective_worker;
  END IF;
  IF to_regclass('public.prospective_campaign_registrations') IS NOT NULL THEN
    GRANT SELECT, INSERT, UPDATE ON public.prospective_campaign_registrations
      TO smart_scanner_prospective_worker;
    GRANT SELECT ON public.prospective_campaign_registrations TO smart_scanner_job_enqueuer;
    GRANT SELECT, INSERT, UPDATE ON public.prospective_campaign_registrations
      TO smart_scanner_job_enqueuer;  -- enqueue sets campaign markers
    GRANT SELECT ON public.prospective_campaign_registrations TO smart_scanner_job_audit_reader;
  END IF;
END
$$;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_runs        TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_run_pairs   TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_pairs       TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.strategy_shadow_evaluations TO smart_scanner_prospective_worker;

-- ---- queue-plane grants ----------------------------------------------------
-- worker: full task lifecycle (claim/update), attempts/events/heartbeats,
-- job counter updates, schedule advancement.
GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_task_attempts TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_prospective_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_workers       TO smart_scanner_prospective_worker;
GRANT SELECT, UPDATE          ON public.job_schedules    TO smart_scanner_prospective_worker;
GRANT SELECT                  ON public.job_dependencies TO smart_scanner_prospective_worker;

-- enqueuer: create jobs + tasks + events, request cancellation, read schedules.
GRANT SELECT, INSERT, UPDATE ON public.job_runs   TO smart_scanner_job_enqueuer;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks  TO smart_scanner_job_enqueuer;
GRANT SELECT, INSERT          ON public.job_events TO smart_scanner_job_enqueuer;
GRANT SELECT ON public.job_schedules  TO smart_scanner_job_enqueuer;
GRANT SELECT ON public.job_workers    TO smart_scanner_job_enqueuer;

-- prospective_runner (web API): enqueue + cancel/retry + schedule mgmt + reads.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_prospective_runner') THEN
    GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_prospective_runner;
    GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_prospective_runner;
    GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_prospective_runner;
    GRANT SELECT                  ON public.job_task_attempts TO smart_scanner_prospective_runner;
    GRANT SELECT                  ON public.job_workers       TO smart_scanner_prospective_runner;
    GRANT SELECT, INSERT, UPDATE ON public.job_schedules     TO smart_scanner_prospective_runner;
    GRANT SELECT                  ON public.job_dependencies  TO smart_scanner_prospective_runner;
  END IF;
END
$$;

-- audit reader: SELECT everywhere queue-side, nothing else.
GRANT SELECT ON public.job_runs          TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_tasks         TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_task_attempts TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_events        TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_workers       TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_schedules     TO smart_scanner_job_audit_reader;
GRANT SELECT ON public.job_dependencies  TO smart_scanner_job_audit_reader;

-- ---- defensive REVOKEs: worker/enqueuer never write bars/outcomes/warmup ---
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.daily_bars     FROM smart_scanner_prospective_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.market_bars_4h FROM smart_scanner_prospective_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_pair_outcomes FROM smart_scanner_prospective_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_outcome_runs  FROM smart_scanner_prospective_worker;
-- NO DELETE / TRUNCATE anywhere for any of these roles (none granted above).

\echo 'durable job-queue roles configured (worker / enqueuer / audit reader + runner queue grants).'
