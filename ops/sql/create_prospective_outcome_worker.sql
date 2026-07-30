-- =============================================================================
-- Least-privilege role for the prospective outcome-maturation durable-queue
-- worker (task type prospective_outcome_maturation.v1)
-- =============================================================================
-- Mirrors ops/sql/create_shadow_outcome_maintainer.sql's exact data-plane
-- grant shape (SELECT the read relations, INSERT/UPDATE ONLY the two outcome
-- write relations) PLUS the queue-plane grants a durable-queue worker needs
-- (claim/lease/heartbeat/attempts/events), mirroring
-- ops/sql/create_job_queue_roles.sql's smart_scanner_prospective_worker
-- pattern. Genuinely SEPARATE from smart_scanner_prospective_worker: this
-- role can NEVER write bars, registrations, campaigns, pairs or strategy
-- evaluations, and — because it only ever polls queue_name='prospective_
-- outcomes' — it mechanically never sees (let alone claims) a
-- 'prospective' queue evaluation task. Apply ONLY to the ISOLATED database.
--
-- Access matrix (traced from app/jobs/handlers/prospective_outcome.py +
-- app/jobs/prospective_outcome_enqueue.py):
--   SELECT  : prospective_campaign_registrations, strategy_shadow_runs,
--             strategy_shadow_run_pairs, strategy_shadow_pairs,
--             strategy_shadow_evaluations, strategy_shadow_pair_outcomes,
--             strategy_shadow_outcome_runs, daily_bars
--   INSERT/UPDATE : strategy_shadow_pair_outcomes, strategy_shadow_outcome_runs
--   queue-plane SELECT/INSERT/UPDATE : job_runs, job_tasks, job_task_attempts,
--             job_events, job_workers; SELECT/UPDATE job_schedules;
--             SELECT job_dependencies
-- NOT granted anywhere: market_bars_4h (no canonical outcome uses it today),
-- daily_bars writes, prospective_campaign_registrations writes,
-- strategy_shadow_pairs/strategy_shadow_evaluations/strategy_shadow_runs
-- writes, history_warmup_* (any), DELETE, TRUNCATE, DDL, role administration.
--
-- Apply:
--   psql "<admin conn>" \
--     -v outcome_worker_password="..." -v db_name="warmup" \
--     -f ops/sql/create_prospective_outcome_worker.sql
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='smart_scanner_prospective_outcome_worker') THEN
    CREATE ROLE smart_scanner_prospective_outcome_worker
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

\if :{?outcome_worker_password}
  ALTER ROLE smart_scanner_prospective_outcome_worker PASSWORD :'outcome_worker_password';
\endif

-- bounded session guards (a stuck query/txn can never hang the queue)
ALTER ROLE smart_scanner_prospective_outcome_worker SET statement_timeout = '55s';
ALTER ROLE smart_scanner_prospective_outcome_worker SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_prospective_outcome_worker SET lock_timeout = '10s';

\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_prospective_outcome_worker;
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_prospective_outcome_worker;

-- ---- data-plane: SELECT-only reads --------------------------------------
GRANT SELECT ON public.prospective_campaign_registrations TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_runs                TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_run_pairs            TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_pairs                 TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_evaluations           TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_pair_outcomes         TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.strategy_shadow_outcome_runs          TO smart_scanner_prospective_outcome_worker;
GRANT SELECT ON public.daily_bars                             TO smart_scanner_prospective_outcome_worker;

-- ---- data-plane: the ONLY two write relations ----------------------------
GRANT INSERT, UPDATE ON public.strategy_shadow_pair_outcomes TO smart_scanner_prospective_outcome_worker;
GRANT INSERT, UPDATE ON public.strategy_shadow_outcome_runs  TO smart_scanner_prospective_outcome_worker;

-- ---- queue-plane: full task lifecycle on ITS OWN queue -------------------
GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_prospective_outcome_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_prospective_outcome_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_task_attempts TO smart_scanner_prospective_outcome_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_prospective_outcome_worker;
GRANT SELECT, INSERT, UPDATE ON public.job_workers       TO smart_scanner_prospective_outcome_worker;
GRANT SELECT, UPDATE          ON public.job_schedules    TO smart_scanner_prospective_outcome_worker;
GRANT SELECT                  ON public.job_dependencies TO smart_scanner_prospective_outcome_worker;

-- ---- defensive REVOKEs: this role never writes bars/registrations/campaigns/
--      pairs/evaluations, even if a future broad GRANT is added by mistake ---
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.daily_bars                       FROM smart_scanner_prospective_outcome_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.prospective_campaign_registrations FROM smart_scanner_prospective_outcome_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_pairs             FROM smart_scanner_prospective_outcome_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_evaluations       FROM smart_scanner_prospective_outcome_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_runs              FROM smart_scanner_prospective_outcome_worker;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON public.strategy_shadow_run_pairs         FROM smart_scanner_prospective_outcome_worker;
-- NO DELETE / TRUNCATE anywhere for this role (none granted above).

\echo 'prospective outcome-worker role configured (data-plane read + 2-table write, queue-plane full lifecycle on its own queue).'
