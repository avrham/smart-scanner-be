-- =============================================================================
-- Least-privilege PostgreSQL role for the Shadow Outcome Maintenance Environment
-- =============================================================================
-- Purpose: a WRITE-capable but narrowly-scoped login used ONLY by the bounded
-- outcome-maturation path (POST /api/admin/shadow-maintenance/outcomes/execute).
-- It can SELECT the plan/calc read relations and INSERT/UPDATE ONLY the two
-- outcome write relations. It has NO DELETE/TRUNCATE/CREATE/TRIGGER anywhere,
-- no ownership, no BYPASSRLS and no broad/default privileges.
--
-- Access matrix (traced from run_shadow_outcome_calculation + the preflight
-- plan; see docs/ and the PR description):
--   SELECT  : strategy_shadow_evaluations, strategy_shadow_pairs,
--             strategy_shadow_pair_outcomes, strategy_shadow_run_pairs,
--             strategy_shadow_runs, daily_bars, patterns, pattern_configs,
--             strategy_shadow_outcome_runs
--   INSERT  : strategy_shadow_pair_outcomes, strategy_shadow_outcome_runs
--   UPDATE  : strategy_shadow_pair_outcomes, strategy_shadow_outcome_runs
--   (NO writes on daily_bars — the calc's read-through cache write is
--    best-effort and caught; correctness never depends on it, so the role is
--    NOT granted daily_bars writes and the cache write simply no-ops.)
--   (NO sequences — every id is a client-supplied UUID.)
--
-- This file contains NO real password, NO Supabase ref, NO hostname, NO db name.
-- An operator applies it manually against the intended development database:
--   psql "<admin conn>" \
--        -v maint_password="$(openssl rand -base64 24)" \
--        -v db_name="postgres" \
--        -f ops/sql/create_shadow_outcome_maintainer.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role: LOGIN, inherits nothing broad, cannot bypass RLS. NOT read-only by
--    default (this role performs narrowly-authorized writes) — the write scope
--    is enforced by exact grants + RLS policies, never by broad privilege.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_outcome_maintainer') THEN
    CREATE ROLE smart_scanner_outcome_maintainer
      LOGIN
      NOINHERIT
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION
      NOBYPASSRLS;
  END IF;
END
$$;

\if :{?maint_password}
  ALTER ROLE smart_scanner_outcome_maintainer PASSWORD :'maint_password';
  \echo 'maintainer password set from -v maint_password'
\else
  \echo 'NOTE: maint_password not supplied; role password left unchanged.'
\endif

-- 2) Bounded session defaults (role-level so they hold through a pooler).
--    NOTE: default_transaction_read_only is deliberately NOT set — this role
--    must perform the narrowly authorized outcome writes.
ALTER ROLE smart_scanner_outcome_maintainer SET statement_timeout = '55s';
ALTER ROLE smart_scanner_outcome_maintainer SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_outcome_maintainer SET lock_timeout = '10s';

-- 3) Connect + schema usage only.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_outcome_maintainer;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_outcome_maintainer;

-- 4) SELECT on the EXACT read relations (plan + calc selection). No ALL TABLES,
--    no default privileges — future tables are never auto-exposed.
GRANT SELECT ON public.strategy_shadow_evaluations   TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.strategy_shadow_pairs          TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.strategy_shadow_pair_outcomes  TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.strategy_shadow_run_pairs      TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.strategy_shadow_runs           TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.daily_bars                     TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.patterns                       TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.pattern_configs                TO smart_scanner_outcome_maintainer;
GRANT SELECT ON public.strategy_shadow_outcome_runs   TO smart_scanner_outcome_maintainer;

-- 5) INSERT + UPDATE on ONLY the two outcome write relations. The upsert writes
--    every outcome column and the run row writes status/telemetry/timestamps,
--    so table-level INSERT/UPDATE on exactly these two relations IS the minimal
--    grant (column-level would add no security on an all-column write and would
--    break the write-once merge). Row scope is enforced by the RLS policies.
GRANT INSERT, UPDATE ON public.strategy_shadow_pair_outcomes TO smart_scanner_outcome_maintainer;
GRANT INSERT, UPDATE ON public.strategy_shadow_outcome_runs  TO smart_scanner_outcome_maintainer;

-- NOT granted (by design): DELETE, TRUNCATE, TRIGGER, CREATE, ownership,
-- BYPASSRLS, sequence privileges, membership in any app/service role, SELECT or
-- write on daily_bars-writes / any other table, and any default privilege.
