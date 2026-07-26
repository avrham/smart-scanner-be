-- =============================================================================
-- Least-privilege PostgreSQL role for the Wyckoff shadow cohort closeout AUDIT
-- =============================================================================
-- Purpose: a read-only login that can run ONLY GET /api/admin/shadow-cohort/
-- closeout and /access-check. It has SELECT on the exact 8 relations the
-- closeout read path touches and NO write privileges anywhere.
--
-- This file contains NO real password, NO Supabase project ref, NO production
-- hostname and NO database name. Fill the placeholders below at apply time.
-- DO NOT run this against production from application code or CI — an operator
-- applies it manually against the intended database.
--
-- Apply (example):
--   psql "<admin connection to the target db>" \
--        -v audit_password="$(openssl rand -base64 24)" \
--        -v db_name="<target database>" \
--        -f ops/sql/create_shadow_audit_reader.sql
--
-- psql variables (both OPTIONAL; the script is safely rerunnable):
--   :audit_password   -> a strong password supplied via -v (never committed).
--                        If omitted, the role's password is left unchanged.
--   :db_name          -> the target database name for the CONNECT grant.
--                        If omitted, the CONNECT grant is skipped (run it
--                        manually). Supabase's database is named `postgres`.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role: a LOGIN role that inherits nothing broad, cannot bypass RLS, and
--    defaults every transaction to READ ONLY. NOINHERIT so it never picks up
--    privileges from any group role it might later be added to.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_audit_reader') THEN
    CREATE ROLE smart_scanner_audit_reader
      LOGIN
      NOINHERIT
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION
      NOBYPASSRLS;            -- must stay false: never bypass row-level security
  END IF;
END
$$;

-- Password is supplied at apply time via psql -v audit_password=... (never
-- stored in this file). Quoted with :'...' so special characters are handled.
\if :{?audit_password}
  ALTER ROLE smart_scanner_audit_reader PASSWORD :'audit_password';
  \echo 'audit_reader password set from -v audit_password'
\else
  \echo 'NOTE: audit_password not supplied; role password left unchanged.'
\endif

-- 2) Session hardening: read-only by default + bounded timeouts. These ALTER
--    ROLE ... SET defaults apply to every session/transaction for this role
--    and are the PRIMARY enforcement layer (they hold even through a Supabase
--    transaction pooler, unlike ad-hoc session SETs).
ALTER ROLE smart_scanner_audit_reader SET default_transaction_read_only = on;
ALTER ROLE smart_scanner_audit_reader SET statement_timeout = '30s';
ALTER ROLE smart_scanner_audit_reader SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE smart_scanner_audit_reader SET lock_timeout = '5s';

-- 3) Connect + schema usage ONLY. The CONNECT grant needs the DB name as an
--    identifier (:"db_name" quotes it safely); skipped when db_name is absent.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_audit_reader;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_audit_reader;

-- 4) SELECT on the EXACT relations the closeout read path requires — nothing
--    else. No GRANT SELECT ON ALL TABLES, no ALTER DEFAULT PRIVILEGES, so
--    future tables are NOT automatically exposed.
GRANT SELECT ON public.strategy_shadow_evaluations   TO smart_scanner_audit_reader;
GRANT SELECT ON public.strategy_shadow_pairs          TO smart_scanner_audit_reader;
GRANT SELECT ON public.strategy_shadow_pair_outcomes  TO smart_scanner_audit_reader;
GRANT SELECT ON public.strategy_shadow_run_pairs      TO smart_scanner_audit_reader;
GRANT SELECT ON public.strategy_shadow_runs           TO smart_scanner_audit_reader;
GRANT SELECT ON public.daily_bars                     TO smart_scanner_audit_reader;
GRANT SELECT ON public.patterns                       TO smart_scanner_audit_reader;
GRANT SELECT ON public.pattern_configs                TO smart_scanner_audit_reader;

-- 5) The closeout path calls no custom SQL functions (only built-ins, which
--    are executable by PUBLIC), so NO explicit EXECUTE grants are required.
--    No sequence privileges are granted (reads never touch sequences).
--
--    NOT granted (by design): CREATE, INSERT, UPDATE, DELETE, TRUNCATE,
--    TRIGGER, membership in any application/service role, and any broad or
--    default-privilege grant.

-- Notes on the connection method / RLS:
--   * The app connects to Postgres directly (or via the Supabase Supavisor
--     pooler) as this login — NOT through the Supabase HTTP API. The Supabase
--     service-role / anon keys are NOT used by the closeout DB path.
--   * BYPASSRLS is false (never grant it). Per this repo's migrations, NONE of
--     the 8 relations enable Row Level Security, so plain SELECT grants are
--     sufficient and NO read policy is required. `verify_shadow_audit_reader.sql`
--     re-checks RLS state and fails readiness if RLS is unexpectedly enabled.
--   * If RLS is ever enabled on any of these tables in production, add the
--     NARROW read-only policy below (scoped to this role) instead of BYPASSRLS:
--
--       -- ALTER TABLE public.<relation> ENABLE ROW LEVEL SECURITY;   -- (owner)
--       -- CREATE POLICY audit_reader_select ON public.<relation>
--       --   FOR SELECT TO smart_scanner_audit_reader USING (true);
--
--   * Under a transaction pooler, per-session SETs may not persist across
--     statements — which is exactly why the read-only default and timeouts are
--     set at the ROLE level (ALTER ROLE ... SET) above.
