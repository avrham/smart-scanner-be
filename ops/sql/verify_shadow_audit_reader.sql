-- =============================================================================
-- Verify the smart_scanner_audit_reader role is correctly least-privileged.
-- =============================================================================
-- Run this CONNECTED AS smart_scanner_audit_reader (so has_*_privilege reflect
-- the audit identity). It is entirely read-only: it performs no INSERT/UPDATE/
-- DELETE/TRUNCATE/DDL and creates no temp tables. It mirrors what the
-- GET /api/admin/shadow-cohort/access-check endpoint reports.
--
--   psql "<audit-reader connection>" -f ops/sql/verify_shadow_audit_reader.sql
--
-- This file contains NO secrets, hostnames, database names or Supabase refs.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Identity + read-only defaults.
SELECT current_user AS database_identity;
SHOW transaction_read_only;
SHOW default_transaction_read_only;   -- must be 'on'

-- 2) Existence + SELECT + absence of every write privilege on the exact
--    closeout relations. Expected: exists=t, can_select=t, all can_* writes=f.
WITH required(relation) AS (
  VALUES ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_runs'),
         ('public.daily_bars'),
         ('public.patterns'),
         ('public.pattern_configs')
)
SELECT
  relation,
  to_regclass(relation) IS NOT NULL                                    AS exists,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'SELECT')                        AS can_select,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'INSERT')                        AS can_insert,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'UPDATE')                        AS can_update,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'DELETE')                        AS can_delete,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'TRUNCATE')                      AS can_truncate,
  to_regclass(relation) IS NOT NULL
    AND has_table_privilege(relation, 'TRIGGER')                       AS can_trigger
FROM required
ORDER BY relation;

-- 3) Fail loudly if ANY required relation is missing SELECT, or holds ANY
--    write privilege. Expected result: zero rows.
WITH required(relation) AS (
  VALUES ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_runs'),
         ('public.daily_bars'),
         ('public.patterns'),
         ('public.pattern_configs')
)
SELECT relation, 'PROBLEM' AS status
FROM required
WHERE to_regclass(relation) IS NULL
   OR NOT has_table_privilege(relation, 'SELECT')
   OR has_table_privilege(relation, 'INSERT')
   OR has_table_privilege(relation, 'UPDATE')
   OR has_table_privilege(relation, 'DELETE')
   OR has_table_privilege(relation, 'TRUNCATE')
   OR has_table_privilege(relation, 'TRIGGER');

-- 4) Sanity: this role must not be a superuser and must not bypass RLS.
SELECT rolname, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole,
       rolreplication
FROM pg_roles
WHERE rolname = current_user;

-- 5) RLS state of the 8 relations. Per this repo's migrations RLS is expected
--    OFF (relrowsecurity=f). If any row shows relrowsecurity=t WITHOUT a SELECT
--    policy granting this role access, plain SELECT grants are NOT sufficient —
--    resolve it (add a narrow read policy) before trusting the audit.
SELECT n.nspname AS schema, c.relname AS relation,
       c.relrowsecurity   AS rls_enabled,
       c.relforcerowsecurity AS rls_forced,
       (SELECT count(*) FROM pg_policies p
         WHERE p.schemaname = n.nspname AND p.tablename = c.relname
           AND p.cmd IN ('SELECT', 'ALL')) AS select_policy_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN (
    'strategy_shadow_evaluations', 'strategy_shadow_pairs',
    'strategy_shadow_pair_outcomes', 'strategy_shadow_run_pairs',
    'strategy_shadow_runs', 'daily_bars', 'patterns', 'pattern_configs')
ORDER BY c.relname;

-- 6) Fail loudly if RLS is enabled on any required relation without a SELECT
--    policy this role could use. Expected result: zero rows.
SELECT n.nspname || '.' || c.relname AS relation, 'RLS_WITHOUT_POLICY' AS status
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN (
    'strategy_shadow_evaluations', 'strategy_shadow_pairs',
    'strategy_shadow_pair_outcomes', 'strategy_shadow_run_pairs',
    'strategy_shadow_runs', 'daily_bars', 'patterns', 'pattern_configs')
  AND c.relrowsecurity = true
  AND NOT EXISTS (
    SELECT 1 FROM pg_policies p
    WHERE p.schemaname = n.nspname AND p.tablename = c.relname
      AND p.cmd IN ('SELECT', 'ALL'));
