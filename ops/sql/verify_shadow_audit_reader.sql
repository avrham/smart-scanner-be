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
SELECT rolname, rolsuper, rolbypassrls, rolcreatedb, rolcreaterole
FROM pg_roles
WHERE rolname = current_user;
