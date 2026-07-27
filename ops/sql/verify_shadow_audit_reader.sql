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

-- 5) RLS state + intended-policy presence for the 8 relations. The live DB has
--    RLS ENABLED; the audit role therefore needs a PERMISSIVE full-row SELECT
--    policy (smart_scanner_audit_reader_select) on each table. select_policy is
--    the count of the intended policy (expected 1 each).
SELECT n.nspname AS schema, c.relname AS relation,
       c.relrowsecurity   AS rls_enabled,
       c.relforcerowsecurity AS rls_forced,
       (SELECT count(*) FROM pg_policies p
         WHERE p.schemaname = n.nspname AND p.tablename = c.relname
           AND p.policyname = 'smart_scanner_audit_reader_select'
           AND p.permissive = 'PERMISSIVE' AND p.cmd IN ('SELECT', 'ALL')
           AND 'smart_scanner_audit_reader' = ANY(p.roles)
           AND regexp_replace(coalesce(p.qual, ''), '[[:space:]()]', '', 'g') = 'true'
       ) AS intended_full_row_select_policy
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN (
    'strategy_shadow_evaluations', 'strategy_shadow_pairs',
    'strategy_shadow_pair_outcomes', 'strategy_shadow_run_pairs',
    'strategy_shadow_runs', 'daily_bars', 'patterns', 'pattern_configs')
ORDER BY c.relname;

-- 6) STRICT gate: RAISE (non-zero exit) on the FIRST relation that is missing,
--    lacks SELECT, holds any write privilege, has RLS disabled, lacks the
--    intended full-row SELECT policy, or has a narrowing restrictive policy.
DO $$
DECLARE
  t          text;
  rel_oid    regclass;
  qname      text;
  rels       constant text[] := ARRAY[
    'public.strategy_shadow_evaluations','public.strategy_shadow_pairs',
    'public.strategy_shadow_pair_outcomes','public.strategy_shadow_run_pairs',
    'public.strategy_shadow_runs','public.daily_bars','public.patterns',
    'public.pattern_configs'];
BEGIN
  FOREACH t IN ARRAY rels LOOP
    rel_oid := to_regclass(t);
    IF rel_oid IS NULL THEN
      RAISE EXCEPTION 'VERIFY FAIL: relation % missing', t; END IF;
    IF NOT has_table_privilege(t, 'SELECT') THEN
      RAISE EXCEPTION 'VERIFY FAIL: no SELECT on %', t; END IF;
    IF has_table_privilege(t,'INSERT') OR has_table_privilege(t,'UPDATE')
       OR has_table_privilege(t,'DELETE') OR has_table_privilege(t,'TRUNCATE')
       OR has_table_privilege(t,'TRIGGER') THEN
      RAISE EXCEPTION 'VERIFY FAIL: unexpected write privilege on %', t; END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      RAISE EXCEPTION 'VERIFY FAIL: RLS not enabled on %', t; END IF;
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies p
      WHERE p.schemaname = split_part(t,'.',1) AND p.tablename = split_part(t,'.',2)
        AND p.policyname = 'smart_scanner_audit_reader_select'
        AND p.permissive = 'PERMISSIVE' AND p.cmd IN ('SELECT','ALL')
        AND 'smart_scanner_audit_reader' = ANY(p.roles)
        AND regexp_replace(coalesce(p.qual,''),'[[:space:]()]','','g') = 'true'
    ) THEN
      RAISE EXCEPTION 'VERIFY FAIL: intended full-row SELECT policy missing on %', t; END IF;
    IF EXISTS (
      SELECT 1 FROM pg_policies p
      WHERE p.schemaname = split_part(t,'.',1) AND p.tablename = split_part(t,'.',2)
        AND p.permissive = 'RESTRICTIVE' AND p.cmd IN ('SELECT','ALL')
        AND (p.roles @> ARRAY['public']::name[]
             OR 'smart_scanner_audit_reader' = ANY(p.roles))
        AND regexp_replace(coalesce(p.qual,''),'[[:space:]()]','','g') <> 'true'
    ) THEN
      RAISE EXCEPTION 'VERIFY FAIL: narrowing restrictive policy on %', t; END IF;
  END LOOP;
  RAISE NOTICE 'VERIFY OK: all 8 relations SELECT-only + RLS full-row policy present';
END
$$;
