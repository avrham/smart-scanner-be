-- =============================================================================
-- Verify smart_scanner_outcome_maintainer is correctly least-privileged.
-- =============================================================================
-- Run CONNECTED AS smart_scanner_outcome_maintainer. Read-only: it performs no
-- INSERT/UPDATE/DELETE/DDL and creates no temp tables. Mirrors what
-- GET /api/admin/shadow-maintenance/access-check reports. No secrets here.
--   psql "<maintainer connection>" -f ops/sql/verify_shadow_outcome_maintainer.sql
-- =============================================================================

\set ON_ERROR_STOP on

SELECT current_user AS database_identity;

-- 1) Read relations: SELECT present, writes absent.
WITH req(relation) AS (
  VALUES ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_runs'),
         ('public.daily_bars'),
         ('public.patterns'),
         ('public.pattern_configs')
)
SELECT relation,
       has_table_privilege(relation,'SELECT')   AS can_select,
       has_table_privilege(relation,'INSERT')   AS can_insert,
       has_table_privilege(relation,'UPDATE')   AS can_update,
       has_table_privilege(relation,'DELETE')   AS can_delete,
       has_table_privilege(relation,'TRUNCATE') AS can_truncate
FROM req ORDER BY relation;

-- 2) Write relations: SELECT + INSERT + UPDATE present; DELETE/TRUNCATE absent.
WITH req(relation) AS (
  VALUES ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_outcome_runs')
)
SELECT relation,
       has_table_privilege(relation,'SELECT')   AS can_select,
       has_table_privilege(relation,'INSERT')   AS can_insert,
       has_table_privilege(relation,'UPDATE')   AS can_update,
       has_table_privilege(relation,'DELETE')   AS can_delete,
       has_table_privilege(relation,'TRUNCATE') AS can_truncate
FROM req ORDER BY relation;

-- 3) FAIL (return rows) if any read relation lacks SELECT or holds any write.
WITH req(relation) AS (
  VALUES ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_runs'),
         ('public.daily_bars'),
         ('public.patterns'),
         ('public.pattern_configs')
)
SELECT relation FROM req
WHERE NOT has_table_privilege(relation,'SELECT')
   OR has_table_privilege(relation,'INSERT')
   OR has_table_privilege(relation,'UPDATE')
   OR has_table_privilege(relation,'DELETE')
   OR has_table_privilege(relation,'TRUNCATE')
   OR has_table_privilege(relation,'TRIGGER');

-- 4) FAIL if a write relation lacks SELECT/INSERT/UPDATE or holds DELETE/
--    TRUNCATE/TRIGGER, or if daily_bars is writable (writes must be absent).
WITH req(relation) AS (
  VALUES ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_outcome_runs')
)
SELECT relation FROM req
WHERE NOT (has_table_privilege(relation,'SELECT')
           AND has_table_privilege(relation,'INSERT')
           AND has_table_privilege(relation,'UPDATE'))
   OR has_table_privilege(relation,'DELETE')
   OR has_table_privilege(relation,'TRUNCATE')
   OR has_table_privilege(relation,'TRIGGER')
UNION ALL
SELECT 'public.daily_bars'
WHERE has_table_privilege('public.daily_bars','INSERT')
   OR has_table_privilege('public.daily_bars','UPDATE')
   OR has_table_privilege('public.daily_bars','DELETE');

-- 5) Elevated attributes must all be false for this role.
SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
FROM pg_roles WHERE rolname = current_user;
