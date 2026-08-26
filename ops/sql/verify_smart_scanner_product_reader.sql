-- =============================================================================
-- Verify smart_scanner_product_reader is correctly least-privileged AND can
-- actually serve the three Product API routes under RLS.
-- =============================================================================
-- Run this CONNECTED AS smart_scanner_product_reader, so every has_*_privilege
-- and every row count reflects the real product identity. Entirely read-only:
-- no INSERT/UPDATE/DELETE/TRUNCATE/DDL and no temp tables.
--
--   psql "<product-reader connection>" -f ops/sql/verify_smart_scanner_product_reader.sql
--
-- Sections 3, 5 and 6 are the assertions: each must return ZERO rows.
-- This file contains NO secrets, hostnames or database names.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Identity + read-only defaults.
SELECT current_user AS database_identity;
SHOW default_transaction_read_only;   -- must be 'on'

-- 2) Role attribute surface. Every capability flag must be false.
SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
       rolreplication, rolbypassrls
FROM pg_roles WHERE rolname = current_user;

-- 3) ASSERT: no dangerous role attribute is set. Expected: zero rows.
SELECT 'role attribute must be false: ' || attr AS violation
FROM (
  SELECT unnest(ARRAY['rolsuper','rolcreatedb','rolcreaterole',
                      'rolreplication','rolbypassrls']) AS attr,
         unnest(ARRAY[rolsuper,rolcreatedb,rolcreaterole,
                      rolreplication,rolbypassrls]) AS val
  FROM pg_roles WHERE rolname = current_user
) t WHERE val;

-- 4) SELECT present and every write privilege absent on the exact 5 relations.
WITH required(relation) AS (
  VALUES ('public.strategy_shadow_runs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_evaluations'),
         ('public.daily_bars')
)
SELECT relation,
       to_regclass(relation) IS NOT NULL                     AS exists,
       has_table_privilege(relation, 'SELECT')               AS can_select,
       has_table_privilege(relation, 'INSERT')               AS can_insert,
       has_table_privilege(relation, 'UPDATE')               AS can_update,
       has_table_privilege(relation, 'DELETE')               AS can_delete,
       has_table_privilege(relation, 'TRUNCATE')             AS can_truncate,
       has_table_privilege(relation, 'TRIGGER')              AS can_trigger
FROM required ORDER BY relation;

-- 5) ASSERT: SELECT everywhere required, writes nowhere. Expected: zero rows.
WITH required(relation) AS (
  VALUES ('public.strategy_shadow_runs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_evaluations'),
         ('public.daily_bars')
)
SELECT relation,
       CASE
         WHEN to_regclass(relation) IS NULL THEN 'missing relation'
         WHEN NOT has_table_privilege(relation, 'SELECT') THEN 'missing SELECT'
         ELSE 'holds a write privilege'
       END AS violation
FROM required
WHERE to_regclass(relation) IS NULL
   OR NOT has_table_privilege(relation, 'SELECT')
   OR has_table_privilege(relation, 'INSERT')
   OR has_table_privilege(relation, 'UPDATE')
   OR has_table_privilege(relation, 'DELETE')
   OR has_table_privilege(relation, 'TRUNCATE')
   OR has_table_privilege(relation, 'TRIGGER');

-- 6) ASSERT: no SELECT anywhere outside the 5 product relations. Expected:
--    zero rows. This is what keeps the product surface from silently widening.
SELECT c.relname AS unexpected_readable_relation
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','v','m','p')
  AND has_table_privilege(c.oid, 'SELECT')
  AND c.relname NOT IN ('strategy_shadow_runs','strategy_shadow_run_pairs',
                        'strategy_shadow_pairs','strategy_shadow_evaluations',
                        'daily_bars')
ORDER BY c.relname;

-- 7) EFFECTIVE RESULT SET under RLS. The role is NOBYPASSRLS, so these counts
--    are what the Product API can actually see. All five must be > 0 for a
--    database holding a completed scan — a SELECT grant without a matching
--    read policy silently returns zero rows, and this is what catches that.
SELECT 'strategy_shadow_runs'        AS relation, count(*) AS visible_rows FROM strategy_shadow_runs
UNION ALL SELECT 'strategy_shadow_run_pairs',   count(*) FROM strategy_shadow_run_pairs
UNION ALL SELECT 'strategy_shadow_pairs',       count(*) FROM strategy_shadow_pairs
UNION ALL SELECT 'strategy_shadow_evaluations', count(*) FROM strategy_shadow_evaluations
UNION ALL SELECT 'daily_bars',                  count(*) FROM daily_bars
ORDER BY relation;

-- 8) The exact shape the Product API resolves: latest campaign run, its
--    session date (root telemetry, campaign block as fallback) and the real
--    universe size taken from persisted pairs.
SELECT r.id AS scan_id,
       r.status,
       COALESCE(r.telemetry->'campaign'->>'as_of_date',
                r.telemetry->>'as_of_date') AS session_date,
       (SELECT count(*) FROM strategy_shadow_run_pairs rp WHERE rp.run_id = r.id)
         AS universe_symbol_count
FROM strategy_shadow_runs r
WHERE r.telemetry->'campaign' IS NOT NULL
ORDER BY r.started_at DESC
LIMIT 5;

-- 9) ASSERT: the write path is genuinely closed. Expected: zero rows.
--    (Attribute-level check; the role also runs default_transaction_read_only.)
SELECT 'writable relation: ' || c.relname AS violation
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
  AND (has_table_privilege(c.oid, 'INSERT')
    OR has_table_privilege(c.oid, 'UPDATE')
    OR has_table_privilege(c.oid, 'DELETE')
    OR has_table_privilege(c.oid, 'TRUNCATE'))
ORDER BY c.relname;
