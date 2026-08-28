-- =============================================================================
-- Verify smart_scanner_external_ingest is correctly least-privileged.
-- =============================================================================
-- Run this CONNECTED AS smart_scanner_external_ingest, so every
-- has_*_privilege reflects the real ingress identity.
--
--   psql "<external-ingest connection>" -f ops/sql/verify_smart_scanner_external_ingest.sql
--
-- Entirely read-only: no INSERT/UPDATE/DELETE/TRUNCATE/DDL, no temp tables.
--
-- This role is the only one in the database reachable by an anonymous
-- internet caller, so the assertions here are the security boundary of the
-- External Intelligence Hub. Sections 3, 5, 7 and 9 are the assertions and
-- EACH MUST RETURN ZERO ROWS.
--
-- This file contains NO secrets, hostnames or database names.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Identity.
SELECT current_user AS database_identity;

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

-- 4) The privileges the gateway actually needs.
WITH required(relation, need_select, need_insert, need_update) AS (
  VALUES ('public.external_signal_deliveries',      true,  true,  false),
         ('public.external_signals',                true,  true,  false),
         ('public.external_signal_sources',         true,  false, false),
         ('public.catalyst_source_state',           true,  true,  true),
         ('public.history_warmup_universes',        true,  false, false),
         ('public.history_warmup_universe_symbols', true,  false, false)
)
SELECT relation,
       to_regclass(relation) IS NOT NULL         AS exists,
       has_table_privilege(relation, 'SELECT')   AS can_select,
       has_table_privilege(relation, 'INSERT')   AS can_insert,
       has_table_privilege(relation, 'UPDATE')   AS can_update,
       has_table_privilege(relation, 'DELETE')   AS can_delete
FROM required ORDER BY relation;

-- 5) ASSERT: exactly the required privileges, no more and no less.
--    Expected: zero rows.
WITH required(relation, need_select, need_insert, need_update) AS (
  VALUES ('public.external_signal_deliveries',      true,  true,  false),
         ('public.external_signals',                true,  true,  false),
         ('public.external_signal_sources',         true,  false, false),
         ('public.catalyst_source_state',           true,  true,  true),
         ('public.history_warmup_universes',        true,  false, false),
         ('public.history_warmup_universe_symbols', true,  false, false)
)
SELECT relation,
       CASE
         WHEN to_regclass(relation) IS NULL THEN 'missing relation'
         WHEN need_select AND NOT has_table_privilege(relation, 'SELECT')
           THEN 'missing SELECT'
         WHEN need_insert AND NOT has_table_privilege(relation, 'INSERT')
           THEN 'missing INSERT'
         WHEN need_update AND NOT has_table_privilege(relation, 'UPDATE')
           THEN 'missing UPDATE'
         WHEN NOT need_insert AND has_table_privilege(relation, 'INSERT')
           THEN 'unexpected INSERT'
         WHEN NOT need_update AND has_table_privilege(relation, 'UPDATE')
           THEN 'unexpected UPDATE'
         WHEN has_table_privilege(relation, 'DELETE')    THEN 'unexpected DELETE'
         WHEN has_table_privilege(relation, 'TRUNCATE')  THEN 'unexpected TRUNCATE'
         WHEN has_table_privilege(relation, 'TRIGGER')   THEN 'unexpected TRIGGER'
       END AS violation
FROM required
WHERE CASE
        WHEN to_regclass(relation) IS NULL THEN true
        WHEN need_select AND NOT has_table_privilege(relation, 'SELECT') THEN true
        WHEN need_insert AND NOT has_table_privilege(relation, 'INSERT') THEN true
        WHEN need_update AND NOT has_table_privilege(relation, 'UPDATE') THEN true
        WHEN NOT need_insert AND has_table_privilege(relation, 'INSERT') THEN true
        WHEN NOT need_update AND has_table_privilege(relation, 'UPDATE') THEN true
        WHEN has_table_privilege(relation, 'DELETE') THEN true
        WHEN has_table_privilege(relation, 'TRUNCATE') THEN true
        WHEN has_table_privilege(relation, 'TRIGGER') THEN true
        ELSE false
      END;

-- 6) THE ISOLATION CHECK. The scanner relations this role must not be able to
--    touch AT ALL. If the ingress credential leaks, this list is what stands
--    between an anonymous caller and the experiment.
WITH forbidden(relation) AS (
  VALUES ('public.strategy_shadow_runs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_outcome_runs'),
         ('public.daily_bars'),
         ('public.market_bars_4h'),
         ('public.patterns'),
         ('public.pattern_configs'),
         ('public.signals'),
         ('public.signal_outcomes'),
         ('public.job_tasks'),
         ('public.job_runs'),
         ('public.job_schedules'),
         ('public.symbol_catalyst_events'),
         ('public.company_news_articles'),
         ('public.sec_filings'),
         ('public.prospective_campaign_registrations')
)
SELECT relation,
       has_table_privilege(relation, 'SELECT') AS can_select,
       has_table_privilege(relation, 'INSERT') AS can_insert,
       has_table_privilege(relation, 'UPDATE') AS can_update,
       has_table_privilege(relation, 'DELETE') AS can_delete
FROM forbidden
WHERE to_regclass(relation) IS NOT NULL
ORDER BY relation;

-- 7) ASSERT: NO privilege of any kind on any scanner relation.
--    Expected: zero rows.
WITH forbidden(relation) AS (
  VALUES ('public.strategy_shadow_runs'),
         ('public.strategy_shadow_run_pairs'),
         ('public.strategy_shadow_pairs'),
         ('public.strategy_shadow_evaluations'),
         ('public.strategy_shadow_pair_outcomes'),
         ('public.strategy_shadow_outcome_runs'),
         ('public.daily_bars'),
         ('public.market_bars_4h'),
         ('public.patterns'),
         ('public.pattern_configs'),
         ('public.signals'),
         ('public.signal_outcomes'),
         ('public.job_tasks'),
         ('public.job_runs'),
         ('public.job_schedules'),
         ('public.symbol_catalyst_events'),
         ('public.company_news_articles'),
         ('public.sec_filings'),
         ('public.prospective_campaign_registrations')
)
SELECT relation, 'ingress role must hold NO privilege here' AS violation
FROM forbidden
WHERE to_regclass(relation) IS NOT NULL
  AND (has_table_privilege(relation, 'SELECT')
    OR has_table_privilege(relation, 'INSERT')
    OR has_table_privilege(relation, 'UPDATE')
    OR has_table_privilege(relation, 'DELETE')
    OR has_table_privilege(relation, 'TRUNCATE'));

-- 8) EFFECTIVE RLS on the shared freshness table. `has_table_privilege` says
--    the role may UPDATE `catalyst_source_state`; only the policy predicate
--    decides WHICH rows. Section 9 checks the predicate itself, because a
--    grant without the predicate would let a leaked ingress credential mark
--    the SEC or news dimension as failed.
SELECT policyname, cmd, qual AS using_predicate, with_check
FROM pg_policies
WHERE schemaname = 'public' AND tablename = 'catalyst_source_state'
  AND policyname LIKE 'smart_scanner_external_ingest%'
ORDER BY policyname;

-- 9) ASSERT: every external-ingest policy on the shared freshness table is
--    scoped to the `external_` namespace. Expected: zero rows.
SELECT policyname, cmd,
       'freshness policy must be scoped to source LIKE external\_%' AS violation
FROM pg_policies
WHERE schemaname = 'public' AND tablename = 'catalyst_source_state'
  AND policyname LIKE 'smart_scanner_external_ingest%'
  AND NOT (
    COALESCE(qual, '') LIKE '%external\_%%' ESCAPE '!'
    OR COALESCE(with_check, '') LIKE '%external\_%%' ESCAPE '!'
  );

-- 10) Visible proof of the namespace confinement: connected as this role, the
--     shared freshness table shows ONLY external rows. The earnings, news and
--     SEC rows exist in the table and are invisible here.
SELECT source, status FROM public.catalyst_source_state ORDER BY source;

-- 11) EFFECTIVE READ on the frozen universe.
--
--     A privilege check is not enough here. RLS is enabled on these relations
--     and this role is NOBYPASSRLS, so a SELECT grant with no policy returns
--     zero rows — and the symptom is nasty: every incoming signal is classified
--     `external_discovery` and silently disappears from the product, because an
--     empty universe cannot be told apart from "this symbol is not in it".
--
--     This section therefore asserts what the role can actually READ, not what
--     it is permitted to read.
SELECT count(*) AS universe_symbols_visible
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = 'WYCKOFF-HISTORY-WARMUP-QUALIFICATION';

-- 12) ASSERT: the frozen universe is readable. Expected: zero rows.
SELECT 'frozen universe is not readable — every signal would be misclassified '
       'as external_discovery' AS violation
WHERE (SELECT count(*)
       FROM public.history_warmup_universe_symbols s
       JOIN public.history_warmup_universes u ON u.id = s.universe_id
       WHERE u.universe_code = 'WYCKOFF-HISTORY-WARMUP-QUALIFICATION') = 0;
