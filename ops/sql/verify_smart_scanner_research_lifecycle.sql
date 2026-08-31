-- =============================================================================
-- verify_smart_scanner_research_lifecycle.sql
--
-- Proves the boundary rather than describing it. Run as an admin AFTER
-- create_smart_scanner_research_lifecycle.sql and its RLS file.
--
-- The negative checks are the point. A role listing is a claim; a
-- `has_table_privilege` returning false, and an actual write that is refused,
-- are evidence.
-- =============================================================================

\set ON_ERROR_STOP off

\echo ''
\echo '=== 1. role attributes (all must be false except rolcanlogin) ==='
SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
       rolbypassrls, rolreplication
FROM pg_roles WHERE rolname = 'smart_scanner_research_lifecycle';

\echo ''
\echo '=== 2. NO membership in any other role (must be empty) ==='
SELECT r.rolname AS member_of
FROM pg_auth_members m
JOIN pg_roles r ON r.oid = m.roleid
JOIN pg_roles g ON g.oid = m.member
WHERE g.rolname = 'smart_scanner_research_lifecycle';

\echo ''
\echo '=== 3. table privileges actually held ==='
SELECT table_name, string_agg(privilege_type, ',' ORDER BY privilege_type) AS privs
FROM information_schema.role_table_grants
WHERE grantee = 'smart_scanner_research_lifecycle' AND table_schema = 'public'
GROUP BY table_name ORDER BY table_name;

\echo ''
\echo '=== 4. THE FORBIDDEN SET — every value must be f ==='
\echo '    (frozen-universe membership, the canonical experiment, its outcomes,'
\echo '     strategy parameters, the product signal surface)'
SELECT rel AS relation, priv,
       has_table_privilege('smart_scanner_research_lifecycle', rel, priv) AS granted
FROM (VALUES
  ('public.history_warmup_universe_symbols','INSERT'),
  ('public.history_warmup_universe_symbols','UPDATE'),
  ('public.history_warmup_universe_symbols','DELETE'),
  ('public.history_warmup_universes','INSERT'),
  ('public.history_warmup_universes','UPDATE'),
  ('public.strategy_shadow_pairs','SELECT'),
  ('public.strategy_shadow_pairs','INSERT'),
  ('public.strategy_shadow_run_pairs','INSERT'),
  ('public.strategy_shadow_runs','INSERT'),
  ('public.strategy_shadow_evaluations','INSERT'),
  ('public.strategy_shadow_pair_outcomes','SELECT'),
  ('public.strategy_shadow_pair_outcomes','INSERT'),
  ('public.patterns','UPDATE'),
  ('public.pattern_configs','UPDATE'),
  ('public.external_signals','INSERT'),
  ('public.external_signal_deliveries','SELECT'),
  ('public.prospective_campaign_registrations','INSERT'),
  ('public.daily_bars','DELETE'),
  ('public.research_symbols','DELETE'),
  ('public.job_runs','DELETE'),
  ('public.job_tasks','DELETE'),
  ('public.job_schedules','INSERT'),
  ('public.job_schedules','DELETE')
) AS t(rel, priv)
ORDER BY rel, priv;

\echo ''
\echo '=== 5. RLS predicates that ARE the boundary ==='
SELECT tablename, policyname, cmd,
       coalesce(qual, '-')       AS using_predicate,
       coalesce(with_check, '-') AS with_check_predicate
FROM pg_policies
WHERE schemaname = 'public'
  AND 'smart_scanner_research_lifecycle' = ANY(roles)
ORDER BY tablename, cmd, policyname;

\echo ''
\echo '=== 6. LIVE PROOF — the writes that must be refused ==='
SET ROLE smart_scanner_research_lifecycle;
\echo '-- current_user:'
SELECT current_user;

\echo '-- 6a. a frozen-universe daily bar (MUST fail: RLS row check):'
BEGIN;
INSERT INTO public.daily_bars (symbol, trading_date, open, high, low, close, volume)
VALUES ('SPY', DATE '1990-01-02', 1, 1, 1, 1, 1);
ROLLBACK;

\echo '-- 6b. the PRODUCT SEC freshness row (MUST fail: scope predicate):'
BEGIN;
INSERT INTO public.catalyst_source_state (source, status, scope)
VALUES ('sec_edgar', 'ok', 'product');
ROLLBACK;

\echo '-- 6c. the RESEARCH SEC freshness row (MUST succeed):'
BEGIN;
INSERT INTO public.catalyst_source_state (source, status, scope)
VALUES ('sec_edgar', 'ok', 'research')
ON CONFLICT (source, scope) DO UPDATE SET status = EXCLUDED.status;
ROLLBACK;

\echo '-- 6d. frozen-universe MEMBERSHIP (MUST fail: no privilege):'
BEGIN;
INSERT INTO public.history_warmup_universe_symbols (universe_id, symbol)
VALUES ('00000000-0000-0000-0000-000000000000', 'ZZZZ');
ROLLBACK;

\echo '-- 6e. a canonical experiment pair (MUST fail: no privilege):'
BEGIN;
SELECT count(*) FROM public.strategy_shadow_pairs;
ROLLBACK;

\echo '-- 6f. a job task on somebody else''s queue (MUST fail: queue scope):'
BEGIN;
INSERT INTO public.job_runs (job_type, job_contract_version, queue_name,
                             idempotency_key, status)
VALUES ('x','x','prospective','rls-probe-'||md5(random()::text),'queued');
ROLLBACK;

RESET ROLE;
\echo ''
\echo '=== verification complete — read section 4 (all f) and section 6 ==='
