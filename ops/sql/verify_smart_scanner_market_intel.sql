-- =============================================================================
-- Verification for smart_scanner_market_intel — run AS THAT ROLE
-- =============================================================================
--   psql "<market-intel DSN>" -f ops/sql/verify_smart_scanner_market_intel.sql
--
-- Every check below must produce the stated expectation. A single deviation is
-- a STOP condition: this role holds a provider credential, so an over-broad
-- grant here is the one that matters most.
-- =============================================================================

\echo '--- 1. identity (expect smart_scanner_market_intel) ---'
SELECT current_user, session_user;

\echo '--- 2. cannot bypass RLS (expect f) ---'
SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user;

\echo '--- 3. writable relations (expect EXACTLY macro_events,'
\echo '       external_discovery_candidates, analyst_grade_events,'
\echo '       catalyst_source_state) ---'
SELECT table_name, string_agg(privilege_type, ',' ORDER BY privilege_type)
FROM information_schema.table_privileges
WHERE grantee = current_user
  AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
GROUP BY table_name
ORDER BY table_name;

\echo '--- 4. analyst_grade_events must be APPEND-ONLY (expect INSERT,SELECT) ---'
SELECT string_agg(privilege_type, ',' ORDER BY privilege_type)
FROM information_schema.table_privileges
WHERE grantee = current_user AND table_name = 'analyst_grade_events';

\echo '--- 5. NO privilege on any scanner relation (expect zero rows) ---'
SELECT table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = current_user
  AND (table_name LIKE 'strategy_shadow%'
       OR table_name LIKE 'job_%'
       OR table_name = 'market_data_jobs');

\echo '--- 6. forbidden-write probe: daily_bars (expect permission denied) ---'
INSERT INTO public.daily_bars(symbol, trading_date, open, high, low, close, volume)
VALUES ('__INTEL_PROBE__', DATE '2000-01-03', 1, 1, 1, 1, 1);

\echo '--- 7. shared-table confinement probe: a NON-external source state row'
\echo '       (expect: new row violates row-level security policy) ---'
INSERT INTO public.catalyst_source_state(source, status)
VALUES ('provider_company_news', 'ok');
