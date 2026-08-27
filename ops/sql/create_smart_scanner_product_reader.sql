-- =============================================================================
-- Least-privilege PostgreSQL role for the Smart Scanner PRODUCT API
-- =============================================================================
-- Purpose: a read-only login that can serve ONLY the three UI-facing routes
--   GET /api/scanner/overview
--   GET /api/scanner/scans
--   GET /api/scanner/symbol
-- It holds SELECT on the EXACT 11 relations those routes touch and NO write
-- privilege anywhere. It is deliberately narrower than
-- smart_scanner_audit_reader (13 relations): the product surface must not grow
-- just because the audit surface did.
--
-- This file contains NO real password, NO hostname and NO database name.
-- An operator applies it manually against the intended database.
--
-- Apply (example):
--   psql "<admin connection to the target db>" \
--        -v product_password="$(openssl rand -base64 24)" \
--        -v db_name="<target database>" \
--        -f ops/sql/create_smart_scanner_product_reader.sql
--
-- psql variables (both OPTIONAL; the script is safely rerunnable):
--   :product_password -> a strong password supplied via -v (never committed).
--                        If omitted, the role's password is left unchanged.
--   :db_name          -> target database name for the CONNECT grant.
--                        If omitted, the CONNECT grant is skipped.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role: LOGIN, inherits nothing, cannot bypass RLS.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_product_reader') THEN
    CREATE ROLE smart_scanner_product_reader
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

\if :{?product_password}
  ALTER ROLE smart_scanner_product_reader PASSWORD :'product_password';
  \echo 'product_reader password set from -v product_password'
\else
  \echo 'NOTE: product_password not supplied; role password left unchanged.'
\endif

-- 2) Session hardening at the ROLE level, so it holds for every session and
--    survives a transaction pooler (unlike ad-hoc per-session SETs).
ALTER ROLE smart_scanner_product_reader SET default_transaction_read_only = on;
ALTER ROLE smart_scanner_product_reader SET statement_timeout = '30s';
ALTER ROLE smart_scanner_product_reader SET idle_in_transaction_session_timeout = '15s';
ALTER ROLE smart_scanner_product_reader SET lock_timeout = '5s';

-- 3) Connect + schema usage only.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_product_reader;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_product_reader;

-- 4) SELECT on the EXACT relations the three product routes read — nothing
--    else. No GRANT SELECT ON ALL TABLES and no ALTER DEFAULT PRIVILEGES, so
--    future tables are NOT automatically exposed.
--      strategy_shadow_runs        -> latest scan identity/state + scan list
--      strategy_shadow_run_pairs   -> scan membership (the real universe)
--      strategy_shadow_pairs       -> per-symbol pair identity
--      strategy_shadow_evaluations -> candidate/control verdicts + evidence
--      daily_bars                  -> readiness + recent price context
--      symbol_catalyst_events      -> earnings / report-filing context (019)
--      catalyst_source_state       -> per-source availability + freshness (019)
--      company_news_articles       -> company news context (020)
--      company_news_symbols        -> per-symbol news association (020)
--      sec_filings                 -> SEC 8-K material events (021)
--      sec_filing_symbols          -> issuer/symbol association (021)
--
--    The catalyst, news and SEC relations are READ-ONLY here for the same reason as
--    the rest: the Product API holds no provider credential and writes nothing.
GRANT SELECT ON public.strategy_shadow_runs        TO smart_scanner_product_reader;
GRANT SELECT ON public.strategy_shadow_run_pairs   TO smart_scanner_product_reader;
GRANT SELECT ON public.strategy_shadow_pairs       TO smart_scanner_product_reader;
GRANT SELECT ON public.strategy_shadow_evaluations TO smart_scanner_product_reader;
GRANT SELECT ON public.daily_bars                  TO smart_scanner_product_reader;
GRANT SELECT ON public.symbol_catalyst_events      TO smart_scanner_product_reader;
GRANT SELECT ON public.catalyst_source_state       TO smart_scanner_product_reader;
GRANT SELECT ON public.company_news_articles       TO smart_scanner_product_reader;
GRANT SELECT ON public.company_news_symbols        TO smart_scanner_product_reader;
GRANT SELECT ON public.sec_filings                 TO smart_scanner_product_reader;
GRANT SELECT ON public.sec_filing_symbols          TO smart_scanner_product_reader;

-- 5) Row-level security.
--    RLS is ENABLED on all eleven relations in the isolated staging database and
--    this role is NOBYPASSRLS, so a plain SELECT grant alone returns zero rows.
--    Add one NARROW read-only policy per relation, scoped TO this role — the
--    same pattern every other reader role in this database already uses. RLS is
--    never disabled and no existing policy is modified, so no other role's
--    effective result set changes.
--
--    These relations carry no tenant/owner discriminator, so the product read
--    set is the whole relation: USING (true) FOR SELECT, for this role only.
DO $$
DECLARE
  rel text;
BEGIN
  FOREACH rel IN ARRAY ARRAY[
    'strategy_shadow_runs',
    'strategy_shadow_run_pairs',
    'strategy_shadow_pairs',
    'strategy_shadow_evaluations',
    'daily_bars',
    'symbol_catalyst_events',
    'catalyst_source_state',
    'company_news_articles',
    'company_news_symbols',
    'sec_filings',
    'sec_filing_symbols'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public' AND tablename = rel
        AND policyname = 'smart_scanner_product_reader_select'
    ) THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_product_reader_select ON public.%I '
        'FOR SELECT TO smart_scanner_product_reader USING (true)', rel);
    END IF;
  END LOOP;
END
$$;

-- NOT granted (by design): CREATE, INSERT, UPDATE, DELETE, TRUNCATE, TRIGGER,
-- sequence privileges, membership in any application/service role, any job
-- queue lifecycle or scheduler privilege, and any provider-related write.
