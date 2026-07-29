-- =============================================================================
-- Least-privilege PostgreSQL role for the History-Warmup Environment
-- =============================================================================
-- Purpose: a narrowly-scoped login used ONLY by the (future) bounded history
-- warmup path. It can SELECT the readiness read relations and INSERT/UPDATE ONLY
-- daily_bars, market_bars_4h and history_warmup_runs. It has NO DELETE/TRUNCATE/
-- CREATE/TRIGGER anywhere, no ownership, no BYPASSRLS, and NO access to
-- campaigns, evaluations, pairs, pair outcomes or arm outcomes. It is a SEPARATE
-- role — it never reuses smart_scanner_outcome_maintainer or
-- smart_scanner_audit_reader.
--
-- Access matrix:
--   SELECT  : daily_bars, market_bars_4h, history_warmup_runs, patterns,
--             pattern_configs
--   INSERT  : daily_bars, market_bars_4h, history_warmup_runs
--   UPDATE  : daily_bars, market_bars_4h, history_warmup_runs
--   NONE    : strategy_shadow_* (evaluations / pairs / run_pairs / runs /
--             pair_outcomes / outcome_runs) — no SELECT, no write; no DELETE /
--             TRUNCATE / DDL / TRIGGER anywhere; no sequences (UUID defaults).
--
-- This file contains NO real password, NO Supabase ref, NO hostname, NO db name.
-- Apply manually against the intended NON-PRODUCTION database:
--   psql "<admin conn>" \
--        -v warmer_password="$(openssl rand -base64 24)" \
--        -v db_name="postgres" \
--        -f ops/sql/create_shadow_history_warmer.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role: LOGIN, no broad inherit, cannot bypass RLS.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_history_warmer') THEN
    CREATE ROLE smart_scanner_history_warmer
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

\if :{?warmer_password}
  ALTER ROLE smart_scanner_history_warmer PASSWORD :'warmer_password';
  \echo 'history-warmer password set from -v warmer_password'
\else
  \echo 'NOTE: warmer_password not supplied; role password left unchanged.'
\endif

-- 2) Bounded session defaults. This role performs the narrowly-authorized
--    market-data writes, so default_transaction_read_only is explicitly OFF
--    (write scope enforced by exact grants + RLS, never broad privilege).
ALTER ROLE smart_scanner_history_warmer SET default_transaction_read_only = off;
ALTER ROLE smart_scanner_history_warmer SET statement_timeout = '55s';
ALTER ROLE smart_scanner_history_warmer SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_history_warmer SET lock_timeout = '10s';

-- 3) Connect + schema usage only.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_history_warmer;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_history_warmer;

-- 4) SELECT on the EXACT readiness read relations. No ALL TABLES, no default
--    privileges — future tables are never auto-exposed. Deliberately NO SELECT
--    on any strategy_shadow_* relation.
GRANT SELECT ON public.daily_bars               TO smart_scanner_history_warmer;
GRANT SELECT ON public.market_bars_4h           TO smart_scanner_history_warmer;
GRANT SELECT ON public.history_warmup_runs      TO smart_scanner_history_warmer;
GRANT SELECT ON public.patterns                 TO smart_scanner_history_warmer;
GRANT SELECT ON public.pattern_configs          TO smart_scanner_history_warmer;
-- history_warmup_run_items (migration 015): SELECT only granted when the table
-- exists (older databases predate it). Owner-safe: skipped when absent.
DO $$
BEGIN
  IF to_regclass('public.history_warmup_run_items') IS NOT NULL THEN
    GRANT SELECT ON public.history_warmup_run_items TO smart_scanner_history_warmer;
    GRANT INSERT, UPDATE ON public.history_warmup_run_items TO smart_scanner_history_warmer;
  END IF;
  -- Frozen universes (migration 016): the warmer may create + freeze a draft
  -- universe (SELECT/INSERT/UPDATE on the universe row; SELECT/INSERT its draft
  -- membership) but NEVER UPDATE/DELETE membership — post-freeze immutability is
  -- enforced by the DB trigger regardless of grants. No DELETE anywhere.
  IF to_regclass('public.history_warmup_universes') IS NOT NULL THEN
    GRANT SELECT, INSERT, UPDATE ON public.history_warmup_universes        TO smart_scanner_history_warmer;
    GRANT SELECT, INSERT         ON public.history_warmup_universe_symbols TO smart_scanner_history_warmer;
  END IF;
END
$$;

-- 5) INSERT + UPDATE on ONLY the write relations. No DELETE / TRUNCATE /
--    TRIGGER / REFERENCES; no other table.
GRANT INSERT, UPDATE ON public.daily_bars          TO smart_scanner_history_warmer;
GRANT INSERT, UPDATE ON public.market_bars_4h      TO smart_scanner_history_warmer;
GRANT INSERT, UPDATE ON public.history_warmup_runs TO smart_scanner_history_warmer;

-- 6) Defensive: ensure NO privileges leaked onto any strategy_shadow_* relation.
REVOKE ALL ON public.strategy_shadow_evaluations   FROM smart_scanner_history_warmer;
REVOKE ALL ON public.strategy_shadow_pairs         FROM smart_scanner_history_warmer;
REVOKE ALL ON public.strategy_shadow_run_pairs     FROM smart_scanner_history_warmer;
REVOKE ALL ON public.strategy_shadow_runs          FROM smart_scanner_history_warmer;
REVOKE ALL ON public.strategy_shadow_pair_outcomes FROM smart_scanner_history_warmer;
-- (strategy_shadow_outcome_runs REVOKE omitted-safe: never granted; add if present)

\echo 'smart_scanner_history_warmer role configured (least-privilege).'
