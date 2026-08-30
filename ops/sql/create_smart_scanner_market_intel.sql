-- =============================================================================
-- Least-privilege PostgreSQL role for the Wave 2 MARKET-INTELLIGENCE ingestion
-- =============================================================================
-- One login for the three PULLED external sources:
--
--   ops.analysis.refresh_macro_calendar    federalreserve.gov + bea.gov
--   ops.analysis.refresh_analyst_grades    FMP /stable/grades
--   ops.analysis.refresh_discovery_candidates   FMP /stable movers  (023)
--
-- WHY NOT REUSE THE EXTERNAL-INGEST ROLE
-- --------------------------------------
-- `smart_scanner_external_ingest` belongs to the ONE app that accepts a POST
-- from the public internet. Everything about it — its Fly app, its bounded
-- route allowlist, its append-only grants — exists to bound the blast radius of
-- an internet-facing write path. These three sources are the opposite shape:
-- we call OUT to them on a schedule, over no exposed port, and we need UPDATE
-- (a calendar entry that moves must be corrected) which that role deliberately
-- does not have. Widening it would trade a real guarantee for a saved file.
--
-- WHY NOT REUSE THE PRODUCT READER
-- --------------------------------
-- It is `default_transaction_read_only` and holds no provider credential, by
-- design. Ingestion is a write path holding an API key; the two must not meet.
--
-- WHAT THIS ROLE CANNOT DO
-- ------------------------
-- Nothing on any scanner relation: no strategy_shadow_*, no daily_bars write,
-- no universe membership write, no job queue, no scheduler. It cannot add a
-- symbol to a universe or enqueue a scan even by accident, which is the
-- experiment boundary expressed as a privilege rather than as a convention.
-- Its ONE shared table, `catalyst_source_state`, is confined by RLS to rows
-- whose source name begins `external_`, so it can never overwrite the earnings,
-- news or SEC freshness rows written by other components.
--
-- This file contains NO real password, NO hostname and NO database name.
--
-- Apply (example):
--   psql "<admin connection to the target db>" \
--        -v intel_password="$(openssl rand -base64 24)" \
--        -v db_name="<target database>" \
--        -f ops/sql/create_smart_scanner_market_intel.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    CREATE ROLE smart_scanner_market_intel
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

\if :{?intel_password}
  ALTER ROLE smart_scanner_market_intel PASSWORD :'intel_password';
  \echo 'market_intel password set from -v intel_password'
\else
  \echo 'NOTE: intel_password not supplied; role password left unchanged.'
\endif

-- 2) Session hardening. NOT read-only — this role writes — but bounded, so a
--    stuck HTTP parse can never hold a lock on a shared table indefinitely.
ALTER ROLE smart_scanner_market_intel SET statement_timeout = '60s';
ALTER ROLE smart_scanner_market_intel SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE smart_scanner_market_intel SET lock_timeout = '10s';

-- 3) Connect + schema usage only.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_market_intel;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_market_intel;

-- 4) READ-ONLY inputs.
--      external_signal_sources          -> the registry (FK target + licensing)
--      history_warmup_universes         -> the frozen universe, to mark which
--      history_warmup_universe_symbols     side of the line a symbol fell on
--      daily_bars                       -> the cross-reference report's
--                                          "do we even hold history for this"
GRANT SELECT ON public.external_signal_sources        TO smart_scanner_market_intel;
GRANT SELECT ON public.history_warmup_universes       TO smart_scanner_market_intel;
GRANT SELECT ON public.history_warmup_universe_symbols TO smart_scanner_market_intel;
GRANT SELECT ON public.daily_bars                     TO smart_scanner_market_intel;

-- 5) The three ingestion targets.
--    macro_events needs UPDATE: a published schedule is corrected in place when
--    an agency moves a date, and the withdrawal sweep marks rather than deletes.
GRANT SELECT, INSERT, UPDATE ON public.macro_events                  TO smart_scanner_market_intel;
--    Discovery ranks are re-stated within a session, so UPDATE is required.
GRANT SELECT, INSERT, UPDATE ON public.external_discovery_candidates TO smart_scanner_market_intel;
--    Analyst grade events are historical facts: INSERT only, never UPDATE.
--    The absence of UPDATE here is the append-only guarantee.
GRANT SELECT, INSERT         ON public.analyst_grade_events          TO smart_scanner_market_intel;
--    Freshness rows for its own sources only (RLS-confined below).
GRANT SELECT, INSERT, UPDATE ON public.catalyst_source_state         TO smart_scanner_market_intel;
--    The A3 aggregation view.
GRANT SELECT                 ON public.external_discovery_current    TO smart_scanner_market_intel;

-- 6) Row-level security. RLS is enabled on these relations and the role is
--    NOBYPASSRLS, so grants alone return and write nothing.
DO $$
DECLARE
  rel text;
BEGIN
  -- Read-only relations: one SELECT policy each.
  FOREACH rel IN ARRAY ARRAY[
    'external_signal_sources',
    'history_warmup_universes',
    'history_warmup_universe_symbols',
    'daily_bars'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname = 'public' AND tablename = rel
                     AND policyname = 'smart_scanner_market_intel_select') THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_market_intel_select ON public.%I '
        'FOR SELECT TO smart_scanner_market_intel USING (true)', rel);
    END IF;
  END LOOP;

  -- Owned relations: full read/write within the relation.
  FOREACH rel IN ARRAY ARRAY[
    'macro_events',
    'external_discovery_candidates',
    'analyst_grade_events'
  ] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname = 'public' AND tablename = rel
                     AND policyname = 'smart_scanner_market_intel_rw') THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_market_intel_rw ON public.%I '
        'FOR ALL TO smart_scanner_market_intel '
        'USING (true) WITH CHECK (true)', rel);
    END IF;
  END LOOP;
END
$$;

-- The one SHARED table. Confined by predicate, not by trust: this role may
-- read every freshness row (a report wants the whole picture) but may only
-- write rows whose source name begins `external_`. The earnings, news and SEC
-- freshness rows are therefore unreachable for writing, which is what stops a
-- broken macro refresh from ever being able to report the news feed as healthy.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies
                 WHERE schemaname = 'public' AND tablename = 'catalyst_source_state'
                   AND policyname = 'smart_scanner_market_intel_state_select') THEN
    CREATE POLICY smart_scanner_market_intel_state_select
      ON public.catalyst_source_state
      FOR SELECT TO smart_scanner_market_intel USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies
                 WHERE schemaname = 'public' AND tablename = 'catalyst_source_state'
                   AND policyname = 'smart_scanner_market_intel_state_insert') THEN
    CREATE POLICY smart_scanner_market_intel_state_insert
      ON public.catalyst_source_state
      FOR INSERT TO smart_scanner_market_intel
      WITH CHECK (source LIKE 'external\_%');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies
                 WHERE schemaname = 'public' AND tablename = 'catalyst_source_state'
                   AND policyname = 'smart_scanner_market_intel_state_update') THEN
    CREATE POLICY smart_scanner_market_intel_state_update
      ON public.catalyst_source_state
      FOR UPDATE TO smart_scanner_market_intel
      USING (source LIKE 'external\_%')
      WITH CHECK (source LIKE 'external\_%');
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- WAVE 3 — the research domain (migration 026 grants these; repeated here so
-- this file remains the single readable statement of what the role may do).
--
--   pattern_configs         SELECT ONLY — the canonical strategy configuration
--                           the research scan must evaluate with. Read, never
--                           written: pattern_configs stays operator-only.
--   sec_filings             SELECT, INSERT, UPDATE — PUBLIC EDGAR data, used
--   sec_filing_symbols      by the lazy enrichment stage for symbols that
--                           survived the research screen. No DELETE.
--   research_symbols        SELECT, INSERT, UPDATE
--   research_scan_results   SELECT, INSERT, UPDATE
--   daily_bars              INSERT, UPDATE — but CONFINED BY RLS to symbols
--                           that are actually research symbols, so a research
--                           warmup can never touch a frozen-universe bar.
--
-- Still NO privilege on any universe relation, so a discovered symbol cannot
-- become a universe member through this role even by accident. That is the
-- experiment boundary expressed as a privilege rather than as a convention.
-- ---------------------------------------------------------------------------

-- NOT granted (by design): every strategy_shadow_* relation, every job queue
-- and scheduler relation, market_data_jobs, any universe MEMBERSHIP write, any
-- DELETE anywhere, TRUNCATE, CREATE, sequence privileges, and membership in
-- any other application role.
