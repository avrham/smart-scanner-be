-- =============================================================================
-- Least-privilege PostgreSQL role for the EXTERNAL-SIGNAL INGRESS
-- =============================================================================
-- Purpose: the login used by the internet-facing webhook gateway
--   POST /api/external/signals
--   GET  /api/external/sources
--   GET  /api/external/health
--
-- This is the ONLY role in this database that an anonymous internet caller can
-- cause to execute SQL. Its privileges are therefore chosen by asking a
-- different question from every other role here: not "what does this component
-- need?" but "what is the worst thing that happens if its credential leaks?"
--
-- The answer, with the grants below, is: rows appended to two tables the
-- scanner does not read, and a freshness row updated for an external source.
-- It CANNOT read or write any scanner relation, cannot change a verdict, an
-- attention tier, an evaluation or an outcome, cannot DELETE anything
-- anywhere, and cannot read price history, campaigns, jobs or any other role's
-- data.
--
-- WRITE PRIVILEGES, IN FULL (there are four, and no others):
--   INSERT on external_signal_deliveries
--   INSERT on external_signals
--   INSERT on catalyst_source_state   } confined by RLS to source LIKE
--   UPDATE on catalyst_source_state   } 'external\_%' — see section 6
--
-- There is NO UPDATE and NO DELETE on external_signals, and that is a design
-- guarantee rather than an oversight: the table is append-only (migration
-- 022), corrections arrive as new rows, and supersession is derived at read
-- time. A gateway that cannot rewrite history cannot be made to rewrite it.
--
-- This file contains NO real password, NO hostname and NO database name.
-- An operator applies it manually against the intended database.
--
-- Apply (example):
--   psql "<admin connection to the target db>" \
--        -v ingest_password="$(openssl rand -base64 24)" \
--        -v db_name="<target database>" \
--        -f ops/sql/create_smart_scanner_external_ingest.sql
--
-- psql variables (both OPTIONAL; the script is safely rerunnable):
--   :ingest_password -> a strong password supplied via -v (never committed).
--                       If omitted, the role's password is left unchanged.
--   :db_name         -> target database name for the CONNECT grant.
--                       If omitted, the CONNECT grant is skipped.
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) Role: LOGIN, inherits nothing, cannot bypass RLS.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_external_ingest') THEN
    CREATE ROLE smart_scanner_external_ingest
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

\if :{?ingest_password}
  ALTER ROLE smart_scanner_external_ingest PASSWORD :'ingest_password';
  \echo 'external_ingest password set from -v ingest_password'
\else
  \echo 'NOTE: ingest_password not supplied; role password left unchanged.'
\endif

-- 2) Session hardening at the ROLE level, so it holds for every session and
--    survives a transaction pooler.
--
--    The timeouts are AGGRESSIVE compared with every other role here, and
--    deliberately so. TradingView allows a webhook three seconds and documents
--    no retry, so a delivery that blocks is a delivery that is lost — and an
--    internet-facing endpoint must never be able to hold a connection open
--    long enough to matter. Every statement this role runs is a single-row
--    insert or a bounded lookup; none of them has any business taking 5s.
ALTER ROLE smart_scanner_external_ingest SET statement_timeout = '5s';
ALTER ROLE smart_scanner_external_ingest SET idle_in_transaction_session_timeout = '5s';
ALTER ROLE smart_scanner_external_ingest SET lock_timeout = '2s';

-- 3) Connect + schema usage only.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_external_ingest;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_external_ingest;

-- 4) The exact grants. No GRANT ... ON ALL TABLES and no ALTER DEFAULT
--    PRIVILEGES, so a future table is NOT automatically exposed to the one
--    role reachable from the internet.
--
--    SELECT accompanies each INSERT because the gateway uses `RETURNING id`,
--    which requires SELECT on the returned column. It is scoped to the same
--    three external tables.
GRANT SELECT, INSERT ON public.external_signal_deliveries TO smart_scanner_external_ingest;
GRANT SELECT, INSERT ON public.external_signals           TO smart_scanner_external_ingest;
--    The registry is READ-ONLY to the gateway. It validates against the
--    registry and reports it; it must never be able to add a source to itself.
GRANT SELECT ON public.external_signal_sources            TO smart_scanner_external_ingest;

--    Freshness. INSERT + UPDATE because the write is an upsert; both are
--    confined by RLS in section 6 to rows this role owns.
GRANT SELECT, INSERT, UPDATE ON public.catalyst_source_state TO smart_scanner_external_ingest;

--    The frozen universe, read-only, so a signal can be classified as
--    scanner_universe vs external_discovery (migration 022, Phase 20). Both
--    relations carry an immutability trigger already, and this role has no
--    write grant on either.
GRANT SELECT ON public.history_warmup_universes        TO smart_scanner_external_ingest;
GRANT SELECT ON public.history_warmup_universe_symbols TO smart_scanner_external_ingest;

-- 5) Row-level security for the external tables.
--    RLS is enabled by migration 022 and this role is NOBYPASSRLS, so a plain
--    grant alone yields nothing. One narrow policy per relation, scoped TO
--    this role. No existing policy is modified, so no other role's effective
--    result set changes.
DO $$
DECLARE
  rel text;
BEGIN
  -- Read policies (needed for RETURNING and for /external/sources).
  FOREACH rel IN ARRAY ARRAY[
    'external_signal_sources',
    'external_signal_deliveries',
    'external_signals'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public' AND tablename = rel
        AND policyname = 'smart_scanner_external_ingest_select'
    ) THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_external_ingest_select ON public.%I '
        'FOR SELECT TO smart_scanner_external_ingest USING (true)', rel);
    END IF;
  END LOOP;

  -- The frozen universe, read-only. RLS is enabled on both relations, and
  -- this role is NOBYPASSRLS, so the SELECT grant above yields ZERO ROWS
  -- without a policy. That failure is SILENT and its symptom is subtle: every
  -- signal is classified `external_discovery` and quietly vanishes from the
  -- product, because an empty universe is indistinguishable from "this symbol
  -- is not in it". Section 11 asserts the universe is actually readable for
  -- exactly this reason.
  FOREACH rel IN ARRAY ARRAY[
    'history_warmup_universes',
    'history_warmup_universe_symbols'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public' AND tablename = rel
        AND policyname = 'smart_scanner_external_ingest_select'
    ) THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_external_ingest_select ON public.%I '
        'FOR SELECT TO smart_scanner_external_ingest USING (true)', rel);
    END IF;
  END LOOP;

  -- Append policies. Only the two signal tables, never the registry.
  FOREACH rel IN ARRAY ARRAY[
    'external_signal_deliveries',
    'external_signals'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public' AND tablename = rel
        AND policyname = 'smart_scanner_external_ingest_insert'
    ) THEN
      EXECUTE format(
        'CREATE POLICY smart_scanner_external_ingest_insert ON public.%I '
        'FOR INSERT TO smart_scanner_external_ingest WITH CHECK (true)', rel);
    END IF;
  END LOOP;
END
$$;

-- 6) The important one: the freshness table is SHARED with the earnings, news
--    and SEC dimensions, and this role is the only internet-reachable writer
--    in the database. A bare UPDATE grant would let a leaked ingress
--    credential mark `sec_edgar_8k` as failed and quietly degrade a dimension
--    it has nothing to do with.
--
--    So the policies below confine every read and write to rows whose source
--    name begins with `external_` — the namespace `app.external_signals.
--    source_state_key()` produces. The predicate is enforced by PostgreSQL,
--    not by the application, so no code path can forget it.
--
--    CONVERGENT: dropped and recreated so an earlier, wider version of these
--    policies is narrowed in place rather than left standing beside the new one.
DROP POLICY IF EXISTS smart_scanner_external_ingest_state_select
  ON public.catalyst_source_state;
DROP POLICY IF EXISTS smart_scanner_external_ingest_state_insert
  ON public.catalyst_source_state;
DROP POLICY IF EXISTS smart_scanner_external_ingest_state_update
  ON public.catalyst_source_state;

CREATE POLICY smart_scanner_external_ingest_state_select
  ON public.catalyst_source_state
  FOR SELECT TO smart_scanner_external_ingest
  USING (source LIKE 'external\_%');

CREATE POLICY smart_scanner_external_ingest_state_insert
  ON public.catalyst_source_state
  FOR INSERT TO smart_scanner_external_ingest
  WITH CHECK (source LIKE 'external\_%');

CREATE POLICY smart_scanner_external_ingest_state_update
  ON public.catalyst_source_state
  FOR UPDATE TO smart_scanner_external_ingest
  USING (source LIKE 'external\_%')
  WITH CHECK (source LIKE 'external\_%');

-- NOT granted (by design): CREATE, DELETE, TRUNCATE, TRIGGER, any sequence
-- privilege, UPDATE on any external table, membership in any application or
-- service role, any job queue or scheduler privilege, and SELECT on every
-- scanner relation — bars, runs, pairs, evaluations, outcomes, campaigns,
-- jobs, tickers, signals and the catalyst/news/SEC content tables.
