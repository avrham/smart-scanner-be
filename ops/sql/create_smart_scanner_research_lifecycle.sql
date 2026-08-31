-- =============================================================================
-- Least-privilege PostgreSQL role for the RESEARCH LIFECYCLE worker
-- =============================================================================
-- The recurring research lifecycle needed an execution boundary and none of the
-- existing identities was the right one:
--
--   smart_scanner_pipeline_driver   cannot see a research table (deliberately),
--                                   and widening it would put research writes
--                                   inside the canonical pipeline's identity.
--   smart_scanner_history_warmer    holds the provider credential, but exists to
--                                   refresh the FROZEN universe's bars. Its
--                                   daily_bars write is unconstrained by symbol.
--   smart_scanner_market_intel      can write the PRODUCT freshness rows. The
--                                   entire point of the research cohort is that
--                                   a research run cannot.
--   smart_scanner_product_reader    read-only, and rightly blind to research.
--   smart_scanner_external_ingest   the internet-facing POST identity; nothing
--                                   about it should grow.
--
-- So: a dedicated role, and the boundaries that matter are enforced by ROW-LEVEL
-- SECURITY PREDICATES rather than by table-level grants, because the dangerous
-- privileges here are all "write to a table that also holds product rows".
--
-- THE FOUR PREDICATES THAT MATTER
-- -------------------------------
--   daily_bars              writes CONFINED to symbols in research_symbols.
--                           It can create history for a discovered symbol and
--                           is structurally unable to touch a bar belonging to
--                           the frozen 25 or the reference market.
--   catalyst_source_state   writes CONFINED to scope='research' plus the ONE
--                           named market-wide discovery feed it refreshes.
--                           It cannot claim the product's SEC/news/earnings
--                           freshness — the exact mistake migration 028 exists
--                           to make impossible.
--   symbol_catalyst_events  DELETE (the rescheduled-earnings supersede path)
--                           CONFINED to research symbols.
--   job_runs / job_tasks    queue-scoped to 'research_lifecycle' (its own work)
--                           and 'history_incremental_refresh' (to ASK for a
--                           core-bar refresh it cannot perform itself).
--
-- WHAT IT CANNOT DO, BY OMISSION
-- ------------------------------
-- No grant at all on: history_warmup_universe_symbols writes (frozen-universe
-- MEMBERSHIP), history_warmup_universes writes, strategy_shadow_* (pairs,
-- evaluations, outcomes, runs — the canonical experiment), patterns,
-- prospective_campaign_registrations, external_signals, external_signal_deliveries.
-- A research run therefore cannot enrol a symbol in the experiment, record a
-- canonical outcome, alter the frozen universe, or touch the Product API's
-- signal surface — not by policy, but because the privilege is absent.
--
-- NO DELETE, ANYWHERE, except the one confined supersede above. No TRUNCATE.
-- No CREATE. No role membership. No superuser attribute.
--
-- This file contains NO password, NO hostname and NO database name.
--
-- Apply (example):
--   psql "<admin connection>" \
--        -v research_password="$(openssl rand -base64 24)" \
--        -v db_name="<target database>" \
--        -f ops/sql/create_smart_scanner_research_lifecycle.sql
--   psql "<admin connection>" \
--        -f ops/sql/create_smart_scanner_research_lifecycle_rls_policies.sql
-- =============================================================================

\set ON_ERROR_STOP on

-- 1) The role.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles
                 WHERE rolname = 'smart_scanner_research_lifecycle') THEN
    CREATE ROLE smart_scanner_research_lifecycle
      LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
      NOBYPASSRLS;          -- must stay false: every boundary here IS a policy
  END IF;
END
$$;

\if :{?research_password}
  ALTER ROLE smart_scanner_research_lifecycle PASSWORD :'research_password';
  \echo 'research lifecycle password set from -v research_password'
\else
  \echo 'NOTE: research_password not supplied; role password left unchanged.'
\endif

-- 2) Session hardening at the ROLE level.
--    A long statement_timeout on purpose: one lifecycle warms symbols at the
--    provider's pace (one per 75s) inside a single connection. Two minutes is
--    ample for any single STATEMENT while still stopping a runaway query.
ALTER ROLE smart_scanner_research_lifecycle SET statement_timeout = '120s';
ALTER ROLE smart_scanner_research_lifecycle SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE smart_scanner_research_lifecycle SET lock_timeout = '10s';

-- 3) Connect + schema usage.
\if :{?db_name}
  GRANT CONNECT ON DATABASE :"db_name" TO smart_scanner_research_lifecycle;
  \echo 'CONNECT granted on database from -v db_name'
\else
  \echo 'NOTE: db_name not supplied; run GRANT CONNECT ON DATABASE ... manually.'
\endif
GRANT USAGE ON SCHEMA public TO smart_scanner_research_lifecycle;

-- ---------------------------------------------------------------------------
-- 4) READ-ONLY relations.
--    The frozen universe and the canonical configuration are INPUTS. The role
--    reads them to know which symbols to ignore, whether the benchmark bars are
--    fresh, and which strategy configuration the scan must use — and holds no
--    write privilege on any of them.
-- ---------------------------------------------------------------------------
GRANT SELECT ON public.history_warmup_universes         TO smart_scanner_research_lifecycle;
GRANT SELECT ON public.history_warmup_universe_symbols  TO smart_scanner_research_lifecycle;
GRANT SELECT ON public.pattern_configs                  TO smart_scanner_research_lifecycle;
GRANT SELECT ON public.patterns                         TO smart_scanner_research_lifecycle;
GRANT SELECT ON public.external_signal_sources          TO smart_scanner_research_lifecycle;

-- ---------------------------------------------------------------------------
-- 5) RESEARCH-OWNED relations: full read/write, no DELETE.
--    Reclassification rewrites a row; it never erases attempt history.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON public.research_symbols                  TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.research_scan_results             TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.research_lifecycle_runs           TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.research_lifecycle_run_symbols    TO smart_scanner_research_lifecycle;

-- ---------------------------------------------------------------------------
-- 6) SHARED relations — the grant is broad, the POLICY is narrow.
--
--    daily_bars is the important one. The grant cannot say "only research
--    symbols"; the RLS predicate can, and does. Without it this role could
--    rewrite a frozen-universe bar, which is canonical evidence.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON public.daily_bars                        TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.external_discovery_candidates     TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.catalyst_source_state             TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.sec_filings                       TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.sec_filing_symbols                TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.company_news_articles             TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.company_news_symbols              TO smart_scanner_research_lifecycle;
-- DELETE here is the ONE deletion this role may perform: the rescheduled-
-- earnings supersede path, which removes a superseded FUTURE row so a moved
-- date never leaves two competing futures on the board. RLS confines it to
-- research symbols, so a product symbol's calendar can never be touched.
GRANT SELECT, INSERT, UPDATE, DELETE ON public.symbol_catalyst_events    TO smart_scanner_research_lifecycle;

-- ---------------------------------------------------------------------------
-- 7) The durable queue plane. RLS below scopes job_runs/job_tasks to exactly
--    two queues.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON public.job_runs          TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.job_tasks         TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.job_task_attempts TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.job_events        TO smart_scanner_research_lifecycle;
GRANT SELECT, INSERT, UPDATE ON public.job_workers       TO smart_scanner_research_lifecycle;
-- The scheduler leader advances next_run_at on the schedule it OWNS. It cannot
-- create or delete a schedule, and it cannot enable a disabled one.
GRANT SELECT, UPDATE ON public.job_schedules             TO smart_scanner_research_lifecycle;
GRANT SELECT ON public.job_dependencies                  TO smart_scanner_research_lifecycle;

-- Nothing may be destroyed on the queue plane.
REVOKE DELETE, TRUNCATE ON public.job_runs          FROM smart_scanner_research_lifecycle;
REVOKE DELETE, TRUNCATE ON public.job_tasks         FROM smart_scanner_research_lifecycle;
REVOKE DELETE, TRUNCATE ON public.job_task_attempts FROM smart_scanner_research_lifecycle;
REVOKE DELETE, TRUNCATE ON public.job_events        FROM smart_scanner_research_lifecycle;
REVOKE DELETE, TRUNCATE ON public.job_schedules     FROM smart_scanner_research_lifecycle;

-- ---------------------------------------------------------------------------
-- 8) Sequences: none of the above uses one (every id is a UUID default), so no
--    sequence privilege is granted. Stated rather than left to inference.
--
-- NOT GRANTED, and this is the enforcement:
--   INSERT/UPDATE/DELETE on history_warmup_universe_symbols   (frozen-universe
--     MEMBERSHIP — a research symbol can never become one of the 25)
--   anything at all on strategy_shadow_pairs / _run_pairs / _runs /
--     _evaluations / _pair_outcomes / _outcome_runs  (the canonical experiment
--     and its outcomes)
--   anything on prospective_campaign_registrations
--   anything on external_signals / external_signal_deliveries
--   any write on patterns / pattern_configs (strategy parameters stay put)
--   ALTER DEFAULT PRIVILEGES (a future table is NOT automatically reachable)
-- ---------------------------------------------------------------------------
\echo 'smart_scanner_research_lifecycle role configured (apply the RLS file next).'
