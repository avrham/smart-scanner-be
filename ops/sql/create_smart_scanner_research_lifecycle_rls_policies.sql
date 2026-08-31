-- =============================================================================
-- create_smart_scanner_research_lifecycle_rls_policies.sql
--
-- With RLS enabled, a grant alone is inert. These policies ARE the boundary —
-- the grants in create_smart_scanner_research_lifecycle.sql are deliberately
-- coarse because table-level privilege cannot express "only research symbols",
-- and the predicate can.
--
-- Idempotent AND CONVERGENT: every constrained policy is DROP+CREATEd so a live
-- database carrying an older predicate is upgraded rather than left with a
-- stale rule that a CREATE-IF-NOT-EXISTS would silently preserve. RLS is never
-- disabled and no other role's policy is touched, so no other role's effective
-- result set changes.
--
-- Run AFTER create_smart_scanner_research_lifecycle.sql.
-- =============================================================================

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION pg_temp._ensure_policy(
    tbl text, pol text, role text, cmd text) RETURNS void AS $fn$
DECLARE
  rel regclass := to_regclass(tbl);
BEGIN
  IF rel IS NULL THEN RETURN; END IF;
  IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel) THEN
    RETURN;                      -- never create an inert policy on a non-RLS relation
  END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = rel AND polname = pol) THEN
    RETURN;
  END IF;
  IF cmd = 'INSERT' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', pol, tbl, role);
  ELSIF cmd = 'ALL' THEN
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)', pol, tbl, role);
  ELSE
    EXECUTE format('CREATE POLICY %I ON %s AS PERMISSIVE FOR %s TO %I USING (true)', pol, tbl, cmd, role);
  END IF;
END
$fn$ LANGUAGE plpgsql;

DO $$
DECLARE
  r constant text := 'smart_scanner_research_lifecycle';
  queues constant text := '(''research_lifecycle'',''history_incremental_refresh'')';
  -- Every row of these is research's own; full-row is the truth, not laziness.
  own_rels constant text[] := ARRAY[
    'public.research_symbols', 'public.research_scan_results',
    'public.research_lifecycle_runs', 'public.research_lifecycle_run_symbols',
    'public.external_discovery_candidates',
    'public.sec_filings', 'public.sec_filing_symbols',
    'public.company_news_articles', 'public.company_news_symbols'];
  read_rels constant text[] := ARRAY[
    'public.history_warmup_universes', 'public.history_warmup_universe_symbols',
    'public.pattern_configs', 'public.patterns',
    'public.external_signal_sources', 'public.job_dependencies'];
  queue_full_rels constant text[] := ARRAY[
    'public.job_task_attempts', 'public.job_events', 'public.job_workers'];
  t text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
    RAISE EXCEPTION 'role % does not exist (run create_smart_scanner_research_lifecycle.sql first)', r;
  END IF;

  -- ---- 1. daily_bars: writes CONFINED TO RESEARCH SYMBOLS -----------------
  -- The single most important predicate in this file. The role must be able to
  -- create history for a discovered symbol and must be structurally unable to
  -- touch a bar belonging to the frozen 25 or the reference market — those are
  -- canonical evidence for the experiment and the product.
  --
  -- SELECT is full-row: the research scan reads BENCHMARK bars (SPY, sector
  -- ETFs) to compute relative strength, and a read cannot corrupt anything.
  -- INSERT/UPDATE are the constrained ones.
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.daily_bars', r||'_select');
  EXECUTE format(
    'CREATE POLICY %I ON public.daily_bars AS PERMISSIVE FOR SELECT TO %I '
    'USING (true)', r||'_select', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.daily_bars', r||'_insert');
  EXECUTE format(
    'CREATE POLICY %I ON public.daily_bars AS PERMISSIVE FOR INSERT TO %I '
    'WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols))',
    r||'_insert', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.daily_bars', r||'_update');
  EXECUTE format(
    'CREATE POLICY %I ON public.daily_bars AS PERMISSIVE FOR UPDATE TO %I '
    'USING (symbol IN (SELECT symbol FROM public.research_symbols)) '
    'WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols))',
    r||'_update', r);

  -- ---- 2. catalyst_source_state: RESEARCH SCOPE, plus one named feed ------
  -- Migration 028 split this table by cohort so a research refresh can never be
  -- read as the product's freshness. This predicate is what makes that
  -- structural rather than conventional: the role literally cannot write
  -- (sec_edgar, product).
  --
  -- The one exception is named, not general. `external_fmp_discovery` is a
  -- MARKET-WIDE movers feed — it has no cohort, the whole market shares it, and
  -- the lifecycle refreshes it as its first stage. Splitting it per cohort
  -- would invent a distinction the source does not have, so instead the
  -- exception is written down here where it can be read and audited.
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.catalyst_source_state', r||'_select');
  EXECUTE format(
    'CREATE POLICY %I ON public.catalyst_source_state AS PERMISSIVE FOR SELECT '
    'TO %I USING (true)', r||'_select', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.catalyst_source_state', r||'_insert');
  EXECUTE format(
    'CREATE POLICY %I ON public.catalyst_source_state AS PERMISSIVE FOR INSERT '
    'TO %I WITH CHECK (scope = ''research'' '
    '                  OR source = ''external_fmp_discovery'')',
    r||'_insert', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.catalyst_source_state', r||'_update');
  EXECUTE format(
    'CREATE POLICY %I ON public.catalyst_source_state AS PERMISSIVE FOR UPDATE '
    'TO %I USING (scope = ''research'' OR source = ''external_fmp_discovery'') '
    'WITH CHECK (scope = ''research'' OR source = ''external_fmp_discovery'')',
    r||'_update', r);

  -- ---- 3. symbol_catalyst_events: DELETE confined to research symbols -----
  -- The supersede path removes a superseded FUTURE earnings row. It is the only
  -- deletion this role may perform anywhere, and it must never reach a product
  -- symbol's calendar.
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.symbol_catalyst_events', r||'_select');
  EXECUTE format(
    'CREATE POLICY %I ON public.symbol_catalyst_events AS PERMISSIVE FOR SELECT '
    'TO %I USING (true)', r||'_select', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.symbol_catalyst_events', r||'_insert');
  EXECUTE format(
    'CREATE POLICY %I ON public.symbol_catalyst_events AS PERMISSIVE FOR INSERT '
    'TO %I WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols))',
    r||'_insert', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.symbol_catalyst_events', r||'_update');
  EXECUTE format(
    'CREATE POLICY %I ON public.symbol_catalyst_events AS PERMISSIVE FOR UPDATE '
    'TO %I USING (symbol IN (SELECT symbol FROM public.research_symbols)) '
    'WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols))',
    r||'_update', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.symbol_catalyst_events', r||'_delete');
  EXECUTE format(
    'CREATE POLICY %I ON public.symbol_catalyst_events AS PERMISSIVE FOR DELETE '
    'TO %I USING (symbol IN (SELECT symbol FROM public.research_symbols))',
    r||'_delete', r);

  -- ---- 4. job_runs / job_tasks: queue-scoped ------------------------------
  -- 'research_lifecycle' is its own work. 'history_incremental_refresh' is
  -- there so a blocked lifecycle can ASK for the core-bar refresh it is
  -- deliberately unable to perform — the dedicated history-refresh worker,
  -- which holds the provider credential and can write a frozen-universe bar,
  -- executes it. Enqueueing is not executing, and this role listens on only
  -- its own queue.
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.job_runs', r||'_qscope');
  EXECUTE format(
    'CREATE POLICY %I ON public.job_runs AS PERMISSIVE FOR ALL TO %I '
    'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
    r||'_qscope', r, queues, queues);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.job_tasks', r||'_qscope');
  EXECUTE format(
    'CREATE POLICY %I ON public.job_tasks AS PERMISSIVE FOR ALL TO %I '
    'USING (queue_name IN %s) WITH CHECK (queue_name IN %s)',
    r||'_qscope', r, queues, queues);

  -- ---- 5. job_schedules: read all, advance ONLY the schedule it owns ------
  -- The leader must UPDATE next_run_at after a fire. It must not be able to
  -- enable a disabled schedule, and it must not be able to advance somebody
  -- else's occurrence past its due time — which would silently skip a run of
  -- the canonical daily pipeline.
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.job_schedules', r||'_select');
  EXECUTE format(
    'CREATE POLICY %I ON public.job_schedules AS PERMISSIVE FOR SELECT TO %I '
    'USING (true)', r||'_select', r);
  EXECUTE format('DROP POLICY IF EXISTS %I ON public.job_schedules', r||'_update');
  EXECUTE format(
    'CREATE POLICY %I ON public.job_schedules AS PERMISSIVE FOR UPDATE TO %I '
    'USING (payload_template ->> ''scheduler_owner'' = ''research_lifecycle'') '
    'WITH CHECK (payload_template ->> ''scheduler_owner'' = ''research_lifecycle'')',
    r||'_update', r);

  -- ---- 6. full-row relations ---------------------------------------------
  FOREACH t IN ARRAY own_rels LOOP
    PERFORM pg_temp._ensure_policy(t, r||'_select', r, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, r||'_insert', r, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, r||'_update', r, 'UPDATE');
  END LOOP;
  FOREACH t IN ARRAY queue_full_rels LOOP
    PERFORM pg_temp._ensure_policy(t, r||'_select', r, 'SELECT');
    PERFORM pg_temp._ensure_policy(t, r||'_insert', r, 'INSERT');
    PERFORM pg_temp._ensure_policy(t, r||'_update', r, 'UPDATE');
  END LOOP;
  FOREACH t IN ARRAY read_rels LOOP
    PERFORM pg_temp._ensure_policy(t, r||'_select', r, 'SELECT');
  END LOOP;

  RAISE NOTICE 'research-lifecycle RLS ensured (daily_bars + source-state + '
               'catalyst-delete + queue scoping).';
END
$$;

\echo 'research-lifecycle RLS policies configured.'
