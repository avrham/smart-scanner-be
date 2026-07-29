-- =============================================================================
-- RLS policies for the new 4H/warmup tables
-- =============================================================================
-- market_bars_4h + history_warmup_runs (RLS enabled by migration 014). Grants:
--   * audit reader  : SELECT only (read the local 4H store for readiness v2).
--   * history warmer: SELECT + INSERT + UPDATE (no DELETE for anyone).
-- Owner-run, rerunnable. Never enables/forces RLS elsewhere, never grants
-- BYPASSRLS, never touches ownership or unrelated policies. No role gets DELETE.
-- Run as the table owner:
--   psql "<admin conn>" -f ops/sql/create_shadow_history_warmer_rls_policies.sql
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
  audit_role  constant text := 'smart_scanner_audit_reader';
  warmer_role constant text := 'smart_scanner_history_warmer';
  rels        text[] := ARRAY['public.market_bars_4h', 'public.history_warmup_runs'];
  t           text;
  rel_oid     regclass;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = warmer_role) THEN
    RAISE EXCEPTION 'role % does not exist (run create_shadow_history_warmer.sql first)', warmer_role;
  END IF;

  -- history_warmup_run_items (migration 015) gets the SAME policy set as the 4H
  -- tables when present (older databases predate it).
  IF to_regclass('public.history_warmup_run_items') IS NOT NULL THEN
    rels := array_append(rels, 'public.history_warmup_run_items');
  END IF;

  FOREACH t IN ARRAY rels LOOP
    rel_oid := to_regclass(t);
    IF rel_oid IS NULL THEN
      RAISE EXCEPTION 'required relation % is missing (run migration 014 first)', t;
    END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', t);
    END IF;

    -- table-level grants (RLS governs rows; grants govern the privilege)
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = audit_role) THEN
      EXECUTE format('GRANT SELECT ON %s TO %I', t, audit_role);
    END IF;
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON %s TO %I', t, warmer_role);

    -- audit reader: SELECT policy (full row)
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = audit_role)
       AND NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                       AND p.polname = 'history_audit_reader_select') THEN
      EXECUTE format('CREATE POLICY history_audit_reader_select ON %s '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', t, audit_role);
    END IF;

    -- history warmer: SELECT / INSERT / UPDATE policies (no DELETE)
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_select') THEN
      EXECUTE format('CREATE POLICY history_warmer_select ON %s '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', t, warmer_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_insert') THEN
      EXECUTE format('CREATE POLICY history_warmer_insert ON %s '
                     'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', t, warmer_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_update') THEN
      EXECUTE format('CREATE POLICY history_warmer_update ON %s '
                     'AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)', t, warmer_role);
    END IF;
    RAISE NOTICE 'RLS policies ensured on %', t;
  END LOOP;

  -- daily_bars: the warmer already HOLDS the SELECT/INSERT/UPDATE grant (see
  -- create_shadow_history_warmer.sql), but the live database enables RLS on
  -- daily_bars (it is one of the eight audit relations — see
  -- create_shadow_audit_rls_policies.sql). With RLS on and NO warmer policy the
  -- grant is inert: the warmer both reads ZERO daily rows (breaking readiness
  -- run as the warmer) and is refused INSERT/UPDATE ("new row violates
  -- row-level security policy"). Add the same SELECT/INSERT/UPDATE policies as
  -- the 4H tables (NO DELETE) so the already-granted daily operations work under
  -- RLS. Guarded: only when RLS is enabled on daily_bars (when it is off the
  -- grant governs and no inert policy is created). Never grants a new privilege.
  rel_oid := to_regclass('public.daily_bars');
  IF rel_oid IS NOT NULL AND (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_daily_select') THEN
      EXECUTE format('CREATE POLICY history_warmer_daily_select ON public.daily_bars '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', warmer_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_daily_insert') THEN
      EXECUTE format('CREATE POLICY history_warmer_daily_insert ON public.daily_bars '
                     'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', warmer_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'history_warmer_daily_update') THEN
      EXECUTE format('CREATE POLICY history_warmer_daily_update ON public.daily_bars '
                     'AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)', warmer_role);
    END IF;
    RAISE NOTICE 'warmer daily_bars RLS policies ensured (RLS on)';
  ELSE
    RAISE NOTICE 'daily_bars RLS off (or absent) — warmer grant governs; no policy created';
  END IF;

  -- Frozen universes (migration 016). universes: warmer SELECT/INSERT/UPDATE
  -- (create + freeze); universe_symbols: warmer SELECT/INSERT ONLY (draft
  -- membership) — NO UPDATE/DELETE policy (post-freeze immutability is also
  -- enforced by the DB trigger). Audit reader: SELECT on both. No DELETE for
  -- anyone.
  rel_oid := to_regclass('public.history_warmup_universes');
  IF rel_oid IS NOT NULL THEN
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      EXECUTE 'ALTER TABLE public.history_warmup_universes ENABLE ROW LEVEL SECURITY';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = audit_role) THEN
      EXECUTE format('GRANT SELECT ON public.history_warmup_universes TO %I', audit_role);
      IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universes_audit_select') THEN
        EXECUTE format('CREATE POLICY history_universes_audit_select ON public.history_warmup_universes '
                       'AS PERMISSIVE FOR SELECT TO %I USING (true)', audit_role);
      END IF;
    END IF;
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON public.history_warmup_universes TO %I', warmer_role);
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universes_warmer_select') THEN
      EXECUTE format('CREATE POLICY history_universes_warmer_select ON public.history_warmup_universes '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', warmer_role); END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universes_warmer_insert') THEN
      EXECUTE format('CREATE POLICY history_universes_warmer_insert ON public.history_warmup_universes '
                     'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', warmer_role); END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universes_warmer_update') THEN
      EXECUTE format('CREATE POLICY history_universes_warmer_update ON public.history_warmup_universes '
                     'AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)', warmer_role); END IF;
    RAISE NOTICE 'warmer/audit universe policies ensured';
  END IF;

  rel_oid := to_regclass('public.history_warmup_universe_symbols');
  IF rel_oid IS NOT NULL THEN
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      EXECUTE 'ALTER TABLE public.history_warmup_universe_symbols ENABLE ROW LEVEL SECURITY';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = audit_role) THEN
      EXECUTE format('GRANT SELECT ON public.history_warmup_universe_symbols TO %I', audit_role);
      IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universe_symbols_audit_select') THEN
        EXECUTE format('CREATE POLICY history_universe_symbols_audit_select ON public.history_warmup_universe_symbols '
                       'AS PERMISSIVE FOR SELECT TO %I USING (true)', audit_role);
      END IF;
    END IF;
    EXECUTE format('GRANT SELECT, INSERT ON public.history_warmup_universe_symbols TO %I', warmer_role);
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universe_symbols_warmer_select') THEN
      EXECUTE format('CREATE POLICY history_universe_symbols_warmer_select ON public.history_warmup_universe_symbols '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', warmer_role); END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid=rel_oid AND polname='history_universe_symbols_warmer_insert') THEN
      EXECUTE format('CREATE POLICY history_universe_symbols_warmer_insert ON public.history_warmup_universe_symbols '
                     'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', warmer_role); END IF;
    RAISE NOTICE 'warmer/audit universe_symbols policies ensured (no update/delete)';
  END IF;
END
$$;
