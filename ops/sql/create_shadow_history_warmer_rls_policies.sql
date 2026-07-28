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
  rels        constant text[] := ARRAY['public.market_bars_4h', 'public.history_warmup_runs'];
  t           text;
  rel_oid     regclass;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = warmer_role) THEN
    RAISE EXCEPTION 'role % does not exist (run create_shadow_history_warmer.sql first)', warmer_role;
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
END
$$;
