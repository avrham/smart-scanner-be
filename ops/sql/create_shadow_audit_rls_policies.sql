-- =============================================================================
-- Narrow read-only RLS SELECT policies for smart_scanner_audit_reader
-- =============================================================================
-- The live Smart Scanner database has Row Level Security ENABLED on the eight
-- audit relations (the repo migrations do not). Table SELECT grants and RLS
-- policies are SEPARATE enforcement layers: with RLS on and no applicable
-- policy, the non-owner audit role reads ZERO rows despite the SELECT grant.
--
-- This script creates exactly one PERMISSIVE, SELECT-only, full-row policy per
-- relation, targeting ONLY smart_scanner_audit_reader. It NEVER enables or
-- disables RLS, never grants BYPASSRLS, never changes ownership, never adds
-- INSERT/UPDATE/DELETE/ALL/WITH CHECK policies, and never touches unrelated
-- policies. Run it as the table owner (Supabase `postgres`).
--
--   psql "<owner admin conn>" -f ops/sql/create_shadow_audit_rls_policies.sql
--
-- Rerunnable: succeeds when the exact intended policy already exists; FAILS
-- (non-zero) when a policy of the intended name exists with a different
-- command / role / permissive mode / USING expression — it never silently
-- drops-and-recreates a mismatched policy.
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
  t          text;
  rel_oid    regclass;
  rec        record;
  target_pol constant text := 'smart_scanner_audit_reader_select';
  audit_role constant text := 'smart_scanner_audit_reader';
  rels       constant text[] := ARRAY[
    'public.strategy_shadow_evaluations',
    'public.strategy_shadow_pairs',
    'public.strategy_shadow_pair_outcomes',
    'public.strategy_shadow_run_pairs',
    'public.strategy_shadow_runs',
    'public.daily_bars',
    'public.patterns',
    'public.pattern_configs'
  ];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = audit_role) THEN
    RAISE EXCEPTION 'role % does not exist (run create_shadow_audit_reader.sql first)',
      audit_role;
  END IF;

  FOREACH t IN ARRAY rels LOOP
    rel_oid := to_regclass(t);
    IF rel_oid IS NULL THEN
      RAISE EXCEPTION 'required relation % is missing', t;
    END IF;

    -- Do NOT enable/disable RLS here. If it is unexpectedly OFF, fail rather
    -- than create an inert policy that would give a false sense of safety.
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      RAISE EXCEPTION 'RLS is not enabled on % — refusing to create an inert policy', t;
    END IF;

    SELECT p.polpermissive AS permissive,
           p.polcmd        AS cmd,   -- 'r' = SELECT
           regexp_replace(
             coalesce(pg_get_expr(p.polqual, p.polrelid), ''),
             '[[:space:]()]', '', 'g') AS qual_norm,
           (SELECT array_agg(r.rolname ORDER BY r.rolname)
              FROM pg_roles r WHERE r.oid = ANY(p.polroles)) AS roles
      INTO rec
    FROM pg_policy p
    WHERE p.polname = target_pol AND p.polrelid = rel_oid;

    IF NOT FOUND THEN
      EXECUTE format(
        'CREATE POLICY %I ON %I.%I AS PERMISSIVE FOR SELECT TO %I USING (true)',
        target_pol, split_part(t, '.', 1), split_part(t, '.', 2), audit_role);
      RAISE NOTICE 'created policy % on %', target_pol, t;
    ELSIF rec.permissive IS DISTINCT FROM true
       OR rec.cmd <> 'r'
       OR rec.qual_norm <> 'true'
       OR rec.roles IS DISTINCT FROM ARRAY[audit_role]::name[] THEN
      RAISE EXCEPTION
        'policy % on % exists with a DIFFERENT definition (cmd=%, permissive=%, roles=%, using_norm=%) — not modifying it',
        target_pol, t, rec.cmd, rec.permissive, rec.roles, rec.qual_norm;
    ELSE
      RAISE NOTICE 'policy % on % already matches intended definition', target_pol, t;
    END IF;
  END LOOP;
END
$$;
