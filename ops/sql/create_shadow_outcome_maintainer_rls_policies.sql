-- =============================================================================
-- Narrow RLS policies for smart_scanner_outcome_maintainer
-- =============================================================================
-- Read relations (8): one PERMISSIVE full-row SELECT policy each (the plan +
-- calc selection read the whole cohort). Writable outcome relation
-- (strategy_shadow_pair_outcomes): a full-row SELECT policy PLUS the NARROWEST
-- INSERT and UPDATE policies — outcome writes are restricted to pairs that both
-- belong to experiment 'wyckoff_v2_vs_baseline' AND are campaign-linked (a
-- persisted run_pairs -> runs.telemetry.campaign relationship). Membership is
-- NEVER inferred from symbol or date. The run bookkeeping relation
-- (strategy_shadow_outcome_runs) gets unconditional maintainer policies ONLY
-- when RLS is enabled on it; otherwise the grant governs.
--
-- Run as the table owner (Supabase `postgres`). NEVER enables/disables/forces
-- RLS, never grants BYPASSRLS, never changes ownership, never touches the audit
-- reader's policies or any unrelated policy. Rerunnable: matching intended
-- policy -> NOTICE; same-named policy with a different command/permissive/roles
-- -> RAISE (never silently replaced). Missing relation / RLS-off on a required
-- relation -> RAISE.
-- =============================================================================

\set ON_ERROR_STOP on

DO $$
DECLARE
  t           text;
  rel_oid     regclass;
  rec         record;
  maint_role  constant text := 'smart_scanner_outcome_maintainer';
  read_rels   constant text[] := ARRAY[
    'public.strategy_shadow_evaluations',
    'public.strategy_shadow_pairs',
    'public.strategy_shadow_pair_outcomes',
    'public.strategy_shadow_run_pairs',
    'public.strategy_shadow_runs',
    'public.daily_bars',
    'public.patterns',
    'public.pattern_configs'
  ];
  outcome_rel constant text := 'public.strategy_shadow_pair_outcomes';
  runs_rel    constant text := 'public.strategy_shadow_outcome_runs';
  -- Campaign-scope predicate for the outcome row (parameterized by pair_id).
  scope_pred  constant text :=
    'EXISTS (SELECT 1 FROM public.strategy_shadow_pairs p '
    'WHERE p.id = public.strategy_shadow_pair_outcomes.pair_id '
    'AND p.experiment_code = ''wyckoff_v2_vs_baseline'' '
    'AND EXISTS (SELECT 1 FROM public.strategy_shadow_run_pairs rp '
    'JOIN public.strategy_shadow_runs r ON r.id = rp.run_id '
    'WHERE rp.pair_id = p.id '
    'AND (r.telemetry -> ''campaign'' ->> ''campaign_id'') IS NOT NULL))';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = maint_role) THEN
    RAISE EXCEPTION 'role % does not exist (run create_shadow_outcome_maintainer.sql first)', maint_role;
  END IF;

  -- ---- read SELECT policies (all 8; RLS must be ON) ---------------------- --
  FOREACH t IN ARRAY read_rels LOOP
    rel_oid := to_regclass(t);
    IF rel_oid IS NULL THEN
      RAISE EXCEPTION 'required relation % is missing', t;
    END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
      RAISE EXCEPTION 'RLS is not enabled on % — refusing to create an inert policy', t;
    END IF;
    SELECT p.polpermissive AS permissive, p.polcmd AS cmd,
           (SELECT array_agg(r.rolname ORDER BY r.rolname)
              FROM pg_roles r WHERE r.oid = ANY(p.polroles)) AS roles,
           regexp_replace(coalesce(pg_get_expr(p.polqual, p.polrelid),''),
                          '[[:space:]()]','','g') AS qual_norm
      INTO rec
    FROM pg_policy p
    WHERE p.polname = 'smart_scanner_outcome_maintainer_select' AND p.polrelid = rel_oid;
    IF NOT FOUND THEN
      EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_select ON %s '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', t, maint_role);
      RAISE NOTICE 'created SELECT policy on %', t;
    ELSIF rec.permissive IS DISTINCT FROM true OR rec.cmd <> 'r'
       OR rec.roles IS DISTINCT FROM ARRAY[maint_role]::name[]
       OR rec.qual_norm <> 'true' THEN
      RAISE EXCEPTION 'SELECT policy on % exists with a DIFFERENT definition', t;
    ELSE
      RAISE NOTICE 'SELECT policy on % already matches', t;
    END IF;
  END LOOP;

  -- ---- outcome INSERT policy (campaign-scoped WITH CHECK) ---------------- --
  rel_oid := to_regclass(outcome_rel);
  SELECT p.polpermissive AS permissive, p.polcmd AS cmd,
         (SELECT array_agg(r.rolname ORDER BY r.rolname)
            FROM pg_roles r WHERE r.oid = ANY(p.polroles)) AS roles,
         (p.polwithcheck IS NOT NULL) AS has_check
    INTO rec
  FROM pg_policy p
  WHERE p.polname = 'smart_scanner_outcome_maintainer_ins' AND p.polrelid = rel_oid;
  IF NOT FOUND THEN
    EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_ins ON %s '
                   'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (%s)',
                   outcome_rel, maint_role, scope_pred);
    RAISE NOTICE 'created INSERT policy on %', outcome_rel;
  ELSIF rec.permissive IS DISTINCT FROM true OR rec.cmd <> 'a'
     OR rec.roles IS DISTINCT FROM ARRAY[maint_role]::name[] OR NOT rec.has_check THEN
    RAISE EXCEPTION 'INSERT policy on % exists with a DIFFERENT definition', outcome_rel;
  ELSE
    RAISE NOTICE 'INSERT policy on % already matches', outcome_rel;
  END IF;

  -- ---- outcome UPDATE policy (campaign-scoped USING + WITH CHECK) -------- --
  SELECT p.polpermissive AS permissive, p.polcmd AS cmd,
         (SELECT array_agg(r.rolname ORDER BY r.rolname)
            FROM pg_roles r WHERE r.oid = ANY(p.polroles)) AS roles,
         (p.polqual IS NOT NULL AND p.polwithcheck IS NOT NULL) AS has_both
    INTO rec
  FROM pg_policy p
  WHERE p.polname = 'smart_scanner_outcome_maintainer_upd' AND p.polrelid = rel_oid;
  IF NOT FOUND THEN
    EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_upd ON %s '
                   'AS PERMISSIVE FOR UPDATE TO %I USING (%s) WITH CHECK (%s)',
                   outcome_rel, maint_role, scope_pred, scope_pred);
    RAISE NOTICE 'created UPDATE policy on %', outcome_rel;
  ELSIF rec.permissive IS DISTINCT FROM true OR rec.cmd <> 'w'
     OR rec.roles IS DISTINCT FROM ARRAY[maint_role]::name[] OR NOT rec.has_both THEN
    RAISE EXCEPTION 'UPDATE policy on % exists with a DIFFERENT definition', outcome_rel;
  ELSE
    RAISE NOTICE 'UPDATE policy on % already matches', outcome_rel;
  END IF;

  -- ---- run bookkeeping relation: policies ONLY when RLS is enabled ------- --
  rel_oid := to_regclass(runs_rel);
  IF rel_oid IS NULL THEN
    RAISE EXCEPTION 'required relation % is missing', runs_rel;
  END IF;
  IF (SELECT relrowsecurity FROM pg_class WHERE oid = rel_oid) THEN
    -- unconditional SELECT/INSERT/UPDATE maintainer policies (run rows are not
    -- pair-scoped; the grant + these policies govern them).
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'smart_scanner_outcome_maintainer_runs_select') THEN
      EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_runs_select ON %s '
                     'AS PERMISSIVE FOR SELECT TO %I USING (true)', runs_rel, maint_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'smart_scanner_outcome_maintainer_runs_ins') THEN
      EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_runs_ins ON %s '
                     'AS PERMISSIVE FOR INSERT TO %I WITH CHECK (true)', runs_rel, maint_role);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = rel_oid
                   AND p.polname = 'smart_scanner_outcome_maintainer_runs_upd') THEN
      EXECUTE format('CREATE POLICY smart_scanner_outcome_maintainer_runs_upd ON %s '
                     'AS PERMISSIVE FOR UPDATE TO %I USING (true) WITH CHECK (true)', runs_rel, maint_role);
    END IF;
    RAISE NOTICE 'run-table maintainer policies ensured on %', runs_rel;
  ELSE
    RAISE NOTICE 'RLS off on % — grant governs, no policy needed', runs_rel;
  END IF;
END
$$;
