-- ===========================================================================
-- 028 — `catalyst_source_state` gets a COHORT, because it always had one
-- ===========================================================================
-- The table has held one row per source since 019, and every reader has
-- interpreted that row as "how fresh is this source FOR THE FROZEN 25". That
-- interpretation was never written down anywhere, which is exactly why it held
-- for six migrations and then failed silently the moment a second cohort
-- appeared.
--
-- WHAT ACTUALLY WENT WRONG
-- ------------------------
-- The research lifecycle reached its enrichment stage with one survivor and
-- called `refresh_sec_filings`. Had it succeeded it would have written
--
--     sec_edgar | ok | last_success_at = now() | symbols_covered = 1
--
-- into the row `GET /api/scanner/overview` reads to decide whether to show the
-- SEC dimension at all. The product would then have reported its 8-K coverage
-- as freshly refreshed on the strength of a refresh for a symbol the product
-- cannot see. Row-level security refused the write, and that refusal is the
-- only reason this is a design note rather than an incident.
--
-- WHY A COLUMN AND NOT A NAME
-- ---------------------------
-- `sec_edgar:research` was considered and rejected. `source` is a shared
-- vocabulary: `external_signal_sources` references the same names,
-- `source_state_key()` already prefixes them, and LIVE RLS predicates match
-- `source LIKE 'external\_%'`. Encoding a second dimension inside that string
-- makes every one of those rules wrong in a way that still parses. Scope is a
-- different question from identity, so it gets its own column and joins the
-- primary key. See app/source_scope.py for the same position stated in code.
--
-- WHY THIS IS SAFE FOR EXISTING DATA
-- ----------------------------------
-- The column defaults to 'product', so every existing row keeps meaning
-- exactly what it meant, and every existing WRITER stays correct without being
-- changed: a caller that says nothing about scope writes the product row,
-- which is what it was already doing. The only behavioural change for the
-- product is that its reads now say `WHERE scope = 'product'` out loud.
--
-- WHAT THIS MIGRATION DOES NOT DO
-- -------------------------------
-- It does not grant anything to the research role and does not enable any
-- enrichment. Those live in 029 and in ops/sql, so this migration can be
-- applied and reasoned about on its own.
-- ===========================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1) The column. NOT NULL with a default, so the backfill is the default.
-- ---------------------------------------------------------------------------
ALTER TABLE public.catalyst_source_state
  ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'product';

-- Constrained to the vocabulary. An unrecognised scope must fail at the write,
-- not become a fourth meaning discovered by a reader six months later.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.catalyst_source_state'::regclass
      AND conname = 'catalyst_source_state_scope_ck'
  ) THEN
    ALTER TABLE public.catalyst_source_state
      ADD CONSTRAINT catalyst_source_state_scope_ck
      CHECK (scope IN ('product', 'research'));
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- 2) Identity becomes (source, scope).
--
-- Done as a guarded swap rather than a blind DROP/ADD so re-running is safe and
-- so a database that already carries the composite key is left alone. The old
-- single-column key is dropped only AFTER the composite one exists, so the
-- table is never momentarily without a unique identity for `source`.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  has_composite boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint c
    WHERE c.conrelid = 'public.catalyst_source_state'::regclass
      AND c.contype = 'p'
      AND (SELECT count(*) FROM unnest(c.conkey)) = 2
  ) INTO has_composite;

  IF has_composite THEN
    RAISE NOTICE '028: catalyst_source_state already keyed on (source, scope).';
    RETURN;
  END IF;

  -- A unique index first; promoting it to the primary key is then atomic and
  -- cannot leave the table unkeyed if the statement is interrupted.
  CREATE UNIQUE INDEX IF NOT EXISTS catalyst_source_state_source_scope_uq
    ON public.catalyst_source_state (source, scope);

  ALTER TABLE public.catalyst_source_state
    DROP CONSTRAINT IF EXISTS catalyst_source_state_pkey;

  ALTER TABLE public.catalyst_source_state
    ADD CONSTRAINT catalyst_source_state_pkey
    PRIMARY KEY USING INDEX catalyst_source_state_source_scope_uq;

  RAISE NOTICE '028: catalyst_source_state re-keyed on (source, scope).';
END
$$;

-- ---------------------------------------------------------------------------
-- 3) Existing rows are the product cohort. Explicit rather than implied by the
--    default, so a row inserted by some path that predates the default is also
--    corrected. Idempotent and a no-op on a fresh apply.
-- ---------------------------------------------------------------------------
UPDATE public.catalyst_source_state
SET scope = 'product'
WHERE scope IS NULL OR scope = '';

-- ---------------------------------------------------------------------------
-- 4) The product reader's read boundary, tightened in place.
--
-- The role already holds SELECT on this relation with a `USING (true)` policy.
-- Now that a second cohort can exist, `true` is too much: it would let a
-- research freshness row reach a product response through nothing worse than a
-- forgotten WHERE clause. The policy is DROPped and recreated (convergent, not
-- create-if-missing) so a live database carrying the old predicate is actually
-- upgraded. No other role's policy is touched and RLS is never disabled.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles
                 WHERE rolname = 'smart_scanner_product_reader') THEN
    RAISE NOTICE '028: product reader role absent; skipping policy tightening.';
    RETURN;
  END IF;
  DROP POLICY IF EXISTS smart_scanner_product_reader_select
    ON public.catalyst_source_state;
  CREATE POLICY smart_scanner_product_reader_select
    ON public.catalyst_source_state
    FOR SELECT TO smart_scanner_product_reader
    USING (scope = 'product');
  RAISE NOTICE '028: product reader confined to scope = product.';
END
$$;

-- ---------------------------------------------------------------------------
-- 5) Read path.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS catalyst_source_state_scope_idx
  ON public.catalyst_source_state (scope);
