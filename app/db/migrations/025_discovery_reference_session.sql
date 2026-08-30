-- ===========================================================================
-- 025 — Discovery temporal provenance: which session do the numbers describe?
-- ===========================================================================
-- A corrective migration following the Wave 2 temporal audit. It adds no
-- source, no capability and no product surface. It records one fact the rows
-- were silently missing.
--
-- WHAT WAS WRONG
-- --------------
-- `external_discovery_candidates` stored two clocks:
--
--     observed_at    when WE fetched          (truthful, unchanged)
--     session_date   the first session that could ACT on the fetch
--
-- and nothing else. `session_date` is forward-rolling by construction — a
-- Sunday fetch is actionable on Monday — so whenever the fetch happened
-- outside a live session, the row was labelled with a session that had not
-- happened yet, while the numbers in it described a session that had.
--
-- MEASURED, from the two snapshots this database already held:
--
--   session_date  observed_at (ET)        NVDA price/change   really describes
--   2026-08-28    Fri 06:24  (pre-open)   227.98 / +8.74%     Thu 2026-08-27
--   2026-08-31    Sun 10:34  (shut)       217.55 / -4.57%     Fri 2026-08-28
--
-- 18 overlapping (symbol, list_kind) pairs across the two snapshots, ZERO with
-- identical numbers — so these are genuinely two different tapes, and both
-- were labelled one session ahead of the tape they carry.
--
-- WHY THE COLUMN IS NOT CALLED `describes_session`
-- ------------------------------------------------
-- Because we would be asserting something the provider never told us. FMP's
-- movers feeds carry NO timestamp — measured, and already recorded in the
-- header of migration 023. The session is our INFERENCE from our own fetch
-- clock, so the column is named `reference_session_date` and travels with
-- `reference_session_basis`, whose only value today is literally
-- `inferred_from_observation_time`. A future feed that does declare a session
-- gets a new basis value and the same column; nothing has to move.
--
-- THREE DATES, THREE QUESTIONS. They must never collapse again:
--
--     observed_at             evidence provenance  — when we looked
--     reference_session_date  what the numbers are — which tape they are from
--     session_date            actionability        — the conservative anchor
--
-- THE IDENTITY IS WIDENED, NOT CHANGED
-- ------------------------------------
-- The unique key gains `reference_session_date`. Adding a column to a unique
-- key can only ever PERMIT more rows; it can never merge rows that were
-- previously distinct, so no existing data can be lost by this. It also
-- restores what migration 023's own comment always claimed the key meant —
-- "a second fetch on the same session updates the rank" — which was false the
-- moment a pre-open fetch (describing Thursday) and an in-session fetch
-- (describing Friday) landed on one actionable session and the second
-- overwrote the first.
--
-- THE BACKFILL IS NOT IN THIS FILE, AND THAT IS DELIBERATE
-- --------------------------------------------------------
-- The inference needs the US equity trading calendar — weekends, the nine
-- federal market holidays, Good Friday, and the observed-day rules. That
-- calendar exists exactly once in this repository, in
-- `app/prospective_session.py`, and re-implementing it in SQL would create a
-- second one that could drift from the first without anybody noticing.
--
-- So the column is added NULLABLE here and populated by
--
--     python -m ops.analysis.refresh_discovery_candidates \
--         --backfill-reference-sessions [--dry-run]
--
-- which reads the same calendar the ingestion writes with. `--dry-run` prints
-- the proposed mapping for every affected row and writes nothing. Rows written
-- from this migration forward always carry the column.
-- ===========================================================================

ALTER TABLE public.external_discovery_candidates
  ADD COLUMN IF NOT EXISTS reference_session_date DATE;

ALTER TABLE public.external_discovery_candidates
  ADD COLUMN IF NOT EXISTS reference_session_basis TEXT;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'external_discovery_reference_basis_ck') THEN
    ALTER TABLE public.external_discovery_candidates
      ADD CONSTRAINT external_discovery_reference_basis_ck
      -- Bounded, and honest about being an inference. A value may be added
      -- when a feed genuinely declares its session; none may be added that
      -- implies one did when it did not.
      CHECK (reference_session_basis IS NULL
             OR reference_session_basis IN ('inferred_from_observation_time'));
  END IF;

  -- The basis is meaningless without the date and vice versa: both or neither.
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'external_discovery_reference_pair_ck') THEN
    ALTER TABLE public.external_discovery_candidates
      ADD CONSTRAINT external_discovery_reference_pair_ck
      CHECK ((reference_session_date IS NULL)
             = (reference_session_basis IS NULL));
  END IF;

  -- The inference can never point at a session AFTER the one the observation
  -- first became actionable in. If it ever did, the row would be claiming the
  -- numbers came from a session nobody could have seen yet — the exact defect
  -- this migration exists to make impossible.
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'external_discovery_reference_order_ck') THEN
    ALTER TABLE public.external_discovery_candidates
      ADD CONSTRAINT external_discovery_reference_order_ck
      CHECK (reference_session_date IS NULL
             OR reference_session_date <= session_date);
  END IF;
END
$$;

-- Widen the identity. The old constraint is dropped only AFTER the new unique
-- index exists, so the table is never briefly unprotected.
CREATE UNIQUE INDEX IF NOT EXISTS external_discovery_identity_v2
  ON public.external_discovery_candidates
     (source, list_kind, symbol, session_date, reference_session_date);

ALTER TABLE public.external_discovery_candidates
  DROP CONSTRAINT IF EXISTS external_discovery_identity_unique;

CREATE INDEX IF NOT EXISTS external_discovery_reference_idx
  ON public.external_discovery_candidates
     (reference_session_date DESC, list_kind, rank);

-- ---------------------------------------------------------------------------
-- The A3 aggregation now answers the question it was always meant to answer.
--
-- "Which symbols did the market notice in one SESSION" is a question about the
-- tape, so the view groups by `reference_session_date`. The actionable session
-- is carried alongside as `first_actionable_session` rather than dropped —
-- a reader deciding what to investigate needs to know both which session the
-- move happened in and the earliest session they could have done anything
-- about it.
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS public.external_discovery_current;

CREATE VIEW public.external_discovery_current AS
SELECT
  reference_session_date,
  symbol,
  max(company_name)                                    AS company_name,
  max(exchange)                                        AS exchange,
  bool_or(in_scanner_universe)                         AS in_scanner_universe,
  array_agg(DISTINCT list_kind ORDER BY list_kind)     AS reasons,
  count(DISTINCT list_kind)                            AS reason_count,
  min(rank)                                            AS best_rank,
  max(abs(COALESCE(change_percent, 0)))                AS max_abs_change_percent,
  max(observed_at)                                     AS observed_at,
  min(session_date)                                    AS first_actionable_session,
  max(reference_session_basis)                         AS reference_session_basis,
  max(licensing_visibility)                            AS licensing_visibility
FROM public.external_discovery_candidates
WHERE reference_session_date IS NOT NULL
GROUP BY reference_session_date, symbol;


-- ---------------------------------------------------------------------------
-- Restore the view's grants.
--
-- DROP VIEW takes every grant on it with it, so a migration that recreates a
-- view and stops there leaves the ingestion role newly unable to read it —
-- silently, until the next run fails. Re-granting here keeps the migration
-- self-contained; ops/sql/create_smart_scanner_market_intel.sql grants the
-- same thing and both are idempotent.
--
-- Guarded on the role existing, because a fresh database applies migrations
-- before any least-privilege role has been created.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    GRANT SELECT ON public.external_discovery_current
      TO smart_scanner_market_intel;
  END IF;
END
$$;
