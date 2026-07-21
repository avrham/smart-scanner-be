-- Phase 8.1A (Evidence Engine): outcome coverage for persisted WATCH signals.
--
-- WHAT THIS ADDS
--   signal_outcomes gains three additive columns so every outcome row states
--   WHICH verdict it measures and WHAT its reference price means:
--
--     signal_verdict            'ENTER' | 'WATCH' (copied from signals.verdict,
--                               never inferred from strategy name/score/return)
--     reference_price_role      'entry_reference'      (ENTER)
--                               'candidate_observation' (WATCH)
--     outcome_coverage_version  'candidate_outcomes.v1'
--
-- PRODUCT SEMANTICS (candidate_outcomes.v1)
--   A WATCH outcome is NOT a simulated trade entry. Its reference price is
--   "the market price when the candidate was observed" — no entry is invented
--   after the WATCH date and the reference date never moves forward to a later
--   trigger. WATCH outcomes measure what happened after the observation so we
--   can evaluate whether waiting (a failed entry confirmation) added value.
--
-- SAFETY / SCOPE
--   * Additive and idempotent: safe to run more than once.
--   * Existing outcome IDs, return values and calculation_version unchanged.
--   * No signals rows modified. No signal_provenance rows modified.
--   * No outcome rows are created here (missing WATCH outcomes are calculated
--     only by the outcome worker, never fabricated by a migration).
--   * The UNIQUE (signal_id) constraint is preserved: one outcome row per
--     immutable signal, all holding windows stored on that row.
--   * Columns stay nullable (no NOT NULL is introduced): a NULL
--     signal_verdict can only mean a legacy row created before this
--     migration, and all such rows were selected as ENTER-only by the
--     Phase 2 worker.
--
-- Run manually in Supabase AFTER 008_sma150_v3.sql. Do not create 010 for this.

-- 1. Additive columns (no defaults on existing rows until the backfill below).
ALTER TABLE public.signal_outcomes
  ADD COLUMN IF NOT EXISTS signal_verdict TEXT;

ALTER TABLE public.signal_outcomes
  ADD COLUMN IF NOT EXISTS reference_price_role TEXT;

ALTER TABLE public.signal_outcomes
  ADD COLUMN IF NOT EXISTS outcome_coverage_version TEXT;

-- 2. Backfill existing rows from their linked signal (join on signal_id).
--    The verdict comes ONLY from signals.verdict. Idempotent: rows already
--    stamped (signal_verdict IS NOT NULL) are never rewritten, so re-running
--    cannot change values the worker has since written.
UPDATE public.signal_outcomes o
SET
  signal_verdict = s.verdict,
  reference_price_role = CASE s.verdict
    WHEN 'ENTER' THEN 'entry_reference'
    WHEN 'WATCH' THEN 'candidate_observation'
    ELSE NULL
  END,
  outcome_coverage_version = 'candidate_outcomes.v1'
FROM public.signals s
WHERE s.id = o.signal_id
  AND o.signal_verdict IS NULL;

-- 3. Bounded index for verdict/version filtering (metrics + API queries).
CREATE INDEX IF NOT EXISTS signal_outcomes_verdict_idx
  ON public.signal_outcomes (signal_verdict, outcome_coverage_version);
