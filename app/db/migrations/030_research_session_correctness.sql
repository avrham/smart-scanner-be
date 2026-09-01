-- 030_research_session_correctness.sql
--
-- ONE constraint widening, and nothing else.
--
-- WHY A MIGRATION IS REQUIRED AT ALL
-- ----------------------------------
-- The P0 correctness work is otherwise pure application logic: session-scoped
-- candidacy is derived from `research_scan_results.scan_session`, which is
-- already half that table's primary key, and calendar parking reuses
-- `research_symbols.warmup_cooldown_until`, which already exists and is
-- already honoured by warmup selection. No new column was added for either,
-- deliberately: provenance we can already express should not become schema.
--
-- What CANNOT be expressed without a change is the new lifecycle state.
-- `research_lifecycle_run_symbols.lifecycle_state` is guarded by a CHECK that
-- enumerates the permitted values, so `scan_stale` — "this symbol is ready and
-- we hold a scan, but for an earlier session, not this run's" — would be
-- rejected at INSERT time.
--
-- WHY `scan_stale` HAD TO BE A STATE AND NOT A FLAG
-- -------------------------------------------------
-- On 2026-08-31 the research run reported two candidates. One of them, ONDS,
-- had last been evaluated on 2026-08-28 and was not scanned by that run at
-- all: it lost the scan's `LIMIT 5` ordering and simply carried its old
-- classification forward, because the funnel read the session-less
-- `research_symbols.candidate_state` column. Folding that case into
-- `scan_pending` would lose the fact that we hold evidence; folding it into a
-- scanned state is the defect itself. It needs its own name.
--
-- BACKWARD COMPATIBILITY
-- ----------------------
-- This is a pure WIDENING. Every value the old constraint permitted is still
-- permitted, so historical rows — including the 60 written by run
-- 43dd5723-6143-4b2c-9067-befebca3418a, which are audit evidence for this very
-- defect and must not be rewritten — remain valid and are left untouched.
-- Re-running the migration is safe.

BEGIN;

ALTER TABLE public.research_lifecycle_run_symbols
  DROP CONSTRAINT IF EXISTS research_lifecycle_run_symbols_state_ck;

ALTER TABLE public.research_lifecycle_run_symbols
  ADD CONSTRAINT research_lifecycle_run_symbols_state_ck
  CHECK (lifecycle_state IN (
    'admission_pending',
    'admission_rejected',
    'history_pending',
    'history_warming',
    'history_unavailable',
    'history_failed',
    'scan_pending',
    -- NEW: ready, evidence exists, but not for this run's target session.
    'scan_stale',
    'classification_pending',
    'scanned_not_candidate',
    'research_candidate'
  ));

COMMIT;
