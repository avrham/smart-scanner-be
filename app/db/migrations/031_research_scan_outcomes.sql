-- ===========================================================================
-- 031 — What actually happened next
-- ===========================================================================
-- Migration 029 said this out loud and meant it:
--
--     NOT AN OUTCOME LEDGER, EITHER
--     No forward return, no realised P&L, no label. A research candidate is a
--     symbol that survived a screen; whether that meant anything is a question
--     this milestone deliberately does not let itself answer, because the
--     honest sample size is one.
--
-- The sample size is no longer one. There are 24 scans across 5 sessions and
-- 15 symbols, the lifecycle adds more every session, and every session that
-- passes without a measurement is a prediction that can never be scored — the
-- forward bars arrive whether or not anybody wrote down what to compare them
-- to. So this is the ledger, and it is added now rather than later for the one
-- reason that matters: the cost of NOT having it is not zero, it is one lost
-- observation per scan per session, permanently.
--
-- WHAT AN OUTCOME IS HERE
-- -----------------------
-- A MARKET-PATH OBSERVATION of one scan over one horizon. Not a trade. There
-- is no side (research is long-only-by-absence: it cannot produce ENTER at
-- all — migration 026's CHECK refuses it), no stop, no target, no simulated R,
-- and no position size. The reference is the close of the scan's own session:
-- the last bar the scan could see, which is `research_scan.py`'s lookahead
-- barrier restated as a price.
--
-- ONE ROW PER (SCAN, HORIZON), AND WHY NOT FIVE COLUMNS
-- -----------------------------------------------------
-- `strategy_shadow_pair_outcomes` (migration 011) carries ret_1d..ret_20d as
-- five columns on one row per pair. That is right for a frozen cohort measured
-- in one campaign-wide sweep, and wrong here.
--
-- Research horizons mature INDEPENDENTLY and at different times, because a
-- research symbol's forward bars arrive only when the bounded freshness top-up
-- gets round to it (five symbols a run). A single row would have to represent
-- "3D measured, 5D still waiting for a bar, 10D not yet eligible" in one
-- `status` column, and it cannot: that is three different answers about three
-- different questions. The brief's O13 asks for exactly those three answers per
-- scan per horizon, without reading a log. So the horizon is part of the key.
--
-- The cost is bounded and small: five rows per scan, at most a handful of scans
-- a session — tens of rows a week, not a warehouse.
--
-- ATTRIBUTION IS SNAPSHOT, NOT JOINED, AND THAT IS DELIBERATE
-- -----------------------------------------------------------
-- The general rule in this project is to link rather than duplicate. It does
-- not apply here, and the reason is in `research_scan.py`:
--
--     ON CONFLICT (symbol, scan_session) DO UPDATE SET
--         verdict = EXCLUDED.verdict, ... setup_state = EXCLUDED.setup_state,
--
-- A re-scan of the same symbol for the same session REWRITES the verdict,
-- the setup state, the rejection reason and the benchmark reading in place.
-- The row's id survives, so a foreign key stays valid while everything it
-- pointed AT changes meaning. An outcome that read its own candidacy through
-- that join would silently become an outcome about a different classification
-- than the one that was actually made when the horizon started running — the
-- brief's O2 in its precise form.
--
-- So the classification, the strategy identity, the config hash and the
-- strategy's own words are COPIED onto the outcome at the moment it is
-- planned, `scan_scanned_at` is copied beside them as the tell, and the
-- foreign key is kept for navigation rather than for meaning.
--
-- IMMUTABILITY IS A TRIGGER, NOT A CONVENTION  (the brief's O8)
-- -------------------------------------------------------------
-- `daily_bars` is upserted. A provider correction to a close we have already
-- measured against is a real possibility, and there are only two honest
-- policies: recompute and version, or freeze and record the divergence. This
-- migration chooses FREEZE, because a research label that changes after the
-- fact is worse than useless — it invalidates every analysis that has already
-- read it, silently, with no version anybody wrote down.
--
-- `research_scan_outcomes_freeze` is what makes that structural. Once a row is
-- `measured`, the database REFUSES an UPDATE to any of its measurement columns
-- or to its status. The engine may still record that it looked again and found
-- something different (`revision_detected`, `revision_notes`) — those columns
-- are outside the frozen set on purpose, because "we noticed" must remain
-- writable when "we changed our mind" does not.
--
-- WHAT IS NOT HERE
-- ----------------
-- No aggregate table, no hit-rate column, no rollup, no dashboard-shaped view.
-- Every question in the brief's Phase 4 list — 5D outcomes for candidates,
-- WATCH vs AVOID, benchmark outperforming or not, distributions by config_hash
-- — is a GROUP BY over these columns. If one is not, the answer is a query.
--
-- NOTHING CANONICAL IS TOUCHED
-- ----------------------------
-- No reference to strategy_shadow_pairs, strategy_shadow_pair_outcomes,
-- prospective_campaign_registrations or any frozen-25 relation. The frozen
-- experiment is not read, not written, not joined, and not mentioned by any
-- foreign key here.
--
-- LICENSING
-- ---------
-- Derived from `research_scan_results`, which exists because of an FMP
-- discovery, so `internal_research_only` travels with the row and the Product
-- API's database role is granted nothing — the same omission that IS the
-- boundary in 026 and 029.
-- ===========================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1) One row per (scan, horizon).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.research_scan_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- ---- immutable scan provenance (O2) ------------------------------------
  -- CASCADE for the same reason migration 011 gives: an outcome is meaningless
  -- without the scan it measures, so it may only disappear if that scan is
  -- deliberately deleted — never independently.
  scan_id UUID NOT NULL
    REFERENCES public.research_scan_results(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  -- Restated from the scan rather than only joined, so this table answers
  -- "all 5D outcomes for scans in session S" without a join, and so the
  -- natural uniqueness below can be a database guarantee.
  scan_session DATE NOT NULL,

  -- ---- the horizon (O3): COMPLETED TRADING SESSIONS, never calendar days --
  horizon_sessions INTEGER NOT NULL,
  horizon_label TEXT NOT NULL,
  -- Resolved ONCE from the market calendar when the row is planned, and never
  -- recomputed. This is the load-bearing anti-lookahead column: the exit bar
  -- must be the bar for THIS DATE, so a missing session can never be silently
  -- skipped and turn a 5D into a six-session measurement.
  horizon_session DATE NOT NULL,

  contract_version TEXT NOT NULL,          -- research_scan_outcome.v1
  calculation_version TEXT NOT NULL,       -- outcome.v1 (the shared pure math)
  market_calendar_version TEXT NOT NULL,   -- us_market_calendar.v1

  -- ---- attribution SNAPSHOT (O11) ----------------------------------------
  -- Copied, not joined. See the header: the scan row is rewritable in place.
  strategy_code TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  -- The session-scoped candidacy, from app.research_funnel.scan_classification
  -- — the SAME function the lifecycle funnel uses, so `research_candidate`
  -- here means exactly what it means in research_lifecycle_run_symbols.
  scan_classification TEXT NOT NULL,
  scan_verdict TEXT,
  scan_structure_state TEXT,
  scan_setup_state TEXT,
  scan_reason_code TEXT,
  scan_rejection_reason TEXT,
  scan_benchmark_relative TEXT,
  -- The tell. If the scan row is later re-scanned, its `scanned_at` moves and
  -- this copy does not, so a divergence is detectable without keeping a second
  -- copy of the whole evidence blob.
  scan_scanned_at TIMESTAMPTZ NOT NULL,

  -- ---- the measurement (O12) ---------------------------------------------
  -- The benchmark is named on the row so a later change of benchmark is
  -- visible in the data rather than inferred from the code of the day.
  benchmark_symbol TEXT NOT NULL,
  -- All four endpoints are persisted, not just the returns, because the whole
  -- point of Phase 12 is that somebody can recompute the answer from raw bars
  -- and get the same number. Storing only the result would make that a matter
  -- of trust.
  entry_close NUMERIC,
  exit_close NUMERIC,
  benchmark_entry_close NUMERIC,
  benchmark_exit_close NUMERIC,
  symbol_return_pct NUMERIC,
  benchmark_return_pct NUMERIC,
  excess_return_pct NUMERIC,

  -- MFE/MAE over the horizon window, at DAILY granularity. Implemented rather
  -- than deferred because `daily_bars` carries real high/low for every session
  -- and the canonical experiment already computes excursions from exactly this
  -- source. What daily bars cannot tell us is intrabar ORDER — whether the low
  -- came before the high — so these are excursion EXTREMES and never a claim
  -- about a path, a stop being hit, or a sequence. `excursion_basis` says so
  -- on every row, and the bar counts beside it mean a partial window can never
  -- be read as a full one.
  mfe_pct NUMERIC,
  mae_pct NUMERIC,
  excursion_basis TEXT,
  window_sessions_expected INTEGER,
  window_sessions_present INTEGER,

  -- SHA-256 over the exact bars used, so a later pass can detect a corrected
  -- close without keeping a second copy of the series.
  bars_hash TEXT,

  -- ---- lifecycle (O13) ----------------------------------------------------
  --   not_yet_eligible  the horizon session has not completed yet
  --   waiting_for_data  it has, and a required bar is not in the store (O7)
  --   measured          frozen, immutable, final
  --   failed_terminal   the data did not arrive within the grace window
  status TEXT NOT NULL DEFAULT 'not_yet_eligible',
  -- A bounded reason CODE (e.g. `missing_symbol_exit_bar`). Never a payload,
  -- never a trace, never a provider message.
  status_reason TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TIMESTAMPTZ,
  -- Set once, when the numbers are written. Never moved.
  measured_at TIMESTAMPTZ,

  -- O8's other half: we record that we looked again and disagreed, and we do
  -- not act on it. Bounded, deterministic records only.
  revision_detected BOOLEAN NOT NULL DEFAULT FALSE,
  revision_notes JSONB NOT NULL DEFAULT '[]'::jsonb,

  licensing_visibility TEXT NOT NULL DEFAULT 'internal_research_only',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- O6, stated twice on purpose. The first is the identity the engine writes
  -- through; the second is the same fact in the vocabulary a reader uses, and
  -- it is what makes "two scans of the same symbol on different sessions are
  -- independent" a schema guarantee rather than a test.
  CONSTRAINT research_scan_outcomes_scan_horizon_uq
    UNIQUE (scan_id, horizon_sessions),
  CONSTRAINT research_scan_outcomes_natural_uq
    UNIQUE (symbol, scan_session, horizon_sessions),

  CONSTRAINT research_scan_outcomes_horizon_ck
    CHECK (horizon_sessions IN (1, 3, 5, 10, 20)),
  CONSTRAINT research_scan_outcomes_horizon_label_ck
    CHECK (horizon_label = horizon_sessions::text || 'D'),
  -- THE ANTI-LOOKAHEAD CONSTRAINT. A horizon that does not lie strictly in the
  -- future of the scan is not a horizon.
  CONSTRAINT research_scan_outcomes_forward_ck
    CHECK (horizon_session > scan_session),

  CONSTRAINT research_scan_outcomes_status_ck
    CHECK (status IN ('not_yet_eligible', 'waiting_for_data',
                      'measured', 'failed_terminal')),
  CONSTRAINT research_scan_outcomes_classification_ck
    CHECK (scan_classification IN ('research_candidate',
                                   'scanned_not_candidate',
                                   'classification_pending')),
  CONSTRAINT research_scan_outcomes_counts_ck
    CHECK (attempt_count >= 0
           AND (window_sessions_expected IS NULL OR window_sessions_expected >= 0)
           AND (window_sessions_present IS NULL OR window_sessions_present >= 0)),
  -- A price is a price. A zero or negative close is a data fault, and a return
  -- computed from one is arithmetic on a fault.
  CONSTRAINT research_scan_outcomes_prices_ck
    CHECK ((entry_close IS NULL OR entry_close > 0)
           AND (exit_close IS NULL OR exit_close > 0)
           AND (benchmark_entry_close IS NULL OR benchmark_entry_close > 0)
           AND (benchmark_exit_close IS NULL OR benchmark_exit_close > 0)),

  -- A measured row is COMPLETE. There is no such thing as a measured outcome
  -- missing an endpoint, a return, or its timestamp.
  CONSTRAINT research_scan_outcomes_measured_complete_ck
    CHECK (status <> 'measured'
           OR (measured_at IS NOT NULL
               AND entry_close IS NOT NULL AND exit_close IS NOT NULL
               AND benchmark_entry_close IS NOT NULL
               AND benchmark_exit_close IS NOT NULL
               AND symbol_return_pct IS NOT NULL
               AND benchmark_return_pct IS NOT NULL
               AND excess_return_pct IS NOT NULL
               AND bars_hash IS NOT NULL)),
  -- And its converse, which is the one that enforces O4 in the schema: a row
  -- that is not measured carries NO NUMBER AT ALL. A partially-written
  -- pending row cannot exist, so no reader can ever pick up a return that was
  -- computed before its horizon completed.
  CONSTRAINT research_scan_outcomes_pending_empty_ck
    CHECK (status = 'measured'
           OR (measured_at IS NULL
               AND entry_close IS NULL AND exit_close IS NULL
               AND benchmark_entry_close IS NULL
               AND benchmark_exit_close IS NULL
               AND symbol_return_pct IS NULL
               AND benchmark_return_pct IS NULL
               AND excess_return_pct IS NULL
               AND mfe_pct IS NULL AND mae_pct IS NULL
               AND bars_hash IS NULL)),
  CONSTRAINT research_scan_outcomes_licensing_ck
    CHECK (licensing_visibility IN ('product_display_allowed',
                                    'internal_research_only',
                                    'unknown_restriction'))
);

-- The maturation worklist: rows whose horizon has arrived and which are not
-- finished. Partial, so it stays small as measured rows accumulate.
CREATE INDEX IF NOT EXISTS research_scan_outcomes_due_idx
  ON public.research_scan_outcomes (horizon_session, horizon_sessions)
  WHERE status IN ('not_yet_eligible', 'waiting_for_data');

-- The reading indexes. Deliberately three, matching the three questions the
-- brief actually names: by horizon+classification, by session, by config.
CREATE INDEX IF NOT EXISTS research_scan_outcomes_analysis_idx
  ON public.research_scan_outcomes (horizon_sessions, scan_classification, status);
CREATE INDEX IF NOT EXISTS research_scan_outcomes_session_idx
  ON public.research_scan_outcomes (scan_session DESC, symbol);
CREATE INDEX IF NOT EXISTS research_scan_outcomes_config_idx
  ON public.research_scan_outcomes (config_hash, horizon_sessions);


-- ---------------------------------------------------------------------------
-- 2) IMMUTABILITY, ENFORCED BY THE DATABASE.
--
-- Everything above is a shape. This is the guarantee. Without it, "measured
-- outcomes are immutable" is a property of the current version of one Python
-- function, and the first backfill script somebody writes by hand in a hurry
-- is the counterexample.
--
-- The frozen set is the MEASUREMENT and the STATUS. Deliberately outside it:
-- attempt_count, last_attempt_at, revision_detected, revision_notes,
-- updated_at — because a later pass must remain able to say "I looked again
-- and the store now disagrees with what I froze" without being able to act
-- on it.
--
-- Modelled on `history_warmup_universe_symbols_guard`, which is this project's
-- existing answer to the same question for the frozen universe.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.research_scan_outcomes_freeze()
RETURNS trigger AS $fn$
BEGIN
  IF OLD.status = 'measured' THEN
    IF NEW.status                IS DISTINCT FROM OLD.status
       OR NEW.measured_at        IS DISTINCT FROM OLD.measured_at
       OR NEW.entry_close        IS DISTINCT FROM OLD.entry_close
       OR NEW.exit_close         IS DISTINCT FROM OLD.exit_close
       OR NEW.benchmark_entry_close IS DISTINCT FROM OLD.benchmark_entry_close
       OR NEW.benchmark_exit_close  IS DISTINCT FROM OLD.benchmark_exit_close
       OR NEW.symbol_return_pct  IS DISTINCT FROM OLD.symbol_return_pct
       OR NEW.benchmark_return_pct IS DISTINCT FROM OLD.benchmark_return_pct
       OR NEW.excess_return_pct  IS DISTINCT FROM OLD.excess_return_pct
       OR NEW.mfe_pct            IS DISTINCT FROM OLD.mfe_pct
       OR NEW.mae_pct            IS DISTINCT FROM OLD.mae_pct
       OR NEW.excursion_basis    IS DISTINCT FROM OLD.excursion_basis
       OR NEW.window_sessions_expected IS DISTINCT FROM OLD.window_sessions_expected
       OR NEW.window_sessions_present  IS DISTINCT FROM OLD.window_sessions_present
       OR NEW.bars_hash          IS DISTINCT FROM OLD.bars_hash
       OR NEW.horizon_session    IS DISTINCT FROM OLD.horizon_session
       OR NEW.scan_id            IS DISTINCT FROM OLD.scan_id
       OR NEW.scan_classification IS DISTINCT FROM OLD.scan_classification
       OR NEW.config_hash        IS DISTINCT FROM OLD.config_hash THEN
      RAISE EXCEPTION
        'research_scan_outcomes %: a measured outcome is immutable', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
    END IF;
  END IF;
  -- The horizon session is derived from the scan session and the market
  -- calendar. Neither may move under a row that already exists, measured or
  -- not: that would re-point a pending observation at a different day.
  IF NEW.scan_session IS DISTINCT FROM OLD.scan_session
     OR NEW.horizon_sessions IS DISTINCT FROM OLD.horizon_sessions
     OR NEW.horizon_session IS DISTINCT FROM OLD.horizon_session THEN
    RAISE EXCEPTION
      'research_scan_outcomes %: the horizon identity is immutable', OLD.id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  NEW.updated_at := NOW();
  RETURN NEW;
END
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS research_scan_outcomes_freeze_trg
  ON public.research_scan_outcomes;
CREATE TRIGGER research_scan_outcomes_freeze_trg
  BEFORE UPDATE ON public.research_scan_outcomes
  FOR EACH ROW EXECUTE FUNCTION public.research_scan_outcomes_freeze();


-- ---------------------------------------------------------------------------
-- 3) One row per maturation RUN.
--
-- Small, bounded, and load-bearing for exactly one thing besides visibility:
-- the durable queue's crash-after-persist reconciliation. The engine writes
-- this row in a `finally`, so a worker that dies after doing the work but
-- before finalising its task has already left the evidence, and the probe can
-- recognise a completed run instead of repeating it.
--
-- The same lesson as `research_lifecycle_runs`: a run that FAILED or is still
-- RUNNING is not durable output, and a probe that treats it as such destroys
-- the continuation (T16). The status vocabulary keeps those distinguishable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.research_outcome_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_key TEXT NOT NULL,
  contract_version TEXT NOT NULL,

  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  duration_seconds NUMERIC,
  status TEXT NOT NULL,
  failure_summary TEXT,                     -- an exception class or a code

  -- The session the run measured AS OF. Not the wall date: the same
  -- distinction migration 025 drew, for the same reason.
  as_of_session DATE,
  mode TEXT NOT NULL DEFAULT 'scheduled',   -- scheduled | backfill | dry_run

  scans_considered INTEGER NOT NULL DEFAULT 0,
  observations_planned INTEGER NOT NULL DEFAULT 0,
  observations_due INTEGER NOT NULL DEFAULT 0,
  measured INTEGER NOT NULL DEFAULT 0,
  waiting_for_data INTEGER NOT NULL DEFAULT 0,
  not_yet_eligible INTEGER NOT NULL DEFAULT 0,
  failed_terminal INTEGER NOT NULL DEFAULT 0,
  revisions_detected INTEGER NOT NULL DEFAULT 0,
  -- TRUE when the run stopped at its bound with work still due, so a short
  -- run is never mistaken for a finished one.
  truncated_by_limit BOOLEAN NOT NULL DEFAULT FALSE,

  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_outcome_runs_run_key_uq UNIQUE (run_key),
  CONSTRAINT research_outcome_runs_status_ck
    CHECK (status IN ('running', 'completed', 'dry_run', 'failed')),
  CONSTRAINT research_outcome_runs_mode_ck
    CHECK (mode IN ('scheduled', 'backfill', 'dry_run')),
  CONSTRAINT research_outcome_runs_nonneg_ck
    CHECK (scans_considered >= 0 AND observations_planned >= 0
           AND observations_due >= 0 AND measured >= 0
           AND waiting_for_data >= 0 AND not_yet_eligible >= 0
           AND failed_terminal >= 0 AND revisions_detected >= 0)
);

CREATE INDEX IF NOT EXISTS research_outcome_runs_started_idx
  ON public.research_outcome_runs (started_at DESC);


-- ---------------------------------------------------------------------------
-- 4) RLS on, as on every table this project adds.
-- ---------------------------------------------------------------------------
ALTER TABLE public.research_scan_outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.research_outcome_runs  ENABLE ROW LEVEL SECURITY;


-- ---------------------------------------------------------------------------
-- 5) The research lifecycle role gains these two tables and nothing else.
--
-- Done here, guarded by role existence, for the reason migration 026 does the
-- same: the role's grant file explicitly does NOT use ALTER DEFAULT PRIVILEGES,
-- so a new table is not automatically reachable — and a migration that creates
-- a table the executing worker cannot write is a migration that applies
-- cleanly and then fails at run time. ops/sql/create_smart_scanner_research_
-- lifecycle*.sql carries the same lines so a rebuilt role is complete too.
--
-- SELECT, INSERT, UPDATE. No DELETE: an outcome ledger from which rows can be
-- removed is not evidence.
--
-- The Product API's reader is granted NOTHING, and the omission IS the licence
-- boundary — unchanged from 026 and 029.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  r text := 'smart_scanner_research_lifecycle';
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON public.research_scan_outcomes TO %I', r);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON public.research_outcome_runs  TO %I', r);

    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='research_scan_outcomes'
                     AND policyname = r||'_rw') THEN
      EXECUTE format(
        'CREATE POLICY %I ON public.research_scan_outcomes '
        'AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)', r||'_rw', r);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='research_outcome_runs'
                     AND policyname = r||'_rw') THEN
      EXECUTE format(
        'CREATE POLICY %I ON public.research_outcome_runs '
        'AS PERMISSIVE FOR ALL TO %I USING (true) WITH CHECK (true)', r||'_rw', r);
    END IF;
  END IF;
END
$$;


-- ---------------------------------------------------------------------------
-- 6) The outcome-maturation schedule — DECLARED, and DISABLED here.
--
-- Created disabled AND paused so applying this migration can never start
-- anything. Enabling it is a separate, deliberate, reversible operator action,
-- exactly as migration 029 established for the lifecycle itself.
--
-- WHY IT RIDES THE EXISTING `research_lifecycle` QUEUE
-- ---------------------------------------------------
-- The queue is the EXECUTION BOUNDARY — which identity may claim this work —
-- and outcome maturation needs precisely the identity the research lifecycle
-- already has: read `research_scan_results`, read `daily_bars` (including the
-- benchmark, which its SELECT policy already permits in full), write research
-- tables, touch nothing canonical. Inventing a second queue would mean a
-- second RLS predicate, a second entry in JOB_WORKER_QUEUES and a redeploy of
-- the worker's configuration, to express a boundary that is already drawn in
-- exactly the right place.
--
-- Failure isolation does not come from the queue and never did: it comes from
-- this being a SEPARATE JOB with a separate task, a separate attempt budget
-- and a separate schedule. An outcome run that fails settles on its own row
-- and cannot reach the lifecycle's — which is the brief's requirement, stated
-- as "outcome maturation should not prevent today's research run from
-- completing".
--
-- TIMING, AND WHY IT IS A PREFERENCE RATHER THAN A CONTRACT
-- ---------------------------------------------------------
-- `market_daily` with a 1140-minute delay -> 11:00 America/New_York on the day
-- AFTER the session, holiday-aware through the same session resolver.
--
-- It is placed there to run AFTER the research lifecycle, because the forward
-- bars this pass measures against are the ones that pass fetches. The
-- lifecycle's LIVE staging schedule is close+960 (08:00 ET the following
-- morning; migration 029 declared 150 and an operator moved it), it may defer
-- itself up to three times at thirty minutes each, and a full run was measured
-- at 23-45 minutes. 11:00 ET clears all of that with room to spare, and the
-- worker's concurrency of 1 means that if the lifecycle is somehow still
-- running, the outcome task simply waits its turn in the queue.
--
-- But the ordering is an EFFICIENCY preference, not a correctness requirement,
-- and it matters that those are different. A pass that runs too early finds
-- the exit bar absent, writes `waiting_for_data`, and measures on a later
-- pass — the same path a genuinely late bar takes. Nothing is lost and nothing
-- is wrong; the only cost is a wasted query. So if the lifecycle's schedule
-- moves again, this row does not have to chase it.
-- ---------------------------------------------------------------------------
INSERT INTO public.job_schedules (
    schedule_code, schedule_version, job_type, job_contract_version,
    schedule_type, timezone, market_close_delay_minutes,
    enabled, paused, payload_template)
SELECT
    'SMART-SCANNER-RESEARCH-OUTCOMES', 1,
    'smart_scanner_research_outcomes.v1', 'smart_scanner_research_outcomes.v1',
    'market_daily', 'America/New_York', 1140,
    FALSE, TRUE,
    jsonb_build_object(
      'scheduler_owner', 'research_lifecycle',
      'task_type', 'smart_scanner_research_outcome_maturation.v1',
      'queue', 'research_lifecycle',
      'scan_limit', 200,
      'observation_limit', 400,
      'description',
      'Bounded staging research-outcome maturation: plan every scan''s five '
      'session-based horizons, measure the ones whose horizon has completed '
      'against the same start/end sessions for symbol and benchmark, and '
      'persist immutably. Reads local bars only; makes no provider call. '
      'Disabled on creation; enabling is a separate operator action.')
WHERE NOT EXISTS (
    SELECT 1 FROM public.job_schedules
    WHERE schedule_code = 'SMART-SCANNER-RESEARCH-OUTCOMES'
      AND schedule_version = 1);
