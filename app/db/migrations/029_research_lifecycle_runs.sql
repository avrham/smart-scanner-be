-- ===========================================================================
-- 029 — The research lifecycle becomes measurable across sessions
-- ===========================================================================
-- Everything the previous milestone learned about the funnel was learned by
-- reading one run's stdout. That is enough to answer "what happened just now"
-- and nothing else: it cannot answer whether the admission pass rate is stable,
-- whether the same symbols keep reappearing, or whether one candidate in seven
-- scans is normal or was a lucky session.
--
-- So: one row per lifecycle run, and one row per symbol per run.
--
-- WHY A CHILD TABLE AND NOT JUST COUNTERS
-- ---------------------------------------
-- The parent's counters are a claim. The child table is the evidence, and its
-- PRIMARY KEY (run_id, symbol) is what makes "every symbol accounted for
-- exactly once" a database guarantee rather than a code comment: a run
-- physically cannot record the same symbol in two lifecycle states.
--
-- That is also why `funnel_conserved` is a stored column. A run whose totals
-- did not add up is not deleted or hidden — it is recorded as not conserving,
-- so a later reader sees a broken run instead of a plausible one.
--
-- WHAT THIS IS NOT
-- ----------------
-- Not an analytics warehouse. There are no daily rollups, no derived metric
-- tables, no history of the metric definitions. Two tables, both bounded by
-- the number of runs (one a day) times the size of the research pool (tens).
-- Every question in the measurement brief is answerable with a GROUP BY over
-- these two tables, and if one is not, the answer is a query, not a table.
--
-- NOT AN OUTCOME LEDGER, EITHER
-- -----------------------------
-- No forward return, no realised P&L, no label. A research candidate is a
-- symbol that survived a screen; whether that meant anything is a question
-- this milestone deliberately does not let itself answer, because the honest
-- sample size is one.
--
-- LICENSING
-- ---------
-- `research_symbols` exists because of an FMP discovery, so everything derived
-- from it is internal_research_only. Neither table is granted to the Product
-- API reader — the same omission that enforces the boundary for
-- `research_symbols` itself. See app/source_licensing.py.
-- ===========================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- 1) One row per lifecycle run.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.research_lifecycle_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Idempotency. A dispatcher that fires twice for the same occurrence, or a
  -- worker that retries a leased task, must not produce two runs claiming to
  -- describe the same execution. Derived from the dispatch identity, so a
  -- deliberate manual re-run gets its own key and is a genuinely new run.
  run_key TEXT NOT NULL,
  contract_version TEXT NOT NULL,

  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  duration_seconds NUMERIC,

  -- running | completed | dry_run | blocked_stale_core_history
  -- | blocked_canonical_config_unavailable | failed
  status TEXT NOT NULL,
  -- Bounded, secret-free: an exception class or a reason code, never a payload.
  failure_summary TEXT,

  -- ---- the context the run was pinned to ---------------------------------
  -- The latest COMPLETED session. Not today's wall date: migration 025's
  -- distinction, kept here so a run is interpretable months later.
  target_session DATE,
  -- The market session the discovery snapshot DESCRIBED.
  discovery_reference_session DATE,
  canonical_history_fresh BOOLEAN,
  -- Copied from the resolved candidate arm, so a run in which the strategy
  -- configuration drifted is visible in the row rather than inferred.
  canonical_config_hash TEXT,
  canonical_min_price NUMERIC,

  -- ---- the funnel, as counters -------------------------------------------
  symbols_considered INTEGER NOT NULL DEFAULT 0,
  symbols_selected INTEGER NOT NULL DEFAULT 0,
  admission_passed INTEGER NOT NULL DEFAULT 0,
  admission_rejected INTEGER NOT NULL DEFAULT 0,
  admission_unknown INTEGER NOT NULL DEFAULT 0,
  admission_pending INTEGER NOT NULL DEFAULT 0,
  history_warmups_attempted INTEGER NOT NULL DEFAULT 0,
  history_ready INTEGER NOT NULL DEFAULT 0,
  history_unavailable INTEGER NOT NULL DEFAULT 0,
  history_failed INTEGER NOT NULL DEFAULT 0,
  research_scanned INTEGER NOT NULL DEFAULT 0,
  research_candidates INTEGER NOT NULL DEFAULT 0,

  -- ---- provider cost, in both directions ---------------------------------
  -- `avoided` is not a vanity metric: it is the count of requests admission
  -- removed, and it is the number that justifies the gate existing.
  provider_calls_used INTEGER NOT NULL DEFAULT 0,
  provider_calls_avoided INTEGER NOT NULL DEFAULT 0,
  provider_budget INTEGER,
  provider_budget_exhausted BOOLEAN NOT NULL DEFAULT FALSE,
  bars_inserted INTEGER NOT NULL DEFAULT 0,

  -- ---- enrichment ---------------------------------------------------------
  enrichment_symbols INTEGER NOT NULL DEFAULT 0,
  enrichment_sources_ok INTEGER NOT NULL DEFAULT 0,
  enrichment_sources_failed INTEGER NOT NULL DEFAULT 0,

  -- ---- the invariant ------------------------------------------------------
  -- FALSE is recorded, never suppressed. A run that did not conserve symbols
  -- is a fact about the system, and hiding it is how a counting bug survives.
  funnel_conserved BOOLEAN NOT NULL DEFAULT TRUE,

  -- The full summary as produced, for the fields no column anticipated.
  -- Bounded by the run summary itself; never a provider payload.
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_lifecycle_runs_run_key_uq UNIQUE (run_key),
  CONSTRAINT research_lifecycle_runs_status_ck CHECK (status IN (
    'running', 'completed', 'dry_run', 'failed',
    'blocked_stale_core_history', 'blocked_canonical_config_unavailable')),
  -- Counters are counts. A negative one means an arithmetic bug upstream and
  -- must not be storable.
  CONSTRAINT research_lifecycle_runs_nonneg_ck CHECK (
    symbols_considered >= 0 AND symbols_selected >= 0
    AND admission_passed >= 0 AND admission_rejected >= 0
    AND admission_unknown >= 0 AND admission_pending >= 0
    AND provider_calls_used >= 0 AND provider_calls_avoided >= 0
    AND bars_inserted >= 0),
  -- The admission tiers must partition the selected pool. Enforced in the
  -- DATABASE as well as in app/research_funnel.py, so a run row that could not
  -- be true is rejected at the write rather than found in a report.
  CONSTRAINT research_lifecycle_runs_admission_partition_ck CHECK (
    status <> 'completed'
    OR admission_passed + admission_rejected
       + admission_unknown + admission_pending = symbols_selected)
);

CREATE INDEX IF NOT EXISTS research_lifecycle_runs_started_idx
  ON public.research_lifecycle_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS research_lifecycle_runs_session_idx
  ON public.research_lifecycle_runs (target_session DESC);

-- ---------------------------------------------------------------------------
-- 2) One row per symbol per run — the evidence behind the counters.
--
-- (run_id, symbol) is the primary key, which is the whole point: exactly-once
-- accounting is enforced by the schema, not by the code that writes it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.research_lifecycle_run_symbols (
  run_id UUID NOT NULL
    REFERENCES public.research_lifecycle_runs(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,

  -- The ONE state from app/research_funnel.py. Not a set of flags: a symbol
  -- carrying two booleans is a symbol that can be counted twice.
  lifecycle_state TEXT NOT NULL,
  -- The admission outcome, kept separately because a symbol that PASSED
  -- admission and later failed warmup still passed admission — folding them
  -- would make the pass rate shrink as symbols move down the funnel.
  admission_tier TEXT NOT NULL,

  warmed BOOLEAN NOT NULL DEFAULT FALSE,
  provider_calls INTEGER NOT NULL DEFAULT 0,
  bars_inserted INTEGER NOT NULL DEFAULT 0,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_lifecycle_run_symbols_pkey PRIMARY KEY (run_id, symbol),
  CONSTRAINT research_lifecycle_run_symbols_state_ck CHECK (lifecycle_state IN (
    'admission_pending', 'admission_rejected', 'history_pending',
    'history_warming', 'history_unavailable', 'history_failed',
    'scan_pending', 'classification_pending', 'scanned_not_candidate',
    'research_candidate')),
  CONSTRAINT research_lifecycle_run_symbols_tier_ck CHECK (admission_tier IN (
    'admission_pending', 'eligible_for_history', 'rejected_before_history',
    'insufficient_admission_data'))
);

CREATE INDEX IF NOT EXISTS research_lifecycle_run_symbols_symbol_idx
  ON public.research_lifecycle_run_symbols (symbol);
CREATE INDEX IF NOT EXISTS research_lifecycle_run_symbols_state_idx
  ON public.research_lifecycle_run_symbols (lifecycle_state);

-- ---------------------------------------------------------------------------
-- 3) RLS on, as on every table this project adds. Policies live in ops/sql per
--    role; without one, a grant alone returns nothing. The Product API reader
--    is granted NOTHING here and gets no policy — the omission IS the licence
--    boundary, exactly as for research_symbols in 026.
-- ---------------------------------------------------------------------------
ALTER TABLE public.research_lifecycle_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.research_lifecycle_run_symbols ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- 4) The research lifecycle schedule — DECLARED, and DISABLED here.
--
-- The row is created disabled and paused so that applying this migration can
-- never start anything. Enabling it is a separate, deliberate, reversible
-- operator action, taken only once the dedicated execution identity exists and
-- has been verified (ops/sql/create_smart_scanner_research_lifecycle.sql).
--
-- SCHEDULER OWNERSHIP, AND WHY IT IS IN THE PAYLOAD
-- ------------------------------------------------
-- The existing scheduler leader materialises EVERY due schedule. That was fine
-- while one component owned every schedule. It is not fine now: the
-- pipeline-driver's role is deliberately unable to touch research tables, and
-- a research task it materialised would be a task it could never explain.
--
-- `payload_template.scheduler_owner` names the worker type allowed to
-- materialise this schedule. A schedule WITHOUT the key behaves exactly as
-- before (any leader may materialise it), so no existing schedule changes
-- meaning and the pipeline-driver needs no redeploy.
--
-- TIMING: `market_daily` with a 150-minute delay -> 18:30 America/New_York,
-- holiday- and early-close-aware via the existing session resolver. The
-- reasoning is in ops/analysis/research_lifecycle.py; in short, the movers
-- feeds describe the COMPLETED tape and the daily bars must have settled
-- before a scan against them means anything.
-- ---------------------------------------------------------------------------
-- `job_schedules` carries no queue column (018) — the queue travels in the
-- payload template, which is also where the task type and the bounded limits
-- live, so one row fully describes what a fire means.
INSERT INTO public.job_schedules (
    schedule_code, schedule_version, job_type, job_contract_version,
    schedule_type, timezone, market_close_delay_minutes,
    enabled, paused, payload_template)
SELECT
    'SMART-SCANNER-RESEARCH-LIFECYCLE', 1,
    'smart_scanner_research_lifecycle.v1', 'smart_scanner_research_lifecycle.v1',
    'market_daily', 'America/New_York', 150,
    FALSE, TRUE,
    jsonb_build_object(
      'scheduler_owner', 'research_lifecycle',
      'task_type', 'smart_scanner_research_lifecycle_run.v1',
      'queue', 'research_lifecycle',
      'admit_limit', 5,
      'warm_limit', 5,
      'provider_budget', 12,
      'refresh_discovery', true,
      'description',
      'Bounded staging research lifecycle: discovery -> admission -> warmup '
      '-> scan -> classification -> research-scoped enrichment. Disabled on '
      'creation; enabling is a separate operator action.')
WHERE NOT EXISTS (
    SELECT 1 FROM public.job_schedules
    WHERE schedule_code = 'SMART-SCANNER-RESEARCH-LIFECYCLE'
      AND schedule_version = 1);
