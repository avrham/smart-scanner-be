-- Phase 8.1B2: paired market-path outcomes for the frozen sma150.v2 vs
-- sma150.v3 shadow pairs created by Phase 8.1B1 (migration 010).
--
-- Additive and idempotent (safe to run more than once). Run manually in the
-- Supabase SQL editor after 010_sma150_shadow_evaluations.sql. Migration 010
-- is NOT modified by this file.
--
-- Core identity: exactly ONE market-path outcome per frozen pair — never one
-- outcome per arm. Both arm evaluations share the same symbol, provider,
-- canonical frame, frame hash, snapshot date and observation close, so there
-- is only ONE observed forward market path. Persisting duplicated
-- control/candidate returns would be numerically identical and conceptually
-- misleading (a fake control_return - candidate_return delta must never
-- exist). The two strategy decisions stay in strategy_shadow_evaluations and
-- are JOINED to the shared pair outcome when reading or aggregating.
--
-- A pair outcome is a MARKET-PATH OBSERVATION, not a simulated trade:
--   * reference_price_role is always 'paired_decision_observation' — the
--     close of the LAST bar of the frozen B1 frame_snapshot, verdict-neutral
--     for ENTER, WATCH and AVOID pairs;
--   * no stop, no target, no simulated R, no trade side;
--   * no same-ticker buy-and-hold baseline (from the same frozen close it is
--     identical to the pair return by definition — a tautological zero delta
--     must not be advertised as evidence).
--
-- Write-once horizon maturation: a NULL horizon may become calculated once;
-- a calculated horizon is FROZEN (never overwritten, never reset to NULL).
-- Divergent recalculations are recorded in bounded revision_notes instead of
-- mutating frozen evidence.
--
-- Foreign-key delete behavior (deliberate, non-destructive):
--   * outcomes.pair_id -> pairs: ON DELETE CASCADE. A pair outcome is
--     meaningless without its pair's frozen frame and decisions; it only
--     disappears if the immutable pair itself is deliberately deleted, never
--     independently (same pattern as strategy_shadow_evaluations in 010).
--   * strategy_shadow_outcome_runs has NO foreign keys: an outcome run is a
--     bounded operational audit record; deleting it can never touch outcome
--     evidence, and deleting outcomes never destroys run history.
--
-- This migration deliberately duplicates NOTHING from migration 010: no
-- frame snapshots, no evaluation details, no config snapshots, no strategy
-- identities. Those remain joined from the existing B1 tables.

-- ---------------------------------------------------------------------------
-- 1. One canonical market-path outcome per frozen pair
-- ---------------------------------------------------------------------------
-- outcome_fingerprint = SHA-256 over the canonical outcome CONTRACT for one
-- pair: outcome fingerprint version, the B1 pair fingerprint (+ its
-- version), calculation version, outcome coverage version and forward frame
-- version. It deliberately EXCLUDES the current forward bar count, forward
-- hash, run ids, statuses and timestamps, so the fingerprint stays STABLE
-- while horizons mature from NULL to calculated.

CREATE TABLE IF NOT EXISTS public.strategy_shadow_pair_outcomes (
  id UUID PRIMARY KEY,
  pair_id UUID NOT NULL UNIQUE
    REFERENCES public.strategy_shadow_pairs(id) ON DELETE CASCADE,

  -- Versioned outcome contract identity (stable across maturation).
  outcome_fingerprint TEXT NOT NULL,
  outcome_fingerprint_version TEXT NOT NULL,  -- 'shadow_pair_outcome_fingerprint.v1'
  calculation_version TEXT NOT NULL,          -- 'outcome.v1' (pure math reuse)
  outcome_coverage_version TEXT NOT NULL,     -- 'shadow_pair_outcomes.v1'
  forward_frame_version TEXT NOT NULL,        -- 'shadow_forward_bars.v1'

  -- Frozen reference: the close of the LAST bar of the B1 frame_snapshot.
  -- NEVER from a fresh provider response, created_at, a later WATCH trigger
  -- or a hypothetical entry.
  -- NULLABLE by design: a row in the 'error' lifecycle (e.g. the frozen
  -- frame itself failed validation, provider_mismatch before any
  -- calculation) legitimately has no resolved reference yet. Every
  -- successfully initialized outcome sets it, and once set it is immutable
  -- (application merge + SQL COALESCE guard) — enforced by tests, not by a
  -- schema NOT NULL that would break the honest error lifecycle.
  reference_price DOUBLE PRECISION,
  reference_price_role TEXT NOT NULL
    CHECK (reference_price_role = 'paired_decision_observation'),

  -- Forward market path metadata (the exact bars used are hashed, stored in
  -- the daily_bars read-through cache, and NOT duplicated as a snapshot).
  -- forward_provider is NULLABLE for the same error-lifecycle reason as
  -- reference_price; every calculated outcome sets it (it must equal the
  -- frozen pair provider) and it is immutable once set.
  forward_provider TEXT,                      -- must equal the frozen pair provider
  forward_data_as_of DATE,                    -- last completed forward bar used
  available_forward_bars INT NOT NULL DEFAULT 0
    CHECK (available_forward_bars BETWEEN 0 AND 20),
  first_forward_date DATE,
  last_forward_date DATE,
  forward_bars_hash TEXT,                     -- SHA-256, shadow_forward_bars.v1

  -- Write-once raw market-path returns (PERCENT), Nth completed trading bar
  -- strictly after snapshot_date. NULL = not yet observable, never zero.
  ret_1d DOUBLE PRECISION,
  ret_3d DOUBLE PRECISION,
  ret_5d DOUBLE PRECISION,
  ret_10d DOUBLE PRECISION,
  ret_20d DOUBLE PRECISION,

  -- Excursions over the AVAILABLE completed forward bars; the bar count is
  -- always persisted next to them so a 3-bar excursion is never presented
  -- as a 20-bar excursion. Updated only when the bar count INCREASES.
  max_favorable_excursion DOUBLE PRECISION,
  max_adverse_excursion DOUBLE PRECISION,
  mfe_mae_bar_count INT
    CHECK (mfe_mae_bar_count IS NULL OR mfe_mae_bar_count BETWEEN 0 AND 20),

  -- Deterministic bounded JSONB:
  --   benchmark_returns: {"SPY": {"1D": num|null, ...}, "QQQ": {...}}
  --   revision_notes:    bounded list of safe deterministic divergence
  --                      records (reason_code / horizon / values / hashes) —
  --                      never raw payloads, traces or credentials.
  benchmark_returns JSONB,
  revision_notes JSONB,

  -- True when a re-fetched snapshot-date bar's close differs from the frozen
  -- reference beyond numeric tolerance. The frozen reference is NEVER
  -- silently repaired or replaced.
  reference_revision_detected BOOLEAN NOT NULL DEFAULT FALSE,

  -- Maturation lifecycle: 0 bars -> pending_forward_bars; 1-19 -> partial;
  -- 20 -> complete; deterministic/operational failure -> error (repairable
  -- via include_recalc, frozen horizons preserved).
  outcome_status TEXT NOT NULL DEFAULT 'pending_forward_bars'
    CHECK (outcome_status IN (
      'pending_forward_bars',
      'partial',
      'complete',
      'error'
    )),

  error_code TEXT,                            -- safe identity, never a trace
  error_message TEXT,                         -- sanitized + bounded

  first_calculated_at TIMESTAMPTZ,            -- set once, never moved
  calculated_at TIMESTAMPTZ,                  -- latest calculation attempt

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One outcome contract per pair; duplicates are impossible.
CREATE UNIQUE INDEX IF NOT EXISTS strategy_shadow_pair_outcomes_fingerprint_uniq
ON public.strategy_shadow_pair_outcomes (outcome_fingerprint, outcome_fingerprint_version);

-- Maturation worklist: pending/partial/error rows are the ones selection
-- revisits (complete rows are only re-checked explicitly via include_recalc).
CREATE INDEX IF NOT EXISTS strategy_shadow_pair_outcomes_pending_idx
ON public.strategy_shadow_pair_outcomes (outcome_status, updated_at)
WHERE outcome_status IN ('pending_forward_bars', 'partial', 'error');

CREATE INDEX IF NOT EXISTS strategy_shadow_pair_outcomes_provider_idx
ON public.strategy_shadow_pair_outcomes (forward_provider);

-- Status + snapshot-date selection support goes through the pair join
-- (pair_id is already UNIQUE-indexed here; strategy_shadow_pairs has its own
-- symbol/snapshot_date index from migration 010).
CREATE INDEX IF NOT EXISTS strategy_shadow_pair_outcomes_status_idx
ON public.strategy_shadow_pair_outcomes (outcome_status);

-- ---------------------------------------------------------------------------
-- 2. Bounded admin-triggered outcome-calculation runs (operational audit)
-- ---------------------------------------------------------------------------
-- One durable row per POST /api/admin/shadow/outcomes/calculate run. Every
-- handled run is finalized as 'completed' or 'failed' — a handled exception
-- must never leave 'running'. No scheduler entry exists for these runs.

CREATE TABLE IF NOT EXISTS public.strategy_shadow_outcome_runs (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'completed', 'failed')),
  requested_selector JSONB,                   -- bounded normalized selectors
  requested_limit INT,
  provider TEXT,
  telemetry JSONB,                            -- bounded counters only
  error_code TEXT,                            -- safe identity, never a trace
  error_message TEXT,                         -- sanitized + bounded
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS strategy_shadow_outcome_runs_status_idx
ON public.strategy_shadow_outcome_runs (status, started_at DESC);
