-- Phase 8.1B1: frozen paired shadow evaluations of sma150.v2 vs sma150.v3
-- on the exact same completed market-data snapshot.
--
-- Additive and idempotent (safe to run more than once). Run manually in the
-- Supabase SQL editor after 009_watch_outcome_coverage.sql.
--
-- Shadow evaluations are an EXPERIMENT record, fully separated from normal
-- signals: they never write to signals / signal_provenance /
-- scan_run_signals / signal_outcomes, they preserve AVOID decisions, and
-- they are never user-facing candidates.
--
-- Four tables:
--   1. strategy_shadow_runs       - one bounded admin-triggered run.
--   2. strategy_shadow_pairs      - one IMMUTABLE row per exact comparison
--                                   input (symbol + canonical completed frame
--                                   + both arm identities/config hashes).
--                                   The canonical OHLCV frame is stored ONCE
--                                   per pair, never once per arm.
--   3. strategy_shadow_run_pairs  - occurrence links: every run that produced
--                                   or re-produced an exact pair records a
--                                   link. Repeated exact comparisons reuse
--                                   the immutable pair and add only a link.
--   4. strategy_shadow_evaluations- one IMMUTABLE row per arm per pair
--                                   (control_v2 / candidate_v3), preserving
--                                   ENTER, WATCH and AVOID verbatim.
--
-- Foreign-key delete behavior (deliberate, non-destructive):
--   * pairs.origin_run_id -> runs: ON DELETE SET NULL. Deleting a run must
--     never destroy the immutable pair evidence it originally created.
--   * run_pairs -> runs/pairs: ON DELETE CASCADE. A link row is meaningless
--     without both sides; the cascade removes ONLY the link, never the
--     immutable pair or evaluation data on the other side (same pattern as
--     scan_run_signals in migration 007).
--   * evaluations.pair_id -> pairs: ON DELETE CASCADE. An evaluation cannot
--     be interpreted without its pair's frozen frame; it only disappears if
--     the pair itself is deliberately deleted, never independently.

-- ---------------------------------------------------------------------------
-- 1. Shadow runs
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.strategy_shadow_runs (
  id UUID PRIMARY KEY,
  experiment_code TEXT NOT NULL,             -- 'sma150_v2_vs_v3'
  experiment_version TEXT NOT NULL,          -- 'sma150_shadow.v1'
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'completed', 'failed')),
  provider TEXT,
  requested_symbols JSONB NOT NULL DEFAULT '[]',  -- bounded (max 25 symbols)
  requested_limit INT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  telemetry JSONB,
  error_code TEXT,                           -- safe identity, never a trace
  error_message TEXT,                        -- sanitized + bounded
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS strategy_shadow_runs_status_idx
ON public.strategy_shadow_runs (status, started_at DESC);

CREATE INDEX IF NOT EXISTS strategy_shadow_runs_experiment_idx
ON public.strategy_shadow_runs (experiment_code, experiment_version);

-- ---------------------------------------------------------------------------
-- 2. Immutable comparison pairs (canonical frame stored once per pair)
-- ---------------------------------------------------------------------------
-- pair_fingerprint = SHA-256 over the canonical comparison inputs:
--   fingerprint version, experiment code/version, symbol, timeframe,
--   provider, frame hash, snapshot date, market_data_as_of, and BOTH arm
--   identities (strategy code/version/policy/config hash).
-- It deliberately EXCLUDES run_id and any insertion/server time, so repeated
-- exact comparisons reuse the same immutable pair (linked via
-- strategy_shadow_run_pairs) instead of duplicating or overwriting it.

CREATE TABLE IF NOT EXISTS public.strategy_shadow_pairs (
  id UUID PRIMARY KEY,
  origin_run_id UUID REFERENCES public.strategy_shadow_runs(id) ON DELETE SET NULL,
  experiment_code TEXT NOT NULL,
  experiment_version TEXT NOT NULL,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL DEFAULT '1d',
  provider TEXT,
  snapshot_date DATE NOT NULL,               -- last canonical COMPLETED bar date
  market_data_as_of TIMESTAMPTZ NOT NULL,    -- == last canonical completed bar
  frame_snapshot_version TEXT NOT NULL,      -- 'daily_ohlcv_snapshot.v1'
  frame_hash TEXT NOT NULL,                  -- SHA-256 of the FULL canonical frame
  frame_bar_count INT NOT NULL,
  frame_first_date DATE NOT NULL,
  frame_last_date DATE NOT NULL,
  frame_snapshot JSONB NOT NULL,             -- exact canonical bars, bounded
  pair_fingerprint TEXT NOT NULL,
  pair_fingerprint_version TEXT NOT NULL,    -- 'shadow_pair_fingerprint.v1'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One immutable row per exact comparison input; duplicates are impossible.
CREATE UNIQUE INDEX IF NOT EXISTS strategy_shadow_pairs_fingerprint_uniq
ON public.strategy_shadow_pairs (pair_fingerprint, pair_fingerprint_version);

CREATE INDEX IF NOT EXISTS strategy_shadow_pairs_symbol_idx
ON public.strategy_shadow_pairs (symbol, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS strategy_shadow_pairs_origin_run_idx
ON public.strategy_shadow_pairs (origin_run_id);

-- ---------------------------------------------------------------------------
-- 3. Run-to-pair occurrence links (every detection, not just the origin)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.strategy_shadow_run_pairs (
  run_id UUID NOT NULL REFERENCES public.strategy_shadow_runs(id) ON DELETE CASCADE,
  pair_id UUID NOT NULL REFERENCES public.strategy_shadow_pairs(id) ON DELETE CASCADE,
  created_new_pair BOOLEAN NOT NULL,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (run_id, pair_id)
);

CREATE INDEX IF NOT EXISTS strategy_shadow_run_pairs_pair_idx
ON public.strategy_shadow_run_pairs (pair_id);

-- ---------------------------------------------------------------------------
-- 4. Immutable per-arm evaluations (ENTER / WATCH / AVOID all preserved)
-- ---------------------------------------------------------------------------
-- evaluation_fingerprint = SHA-256 over: fingerprint version, the pair
-- fingerprint, arm code, strategy code/version/policy, config hash, verdict
-- and the deterministic hash of the ORIGINAL (pre-pruning) details. No
-- UPDATE path may rewrite an existing evaluation.

CREATE TABLE IF NOT EXISTS public.strategy_shadow_evaluations (
  id UUID PRIMARY KEY,
  pair_id UUID NOT NULL REFERENCES public.strategy_shadow_pairs(id) ON DELETE CASCADE,
  arm_code TEXT NOT NULL CHECK (arm_code IN ('control_v2', 'candidate_v3')),
  strategy_code TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  decision_policy_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  config_snapshot JSONB NOT NULL,            -- sanitized resolved config
  verdict TEXT NOT NULL CHECK (verdict IN ('ENTER', 'WATCH', 'AVOID')),
  score DOUBLE PRECISION,                    -- NULL when the arm emits none
  reason TEXT,
  rejection_reason TEXT,
  details_snapshot JSONB NOT NULL,           -- bounded, deterministic
  evidence_original_sha256 TEXT,             -- hash of the UNPRUNED details
  evaluation_fingerprint TEXT NOT NULL,
  evaluation_fingerprint_version TEXT NOT NULL, -- 'shadow_evaluation_fingerprint.v1'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (pair_id, arm_code)
);

CREATE UNIQUE INDEX IF NOT EXISTS strategy_shadow_evaluations_fingerprint_uniq
ON public.strategy_shadow_evaluations (evaluation_fingerprint, evaluation_fingerprint_version);

CREATE INDEX IF NOT EXISTS strategy_shadow_evaluations_verdict_idx
ON public.strategy_shadow_evaluations (arm_code, verdict);
