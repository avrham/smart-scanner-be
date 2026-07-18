-- Phase 2 (Evidence Engine): signal outcome tracking + baseline comparison.
--
-- Purpose: after a signal is generated we want to measure, deterministically and
-- after the fact, how it actually performed over fixed holding windows and how
-- that compares to simple baselines (same-ticker buy & hold, SPY, QQQ). This is
-- the product's "value gate": we should never present a strategy as useful
-- without sample size AND baseline comparison.
--
-- Design notes / simplicity choices:
--   * One row per signal (signal_id UNIQUE, FK to signals). Recalculation upserts.
--   * The signal's own per-window returns are explicit numeric columns
--     (ret_1d..ret_20d) so they are trivially aggregatable in SQL if needed.
--   * The baseline breakdowns (SPY / QQQ / same-ticker buy&hold) are stored as
--     small JSONB maps keyed by window label ("1D".."20D"). This keeps the table
--     readable instead of exploding into ~20 numeric columns. Aggregation reads
--     them in Python (app/workers/outcomes/metrics.py).
--   * Returns are stored as PERCENT (e.g. 3.5 == +3.5%), side-adjusted for the
--     signal (LONG vs SHORT). Baselines are naive LONG buy & hold.
--   * stop/target/invalidation and the derived hit_stop/hit_target/simulated_r
--     are nullable: sma150_bounce has no stop/target today, so they stay NULL
--     and are simply excluded from metrics until strategies provide them.
--
-- Run this manually in Supabase AFTER 001_initial_schema.sql and
-- 002_phase1_sma150_config.sql. Safe to re-run (idempotent DDL).

CREATE TABLE IF NOT EXISTS public.signal_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Link back to the signal that produced this outcome.
  signal_id UUID NOT NULL REFERENCES public.signals(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  pattern_code TEXT,                          -- strategy / pattern that fired

  side TEXT NOT NULL DEFAULT 'LONG'           -- 'LONG' | 'SHORT'
    CHECK (side IN ('LONG', 'SHORT')),

  signal_timestamp TIMESTAMPTZ NOT NULL,      -- when the signal was decided

  -- Trade reference levels (nullable; not all strategies define them).
  entry_price NUMERIC,
  stop_price NUMERIC,
  target_price NUMERIC,
  invalidation NUMERIC,

  -- Signal forward returns per holding window, side-adjusted, in PERCENT.
  -- NULL when there were not enough future bars to fill that window.
  ret_1d NUMERIC,
  ret_3d NUMERIC,
  ret_5d NUMERIC,
  ret_10d NUMERIC,
  ret_20d NUMERIC,

  -- Baselines, per-window maps in PERCENT, e.g. {"1D": 0.4, "3D": 1.1, ...}.
  --   benchmark_returns: {"SPY": {"1D":..}, "QQQ": {"1D":..}}
  --   same_ticker_buy_hold: {"1D":.., "3D":.., ...} (naive LONG hold)
  benchmark_returns JSONB,
  same_ticker_buy_hold JSONB,

  -- Excursions over the evaluated window, in PERCENT.
  max_favorable_excursion NUMERIC,            -- best unrealized move (>=0 typical)
  max_adverse_excursion NUMERIC,              -- worst unrealized move (<=0 typical)

  -- Stop / target evaluation (NULL when stop/target not defined).
  hit_stop BOOLEAN,
  hit_target BOOLEAN,
  simulated_r NUMERIC,                        -- simplified R = end-of-window ret / initial risk

  outcome_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (outcome_status IN ('pending', 'calculated', 'insufficient_data', 'error')),
  calculation_version TEXT NOT NULL DEFAULT 'outcome.v1',

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- One outcome row per signal. Recalculation upserts this row.
  UNIQUE (signal_id)
);

-- Aggregation / lookup indexes.
CREATE INDEX IF NOT EXISTS signal_outcomes_pattern_side_idx
  ON public.signal_outcomes (pattern_code, side);

CREATE INDEX IF NOT EXISTS signal_outcomes_status_idx
  ON public.signal_outcomes (outcome_status);

CREATE INDEX IF NOT EXISTS signal_outcomes_symbol_idx
  ON public.signal_outcomes (symbol);
