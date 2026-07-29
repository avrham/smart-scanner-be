-- ===========================================================================
-- 015_history_warmup_run_items.sql — per-symbol history-warmup execution items
-- ===========================================================================
-- FOUNDATION/EXECUTE support. Additive: ONE new table + indexes. Touches no
-- existing table, no existing row, no campaign/evaluation/outcome/pair/4H/daily
-- data, no role here (grants + RLS policies live in
-- ops/sql/create_shadow_history_warmer*.sql, owner-run, exactly like the audit /
-- outcome-maintainer / 4H tables). No provider execution. No strategy change.
--
-- `history_warmup_runs` (migration 014) is the per-EXECUTE-request run row (one
-- per bounded batch; carries idempotency_key, cooldown, hashes, status). This
-- table records ONE row per (run, symbol, attempt) — the per-symbol execution
-- item — with fully SCALAR, bounded columns: no arrays, no unbounded JSON, no
-- raw provider payload, no secret. It is the durable retry-plan + telemetry
-- source for the bounded warmup execute path.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.history_warmup_run_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES public.history_warmup_runs(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  attempt INT NOT NULL DEFAULT 1,
  mode TEXT NOT NULL DEFAULT 'normal' CHECK (mode IN ('normal', 'retry')),
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'completed', 'failed')),
  daily_status TEXT
    CHECK (daily_status IS NULL OR daily_status IN ('pending', 'completed', 'failed', 'skipped')),
  four_hour_status TEXT
    CHECK (four_hour_status IS NULL OR four_hour_status IN ('pending', 'completed', 'failed', 'skipped')),
  daily_rows_inserted INT NOT NULL DEFAULT 0,
  daily_rows_updated INT NOT NULL DEFAULT 0,
  daily_rows_unchanged INT NOT NULL DEFAULT 0,
  four_hour_rows_inserted INT NOT NULL DEFAULT 0,
  four_hour_rows_updated INT NOT NULL DEFAULT 0,
  four_hour_rows_unchanged INT NOT NULL DEFAULT 0,
  provider_request_count INT NOT NULL DEFAULT 0,
  -- bounded safe error identity + classification; NEVER a raw trace/payload
  error_code TEXT,
  error_class TEXT
    CHECK (error_class IS NULL OR error_class IN ('retryable', 'terminal', 'operator_error')),
  retryable BOOLEAN NOT NULL DEFAULT FALSE,
  -- deterministic idempotency identity of the owning execute request (also on
  -- history_warmup_runs.idempotency_key); stored here for item-level audit.
  execution_identity TEXT NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT history_warmup_run_items_symbol_upper CHECK (symbol = upper(symbol)),
  CONSTRAINT history_warmup_run_items_attempt_pos CHECK (attempt >= 1),
  CONSTRAINT history_warmup_run_items_counts_nonneg CHECK (
    daily_rows_inserted >= 0 AND daily_rows_updated >= 0 AND daily_rows_unchanged >= 0
    AND four_hour_rows_inserted >= 0 AND four_hour_rows_updated >= 0
    AND four_hour_rows_unchanged >= 0 AND provider_request_count >= 0
  ),
  -- one row per run + symbol + attempt (the execution identity within a run)
  CONSTRAINT history_warmup_run_items_identity UNIQUE (run_id, symbol, attempt)
);

-- current retry plan: latest item per symbol + retryable filter
CREATE INDEX IF NOT EXISTS history_warmup_run_items_symbol_created_idx
  ON public.history_warmup_run_items (symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS history_warmup_run_items_retryable_idx
  ON public.history_warmup_run_items (retryable, status);
-- run lookup
CREATE INDEX IF NOT EXISTS history_warmup_run_items_run_idx
  ON public.history_warmup_run_items (run_id);

-- Enable RLS (fail-closed; audit-reader SELECT + history-warmer
-- SELECT/INSERT/UPDATE policies are created by the ops/sql policy script,
-- owner-run, mirroring the 4H tables). No DELETE policy for anyone.
ALTER TABLE public.history_warmup_run_items ENABLE ROW LEVEL SECURITY;
