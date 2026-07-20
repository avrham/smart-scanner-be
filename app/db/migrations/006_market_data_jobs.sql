-- Phase 7A: durable market-data jobs.
--
-- Additive and idempotent (safe to run more than once). Run manually in the
-- Supabase SQL editor after 005_massive_provider.sql.
--
-- Purpose: market-data operations (initially market-cap enrichment) become
-- durable jobs with queued/running/completed/failed/cancelled states instead
-- of fire-and-forget background tasks. The partial unique index provides
-- DURABLE duplicate protection: at most one active (queued or running) job
-- per (job_type, provider, trading_date) — enforced by the database, not an
-- in-memory lock.
--
-- Safety notes:
--  * error stores a sanitized message only (no tracebacks, no secrets).
--  * selected_symbols and progress are bounded by the application (<= 25
--    symbols, counter-only progress).
--  * job_type is TEXT (not an enum) so future job types (daily_sync,
--    universe_sync, historical_backfill) need no schema change.

CREATE TABLE IF NOT EXISTS public.market_data_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
  provider TEXT NOT NULL,
  -- Unique indexes treat NULLs as distinct, so a NULL trading_date would
  -- bypass the active-job duplicate protection below. Enrichment jobs must
  -- always carry a resolved date (also enforced in application code).
  trading_date DATE
    CHECK (job_type <> 'market_cap_enrichment' OR trading_date IS NOT NULL),
  requested_limit INT,
  selection_strategy TEXT,
  selected_symbols JSONB,
  progress JSONB,
  result JSONB,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable duplicate-job protection: one active job per type/provider/date.
CREATE UNIQUE INDEX IF NOT EXISTS market_data_jobs_active_uniq
ON public.market_data_jobs (job_type, provider, trading_date)
WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS market_data_jobs_recent_idx
ON public.market_data_jobs (job_type, created_at DESC);

CREATE INDEX IF NOT EXISTS market_data_jobs_status_idx
ON public.market_data_jobs (status, updated_at DESC);
