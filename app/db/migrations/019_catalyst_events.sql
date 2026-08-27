-- ===========================================================================
-- 019 — Corporate catalyst events (Earnings Catalyst Context V1)
-- ===========================================================================
-- The SMALLEST model that answers "is there a known corporate event that
-- changes how I should read this setup?". Deliberately NOT a generic events
-- platform: one narrow table for dated, per-symbol corporate events, plus a
-- single-row-per-source freshness marker.
--
-- Catalyst data is CONTEXT. Nothing here feeds the Wyckoff evaluation, the
-- candidate verdict or the attention tier, and no scanner table references it.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.symbol_catalyst_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,

  -- What kind of dated corporate event this row records.
  --   'earnings'                 an earnings announcement (calendar event)
  --   'financial_report_filing'  the date a periodic financial report was FILED
  -- These are NOT interchangeable and must never be presented as one another.
  event_type TEXT NOT NULL,

  event_date DATE NOT NULL,

  -- Session semantics. 'unknown' is a first-class value: never guess.
  --   before_market | after_market | during_market | unknown
  session_timing TEXT NOT NULL DEFAULT 'unknown',

  -- How much the date can be trusted.
  --   confirmed  the company/provider confirmed this date
  --   estimated  a projected date that can still move
  --   filed      an already-occurred filing with a recorded date (a fact)
  certainty TEXT NOT NULL,

  fiscal_period TEXT,
  fiscal_year TEXT,

  -- Provenance. `source` names the feed; `source_reference` is a stable
  -- pointer back to the underlying document/record where one exists.
  source TEXT NOT NULL,
  source_reference TEXT,

  -- When WE observed this record. Drives staleness and point-in-time answers.
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT symbol_catalyst_events_type_ck
    CHECK (event_type IN ('earnings', 'financial_report_filing')),
  CONSTRAINT symbol_catalyst_events_timing_ck
    CHECK (session_timing IN ('before_market', 'after_market', 'during_market', 'unknown')),
  CONSTRAINT symbol_catalyst_events_certainty_ck
    CHECK (certainty IN ('confirmed', 'estimated', 'filed')),

  -- Idempotent upsert key: re-running ingestion updates a row instead of
  -- duplicating it. A RESCHEDULED event arrives as a new date for the same
  -- (symbol, type, period) and is handled by the ingestion layer, which
  -- supersedes the prior date rather than leaving two futures on the board.
  CONSTRAINT symbol_catalyst_events_unique
    UNIQUE (symbol, event_type, event_date)
);

CREATE INDEX IF NOT EXISTS symbol_catalyst_events_symbol_date_idx
  ON public.symbol_catalyst_events (symbol, event_date DESC);

CREATE INDEX IF NOT EXISTS symbol_catalyst_events_type_date_idx
  ON public.symbol_catalyst_events (event_type, event_date DESC);

-- ---------------------------------------------------------------------------
-- Freshness / availability marker — one row per source.
--
-- This is what lets the product tell "we refreshed and there is genuinely no
-- upcoming event" apart from "we have never refreshed" and from "the source is
-- unavailable to us". Without it, an empty table is ambiguous.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.catalyst_source_state (
  source TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  last_refresh_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  symbols_covered INTEGER NOT NULL DEFAULT 0,
  events_upserted INTEGER NOT NULL DEFAULT 0,
  detail TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT catalyst_source_state_status_ck
    CHECK (status IN ('ok', 'unavailable', 'error', 'never_run'))
);

-- RLS: enabled on every new table (policies are created by ops/sql per role).
ALTER TABLE public.symbol_catalyst_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.catalyst_source_state ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Operating model: declare the refresh in the DISABLED daily-pipeline template.
--
-- `stages` on that template is a documentation list of intended daily steps,
-- not executable dispatch, and the schedule stays disabled here. Migration 018
-- is left untouched — this migration owns its own change.
--
-- Ordering is deliberate: the catalyst refresh sits AFTER the campaign so it
-- can never delay or block a scan. It needs no price history, produces nothing
-- the scan reads, and `refresh_catalysts` already absorbs a provider failure
-- into `catalyst_source_state` instead of raising — so the worst case is that
-- the product truthfully reports the calendar as unavailable.
--
-- It must run on the component that already holds the provider credential (the
-- history-warmup worker). The Product API holds none and only ever READS the
-- persisted tables above.
-- ---------------------------------------------------------------------------
UPDATE public.job_schedules
SET payload_template = jsonb_set(
      payload_template, '{stages}',
      '["history_universe_refresh.v1","readiness_verification",
        "prospective_daily_campaign.v1","catalyst_refresh.v1",
        "outcome_maturation.v1","daily_quality_audit.v1"]'::jsonb)
WHERE schedule_code = 'SMART-SCANNER-DAILY-PIPELINE'
  AND schedule_version = 1
  AND NOT (payload_template -> 'stages' ? 'catalyst_refresh.v1');
