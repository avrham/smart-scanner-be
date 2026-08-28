-- ===========================================================================
-- 023 — External discovery (market-wide movers)
-- ===========================================================================
-- Answers ONE product question, and it is NOT the one migration 022 answers:
-- "what is the wider market paying attention to that our frozen 25-symbol
-- universe would never show us?"
--
-- WHY THIS IS A DIFFERENT DOMAIN FROM `external_signals` (022)
-- ------------------------------------------------------------
-- An external SIGNAL is a claim about a symbol — an indicator saying "bullish
-- on the 4H". A MOVER is not a claim; it is a measurement anyone with the
-- whole tape can make, and the only reason we cannot make it ourselves is that
-- we deliberately hold 25 symbols rather than 8,000. Storing a ranked
-- observation beside an opinion would blur exactly that distinction, and the
-- confluence reading in 022 would start counting "it moved a lot" as a source
-- agreeing with the scanner.
--
-- So this is a separate table, it is NOT part of the external_intelligence
-- product block, and it feeds no confluence reading.
--
-- WHAT THIS IS FOR
-- ----------------
-- Research, and specifically the Phase 20 boundary: the frozen experimental
-- universe must not be contaminated, but "what else should we be looking at?"
-- is a real question that a 25-symbol scanner structurally cannot answer.
-- These rows are the answer, kept where they cannot leak into the experiment.
--
-- WHY IT IS NOT SURFACED IN THE PRODUCT API OR THE UI
-- ---------------------------------------------------
-- Measured, not assumed. The provider's individual plans are licensed for
-- personal, non-commercial use and forbid integrating the data into tools
-- accessible by third parties; the pricing page separately requires a Data
-- Display and Licensing Agreement to display or redistribute. Displaying these
-- rows in the Product API or the UI is precisely the use that licence
-- excludes. Ingesting them for our own research is not, so the data path stops
-- at the database and at ops/analysis. If a display licence is ever acquired,
-- surfacing them is an additive change to the Product API and nothing here has
-- to move.
--
-- MEASURED ENTITLEMENT (2026-08-28, against the live key)
-- --------------------------------------------------------
--   /api/v3/*                      HTTP 403  "Legacy Endpoint" — the base URL
--                                  this repository's existing FMP provider
--                                  still uses is DEAD.
--   /stable/biggest-gainers        HTTP 200  free tier
--   /stable/biggest-losers         HTTP 200  free tier
--   /stable/most-actives           HTTP 200  free tier
--   /stable/company-screener       HTTP 402  requires a paid tier
--
-- The screener is therefore NOT part of this migration. Only the three feeds
-- that actually answer on the current entitlement are modelled.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.external_discovery_candidates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),

  --   top_gainers   largest positive move
  --   top_losers    largest negative move
  --   most_active   largest traded volume
  --
  -- Kept as three separate lists rather than merged into one "attention"
  -- table. They are different measurements and a symbol can be on more than
  -- one; collapsing them would lose which one it was on.
  list_kind TEXT NOT NULL,

  symbol TEXT NOT NULL,
  company_name TEXT,
  exchange TEXT,

  -- The provider's own ordering, 1-based. Stored because the RANK is the
  -- information — "AAPL was 3rd most active" says something "AAPL appeared"
  -- does not.
  rank INTEGER NOT NULL,

  price NUMERIC,
  change_amount NUMERIC,
  change_percent NUMERIC,

  -- THE POINT-IN-TIME GATE. When WE fetched the list. There is no provider
  -- timestamp on these feeds, so unlike a filing or an article there is no
  -- authority-asserted moment — the fetch clock is the only honest one, and
  -- pretending otherwise would invent precision.
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- The trading session that fetch belongs to, by the same 16:00 America/
  -- New_York clock every other layer here uses.
  session_date DATE NOT NULL,

  -- THE EXPERIMENT BOUNDARY, same vocabulary as migration 022. A mover that
  -- happens to be one of our 25 is interesting context; a mover outside them
  -- is the entire point of this table. Neither may enter the experiment.
  in_scanner_universe BOOLEAN NOT NULL DEFAULT false,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT external_discovery_list_ck
    CHECK (list_kind IN ('top_gainers', 'top_losers', 'most_active')),
  CONSTRAINT external_discovery_symbol_ck
    CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,15}$'),
  CONSTRAINT external_discovery_rank_ck CHECK (rank >= 1),

  -- Idempotency. One row per (source, list, symbol, session): a second fetch
  -- on the same session updates the rank rather than inserting a duplicate,
  -- and the intraday churn of a movers list never becomes row growth.
  CONSTRAINT external_discovery_identity_unique
    UNIQUE (source, list_kind, symbol, session_date)
);

CREATE INDEX IF NOT EXISTS external_discovery_session_idx
  ON public.external_discovery_candidates (session_date DESC, list_kind, rank);
-- The research read: "which symbols keep appearing that we never scan?"
CREATE INDEX IF NOT EXISTS external_discovery_outside_idx
  ON public.external_discovery_candidates (symbol, session_date DESC)
  WHERE in_scanner_universe = false;

ALTER TABLE public.external_discovery_candidates ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Freshness reuses `catalyst_source_state` with an `external_fmp_discovery`
-- row, for the same reason every source since 019 has: an empty table must
-- never be readable as "the market was quiet".
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Operating model: declared in the DISABLED daily-pipeline template, after the
-- SEC refresh.
--
-- Unlike migration 022 — whose data is PUSHED and therefore has no refresh
-- stage at all — this one is genuinely pulled on our schedule, so it belongs
-- in the pipeline like the earnings, news and SEC refreshes before it.
--
-- Placement is deliberate: last, so it can never delay a scan or cost another
-- dimension its refresh, and `refresh_discovery_candidates` absorbs a provider
-- failure into `catalyst_source_state` instead of raising. The worst case is
-- the product truthfully reporting discovery as unavailable — including when
-- no FMP key is configured at all, which is the default.
--
-- Cadence: once per daily pipeline run. A movers list churns intraday, and
-- this deliberately does NOT chase that: the product's smallest window is a
-- whole trading session, and a once-a-day snapshot answers "what was the
-- market watching today" without pretending to be a live feed.
-- ---------------------------------------------------------------------------
UPDATE public.job_schedules
SET payload_template = jsonb_set(
      payload_template, '{stages}',
      '["history_universe_refresh.v1","readiness_verification",
        "prospective_daily_campaign.v1","catalyst_refresh.v1",
        "company_news_refresh.v1","sec_filings_refresh.v1",
        "external_discovery_refresh.v1",
        "outcome_maturation.v1","daily_quality_audit.v1"]'::jsonb)
WHERE schedule_code = 'SMART-SCANNER-DAILY-PIPELINE'
  AND schedule_version = 1
  AND NOT (payload_template -> 'stages' ? 'external_discovery_refresh.v1');
