-- ===========================================================================
-- 024 — External Intelligence Wave 2
--       market calendar context, analyst change events, registry V2
-- ===========================================================================
-- Wave 1 answered "what did another system CLAIM about this symbol". Wave 2
-- answers three questions it structurally could not:
--
--   * what SCHEDULED market-wide event is approaching?          (macro_events)
--   * who changed their mind about a company we hold?    (analyst_grade_events)
--   * which of our sources may we actually SHOW?                 (registry V2)
--
-- WHY THREE CONCEPTS AND NOT ONE TABLE
-- ------------------------------------
-- It would have been less work to widen `external_signals` three times. It
-- would also have been wrong, and in a way that is expensive to undo later.
--
--   A SIGNAL is an opinion about a price series. It has a direction, it can
--   agree or disagree with our own reading, and it is the only one of the
--   three that belongs in a confluence sentence.
--
--   A SCHEDULED EVENT is a fact about the calendar. It has no direction, no
--   symbol, and no opinion. "FOMC on Wednesday" is true for every row on the
--   screen at once, which is exactly why it is not a per-symbol field.
--
--   A GRADE CHANGE is a third party's published action on one company. It has
--   a subject and a date but no timeframe, no price level, and no view of the
--   chart we are looking at.
--
-- Folding any of these into `external_signals` would let a calendar entry
-- borrow a signal's semantics — a macro event would acquire a `direction` it
-- does not have, and "CPI is Thursday" would start counting as a source
-- agreeing with the scanner. Separate tables keep that impossible rather than
-- merely discouraged.
--
-- WHAT IS DELIBERATELY NOT MODELLED
-- ---------------------------------
-- No consensus/actual/previous columns on `macro_events`. Neither selected
-- source publishes them, and a NULL column reads as "the fetch failed" rather
-- than "this source does not have this". When a source that carries consensus
-- is acquired, adding the columns is additive and honest; adding them now
-- would be coverage theatre.
--
-- No macro score, no risk level, no bullish/bearish. The proximity vocabulary
-- (today / tomorrow / within_3_days / recently_released / none_nearby /
-- unavailable) is computed in `app/macro_calendar.py` from the calendar alone.
--
-- WHAT WAS MEASURED, 2026-08-30, AND WHAT IT RULED OUT
-- ---------------------------------------------------
--   federalreserve.gov FOMC calendar     200  -> implemented
--   bea.gov/news/schedule                200  -> implemented
--   www.bls.gov  (ANY path, incl. robots.txt, from two clients)
--                                        403  -> CPI / PPI / nonfarm payrolls
--                                                / unemployment rate CANNOT be
--                                                scheduled from the primary
--                                                source. Not modelled.
--   FMP /stable/economic-calendar        402  -> paid tier; not a fallback we
--                                                may display in any case.
--   FMP /stable/grades?symbol=           200  -> implemented (full history)
--   FMP /stable/price-target-news        402  -> per-symbol targets not
--                                                entitled; the market-wide
--                                                feed caps `limit` at 10, so
--                                                covering 25 symbols from it
--                                                is not possible either.
--
-- LICENSING IS NOW A COLUMN, NOT A CONVENTION
-- -------------------------------------------
-- Wave 1 kept FMP out of the product by never wiring it up. That holds exactly
-- until a second restricted source arrives — and Wave 2 adds one that is far
-- more tempting to display. So the position becomes data: every registry row
-- declares `licensing_visibility`, `app/source_licensing.py` states the same
-- answer in code for when the row is unreachable, and the Product API's
-- database role is granted nothing on the internal-only relations.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1) EXTERNAL SOURCE REGISTRY V2
--
-- Three new capability/licence columns. Additive and defaulted, so a running
-- Product API on the previous code keeps working through the migration.
--
-- `supports_symbol_scan` is left exactly as it is. It already means "can be
-- asked about a symbol we name", which is a different question from "can hand
-- us symbols we do not hold" (supports_discovery) and from "publishes a
-- forward schedule" (supports_calendar).
-- ---------------------------------------------------------------------------
ALTER TABLE public.external_signal_sources
  ADD COLUMN IF NOT EXISTS supports_discovery BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE public.external_signal_sources
  ADD COLUMN IF NOT EXISTS supports_calendar BOOLEAN NOT NULL DEFAULT false;

-- Defaulted to the CLOSED answer on purpose: a source added later and never
-- classified is not displayable until somebody says so in writing.
ALTER TABLE public.external_signal_sources
  ADD COLUMN IF NOT EXISTS licensing_visibility TEXT NOT NULL
    DEFAULT 'unknown_restriction';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'external_signal_sources_licensing_ck') THEN
    ALTER TABLE public.external_signal_sources
      ADD CONSTRAINT external_signal_sources_licensing_ck
      CHECK (licensing_visibility IN ('product_display_allowed',
                                      'internal_research_only',
                                      'unknown_restriction'));
  END IF;
END
$$;

-- The two calendar publishers are federal agencies rather than vendors, so the
-- registry needs a transport and a status vocabulary that already fits: they
-- are pulled over HTTP like any other API source, and they are `live`.
INSERT INTO public.external_signal_sources (
  source, display_name, transports, supports_realtime, supports_historical,
  supports_symbol_scan, supports_signal_events, supports_discovery,
  supports_calendar, emits_signals, requires_paid_plan, licensing_visibility,
  status, notes)
VALUES
  ('federal_reserve', 'Federal Reserve Board (FOMC calendar)',
   ARRAY['api'], false, true, false, false, false, true, false, false,
   'product_display_allowed', 'live',
   'MEASURED 2026-08-30: federalreserve.gov/monetarypolicy/fomccalendars.htm '
   'returns 200 and publishes the FOMC meeting calendar for the current and '
   'following year as structured markup — month, day range, and links that '
   'reveal whether a meeting carries a press conference and a Summary of '
   'Economic Projections. No credential, no plan, no rate limit encountered. '
   'Works of the U.S. Government are not subject to copyright protection in '
   'the United States (17 U.S.C. 105), so this is displayable. It publishes a '
   'SCHEDULE only: no consensus, no actual, no previous, and none is invented.'),
  ('bea', 'Bureau of Economic Analysis (release schedule)',
   ARRAY['api'], false, false, false, false, false, true, false, false,
   'product_display_allowed', 'live',
   'MEASURED 2026-08-30: bea.gov/news/schedule returns 200 with a single '
   'forward-looking release table carrying date, release time (08:30 ET) and '
   'the release title. Two of its releases matter to broad US equities and '
   'only those two are modelled: GDP, and Personal Income and Outlays (the '
   'PCE price index release). Public U.S. Government information, displayable. '
   'FORWARD ONLY: the page does not list past releases, so history accrues '
   'from our own point-in-time snapshots rather than from a backfill.')
ON CONFLICT (source) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  transports = EXCLUDED.transports,
  supports_calendar = EXCLUDED.supports_calendar,
  licensing_visibility = EXCLUDED.licensing_visibility,
  status = EXCLUDED.status,
  notes = EXCLUDED.notes,
  updated_at = NOW();

-- Classify every pre-existing row. These are the MEASURED positions, and the
-- same values `app/source_licensing.py` carries.
UPDATE public.external_signal_sources SET
  licensing_visibility = 'product_display_allowed', updated_at = NOW()
WHERE source IN ('tradingview', 'ai_edge');

UPDATE public.external_signal_sources SET
  licensing_visibility = 'internal_research_only', updated_at = NOW()
WHERE source IN ('fmp', 'trendspider');

UPDATE public.external_signal_sources SET
  licensing_visibility = 'unknown_restriction', updated_at = NOW()
WHERE source IN ('finviz', 'koyfin', 'openbb');

-- FMP is no longer "available, not integrated": two bounded pull paths run
-- against it. Its capability flags and status are corrected to match reality,
-- and its licence class keeps it out of the product regardless.
UPDATE public.external_signal_sources SET
  status = 'live',
  supports_discovery = true,
  supports_symbol_scan = true,
  supports_historical = true,
  supports_signal_events = false,
  emits_signals = false,
  notes =
    'MEASURED against the live key on 2026-08-30. /api/v3 is DEAD (403 Legacy '
    'Endpoint); /stable is current. ENTITLED on this plan: biggest-gainers, '
    'biggest-losers, most-actives, grades (per-symbol upgrade/downgrade '
    'history, complete back to 2012), grades-consensus, price-target-summary '
    'and -consensus, analyst-estimates, ratios/key-metrics/financial-growth, '
    'profile, holidays-by-exchange, treasury-rates. NOT ENTITLED (HTTP 402): '
    'company-screener, stock-list, price-target-news (per-symbol), '
    'economic-calendar; the market-wide price-target feed caps limit at 10, '
    'which is why per-symbol price-target CHANGE events are not modelled. Two '
    'bounded pulls are live: market movers -> external_discovery_candidates, '
    'analyst grade changes -> analyst_grade_events. NEITHER is displayable: '
    'individual FMP plans are personal and non-commercial and forbid '
    'integrating the data into tools accessible by third parties, so both '
    'paths stop at the database and the Product API role is granted nothing '
    'on either table. This is a licence boundary, not a technical one — '
    'paraphrasing the values into a product field would breach it just the '
    'same.',
  updated_at = NOW()
WHERE source = 'fmp';


-- ---------------------------------------------------------------------------
-- 2) MACRO EVENTS — the scheduled, market-wide calendar
--
-- No symbol column, and that is the design rather than an omission. A macro
-- event is true for the whole screen; giving it a symbol would invite it to be
-- rendered as a per-row chip on 25 rows, which is 25 copies of one fact and
-- reads as 25 findings.
--
-- Every row is what a SOURCE PUBLISHED, when we read it. `observed_at` is our
-- read; `first_observed_at` is the first time we ever saw this event, which is
-- what makes "the Fed moved this meeting" visible rather than silent.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.macro_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),

  -- Only the types we actually ingest are permitted. Listing an event type we
  -- cannot populate would read as coverage; the blocked ones are named in the
  -- header comment instead, where they cost nothing and mislead nobody.
  event_type TEXT NOT NULL,

  -- The source's own words, kept verbatim. It is the provenance a reader needs
  -- to check us, and it carries the reference period ("August 2026") that our
  -- normalised type deliberately drops.
  title TEXT NOT NULL,

  scheduled_date DATE NOT NULL,
  -- FOMC meetings run two days and the decision lands on the second. The start
  -- is kept because "the meeting begins today" is a different, real fact.
  scheduled_start_date DATE,
  -- NULL is the honest answer when a source publishes a date and no clock.
  scheduled_time_local TIME,
  scheduled_timezone TEXT NOT NULL DEFAULT 'America/New_York',

  -- Whether the SOURCE still lists this event. Neither publisher exposes a
  -- cancellation flag, so a schedule entry that stops being listed while still
  -- in the future is recorded as `withdrawn` — an honest "the source no longer
  -- says this", never a claim that the event was cancelled.
  source_listing TEXT NOT NULL DEFAULT 'listed',

  -- Only ever set from a source that states them. NULL means "not published",
  -- which for a future FOMC meeting is the normal case.
  has_press_conference BOOLEAN,
  has_projections BOOLEAN,

  source_reference TEXT NOT NULL,
  source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  first_observed_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT macro_events_type_ck
    CHECK (event_type IN ('fomc_rate_decision', 'gdp', 'pce')),
  CONSTRAINT macro_events_listing_ck
    CHECK (source_listing IN ('listed', 'withdrawn')),
  CONSTRAINT macro_events_span_ck
    CHECK (scheduled_start_date IS NULL
           OR scheduled_start_date <= scheduled_date),
  -- One event of one type per source per date. A source that published two
  -- would be telling us something we do not currently model, and failing the
  -- write is better than silently keeping whichever arrived last.
  CONSTRAINT macro_events_identity_unique
    UNIQUE (source, event_type, scheduled_date)
);

CREATE INDEX IF NOT EXISTS macro_events_schedule_idx
  ON public.macro_events (scheduled_date, event_type);

CREATE INDEX IF NOT EXISTS macro_events_listed_idx
  ON public.macro_events (scheduled_date)
  WHERE source_listing = 'listed';


-- ---------------------------------------------------------------------------
-- 3) ANALYST GRADE CHANGE EVENTS — internal research only
--
-- A change event, not a state. "Upgraded to Buy at Jefferies on 2026-08-10" is
-- information; "consensus is Buy" is a number that has been true for months
-- and tells a scanner nothing about today.
--
-- POINT IN TIME, AND WHY IT IS CONSERVATIVE
-- The provider publishes a DATE and no clock. We cannot tell whether a grade
-- landed before the open or after the close, so `session_date` is the FIRST
-- SESSION STRICTLY AFTER the event date. That deliberately gives up a day of
-- edge in exchange for never manufacturing one: any measurement built on these
-- rows is then immune to intraday timing we do not have.
--
-- `licensing_visibility` is stored per row rather than only on the registry
-- because it records the class AT INGESTION. If a display licence is ever
-- acquired, the rows collected under the old terms are still identifiable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.analyst_grade_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),
  symbol TEXT NOT NULL,

  event_date DATE NOT NULL,
  session_date DATE NOT NULL,

  grading_company TEXT NOT NULL,
  previous_grade TEXT,
  new_grade TEXT,

  -- The provider's own word, then ours. Both, for the same reason every other
  -- normalisation here keeps both: a vocabulary change upstream must be
  -- visible rather than silently remapped.
  action TEXT NOT NULL,
  action_normalized TEXT NOT NULL,

  in_scanner_universe BOOLEAN NOT NULL DEFAULT false,
  licensing_visibility TEXT NOT NULL DEFAULT 'internal_research_only',

  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT analyst_grade_events_symbol_ck
    CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,15}$'),
  CONSTRAINT analyst_grade_events_action_ck
    CHECK (action_normalized IN ('upgrade', 'downgrade', 'maintain',
                                 'initialise', 'other')),
  CONSTRAINT analyst_grade_events_licensing_ck
    CHECK (licensing_visibility IN ('product_display_allowed',
                                    'internal_research_only',
                                    'unknown_restriction')),
  CONSTRAINT analyst_grade_events_session_ck
    CHECK (session_date > event_date)
);

-- Identity has to include the grades themselves: the same firm can act twice
-- on the same symbol on the same day (an initiation and a target change reach
-- this feed as separate rows), and COALESCE keeps a NULL previous grade — the
-- normal shape of an initiation — from defeating the uniqueness.
CREATE UNIQUE INDEX IF NOT EXISTS analyst_grade_events_identity_unique
  ON public.analyst_grade_events (
    source, symbol, event_date, grading_company, action,
    COALESCE(previous_grade, ''), COALESCE(new_grade, ''));

CREATE INDEX IF NOT EXISTS analyst_grade_events_symbol_session_idx
  ON public.analyst_grade_events (symbol, session_date DESC);

CREATE INDEX IF NOT EXISTS analyst_grade_events_universe_idx
  ON public.analyst_grade_events (session_date DESC)
  WHERE in_scanner_universe = true;


-- ---------------------------------------------------------------------------
-- 4) DISCOVERY MODEL, EXTENDED (A2/A3)
--
-- The existing table already carried source, symbol, reason (`list_kind`),
-- rank, point-in-time provenance and the universe boundary. Two things were
-- missing: the raw source payload we based a row on, and the licence class
-- that applied when we took it.
-- ---------------------------------------------------------------------------
ALTER TABLE public.external_discovery_candidates
  ADD COLUMN IF NOT EXISTS source_metadata JSONB NOT NULL
    DEFAULT '{}'::jsonb;

ALTER TABLE public.external_discovery_candidates
  ADD COLUMN IF NOT EXISTS licensing_visibility TEXT NOT NULL
    DEFAULT 'internal_research_only';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'external_discovery_licensing_ck') THEN
    ALTER TABLE public.external_discovery_candidates
      ADD CONSTRAINT external_discovery_licensing_ck
      CHECK (licensing_visibility IN ('product_display_allowed',
                                      'internal_research_only',
                                      'unknown_restriction'));
  END IF;
END
$$;

-- A3 — the deterministic aggregation. One row per symbol per session, with
-- EVERY reason preserved as an array rather than collapsed into a count.
--
-- There is no score here and there will not be one. "CRM appeared in top
-- gainers and most actives" is a fact a reader can check; a number derived
-- from it is a claim we cannot support, and it would immediately become the
-- thing people sort on.
CREATE OR REPLACE VIEW public.external_discovery_current AS
SELECT
  session_date,
  symbol,
  max(company_name)                                    AS company_name,
  max(exchange)                                        AS exchange,
  bool_or(in_scanner_universe)                         AS in_scanner_universe,
  array_agg(DISTINCT list_kind ORDER BY list_kind)     AS reasons,
  count(DISTINCT list_kind)                            AS reason_count,
  min(rank)                                            AS best_rank,
  max(abs(COALESCE(change_percent, 0)))                AS max_abs_change_percent,
  max(observed_at)                                     AS observed_at,
  max(licensing_visibility)                            AS licensing_visibility
FROM public.external_discovery_candidates
GROUP BY session_date, symbol;


-- ---------------------------------------------------------------------------
-- 5) RLS on every new relation, as every migration here does. Policies are
--    created per role by ops/sql, never here.
-- ---------------------------------------------------------------------------
ALTER TABLE public.macro_events          ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.analyst_grade_events  ENABLE ROW LEVEL SECURITY;


-- ---------------------------------------------------------------------------
-- 6) Operating model: declare the two new refreshes in the DISABLED
--    daily-pipeline template.
--
--    `stages` is a documentation list of intended daily steps, not executable
--    dispatch, and the schedule stays disabled. Cadence, and why each differs:
--
--      macro_calendar_refresh.v1   daily. A published schedule changes rarely,
--                                  but the whole point is noticing the day it
--                                  does.
--      analyst_grades_refresh.v1   daily, after the campaign, because a grade
--                                  change is only actionable from the next
--                                  session anyway.
--      external_discovery_refresh  unchanged (023): market-session cadence.
--      ai_edge / tradingview       neither appears here at all — they are
--                                  event-driven webhooks and have no refresh.
-- ---------------------------------------------------------------------------
UPDATE public.job_schedules
SET payload_template = jsonb_set(
      payload_template, '{stages}',
      '["history_universe_refresh.v1","readiness_verification",
        "prospective_daily_campaign.v1","catalyst_refresh.v1",
        "company_news_refresh.v1","sec_filings_refresh.v1",
        "external_discovery_refresh.v1","macro_calendar_refresh.v1",
        "analyst_grades_refresh.v1",
        "outcome_maturation.v1","daily_quality_audit.v1"]'::jsonb)
WHERE schedule_code = 'SMART-SCANNER-DAILY-PIPELINE'
  AND schedule_version = 1
  AND NOT (payload_template -> 'stages' ? 'macro_calendar_refresh.v1');
