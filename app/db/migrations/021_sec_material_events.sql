-- ===========================================================================
-- 021 — SEC material events (SEC / 8-K Material Event Context V1)
-- ===========================================================================
-- Answers ONE product question: "what material corporate event was formally
-- disclosed, when did it become public, and what kind of event was it?"
--
-- It is NOT a filing archive, NOT a document search and NOT an interpretation
-- layer. SEC context sits BESIDE the strategy result: it cannot reach the
-- candidate verdict, the Wyckoff evaluation, the attention tier, the ordering
-- or ENTER eligibility, and no scanner table references it.
--
-- WHY THIS IS A SEPARATE DOMAIN FROM `company_news_articles` (020)
-- ----------------------------------------------------------------
-- A news article is something a publisher chose to write. An 8-K is something
-- a registrant was REQUIRED to file, under a numbered item, with a timestamped
-- public acceptance. They have different identities (URL vs accession number),
-- different truth conditions and different failure modes. Cramming filings into
-- the news table would destroy exactly the distinction this milestone exists to
-- create.
--
-- WHY THERE IS NO `sec_filing_items` TABLE
-- ----------------------------------------
-- Item codes are an ordered, bounded, immutable property OF a filing — never
-- an entity with a life of its own. `item_codes TEXT[]` preserves every code,
-- its order and its multiplicity, and Postgres indexes/aggregates it directly
-- (`@>`, `unnest`). A third table would add a join and a foreign key to store
-- the same facts. This is the smallest schema that loses nothing.
--
-- WHAT IS DELIBERATELY NOT STORED
-- -------------------------------
-- No filing text, no exhibit bodies, no summary, no interpretation, no
-- sentiment and no importance score. The product states which items were filed
-- and links to the source document. A reader who wants to know what a filing
-- says reads the filing.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.sec_filings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Provenance. `source` names the access path, not a vendor: this data comes
  -- from the SEC's own structured submissions endpoint, so there is no
  -- entitlement question and no intermediary to drift from.
  source TEXT NOT NULL DEFAULT 'sec_edgar_submissions',

  -- SEC-NATIVE IDENTITY. The accession number is assigned by EDGAR, is unique
  -- across all filings by all registrants forever, and is what makes a refresh
  -- idempotent without any heuristics.
  accession_number TEXT NOT NULL,
  cik TEXT NOT NULL,

  -- '8-K', '8-K/A', '8-K12B', ... Kept verbatim: an amendment is a different
  -- form and must never be presented as the original.
  form TEXT NOT NULL,

  -- THE POINT-IN-TIME GATE. `accepted_at` is EDGAR's acceptance timestamp —
  -- the moment the filing became public. It is the ONLY column that may decide
  -- what a historical scan session was allowed to see.
  accepted_at TIMESTAMPTZ NOT NULL,

  -- Context, never the gate:
  --   filing_date       the EDGAR filing date (a date, no clock)
  --   period_of_report  WHEN THE EVENT HAPPENED. Explicitly NOT a disclosure
  --                     timestamp: an event on the 27th disclosed on the 30th
  --                     was unknowable on the 29th, and using this column as
  --                     the gate is precisely the lookahead bug to avoid.
  filing_date DATE NOT NULL,
  period_of_report DATE,

  -- Original SEC item codes, in filing order, e.g. {'2.02','9.01'}. Source
  -- evidence: every derived label below can be recomputed from these.
  item_codes TEXT[] NOT NULL DEFAULT '{}',
  -- Deterministic event families derived from `item_codes` by a VERSIONED
  -- normalizer. Stored so the product reads one shape, alongside the version
  -- that produced them so a taxonomy change is visible rather than silent.
  event_types TEXT[] NOT NULL DEFAULT '{}',
  taxonomy_version TEXT NOT NULL,

  -- Structural visibility, NOT importance. True when the filing carries at
  -- least one item that describes WHAT HAPPENED, as opposed to only items that
  -- describe how something was furnished (9.01 exhibits, 7.01 Reg FD).
  is_primary_event BOOLEAN NOT NULL,

  -- Amendment linkage. An 8-K/A NEVER overwrites its original: both rows
  -- survive and this column records the relationship where EDGAR expresses it.
  amends_accession_number TEXT,

  primary_document TEXT,
  filing_url TEXT NOT NULL,

  -- When WE first stored it. Drives freshness and audit only — never
  -- visibility (see `accepted_at`).
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Accession numbers are 18 digits in 10-2-6 form; anything else is not an
  -- EDGAR identity and must not be stored as one.
  CONSTRAINT sec_filings_accession_ck
    CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
  CONSTRAINT sec_filings_form_ck CHECK (form <> ''),
  CONSTRAINT sec_filings_cik_ck CHECK (cik ~ '^[0-9]{10}$'),

  -- Idempotency, from SEC's own identity. A re-run updates; it cannot duplicate.
  CONSTRAINT sec_filings_identity_unique UNIQUE (source, accession_number)
);

CREATE INDEX IF NOT EXISTS sec_filings_accepted_idx
  ON public.sec_filings (accepted_at DESC);
CREATE INDEX IF NOT EXISTS sec_filings_cik_accepted_idx
  ON public.sec_filings (cik, accepted_at DESC);
CREATE INDEX IF NOT EXISTS sec_filings_items_idx
  ON public.sec_filings USING GIN (item_codes);

-- ---------------------------------------------------------------------------
-- Issuer -> symbol association.
--
-- One CIK can carry several tickers (dual-class share structures such as
-- GOOGL/GOOG), so the relationship is genuinely many-to-one and cannot be a
-- column on the filing. It is also where the frozen scanner universe is
-- applied: filings are linked only to symbols the product actually scans.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.sec_filing_symbols (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filing_id UUID NOT NULL
    REFERENCES public.sec_filings(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  cik TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT sec_filing_symbols_unique UNIQUE (symbol, filing_id)
);

CREATE INDEX IF NOT EXISTS sec_filing_symbols_symbol_idx
  ON public.sec_filing_symbols (symbol);

-- ---------------------------------------------------------------------------
-- Freshness reuses `catalyst_source_state` (019) with a fourth source row,
-- `sec_edgar_8k`. Deliberately NOT a new table: the Product API already
-- batch-loads that one relation, and "when did this source last succeed, and
-- is it reachable at all" is the same question for every catalyst source. An
-- empty filings table must never be readable as "no company filed anything".
-- ---------------------------------------------------------------------------

-- RLS: enabled on every new table (policies are created by ops/sql per role).
ALTER TABLE public.sec_filings        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.sec_filing_symbols ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Operating model: declare the refresh in the DISABLED daily-pipeline
-- template, after the news refresh.
--
-- `stages` on that template is a documentation list of intended daily steps,
-- not executable dispatch, and the schedule stays disabled here. Migrations
-- 018, 019 and 020 are left untouched — this migration owns its own change.
--
-- Placement is deliberate: SEC sits AFTER the campaign so it can never delay a
-- scan, and after news so that neither dimension can cost the other its
-- refresh. `refresh_sec_filings` absorbs a source failure into
-- `catalyst_source_state` instead of raising.
--
-- Cadence: an 8-K is a durable, dated disclosure rather than a stream, and the
-- product's smallest window is a whole trading session — so ONE refresh per
-- daily pipeline run is sufficient for V1, and no new scheduler is introduced.
-- The one behaviour a daily cadence does not cover is an intra-session 8-K
-- appearing on the SAME session it was accepted; that is documented, not
-- silently ignored.
--
-- It must run on a component that can reach the SEC endpoint with a declared
-- User-Agent (the history-warmup worker). The Product API only ever READS.
-- ---------------------------------------------------------------------------
UPDATE public.job_schedules
SET payload_template = jsonb_set(
      payload_template, '{stages}',
      '["history_universe_refresh.v1","readiness_verification",
        "prospective_daily_campaign.v1","catalyst_refresh.v1",
        "company_news_refresh.v1","sec_filings_refresh.v1",
        "outcome_maturation.v1","daily_quality_audit.v1"]'::jsonb)
WHERE schedule_code = 'SMART-SCANNER-DAILY-PIPELINE'
  AND schedule_version = 1
  AND NOT (payload_template -> 'stages' ? 'sec_filings_refresh.v1');
