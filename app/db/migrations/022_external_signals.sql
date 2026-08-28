-- ===========================================================================
-- 022 — External intelligence (External Intelligence Hub V1)
-- ===========================================================================
-- Answers ONE product question: "what did a system OUTSIDE Smart Scanner
-- claim about this symbol, when did it claim it, and could we have known?"
--
-- This is the first domain in the repository whose data is PUSHED to us by a
-- third party over the public internet, rather than pulled by us from a
-- provider we chose. Every design decision below follows from that one fact.
--
-- WHY THIS IS NOT A CATALYST SOURCE
-- --------------------------------
-- 019/020/021 all answer "what HAPPENED to this company" — an earnings date,
-- a published article, a filed 8-K. Those are events in the world. An external
-- signal is not an event in the world: it is ANOTHER SYSTEM'S OPINION about
-- the same price series we already hold. Storing an opinion beside a
-- disclosure would let the opinion borrow the disclosure's authority. It is
-- therefore a sibling of `catalyst_context`, never a member of it, and the
-- Product API exposes it under its own `external_intelligence` block.
--
-- WHY THE GATE IS `effective_at`, AND WHY THAT EQUALS RECEIPT
-- -----------------------------------------------------------
-- Every other point-in-time gate in this repository trusts a timestamp
-- asserted by an authority: EDGAR's acceptance clock, a publisher's
-- publication clock. Here the timestamp arrives inside a payload posted by
-- whoever holds the ingress URL. A backdated `source_timestamp` — whether
-- malicious or merely a misconfigured chart — would manufacture lookahead
-- that looks exactly like a finding.
--
-- So the columns are split and only one of them may decide visibility:
--
--     observed_at    when the SOURCE says it fired      <- evidence, displayed
--     received_at    when it reached this server        <- provable
--     effective_at   the visibility gate                <- = received_at
--
-- We could not have acted on a signal before it arrived, whatever it claims.
-- `observed_at` is preserved verbatim so the claim stays inspectable, and
-- `clock_skew_seconds` records the disagreement rather than hiding it.
--
-- WHY THE SIGNAL TABLE IS APPEND-ONLY
-- -----------------------------------
-- A webhook can be delivered twice, can repeat a state, and can be corrected.
-- None of those may destroy a prior observation, because the prior observation
-- is what we were actually looking at on the day. So:
--   * duplicates collapse on `idempotency_key` and never overwrite;
--   * a correction is a NEW row carrying `supersedes_signal_id`, and
--     supersession is DERIVED at read time from the newer row's pointer.
-- There is deliberately no `superseded_at` column to UPDATE. That single
-- omission is what lets the ingest role hold INSERT on these tables and
-- nothing else: no UPDATE and no DELETE on any external table, and no
-- privilege of any kind on any scanner relation.
--
-- Its ONE update privilege is on the shared `catalyst_source_state` freshness
-- table, and RLS confines that to rows named `external_%` — so a leaked
-- ingress credential cannot mark the SEC or news dimension as failed. See
-- ops/sql/create_smart_scanner_external_ingest.sql section 6.
--
-- WHAT IS DELIBERATELY NOT STORED
-- -------------------------------
-- No confluence score, no combined probability, no ranking weight and no
-- invented confidence. When a source supplies no confidence the column is
-- NULL and the product says `unavailable` — it never substitutes a number.
-- External signals cannot reach the Wyckoff verdict, the attention tier, the
-- ordering or ENTER eligibility, and no scanner table references these tables.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. The source registry.
--
-- Small, auditable, and honest about what a source CAN do rather than what we
-- wish it did. A source is listed here whether or not an adapter exists —
-- "we evaluated this and it cannot push us data" is a fact worth storing, and
-- storing it is what stops the same evaluation being redone every quarter.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.external_signal_sources (
  source TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,

  -- How data could physically reach us. An empty array means "no mechanism we
  -- may use" — the honest state for a UI-only product.
  transports TEXT[] NOT NULL DEFAULT '{}',

  -- Capability facts, each independently checkable.
  supports_realtime BOOLEAN NOT NULL DEFAULT false,
  supports_historical BOOLEAN NOT NULL DEFAULT false,
  supports_symbol_scan BOOLEAN NOT NULL DEFAULT false,
  supports_signal_events BOOLEAN NOT NULL DEFAULT false,
  -- THE distinction that decides whether a source belongs in this hub at all:
  -- does it emit a claim about a symbol, or only rows of data we could compute?
  emits_signals BOOLEAN NOT NULL DEFAULT false,
  requires_paid_plan BOOLEAN NOT NULL DEFAULT false,

  --   live                      an adapter exists and data can arrive now
  --   requires_manual_setup     server side is complete; a human must finish
  --                             configuration in the third-party product
  --   available_not_integrated  integrable, deliberately not built in V1
  --   evaluated_deferred        evaluated; adds abstraction, not information
  --   unavailable               no mechanism we may use without breaking terms
  status TEXT NOT NULL,

  -- Why the status is what it is. Prose on purpose: the next person to read
  -- this table needs the reasoning, not a code.
  notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT external_signal_sources_status_ck
    CHECK (status IN ('live', 'requires_manual_setup',
                      'available_not_integrated', 'evaluated_deferred',
                      'unavailable')),
  CONSTRAINT external_signal_sources_source_ck
    CHECK (source ~ '^[a-z][a-z0-9_]{1,39}$')
);

-- ---------------------------------------------------------------------------
-- 2. Raw delivery audit.
--
-- One row per HTTP request that reached the gateway — accepted, rejected or
-- duplicate. This table is the reason a rejection is diagnosable at all: a
-- gateway that only records what it accepted cannot answer "the alert fired,
-- why is nothing showing?", which is the question that will actually be asked.
--
-- It is also where replay protection lives. `body_fingerprint` is a SHA-256
-- over the exact bytes received; a retried delivery reproduces it exactly and
-- is recorded as `duplicate` rather than becoming a second observation.
--
-- KNOWN AND ACCEPTED: two genuinely distinct alerts with byte-identical
-- bodies (same symbol, same state, same source timestamp) are indistinguishable
-- from a retry and collapse. Every practical alert body carries a firing
-- timestamp, so this costs nothing real, and collapsing is the safe direction.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.external_signal_deliveries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),
  transport TEXT NOT NULL DEFAULT 'webhook',

  -- THE provable clock. Also the parent of every signal's `effective_at`.
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  body_fingerprint TEXT NOT NULL,
  payload_bytes INTEGER NOT NULL,

  --   accepted   validated and turned into >= 1 signal
  --   duplicate  a delivery we had already seen
  --   rejected   failed validation; nothing was normalised
  status TEXT NOT NULL,
  -- A short, stable, secret-free code. Never an exception string, never a SQL
  -- error and never anything derived from the request's credentials.
  rejection_reason TEXT,

  -- The claim, preserved. Provenance requires being able to answer "what
  -- EXACTLY did the source say", and a normalised row cannot answer that.
  -- Stored only for deliveries that passed schema validation, capped upstream,
  -- and written with secret-shaped keys redacted by the gateway.
  raw_payload JSONB,

  signal_count INTEGER NOT NULL DEFAULT 0,

  CONSTRAINT external_signal_deliveries_status_ck
    CHECK (status IN ('accepted', 'duplicate', 'rejected')),
  CONSTRAINT external_signal_deliveries_bytes_ck
    CHECK (payload_bytes >= 0),

  -- Replay protection, enforced by the database rather than by a code path
  -- that can be forgotten.
  CONSTRAINT external_signal_deliveries_replay_unique
    UNIQUE (source, body_fingerprint)
);

CREATE INDEX IF NOT EXISTS external_signal_deliveries_received_idx
  ON public.external_signal_deliveries (received_at DESC);
CREATE INDEX IF NOT EXISTS external_signal_deliveries_source_received_idx
  ON public.external_signal_deliveries (source, received_at DESC);

-- ---------------------------------------------------------------------------
-- 3. The normalised signal.
--
-- Every semantic field is stored TWICE: exactly as the source said it, and in
-- our vocabulary. The raw column is the evidence; the normalised column is
-- what the product may aggregate on. When normalisation cannot map a value it
-- stores `unknown` and keeps the raw string — a source's own word is never
-- discarded to make our enum tidy.
--
-- There is deliberately no BUY/SELL anywhere in this schema. Those are
-- execution words. This system records opinions and measures them; it places
-- no orders, and its vocabulary should not imply otherwise.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.external_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),
  delivery_id UUID NOT NULL
    REFERENCES public.external_signal_deliveries(id) ON DELETE CASCADE,

  -- The source's own identity for this signal when it supplies one. NULL is a
  -- legitimate answer (TradingView alerts carry no id), and the idempotency
  -- key below covers that case without inventing one.
  source_signal_id TEXT,

  symbol TEXT NOT NULL,

  -- THE UNIVERSE BOUNDARY (and the reason out-of-universe signals are ACCEPTED
  -- rather than rejected).
  --
  -- An external scanner will legitimately name symbols our frozen 25-symbol
  -- experiment never inspects. Rejecting them would throw away the answer to
  -- "what else should we be looking at?" — which is one of the better reasons
  -- to connect an external system at all. Silently accepting them into the
  -- scanner surface would corrupt a live prospective experiment, which is far
  -- worse.
  --
  -- So both are avoided: the signal is stored, and this column records which
  -- side of the line it fell on.
  --   scanner_universe    the symbol is in the frozen experimental universe;
  --                       the Product API may surface it
  --   external_discovery  the symbol is outside it; RESEARCH ONLY. It never
  --                       reaches the scanner surface (which only ever queries
  --                       universe symbols) and never joins the experiment.
  symbol_scope TEXT NOT NULL DEFAULT 'external_discovery',

  -- ---- the three clocks (see the header) --------------------------------- #
  observed_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ NOT NULL,
  effective_at TIMESTAMPTZ NOT NULL,
  -- observed_at - received_at, in seconds. Signed and kept even when small:
  -- a source whose clock drifts is a fact about the source.
  clock_skew_seconds INTEGER,

  -- ---- semantics: raw, then ours ----------------------------------------- #
  timeframe TEXT,                 -- '240', '1D', '60' … exactly as sent
  timeframe_normalized TEXT,      -- '4h', '1d', '1h' … or NULL if unmappable

  signal_type TEXT NOT NULL,      -- the source's own word
  signal_type_normalized TEXT NOT NULL,

  direction TEXT,                 -- the source's own word
  direction_normalized TEXT NOT NULL,

  -- NULL when the source supplies none. `confidence_scale` says what the
  -- number MEANS; a bare number whose scale is unknown is not evidence.
  confidence NUMERIC,
  confidence_scale TEXT,

  -- ---- provenance -------------------------------------------------------- #
  indicator TEXT,                 -- script / indicator identifier
  indicator_version TEXT,         -- source version or configuration
  alert_id TEXT,                  -- the source's alert identifier
  contract_version TEXT NOT NULL, -- the payload contract we validated against
  source_payload_version TEXT,    -- the version the source declared

  -- Everything else the source sent that we chose not to promote to a column.
  -- Bounded and validated by the gateway before it lands here.
  source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- ---- corrections (never destructive) ----------------------------------- #
  -- A correction is a NEW row pointing back. Supersession is DERIVED at read
  -- time from this pointer, so nothing here is ever UPDATEd and the ingest
  -- role needs no UPDATE privilege.
  supersedes_signal_id UUID
    REFERENCES public.external_signals(id) ON DELETE SET NULL,

  -- Idempotency. Derived by the gateway from the source's own identity when it
  -- has one, and otherwise from the semantic tuple, so a repeated delivery
  -- collapses while a genuinely new observation of the same state does not.
  idempotency_key TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT external_signals_symbol_ck CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,15}$'),
  CONSTRAINT external_signals_scope_ck
    CHECK (symbol_scope IN ('scanner_universe', 'external_discovery')),
  CONSTRAINT external_signals_direction_ck
    CHECK (direction_normalized IN ('bullish', 'bearish', 'neutral', 'unknown')),
  CONSTRAINT external_signals_type_ck
    CHECK (signal_type_normalized IN (
      'entry_signal', 'exit_signal', 'classification', 'regime_filter',
      'trend', 'reversal', 'breakout', 'setup', 'alert', 'unknown')),
  CONSTRAINT external_signals_timeframe_ck
    CHECK (timeframe_normalized IS NULL OR timeframe_normalized IN (
      '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '1w', '1M')),
  CONSTRAINT external_signals_confidence_ck
    CHECK (confidence IS NULL OR (confidence_scale IS NOT NULL)),

  -- One observation per source identity, forever. A second delivery of the
  -- same alert updates nothing and inserts nothing.
  CONSTRAINT external_signals_idempotency_unique UNIQUE (source, idempotency_key)
);

CREATE INDEX IF NOT EXISTS external_signals_symbol_effective_idx
  ON public.external_signals (symbol, effective_at DESC);
CREATE INDEX IF NOT EXISTS external_signals_source_effective_idx
  ON public.external_signals (source, effective_at DESC);
CREATE INDEX IF NOT EXISTS external_signals_supersedes_idx
  ON public.external_signals (supersedes_signal_id)
  WHERE supersedes_signal_id IS NOT NULL;
-- Research reads ("what did external systems name that we never scan?") are a
-- different access pattern from the product read and get their own index.
CREATE INDEX IF NOT EXISTS external_signals_discovery_idx
  ON public.external_signals (symbol, effective_at DESC)
  WHERE symbol_scope = 'external_discovery';

-- ---------------------------------------------------------------------------
-- 4. Point-in-time association to scanner sessions (the measurement path).
--
-- A VIEW, not a table, and that is the whole point: there is no writer, no
-- backfill job and no cached association that could drift from the rule. The
-- rule is evaluated from the two facts every time it is asked.
--
-- `session_close` is 16:00 America/New_York on the scan session, DST included
-- — the SAME clock `app.news.session_close_utc` uses, so the SQL path and the
-- Python path cannot disagree about when a session ended.
--
-- A signal is attached to a session iff it arrived before that session closed.
-- `sessions_ago` is left to the caller (calendar-aware counting lives in
-- Python); this view supplies the honest raw timing, which is what Phase 13
-- measurement needs and what a fitted window would destroy.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.external_signal_session_links AS
SELECT
  s.id                AS signal_id,
  s.source,
  s.symbol,
  s.effective_at,
  s.signal_type_normalized,
  s.direction_normalized,
  s.timeframe_normalized,
  r.id                AS run_id,
  rp.pair_id,
  (COALESCE(r.telemetry->'campaign'->>'as_of_date',
            r.telemetry->>'as_of_date'))::date AS session_date,
  ((COALESCE(r.telemetry->'campaign'->>'as_of_date',
             r.telemetry->>'as_of_date')::date + time '16:00')
     AT TIME ZONE 'America/New_York')          AS session_close,
  -- Calendar days between arrival and the session, for a quick sanity read.
  -- NEVER a fitted window: the product's windows live in Python and are fixed
  -- a priori, and this column exists so raw timing stays visible.
  ((COALESCE(r.telemetry->'campaign'->>'as_of_date',
             r.telemetry->>'as_of_date')::date) - s.effective_at::date)
                                               AS calendar_days_before_session
FROM public.external_signals s
JOIN public.strategy_shadow_pairs p
  ON p.symbol = s.symbol
JOIN public.strategy_shadow_run_pairs rp
  ON rp.pair_id = p.id
JOIN public.strategy_shadow_runs r
  ON r.id = rp.run_id
WHERE r.telemetry->'campaign' IS NOT NULL
  AND COALESCE(r.telemetry->'campaign'->>'as_of_date',
               r.telemetry->>'as_of_date') IS NOT NULL
  -- THE GATE. Nothing that arrived after the close may be attached to it.
  AND s.effective_at <= ((COALESCE(r.telemetry->'campaign'->>'as_of_date',
                                   r.telemetry->>'as_of_date')::date
                          + time '16:00') AT TIME ZONE 'America/New_York');

-- ---------------------------------------------------------------------------
-- 5. Freshness.
--
-- Reuses `catalyst_source_state` (019) with one row per external source, for
-- the same reason 020 and 021 did: "when did this source last reach us, and is
-- it configured at all" is the same question everywhere, and an empty signals
-- table must never be readable as "no external system saw anything".
--
-- The vocabulary is unchanged, but the MEANING of `last_success_at` differs and
-- that difference is deliberate: for a pulled source it means "our refresh
-- ran"; for a pushed source it means "a delivery arrived". A webhook source
-- that no one has configured yet therefore reports `never_run`, which is the
-- truth, rather than an error.
-- ---------------------------------------------------------------------------

-- RLS: enabled on every new table (policies are created by ops/sql per role).
ALTER TABLE public.external_signal_sources    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.external_signal_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.external_signals           ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- 6. Seed the registry.
--
-- Two live sources and five evaluated ones. The five are seeded with their
-- MEASURED capability and an honest status — none of them has an adapter,
-- because none of them can currently push us a signal, and an adapter that
-- cannot receive data is a liability that reads as coverage.
--
-- Idempotent: a re-run refreshes capability metadata and never duplicates.
-- ---------------------------------------------------------------------------
INSERT INTO public.external_signal_sources (
  source, display_name, transports, supports_realtime, supports_historical,
  supports_symbol_scan, supports_signal_events, emits_signals,
  requires_paid_plan, status, notes)
VALUES
  ('tradingview', 'TradingView (generic alert)',
   ARRAY['webhook'], true, false, false, true, true, true,
   'live',
   'Generic ingress for ANY TradingView indicator or strategy. MEASURED: '
   'webhook alerts require a paid plan (Essential tier and above) with 2FA '
   'enabled; TradingView exposes NO read API, so the outbound webhook is the '
   'only mechanism and there is no backfill or replay. It cannot send custom '
   'HTTP headers, which is why the ingress credential travels in the URL. '
   'Ports 80/443 only, 3s timeout, no documented retry (treat as at-most-once), '
   'body capped at 4000 chars from the alert dialog. Below Premium an alert '
   'EXPIRES after two months and must be re-armed by the account owner.'),

  ('ai_edge', 'AI Edge / Lorentzian Classification',
   ARRAY['webhook'], true, false, false, true, true, true,
   'requires_manual_setup',
   'A TradingView indicator by the AI Edge publisher, so it reaches us through '
   'the same gateway with a dedicated normaliser. Server side is complete; the '
   'account owner must create each alert on their own chart, because the script '
   'uses alertcondition() (eight discrete conditions in the open-source build) '
   'rather than alert(), so there is no "any alert() call" option and alerts '
   'cannot be provisioned programmatically. CONFIDENCE IS UNAVAILABLE and that '
   'is measured, not assumed: the internal vote score is drawn with label.new() '
   'and {{plot_N}} can only read plot() output, so no real confidence can reach '
   'an alert message. Also publishes open-source Python/Rust ports, which would '
   'give confidence and backfill if ever run in-house.'),

  ('trendspider', 'TrendSpider',
   ARRAY['webhook'], true, false, true, true, true, true,
   'available_not_integrated',
   'MEASURED: alert and Strategy-Bot webhooks exist and emit genuinely '
   'non-OHLCV events (dynamic trendline touch/break/bounce, auto S/R, Raindrop '
   'volume profile). No public read API. Two blockers, not one: the webhook URL '
   'is configured PER ACCOUNT rather than per alert, and the terms forbid '
   'redistribution and third-party access — fine as a private single-tenant '
   'input, blocking for a multi-user product. No confidence value available. '
   'Revisit only with a live subscription and a licence review.'),

  ('finviz', 'Finviz',
   ARRAY['import'], false, false, true, true, true, true,
   'available_not_integrated',
   'MEASURED: an Elite CSV export endpoint exists and is the only mechanism; '
   'there is no documented API. It DOES carry real signal presets (unusual '
   'volume, new highs/lows, overbought/oversold, chart patterns) plus short '
   'float, institutional and insider ownership, and — the genuinely new thing — '
   'a market-wide attention rank our 25-symbol universe cannot compute. NOT '
   'integrated because the legal position is unresolved: no Terms of Use is '
   'published anywhere on the site, and robots.txt explicitly disallows the '
   'export path. Any future integration must be a supported import boundary, '
   'never a scrape of an authenticated interface.'),

  ('koyfin', 'Koyfin',
   ARRAY[]::text[], false, false, false, false, false, true,
   'unavailable',
   'MEASURED AND CLOSED: Koyfin states in its own help centre that it does not '
   'offer API access at all, by policy, because of restrictions from its data '
   'vendors. Manual in-browser CSV export exists, but financials, estimates and '
   'valuation — precisely the columns that would add value — are contractually '
   'blocked from download. Addressable new information for a backend: zero. '
   'This is not a roadmap item; it is a closed question.'),

  ('openbb', 'OpenBB',
   ARRAY['api'], false, true, false, false, false, false,
   'evaluated_deferred',
   'MEASURED: OpenBB hosts and serves NO data of its own — it is a connector '
   'layer over providers we would still need keys and subscriptions for. Two '
   'concrete disqualifiers beyond that: it is AGPL-3.0-only (serving a modified '
   'version over a network triggers source-release obligations absent a '
   'commercial licence), and even a slim install pins fastapi to an EXACT '
   'version, which is a hard pin on this backend own web framework. Useful '
   'offline for research; never inside this process.'),

  ('fmp', 'Financial Modeling Prep',
   ARRAY['api'], false, true, true, true, false, false,
   'available_not_integrated',
   'MEASURED against the live key on 2026-08-28. The /api/v3 base URL this '
   'repository still uses is DEAD — it returns HTTP 403 "Legacy Endpoint"; the '
   'current base is /stable/ with a changed call shape. On /stable/ the '
   'market-movers feeds (biggest-gainers, biggest-losers, most-actives) return '
   'HTTP 200 on the current free key, while company-screener returns HTTP 402 '
   '(Starter tier and above). Movers are the one genuinely new thing here: a '
   'market-wide attention cohort that a frozen 25-symbol universe structurally '
   'cannot compute. It is a DISCOVERY source, never a signal source and never a '
   'replacement for the canonical price history. Note the licence: individual '
   'plans are personal/non-commercial and forbid third-party access to the '
   'data.')

ON CONFLICT (source) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  transports = EXCLUDED.transports,
  supports_realtime = EXCLUDED.supports_realtime,
  supports_historical = EXCLUDED.supports_historical,
  supports_symbol_scan = EXCLUDED.supports_symbol_scan,
  supports_signal_events = EXCLUDED.supports_signal_events,
  emits_signals = EXCLUDED.emits_signals,
  requires_paid_plan = EXCLUDED.requires_paid_plan,
  status = EXCLUDED.status,
  notes = EXCLUDED.notes,
  updated_at = NOW();

-- ---------------------------------------------------------------------------
-- 7. Operating model.
--
-- There is NO daily-pipeline stage for this dimension, and its absence is the
-- design: an external signal is PUSHED the moment the third party decides,
-- not pulled on our schedule. Adding a refresh stage would imply a polling
-- path that does not exist. `job_schedules` is therefore left untouched by
-- this migration — the first migration in this series with nothing to add
-- there, because the ingestion is an internet-facing endpoint instead of a job.
-- ---------------------------------------------------------------------------
