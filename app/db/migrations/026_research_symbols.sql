-- ===========================================================================
-- 026 — The research domain: symbols we may STUDY, never trade beside
-- ===========================================================================
-- Wave 2 measured the scanner's blind spot exactly: of 68 symbols the market
-- noticed in one session, ONE was inside the frozen 25 and 67 had too little
-- local history to analyse at all. We could see what we were missing and could
-- do nothing about it. These two tables close that, and nothing else.
--
-- THE BOUNDARY, EXPRESSED AS SCHEMA RATHER THAN AS A RULE
-- ------------------------------------------------------
-- A research symbol must never become a universe member, an experiment pair,
-- or a canonical outcome. So:
--
--   * `research_symbols` is NOT `history_warmup_universe_symbols`. No foreign
--     key joins them and no code path writes one from the other.
--   * `research_scan_results` is NOT `strategy_shadow_evaluations`. It has no
--     pair_id, no run_id, no arm_code — the columns an experiment row needs in
--     order to be an experiment row are simply absent, so one cannot be
--     mistaken for the other by a query or by a person.
--   * There is no `allow_enter`, no attention tier and no score anywhere here.
--
-- WHY NOT A `history_warmup_universes` ROW
-- ----------------------------------------
-- That model is immutable BY TRIGGER: `history_warmup_universe_symbols_guard`
-- refuses every insert, update and delete the moment a universe leaves
-- `draft`, and the warmup path pins a hash at freeze. That is exactly right
-- for a cohort whose interpretability depends on never changing — and exactly
-- wrong for a research set that must grow whenever the market surfaces
-- something new. Reusing it would force one of two bad outcomes: freeze a set
-- we need to extend, or park a universe permanently in `draft` and silently
-- weaken the immutability guarantee that the frozen 25 depend on.
--
-- THREE DATES, STILL SEPARATE (migration 025)
-- -------------------------------------------
--   first/latest_reference_session   which market session surfaced it
--   first_actionable_session         earliest session anybody could act
--   *_observed_at / warmed_at        our own clocks
--
-- LICENSING
-- ---------
-- Every row here exists BECAUSE of an FMP discovery, whose licence is
-- internal_research_only. The class is carried on the row and neither table is
-- granted to the Product API's database role — the same three-layer
-- enforcement Wave 2 established, unchanged and not weakened.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.research_symbols (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,

  -- ---- discovery provenance (why this symbol is here at all) --------------
  discovery_source TEXT NOT NULL
    REFERENCES public.external_signal_sources(source),
  -- Every list that ever surfaced it, preserved as a set rather than counted.
  -- "top gainer AND most active" stays two checkable facts.
  discovery_reasons TEXT[] NOT NULL DEFAULT '{}',
  discovery_observation_count INTEGER NOT NULL DEFAULT 1,
  first_observed_at TIMESTAMPTZ NOT NULL,
  latest_observed_at TIMESTAMPTZ NOT NULL,
  -- The MARKET session the snapshot described, not the one it became
  -- actionable in. See migration 025 for why those are different columns.
  first_reference_session DATE NOT NULL,
  latest_reference_session DATE NOT NULL,
  first_actionable_session DATE NOT NULL,
  best_rank INTEGER,

  -- ---- lifecycle ----------------------------------------------------------
  -- Recorded, but never trusted as the source of truth: the state is RECOMPUTED
  -- from bar counts and attempts on every pass. A state machine that can only
  -- be advanced by whatever advanced it last is a state machine that gets
  -- stuck the first time a process dies mid-transition.
  state TEXT NOT NULL DEFAULT 'discovered',
  history_daily_bars INTEGER NOT NULL DEFAULT 0,
  history_first_session DATE,
  history_latest_session DATE,
  warmup_attempts INTEGER NOT NULL DEFAULT 0,
  warmup_last_attempt_at TIMESTAMPTZ,
  warmup_cooldown_until TIMESTAMPTZ,
  -- Bounded, secret-free. A provider error code, never a payload.
  warmup_last_error_code TEXT,
  warmup_last_error_class TEXT,
  warmup_provider_requests INTEGER NOT NULL DEFAULT 0,

  research_scanned_at TIMESTAMPTZ,
  licensing_visibility TEXT NOT NULL DEFAULT 'internal_research_only',

  admitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_symbols_symbol_unique UNIQUE (symbol),
  CONSTRAINT research_symbols_symbol_ck CHECK (symbol ~ '^[A-Z][A-Z0-9.\-]{0,15}$'),
  CONSTRAINT research_symbols_state_ck
    CHECK (state IN ('discovered', 'history_required', 'history_warming',
                     'research_ready', 'research_scanned', 'unavailable',
                     'failed')),
  CONSTRAINT research_symbols_error_class_ck
    CHECK (warmup_last_error_class IS NULL
           OR warmup_last_error_class IN ('retryable', 'terminal',
                                          'operator_error')),
  CONSTRAINT research_symbols_licensing_ck
    CHECK (licensing_visibility IN ('product_display_allowed',
                                    'internal_research_only',
                                    'unknown_restriction')),
  CONSTRAINT research_symbols_counts_ck
    CHECK (history_daily_bars >= 0 AND warmup_attempts >= 0
           AND discovery_observation_count >= 1
           AND warmup_provider_requests >= 0),
  -- The same ordering migration 025 established, restated where it applies:
  -- a snapshot cannot become actionable before the session it describes.
  CONSTRAINT research_symbols_session_order_ck
    CHECK (first_reference_session <= latest_reference_session
           AND first_reference_session <= first_actionable_session)
);

CREATE INDEX IF NOT EXISTS research_symbols_state_idx
  ON public.research_symbols (state, latest_reference_session DESC);

CREATE INDEX IF NOT EXISTS research_symbols_ready_idx
  ON public.research_symbols (latest_reference_session DESC)
  WHERE state IN ('research_ready', 'research_scanned');


-- ---------------------------------------------------------------------------
-- Research scan results.
--
-- Deliberately shaped so it CANNOT be mistaken for an experiment evaluation.
-- It carries the canonical strategy's own verdict verbatim — the same
-- `_resolve_arm` / `build_canonical_frame` / `_evaluate_arm` path the
-- experiment uses, because a second Wyckoff implementation would be a second
-- set of answers — but it has no pair, no run, no arm and no outcome, and the
-- contract version says `research_scan` in its name so a reader of raw rows
-- knows what they are holding.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.research_scan_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL REFERENCES public.research_symbols(symbol)
    ON DELETE CASCADE,

  -- The session the evaluation was pinned to. A completed session, always:
  -- the research scan reads local bars only and cannot see a forming one.
  scan_session DATE NOT NULL,
  scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  contract_version TEXT NOT NULL,
  -- Canonical identity, copied from the resolved arm rather than restated, so
  -- a drifting strategy version is visible in the row.
  strategy_code TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  frame_hash TEXT,
  bars_evaluated INTEGER,

  -- The strategy's own words, unmodified.
  verdict TEXT,
  score NUMERIC,
  reason TEXT,
  rejection_reason TEXT,
  structure_state TEXT,
  setup_state TEXT,
  reason_code TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- Context that needs no sector mapping.
  benchmark_symbol TEXT,
  benchmark_relative TEXT,
  benchmark_excess_pct NUMERIC,
  -- A discovered symbol is usually absent from the hand-made sector registry.
  -- The state is explicit and never guessed; `sector_unknown` is an answer.
  sector_state TEXT NOT NULL DEFAULT 'sector_unknown',
  sector_symbol TEXT,

  -- Control arm context where the symbol has enough history for it.
  control_verdict TEXT,
  control_reason TEXT,

  licensing_visibility TEXT NOT NULL DEFAULT 'internal_research_only',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT research_scan_identity_unique UNIQUE (symbol, scan_session),
  CONSTRAINT research_scan_sector_ck
    CHECK (sector_state IN ('sector_known', 'sector_unknown',
                            'reference_unavailable')),
  CONSTRAINT research_scan_licensing_ck
    CHECK (licensing_visibility IN ('product_display_allowed',
                                    'internal_research_only',
                                    'unknown_restriction')),
  -- The load-bearing one. A research scan may never be ENTER-eligible, and the
  -- database refuses rather than trusting the caller. `ENTER` is a live
  -- execution word; research produces evidence, not eligibility.
  CONSTRAINT research_scan_no_enter_ck
    CHECK (verdict IS NULL OR verdict <> 'ENTER')
);

CREATE INDEX IF NOT EXISTS research_scan_session_idx
  ON public.research_scan_results (scan_session DESC, symbol);


-- RLS on both, like every other table added here. Policies are created per
-- role by ops/sql, never in a migration.
ALTER TABLE public.research_symbols       ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.research_scan_results  ENABLE ROW LEVEL SECURITY;


-- ---------------------------------------------------------------------------
-- The ingestion role gains exactly these two tables and nothing else. It still
-- holds no privilege on any scanner or experiment relation, so the boundary at
-- the top of this file is a privilege, not a promise.
--
-- The Product API's role is deliberately NOT granted either table: research
-- rows exist because of an internal-research-only discovery, and the licence
-- boundary does not move because the data became more interesting.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    GRANT SELECT, INSERT, UPDATE ON public.research_symbols
      TO smart_scanner_market_intel;
    GRANT SELECT, INSERT, UPDATE ON public.research_scan_results
      TO smart_scanner_market_intel;

    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='research_symbols'
                     AND policyname='smart_scanner_market_intel_rw') THEN
      CREATE POLICY smart_scanner_market_intel_rw ON public.research_symbols
        FOR ALL TO smart_scanner_market_intel USING (true) WITH CHECK (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='research_scan_results'
                     AND policyname='smart_scanner_market_intel_rw') THEN
      CREATE POLICY smart_scanner_market_intel_rw ON public.research_scan_results
        FOR ALL TO smart_scanner_market_intel USING (true) WITH CHECK (true);
    END IF;
  END IF;
END
$$;

-- The research warmup writes daily bars through the SAME provider abstraction
-- and the SAME `upsert_daily_bars` primitive the frozen universe uses, so the
-- ingestion role needs to write that one shared table. It is granted INSERT
-- and UPDATE and NOT DELETE: a research warmup may add history, never remove
-- anybody's.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    GRANT INSERT, UPDATE ON public.daily_bars TO smart_scanner_market_intel;
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='daily_bars'
                     AND policyname='smart_scanner_market_intel_bars_write') THEN
      -- Confined by predicate to symbols that are actually research symbols,
      -- so this grant can never be used to touch a frozen-universe bar.
      CREATE POLICY smart_scanner_market_intel_bars_write ON public.daily_bars
        FOR INSERT TO smart_scanner_market_intel
        WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols));
      CREATE POLICY smart_scanner_market_intel_bars_update ON public.daily_bars
        FOR UPDATE TO smart_scanner_market_intel
        USING (symbol IN (SELECT symbol FROM public.research_symbols))
        WITH CHECK (symbol IN (SELECT symbol FROM public.research_symbols));
    END IF;
  END IF;
END
$$;
