-- ===========================================================================
-- 027 — Admission before warmup, and "scanned" is not "worth a look"
-- ===========================================================================
-- Two corrections, both forced by what the first live research cohort actually
-- produced rather than by design review.
--
-- 1. WE BOUGHT HISTORY FOR SYMBOLS WE COULD HAVE REJECTED FOR FREE
--
--      CELU  0 -> 500 bars -> AVOID / price_below_minimum
--      NVD   0 -> 500 bars -> AVOID / price_below_minimum
--      PPCB  0 -> 437 bars -> AVOID / price_below_minimum
--
--    Three of three scanned symbols failed the same hard, already-canonical
--    gate, and every one of them was knowable from a row we already held.
--    `min_price` is not new and is not ours: it is read from the resolved
--    strategy configuration, so admission and the strategy can never disagree.
--
--    The columns below record the decision AND its provenance — which price,
--    from which source, describing which market session — so a skipped
--    provider request can be audited rather than merely trusted.
--
-- 2. "SCANNED" WAS BEING REPORTED AS "WORTH A HUMAN LOOK"
--
--    The same three symbols were listed as candidates because their DISCOVERY
--    reasons were strong, while their canonical verdict was a hard AVOID.
--    Discovery strength explains why we LOOKED; only scan evidence can say
--    whether the symbol SURVIVED. `candidate_state` separates them, and the
--    two reason lists are stored in separate columns so they cannot be summed
--    into one impression.
--
-- LICENCE — STATED, NOT DISCOVERED LATER
-- --------------------------------------
-- `admission_price_source` exists because it changes what the decision is.
-- `local_daily_bars` is our own canonical data and carries no restriction.
-- `discovery_snapshot` is FMP's price, already stored, costing no request —
-- and INTERNAL RESEARCH ONLY. When admission rejects on that source, the
-- REJECTION ITSELF is derived from restricted data. Contained today because
-- the whole research domain is internal; recorded on the row so that if
-- research is ever exposed, the affected rows are identifiable rather than
-- needing to be re-derived.
-- ===========================================================================

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_state TEXT;

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_reason TEXT;

-- The price the decision was made on, and where it came from. Both, always:
-- a number without its source cannot be licensed, dated, or checked.
ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_price NUMERIC;

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_price_source TEXT;

-- The MARKET session the admission price describes (migration 025's
-- distinction), never the session it became actionable in.
ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_reference_session DATE;

-- The canonical minimum in force when the decision was made. Stored rather
-- than looked up later, because an operator changing `min_price` must not
-- silently rewrite the history of past decisions.
ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_min_price NUMERIC;

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS admission_evaluated_at TIMESTAMPTZ;

-- P2 — the candidate verdict, kept apart from the discovery reasons.
ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS candidate_state TEXT;

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS candidate_reason TEXT;

-- WHY WE LOOKED (discovery) and WHAT THE SCREEN FOUND (strategy evidence), in
-- two columns. One column holding both is precisely how the first report came
-- to call a hard-AVOID symbol a candidate.
ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS looked_because TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE public.research_symbols
  ADD COLUMN IF NOT EXISTS screen_findings TEXT[] NOT NULL DEFAULT '{}';

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'research_symbols_admission_state_ck') THEN
    ALTER TABLE public.research_symbols
      ADD CONSTRAINT research_symbols_admission_state_ck
      CHECK (admission_state IS NULL
             OR admission_state IN ('eligible_for_history',
                                    'rejected_before_history',
                                    'insufficient_admission_data'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'research_symbols_admission_source_ck') THEN
    ALTER TABLE public.research_symbols
      ADD CONSTRAINT research_symbols_admission_source_ck
      CHECK (admission_price_source IS NULL
             OR admission_price_source IN ('local_daily_bars',
                                           'discovery_snapshot'));
  END IF;

  -- A price without a source, or a source without a price, is unauditable.
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'research_symbols_admission_pair_ck') THEN
    ALTER TABLE public.research_symbols
      ADD CONSTRAINT research_symbols_admission_pair_ck
      CHECK ((admission_price IS NULL) = (admission_price_source IS NULL));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                 WHERE conname = 'research_symbols_candidate_state_ck') THEN
    ALTER TABLE public.research_symbols
      ADD CONSTRAINT research_symbols_candidate_state_ck
      CHECK (candidate_state IS NULL
             OR candidate_state IN ('research_candidate',
                                    'scanned_not_candidate',
                                    'insufficient_data', 'unavailable'));
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS research_symbols_admission_idx
  ON public.research_symbols (admission_state, latest_reference_session DESC);

CREATE INDEX IF NOT EXISTS research_symbols_candidate_idx
  ON public.research_symbols (candidate_state)
  WHERE candidate_state = 'research_candidate';


-- ---------------------------------------------------------------------------
-- P3 — reconcile rows parked as `failed` by a classifier that did not yet
-- exist.
--
-- LGPS reached `failed` because it burned three attempts before
-- `provider_history_exhausted` was a possible answer. The evidence that it is
-- actually `unavailable` is already in the row and needs no provider call:
-- the symbol HAS bars, they are usable, and its whole listing is younger than
-- the 24-month gate — so no retry can change the outcome today.
--
-- Deliberately NOT special-cased by symbol. The predicate is the evidence.
-- Attempt counters and timestamps are untouched: this reclassifies, it does
-- not erase the audit trail of what was tried.
-- ---------------------------------------------------------------------------
UPDATE public.research_symbols r
SET state = 'unavailable',
    warmup_last_error_code = COALESCE(warmup_last_error_code,
                                      'provider_history_exhausted'),
    warmup_last_error_class = 'terminal',
    warmup_cooldown_until = NULL,
    updated_at = NOW()
WHERE r.state = 'failed'
  AND r.history_daily_bars > 0
  AND EXISTS (
    -- The listing itself is too young: fewer than 24 COMPLETED month groups
    -- are present, and the oldest bar we hold is newer than the provider's own
    -- two-year horizon — i.e. this is all there is, not all we fetched.
    SELECT 1 FROM public.daily_bars b
    WHERE b.symbol = r.symbol
    GROUP BY b.symbol
    HAVING count(DISTINCT date_trunc('month', b.trading_date)) - 1 < 24
       AND min(b.trading_date) > (CURRENT_DATE - INTERVAL '2 years' + INTERVAL '7 days')
  );


-- ---------------------------------------------------------------------------
-- The ingestion role must be able to READ the canonical strategy configuration.
--
-- Found by the fail-closed path doing its job: the first lifecycle run stopped
-- with `blocked_canonical_config_unavailable` because
-- `smart_scanner_market_intel` held no privilege on `pattern_configs`. Under
-- the old lenient resolver this would have been invisible — the run would have
-- proceeded on strategy defaults and produced a plausible verdict from a
-- configuration nobody chose.
--
-- SELECT only. The research path reads the canonical configuration; it may
-- never write one, and `pattern_configs` remains an operator-only surface.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    GRANT SELECT ON public.pattern_configs TO smart_scanner_market_intel;
    IF NOT EXISTS (SELECT 1 FROM pg_policies
                   WHERE schemaname='public' AND tablename='pattern_configs'
                     AND policyname='smart_scanner_market_intel_select') THEN
      CREATE POLICY smart_scanner_market_intel_select ON public.pattern_configs
        FOR SELECT TO smart_scanner_market_intel USING (true);
    END IF;
  END IF;
END
$$;


-- ---------------------------------------------------------------------------
-- Lazy enrichment needs to WRITE the SEC domain for the symbols that survived.
--
-- Found the same way as the config grant: the first lifecycle run reached the
-- enrichment stage, failed with InsufficientPrivilegeError, and — because the
-- stage is isolated — cost the run nothing else. Granting it here rather than
-- widening the role wholesale.
--
-- `sec_filings` / `sec_filing_symbols` hold PUBLIC EDGAR data: U.S. Government
-- filings, no credential, no third-party restriction. They are already granted
-- to the Product API's reader for the frozen 25, so enriching a research
-- symbol adds no licensing exposure — unlike the FMP-derived tables, which
-- stay unreachable to the product.
--
-- INSERT and UPDATE, never DELETE: enrichment may add filings, never remove
-- anybody's.
-- ---------------------------------------------------------------------------
DO $$
DECLARE rel text;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'smart_scanner_market_intel') THEN
    FOREACH rel IN ARRAY ARRAY['sec_filings', 'sec_filing_symbols'] LOOP
      EXECUTE format('GRANT SELECT, INSERT, UPDATE ON public.%I '
                     'TO smart_scanner_market_intel', rel);
      IF NOT EXISTS (SELECT 1 FROM pg_policies
                     WHERE schemaname='public' AND tablename=rel
                       AND policyname='smart_scanner_market_intel_rw') THEN
        EXECUTE format(
          'CREATE POLICY smart_scanner_market_intel_rw ON public.%I '
          'FOR ALL TO smart_scanner_market_intel '
          'USING (true) WITH CHECK (true)', rel);
      END IF;
    END LOOP;
  END IF;
END
$$;
