-- ===========================================================================
-- 032 — the freeze trigger must guard the WHOLE meaning of a result
-- ===========================================================================
-- Migration 031 froze a measured outcome's NUMBERS. An acceptance audit of the
-- deployed engine found that it did not freeze what those numbers are ABOUT.
--
-- The guarded set was:
--
--     status, measured_at, entry_close, exit_close, benchmark_entry_close,
--     benchmark_exit_close, symbol_return_pct, benchmark_return_pct,
--     excess_return_pct, mfe_pct, mae_pct, excursion_basis,
--     window_sessions_expected, window_sessions_present, bars_hash,
--     horizon_session, horizon_sessions, scan_session, scan_id,
--     scan_classification, config_hash
--
-- Ten columns that carry the result's meaning were outside it:
--
--     strategy_code, strategy_version          which strategy produced the view
--     scan_verdict                             what it said
--     scan_structure_state, scan_setup_state   the evidence behind that
--     scan_reason_code, scan_rejection_reason  its own words for why
--     scan_benchmark_relative                  the reading at scan time
--     scan_scanned_at                          the revision tell (031's header
--                                              names this as THE detector for a
--                                              re-scan; a detector that can be
--                                              edited detects nothing)
--     benchmark_symbol                         WHICH benchmark the excess is
--                                              measured against
--
-- and so were the three version columns that say how to interpret all of it.
--
-- WHY THIS MATTERS AND WHY IT IS NOT THEORETICAL
-- ----------------------------------------------
-- `benchmark_symbol` alone is enough. A measured row says `excess_return_pct =
-- -4.27`; flipping that column from SPY to QQQ leaves a row that is internally
-- consistent, passes every CHECK, survives the trigger, and now claims an
-- excess against a benchmark it was never computed from. Nothing in this
-- project's code does that today. The whole argument of 031's trigger is that
-- "nothing does that today" is not a guarantee — the first hand-written
-- backfill script somebody writes in a hurry is the counterexample.
--
-- The same applies to `scan_setup_state`: an outcome grouped by setup state is
-- the first analysis this ledger exists to support, and a measured row whose
-- setup state can be edited afterwards is a row that can be moved between
-- cohorts after the fact.
--
-- WHAT STAYS WRITABLE, AND WHY
-- ----------------------------
-- Unchanged from 031: attempt_count, last_attempt_at, revision_detected,
-- revision_notes, updated_at. "We looked again and the store disagrees" must
-- remain recordable exactly when "we changed our mind" must not.
--
-- Also deliberately still writable: `symbol`, `horizon_label` and
-- `licensing_visibility`. The first two are already pinned by immutable
-- columns — `symbol` by the natural UNIQUE key and the scan_id foreign key,
-- `horizon_label` by a CHECK that ties it to the frozen `horizon_sessions` —
-- so guarding them again would add a second lock to a door that is already
-- bolted. `licensing_visibility` is a classification of the row rather than a
-- fact about the market, and a licence that could never be corrected on an
-- existing row would be the wrong kind of immutable.
--
-- BACKWARD COMPATIBILITY
-- ----------------------
-- A pure WIDENING of an existing refusal. Every UPDATE the old trigger allowed
-- and the new one still allows behaves identically; the only new outcome is
-- that a previously-permitted rewrite of a MEASURED row's attribution is now
-- refused. No row is read, altered or migrated. `CREATE OR REPLACE FUNCTION`
-- plus a DROP/CREATE of the trigger makes re-running this safe, and a database
-- that already has 032 converges rather than duplicating.
--
-- Nothing outside `research_scan_outcomes` is touched.
-- ===========================================================================

\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION public.research_scan_outcomes_freeze()
RETURNS trigger AS $fn$
BEGIN
  IF OLD.status = 'measured' THEN
    IF -- ---- the lifecycle -------------------------------------------------
       NEW.status                IS DISTINCT FROM OLD.status
       OR NEW.measured_at        IS DISTINCT FROM OLD.measured_at
       -- ---- the measurement ----------------------------------------------
       OR NEW.entry_close        IS DISTINCT FROM OLD.entry_close
       OR NEW.exit_close         IS DISTINCT FROM OLD.exit_close
       OR NEW.benchmark_entry_close IS DISTINCT FROM OLD.benchmark_entry_close
       OR NEW.benchmark_exit_close  IS DISTINCT FROM OLD.benchmark_exit_close
       OR NEW.symbol_return_pct  IS DISTINCT FROM OLD.symbol_return_pct
       OR NEW.benchmark_return_pct IS DISTINCT FROM OLD.benchmark_return_pct
       OR NEW.excess_return_pct  IS DISTINCT FROM OLD.excess_return_pct
       OR NEW.mfe_pct            IS DISTINCT FROM OLD.mfe_pct
       OR NEW.mae_pct            IS DISTINCT FROM OLD.mae_pct
       OR NEW.excursion_basis    IS DISTINCT FROM OLD.excursion_basis
       OR NEW.window_sessions_expected IS DISTINCT FROM OLD.window_sessions_expected
       OR NEW.window_sessions_present  IS DISTINCT FROM OLD.window_sessions_present
       OR NEW.bars_hash          IS DISTINCT FROM OLD.bars_hash
       -- ---- what the numbers are ABOUT (new in 032) -----------------------
       OR NEW.benchmark_symbol   IS DISTINCT FROM OLD.benchmark_symbol
       OR NEW.strategy_code      IS DISTINCT FROM OLD.strategy_code
       OR NEW.strategy_version   IS DISTINCT FROM OLD.strategy_version
       OR NEW.config_hash        IS DISTINCT FROM OLD.config_hash
       OR NEW.scan_classification IS DISTINCT FROM OLD.scan_classification
       OR NEW.scan_verdict       IS DISTINCT FROM OLD.scan_verdict
       OR NEW.scan_structure_state IS DISTINCT FROM OLD.scan_structure_state
       OR NEW.scan_setup_state   IS DISTINCT FROM OLD.scan_setup_state
       OR NEW.scan_reason_code   IS DISTINCT FROM OLD.scan_reason_code
       OR NEW.scan_rejection_reason IS DISTINCT FROM OLD.scan_rejection_reason
       OR NEW.scan_benchmark_relative IS DISTINCT FROM OLD.scan_benchmark_relative
       OR NEW.scan_scanned_at    IS DISTINCT FROM OLD.scan_scanned_at
       -- ---- how to interpret it (new in 032) ------------------------------
       OR NEW.contract_version   IS DISTINCT FROM OLD.contract_version
       OR NEW.calculation_version IS DISTINCT FROM OLD.calculation_version
       OR NEW.market_calendar_version IS DISTINCT FROM OLD.market_calendar_version
       -- ---- the identity --------------------------------------------------
       OR NEW.scan_id            IS DISTINCT FROM OLD.scan_id THEN
      RAISE EXCEPTION
        'research_scan_outcomes %: a measured outcome is immutable', OLD.id
        USING ERRCODE = 'integrity_constraint_violation';
    END IF;
  END IF;
  -- The horizon identity is derived from the scan session and the market
  -- calendar. Neither may move under a row that already exists, measured or
  -- not: that would re-point a pending observation at a different day.
  IF NEW.scan_session IS DISTINCT FROM OLD.scan_session
     OR NEW.horizon_sessions IS DISTINCT FROM OLD.horizon_sessions
     OR NEW.horizon_session IS DISTINCT FROM OLD.horizon_session THEN
    RAISE EXCEPTION
      'research_scan_outcomes %: the horizon identity is immutable', OLD.id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  NEW.updated_at := NOW();
  RETURN NEW;
END
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS research_scan_outcomes_freeze_trg
  ON public.research_scan_outcomes;
CREATE TRIGGER research_scan_outcomes_freeze_trg
  BEFORE UPDATE ON public.research_scan_outcomes
  FOR EACH ROW EXECUTE FUNCTION public.research_scan_outcomes_freeze();
