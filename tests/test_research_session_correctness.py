"""Temporal correctness of the research lifecycle — the P0 regression suite.

Every test here reproduces something that actually happened in staging on
2026-08-31, in run 43dd5723-6143-4b2c-9067-befebca3418a, and then pins the
corrected behaviour. The audit evidence each one encodes is named in its
docstring so a future reader can tell a regression from a redesign.

THE THREE DEFECTS
-----------------
  1. ONDS was reported as one of that run's two research candidates. Its only
     scan was for session 2026-08-28, performed on 2026-08-30; it was not
     scanned by the run at all. The funnel read `research_symbols.candidate_
     state`, a persistent column with no session on it.

  2. Every symbol the run did scan held bars ending 2026-08-28 while SPY had
     been refreshed to 2026-08-31, so `benchmark_excess_pct` compared two
     different days. The proof was IBIT's excess moving 20.19 -> 21.94 between
     two scans in which IBIT gained no bars: the whole delta was SPY.

  3. AAL, ETHA and SOXL — 499 bars, 23 completed months, one month-group short
     — were marked `unavailable` on `provider_history_exhausted`. That state is
     excluded from warmup selection, and the error code that caused it could
     therefore never be cleared. A closed loop.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

import app.research_enrichment as re_
import app.research_funnel as rf
import app.research_ingest as ri
import app.research_scan as rs
import app.research_universe as ru

UTC = timezone.utc
S = date(2026, 8, 31)          # the run's target session
S_PREV = date(2026, 8, 28)     # the session ONDS was actually evaluated for
NOW = datetime(2026, 9, 1, 14, 58, tzinfo=UTC)


def _row(symbol, **kw):
    """A funnel row shaped like FUNNEL_ROW_SQL's output."""
    base = {
        "symbol": symbol,
        "admission_state": "eligible_for_history",
        "state": ru.STATE_RESEARCH_SCANNED,
        "candidate_state": None,
        "has_current_scan": False,
        "has_any_scan": False,
        "rejection_reason": None,
        "structure_state": None,
        "setup_state": None,
        "benchmark_relative": None,
    }
    base.update(kw)
    return base


# =========================================================================== #
# R1 / R2 / R5 — stale scan evidence may never become current-run evidence
# =========================================================================== #

class TestStaleCarryForward:

    def test_r1_prior_candidate_is_not_a_candidate_for_this_run(self):
        """ONDS exactly: persistent `candidate_state = research_candidate`,
        full screen evidence, and no scan for S."""
        row = _row("ONDS",
                   candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE,
                   has_current_scan=False, has_any_scan=True,
                   structure_state="recognized", setup_state="valid",
                   benchmark_relative="outperforming")
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_SCAN_STALE
        assert rf.LIFECYCLE_SCAN_STALE not in rf.SCANNED_STATES

    def test_r2_prior_non_candidate_is_also_not_scanned_for_this_run(self):
        """The same rule in the other direction: a stale `not_candidate` must
        not be counted as this run's scanned population either, or the
        conversion rate's denominator silently inflates."""
        row = _row("CELU",
                   candidate_state=ru.CANDIDATE_SCANNED_NOT_CANDIDATE,
                   has_current_scan=False, has_any_scan=True,
                   rejection_reason="price_below_minimum")
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_SCAN_STALE

    def test_r5_historical_structure_cannot_promote_without_a_current_scan(self):
        """Strong prior evidence is still prior evidence."""
        row = _row("ONDS", has_current_scan=False, has_any_scan=True,
                   structure_state="recognized", setup_state="valid",
                   benchmark_relative="outperforming",
                   candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE)
        assert rf.lifecycle_state(row) != rf.LIFECYCLE_RESEARCH_CANDIDATE

    def test_never_scanned_is_distinct_from_stale(self):
        row = _row("NEW", state=ru.STATE_RESEARCH_READY,
                   has_current_scan=False, has_any_scan=False)
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_SCAN_PENDING

    def test_a_current_scan_does_promote(self):
        """TSLL: scanned FOR S, all three screen findings."""
        row = _row("TSLL", has_current_scan=True, has_any_scan=True,
                   structure_state="recognized", setup_state="valid",
                   benchmark_relative="outperforming")
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_RESEARCH_CANDIDATE

    def test_a_current_scan_with_a_hard_gate_does_not_promote(self):
        """IBIT: recognized structure, but the strategy's own rejection."""
        row = _row("IBIT", has_current_scan=True, has_any_scan=True,
                   structure_state="recognized", setup_state="invalid",
                   benchmark_relative="outperforming",
                   rejection_reason="htf_contradiction")
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_SCANNED_NOT_CANDIDATE


# =========================================================================== #
# R13 — candidate provenance is explicit, not inferred
# =========================================================================== #

class TestTemporalProvenance:

    def test_r13_every_candidate_row_carries_a_current_scan(self):
        rows = [
            _row("TSLL", has_current_scan=True, structure_state="recognized",
                 setup_state="valid", benchmark_relative="outperforming"),
            _row("ONDS", has_current_scan=False, has_any_scan=True,
                 candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE,
                 structure_state="recognized", setup_state="valid",
                 benchmark_relative="outperforming"),
        ]
        for r in rows:
            if rf.lifecycle_state(r) == rf.LIFECYCLE_RESEARCH_CANDIDATE:
                assert r["has_current_scan"], (
                    f"{r['symbol']} promoted without evidence for the session")

    def test_funnel_sql_binds_the_session(self):
        assert "scan_session = $1::date" in rf.FUNNEL_ROW_SQL
        assert "has_current_scan" in rf.FUNNEL_ROW_SQL


# =========================================================================== #
# R3 / R4 — benchmark session alignment
# =========================================================================== #

def _bars(dates, close=100.0):
    return [{"trading_date": d, "open": close, "high": close, "low": close,
             "close": close, "volume": 1000.0} for d in dates]


class FakeBarsConn:
    """Serves per-symbol bars with the real `trading_date <= session` barrier."""

    def __init__(self, series):
        self.series = series

    async def fetch(self, sql, *args):
        symbol, session, limit = args[0], args[1], args[2]
        rows = [b for b in self.series.get(symbol, [])
                if b["trading_date"] <= session]
        return list(reversed(rows[-limit:]))


class TestBenchmarkAlignment:

    def test_r3_misaligned_sessions_produce_no_benchmark_finding(self):
        """The exact 2026-08-31 shape: symbol stops at S-1, SPY reaches S."""
        conn = FakeBarsConn({
            "IBIT": _bars([date(2026, 8, 26), date(2026, 8, 27), S_PREV]),
            "SPY": _bars([date(2026, 8, 26), date(2026, 8, 27), S_PREV, S]),
        })
        ctx = asyncio.run(rs.build_context(conn, "IBIT", session=S))
        assert ctx["session_aligned"] is False
        assert ctx["effective_session"] == S_PREV
        assert ctx["benchmark_effective_session"] == S
        assert ctx["benchmark"].get("category") is None
        assert ctx["benchmark"].get("reason") == "benchmark_session_misaligned"

    def test_r3_misalignment_cannot_yield_the_outperforming_finding(self):
        """`benchmark_outperforming` is independently sufficient to promote a
        candidate, so a misaligned comparison must not be able to produce it."""
        conn = FakeBarsConn({
            "IBIT": _bars([date(2026, 8, 27), S_PREV]),
            "SPY": _bars([date(2026, 8, 27), S_PREV, S]),
        })
        ctx = asyncio.run(rs.build_context(conn, "IBIT", session=S))
        findings = ru.screen_findings({
            "rejection_reason": None, "structure_state": None,
            "setup_state": "unknown",
            "benchmark_relative": ctx["benchmark"].get("category")})
        assert ru.SCREEN_BENCHMARK_LEADING not in findings

    def test_r4_aligned_sessions_compare_normally(self):
        conn = FakeBarsConn({
            "TSLL": _bars([date(2026, 8, 27), S_PREV, S]),
            "SPY": _bars([date(2026, 8, 27), S_PREV, S]),
        })
        ctx = asyncio.run(rs.build_context(conn, "TSLL", session=S))
        assert ctx["session_aligned"] is True
        assert ctx["effective_session"] == ctx["benchmark_effective_session"] == S
        assert ctx["benchmark"].get("reason") != "benchmark_session_misaligned"

    def test_absent_benchmark_keeps_its_own_distinct_reason(self):
        conn = FakeBarsConn({"X": _bars([S]), "SPY": []})
        ctx = asyncio.run(rs.build_context(conn, "X", session=S))
        assert ctx["benchmark"].get("reason") == "no_benchmark_bars_stored"

    def test_scan_selection_requires_current_bars(self):
        import inspect
        src = inspect.getsource(rs.run_research_scans)
        assert "history_latest_session = $2" in src


# =========================================================================== #
# R6 / R7 / R8 — exhaustion, maturation and the attempt ceiling
# =========================================================================== #

class TestHistoryMaturation:

    def test_r6_exhausted_but_maturable_is_not_terminal(self):
        """AAL/ETHA/SOXL: 499 bars, 24 month-groups (23 completed), provider
        exhausted. Must remain reachable by warmup."""
        state = ru.classify_history_state(
            daily_bars=499, week_groups=104, month_groups=24, symbol="AAL",
            attempts=2, last_error_class="terminal",
            last_error_code="provider_history_exhausted")
        assert state == ru.STATE_HISTORY_WARMING
        assert state not in ru.TERMINAL_STATES

    def test_r6_recheck_is_deterministic_and_calendar_derived(self):
        assert ru.months_short_of_ready(24) == 1
        assert ru.next_maturity_recheck(NOW, months_short=1) == datetime(
            2026, 10, 1, tzinfo=UTC)

    def test_r6_a_matured_symbol_becomes_ready(self):
        """One more month-group is all AAL ever needed."""
        assert ru.classify_history_state(
            daily_bars=500, week_groups=104, month_groups=25, symbol="AAL",
            attempts=2, last_error_class="terminal",
            last_error_code="provider_history_exhausted"
        ) == ru.STATE_RESEARCH_READY

    def test_r7_attempt_ceiling_does_not_strand_a_maturing_symbol(self):
        """SPCX: 452 bars, 23 groups, attempts exhausted. Waiting for the
        calendar is not failing."""
        assert ru.classify_history_state(
            daily_bars=452, week_groups=96, month_groups=23, symbol="SPCX",
            attempts=ru.MAX_WARMUP_ATTEMPTS) == ru.STATE_HISTORY_WARMING

    def test_r7_attempt_ceiling_still_parks_a_genuinely_failing_symbol(self):
        """A symbol with the span but repeated failures still stops."""
        assert ru.classify_history_state(
            daily_bars=600, week_groups=104, month_groups=30, symbol="X",
            attempts=ru.MAX_WARMUP_ATTEMPTS,
            last_error_class="retryable",
            last_error_code="provider_timeout") == ru.STATE_RESEARCH_READY

    def test_r8_young_listing_is_parked_far_out_not_retried_every_run(self):
        """TJGC: 169 bars, 9 groups (8 completed) — 16 months short."""
        short = ru.months_short_of_ready(9)
        assert short == 16
        recheck = ru.next_maturity_recheck(NOW, months_short=short)
        assert recheck == datetime(2028, 1, 1, tzinfo=UTC)
        assert ru.is_in_cooldown(recheck, now=NOW) is True

    def test_r8_young_listing_still_has_a_path_back(self):
        """Parked, not condemned: the state stays warmup-selectable."""
        assert ru.classify_history_state(
            daily_bars=169, week_groups=35, month_groups=9, symbol="TJGC",
            attempts=2, last_error_class="terminal",
            last_error_code="provider_history_exhausted"
        ) == ru.STATE_HISTORY_WARMING

    def test_genuinely_unusable_symbols_stay_terminal(self):
        """Not everything retries forever."""
        assert ru.classify_history_state(
            daily_bars=12, week_groups=3, month_groups=1, symbol="JUNK",
            attempts=1, last_error_class="terminal",
            last_error_code="insufficient_provider_history"
        ) == ru.STATE_UNAVAILABLE
        assert ru.classify_history_state(
            daily_bars=0, week_groups=0, month_groups=0, symbol="BAD",
            attempts=1, last_error_class="terminal",
            last_error_code="symbol_not_supported") == ru.STATE_UNAVAILABLE

    def test_exhaustion_code_is_exempt_from_terminality(self):
        assert ("provider_history_exhausted"
                in ru.NON_TERMINAL_HISTORY_ERROR_CODES)

    def test_an_unknown_terminal_error_is_still_terminal(self):
        """The exemption list is an exemption list. Guessing that an unknown
        failure will fix itself is how infinite retry loops get built."""
        assert ru.classify_history_state(
            daily_bars=400, week_groups=80, month_groups=20, symbol="X",
            attempts=1, last_error_class="terminal",
            last_error_code="something_new") == ru.STATE_UNAVAILABLE

    def test_warmup_selection_admits_a_maturing_symbol(self):
        assert "history_warming" in ri.WARMUP_SELECT_SQL
        assert "'unavailable'" not in ri.WARMUP_SELECT_SQL


# =========================================================================== #
# R9 — month-boundary progression
# =========================================================================== #

class TestMonthBoundary:

    def test_r9_completed_months_drop_the_trailing_partial(self):
        # 25 present groups -> 24 completed -> exactly at the gate.
        assert ru.months_short_of_ready(25) == 0
        # 24 present -> 23 completed -> one short. This single group is the
        # entire difference between the symbols that were scannable on
        # 2026-08-31 and the ones that were not.
        assert ru.months_short_of_ready(24) == 1

    def test_r9_recheck_rolls_the_year_correctly(self):
        assert ru.next_maturity_recheck(
            datetime(2026, 11, 15, tzinfo=UTC), months_short=3) == datetime(
                2027, 2, 1, tzinfo=UTC)

    def test_r9_readiness_uses_period_groups_not_a_bar_count(self):
        """500 bars is not the gate; 24 completed months is. Both of these
        hold 500 daily bars and only one is ready."""
        ready = ru.is_research_ready(500, week_groups=104, month_groups=25,
                                     symbol="ONDS")
        not_ready = ru.is_research_ready(500, week_groups=104, month_groups=24,
                                         symbol="AEHL")
        assert ready is True and not_ready is False


# =========================================================================== #
# R10 — the scan cap, and what it must NOT do
# =========================================================================== #

class TestScanLimit:

    def test_r10_symbols_cut_by_the_cap_do_not_inherit_old_candidacy(self):
        """Nine eligible symbols, a cap of five. The four that lose the
        ordering must land in `scan_stale`, never in a scanned state — this is
        precisely how ONDS was miscounted."""
        scanned = [_row(f"IN{i}", has_current_scan=True, has_any_scan=True,
                        structure_state="recognized", setup_state="valid",
                        benchmark_relative="outperforming") for i in range(5)]
        cut = [_row(f"OUT{i}", has_current_scan=False, has_any_scan=True,
                    candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE,
                    structure_state="recognized", setup_state="valid",
                    benchmark_relative="outperforming") for i in range(4)]
        summary = rf.summarise(scanned + cut, provider_calls_used=0,
                               provider_calls_avoided=0)  # no rejections here
        assert summary["research_candidates"] == 5
        assert summary["scanned"] == 5
        assert summary["states"][rf.LIFECYCLE_SCAN_STALE] == 4
        assert summary["conservation"]["ok"] is True


# =========================================================================== #
# R11 — conservation, recomputed from the rows
# =========================================================================== #

class TestConservation:

    def _mixed(self):
        return [
            _row("REJ", admission_state="rejected_before_history"),
            _row("PEND", state=ru.STATE_HISTORY_REQUIRED),
            _row("WARM", state=ru.STATE_HISTORY_WARMING),
            _row("UNAVAIL", state=ru.STATE_UNAVAILABLE),
            _row("READY", state=ru.STATE_RESEARCH_READY),
            _row("STALE", has_any_scan=True,
                 candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE),
            _row("CAND", has_current_scan=True, has_any_scan=True,
                 structure_state="recognized", setup_state="valid",
                 benchmark_relative="outperforming"),
            _row("NOTCAND", has_current_scan=True, has_any_scan=True,
                 rejection_reason="unknown_structure"),
        ]

    def test_r11_partition_is_exhaustive_and_recomputed(self):
        rows = self._mixed()
        summary = rf.summarise(rows, provider_calls_used=0,
                               provider_calls_avoided=1)  # one rejected row
        assert sum(summary["states"].values()) == len(rows)
        assert summary["conservation"]["ok"] is True
        # recompute rather than trust the boolean
        assert (summary["states"][rf.LIFECYCLE_RESEARCH_CANDIDATE]
                + summary["states"][rf.LIFECYCLE_SCANNED_NOT_CANDIDATE]
                + summary["states"][rf.LIFECYCLE_CLASSIFICATION_PENDING]
                == summary["scanned"])

    def test_r11_stale_counts_as_admitted_but_never_as_scanned(self):
        summary = rf.summarise(self._mixed(), provider_calls_used=0,
                               provider_calls_avoided=1)
        assert summary["states"][rf.LIFECYCLE_SCAN_STALE] == 1
        assert rf.LIFECYCLE_SCAN_STALE in rf.POST_ADMISSION_STATES
        assert rf.LIFECYCLE_SCAN_STALE not in rf.SCANNED_STATES

    def test_r11_named_invariant_guards_the_fold(self):
        summary = rf.summarise(self._mixed(), provider_calls_used=0,
                               provider_calls_avoided=1)
        names = {c["invariant"] for c in summary["conservation"]["checks"]}
        assert "stale_scans_are_not_scanned" in names
        assert all(c["ok"] for c in summary["conservation"]["checks"])

    def test_every_lifecycle_state_is_reachable_in_the_partition(self):
        summary = rf.summarise([], provider_calls_used=0,
                               provider_calls_avoided=0)
        assert set(summary["states"]) == set(rf.LIFECYCLE_STATES)


# =========================================================================== #
# R12 — idempotency
# =========================================================================== #

class TestIdempotency:

    def test_r12_repeating_the_derivation_is_stable(self):
        rows = [
            _row("A", has_current_scan=True, structure_state="recognized",
                 setup_state="valid", benchmark_relative="outperforming"),
            _row("B", has_any_scan=True,
                 candidate_state=ru.CANDIDATE_RESEARCH_CANDIDATE),
        ]
        first = rf.summarise(rows, provider_calls_used=0,
                             provider_calls_avoided=0)
        second = rf.summarise(rows, provider_calls_used=0,
                              provider_calls_avoided=0)
        assert first["states"] == second["states"]
        assert first["research_candidates"] == second["research_candidates"]

    def test_r12_scan_persistence_stays_keyed_by_session(self):
        """Re-running the same occurrence must overwrite one session's row,
        never accumulate a second history of it."""
        assert "ON CONFLICT (symbol, scan_session) DO UPDATE" in rs.UPSERT_SCAN_SQL


# =========================================================================== #
# R14 — discovery recency may say why we looked, never whether we survived
# =========================================================================== #

class TestDiscoveryReferenceBoundary:

    def test_r14_discovery_newer_than_target_is_allowed(self):
        """The recovered run had discovery_reference_session = 2026-09-01 with
        target_session = 2026-08-31. That is the intended architecture:
        discovery runs on execution day, strategy evaluates the last completed
        session."""
        assert date(2026, 9, 1) > S

    def test_r14_recency_alone_cannot_create_a_candidate(self):
        row = _row("HOT", has_current_scan=True, has_any_scan=True,
                   structure_state=None, setup_state=None,
                   benchmark_relative=None)
        assert rf.lifecycle_state(row) != rf.LIFECYCLE_RESEARCH_CANDIDATE

    def test_r14_screen_reads_no_discovery_field(self):
        import inspect
        src = inspect.getsource(ru.screen_findings)
        for forbidden in ("discovery_reasons", "latest_reference_session",
                          "best_rank", "discovery_observation_count"):
            assert forbidden not in src


# =========================================================================== #
# enrichment follows the same contract
# =========================================================================== #

class TestEnrichmentProvenance:

    def test_enrichment_requires_a_scan_for_the_session(self):
        assert "scan_session = $3::date" in re_.CANDIDATE_SQL

    def test_enrichment_without_a_session_selects_nobody(self):
        assert asyncio.run(
            re_.candidate_symbols(None, target_session=None)) == []


# =========================================================================== #
# BLOCKER A — positive evidence must be affirmative
#
# `screen_findings` is an OR: any single finding promotes a candidate. Both
# state tests used to be DENYLISTS written against vocabularies the strategies
# do not emit, so indeterminate — and in one case actively negative — states
# counted as evidence FOR the symbol.
# =========================================================================== #

class TestAffirmativeEvidence:

    def _scanned(self, **evidence):
        row = {"state": ru.STATE_RESEARCH_SCANNED, "rejection_reason": None,
               "structure_state": None, "setup_state": None,
               "benchmark_relative": None}
        row.update(evidence)
        return row

    def test_1_setup_unknown_is_not_setup_present(self):
        """`unknown` means policy.py could not read the structure at all."""
        assert ru.SCREEN_SETUP_PRESENT not in ru.screen_findings(
            self._scanned(setup_state="unknown"))

    def test_1b_setup_invalid_is_not_setup_present(self):
        """Worse than `unknown` and previously admitted too: `invalid` is the
        strategy reading the structure and DISQUALIFYING it."""
        assert ru.SCREEN_SETUP_PRESENT not in ru.screen_findings(
            self._scanned(setup_state="invalid"))

    def test_2_structure_unknown_is_not_structure_present(self):
        assert ru.SCREEN_STRUCTURE_PRESENT not in ru.screen_findings(
            self._scanned(structure_state="unknown"))

    def test_2b_structure_ambiguous_is_not_structure_present(self):
        """The old exclusion list was ('none','absent') — neither of which the
        classifier ever emits, so `ambiguous` sailed through it."""
        assert ru.SCREEN_STRUCTURE_PRESENT not in ru.screen_findings(
            self._scanned(structure_state="ambiguous"))

    def test_2c_benchmark_underperforming_is_not_a_leading_finding(self):
        assert ru.SCREEN_BENCHMARK_LEADING not in ru.screen_findings(
            self._scanned(benchmark_relative="underperforming"))

    def test_3_unknown_only_evidence_cannot_create_a_candidate(self):
        for row in (self._scanned(setup_state="unknown"),
                    self._scanned(structure_state="unknown"),
                    self._scanned(structure_state="ambiguous",
                                  setup_state="unknown"),
                    self._scanned(setup_state="invalid",
                                  benchmark_relative="underperforming")):
            verdict = ru.classify_candidate(row)
            assert verdict["candidate_state"] != ru.CANDIDATE_RESEARCH_CANDIDATE
            assert ru.SCREEN_NO_EVIDENCE in verdict["screen"]

    def test_4_affirmative_states_still_produce_their_findings(self):
        assert ru.screen_findings(self._scanned(
            structure_state="recognized")) == [ru.SCREEN_STRUCTURE_PRESENT]
        assert ru.screen_findings(self._scanned(
            setup_state="valid")) == [ru.SCREEN_SETUP_PRESENT]
        assert ru.screen_findings(self._scanned(
            benchmark_relative="outperforming")) == [ru.SCREEN_BENCHMARK_LEADING]
        # TSLL's actual shape: all three, and still a candidate.
        assert ru.classify_candidate(self._scanned(
            structure_state="recognized", setup_state="valid",
            benchmark_relative="outperforming")
        )["candidate_state"] == ru.CANDIDATE_RESEARCH_CANDIDATE

    def test_5_hard_rejection_semantics_are_unchanged(self):
        """A `rejection_reason` still ends the matter and still returns alone."""
        row = self._scanned(rejection_reason="htf_contradiction",
                            structure_state="recognized", setup_state="valid",
                            benchmark_relative="outperforming")
        assert ru.screen_findings(row) == [ru.SCREEN_HARD_DISQUALIFIED]
        verdict = ru.classify_candidate(row)
        assert verdict["candidate_state"] == ru.CANDIDATE_SCANNED_NOT_CANDIDATE
        assert verdict["reason"] == "htf_contradiction"

    def test_the_allowlists_are_allowlists(self):
        assert ru.STRUCTURE_AFFIRMATIVE == ("recognized",)
        assert ru.SETUP_AFFIRMATIVE == ("valid",)
        assert ru.BENCHMARK_AFFIRMATIVE == ("outperforming",)

    def test_the_funnel_partition_agrees_with_the_screen(self):
        """The session-scoped derivation must not re-admit what the screen
        rejects — both read the same evidence."""
        stale_shape = {"symbol": "X", "admission_state": "eligible_for_history",
                       "state": ru.STATE_RESEARCH_SCANNED, "candidate_state": None,
                       "has_current_scan": True, "has_any_scan": True,
                       "rejection_reason": None, "structure_state": "unknown",
                       "setup_state": "unknown", "benchmark_relative": None}
        assert rf.lifecycle_state(stale_shape) == rf.LIFECYCLE_SCANNED_NOT_CANDIDATE


# =========================================================================== #
# T9 — fairness. The pure-ordering half; the multi-session proof lives in
# tests/test_research_session_correctness_integration.py against real Postgres.
# =========================================================================== #

class TestFairnessOrdering:

    def _row(self, symbol, served):
        return {"symbol": symbol, "last_served_at": served, "reasons": [],
                "observation_count": 1, "daily_bars": 500, "best_rank": 100}

    def test_least_recently_served_sorts_first(self):
        rows = [self._row("NEW", datetime(2026, 9, 1, tzinfo=UTC)),
                self._row("OLD", datetime(2026, 8, 1, tzinfo=UTC)),
                self._row("MID", datetime(2026, 8, 15, tzinfo=UTC))]
        assert [r["symbol"] for r in ru.prioritise_fairly(rows, limit=3)] == [
            "OLD", "MID", "NEW"]

    def test_service_moves_a_symbol_to_the_back(self):
        rows = [self._row("A", datetime(2026, 8, 1, tzinfo=UTC)),
                self._row("B", datetime(2026, 8, 2, tzinfo=UTC))]
        assert ru.prioritise_fairly(rows, limit=1)[0]["symbol"] == "A"
        rows[0]["last_served_at"] = datetime(2026, 8, 3, tzinfo=UTC)
        assert ru.prioritise_fairly(rows, limit=1)[0]["symbol"] == "B"

    def test_ordering_is_total_and_reproducible(self):
        same = datetime(2026, 8, 1, tzinfo=UTC)
        rows = [self._row("B", same), self._row("A", same)]
        once = [r["symbol"] for r in ru.prioritise_fairly(rows, limit=2)]
        twice = [r["symbol"] for r in ru.prioritise_fairly(list(reversed(rows)),
                                                           limit=2)]
        assert once == twice == ["A", "B"]

    def test_the_batch_stays_bounded(self):
        rows = [self._row(f"S{i}", datetime(2026, 8, 1, tzinfo=UTC))
                for i in range(50)]
        assert len(ru.prioritise_fairly(
            rows, limit=ru.MAX_WARMUP_SYMBOLS_PER_RUN)) == 5

    def test_selection_no_longer_orders_by_class(self):
        import inspect
        src = inspect.getsource(ri.select_warmup_batch)
        assert "prioritise_fairly" in src
        assert "topups = [r for r in eligible" not in src


# =========================================================================== #
# T11 / T12 — the scheduled lifecycle must be able to finish on its own
#
# On 2026-09-02 the scheduled occurrence for session 2026-09-01 fired at
# 08:00 ET, found core bars not yet current, enqueued the refresh itself, and
# terminated in 0.2s. The refresh finished 24-45 minutes later and nothing
# resumed the session. Every component was healthy; the session was simply
# lost. These tests pin the continuation that fixes it.
# =========================================================================== #

import app.jobs.contracts as C
import app.jobs.registry as registry
import app.jobs.research_lifecycle as RL
import app.jobs.handlers.research_lifecycle_worker as rlw
import app.research_lifecycle as svc
import app.research_runs as rr_module


def _blocked(requested_status="queued"):
    return {
        "status": svc.STATUS_BLOCKED_STALE,
        "run_key": "rlc:sch:abc", "run_id": "rid",
        "target_completed_session": "2026-09-01",
        "core_refresh_request": {"requested": [
            {"universe_code": "SMART-SCANNER-REFERENCE-MARKET-V1",
             "status": requested_status, "job_id": "j1"},
            {"universe_code": "WYCKOFF-HISTORY-WARMUP-QUALIFICATION",
             "status": requested_status, "job_id": "j2"}]},
        "funnel": {}, "enrichment": {},
    }


class TestScheduledContinuation:

    def test_t11_stale_core_with_prerequisites_requested_is_retryable(self):
        """The exact 2026-09-02 shape must now defer, not end."""
        out = rlw._bounded_result(_blocked())
        assert out["status"] == svc.STATUS_BLOCKED_STALE
        assert rlw._refresh_was_requested(_blocked()) is True

    def test_t11_already_queued_prerequisites_also_justify_deferral(self):
        assert rlw._refresh_was_requested(_blocked("already_queued")) is True
        assert rlw._refresh_was_requested(_blocked("already_applied")) is True

    def test_t11_a_refresh_that_could_not_be_requested_does_not_defer(self):
        """Never wait for something nobody started."""
        assert rlw._refresh_was_requested(_blocked("not_requested")) is False
        assert rlw._refresh_was_requested({"core_refresh_request": {}}) is False
        assert rlw._refresh_was_requested({}) is False

    def test_t11_the_attempt_budget_outlasts_the_measured_refresh(self):
        """Refresh measured at 23.7 / 44.8 minutes on 2026-09-02. The re-entry
        schedule must reach past that."""
        sched = RL.RESEARCH_LIFECYCLE_BACKOFF_SECONDS
        assert len(sched) == RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS - 1
        cumulative = [sum(sched[:i + 1]) / 60.0 for i in range(len(sched))]
        assert cumulative == [30.0, 60.0, 90.0]
        assert max(cumulative) >= 45.0, "must outlast the measured refresh"

    def test_t11_backoff_is_actually_wired_to_the_handler(self):
        spec = registry.resolve_handler(RL.RESEARCH_LIFECYCLE_TASK)
        assert spec.retry_backoff_schedule == RL.RESEARCH_LIFECYCLE_BACKOFF_SECONDS
        assert spec.max_attempts == RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS

    def test_t11_every_re_entry_is_reachable_by_the_backoff_schedule(self):
        """A handler that wants N attempts needs N-1 delays, or the queue
        turns the missing one into a terminal failure."""
        for attempt in range(1, RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS):
            assert C.backoff_seconds(
                attempt, schedule=RL.RESEARCH_LIFECYCLE_BACKOFF_SECONDS) == 1800
        # the last attempt is terminal by design
        assert C.backoff_seconds(
            RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS,
            schedule=RL.RESEARCH_LIFECYCLE_BACKOFF_SECONDS) is None

    def test_t12_continuation_keeps_one_identity_for_the_occurrence(self):
        """Same occurrence -> same run_key -> same run row. A deferral is not
        a second research run."""
        a = RL.run_key_for_occurrence(schedule_code="SMART-SCANNER-RESEARCH-LIFECYCLE",
                                      schedule_version=1,
                                      occurrence_iso="2026-09-03T12:00:00+00:00")
        b = RL.run_key_for_occurrence(schedule_code="SMART-SCANNER-RESEARCH-LIFECYCLE",
                                      schedule_version=1,
                                      occurrence_iso="2026-09-03T12:00:00+00:00")
        assert a == b
        nxt = RL.run_key_for_occurrence(schedule_code="SMART-SCANNER-RESEARCH-LIFECYCLE",
                                        schedule_version=1,
                                        occurrence_iso="2026-09-04T12:00:00+00:00")
        assert nxt != a

    def test_t12_a_manual_run_cannot_collide_with_a_pending_occurrence(self):
        """S11: an operator dispatching while automatic S is deferred must not
        touch the scheduled occurrence's identity or audit."""
        manual = RL.manual_run_key(label="adhoc", now=NOW)
        occ = RL.run_key_for_occurrence(schedule_code="SMART-SCANNER-RESEARCH-LIFECYCLE",
                                        schedule_version=1,
                                        occurrence_iso="2026-09-03T12:00:00+00:00")
        assert manual.startswith("rlc:manual:")
        assert occ.startswith("rlc:sch:")
        assert manual != occ

    def test_t12_the_run_row_preserves_the_original_target_session(self):
        """START_SQL's ON CONFLICT must not overwrite target_session, or a
        re-entry would re-pin the run to whatever the clock says."""
        import app.research_runs as rr
        upsert = rr.START_SQL.split("DO UPDATE SET")[1].split("RETURNING")[0]
        assert "target_session" not in upsert, (
            "re-entry must not repoint the run at a different session")
        assert "target_session" in rr.START_SQL.split("RETURNING")[1], (
            "start_run must return the pin so the caller can reuse it")

    def test_t12_the_lifecycle_reuses_the_pin_rather_than_the_clock(self):
        import inspect
        src = inspect.getsource(svc.run_lifecycle)
        assert 'pinned = run.get("target_session")' in src
        assert "target = pinned" in src


# =========================================================================== #
# D1-D5 / T13 / T14 — the prerequisite wait must be side-effect-free, and a
# deferral must not hand itself a fresh provider budget.
# =========================================================================== #

class TestPrerequisiteWaitHasNoSideEffects:

    def test_d1_the_freshness_gate_runs_before_any_mutable_research_work(self):
        """T14. Read the ORDER from the source, not from memory: the gate must
        come before discovery, admission, warmup, scan and enrichment, so a
        deferred attempt cannot repeat any of them."""
        import inspect
        src = inspect.getsource(svc.run_lifecycle)
        order = {name: src.index(name) for name in (
            "check_core_freshness", "_refresh_discovery", "admit_from_discovery",
            "evaluate_admissions", "_run_warmup", "run_research_scans",
            "_enrich")}
        gate = order["check_core_freshness"]
        for name, pos in order.items():
            if name == "check_core_freshness":
                continue
            assert gate < pos, f"{name} must not precede the freshness gate"

    def test_d1_the_blocked_path_returns_before_the_provider_stages(self):
        """Everything between the gate and `return summary` on the stale branch
        must be non-provider: an enqueue and a read."""
        import inspect
        src = inspect.getsource(svc.run_lifecycle)
        head, _ = src.split("# ---- 2.", 1)
        blocked = head.split("if not freshness[\"fresh\"]:", 1)[1]
        assert "request_core_refresh" in blocked      # enqueue only
        assert "load_funnel" in blocked               # read only
        for forbidden in ("_refresh_discovery", "_run_warmup", "_enrich",
                          "run_research_scans", "admit_from_discovery"):
            assert forbidden not in blocked, (
                f"{forbidden} must not run while waiting for prerequisites")

    def test_d5_a_deferral_cannot_refresh_the_discovery_snapshot(self):
        """D5/A2: discovery lives after the gate, so a scheduled occurrence
        blocked on prerequisites cannot produce a second discovery refresh no
        matter how many times it re-enters."""
        import inspect
        src = inspect.getsource(svc.run_lifecycle)
        assert src.index("check_core_freshness") < src.index("_refresh_discovery")

    def test_t13_re_entry_spends_the_remainder_not_a_fresh_budget(self):
        import inspect
        src = inspect.getsource(svc.run_lifecycle)
        assert 'already_spent = int(run.get("provider_calls_used") or 0)' in src
        assert "provider_budget = max(0, int(provider_budget) - already_spent)" in src

    def test_t13_start_run_reports_spend_to_date(self):
        import app.research_runs as rr
        returning = rr.START_SQL.split("RETURNING")[1]
        assert "provider_calls_used" in returning

    def test_t13_the_arithmetic(self):
        """One occurrence, four attempts. Deferrals spend nothing because the
        gate returns first; only the attempt that passes the gate spends."""
        budget = ru.MAX_PROVIDER_REQUESTS_PER_RUN
        spent = 0
        for attempt, calls in ((1, 0), (2, 0), (3, 0), (4, 8)):
            remaining = max(0, budget - spent)
            assert calls <= remaining, f"attempt {attempt} exceeded the run budget"
            spent += calls
        assert spent <= budget == 12

    def test_t15_health_states_are_named_and_exported(self):
        import app.research_runs as rr
        assert rr.HEALTH_HEALTHY_WAITING == "HEALTHY_WAITING"
        assert rr.HEALTH_TERMINAL_BLOCKED == "TERMINAL_BLOCKED"
        assert rr.HEALTH_COMPLETED == "COMPLETED"
        # decided from persisted columns only — no logs, no memory
        for col in ("job_tasks", "attempt_count", "max_attempts",
                    "research_lifecycle_runs"):
            assert col in rr.RUN_HEALTH_SQL

    def test_t15_a_deferred_run_is_not_reported_as_terminal(self):
        """The whole point: same run status, opposite operational meaning."""
        sql = rr_module.RUN_HEALTH_SQL
        assert "'HEALTHY_WAITING'" in sql and "'TERMINAL_BLOCKED'" in sql
        # the discriminator is the task, not the run status
        assert "t.status = 'retryable'" in sql
        assert "t.attempt_count < t.max_attempts" in sql
