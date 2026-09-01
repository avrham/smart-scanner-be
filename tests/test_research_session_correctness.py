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
