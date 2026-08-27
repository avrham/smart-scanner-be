"""Market-context layer (app/market_context.py + its Product API exposure).

The layer's whole value depends on it being TRUTHFUL about what it can and
cannot measure, so most of these tests are about the boundary between
`available`, `insufficient_history` and `unavailable` — not about arithmetic.
"""

import math

import pytest

import app.market_context as mc
import app.scanner_view as sv


def bars(closes, volumes=None):
    """Ascending daily bars; volume defaults to a flat series."""
    vols = volumes if volumes is not None else [1_000_000.0] * len(closes)
    return [{"close": float(c), "volume": float(v)} for c, v in zip(closes, vols)]


def rising(n, start=100.0, step=1.0):
    return bars([start + step * i for i in range(n)])


# --------------------------------------------------------------------------- #
# Horizon returns
# --------------------------------------------------------------------------- #

class TestHorizonReturn:
    def test_computes_percent_over_the_requested_bars(self):
        b = bars([100.0, 101.0, 102.0, 103.0, 104.0, 110.0])
        assert mc.horizon_return_pct(b, 5) == pytest.approx(10.0)

    def test_needs_one_more_bar_than_the_horizon(self):
        assert mc.horizon_return_pct(bars([100.0] * 5), 5) is None
        assert mc.horizon_return_pct(bars([100.0] * 6), 5) == pytest.approx(0.0)

    def test_guards_a_zero_reference_price(self):
        b = bars([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        assert mc.horizon_return_pct(b, 5) is None

    def test_never_returns_nan_or_inf(self):
        for series in ([0.0] * 10, [1e-12] * 10, [100.0] * 10):
            r = mc.horizon_return_pct(bars(series), 5)
            assert r is None or math.isfinite(r)


class TestPercentileRank:
    def test_ranks_within_the_population(self):
        assert mc.percentile_rank(5.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(100.0)
        assert mc.percentile_rank(1.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(0.0)
        assert mc.percentile_rank(3.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(50.0)

    def test_a_population_of_one_cannot_be_ranked(self):
        assert mc.percentile_rank(1.0, [1.0]) is None
        assert mc.percentile_rank(1.0, []) is None


class TestClassification:
    def test_equal_thirds_of_the_cross_section(self):
        assert mc.classify_relative_strength(90.0) == mc.RS_OUTPERFORMING
        assert mc.classify_relative_strength(50.0) == mc.RS_IN_LINE
        assert mc.classify_relative_strength(10.0) == mc.RS_UNDERPERFORMING

    def test_unrankable_has_no_category(self):
        assert mc.classify_relative_strength(None) is None

    def test_volume_thresholds(self):
        assert mc.classify_volume(2.0) == mc.VOLUME_ELEVATED
        assert mc.classify_volume(1.0) == mc.VOLUME_NORMAL
        assert mc.classify_volume(0.3) == mc.VOLUME_LIGHT
        assert mc.classify_volume(None) is None


# --------------------------------------------------------------------------- #
# Relative strength
# --------------------------------------------------------------------------- #

class TestRelativeStrength:
    def _universe(self, n=25, length=70):
        # each symbol rises at a different rate, so the cross-section is ordered
        return {
            f"S{i:02d}": bars([100.0 + i * 0.1 * k for k in range(length)])
            for i in range(n)
        }

    def test_strongest_symbol_is_outperforming(self):
        u = self._universe()
        rs = mc.build_relative_strength("S24", u)
        assert rs["status"] == mc.STATUS_AVAILABLE
        assert rs["category"] == mc.RS_OUTPERFORMING
        assert rs["excess_pct"] > 0

    def test_weakest_symbol_is_underperforming(self):
        rs = mc.build_relative_strength("S00", self._universe())
        assert rs["category"] == mc.RS_UNDERPERFORMING

    def test_comparator_is_named_as_the_universe_not_the_market(self):
        rs = mc.build_relative_strength("S05", self._universe())
        assert rs["comparator"] == mc.COMPARATOR_UNIVERSE_MEDIAN
        assert "market" not in rs["comparator"]
        assert rs["comparator_symbol_count"] == 25

    def test_reports_every_declared_horizon(self):
        rs = mc.build_relative_strength("S05", self._universe())
        assert [h["days"] for h in rs["horizons"]] == list(mc.RS_HORIZONS)
        assert rs["primary_horizon_days"] == mc.RS_PRIMARY_HORIZON

    def test_short_history_degrades_per_horizon_not_wholesale(self):
        # 30 bars: 5D and 20D resolve, 60D cannot
        u = {f"S{i}": rising(30, step=1 + i) for i in range(5)}
        rs = mc.build_relative_strength("S0", u)
        by_days = {h["days"]: h for h in rs["horizons"]}
        assert by_days[5]["status"] == mc.STATUS_AVAILABLE
        assert by_days[20]["status"] == mc.STATUS_AVAILABLE
        assert by_days[60]["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert by_days[60]["category"] is None

    def test_insufficient_primary_horizon_marks_the_whole_block(self):
        u = {f"S{i}": rising(10, step=1 + i) for i in range(5)}
        rs = mc.build_relative_strength("S0", u)
        assert rs["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert rs["category"] is None

    def test_symbol_absent_from_the_store_is_unavailable(self):
        rs = mc.build_relative_strength("NOPE", self._universe())
        assert rs["status"] == mc.STATUS_UNAVAILABLE
        assert rs["reason"] == "no_stored_bars_for_symbol"
        assert rs["category"] is None
        # the DTO shape must not change with availability
        for key in ("excess_pct", "percentile", "horizons", "comparator"):
            assert key in rs

    def test_a_single_symbol_universe_cannot_be_ranked(self):
        rs = mc.build_relative_strength("ONLY", {"ONLY": rising(70)})
        assert rs["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert rs["category"] is None

    def test_a_flat_universe_produces_no_spurious_ordering(self):
        u = {f"S{i:02d}": bars([100.0] * 70) for i in range(25)}
        rs = mc.build_relative_strength("S00", u)
        # everyone identical -> nobody is below anyone -> percentile 0
        assert rs["excess_pct"] == pytest.approx(0.0)
        assert math.isfinite(rs["percentile"])


# --------------------------------------------------------------------------- #
# Volume
# --------------------------------------------------------------------------- #

class TestVolumeContext:
    def test_detects_a_participation_spike(self):
        vols = [1_000_000.0] * 20 + [3_000_000.0]
        v = mc.build_volume_context(bars([100.0] * 21, vols))
        assert v["status"] == mc.STATUS_AVAILABLE
        assert v["category"] == mc.VOLUME_ELEVATED
        assert v["relative_volume"] == pytest.approx(3.0)

    def test_detects_light_participation(self):
        vols = [1_000_000.0] * 20 + [200_000.0]
        v = mc.build_volume_context(bars([100.0] * 21, vols))
        assert v["category"] == mc.VOLUME_LIGHT

    def test_the_session_is_excluded_from_its_own_average(self):
        # a huge session must not dilute the baseline it is compared against
        vols = [1_000_000.0] * 20 + [10_000_000.0]
        v = mc.build_volume_context(bars([100.0] * 21, vols))
        assert v["average_volume"] == pytest.approx(1_000_000.0)
        assert v["relative_volume"] == pytest.approx(10.0)

    def test_short_history_is_insufficient_not_zero(self):
        v = mc.build_volume_context(bars([100.0] * 10))
        assert v["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert v["category"] is None
        assert v["relative_volume"] is None

    def test_a_zero_trailing_average_never_divides_by_zero(self):
        vols = [0.0] * 20 + [500_000.0]
        v = mc.build_volume_context(bars([100.0] * 21, vols))
        assert v["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert v["relative_volume"] is None
        assert v["category"] is None

    def test_all_zero_volume_is_handled(self):
        v = mc.build_volume_context(bars([100.0] * 21, [0.0] * 21))
        assert v["relative_volume"] is None
        assert v["category"] is None

    def test_null_volume_is_treated_as_zero_not_a_crash(self):
        b = [{"close": 100.0, "volume": None} for _ in range(21)]
        v = mc.build_volume_context(b)
        assert v["status"] == mc.STATUS_INSUFFICIENT_HISTORY


# --------------------------------------------------------------------------- #
# Breadth — of the scanner universe, and never claimed to be more
# --------------------------------------------------------------------------- #

class TestUniverseBreadth:
    def test_scope_is_the_scanner_universe_not_the_market(self):
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        b = mc.build_universe_breadth(u)
        assert b["scope"] == mc.BREADTH_SCOPE_SCANNER_UNIVERSE
        assert b["scope"] != "market"
        assert b["symbol_count"] == 25

    def test_counts_advancers_over_each_window(self):
        up = {f"U{i}": rising(70) for i in range(3)}
        down = {f"D{i}": bars([200.0 - k for k in range(70)]) for i in range(1)}
        b = mc.build_universe_breadth({**up, **down})
        assert b["positive_5d"]["positive"] == 3
        assert b["positive_5d"]["measured"] == 4
        assert b["positive_5d"]["pct"] == pytest.approx(75.0)

    def test_empty_store_is_unavailable(self):
        b = mc.build_universe_breadth({})
        assert b["status"] == mc.STATUS_UNAVAILABLE
        assert b["symbol_count"] == 0

    def test_symbols_too_short_for_the_trend_window_are_excluded_not_guessed(self):
        u = {"SHORT": rising(10), "LONG": rising(70)}
        b = mc.build_universe_breadth(u)
        assert b["above_trend"]["measured"] == 1


# --------------------------------------------------------------------------- #
# The dimensions we cannot measure, and the isolation that keeps them honest
# --------------------------------------------------------------------------- #

class TestUnavailableDimensions:
    def test_benchmark_is_reported_unavailable_never_proxied(self):
        b = mc.build_benchmark_context()
        assert b["status"] == mc.STATUS_UNAVAILABLE
        assert b["reason"] == "no_benchmark_series_stored"

    def test_sector_is_reported_unavailable_never_invented(self):
        s = mc.build_sector_context()
        assert s["status"] == mc.STATUS_UNAVAILABLE
        assert s["reason"] == "no_sector_metadata_stored"

    def test_relative_strength_never_claims_a_benchmark_it_lacks(self):
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        ctx = mc.build_market_context("S00", u, as_of_session="2026-08-25")
        assert ctx["benchmark_context"]["status"] == mc.STATUS_UNAVAILABLE
        assert ctx["relative_strength"]["comparator"] == mc.COMPARATOR_UNIVERSE_MEDIAN

    def test_context_universe_is_exactly_what_was_supplied(self):
        """Candidate/benchmark isolation.

        Relative strength ranks against the symbols handed in — the scan's own
        pair-derived universe — so no non-candidate series can silently enter
        the comparison, and adding a benchmark later cannot make it a candidate.
        """
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        rs = mc.build_relative_strength("S00", u)
        assert rs["comparator_symbol_count"] == 25
        for h in rs["horizons"]:
            assert h["comparator_symbol_count"] <= 25


class TestMarketContextDto:
    def test_schema_is_stable_and_versioned(self):
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        ctx = mc.build_market_context("S05", u, as_of_session="2026-08-25")
        assert ctx["contract_version"] == mc.MARKET_CONTEXT_CONTRACT_VERSION
        assert ctx["as_of_session"] == "2026-08-25"
        assert set(ctx) == {
            "contract_version", "as_of_session", "relative_strength",
            "volume_context", "benchmark_context", "sector_context",
        }

    def test_every_dimension_declares_an_explicit_status(self):
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        ctx = mc.build_market_context("S05", u, as_of_session="2026-08-25")
        for key in ("relative_strength", "volume_context", "benchmark_context",
                    "sector_context"):
            assert ctx[key]["status"] in mc.CONTEXT_STATUSES

    def test_context_for_an_unscanned_symbol_degrades_cleanly(self):
        ctx = mc.build_market_context("NOPE", {}, as_of_session=None)
        assert ctx["relative_strength"]["status"] == mc.STATUS_UNAVAILABLE
        assert ctx["volume_context"]["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert ctx["as_of_session"] is None


class TestRowContext:
    def test_row_context_stays_compact(self):
        u = {f"S{i}": rising(70, step=1 + i) for i in range(25)}
        row = sv.build_row_context("S24", u)
        assert set(row) == {
            "relative_strength", "relative_strength_status",
            "relative_strength_excess_pct", "relative_strength_horizon_days",
            "comparator", "volume", "volume_status", "relative_volume",
        }
        assert row["relative_strength"] == mc.RS_OUTPERFORMING
        assert row["comparator"] == mc.COMPARATOR_UNIVERSE_MEDIAN

    def test_row_context_is_absent_of_history_not_wrong(self):
        row = sv.build_row_context("X", {"X": rising(3)})
        assert row["relative_strength"] is None
        assert row["relative_strength_status"] != mc.STATUS_AVAILABLE
        assert row["volume"] is None


class TestContextDoesNotTouchTheStrategy:
    def test_attention_classification_takes_no_context_input(self):
        """Context must never be able to change a verdict or a tier."""
        import inspect
        params = set(inspect.signature(sv.classify_attention).parameters)
        assert params == {
            "has_candidate_result", "candidate_verdict", "setup_state",
            "readiness_status", "control_verdict",
        }
        for forbidden in ("relative_strength", "volume", "breadth", "context"):
            assert forbidden not in params

    def test_attention_sort_key_ignores_context(self):
        strong = {"symbol": "AAA", "attention": sv.ATTENTION_LOW,
                  "setup_state": "invalid",
                  "market_context": {"relative_strength": mc.RS_OUTPERFORMING}}
        weak = {"symbol": "BBB", "attention": sv.ATTENTION_HIGH,
                "setup_state": "valid",
                "market_context": {"relative_strength": mc.RS_UNDERPERFORMING}}
        ordered = sorted([strong, weak], key=sv.attention_sort_key)
        assert [r["symbol"] for r in ordered] == ["BBB", "AAA"]
