"""Reference-market data: isolation from candidates, RS formulation, regime.

The first class is the important one. Reference symbols exist only to provide
context, and the product breaks in a serious way if one ever becomes a scanner
candidate — so isolation is asserted from several independent directions.
"""

import pytest

import app.market_context as mc
import app.reference_market as rm
import app.scanner_view as sv


def bars(closes, volumes=None):
    vols = volumes if volumes is not None else [1_000_000.0] * len(closes)
    return [{"close": float(c), "volume": float(v)} for c, v in zip(closes, vols)]


def flat(n, value=100.0):
    return bars([value] * n)


def compounding(n, start=100.0, pct_per_bar=1.0):
    out, price = [], start
    for _ in range(n):
        out.append(price)
        price *= 1 + pct_per_bar / 100.0
    return bars(out)


# --------------------------------------------------------------------------- #
# THE NON-NEGOTIABLE RULE
# --------------------------------------------------------------------------- #

class TestReferenceSymbolIsolation:
    def test_reference_universe_is_not_the_candidate_universe(self):
        assert rm.REFERENCE_UNIVERSE_CODE != rm.CANDIDATE_UNIVERSE_CODE

    def test_no_reference_symbol_is_a_candidate_symbol(self):
        candidates = set(rm.SYMBOL_SECTORS)
        references = set(rm.reference_symbols())
        assert candidates & references == set()

    def test_every_reference_symbol_is_recognised_as_such(self):
        for symbol in rm.reference_symbols():
            assert rm.is_reference_symbol(symbol) is True
            assert rm.reference_kind(symbol) in rm.REFERENCE_KINDS

    def test_candidate_symbols_are_never_reference_symbols(self):
        for symbol in rm.SYMBOL_SECTORS:
            assert rm.is_reference_symbol(symbol) is False
            assert rm.reference_kind(symbol) is None

    def test_the_guard_raises_rather_than_silently_filtering(self):
        with pytest.raises(ValueError) as exc:
            rm.assert_no_reference_symbols(["AAPL", "SPY", "MSFT"])
        assert "SPY" in str(exc.value)

    def test_the_guard_accepts_a_pure_candidate_list(self):
        rm.assert_no_reference_symbols(sorted(rm.SYMBOL_SECTORS))

    def test_the_guard_is_case_insensitive(self):
        with pytest.raises(ValueError):
            rm.assert_no_reference_symbols(["spy"])

    def test_reference_bars_do_not_enter_the_scanned_universe(self):
        """Context reads reference series from a SEPARATE mapping.

        `bars_by_symbol` is the scanned universe and `reference_bars` is the
        reference set; universe-relative strength and breadth only ever see the
        former, so a reference series cannot inflate symbol_count or breadth.
        """
        universe = {s: compounding(70) for s in list(rm.SYMBOL_SECTORS)[:25]}
        references = {s: compounding(70) for s in rm.reference_symbols()}
        breadth = mc.build_universe_breadth(universe)
        assert breadth["symbol_count"] == 25
        rs = mc.build_relative_strength("AAPL", universe)
        assert rs["comparator_symbol_count"] == 25
        # and nothing from the reference set leaked into the comparison
        for horizon in rs["horizons"]:
            assert horizon["comparator_symbol_count"] <= 25
        assert set(universe) & set(references) == set()

    def test_row_context_never_reports_a_reference_symbol_as_the_subject(self):
        universe = {"AAPL": compounding(70)}
        references = {s: compounding(70) for s in rm.reference_symbols()}
        row = sv.build_row_context("AAPL", universe, references)
        assert row["benchmark_symbol"] == rm.PRIMARY_BENCHMARK
        assert row["benchmark_symbol"] not in universe


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

class TestSectorRegistry:
    def test_every_candidate_symbol_has_a_sector(self):
        assert len(rm.SYMBOL_SECTORS) == 25

    def test_every_sector_has_a_benchmark_that_we_actually_ingest(self):
        ingested = set(rm.reference_symbols(rm.REFERENCE_SECTOR))
        for sector, etf in rm.SECTOR_BENCHMARKS.items():
            assert etf in ingested, f"{sector} -> {etf} is not in the reference set"

    def test_every_mapped_sector_is_a_known_sector(self):
        for symbol, sector in rm.SYMBOL_SECTORS.items():
            assert sector in rm.SECTOR_BENCHMARKS, f"{symbol} has unknown sector {sector}"

    def test_we_ingest_no_sector_etf_we_do_not_use(self):
        used = set(rm.SECTOR_BENCHMARKS.values())
        ingested = set(rm.reference_symbols(rm.REFERENCE_SECTOR))
        assert ingested == used

    def test_lookup_resolves_symbol_to_its_own_sector_benchmark(self):
        assert rm.sector_benchmark_for("AAPL") == "XLK"
        assert rm.sector_benchmark_for("JPM") == "XLF"
        assert rm.sector_benchmark_for("XOM") == "XLE"

    def test_unmapped_symbol_yields_no_benchmark_rather_than_a_default(self):
        assert rm.sector_for("NOTREAL") is None
        assert rm.sector_benchmark_for("NOTREAL") is None

    def test_provenance_is_auditable(self):
        p = rm.sector_registry_provenance()
        assert p["version"] and p["source"] and p["effective_date"]

    def test_every_reference_symbol_states_why_it_exists(self):
        for entry in rm.REFERENCE_SYMBOLS:
            assert entry.reason and len(entry.reason) > 20
            assert entry.name


# --------------------------------------------------------------------------- #
# Relative strength against a named reference
# --------------------------------------------------------------------------- #

class TestReferenceRelativeStrength:
    def test_relative_return_is_the_difference_of_percent_returns(self):
        symbol = compounding(70, pct_per_bar=1.0)
        reference = compounding(70, pct_per_bar=0.5)
        block = mc.build_reference_relative_strength(
            symbol, reference, reference_symbol="SPY",
            reference_kind=rm.REFERENCE_BROAD_MARKET)
        h20 = next(h for h in block["horizons"] if h["days"] == 20)
        assert h20["relative_return_pct"] == pytest.approx(
            h20["symbol_return_pct"] - h20["reference_return_pct"], abs=0.01)
        assert block["category"] == mc.REL_OUTPERFORMING

    def test_a_weaker_symbol_underperforms(self):
        block = mc.build_reference_relative_strength(
            compounding(70, pct_per_bar=0.1), compounding(70, pct_per_bar=1.0),
            reference_symbol="SPY", reference_kind=rm.REFERENCE_BROAD_MARKET)
        assert block["category"] == mc.REL_UNDERPERFORMING
        assert block["relative_return_pct"] < 0

    def test_an_identical_series_is_in_line(self):
        block = mc.build_reference_relative_strength(
            compounding(70), compounding(70),
            reference_symbol="SPY", reference_kind=rm.REFERENCE_BROAD_MARKET)
        assert block["category"] == mc.REL_IN_LINE
        assert block["relative_return_pct"] == pytest.approx(0.0, abs=0.001)

    def test_the_neutral_band_keeps_trivial_spreads_in_line(self):
        assert mc.classify_relative_return(0.5) == mc.REL_IN_LINE
        assert mc.classify_relative_return(-0.5) == mc.REL_IN_LINE
        assert mc.classify_relative_return(mc.REL_NEUTRAL_BAND_PCT + 0.1) == mc.REL_OUTPERFORMING
        assert mc.classify_relative_return(None) is None

    def test_a_missing_reference_is_unavailable_not_zero(self):
        block = mc.build_reference_relative_strength(
            compounding(70), None, reference_symbol="SPY",
            reference_kind=rm.REFERENCE_BROAD_MARKET,
            unavailable_reason="no_benchmark_series_stored")
        assert block["status"] == mc.STATUS_UNAVAILABLE
        assert block["reason"] == "no_benchmark_series_stored"
        assert block["category"] is None
        assert block["relative_return_pct"] is None

    def test_a_short_reference_degrades_per_horizon(self):
        block = mc.build_reference_relative_strength(
            compounding(70), compounding(30), reference_symbol="SPY",
            reference_kind=rm.REFERENCE_BROAD_MARKET)
        by_days = {h["days"]: h for h in block["horizons"]}
        assert by_days[5]["status"] == mc.STATUS_AVAILABLE
        assert by_days[20]["status"] == mc.STATUS_AVAILABLE
        assert by_days[60]["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert by_days[60]["relative_return_pct"] is None

    def test_horizons_align_both_series_on_the_same_window(self):
        block = mc.build_reference_relative_strength(
            compounding(70), compounding(70), reference_symbol="SPY",
            reference_kind=rm.REFERENCE_BROAD_MARKET)
        for h in block["horizons"]:
            if h["status"] == mc.STATUS_AVAILABLE:
                assert h["symbol_return_pct"] is not None
                assert h["reference_return_pct"] is not None

    def test_never_divides_by_a_zero_reference_price(self):
        block = mc.build_reference_relative_strength(
            compounding(70), bars([0.0] * 70), reference_symbol="SPY",
            reference_kind=rm.REFERENCE_BROAD_MARKET)
        assert block["status"] == mc.STATUS_INSUFFICIENT_HISTORY


class TestBenchmarkAndSectorBlocks:
    def _refs(self, **overrides):
        base = {s: compounding(70, pct_per_bar=0.5) for s in rm.reference_symbols()}
        base.update(overrides)
        return base

    def test_benchmark_block_names_the_primary_benchmark(self):
        block = mc.build_benchmark_relative_strength(
            "AAPL", {"AAPL": compounding(70, pct_per_bar=1.0)}, self._refs())
        assert block["reference_symbol"] == rm.PRIMARY_BENCHMARK
        assert block["reference_kind"] == rm.REFERENCE_BROAD_MARKET
        assert block["status"] == mc.STATUS_AVAILABLE

    def test_secondary_references_are_reported_but_are_not_the_benchmark(self):
        block = mc.build_benchmark_relative_strength(
            "AAPL", {"AAPL": compounding(70)}, self._refs())
        secondary = {b["reference_symbol"] for b in block["secondary_references"]}
        assert secondary == set(rm.SECONDARY_BENCHMARKS)
        assert rm.PRIMARY_BENCHMARK not in secondary

    def test_sector_block_uses_the_symbols_own_sector_etf(self):
        block = mc.build_sector_relative_strength(
            "JPM", {"JPM": compounding(70, pct_per_bar=1.0)}, self._refs())
        assert block["sector"] == "Financials"
        assert block["reference_symbol"] == "XLF"
        assert block["status"] == mc.STATUS_AVAILABLE

    def test_missing_sector_metadata_is_explicit_and_never_falls_back(self):
        block = mc.build_sector_relative_strength(
            "NOTREAL", {"NOTREAL": compounding(70)}, self._refs())
        assert block["status"] == mc.STATUS_UNAVAILABLE
        assert block["reason"] == "no_sector_metadata_for_symbol"
        # crucially it did NOT silently use the broad benchmark
        assert block["reference_symbol"] is None
        assert block["reference_symbol"] != rm.PRIMARY_BENCHMARK

    def test_missing_sector_history_is_explicit_and_never_falls_back(self):
        refs = {s: compounding(70) for s in rm.reference_symbols() if s != "XLF"}
        block = mc.build_sector_relative_strength(
            "JPM", {"JPM": compounding(70)}, refs)
        assert block["status"] == mc.STATUS_UNAVAILABLE
        assert block["reason"] == "no_sector_benchmark_series_stored"
        assert block["reference_symbol"] == "XLF"

    def test_sector_block_carries_registry_provenance(self):
        block = mc.build_sector_relative_strength(
            "AAPL", {"AAPL": compounding(70)}, self._refs())
        assert block["sector_registry"]["version"] == rm.SECTOR_REGISTRY_VERSION


# --------------------------------------------------------------------------- #
# Market Regime V1
# --------------------------------------------------------------------------- #

class TestMarketRegime:
    def test_rising_benchmark_above_its_trend_is_supportive(self):
        regime = mc.build_market_regime({"SPY": compounding(70, pct_per_bar=0.5)})
        assert regime["status"] == mc.STATUS_AVAILABLE
        assert regime["regime"] == mc.REGIME_SUPPORTIVE
        assert regime["trend"] == "above"
        assert regime["direction"] == "up"

    def test_falling_benchmark_below_its_trend_is_defensive(self):
        regime = mc.build_market_regime({"SPY": compounding(70, pct_per_bar=-0.5)})
        assert regime["regime"] == mc.REGIME_DEFENSIVE
        assert regime["trend"] == "below"
        assert regime["direction"] == "down"

    def test_disagreeing_evidence_is_mixed(self):
        # A steep 60-bar advance followed by a shallow 20-bar pullback: the
        # close is still well above its 50-session average (trend up) while the
        # 20-session return has turned negative (direction down).
        closes = [100.0 + 5 * i for i in range(60)] + [400.0 - 0.4 * (i + 1)
                                                        for i in range(20)]
        regime = mc.build_market_regime({"SPY": bars(closes)})
        assert regime["trend"] == "above"
        assert regime["direction"] == "down"
        assert regime["regime"] == mc.REGIME_MIXED

    def test_missing_benchmark_is_unavailable(self):
        regime = mc.build_market_regime({})
        assert regime["status"] == mc.STATUS_UNAVAILABLE
        assert regime["reason"] == "no_benchmark_series_stored"
        assert regime["regime"] == mc.REGIME_INSUFFICIENT

    def test_short_benchmark_history_is_insufficient(self):
        regime = mc.build_market_regime({"SPY": compounding(30)})
        assert regime["status"] == mc.STATUS_INSUFFICIENT_HISTORY
        assert regime["regime"] == mc.REGIME_INSUFFICIENT

    def test_is_deterministic(self):
        series = {"SPY": compounding(70, pct_per_bar=0.3)}
        assert mc.build_market_regime(series) == mc.build_market_regime(series)

    def test_breadth_is_secondary_colour_and_does_not_decide_the_regime(self):
        spy = {"SPY": compounding(70, pct_per_bar=0.5)}
        weak_breadth = mc.build_universe_breadth(
            {f"S{i}": compounding(70, pct_per_bar=-1.0) for i in range(25)})
        with_breadth = mc.build_market_regime(spy, universe_breadth=weak_breadth)
        without = mc.build_market_regime(spy)
        # a uniformly weak universe must not flip a supportive benchmark read
        assert with_breadth["regime"] == without["regime"] == mc.REGIME_SUPPORTIVE
        assert with_breadth["universe_breadth"]["note"].startswith("Secondary")

    def test_regime_is_context_and_takes_no_strategy_input(self):
        import inspect
        params = set(inspect.signature(mc.build_market_regime).parameters)
        assert params == {"reference_bars", "universe_breadth"}
        for forbidden in ("verdict", "attention", "setup_state"):
            assert forbidden not in params


# --------------------------------------------------------------------------- #
# DTO
# --------------------------------------------------------------------------- #

class TestMarketContextDto:
    def _universe(self):
        return {s: compounding(70, pct_per_bar=0.8) for s in rm.SYMBOL_SECTORS}

    def _refs(self):
        return {s: compounding(70, pct_per_bar=0.4) for s in rm.reference_symbols()}

    def test_three_reference_frames_are_separate_and_named(self):
        ctx = mc.build_market_context(
            "AAPL", self._universe(), as_of_session="2026-08-25",
            reference_bars=self._refs())
        assert set(ctx) == {
            "contract_version", "as_of_session",
            "scanner_universe_relative_strength", "benchmark_relative_strength",
            "sector_relative_strength", "volume_context", "market_regime",
        }
        assert ctx["scanner_universe_relative_strength"]["comparator"] == \
            mc.COMPARATOR_UNIVERSE_MEDIAN
        assert ctx["benchmark_relative_strength"]["reference_symbol"] == "SPY"
        assert ctx["sector_relative_strength"]["reference_symbol"] == "XLK"

    def test_every_frame_declares_an_explicit_status(self):
        ctx = mc.build_market_context(
            "AAPL", self._universe(), as_of_session="2026-08-25",
            reference_bars=self._refs())
        for key in ("scanner_universe_relative_strength",
                    "benchmark_relative_strength", "sector_relative_strength",
                    "volume_context", "market_regime"):
            assert ctx[key]["status"] in mc.CONTEXT_STATUSES

    def test_without_reference_data_the_frames_report_unavailable(self):
        ctx = mc.build_market_context(
            "AAPL", self._universe(), as_of_session="2026-08-25")
        assert ctx["benchmark_relative_strength"]["status"] == mc.STATUS_UNAVAILABLE
        assert ctx["sector_relative_strength"]["status"] == mc.STATUS_UNAVAILABLE
        assert ctx["market_regime"]["status"] == mc.STATUS_UNAVAILABLE
        # the universe frame still works — it needs no reference data
        assert ctx["scanner_universe_relative_strength"]["status"] == mc.STATUS_AVAILABLE

    def test_no_composite_score_anywhere(self):
        ctx = mc.build_market_context(
            "AAPL", self._universe(), as_of_session="2026-08-25",
            reference_bars=self._refs())
        flat_keys = " ".join(ctx.keys()).lower()
        for banned in ("composite", "total_score", "overall", "combined"):
            assert banned not in flat_keys


class TestRowContextStaysCompact:
    def test_row_context_carries_all_three_frames_without_the_detail(self):
        universe = {s: compounding(70) for s in rm.SYMBOL_SECTORS}
        refs = {s: compounding(70, pct_per_bar=0.4) for s in rm.reference_symbols()}
        row = sv.build_row_context("AAPL", universe, refs)
        assert row["relative_strength"] is not None
        assert row["benchmark_relative"] is not None
        assert row["sector_relative"] is not None
        assert row["sector"] == "Information Technology"
        # no nested horizon detail on the row
        assert "horizons" not in row
        assert "secondary_references" not in row

    def test_row_context_without_reference_data_degrades_cleanly(self):
        universe = {s: compounding(70) for s in rm.SYMBOL_SECTORS}
        row = sv.build_row_context("AAPL", universe)
        assert row["benchmark_relative"] is None
        assert row["benchmark_relative_status"] == mc.STATUS_UNAVAILABLE
        assert row["sector_relative"] is None
