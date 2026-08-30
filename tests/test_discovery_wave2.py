"""Wave 2 additions to the discovery path: aggregation, provenance, boundary.

A3 asks that a symbol appearing in several lists keeps EVERY reason instead of
becoming a number, and A4 asks what the frozen universe is not seeing. Both are
pure or fake-backed here; neither touches FMP or a database.
"""

import asyncio
from datetime import date, datetime, timezone

import app.external_discovery as ed
from app.source_licensing import LICENSING_INTERNAL_ONLY

UTC = timezone.utc
NOW = datetime(2026, 8, 28, 17, 0, tzinfo=UTC)
SESSION = date(2026, 8, 28)


def candidate(symbol, list_kind, rank=1, change=5.0, inside=False):
    return {"symbol": symbol, "list_kind": list_kind, "rank": rank,
            "change_percent": change, "in_scanner_universe": inside,
            "company_name": f"{symbol} Inc.", "exchange": "NASDAQ",
            "observed_at": NOW}


class TestAggregation:
    def test_every_reason_is_preserved(self):
        rows = [candidate("CRM", "top_gainers", rank=2, change=22.6),
                candidate("CRM", "most_active", rank=7, change=22.6)]
        crm = ed.aggregate_discovery(rows)[0]
        assert crm["reasons"] == ["most_active", "top_gainers"]
        assert crm["reason_count"] == 2

    def test_the_rollup_produces_no_score(self):
        rows = [candidate("CRM", "top_gainers"), candidate("CRM", "most_active")]
        entry = ed.aggregate_discovery(rows)[0]
        for banned in ("score", "opportunity", "rating", "weight",
                       "confidence"):
            assert banned not in entry

    def test_best_rank_is_the_lowest_seen(self):
        rows = [candidate("NVDA", "most_active", rank=9),
                candidate("NVDA", "top_gainers", rank=2)]
        assert ed.aggregate_discovery(rows)[0]["best_rank"] == 2

    def test_multi_list_symbols_sort_first(self):
        rows = [candidate("AAPL", "most_active", rank=1),
                candidate("CRM", "top_gainers", rank=5),
                candidate("CRM", "most_active", rank=6)]
        assert [e["symbol"] for e in ed.aggregate_discovery(rows)] \
            == ["CRM", "AAPL"]

    def test_universe_membership_is_true_if_any_row_says_so(self):
        rows = [candidate("NVDA", "most_active", inside=True),
                candidate("NVDA", "top_gainers", inside=False)]
        assert ed.aggregate_discovery(rows)[0]["in_scanner_universe"] is True

    def test_an_empty_session_aggregates_to_nothing(self):
        assert ed.aggregate_discovery([]) == []


class TestProvenance:
    def test_the_source_row_is_kept_bounded(self):
        raw = {"symbol": "CRM", "name": "Salesforce", "price": 250.0,
               "change": 46.0, "changesPercentage": 22.6, "volume": 91_000_000,
               "junk": "x" * 500, "nested": {"a": 1}}
        kept = ed.bound_source_row(raw)
        assert kept["changesPercentage"] == 22.6
        assert kept["volume"] == 91_000_000
        # Unlisted and non-scalar fields never reach the column.
        assert "junk" not in kept and "nested" not in kept

    def test_every_candidate_is_stamped_with_the_licence_class(self):
        row = {"symbol": "CRM", "name": "Salesforce", "price": 1.0,
               "change": 0.1, "changesPercentage": 10.0}
        stored = ed.normalize_candidate(row, list_kind="top_gainers", rank=1,
                                        observed_at=NOW, session_date=SESSION)
        assert stored["licensing_visibility"] == LICENSING_INTERNAL_ONLY
        assert stored["source_metadata"]["changesPercentage"] == 10.0


class FakeConn:
    """Answers the three queries `cross_reference_universe` issues."""

    def __init__(self, view_rows, bars):
        self.view_rows = view_rows
        self.bars = bars

    async def fetchrow(self, sql, *args):
        return {"session_date": SESSION}

    async def fetch(self, sql, *args):
        if "external_discovery_current" in sql:
            return self.view_rows
        if "daily_bars" in sql:
            return [{"symbol": s, "bars": n, "last_bar": SESSION}
                    for s, n in self.bars.items()]
        return []


def view_row(symbol, reasons, inside, rank=1):
    return {"session_date": SESSION, "symbol": symbol,
            "company_name": None, "exchange": None,
            "in_scanner_universe": inside, "reasons": reasons,
            "reason_count": len(reasons), "best_rank": rank,
            "max_abs_change_percent": 10.0, "observed_at": NOW,
            "licensing_visibility": LICENSING_INTERNAL_ONLY}


class TestCrossReference:
    def _report(self):
        conn = FakeConn(
            [view_row("NVDA", ["most_active"], True, rank=2),
             view_row("CRM", ["most_active", "top_gainers"], False, rank=1),
             view_row("XYZQ", ["top_gainers"], False, rank=4)],
            {"NVDA": 900, "CRM": 12})
        return asyncio.run(ed.cross_reference_universe(conn))

    def test_the_universe_boundary_is_reported_both_ways(self):
        report = self._report()
        assert [r["symbol"] for r in report["inside_universe"]] == ["NVDA"]
        assert [r["symbol"] for r in report["outside_universe"]] \
            == ["CRM", "XYZQ"]

    def test_multi_category_symbols_are_named(self):
        assert [r["symbol"] for r in self._report()["multi_category"]] == ["CRM"]

    def test_local_history_coverage_is_split_at_the_control_gate(self):
        report = self._report()
        assert report["min_local_bars"] == ed.MIN_LOCAL_BARS == 200
        assert [r["symbol"] for r in report["with_local_history"]] == ["NVDA"]
        # CRM has 12 bars, XYZQ has none: neither can be studied locally, and
        # that is the finding rather than a gap to paper over.
        assert [r["symbol"] for r in report["insufficient_local_history"]] \
            == ["CRM", "XYZQ"]

    def test_an_empty_store_returns_an_empty_report_not_an_error(self):
        class Empty:
            async def fetchrow(self, sql, *args):
                return {"session_date": None}

            async def fetch(self, sql, *args):
                return []

        report = asyncio.run(ed.cross_reference_universe(Empty()))
        assert report["session_date"] is None and report["discovered"] == 0


class TestExperimentBoundary:
    def test_the_module_can_not_write_to_a_universe(self):
        source = open("app/external_discovery.py", encoding="utf-8").read()
        for forbidden in ("INSERT INTO public.history_warmup",
                          "UPDATE public.history_warmup",
                          "INSERT INTO public.strategy_shadow",
                          "job_tasks"):
            assert forbidden not in source, forbidden

    def test_the_ingestion_role_holds_no_universe_write(self):
        # GRANT lines only: the prose names the relations this role must not
        # reach, which is the file documenting its own boundary.
        grants = [line.strip() for line
                  in open("ops/sql/create_smart_scanner_market_intel.sql",
                          encoding="utf-8")
                  if line.strip().upper().startswith("GRANT ")]
        joined = "\n".join(grants)
        assert "GRANT SELECT ON public.history_warmup_universe_symbols" in joined
        assert "strategy_shadow" not in joined
        assert "job_" not in joined
        for line in grants:
            if "history_warmup" in line or "daily_bars" in line:
                assert line.upper().startswith("GRANT SELECT ON"), line
            assert "DELETE" not in line.upper(), line
            assert "TRUNCATE" not in line.upper(), line
