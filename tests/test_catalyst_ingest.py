"""Catalyst ingestion (app/catalyst_ingest.py).

Two things matter here and nothing else does: that a provider row is turned into
an honest record (never a guessed one), and that running the refresh twice does
not change the result. The provider itself is faked — these tests must not need
a credential or a network.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

import app.catalyst_ingest as ci

UTC = timezone.utc
OBSERVED = datetime(2026, 8, 26, 13, 0, tzinfo=UTC)

# The repo drives coroutines from sync tests rather than depending on an async
# pytest plugin; these helpers keep that convention readable.
run = asyncio.run


def upsert(conn, events, today):
    return run(ci.upsert_events(conn, events, today=today))


def refresh(conn, client, symbols, now=OBSERVED):
    return run(ci.refresh_catalysts(conn, client, symbols, now=now))


class FakeClient:
    """Records the calls made and replays canned payloads by path."""

    def __init__(self, payloads=None, error=None):
        self.payloads = payloads or {}
        self.error = error
        self.calls = []

    async def _request(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        if self.error is not None and path == ci.EARNINGS_CALENDAR_PATH:
            raise self.error
        by_symbol = self.payloads.get(path, {})
        return {"results": by_symbol.get((params or {}).get("ticker"), [])}


class ProviderError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class FakeConn:
    """Applies the real SQL semantics we depend on, in memory."""

    def __init__(self):
        self.rows = []          # symbol_catalyst_events
        self.state = {}         # catalyst_source_state
        self.executed = []

    async def execute(self, sql, *args):
        self.executed.append(sql)
        if sql is ci.UPSERT_SQL or "INSERT INTO public.symbol_catalyst_events" in sql:
            key = (args[0], args[1], args[2])
            row = {"symbol": args[0], "event_type": args[1], "event_date": args[2],
                   "session_timing": args[3], "certainty": args[4],
                   "fiscal_period": args[5], "fiscal_year": args[6],
                   "source": args[7], "source_reference": args[8],
                   "observed_at": args[9]}
            for i, existing in enumerate(self.rows):
                if (existing["symbol"], existing["event_type"],
                        existing["event_date"]) == key:
                    # COALESCE semantics for the two nullable columns.
                    row["fiscal_period"] = args[5] or existing["fiscal_period"]
                    row["fiscal_year"] = args[6] or existing["fiscal_year"]
                    row["source_reference"] = args[8] or existing["source_reference"]
                    self.rows[i] = row
                    return
            self.rows.append(row)
        elif "DELETE FROM public.symbol_catalyst_events" in sql:
            symbol, etype, period, year, keep_date, floor_date = args
            self.rows = [r for r in self.rows if not (
                r["symbol"] == symbol and r["event_type"] == etype
                and r["fiscal_period"] == period and r["fiscal_year"] == year
                and r["event_date"] != keep_date
                and r["event_date"] >= floor_date)]
        elif "INSERT INTO public.catalyst_source_state" in sql:
            prior = self.state.get(args[0], {})
            self.state[args[0]] = {
                "source": args[0], "status": args[1], "last_refresh_at": args[2],
                "last_success_at": args[3] or prior.get("last_success_at"),
                "symbols_covered": args[4], "events_upserted": args[5],
                "detail": args[6]}


def earnings_payload(rows):
    return {ci.EARNINGS_CALENDAR_PATH: rows}


def filings_payload(rows):
    return {ci.FINANCIALS_PATH: rows}


# --------------------------------------------------------------------------- #
# Normalisation
# --------------------------------------------------------------------------- #

class TestTimingNormalisation:
    @pytest.mark.parametrize("raw", ["bmo", "BMO", " bmo ", "before_market",
                                     "premarket", "pre-market"])
    def test_recognises_before_market_spellings(self, raw):
        assert ci.normalize_earnings_timing(raw) == ci.TIMING_BEFORE_MARKET

    @pytest.mark.parametrize("raw", ["amc", "AMC", "after_market", "postmarket"])
    def test_recognises_after_market_spellings(self, raw):
        assert ci.normalize_earnings_timing(raw) == ci.TIMING_AFTER_MARKET

    @pytest.mark.parametrize("raw", ["dmt", "during_market", "intraday"])
    def test_recognises_during_market_spellings(self, raw):
        assert ci.normalize_earnings_timing(raw) == ci.TIMING_DURING_MARKET

    @pytest.mark.parametrize("raw", [None, "", "   ", "sometime", "09:30", 7])
    def test_never_guesses_an_unrecognised_token(self, raw):
        assert ci.normalize_earnings_timing(raw) == ci.TIMING_UNKNOWN


class TestEarningsNormalisation:
    def test_maps_a_full_provider_row(self):
        rec = ci.normalize_earnings_record("aapl", {
            "date": "2026-10-29", "time": "amc", "date_confirmed": True,
            "fiscal_period": "Q4", "fiscal_year": "2026", "id": "bz-1",
        }, observed_at=OBSERVED)
        assert rec["symbol"] == "AAPL"
        assert rec["event_type"] == ci.EVENT_EARNINGS
        assert rec["event_date"] == date(2026, 10, 29)
        assert rec["session_timing"] == ci.TIMING_AFTER_MARKET
        assert rec["certainty"] == ci.CERTAINTY_CONFIRMED
        assert rec["source"] == ci.SOURCE_EARNINGS_CALENDAR
        assert rec["source_reference"] == "bz-1"
        assert rec["observed_at"] == OBSERVED

    @pytest.mark.parametrize("flag", [True, 1, "1", "true", "True"])
    def test_treats_every_truthy_confirmation_as_confirmed(self, flag):
        rec = ci.normalize_earnings_record(
            "AAPL", {"date": "2026-10-29", "date_confirmed": flag},
            observed_at=OBSERVED)
        assert rec["certainty"] == ci.CERTAINTY_CONFIRMED

    @pytest.mark.parametrize("flag", [None, False, 0, "0", "false", "maybe"])
    def test_anything_else_stays_estimated_and_is_never_upgraded(self, flag):
        rec = ci.normalize_earnings_record(
            "AAPL", {"date": "2026-10-29", "date_confirmed": flag},
            observed_at=OBSERVED)
        assert rec["certainty"] == ci.CERTAINTY_ESTIMATED

    @pytest.mark.parametrize("row", [
        {}, {"date": None}, {"date": ""}, {"date": "not-a-date"},
        {"date": "2026-13-45"},
    ])
    def test_drops_a_row_with_no_usable_date(self, row):
        assert ci.normalize_earnings_record("AAPL", row, observed_at=OBSERVED) is None

    def test_accepts_the_alternate_field_names(self):
        rec = ci.normalize_earnings_record(
            "AAPL", {"earnings_date": "2026-10-29", "timing": "bmo",
                     "period": "Q4", "period_year": "2026"},
            observed_at=OBSERVED)
        assert rec["event_date"] == date(2026, 10, 29)
        assert rec["session_timing"] == ci.TIMING_BEFORE_MARKET
        assert rec["fiscal_period"] == "Q4"


class TestFilingNormalisation:
    def test_a_filing_is_always_a_recorded_past_fact(self):
        rec = ci.normalize_filing_record({
            "filing_date": "2026-08-01", "tickers": ["AAPL"],
            "fiscal_period": "Q3", "fiscal_year": "2026",
            "source_filing_url": "https://example.test/f",
        }, observed_at=OBSERVED)
        assert rec["event_type"] == ci.EVENT_FINANCIAL_REPORT_FILING
        assert rec["certainty"] == ci.CERTAINTY_FILED
        assert rec["source"] == ci.SOURCE_FINANCIAL_FILINGS

    def test_filing_timing_is_never_inferred_from_an_acceptance_time(self):
        rec = ci.normalize_filing_record(
            {"filing_date": "2026-08-01", "tickers": ["AAPL"],
             "acceptance_datetime": "20260801163000"}, observed_at=OBSERVED)
        assert rec["session_timing"] == ci.TIMING_UNKNOWN

    def test_a_multi_class_filing_is_attributed_to_the_symbol_we_asked_for(self):
        # GOOGL's report lists GOOG first; JPM's lists a preferred line first.
        # Taking the first ticker would silently lose both symbols.
        row = {"filing_date": "2026-07-29",
               "tickers": ["GOOG", "GOOGL", "GOOGM", "GOOGN"]}
        assert ci.normalize_filing_record(
            row, observed_at=OBSERVED, symbol="GOOGL")["symbol"] == "GOOGL"
        assert ci.normalize_filing_record(
            row, observed_at=OBSERVED, symbol="GOOG")["symbol"] == "GOOG"

    def test_a_filing_that_does_not_cover_us_is_dropped_not_reassigned(self):
        row = {"filing_date": "2026-07-29", "tickers": ["AMJB", "JPMpC"]}
        assert ci.normalize_filing_record(
            row, observed_at=OBSERVED, symbol="JPM") is None

    def test_drops_a_row_with_no_symbol_or_no_date(self):
        assert ci.normalize_filing_record(
            {"filing_date": "2026-08-01", "tickers": []}, observed_at=OBSERVED) is None
        assert ci.normalize_filing_record(
            {"tickers": ["AAPL"]}, observed_at=OBSERVED) is None

    def test_a_long_reference_is_truncated_not_rejected(self):
        rec = ci.normalize_filing_record(
            {"filing_date": "2026-08-01", "tickers": ["AAPL"],
             "source_filing_url": "u" * 900}, observed_at=OBSERVED)
        assert len(rec["source_reference"]) == 400


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #

class TestUpsert:
    def event(self, **over):
        e = {"symbol": "AAPL", "event_type": ci.EVENT_EARNINGS,
             "event_date": date(2026, 10, 29), "session_timing": "amc",
             "certainty": ci.CERTAINTY_ESTIMATED, "fiscal_period": "Q4",
             "fiscal_year": "2026", "source": ci.SOURCE_EARNINGS_CALENDAR,
             "source_reference": "bz-1", "observed_at": OBSERVED}
        e.update(over)
        return e

    def test_writing_the_same_event_twice_leaves_one_row(self):
        conn = FakeConn()
        for _ in range(2):
            upsert(conn, [self.event()], date(2026, 8, 26))
        assert len(conn.rows) == 1

    def test_a_second_pass_upgrades_estimated_to_confirmed(self):
        conn = FakeConn()
        upsert(conn, [self.event()], date(2026, 8, 26))
        upsert(
            conn, [self.event(certainty=ci.CERTAINTY_CONFIRMED)],
            date(2026, 8, 26))
        assert len(conn.rows) == 1
        assert conn.rows[0]["certainty"] == ci.CERTAINTY_CONFIRMED

    def test_a_rescheduled_event_does_not_leave_two_futures(self):
        conn = FakeConn()
        upsert(conn, [self.event()], date(2026, 8, 26))
        upsert(conn, [self.event(event_date=date(2026, 11, 5))],
                               date(2026, 8, 26))
        assert [r["event_date"] for r in conn.rows] == [date(2026, 11, 5)]

    def test_history_is_never_rewritten_by_a_reschedule(self):
        conn = FakeConn()
        past = self.event(event_date=date(2026, 5, 1), fiscal_period="Q2")
        upsert(conn, [past], date(2026, 8, 26))
        upsert(conn, [self.event()], date(2026, 8, 26))
        upsert(conn, [self.event(event_date=date(2026, 11, 5))],
                               date(2026, 8, 26))
        dates = sorted(r["event_date"] for r in conn.rows)
        assert dates == [date(2026, 5, 1), date(2026, 11, 5)]

    def test_different_quarters_coexist(self):
        conn = FakeConn()
        upsert(conn, [
            self.event(event_date=date(2026, 10, 29), fiscal_period="Q4"),
            self.event(event_date=date(2027, 1, 28), fiscal_period="Q1"),
        ], date(2026, 8, 26))
        assert len(conn.rows) == 2

    def test_an_event_with_no_fiscal_period_supersedes_nothing(self):
        # Without a period we cannot tell a reschedule from a second event, so
        # we must not delete anything.
        conn = FakeConn()
        upsert(conn, [self.event(fiscal_period=None)],
                               date(2026, 8, 26))
        upsert(
            conn, [self.event(event_date=date(2026, 11, 5), fiscal_period=None)],
            date(2026, 8, 26))
        assert len(conn.rows) == 2

    def test_two_symbols_never_supersede_each_other(self):
        conn = FakeConn()
        upsert(conn, [
            self.event(symbol="AAPL"), self.event(symbol="MSFT"),
        ], date(2026, 8, 26))
        assert {r["symbol"] for r in conn.rows} == {"AAPL", "MSFT"}


# --------------------------------------------------------------------------- #
# The refresh entry point
# --------------------------------------------------------------------------- #

class TestRefresh:
    def test_a_working_run_records_both_sources_as_ok(self):
        client = FakeClient({
            **earnings_payload({"AAPL": [
                {"date": "2026-10-29", "time": "amc", "date_confirmed": True,
                 "fiscal_period": "Q4", "fiscal_year": "2026"}]}),
            **filings_payload({"AAPL": [
                {"filing_date": "2026-08-01", "tickers": ["AAPL"],
                 "fiscal_period": "Q3", "fiscal_year": "2026"}]}),
        })
        conn = FakeConn()
        summary = refresh(conn, client, ["AAPL"], )
        assert summary["sources"][ci.SOURCE_EARNINGS_CALENDAR]["status"] == "ok"
        assert summary["sources"][ci.SOURCE_FINANCIAL_FILINGS]["status"] == "ok"
        assert {r["event_type"] for r in conn.rows} == {
            ci.EVENT_EARNINGS, ci.EVENT_FINANCIAL_REPORT_FILING}

    @pytest.mark.parametrize("status", [401, 402, 403, 404])
    def test_an_entitlement_failure_is_recorded_not_raised(self, status):
        client = FakeClient(filings_payload({"AAPL": [
            {"filing_date": "2026-08-01", "tickers": ["AAPL"]}]}),
            error=ProviderError(status))
        conn = FakeConn()
        summary = refresh(conn, client, ["AAPL"], )
        earnings = conn.state[ci.SOURCE_EARNINGS_CALENDAR]
        assert earnings["status"] == "unavailable"
        assert "provider_not_entitled" in earnings["detail"]
        assert earnings["last_success_at"] is None
        assert summary["sources"][ci.SOURCE_EARNINGS_CALENDAR]["reason"] == \
            "provider_not_entitled"

    def test_an_unavailable_earnings_source_never_hides_the_filings(self):
        client = FakeClient(filings_payload({"AAPL": [
            {"filing_date": "2026-08-01", "tickers": ["AAPL"]}]}),
            error=ProviderError(403))
        conn = FakeConn()
        refresh(conn, client, ["AAPL"], )
        assert conn.state[ci.SOURCE_FINANCIAL_FILINGS]["status"] == "ok"
        assert [r["event_type"] for r in conn.rows] == \
            [ci.EVENT_FINANCIAL_REPORT_FILING]

    def test_an_unexpected_error_degrades_to_error_not_a_crash(self):
        client = FakeClient(filings_payload({"AAPL": []}),
                            error=ProviderError(500))
        conn = FakeConn()
        summary = refresh(conn, client, ["AAPL"], )
        assert conn.state[ci.SOURCE_EARNINGS_CALENDAR]["status"] == "error"
        assert summary["sources"][ci.SOURCE_EARNINGS_CALENDAR]["status"] == "error"

    def test_a_failure_detail_never_carries_the_credential(self):
        client = FakeClient({}, error=ProviderError(403))
        conn = FakeConn()
        refresh(conn, client, ["AAPL"], )
        detail = conn.state[ci.SOURCE_EARNINGS_CALENDAR]["detail"] or ""
        assert "apiKey" not in detail and "api_key" not in detail

    def test_the_run_is_idempotent(self):
        payloads = {
            **earnings_payload({"AAPL": [
                {"date": "2026-10-29", "date_confirmed": True,
                 "fiscal_period": "Q4", "fiscal_year": "2026"}]}),
            **filings_payload({"AAPL": [
                {"filing_date": "2026-08-01", "tickers": ["AAPL"]}]}),
        }
        conn = FakeConn()
        refresh(conn, FakeClient(payloads), ["AAPL"], )
        first = [dict(r) for r in conn.rows]
        refresh(conn, FakeClient(payloads), ["AAPL"], )
        assert conn.rows == first

    def test_symbols_are_normalised_and_blanks_dropped(self):
        client = FakeClient(filings_payload({"AAPL": []}))
        conn = FakeConn()
        summary = refresh(
            conn, client, [" aapl ", "", None, "  "], )
        assert summary["symbols"] == 1
        assert all(c[1].get("ticker") == "AAPL" for c in client.calls)

    def test_a_filing_for_another_ticker_is_not_attributed_to_us(self):
        client = FakeClient(filings_payload({"AAPL": [
            {"filing_date": "2026-08-01", "tickers": ["MSFT"]}]}))
        conn = FakeConn()
        refresh(conn, client, ["AAPL"], )
        assert conn.rows == []

    def test_the_request_volume_is_bounded_by_the_symbol_list(self):
        client = FakeClient(filings_payload({s: [] for s in ("AAPL", "MSFT")}))
        conn = FakeConn()
        refresh(conn, client, ["AAPL", "MSFT"], )
        # Two sources x two symbols. No pagination loop, no per-day walk.
        assert len(client.calls) == 4
        assert all(c[1].get("limit", 0) <= ci.FILINGS_PER_SYMBOL or
                   c[0] == ci.EARNINGS_CALENDAR_PATH for c in client.calls)
