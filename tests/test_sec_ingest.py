"""SEC ingestion: EDGAR's shape, accession identity, and the access policy.

The DB is a small in-memory fake enforcing exactly the constraint the real
schema declares — `UNIQUE (source, accession_number)` on filings and
`UNIQUE (symbol, filing_id)` on links. That is enough to prove idempotency and
amendment behaviour without a live Postgres, and the same statements are
exercised against a real database in the staging run.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

import app.sec_events as se
import app.sec_ingest as si

UTC = timezone.utc
run = asyncio.run
NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def edgar_row(**over):
    """One record out of EDGAR's parallel-array `filings.recent` block."""
    row = {
        "accessionNumber": "0000320193-26-000018",
        "filingDate": "2026-07-30",
        "reportDate": "2026-07-30",
        "acceptanceDateTime": "2026-07-30T20:30:28.000Z",
        "act": "34",
        "form": "8-K",
        "items": "2.02,9.01",
        "primaryDocument": "aapl-20260730.htm",
        "primaryDocDescription": "8-K",
        "size": 417360,
    }
    row.update(over)
    return row


def submissions(rows, *, tickers=("AAPL",), name="Apple Inc."):
    """EDGAR returns parallel arrays, one per field. Rebuild that shape."""
    keys = sorted({k for r in rows for k in r})
    return {
        "cik": "320193", "name": name, "tickers": list(tickers),
        "filings": {"recent": {k: [r.get(k) for r in rows] for k in keys}},
    }


class FakeConn:
    def __init__(self):
        self.filings = {}       # id -> row
        self.by_identity = {}   # (source, accession) -> id
        self.links = {}         # (symbol, filing_id) -> cik
        self.source_state = {}
        self._next = 0

    async def fetchrow(self, sql, *args):
        assert "INSERT INTO public.sec_filings" in sql
        cols = ["source", "accession_number", "cik", "form", "accepted_at",
                "filing_date", "period_of_report", "item_codes", "event_types",
                "taxonomy_version", "is_primary_event", "amends_accession_number",
                "primary_document", "filing_url", "observed_at"]
        row = dict(zip(cols, args))
        key = (row["source"], row["accession_number"])
        if key in self.by_identity:
            existing = self.by_identity[key]
            self.filings[existing].update(row)
            return {"id": existing, "inserted": False}
        self._next += 1
        row["id"] = self._next
        self.filings[self._next] = row
        self.by_identity[key] = self._next
        return {"id": self._next, "inserted": True}

    async def execute(self, sql, *args):
        if "sec_filing_symbols" in sql:
            filing_id, symbol, cik = args
            self.links[(symbol, filing_id)] = cik
            return
        if "catalyst_source_state" in sql:
            self.source_state[args[0]] = {
                "source": args[0], "status": args[1], "last_refresh_at": args[2],
                "last_success_at": args[3], "symbols_covered": args[4],
                "events_upserted": args[5], "detail": args[6]}
            return
        raise AssertionError(f"unexpected execute: {sql[:60]}")


class FakeSecClient(si.SecClient):
    """A SecClient that answers from a script instead of the network."""

    def __init__(self, *, ticker_map=None, by_cik=None, error=None):
        super().__init__("Test agent (test@example.com)", interval_seconds=0)
        self.ticker_map = ticker_map or {"AAPL": "0000320193"}
        self.by_cik = by_cik or {}
        self.error = error
        self.urls = []

    async def get_json(self, url):
        self.urls.append(url)
        if self.error:
            raise self.error
        if url == si.SEC_TICKER_MAP_URL:
            return {str(i): {"ticker": t, "cik_str": int(c)}
                    for i, (t, c) in enumerate(self.ticker_map.items())}
        for cik, payload in self.by_cik.items():
            if f"CIK{cik}.json" in url:
                return payload
        return None


# --------------------------------------------------------------------------- #
# SEC access policy
# --------------------------------------------------------------------------- #

class TestAccessPolicy:
    def test_a_client_refuses_to_exist_without_a_declared_user_agent(self):
        # SEC policy requires identification. Sending a generic default would be
        # a violation dressed up as a convenience, so this fails loudly instead.
        for bad in ("", "   ", None):
            with pytest.raises(si.SecSourceUnavailable) as caught:
                si.SecClient(bad)
            assert caught.value.reason == "missing_user_agent"

    def test_a_declared_user_agent_is_accepted(self):
        assert si.SecClient("Smart Scanner (ops@example.com)").user_agent

    def test_only_the_structured_json_endpoints_are_used(self):
        # Not HTML scraping, and not the full-text search UI.
        assert si.SEC_SUBMISSIONS_URL.startswith("https://data.sec.gov/submissions/")
        assert si.SEC_TICKER_MAP_URL.endswith(".json")

    def test_requests_are_spaced_rather_than_bursted(self):
        assert si.REQUEST_INTERVAL_SECONDS >= 0.1


# --------------------------------------------------------------------------- #
# normalisation
# --------------------------------------------------------------------------- #

class TestNormalizeFiling:
    def test_the_stored_row_carries_only_checkable_facts(self):
        row = si.normalize_filing(edgar_row(), cik="0000320193", observed_at=NOW)
        assert set(row) == {
            "source", "accession_number", "cik", "form", "accepted_at",
            "filing_date", "period_of_report", "item_codes", "event_types",
            "taxonomy_version", "is_primary_event", "amends_accession_number",
            "primary_document", "filing_url", "observed_at"}

    def test_acceptance_is_parsed_as_the_utc_it_declares(self):
        row = si.normalize_filing(edgar_row(), cik="0000320193", observed_at=NOW)
        assert row["accepted_at"] == datetime(2026, 7, 30, 20, 30, 28, tzinfo=UTC)

    def test_a_timestamp_without_an_offset_is_read_as_utc_not_local(self):
        # A silent local-time reading would shift the point-in-time gate by hours.
        row = si.normalize_filing(
            edgar_row(acceptanceDateTime="2026-07-30T20:30:28"),
            cik="0000320193", observed_at=NOW)
        assert row["accepted_at"].tzinfo is not None
        assert row["accepted_at"].hour == 20

    def test_the_event_date_and_the_disclosure_date_are_kept_apart(self):
        row = si.normalize_filing(
            edgar_row(reportDate="2026-07-27", filingDate="2026-07-30"),
            cik="0000320193", observed_at=NOW)
        assert row["period_of_report"] == date(2026, 7, 27)
        assert row["filing_date"] == date(2026, 7, 30)

    def test_item_codes_and_families_are_derived_and_versioned(self):
        row = si.normalize_filing(edgar_row(), cik="0000320193", observed_at=NOW)
        assert row["item_codes"] == ["2.02", "9.01"]
        assert row["event_types"] == [se.EVENT_RESULTS, se.EVENT_EXHIBITS]
        assert row["is_primary_event"] is True
        assert row["taxonomy_version"] == se.SEC_TAXONOMY_VERSION

    @pytest.mark.parametrize("missing", ["accessionNumber", "acceptanceDateTime", "form"])
    def test_a_filing_missing_an_honesty_critical_field_is_dropped(self, missing):
        # Identity and point-in-time rest on these. A guess at either would be
        # worse than not carrying the filing.
        assert si.normalize_filing(edgar_row(**{missing: None}),
                                   cik="0000320193", observed_at=NOW) is None

    @pytest.mark.parametrize("form,kept", [
        ("8-K", True), ("8-K/A", True), ("8-K12B", True),
        ("10-K", False), ("10-Q", False), ("4", False), ("SC 13D", False),
        ("DEF 14A", False), ("S-1", False),
    ])
    def test_only_current_reports_are_ingested(self, form, kept):
        row = si.normalize_filing(edgar_row(form=form), cik="0000320193",
                                  observed_at=NOW)
        assert (row is not None) is kept

    def test_the_filing_url_points_at_the_primary_document(self):
        row = si.normalize_filing(edgar_row(), cik="0000320193", observed_at=NOW)
        assert row["filing_url"] == (
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000018/aapl-20260730.htm")

    def test_a_filing_with_no_named_document_links_to_its_index(self):
        row = si.normalize_filing(edgar_row(primaryDocument=None),
                                  cik="0000320193", observed_at=NOW)
        assert row["filing_url"].endswith("/000032019326000018")

    def test_ciks_are_padded_the_way_edgar_uses_them(self):
        assert si.normalize_cik("320193") == "0000320193"
        assert si.normalize_cik("0000320193") == "0000320193"
        assert si.normalize_cik(None) is None


class TestExtractRecentFilings:
    def test_parallel_arrays_are_zipped_by_index(self):
        # The failure this prevents: pairing one filing's form with another
        # filing's timestamp.
        payload = submissions([
            edgar_row(accessionNumber="0000320193-26-000018", form="8-K",
                      acceptanceDateTime="2026-07-30T20:30:28.000Z"),
            edgar_row(accessionNumber="0000320193-26-000019", form="10-Q",
                      acceptanceDateTime="2026-08-01T10:00:00.000Z"),
        ])
        rows = si.extract_recent_filings(payload, cik="0000320193", observed_at=NOW)
        assert len(rows) == 1                       # the 10-Q is not a current report
        assert rows[0]["accession_number"] == "0000320193-26-000018"
        assert rows[0]["accepted_at"].day == 30

    def test_filings_older_than_the_lookback_are_dropped(self):
        payload = submissions([
            edgar_row(accessionNumber="0000320193-25-000001", filingDate="2025-01-05",
                      acceptanceDateTime="2025-01-05T12:00:00.000Z"),
            edgar_row(),
        ])
        rows = si.extract_recent_filings(payload, cik="0000320193",
                                         observed_at=NOW, since=date(2026, 1, 1))
        assert [r["filing_date"] for r in rows] == [date(2026, 7, 30)]

    def test_every_ticker_edgar_associates_with_the_issuer_is_returned(self):
        payload = submissions([edgar_row()], tickers=("GOOGL", "GOOG"))
        assert si.extract_symbols(payload) == ["GOOG", "GOOGL"]

    def test_an_empty_submissions_document_yields_nothing_rather_than_failing(self):
        assert si.extract_recent_filings({}, cik="0000320193", observed_at=NOW) == []


# --------------------------------------------------------------------------- #
# persistence
# --------------------------------------------------------------------------- #

class TestUpsertAndIdentity:
    def _row(self, **over):
        return si.normalize_filing(edgar_row(**over), cik="0000320193",
                                   observed_at=NOW)

    def test_running_the_same_refresh_twice_stores_one_filing(self):
        conn = FakeConn()
        rows = [self._row()]
        links = {"0000320193-26-000018": {"AAPL"}}
        first = run(si.upsert_filings(conn, rows, links=links, universe={"AAPL"}))
        second = run(si.upsert_filings(conn, rows, links=links, universe={"AAPL"}))
        assert len(conn.filings) == 1
        assert first["filings_inserted"] == 1
        assert second["filings_inserted"] == 0      # idempotent
        assert second["filings_updated"] == 1

    def test_an_amendment_lands_beside_its_original_not_over_it(self):
        conn = FakeConn()
        original = self._row(accessionNumber="0000104169-26-000023", form="8-K",
                             items="5.02,9.01",
                             acceptanceDateTime="2026-01-16T14:01:15.000Z")
        amendment = self._row(accessionNumber="0000104169-26-000024", form="8-K/A",
                              items="5.02",
                              acceptanceDateTime="2026-01-16T14:03:05.000Z")
        links = {"0000104169-26-000023": {"WMT"}, "0000104169-26-000024": {"WMT"}}
        run(si.upsert_filings(conn, [original, amendment], links=links,
                              universe={"WMT"}))
        assert len(conn.filings) == 2
        forms = sorted(r["form"] for r in conn.filings.values())
        assert forms == ["8-K", "8-K/A"]

    def test_only_symbols_we_actually_track_get_linked(self):
        conn = FakeConn()
        run(si.upsert_filings(
            conn, [self._row()],
            links={"0000320193-26-000018": {"AAPL", "GOOG"}}, universe={"AAPL"}))
        assert {s for s, _ in conn.links} == {"AAPL"}

    def test_one_issuer_filing_can_belong_to_several_tickers(self):
        conn = FakeConn()
        run(si.upsert_filings(
            conn, [self._row()],
            links={"0000320193-26-000018": {"GOOGL", "GOOG"}},
            universe={"GOOGL", "GOOG"}))
        assert {s for s, _ in conn.links} == {"GOOGL", "GOOG"}
        assert len(conn.filings) == 1

    def test_a_re_run_refreshes_a_corrected_item_list(self):
        conn = FakeConn()
        run(si.upsert_filings(conn, [self._row(items="2.02")],
                              links={"0000320193-26-000018": {"AAPL"}},
                              universe={"AAPL"}))
        run(si.upsert_filings(conn, [self._row(items="2.02,9.01")],
                              links={"0000320193-26-000018": {"AAPL"}},
                              universe={"AAPL"}))
        stored = next(iter(conn.filings.values()))
        assert stored["item_codes"] == ["2.02", "9.01"]


# --------------------------------------------------------------------------- #
# the refresh as a whole
# --------------------------------------------------------------------------- #

class TestRefresh:
    def _client(self, **over):
        return FakeSecClient(
            ticker_map={"AAPL": "0000320193"},
            by_cik={"0000320193": submissions([edgar_row()])}, **over)

    def test_a_successful_refresh_records_an_ok_source_state(self):
        conn = FakeConn()
        summary = run(si.refresh_sec_filings(conn, self._client(), ["AAPL"], now=NOW))
        assert summary["status"] == si.STATE_OK
        assert summary["filings_inserted"] == 1
        state = conn.source_state[se.SOURCE_SEC_EDGAR]
        assert state["status"] == "ok"
        assert state["last_success_at"] == NOW

    def test_a_source_failure_is_recorded_not_raised(self):
        # An EDGAR outage must cost the product its SEC dimension and nothing
        # else — so the refresh returns, and the state says why.
        conn = FakeConn()
        client = self._client(error=si.SecSourceUnavailable(
            "sec_rate_limited", "SEC returned HTTP 429"))
        summary = run(si.refresh_sec_filings(conn, client, ["AAPL"], now=NOW))
        assert summary["status"] == si.STATE_UNAVAILABLE
        state = conn.source_state[se.SOURCE_SEC_EDGAR]
        assert state["status"] == "unavailable"
        assert state["last_success_at"] is None
        assert "sec_rate_limited" in state["detail"]

    def test_an_unexpected_failure_is_recorded_as_an_error_not_a_success(self):
        conn = FakeConn()
        client = self._client(error=ValueError("boom"))
        summary = run(si.refresh_sec_filings(conn, client, ["AAPL"], now=NOW))
        assert summary["status"] == si.STATE_ERROR
        assert conn.source_state[se.SOURCE_SEC_EDGAR]["last_success_at"] is None

    def test_a_symbol_with_no_filings_is_a_success_with_zero_rows(self):
        conn = FakeConn()
        client = FakeSecClient(ticker_map={"NFLX": "0001065280"},
                               by_cik={"0001065280": submissions([])})
        summary = run(si.refresh_sec_filings(conn, client, ["NFLX"], now=NOW))
        assert summary["status"] == si.STATE_OK
        assert summary["filings_inserted"] == 0
        assert conn.source_state[se.SOURCE_SEC_EDGAR]["status"] == "ok"

    def test_an_unresolvable_symbol_is_reported_rather_than_silently_skipped(self):
        conn = FakeConn()
        client = FakeSecClient(ticker_map={"AAPL": "0000320193"},
                               by_cik={"0000320193": submissions([edgar_row()])})
        summary = run(si.refresh_sec_filings(conn, client, ["AAPL", "ZZZZ"], now=NOW))
        assert summary["unresolved_symbols"] == ["ZZZZ"]
        assert summary["status"] == si.STATE_OK

    def test_one_issuer_is_fetched_once_however_many_tickers_it_has(self):
        conn = FakeConn()
        client = FakeSecClient(
            ticker_map={"GOOGL": "0001652044", "GOOG": "0001652044"},
            by_cik={"0001652044": submissions([edgar_row()],
                                              tickers=("GOOGL", "GOOG"))})
        summary = run(si.refresh_sec_filings(conn, client, ["GOOGL", "GOOG"], now=NOW))
        submissions_calls = [u for u in client.urls if "submissions" in u]
        assert len(submissions_calls) == 1
        assert summary["issuers"] == 1
        assert {s for s, _ in conn.links} == {"GOOGL", "GOOG"}

    def test_symbols_are_normalised_before_resolution(self):
        conn = FakeConn()
        client = self._client()
        run(si.refresh_sec_filings(conn, client, [" aapl ", ""], now=NOW))
        assert any("CIK0000320193.json" in u for u in client.urls)
