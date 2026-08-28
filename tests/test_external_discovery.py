"""Market-wide discovery: bounded, ranked, and unable to reach the experiment.

The normalisation here is pure, so every test runs without a database and
without FMP. The two things worth guarding are the ones that would be quiet
failures: a symbol shape we cannot store, and the boundary that keeps a
discovered symbol out of the frozen universe.
"""

import asyncio
from datetime import date, datetime, timezone

import pytest

import app.external_discovery as ed

UTC = timezone.utc
NOW = datetime(2026, 8, 28, 17, 0, tzinfo=UTC)
SESSION = date(2026, 8, 28)
UNIVERSE = {"AAPL", "MSFT", "NVDA"}


def feed(*symbols, **extra):
    return [{"symbol": s, "name": f"{s} Inc.", "price": 10.0,
             "change": 1.0, "changesPercentage": 11.1,
             "exchange": "NASDAQ", **extra} for s in symbols]


class FakeConn:
    def __init__(self):
        self.rows = {}
        self.state = []

    async def fetchrow(self, sql, *args):
        key = (args[0], args[1], args[2], args[10])
        inserted = key not in self.rows
        self.rows[key] = args
        return {"inserted": inserted}

    async def execute(self, sql, *args):
        self.state.append({"source": args[0], "status": args[1], "detail": args[6]})
        return "OK"


class FakeClient:
    """Returns a canned feed per endpoint, or raises the configured failure."""

    def __init__(self, feeds=None, failures=None):
        self.feeds = feeds or {}
        self.failures = failures or {}
        self.calls = []

    async def get_list(self, path):
        self.calls.append(path)
        if path in self.failures:
            raise self.failures[path]
        return self.feeds.get(path, [])

    async def pause(self):
        return None


class TestSymbolNormalization:
    @pytest.mark.parametrize("raw,expected", [
        ("aapl", "AAPL"), ("BRK.B", "BRK.B"), ("RDS-A", "RDS-A"),
    ])
    def test_storable_symbols_survive(self, raw, expected):
        assert ed.normalize_symbol(raw) == expected

    @pytest.mark.parametrize("raw", ["", None, "1234", "TOO-LONG-SYMBOL-NAME",
                                     "AB CD", "SYM$", "汉字"])
    def test_unstorable_symbols_are_dropped_not_mangled(self, raw):
        # These would fail the DB CHECK constraint. Dropping them keeps a
        # foreign listing out of the table; mangling them would invent a ticker.
        assert ed.normalize_symbol(raw) is None

    def test_a_normalised_symbol_always_satisfies_the_db_constraint(self):
        import re
        pattern = re.compile(r"^[A-Z][A-Z0-9.\-]{0,15}$")
        for raw in ("aapl", "brk.b", "rds-a", "T", "ABCDEFGHIJKLMNOP"):
            out = ed.normalize_symbol(raw)
            assert out is None or pattern.match(out)


class TestListNormalization:
    def test_rank_is_dense_even_when_a_symbol_is_dropped(self):
        rows = feed("AAPL") + [{"symbol": "汉字"}] + feed("TSLA")
        out = ed.normalize_list(rows, list_kind=ed.LIST_TOP_GAINERS,
                               observed_at=NOW, session_date=SESSION)
        # A hole in the ranking would misstate the provider's ordering.
        assert [c["rank"] for c in out] == [1, 2]
        assert [c["symbol"] for c in out] == ["AAPL", "TSLA"]

    def test_the_list_is_bounded(self):
        out = ed.normalize_list(feed(*[f"SYM{i}" for i in range(80)]),
                                list_kind=ed.LIST_MOST_ACTIVE,
                                observed_at=NOW, session_date=SESSION, limit=5)
        assert len(out) == 5

    def test_duplicates_collapse(self):
        out = ed.normalize_list(feed("AAPL", "AAPL"),
                                list_kind=ed.LIST_TOP_GAINERS,
                                observed_at=NOW, session_date=SESSION)
        assert len(out) == 1

    def test_both_percentage_spellings_are_read(self):
        a = ed.normalize_list([{"symbol": "AAPL", "changesPercentage": 5.0}],
                              list_kind=ed.LIST_TOP_GAINERS,
                              observed_at=NOW, session_date=SESSION)
        b = ed.normalize_list([{"symbol": "AAPL", "changePercentage": 5.0}],
                              list_kind=ed.LIST_TOP_GAINERS,
                              observed_at=NOW, session_date=SESSION)
        assert a[0]["change_percent"] == 5.0
        assert b[0]["change_percent"] == 5.0


class TestUniverseBoundary:
    def test_a_universe_symbol_is_flagged_as_one(self):
        out = ed.normalize_list(feed("AAPL"), list_kind=ed.LIST_MOST_ACTIVE,
                                observed_at=NOW, session_date=SESSION,
                                universe=UNIVERSE)
        assert out[0]["in_scanner_universe"] is True

    def test_an_outside_symbol_is_stored_and_marked(self):
        # The entire point of the table: a symbol we never scan is exactly what
        # we want to learn about, and it must be visibly outside.
        out = ed.normalize_list(feed("PLTR"), list_kind=ed.LIST_MOST_ACTIVE,
                                observed_at=NOW, session_date=SESSION,
                                universe=UNIVERSE)
        assert out[0]["in_scanner_universe"] is False

    def test_no_universe_means_nothing_is_claimed_to_be_inside(self):
        out = ed.normalize_list(feed("AAPL"), list_kind=ed.LIST_MOST_ACTIVE,
                                observed_at=NOW, session_date=SESSION,
                                universe=None)
        assert out[0]["in_scanner_universe"] is False


class TestRefresh:
    def run(self, conn, client, **kw):
        return asyncio.run(ed.refresh_discovery_candidates(
            conn, client, now=NOW, universe=UNIVERSE, **kw))

    def test_a_successful_refresh_writes_every_entitled_list(self):
        conn = FakeConn()
        client = FakeClient(feeds={p: feed("AAPL", "PLTR")
                                   for p in ed.ENDPOINTS.values()})
        result = self.run(conn, client)
        assert result["status"] == ed.STATE_OK
        assert result["inserted"] == 6          # 3 lists x 2 symbols
        assert result["distinct_symbols"] == 2
        assert conn.state[-1]["status"] == ed.STATE_OK

    def test_a_second_refresh_updates_rather_than_duplicating(self):
        conn = FakeConn()
        client = FakeClient(feeds={p: feed("AAPL") for p in ed.ENDPOINTS.values()})
        self.run(conn, client)
        second = self.run(conn, client)
        assert second["inserted"] == 0
        assert second["updated"] == 3

    def test_no_credential_reports_unavailable_not_an_error(self):
        # By far the most common state: nobody has configured an FMP key. It
        # is not a fault and must not be reported as one.
        conn = FakeConn()
        result = asyncio.run(
            ed.refresh_discovery_candidates(conn, None, now=NOW))
        assert result["status"] == ed.STATE_UNAVAILABLE
        assert result["reason"] == "missing_api_key"

    def test_an_unentitled_feed_costs_only_itself(self):
        conn = FakeConn()
        client = FakeClient(
            feeds={ed.ENDPOINTS[ed.LIST_TOP_GAINERS]: feed("AAPL"),
                   ed.ENDPOINTS[ed.LIST_TOP_LOSERS]: feed("PLTR")},
            failures={ed.ENDPOINTS[ed.LIST_MOST_ACTIVE]:
                      ed.DiscoverySourceUnavailable("not_entitled")})
        result = self.run(conn, client)
        # Two working feeds are more useful than none, and the third's failure
        # is reported rather than hidden.
        assert result["status"] == ed.STATE_OK
        assert any("not_entitled" in f for f in result["failures"])
        assert result["inserted"] == 2

    def test_a_total_outage_is_reported_as_unavailable(self):
        conn = FakeConn()
        client = FakeClient(failures={
            p: ed.DiscoverySourceUnavailable("provider_unavailable")
            for p in ed.ENDPOINTS.values()})
        result = self.run(conn, client)
        assert result["status"] == ed.STATE_UNAVAILABLE
        assert len(result["failures"]) == 3

    def test_the_refresh_never_raises(self):
        conn = FakeConn()
        client = FakeClient(failures={p: RuntimeError("boom")
                                      for p in ed.ENDPOINTS.values()})
        result = self.run(conn, client)   # must not raise
        assert result["status"] == ed.STATE_UNAVAILABLE


class TestClientConstruction:
    def test_a_missing_key_is_refused_at_construction(self):
        with pytest.raises(ed.DiscoverySourceUnavailable) as exc:
            ed.FmpDiscoveryClient("")
        assert exc.value.reason == "missing_api_key"

    def test_it_uses_the_current_base_not_the_dead_legacy_one(self):
        # /api/v3 returns HTTP 403 "Legacy Endpoint" as of 2026-08-28. A client
        # silently pointed there would report the provider as down forever.
        client = ed.FmpDiscoveryClient("k")
        assert client.base_url.endswith("/stable")
        assert "/api/v3" not in client.base_url


class TestIsolation:
    def test_discovery_is_not_an_external_signal_source(self):
        # A mover is a measurement, not a claim. If it were fed into the
        # confluence reading, "it moved a lot" would start counting as a source
        # agreeing with the scanner.
        import app.external_signals as ex
        assert ed.SOURCE_FMP not in ex.WEBHOOK_SOURCES

    def test_the_product_api_does_not_read_discovery(self):
        # The licence permits ingesting this data for our own research and not
        # displaying it to third parties. The code boundary matches the licence
        # boundary rather than relying on someone remembering it.
        import pathlib
        scanner = (pathlib.Path(__file__).resolve().parents[1]
                   / "app" / "routers" / "scanner.py").read_text()
        assert "external_discovery" not in scanner
        assert "external_discovery_candidates" not in scanner

    def test_no_decision_module_can_see_discovery(self):
        import ast
        import pathlib
        app_dir = pathlib.Path(__file__).resolve().parents[1] / "app"
        for name in ("scanner_view.py", "market_context.py",
                     "prospective_campaign.py", "external_signals.py"):
            tree = ast.parse((app_dir / name).read_text())
            names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names.update(a.name for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names.add(node.module)
            assert not any(n.endswith("external_discovery") for n in names), name
