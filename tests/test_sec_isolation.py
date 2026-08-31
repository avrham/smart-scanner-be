"""SEC events are CONTEXT. This file is the structural proof, not the promise.

Wording changes; boundaries do not. These tests assert that no module which
decides what the scanner SAYS about a setup can reach the SEC layer, that the
SEC layer produces nothing rankable, and that adding it left Earnings V1 and
News V1 exactly where they were.
"""

import ast
import pathlib
from datetime import date, datetime, timezone

import app.scanner_view as sv
import app.sec_events as se

UTC = timezone.utc
APP = pathlib.Path(__file__).resolve().parents[1] / "app"

# Everything that decides WHAT the scanner says about a setup. None of these may
# know that SEC filings exist.
DECISION_MODULES = (
    "scanner_view.py",
    "market_context.py",
    "prospective_campaign.py",
    "prospective_readiness.py",
    "prospective_session.py",
    "reference_market.py",
    "catalyst.py",     # earnings must not learn about filings either
    "news.py",         # nor may news
)


def imported_names(path: pathlib.Path):
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


class TestNoDecisionModuleKnowsAboutSecFilings:
    def test_decision_modules_do_not_import_the_sec_layer(self):
        offenders = {}
        for name in DECISION_MODULES:
            path = APP / name
            assert path.exists(), f"{name} moved; update this guard"
            bad = {m for m in imported_names(path)
                   if m.split(".")[-1].startswith("sec_")}
            if bad:
                offenders[name] = sorted(bad)
        assert offenders == {}, (
            "a decision module imported the SEC layer — filings must never be "
            f"reachable from a verdict: {offenders}")

    def test_decision_modules_never_mention_sec_events_at_all(self):
        # Stronger than the import check: catches a dict lookup like
        # row["catalyst_context"]["sec_events"] sneaking into a ranking function.
        offenders = [n for n in DECISION_MODULES
                     if "sec_events" in (APP / n).read_text().lower()]
        assert offenders == [], f"SEC referenced inside decision code: {offenders}"

    def test_the_sec_layer_does_not_reach_back_into_decisions(self):
        # It may use the trading calendar, the catalyst layer's session
        # arithmetic and the news layer's market clock — nothing else.
        for name in ("sec_events.py", "sec_ingest.py"):
            reached = {m for m in imported_names(APP / name) if m.startswith("app.")}
            assert reached <= {"app.sec_events", "app.news", "app.catalyst",
                               "app.prospective_session",
                               # Pure cohort vocabulary (028): constants only.
                               "app.source_scope"}, \
                f"{name} imports decision code: {sorted(reached)}"

    def test_the_two_closed_layers_are_unchanged_by_the_arrival_of_sec(self):
        # Earnings V1 and News V1 are closed. Their contract versions are
        # canaries: if SEC work ever edits either model, this fails.
        import app.catalyst as cat
        import app.news as nw
        assert cat.CATALYST_CONTEXT_CONTRACT_VERSION == \
            "smart_scanner_catalyst_context.v1"
        assert nw.NEWS_CONTEXT_CONTRACT_VERSION == "smart_scanner_news_context.v1"
        earnings = cat.build_catalyst_context(
            [], as_of_session=None,
            earnings_freshness={"status": "unavailable", "reason": None},
            filings_freshness={"status": "unavailable", "reason": None})
        assert "sec_events" not in earnings
        assert "sec_events" not in nw.empty_news_context()

    def test_sec_does_not_live_inside_the_news_block(self):
        # An article is somebody's account of an event; a filing is the
        # registrant's own disclosure of one. Nesting would let commentary
        # borrow the authority of a filing.
        import app.news as nw
        assert "sec" not in repr(nw.empty_news_context()).lower()


class TestAttentionIsUnmovedBySecFilings:
    def row(self, **over):
        base = {"symbol": "AAPL", "verdict": "WATCH", "candidate_score": 0.71,
                "setup_present": True, "close": 210.0, "cross_arm": "armed",
                "candidate_details": {"structure_state": "accumulation"}}
        base.update(over)
        return base

    def loud(self):
        return se.build_row_sec(se.build_sec_context([{
            "accession_number": "0000320193-26-000018", "cik": "0000320193",
            "form": "8-K", "accepted_at": datetime(2026, 8, 25, 13, tzinfo=UTC),
            "filing_date": date(2026, 8, 25), "period_of_report": date(2026, 8, 24),
            "item_codes": ["5.02", "9.01"],
            "event_types": ["management_change", "financial_statements_and_exhibits"],
            "taxonomy_version": se.SEC_TAXONOMY_VERSION, "is_primary_event": True,
            "amends_accession_number": None,
            "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/x/a.htm",
        }], as_of_session=date(2026, 8, 25),
            freshness={"status": se.STATUS_AVAILABLE, "reason": None,
                       "last_refresh_at": None, "last_success_at": None,
                       "age_hours": 1.0, "detail": None}))

    def test_attention_cannot_physically_receive_sec_data(self):
        import inspect
        params = inspect.signature(sv.classify_attention).parameters
        assert not any("sec" in p for p in params)
        assert not any("filing" in p for p in params)

    def test_sort_order_does_not_change_when_one_symbol_has_a_filing(self):
        rows = [self.row(symbol=s, verdict=v, candidate_score=c)
                for s, v, c in (("AAPL", "WATCH", 0.71), ("MSFT", "AVOID", 0.40),
                                ("NVDA", "WATCH", 0.90))]
        built = [sv.build_overview_row(r, scanner_state="current") for r in rows]
        before = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]
        loud = self.loud()
        assert loud["notable_count"] == 1          # the fixture really is loud
        built[0]["catalyst_context"] = {"sec_events": loud}
        after = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]
        assert before == after

    def test_the_attention_summary_is_unchanged(self):
        rows = [sv.build_overview_row(self.row(symbol=s), scanner_state="current")
                for s in ("AAPL", "MSFT")]
        before = sv.summarize_attention(rows)
        for r in rows:
            r["catalyst_context"] = {"sec_events": {"notable_count": 3}}
        assert sv.summarize_attention(rows) == before


class TestTheSecVocabularyCannotBecomeARanking:
    def test_no_event_family_carries_a_direction(self):
        # Every family names WHAT KIND of event was disclosed. None of them says
        # whether it was good news, because an 8-K does not contain that.
        blob = " ".join(se.EVENT_TYPES).lower()
        for banned in ("positive", "negative", "bullish", "bearish", "good",
                       "bad", "risk", "opportunity", "beat", "miss"):
            assert banned not in blob, f"{banned} leaked into the SEC taxonomy"

    def test_supporting_is_a_structural_fact_not_an_importance_tier(self):
        # There are exactly two supporting items and both describe HOW something
        # was furnished. Nothing else is demoted, and nothing is promoted.
        assert se.SUPPORTING_ITEMS == {"7.01", "9.01"}

    def test_notability_returns_a_boolean_never_a_magnitude(self):
        assert se.is_notable(se.PROXIMITY_TODAY, primary=True) is True
        assert se.is_notable(se.PROXIMITY_TODAY, primary=False) is False

    def test_no_legal_or_interpretive_text_is_produced(self):
        # The layer reports codes and links. It never explains a filing.
        source = (APP / "sec_events.py").read_text().lower()
        for banned in ("means that", "indicates that", "suggests that",
                       "summary of the filing"):
            assert banned not in source
