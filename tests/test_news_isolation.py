"""News is CONTEXT. This file is the structural proof, not the promise.

Wording changes; boundaries do not. These tests assert that no module which
decides what the scanner SAYS about a setup can even reach the news layer, that
the news layer produces nothing rankable, and that no machine opinion survives
anywhere in the product surface.
"""

import ast
import pathlib
from datetime import date, datetime, timezone

import app.news as nw
import app.scanner_view as sv

UTC = timezone.utc
APP = pathlib.Path(__file__).resolve().parents[1] / "app"

# Everything that decides WHAT the scanner says about a setup. None of these may
# know that news exists.
DECISION_MODULES = (
    "scanner_view.py",
    "market_context.py",
    "prospective_campaign.py",
    "prospective_readiness.py",
    "prospective_session.py",
    "reference_market.py",
    "catalyst.py",          # earnings must not learn about news either
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


class TestNoDecisionModuleKnowsAboutNews:
    def test_decision_modules_do_not_import_the_news_layer(self):
        offenders = {}
        for name in DECISION_MODULES:
            path = APP / name
            assert path.exists(), f"{name} moved; update this guard"
            bad = {m for m in imported_names(path) if m.split(".")[-1].startswith("news")}
            if bad:
                offenders[name] = sorted(bad)
        assert offenders == {}, (
            "a decision module imported the news layer — news must never be "
            f"reachable from a verdict: {offenders}")

    def test_decision_modules_never_mention_news_at_all(self):
        # Stronger than the import check: catches a dict lookup like
        # row["catalyst_context"]["news"] sneaking into a ranking function.
        offenders = [n for n in DECISION_MODULES
                     if "news" in (APP / n).read_text().lower()]
        assert offenders == [], f"news referenced inside decision code: {offenders}"

    def test_the_news_layer_does_not_reach_back_into_decisions(self):
        # It may use the trading calendar and the catalyst layer's session
        # arithmetic, and nothing else from the strategy.
        for name in ("news.py", "news_ingest.py"):
            reached = {m for m in imported_names(APP / name) if m.startswith("app.")}
            assert reached <= {"app.news", "app.catalyst", "app.prospective_session",
                               "app.workers.massive_client",
                               # Pure cohort vocabulary (028): constants only.
                               "app.source_scope"}, \
                f"{name} imports decision code: {sorted(reached)}"

    def test_earnings_context_is_unchanged_by_the_arrival_of_news(self):
        # Catalyst V1 is closed. Its contract version is a canary: if news work
        # ever edits the earnings model, this fails.
        import app.catalyst as cat
        assert cat.CATALYST_CONTEXT_CONTRACT_VERSION == \
            "smart_scanner_catalyst_context.v1"
        assert "news" not in cat.build_catalyst_context(
            [], as_of_session=None,
            earnings_freshness={"status": "unavailable", "reason": None},
            filings_freshness={"status": "unavailable", "reason": None})


class TestAttentionIsUnmovedByNews:
    """A row carrying a fresh catalyst headline must be classified exactly as
    the same row with none."""

    SESSION = date(2026, 8, 25)

    def row(self, **over):
        base = {"symbol": "AAPL", "verdict": "WATCH", "candidate_score": 0.71,
                "setup_present": True, "close": 210.0, "cross_arm": "armed",
                "candidate_details": {"structure_state": "accumulation"}}
        base.update(over)
        return base

    def test_attention_cannot_physically_receive_news_data(self):
        import inspect
        params = inspect.signature(sv.classify_attention).parameters
        assert "news" not in params
        assert not any("news" in p for p in params)

    def test_sort_order_does_not_change_when_one_symbol_has_news(self):
        rows = [self.row(symbol=s, verdict=v, candidate_score=c)
                for s, v, c in (("AAPL", "WATCH", 0.71), ("MSFT", "AVOID", 0.40),
                                ("NVDA", "WATCH", 0.90))]
        built = [sv.build_overview_row(r, scanner_state="current") for r in rows]
        before = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]
        loud = nw.build_row_news(nw.build_news_context(
            [{"published_at": datetime(2026, 8, 25, 14, tzinfo=UTC),
              "title": "Apple unveils new Macs",
              "title_normalized": "apple unveils new macs",
              "publisher": "Reuters", "article_url": "https://r.com/1",
              "category": nw.CATEGORY_PRODUCT_ANNOUNCEMENT,
              "category_source": nw.CATEGORY_SOURCE_DERIVED_TITLE,
              "scope": nw.SCOPE_COMPANY_SPECIFIC,
              "relevance": nw.RELEVANCE_PRIMARY, "ticker_breadth": 1}],
            symbol="AAPL", as_of_session=date(2026, 8, 25),
            freshness={"status": nw.STATUS_AVAILABLE, "reason": None,
                       "last_refresh_at": None, "last_success_at": None,
                       "age_hours": 1.0, "detail": None}))
        assert loud["notable_count"] == 1          # the fixture really is loud
        built[0]["catalyst_context"] = {"news": loud}
        after = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]
        assert before == after

    def test_the_attention_summary_is_unchanged(self):
        rows = [sv.build_overview_row(self.row(symbol=s), scanner_state="current")
                for s in ("AAPL", "MSFT")]
        before = sv.summarize_attention(rows)
        for r in rows:
            r["catalyst_context"] = {"news": {"notable_count": 3}}
        assert sv.summarize_attention(rows) == before


class TestTheNewsVocabularyCannotBecomeARanking:
    def _sample_item(self):
        ctx = nw.build_news_context(
            [{"published_at": datetime(2026, 8, 25, 14, tzinfo=UTC),
              "title": "Apple upgraded at Morgan Stanley",
              "title_normalized": "apple upgraded at morgan stanley",
              "publisher": "Reuters", "article_url": "https://r.com/1",
              "category": nw.CATEGORY_ANALYST_ACTION,
              "category_source": nw.CATEGORY_SOURCE_DERIVED_TITLE,
              "scope": nw.SCOPE_COMPANY_SPECIFIC,
              "relevance": nw.RELEVANCE_PRIMARY, "ticker_breadth": 1}],
            symbol="AAPL", as_of_session=date(2026, 8, 25),
            freshness={"status": nw.STATUS_AVAILABLE, "reason": None,
                       "last_refresh_at": None, "last_success_at": None,
                       "age_hours": 1.0, "detail": None})
        return ctx

    def test_no_output_is_a_score_a_rating_or_a_weight(self):
        blob = repr(self._sample_item()).lower()
        for banned in ("score", "rating", "weight", "rank", "confidence"):
            assert banned not in blob, f"{banned} leaked into the news contract"

    def test_no_output_is_a_sentiment_or_a_direction(self):
        # The provider ships sentiment on every article. If it ever reaches the
        # product surface, this is what catches it.
        blob = repr(self._sample_item()).lower()
        for banned in ("sentiment", "bullish", "bearish", "positive", "negative",
                       "reasoning"):
            assert banned not in blob, f"{banned} leaked into the news contract"

    def test_the_only_numbers_are_distances_and_counts(self):
        ctx = self._sample_item()
        numeric = {k for k, v in ctx.items() if isinstance(v, (int, float))
                   and not isinstance(v, bool)}
        numeric |= {k for k, v in ctx["items"][0].items()
                    if isinstance(v, (int, float)) and not isinstance(v, bool)}
        assert numeric <= {"sessions_ago", "in_window_count", "notable_count",
                           "window_sessions", "age_hours"}

    def test_the_vocabulary_contains_no_action_words(self):
        forbidden = {"buy", "sell", "long", "short", "enter", "exit", "target",
                     "bullish", "bearish", "signal", "recommend"}
        for word_list in (nw.CATEGORIES, nw.SCOPES, nw.RELEVANCES, nw.PROXIMITIES,
                          nw.NEWS_STATUSES):
            for token in word_list:
                assert not (set(token.split("_")) & forbidden), \
                    f"action word in the news vocabulary: {token}"

    def test_notability_is_a_silence_gate_not_a_comparison(self):
        # Two notable items are equally notable. Nothing in the model orders one
        # above the other, and `is_notable` returns a bool, never a magnitude.
        assert nw.is_notable(nw.PROXIMITY_TODAY, scope=nw.SCOPE_COMPANY_SPECIFIC,
                             relevance=nw.RELEVANCE_PRIMARY) is True
        assert nw.is_notable(nw.PROXIMITY_RECENT, scope=nw.SCOPE_COMPANY_SPECIFIC,
                             relevance=nw.RELEVANCE_PRIMARY) is True
