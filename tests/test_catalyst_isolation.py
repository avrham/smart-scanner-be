"""The guarantee that catalyst context stays CONTEXT.

The product promise is: "this setup exists, AND earnings are approaching" —
never "this setup is better/worse because earnings are approaching". That
promise is only worth anything if it is structurally impossible to break, so
these tests assert the boundary rather than the wording:

  * no decision-making module may even import the catalyst layer;
  * attention, verdict and Wyckoff outputs must be byte-identical whether a
    symbol has an imminent earnings event or none at all;
  * the catalyst layer must not be able to write anything a decision reads.
"""

import ast
import pathlib
from datetime import date, datetime, timezone

import app.catalyst as cat
import app.scanner_view as sv

UTC = timezone.utc
APP = pathlib.Path(__file__).resolve().parents[1] / "app"

# Everything that decides WHAT the scanner says about a setup. None of these may
# know that catalysts exist.
DECISION_MODULES = (
    "scanner_view.py",
    "market_context.py",
    "prospective_campaign.py",
    "prospective_readiness.py",
    "prospective_session.py",
    "reference_market.py",
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


class TestNoDecisionModuleKnowsAboutCatalysts:
    def test_decision_modules_do_not_import_the_catalyst_layer(self):
        offenders = {}
        for name in DECISION_MODULES:
            path = APP / name
            assert path.exists(), f"{name} moved; update this guard"
            bad = {m for m in imported_names(path) if "catalyst" in m}
            if bad:
                offenders[name] = sorted(bad)
        assert offenders == {}, (
            "a decision module imported the catalyst layer — catalyst data must "
            f"never be reachable from a verdict: {offenders}")

    def test_decision_modules_never_mention_catalysts_at_all(self):
        # Stronger than the import check: catches a dict lookup like
        # row["catalyst_context"] sneaking into a ranking function.
        offenders = [n for n in DECISION_MODULES
                     if "catalyst" in (APP / n).read_text().lower()]
        assert offenders == [], (
            f"catalyst referenced inside decision code: {offenders}")

    def test_the_catalyst_layer_does_not_reach_back_into_decisions(self):
        # It may use the trading calendar, and nothing else from the strategy.
        for name in ("catalyst.py", "catalyst_ingest.py"):
            reached = {m for m in imported_names(APP / name)
                       if m.startswith("app.")}
            assert reached <= {"app.catalyst", "app.prospective_session",
                               "app.workers.massive_client",
                               # Pure cohort vocabulary (028): constants and two
                               # predicates, no DB, no strategy, no decision code.
                               "app.source_scope"}, \
                f"{name} imports decision code: {sorted(reached)}"


class TestAttentionIsUnmovedByCatalysts:
    """A row carrying an imminent earnings event must be classified exactly as
    the same row with no event."""

    SESSION = date(2026, 8, 26)

    def row(self, **over):
        base = {
            "symbol": "AAPL", "verdict": "WATCH", "candidate_score": 0.71,
            "setup_present": True, "close": 210.0, "cross_arm": "armed",
            "candidate_details": {"structure_state": "accumulation"},
        }
        base.update(over)
        return base

    def catalysts(self, notable):
        events = ([{"symbol": "AAPL", "event_type": cat.EVENT_EARNINGS,
                    "event_date": date(2026, 8, 27), "session_timing": "after_market",
                    "certainty": "confirmed", "fiscal_period": "Q3",
                    "fiscal_year": "2026", "source": "s", "source_reference": "r",
                    "observed_at": datetime(2026, 8, 20, tzinfo=UTC)}]
                  if notable else [])
        ok = {"status": cat.STATUS_AVAILABLE, "reason": None,
              "last_refresh_at": None, "last_success_at": None,
              "age_hours": 1.0, "detail": None}
        return cat.build_catalyst_context(
            events, as_of_session=self.SESSION,
            earnings_freshness=ok, filings_freshness=ok)

    def test_a_notable_catalyst_is_actually_present_in_the_fixture(self):
        # Guards the two tests below from passing vacuously.
        assert self.catalysts(True)["earnings"]["notable"] is True
        assert self.catalysts(False)["earnings"]["notable"] is False

    def test_attention_cannot_physically_receive_catalyst_data(self):
        # `classify_attention` takes named scalars, not a row dict — so there is
        # no argument through which a catalyst field could ever reach it. This
        # is the structural version of "catalysts don't change attention".
        import inspect
        params = inspect.signature(sv.classify_attention).parameters
        assert all(p.kind is inspect.Parameter.KEYWORD_ONLY
                   for p in params.values())
        assert set(params) == {
            "has_candidate_result", "candidate_verdict", "setup_state",
            "readiness_status", "control_verdict"}, (
            "classify_attention grew an input — check it is not catalyst-derived")

    def test_the_overview_row_is_unchanged_apart_from_the_added_field(self):
        plain = sv.build_overview_row(self.row(), scanner_state="current")
        attached = sv.build_overview_row(
            self.row(catalyst_context=cat.build_row_catalyst(self.catalysts(True))),
            scanner_state="current")
        assert plain == attached, (
            "catalyst data leaked into the decision fields of an overview row")

    def test_sort_order_does_not_change_when_one_symbol_has_earnings(self):
        rows = [self.row(symbol=s, verdict=v, candidate_score=c)
                for s, v, c in (("AAPL", "WATCH", 0.71), ("MSFT", "AVOID", 0.40),
                                ("NVDA", "WATCH", 0.90))]
        built = [sv.build_overview_row(r, scanner_state="current") for r in rows]
        before = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]

        for r, notable in zip(built, (True, False, True)):
            r["catalyst_context"] = cat.build_row_catalyst(self.catalysts(notable))
        after = [r["symbol"] for r in sorted(built, key=sv.attention_sort_key)]
        assert before == after

    def test_the_attention_summary_is_unchanged(self):
        rows = [sv.build_overview_row(self.row(symbol=s), scanner_state="current")
                for s in ("AAPL", "MSFT")]
        before = sv.summarize_attention(rows)
        for r in rows:
            r["catalyst_context"] = cat.build_row_catalyst(self.catalysts(True))
        assert sv.summarize_attention(rows) == before


class TestCatalystsCreateNoRecommendation:
    def test_the_vocabulary_contains_no_action_words(self):
        forbidden = {"enter", "buy", "sell", "long", "short", "signal",
                     "recommend", "bullish", "bearish", "score", "rank",
                     "good", "bad", "better", "worse", "risk"}
        vocabulary = set(cat.PROXIMITIES) | set(cat.CATALYST_STATUSES) | {
            cat.SAME_SESSION_BEFORE_OPEN, cat.SAME_SESSION_INTRADAY,
            cat.SAME_SESSION_AFTER_CLOSE, cat.SAME_SESSION_UNKNOWN,
        } | set(cat.CERTAINTIES) | set(cat.SESSION_TIMINGS)
        for word in vocabulary:
            parts = set(word.split("_"))
            assert not (parts & forbidden), \
                f"catalyst vocabulary '{word}' reads as a recommendation"

    def test_no_catalyst_output_is_a_number_that_could_be_ranked(self):
        # Session distances are descriptive facts; there is deliberately no
        # composite catalyst score for anything to sort or threshold on.
        ok = {"status": cat.STATUS_AVAILABLE, "reason": None,
              "last_refresh_at": None, "last_success_at": None,
              "age_hours": 1.0, "detail": None}
        context = cat.build_catalyst_context(
            [], as_of_session=date(2026, 8, 26),
            earnings_freshness=ok, filings_freshness=ok)
        numeric = {k for k, v in context["earnings"].items()
                   if isinstance(v, (int, float)) and not isinstance(v, bool)}
        assert numeric <= {"sessions_until", "calendar_days_until"}
        assert not any("score" in k or "rating" in k or "weight" in k
                       for k in context["earnings"])
