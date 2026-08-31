"""Research lifecycle automation: conservation, cohorts, dispatch, boundaries.

Each class here corresponds to a thing that was true and unverifiable in the
previous milestone, or unsafe and therefore deferred:

  * the funnel's totals could not be reconciled — now they must add up or fail;
  * "conversion" named two different rates over two different populations;
  * a research catalyst refresh would have claimed the PRODUCT's freshness;
  * the lifecycle could only be run by hand, so scheduling it was a rewrite.
"""

import ast
import asyncio
import pathlib
import re
from datetime import date, datetime, timezone

import pytest

import app.jobs.research_lifecycle as RL
import app.research_admission as ra
import app.research_enrichment as renrich
import app.research_funnel as rf
import app.research_runs as rr
import app.research_universe as ru
import app.source_scope as ss

UTC = timezone.utc
NOW = datetime(2026, 8, 31, 22, 30, tzinfo=UTC)
SESSION = date(2026, 8, 28)
MIGRATIONS = pathlib.Path("app/db/migrations")


def _row(symbol, *, admission=ra.ADMISSION_ELIGIBLE, state=None, candidate=None):
    return {"symbol": symbol, "admission_state": admission, "state": state,
            "candidate_state": candidate}


def _executable_source(path: str) -> str:
    """A module's code with DOCSTRINGS removed and SQL literals KEPT.

    Prose in these files names the relations and scopes deliberately; matching
    raw text would fail on a correct file. A query string is exactly where an
    accidental read or write would appear, so string EXPRESSION STATEMENTS (a
    docstring, precisely) are blanked and nothing else. `ast.unparse` drops
    `#` comments on its own.
    """
    tree = ast.parse(open(path, encoding="utf-8").read())
    for node in ast.walk(tree):
        for field in ("body", "orelse", "finalbody"):
            statements = getattr(node, field, None)
            if not isinstance(statements, list):
                continue
            for statement in statements:
                if (isinstance(statement, ast.Expr)
                        and isinstance(statement.value, ast.Constant)
                        and isinstance(statement.value.value, str)):
                    statement.value.value = ""
    return ast.unparse(tree)


# =========================================================================== #
# P0 — funnel accounting
# =========================================================================== #

class TestFunnelPartition:
    def test_every_symbol_lands_in_exactly_one_state(self):
        # Exhaustiveness by exercise: one row per shape the columns can take,
        # including the contradictory ones.
        rows = [
            _row("A", admission=None),
            _row("B", admission=ra.ADMISSION_REJECTED),
            _row("C", state=ru.STATE_DISCOVERED),
            _row("D", state=ru.STATE_HISTORY_REQUIRED),
            _row("E", state=ru.STATE_HISTORY_WARMING),
            _row("F", state=ru.STATE_UNAVAILABLE),
            _row("G", state=ru.STATE_FAILED),
            _row("H", state=ru.STATE_RESEARCH_READY),
            _row("I", state=ru.STATE_RESEARCH_SCANNED),
            _row("J", state=ru.STATE_RESEARCH_SCANNED,
                 candidate=ru.CANDIDATE_RESEARCH_CANDIDATE),
            _row("K", state=ru.STATE_RESEARCH_SCANNED,
                 candidate=ru.CANDIDATE_SCANNED_NOT_CANDIDATE),
            _row("L", admission=ra.ADMISSION_UNKNOWN, state=None),
            _row("M", state="a_state_nobody_has_defined_yet"),
        ]
        states = [rf.lifecycle_state(r) for r in rows]
        assert all(s in rf.LIFECYCLE_STATES for s in states)
        summary = rf.summarise(rows)
        assert sum(summary["states"].values()) == len(rows)

    def test_a_rejected_symbol_can_never_also_be_counted_downstream(self):
        # The dangerous shape: rejected, but carrying stale downstream columns
        # from before the gate existed. It must count ONCE, as rejected.
        row = _row("CELU", admission=ra.ADMISSION_REJECTED,
                   state=ru.STATE_RESEARCH_SCANNED,
                   candidate=ru.CANDIDATE_RESEARCH_CANDIDATE)
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_ADMISSION_REJECTED
        summary = rf.summarise([row])
        assert summary["states"][rf.LIFECYCLE_RESEARCH_CANDIDATE] == 0
        assert summary["conservation"]["ok"]

    def test_a_scanned_but_unclassified_symbol_is_not_a_non_candidate(self):
        # Folding "not yet judged" into "did not survive" is how a symbol gets
        # reported as rejected before anything judged it.
        row = _row("X", state=ru.STATE_RESEARCH_SCANNED, candidate=None)
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_CLASSIFICATION_PENDING
        assert rf.lifecycle_state(row) != rf.LIFECYCLE_SCANNED_NOT_CANDIDATE

    def test_admission_tier_survives_a_later_history_failure(self):
        # A symbol that PASSED admission and then failed warmup still passed
        # admission — otherwise the pass rate shrinks as symbols move on.
        row = _row("Y", state=ru.STATE_FAILED)
        assert rf.lifecycle_state(row) == rf.LIFECYCLE_HISTORY_FAILED
        assert rf.admission_tier(row) == ra.ADMISSION_ELIGIBLE


class TestFunnelConservation:
    def _live_cohort(self):
        # The exact shape of the previously reported live run.
        return ([_row(f"R{i}", admission=ra.ADMISSION_REJECTED) for i in range(30)]
                + [_row(f"P{i}", state=ru.STATE_HISTORY_REQUIRED) for i in range(6)]
                + [_row(f"U{i}", state=ru.STATE_UNAVAILABLE) for i in range(2)]
                + [_row("ONDS", state=ru.STATE_RESEARCH_SCANNED,
                        candidate=ru.CANDIDATE_RESEARCH_CANDIDATE)]
                + [_row(f"S{i}", state=ru.STATE_RESEARCH_SCANNED,
                        candidate=ru.CANDIDATE_SCANNED_NOT_CANDIDATE)
                   for i in range(6)])

    def test_the_live_cohort_conserves(self):
        summary = rf.summarise(self._live_cohort(), provider_calls_used=7,
                               provider_calls_avoided=30)
        assert summary["selected_for_research"] == 45
        assert summary["admission"] == {"passed": 15, "rejected": 30,
                                        "unknown": 0, "pending": 0}
        assert summary["scanned"] == 7
        assert summary["research_candidates"] == 1
        assert summary["conservation"]["ok"]
        rf.assert_conservation(summary)

    def test_impossible_totals_fail_loudly(self):
        summary = rf.summarise(self._live_cohort(), provider_calls_used=7,
                               provider_calls_avoided=30)
        # Hand-corrupt one counter, exactly as a stray FILTER clause would.
        summary["states"][rf.LIFECYCLE_RESEARCH_CANDIDATE] += 3
        result = rf.check_conservation(summary)
        assert not result["ok"]
        with pytest.raises(rf.FunnelConservationError):
            rf.assert_conservation(summary)

    def test_avoided_calls_must_equal_rejections(self):
        summary = rf.summarise(self._live_cohort(), provider_calls_used=7,
                               provider_calls_avoided=29)   # off by one
        violated = {v["invariant"] for v in summary["conservation"]["violations"]}
        assert "avoided_calls_equal_rejections" in violated

    def test_conservation_error_is_not_swallowed_by_a_broad_except(self):
        # It subclasses AssertionError precisely so an `except Exception` that
        # was written to absorb provider failures cannot hide it.
        assert issubclass(rf.FunnelConservationError, AssertionError)
        assert not issubclass(rf.FunnelConservationError, ValueError)


class TestRateDenominators:
    def test_the_two_rates_are_different_measurements(self):
        summary = rf.summarise(
            [_row(f"R{i}", admission=ra.ADMISSION_REJECTED) for i in range(30)]
            + [_row(f"P{i}", state=ru.STATE_HISTORY_REQUIRED) for i in range(6)]
            + [_row(f"U{i}", state=ru.STATE_UNAVAILABLE) for i in range(2)]
            + [_row("ONDS", state=ru.STATE_RESEARCH_SCANNED,
                    candidate=ru.CANDIDATE_RESEARCH_CANDIDATE)]
            + [_row(f"S{i}", state=ru.STATE_RESEARCH_SCANNED,
                    candidate=ru.CANDIDATE_SCANNED_NOT_CANDIDATE)
               for i in range(6)],
            provider_calls_used=7, provider_calls_avoided=30)
        admission = summary["rates"]["admission_pass_rate"]
        candidate = summary["rates"]["candidate_conversion_rate"]
        assert (admission["numerator"], admission["denominator"]) == (15, 45)
        assert admission["percent"] == 33.3
        assert admission["of"] == "symbols_selected_for_research"
        assert (candidate["numerator"], candidate["denominator"]) == (1, 7)
        assert candidate["percent"] == 14.3
        assert candidate["of"] == "symbols_scanned"
        # The two must never share a denominator or a population name.
        assert admission["denominator"] != candidate["denominator"]
        assert admission["of"] != candidate["of"]

    def test_a_rate_is_never_quotable_without_its_denominator(self):
        r = rf.rate(1, 7, of="symbols_scanned")
        assert set(r) == {"numerator", "denominator", "of", "percent"}

    def test_zero_over_zero_is_not_zero_percent(self):
        # "we scanned nothing" must not be reported as "nothing converted".
        assert rf.rate(0, 0, of="symbols_scanned")["percent"] is None

    def test_calls_per_candidate_is_none_with_no_candidates(self):
        summary = rf.summarise([_row("A", state=ru.STATE_RESEARCH_SCANNED,
                                     candidate=ru.CANDIDATE_SCANNED_NOT_CANDIDATE)],
                               provider_calls_used=5)
        assert summary["provider"]["calls_per_candidate"] is None


# =========================================================================== #
# P2 — cohort-scoped source freshness
# =========================================================================== #

class TestSourceScope:
    def test_silence_means_the_product_cohort(self):
        # Every writer that predates cohorts keeps writing the row it wrote.
        assert ss.normalise_scope(None) == ss.SCOPE_PRODUCT
        assert ss.DEFAULT_SCOPE == ss.SCOPE_PRODUCT

    def test_an_unrecognised_scope_raises_rather_than_defaulting(self):
        # Coercing a typo to `product` would turn it into a product write —
        # the single failure this vocabulary exists to prevent.
        with pytest.raises(ss.UnknownSourceScope):
            ss.normalise_scope("reserch")
        with pytest.raises(ss.UnknownSourceScope):
            ss.normalise_scope("")

    def test_research_is_not_a_product_scope(self):
        assert not ss.is_product_scope(ss.SCOPE_RESEARCH)
        assert ss.is_product_scope(ss.SCOPE_PRODUCT)

    def test_the_scope_is_a_column_and_never_encoded_in_the_source_name(self):
        # The rejected shortcut. `source` is a shared vocabulary matched by
        # LIKE 'external\_%' in live RLS and prefixed by source_state_key();
        # a composite name would make every one of those rules quietly wrong.
        for path in ("app/sec_ingest.py", "app/news_ingest.py",
                     "app/catalyst_ingest.py", "app/research_enrichment.py"):
            source = _executable_source(path)
            assert "sec_edgar:research" not in source
            assert ":research" not in source


class TestSourceStateWriters:
    STANDARD = ("app/catalyst_ingest.py", "app/news_ingest.py",
                "app/sec_ingest.py", "app/external_ingest.py",
                "app/external_discovery.py", "app/analyst_events.py",
                "app/macro_ingest.py")

    def test_every_writer_conflicts_on_the_composite_key(self):
        # Migration 028 re-keys the table. A writer still saying
        # ON CONFLICT (source) would raise on every refresh.
        for path in self.STANDARD:
            source = _executable_source(path)
            assert "ON CONFLICT (source, scope)" in source, path
            assert "ON CONFLICT (source)" not in source, path

    def test_the_three_research_sources_take_a_scope_and_the_rest_pin_product(self):
        import app.catalyst_ingest as ci
        import app.news_ingest as ni
        import app.sec_ingest as si
        import inspect
        for module in (ci, ni, si):
            params = inspect.signature(module.record_source_state).parameters
            assert "scope" in params, module.__name__
            assert params["scope"].default is None
        # The market-wide / product-only sources take no scope at all: there is
        # no research cohort for an FOMC meeting or a pushed TradingView alert.
        import app.analyst_events as ae
        import app.external_discovery as ed
        import app.external_ingest as ei
        import app.macro_ingest as mi
        for module in (ae, ed, ei, mi):
            params = inspect.signature(module.record_source_state).parameters
            assert "scope" not in params, module.__name__
            assert "SCOPE_PRODUCT" in _executable_source(
                module.__file__.replace(str(pathlib.Path.cwd()) + "/", ""))

    def test_the_refreshers_thread_scope_all_the_way_down(self):
        import inspect
        import app.catalyst_ingest as ci
        import app.news_ingest as ni
        import app.sec_ingest as si
        for fn in (si.refresh_sec_filings, ni.refresh_company_news,
                   ci.refresh_catalysts):
            assert "scope" in inspect.signature(fn).parameters, fn.__name__


class TestProductReadsCanonicalScopeOnly:
    def test_the_product_freshness_query_is_scoped(self):
        source = _executable_source("app/routers/scanner.py")
        assert "FROM catalyst_source_state " in source
        assert "WHERE scope = $1" in source
        assert "SCOPE_PRODUCT" in source

    def test_the_external_dimension_join_is_scoped(self):
        source = _executable_source("app/routers/external.py")
        assert "c.scope = $2" in source

    def test_no_product_router_reads_the_research_scope(self):
        for path in ("app/routers/scanner.py", "app/routers/external.py"):
            source = _executable_source(path)
            assert "'research'" not in source
            assert "SCOPE_RESEARCH" not in source


class TestScopeMigration:
    SQL = (MIGRATIONS / "028_source_state_scope.sql").read_text(encoding="utf-8")

    def test_existing_rows_default_to_product(self):
        # The backfill IS the default: the migration must not require a data
        # rewrite for existing product freshness to keep meaning what it meant.
        assert "ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'product'" \
            in self.SQL

    def test_the_primary_key_becomes_composite(self):
        assert "PRIMARY KEY USING INDEX catalyst_source_state_source_scope_uq" \
            in self.SQL
        # And the unique index is created BEFORE the old key is dropped, so the
        # table is never momentarily without an identity for `source`.
        assert (self.SQL.index("CREATE UNIQUE INDEX IF NOT EXISTS "
                               "catalyst_source_state_source_scope_uq")
                < self.SQL.index("DROP CONSTRAINT IF EXISTS "
                                 "catalyst_source_state_pkey"))

    def test_the_scope_vocabulary_is_constrained_in_the_database(self):
        assert "CHECK (scope IN ('product', 'research'))" in self.SQL

    def test_the_product_reader_policy_is_tightened_not_widened(self):
        assert "USING (scope = 'product')" in self.SQL
        # Convergent: an existing USING(true) predicate must actually be
        # replaced, not left in place by a CREATE-if-missing.
        assert "DROP POLICY IF EXISTS smart_scanner_product_reader_select" \
            in self.SQL


# =========================================================================== #
# P3 — lazy enrichment
# =========================================================================== #

class FakeEnrichConn:
    def __init__(self, candidates):
        self.candidates = candidates
        self.queries = []

    async def fetch(self, sql, *args):
        self.queries.append((sql, args))
        return [{"symbol": s} for s in self.candidates]


class TestLazyEnrichment:
    def test_only_research_candidates_are_eligible(self):
        conn = FakeEnrichConn(["ONDS"])
        assert asyncio.run(renrich.candidate_symbols(conn)) == ["ONDS"]
        sql, args = conn.queries[0]
        # Filtered on what the SCREEN found, never on why we looked.
        assert "candidate_state = $1" in sql
        assert args[0] == ru.CANDIDATE_RESEARCH_CANDIDATE
        assert "discovery_reasons" not in sql

    def test_scanned_not_candidate_is_never_enriched(self):
        conn = FakeEnrichConn([])
        summary = asyncio.run(renrich.enrich_research_candidates(conn, now=NOW))
        assert summary["enriched"] == 0
        assert summary["provider_requests"] == 0
        assert all(s["status"] == renrich.STATUS_SKIPPED
                   and s["reason"] == renrich.REASON_NO_CANDIDATES
                   for s in summary["sources"].values())

    def test_every_write_is_research_scoped(self):
        source = _executable_source("app/research_enrichment.py")
        assert "SCOPE_RESEARCH" in source
        assert "SCOPE_PRODUCT" not in source
        assert source.count("scope=SCOPE_RESEARCH") == 3   # sec, news, earnings

    def test_a_source_failure_is_isolated(self):
        conn = FakeEnrichConn(["ONDS"])
        summary = asyncio.run(renrich.enrich_research_candidates(
            conn, now=NOW, massive_api_key="", sec_user_agent=""))
        # No credential is `unavailable`, not an error, and the other sources
        # are still reported rather than skipped by an exception.
        assert set(summary["sources"]) == set(renrich.ENRICHMENT_SOURCES)
        assert summary["sources"][renrich.SOURCE_SEC]["status"] == \
            renrich.STATUS_UNAVAILABLE
        assert summary["sources"][renrich.SOURCE_SEC]["reason"] == \
            renrich.REASON_NO_USER_AGENT

    def test_an_exception_in_one_source_does_not_end_the_stage(self):
        conn = FakeEnrichConn(["ONDS"])

        async def boom():
            raise RuntimeError("edgar exploded")

        import app.research_enrichment as mod
        original = mod._enrich_sec
        mod._enrich_sec = lambda *a, **k: boom()
        try:
            summary = asyncio.run(mod.enrich_research_candidates(
                conn, now=NOW, sec_user_agent="x", massive_api_key=""))
        finally:
            mod._enrich_sec = original
        assert summary["sources"][mod.SOURCE_SEC]["status"] == mod.STATUS_ERROR
        # The class name only — never the message, which on an HTTP client can
        # carry a URL with a key in it.
        assert summary["sources"][mod.SOURCE_SEC]["reason"] == "RuntimeError"
        assert "exploded" not in str(summary)
        assert mod.SOURCE_NEWS in summary["sources"]

    def test_enrichment_cannot_change_a_candidate_state(self):
        # Catalysts are evidence attached to a candidate, never a promotion.
        # The module holds no write against research_symbols at all.
        source = _executable_source("app/research_enrichment.py")
        assert "UPDATE public.research_symbols" not in source
        assert "SET candidate_state" not in source
        assert "candidate_state=" not in source
        assert "INSERT INTO public.research_symbols" not in source

    def test_the_bound_is_small_and_hard(self):
        assert renrich.MAX_ENRICHED_SYMBOLS == 10
        conn = FakeEnrichConn(["A"] * 50)
        asyncio.run(renrich.candidate_symbols(conn, limit=999))
        assert conn.queries[0][1][1] == renrich.MAX_ENRICHED_SYMBOLS

    def test_enrichment_has_its_own_provider_budget(self):
        # Buying context for today's survivor must not cost tomorrow's symbol
        # its history, so the two ceilings are separate numbers.
        assert renrich.MAX_ENRICHMENT_PROVIDER_REQUESTS < \
            ru.MAX_PROVIDER_REQUESTS_PER_RUN

    def test_analyst_grades_are_excluded_on_the_record(self):
        assert "analyst_grades" in renrich.EXCLUDED_SOURCES
        assert "fmp" in renrich.EXCLUDED_SOURCES["analyst_grades"]
        assert "analyst" not in renrich.ENRICHMENT_SOURCES


# =========================================================================== #
# P1 / P8 — run persistence and measurement
# =========================================================================== #

class TestRunAudit:
    SQL = (MIGRATIONS / "029_research_lifecycle_runs.sql").read_text(encoding="utf-8")

    def test_exactly_once_accounting_is_a_primary_key(self):
        assert "PRIMARY KEY (run_id, symbol)" in self.SQL

    def test_a_run_key_is_unique_so_a_retry_cannot_fork_the_history(self):
        assert "research_lifecycle_runs_run_key_uq UNIQUE (run_key)" in self.SQL
        assert "ON CONFLICT (run_key) DO UPDATE" in rr.START_SQL

    def test_the_admission_partition_is_enforced_in_the_database(self):
        assert "research_lifecycle_runs_admission_partition_ck" in self.SQL
        assert ("admission_passed + admission_rejected\n"
                "       + admission_unknown + admission_pending "
                "= symbols_selected") in self.SQL

    def test_a_non_conserving_run_is_recorded_not_hidden(self):
        assert "funnel_conserved BOOLEAN NOT NULL DEFAULT TRUE" in self.SQL
        # And it is written before the assertion fires, so the evidence of the
        # bug survives the failure it causes.
        source = _executable_source("app/research_lifecycle.py")
        assert source.index("finish_run") < source.index("assert_conservation")

    def test_the_child_state_vocabulary_matches_the_partition(self):
        listed = set(re.findall(r"'([a-z_]+)'",
                                self.SQL.split("run_symbols_state_ck")[1]
                                .split("))")[0]))
        assert listed == set(rf.LIFECYCLE_STATES)

    def test_a_blocked_run_is_not_measurable(self):
        # A session in which nothing was attempted must not dilute a rate.
        assert rr.MEASURABLE_STATUSES == (rr.RUN_STATUS_COMPLETED,)
        assert rr.RUN_STATUS_BLOCKED_STALE not in rr.MEASURABLE_STATUSES

    def test_the_stored_summary_is_bounded(self):
        big = {"status": "completed",
               "funnel": {"per_symbol": [{"symbol": "S" * 50}] * 5000}}
        text = rr._bounded_summary(big)
        assert len(text) <= rr.MAX_SUMMARY_BYTES
        # And it says what it dropped rather than truncating into invalid JSON.
        import json
        json.loads(text)

    def test_a_counter_column_survives_the_shape_of_the_field_it_counts(self):
        # Regression, found live: `warmup.selected` is the LIST of symbols a
        # human reads, and the writer passed it to an INTEGER column. int([..])
        # raised, the caller swallowed it, and the run row sat at `running`
        # while its task reported success — the audit was simply lost.
        assert rr._count(["AAL", "ETHA", "SOXL"]) == 3
        assert rr._count(3) == 3
        assert rr._count(None) == 0
        # And the lifecycle now emits the scalar beside the list, so the audit
        # never has to infer a number from a field shaped for reading.
        source = _executable_source("app/research_lifecycle.py")
        assert "'warmups_attempted': len(warm['selected'])" in source

    def test_a_failed_audit_write_is_recorded_not_left_running(self):
        source = _executable_source("app/research_lifecycle.py")
        tail = source.split("could not persist research lifecycle run")[1]
        assert "fail_run" in tail
        assert "audit_write_failed" in tail

    def test_the_median_is_a_median_and_not_a_mean(self):
        # One run that spent twelve requests on a symbol the provider then
        # refused would drag a mean somewhere no run actually was.
        assert "percentile_cont(0.5)" in rr.MEDIAN_CALLS_SQL
        assert "avg(" not in rr.MEDIAN_CALLS_SQL

    def test_measurement_makes_no_predictive_claim(self):
        # The COLUMN LIST, not the prose — the prose says out loud that there is
        # no outcome column, and matching raw text would fail on a correct file.
        body = self.SQL.split("CREATE TABLE IF NOT EXISTS "
                              "public.research_lifecycle_runs")[1].split(");")[0]
        columns = [ln.strip().split()[0].lower()
                   for ln in body.splitlines()
                   if ln.strip() and not ln.strip().startswith(("-", "CONSTRAINT"))]
        for banned in ("forward_return", "pnl", "profit", "alpha", "outcome",
                       "label", "win_rate"):
            assert not any(banned in c for c in columns), banned


# =========================================================================== #
# P4 / P5 — dispatch, ownership, scheduling
# =========================================================================== #

class TestDispatchIdentity:
    def test_the_bounds_are_clamped_and_a_template_can_only_lower_them(self):
        wide = RL.task_payload_from_template(
            {"provider_budget": 9999, "warm_limit": 400, "admit_limit": 100},
            run_key="k")
        assert wide["provider_budget"] == RL.DEFAULT_PROVIDER_BUDGET
        assert wide["warm_limit"] == RL.DEFAULT_WARM_LIMIT
        assert wide["admit_limit"] == RL.DEFAULT_ADMIT_LIMIT
        narrow = RL.task_payload_from_template({"provider_budget": 2},
                                               run_key="k")
        assert narrow["provider_budget"] == 2

    def test_a_malformed_template_falls_back_rather_than_raising(self):
        payload = RL.task_payload_from_template({"provider_budget": "twelve"},
                                                run_key="k")
        assert payload["provider_budget"] == RL.DEFAULT_PROVIDER_BUDGET
        assert RL.task_payload_from_template("not json", run_key="k")["warm_limit"]

    def test_one_occurrence_is_one_run_key(self):
        a = RL.run_key_for_occurrence(
            schedule_code=RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE,
            schedule_version=1, occurrence_iso="2026-09-01T22:30:00+00:00")
        b = RL.run_key_for_occurrence(
            schedule_code=RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE,
            schedule_version=1, occurrence_iso="2026-09-01T22:30:00+00:00")
        c = RL.run_key_for_occurrence(
            schedule_code=RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE,
            schedule_version=1, occurrence_iso="2026-09-02T22:30:00+00:00")
        assert a == b and a != c

    def test_a_manual_run_can_never_collide_with_a_scheduled_occurrence(self):
        # Colliding would silently overwrite the scheduled run's audit row.
        manual = RL.manual_run_key(label="proof", now=NOW)
        assert manual.startswith("rlc:manual:")
        scheduled = RL.run_key_for_occurrence(
            schedule_code=RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE,
            schedule_version=1, occurrence_iso=NOW.isoformat())
        assert manual != scheduled
        assert not scheduled.startswith("rlc:manual:")

    def test_the_cli_and_the_scheduler_use_the_same_dispatcher(self):
        # The whole point of moving the lifecycle out of ops/. If these ever
        # diverge, "proven by hand" stops meaning "proven".
        cli = _executable_source("ops/analysis/research_lifecycle.py")
        sched = _executable_source("app/jobs/scheduler.py")
        assert "RL.enqueue_research_lifecycle" in cli
        assert "task_payload_from_template" in cli
        assert "RL.task_payload_from_template" in sched
        # and the CLI holds no lifecycle logic of its own
        assert "run_warmup" not in cli
        assert "evaluate_admissions" not in cli

    def test_two_attempts_not_three(self):
        assert RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS == 2
        from app.jobs.registry import resolve_handler
        spec = resolve_handler(RL.RESEARCH_LIFECYCLE_TASK)
        assert spec.max_attempts == 2
        assert spec.queue_name == RL.RESEARCH_LIFECYCLE_QUEUE


class TestScheduleOwnership:
    def test_an_unowned_schedule_behaves_exactly_as_before(self):
        from app.jobs.scheduler import _schedule_is_ownable
        assert _schedule_is_ownable({"payload_template": None})
        assert _schedule_is_ownable({"payload_template": {}})
        assert _schedule_is_ownable(
            {"payload_template": {"stages": ["catalyst_refresh.v1"]}})

    def test_an_owned_schedule_is_skipped_by_a_different_leader(self, monkeypatch):
        from app.config import settings
        from app.jobs.scheduler import _schedule_is_ownable
        owned = {"payload_template": {"scheduler_owner": "research_lifecycle"}}
        monkeypatch.setattr(settings, "JOB_SCHEDULER_OWNER", "", raising=False)
        monkeypatch.setattr(settings, "JOB_WORKER_TYPE", "pipeline_driver",
                            raising=False)
        assert not _schedule_is_ownable(owned)
        monkeypatch.setattr(settings, "JOB_SCHEDULER_OWNER",
                            "research_lifecycle", raising=False)
        assert _schedule_is_ownable(owned)

    def test_skipping_does_not_consume_the_occurrence(self):
        # A leader that is not the owner must leave next_run_at alone, or the
        # owning leader never sees the schedule as due.
        source = _executable_source("app/jobs/scheduler.py")
        tick = source.split("async def _tick_as_leader")[1]
        skip = tick.index("skipped_not_owned += 1")
        advance = tick.index("UPDATE job_schedules SET next_run_at")
        assert skip < advance
        assert "continue" in tick[skip:skip + 160]


class TestScheduleDeclaration:
    SQL = (MIGRATIONS / "029_research_lifecycle_runs.sql").read_text(encoding="utf-8")

    def test_the_schedule_is_created_disabled_and_paused(self):
        # Applying a migration must never start anything.
        assert "FALSE, TRUE," in self.SQL
        assert RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE in self.SQL

    def test_it_declares_its_own_owner_and_queue(self):
        assert "'scheduler_owner', 'research_lifecycle'" in self.SQL
        assert "'queue', 'research_lifecycle'" in self.SQL

    def test_the_timing_is_after_the_close_and_calendar_aware(self):
        assert "'market_daily', 'America/New_York', 150" in self.SQL
        from app.jobs.scheduler import next_market_daily_occurrence
        from zoneinfo import ZoneInfo
        fired = next_market_daily_occurrence(
            datetime(2026, 9, 1, 12, 0, tzinfo=UTC), 150)
        local = fired.astimezone(ZoneInfo("America/New_York"))
        assert (local.hour, local.minute) == (18, 30)

    def test_the_declared_budget_is_within_the_clamp(self):
        assert "'provider_budget', 12" in self.SQL
        assert RL.DEFAULT_PROVIDER_BUDGET == 12


# =========================================================================== #
# P6 / P7 — freshness gate and provider budget
# =========================================================================== #

class TestFreshnessGate:
    def test_stale_core_history_blocks_rather_than_degrades(self):
        source = _executable_source("app/research_lifecycle.py")
        gate = source.split("check_core_freshness(conn, target=target)")[1]
        blocked = gate.index("STATUS_BLOCKED_STALE")
        # The gate returns before anything downstream can run.
        assert "return summary" in gate[blocked:blocked + 1200]
        assert gate.index("return summary") < gate.index("evaluate_admissions")

    def test_both_core_universes_are_required(self):
        import app.research_lifecycle as svc
        assert svc.CORE_UNIVERSES == ("WYCKOFF-HISTORY-WARMUP-QUALIFICATION",
                                      "SMART-SCANNER-REFERENCE-MARKET-V1")

    def test_it_asks_for_the_existing_refresh_and_never_reimplements_one(self):
        source = _executable_source("app/research_lifecycle.py")
        assert "enqueue_history_incremental_refresh" in source
        # No second provider path, no direct bar write for the core cohort.
        assert "upsert_daily_bars" not in source
        assert "MassiveClient" not in source


class TestProviderBudget:
    def test_the_ceiling_is_enforced_not_hoped_for(self):
        import app.research_ingest as ri
        source = _executable_source("app/research_ingest.py")
        assert "summary['provider_requests'] >= max_requests" in source
        assert "summary['budget_exhausted'] = True" in source

    def test_the_discovery_cost_comes_out_of_the_same_budget(self):
        source = _executable_source("app/research_lifecycle.py")
        assert "remaining = max(0, int(provider_budget)" in source
        assert "max_requests=remaining" in source

    def test_exhaustion_is_stated_and_not_a_retry_storm(self):
        # A run that stopped early must SAY so — otherwise the missing symbols
        # read as "nothing left to do" and nobody resumes them.
        source = _executable_source("app/research_ingest.py")
        after = source.split("summary['budget_exhausted'] = True")[1]
        assert after.lstrip().startswith("break")

    def test_the_bounds_are_derived_from_the_measured_provider_limit(self):
        from app.config import settings
        assert settings.MASSIVE_REQUESTS_PER_MINUTE == 5
        assert ru.MAX_CONCURRENT_WARMUPS == 1
        assert ru.MAX_PROVIDER_REQUESTS_PER_RUN == 12


# =========================================================================== #
# P9 / isolation / licensing
# =========================================================================== #

class TestCandidateSemanticsPreserved:
    def test_scanned_is_still_not_candidate(self):
        assert ru.CANDIDATE_RESEARCH_CANDIDATE != ru.CANDIDATE_SCANNED_NOT_CANDIDATE
        assert rf.LIFECYCLE_SCANNED_NOT_CANDIDATE in rf.SCANNED_STATES
        assert rf.LIFECYCLE_RESEARCH_CANDIDATE in rf.SCANNED_STATES

    def test_there_is_no_score_anywhere_in_the_new_modules(self):
        for path in ("app/research_funnel.py", "app/research_runs.py",
                     "app/research_enrichment.py", "app/research_lifecycle.py"):
            source = _executable_source(path)
            assert "opportunity_score" not in source
            assert "def score(" not in source

    def test_a_candidate_is_always_a_scanned_symbol(self):
        summary = rf.summarise([_row("A", state=ru.STATE_RESEARCH_SCANNED,
                                     candidate=ru.CANDIDATE_RESEARCH_CANDIDATE)])
        names = {c["invariant"] for c in summary["conservation"]["checks"]}
        assert "candidates_are_scanned" in names


class TestIsolationAndLeastPrivilege:
    ROLE = pathlib.Path(
        "ops/sql/create_smart_scanner_research_lifecycle.sql").read_text(encoding="utf-8")
    RLS = pathlib.Path(
        "ops/sql/create_smart_scanner_research_lifecycle_rls_policies.sql"
    ).read_text(encoding="utf-8")

    def _grant_lines(self):
        return [ln for ln in self.ROLE.splitlines()
                if ln.strip().upper().startswith("GRANT")]

    def test_the_frozen_universe_membership_is_never_writable(self):
        for line in self._grant_lines():
            if "history_warmup_universe_symbols" in line:
                assert "INSERT" not in line and "UPDATE" not in line \
                    and "DELETE" not in line, line

    def test_the_canonical_experiment_is_not_granted_at_all(self):
        for relation in ("strategy_shadow_pairs", "strategy_shadow_run_pairs",
                         "strategy_shadow_runs", "strategy_shadow_evaluations",
                         "strategy_shadow_pair_outcomes",
                         "prospective_campaign_registrations",
                         "external_signals", "external_signal_deliveries"):
            assert not any(relation in ln for ln in self._grant_lines()), relation

    def test_strategy_parameters_are_read_only(self):
        for line in self._grant_lines():
            if "pattern_configs" in line or "public.patterns " in line:
                assert line.strip().startswith("GRANT SELECT ON"), line

    def test_daily_bar_writes_are_confined_to_research_symbols(self):
        # The single most important predicate: without it this role could
        # rewrite a frozen-universe bar, which is canonical evidence.
        assert ("WITH CHECK (symbol IN (SELECT symbol FROM "
                "public.research_symbols))") in self.RLS
        assert "_insert" in self.RLS and "daily_bars" in self.RLS

    def test_the_product_freshness_row_is_unwritable_by_research(self):
        assert "scope = ''research''" in self.RLS
        # exactly one named market-wide exception, and it is not a catalyst
        assert self.RLS.count("external_fmp_discovery") >= 2
        # The EXECUTABLE statements only: the comments name `sec_edgar` on
        # purpose, to say which write the predicate exists to refuse.
        statements = "\n".join(
            ln for ln in self.RLS.splitlines() if not ln.strip().startswith("--"))
        assert "sec_edgar" not in statements

    def test_the_queue_scope_is_exactly_two_queues(self):
        assert ("'(''research_lifecycle'',''history_incremental_refresh'')'"
                in self.RLS)

    def test_it_can_only_advance_the_schedule_it_owns(self):
        assert ("payload_template ->> ''scheduler_owner'' = "
                "''research_lifecycle''") in self.RLS

    def test_the_only_delete_is_the_confined_supersede(self):
        deletes = [ln for ln in self._grant_lines() if "DELETE" in ln]
        assert len(deletes) == 1
        assert "symbol_catalyst_events" in deletes[0]
        assert "FOR DELETE" in self.RLS

    def test_the_verification_script_proves_the_negatives(self):
        verify = pathlib.Path(
            "ops/sql/verify_smart_scanner_research_lifecycle.sql"
        ).read_text(encoding="utf-8")
        assert "SET ROLE smart_scanner_research_lifecycle" in verify
        for probe in ("history_warmup_universe_symbols",
                      "strategy_shadow_pairs", "'sec_edgar', 'ok', 'product'"):
            assert probe in verify


class TestLicensingBoundaryHolds:
    def test_the_new_research_tables_are_not_granted_to_the_product_reader(self):
        reader = pathlib.Path(
            "ops/sql/create_smart_scanner_product_reader.sql").read_text(encoding="utf-8")
        grants = [ln for ln in reader.splitlines()
                  if ln.strip().upper().startswith("GRANT SELECT ON")]
        for relation in ("research_symbols", "research_scan_results",
                         "research_lifecycle_runs",
                         "research_lifecycle_run_symbols",
                         "external_discovery_candidates",
                         "analyst_grade_events"):
            assert not any(relation in ln for ln in grants), relation

    def test_the_licensing_module_is_not_weakened(self):
        import app.source_licensing as lic
        assert lic.SOURCE_LICENSING["fmp"] == lic.LICENSING_INTERNAL_ONLY
        assert not lic.is_product_displayable(lic.LICENSING_UNKNOWN)
        for relation in ("external_discovery_candidates", "analyst_grade_events"):
            assert relation in lic.PRODUCT_FORBIDDEN_RELATIONS

    def test_the_run_audit_carries_no_restricted_provider_value(self):
        SQL = (MIGRATIONS / "029_research_lifecycle_runs.sql").read_text()
        for field in ("price", "quote", "change_percent", "market_cap"):
            assert field not in SQL.lower().replace("min_price", "")


class TestFlyAppBoundary:
    TOML = pathlib.Path("fly.research-lifecycle.toml").read_text(encoding="utf-8")

    def _directives(self):
        # The comments explain the absence on purpose; only real TOML counts.
        return "\n".join(ln for ln in self.TOML.splitlines()
                          if not ln.strip().startswith("#"))

    def test_it_is_not_internet_facing(self):
        directives = self._directives()
        assert "[http_service]" not in directives
        assert "[[services]]" not in directives

    def test_it_claims_only_its_own_queue(self):
        assert "JOB_WORKER_QUEUES = 'research_lifecycle'" in self.TOML
        assert "JOB_WORKER_CONCURRENCY = '1'" in self.TOML

    def test_it_connects_as_the_dedicated_role(self):
        assert ("JOB_WORKER_EXPECTED_DB_ROLE = "
                "'smart_scanner_research_lifecycle'") in self.TOML

    def test_its_scheduler_leadership_is_scoped_and_uses_its_own_lock(self):
        assert "JOB_SCHEDULER_OWNER = 'research_lifecycle'" in self.TOML
        from app.config import Settings
        assert ("JOB_SCHEDULER_ADVISORY_LOCK_KEY = '1381188424'" in self.TOML)
        # distinct from the existing global leader's key
        assert 1381188424 != Settings().JOB_SCHEDULER_ADVISORY_LOCK_KEY

    def test_no_secret_value_is_committed(self):
        for token in ("MASSIVE_API_KEY =", "FMP_API_KEY =",
                      "DATABASE_URL =", "postgres://"):
            assert token not in self.TOML
