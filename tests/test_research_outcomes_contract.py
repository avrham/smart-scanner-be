"""The outcome engine's boundaries, stated as tests rather than as prose.

Three kinds of claim live here, and none of them is about arithmetic:

  * SCHEDULING — the new schedule is declared disabled, owned by the right
    leader, timed after the lifecycle, and materialised into the right task.
  * ISOLATION — the frozen-25 canonical experiment is not reachable from any
    of this, and the P0-validated research lifecycle is not modified by it.
  * PROVENANCE — the ledger's vocabulary is the funnel's vocabulary, and the
    module makes no provider call.

They are checked against the real files, because a boundary that is only
described in a docstring is a boundary until somebody edits the file.
"""

from __future__ import annotations

import ast
import pathlib
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import app.jobs.research_lifecycle as RL
import app.jobs.research_outcomes as RO
import app.research_funnel as rf
import app.research_outcomes as ro

UTC = timezone.utc
MIGRATIONS = pathlib.Path("app/db/migrations")
MIGRATION_031 = (MIGRATIONS / "031_research_scan_outcomes.sql").read_text(
    encoding="utf-8")


# =========================================================================== #
# the schedule
# =========================================================================== #

class TestScheduleDeclaration:

    def test_the_schedule_is_created_disabled_and_paused(self):
        """Applying a migration must never start anything."""
        assert "FALSE, TRUE," in MIGRATION_031
        assert RO.RESEARCH_OUTCOMES_SCHEDULE_CODE in MIGRATION_031

    def test_it_declares_its_owner_and_the_lifecycle_queue(self):
        assert "'scheduler_owner', 'research_lifecycle'" in MIGRATION_031
        assert "'queue', 'research_lifecycle'" in MIGRATION_031
        assert RO.RESEARCH_OUTCOMES_QUEUE == RL.RESEARCH_LIFECYCLE_QUEUE

    def test_it_fires_after_the_lifecycle_has_fetched_the_day_s_bars(self):
        """11:00 ET the morning after the session — three hours after the
        lifecycle's live 08:00 ET start, which clears its bounded 3 x 30-minute
        deferral window and its measured 23-45 minute runtime.

        Compared PER SESSION. `next_market_daily_occurrence` takes an `after`
        instant, so two different delays evaluated from one `after` can land on
        two different sessions and the comparison would be meaningless.
        """
        assert "'market_daily', 'America/New_York', 1140" in MIGRATION_031
        from app.jobs.scheduler import next_market_daily_occurrence
        from app.prospective_session import session_cutoff_utc

        close = session_cutoff_utc(date(2026, 9, 1))
        outcome = close + timedelta(minutes=1140)
        lifecycle_live = close + timedelta(minutes=960)   # the LIVE staging row
        assert outcome - lifecycle_live == timedelta(hours=3)
        assert outcome.astimezone(ZoneInfo("America/New_York")).strftime(
            "%H:%M") == "11:00"

        # And the scheduler really does resolve it to that instant.
        fired = next_market_daily_occurrence(close - timedelta(hours=1), 1140)
        assert fired == outcome

    def test_the_declared_bounds_are_within_the_clamp(self):
        assert "'scan_limit', 200" in MIGRATION_031
        assert "'observation_limit', 400" in MIGRATION_031
        assert ro.DEFAULT_SCAN_LIMIT == 200
        assert ro.DEFAULT_OBSERVATION_LIMIT == 400

    def test_the_migration_declares_exactly_one_schedule(self):
        assert MIGRATION_031.count("INSERT INTO public.job_schedules") == 1


class TestSchedulerMaterialisation:

    def _schedule(self):
        return {"schedule_code": RO.RESEARCH_OUTCOMES_SCHEDULE_CODE,
                "schedule_version": 1,
                "job_type": RO.RESEARCH_OUTCOMES_JOB_TYPE,
                "payload_template": {"scheduler_owner": "research_lifecycle",
                                     "queue": "research_lifecycle",
                                     "scan_limit": 200,
                                     "observation_limit": 400}}

    def test_the_schedule_materialises_the_outcome_task(self):
        from app.jobs.scheduler import _research_outcomes_spec
        spec = _research_outcomes_spec(
            self._schedule(), datetime(2026, 9, 4, 23, 30, tzinfo=UTC))
        assert spec["task_type"] == RO.RESEARCH_OUTCOMES_TASK
        assert spec["queue"] == "research_lifecycle"
        assert spec["task_key"].startswith("roctask:roc:")
        assert spec["payload"]["scan_limit"] == 200
        assert spec["payload"]["include_settled"] is False

    def test_two_fires_of_one_occurrence_produce_one_task_key(self):
        from app.jobs.scheduler import _research_outcomes_spec
        occurrence = datetime(2026, 9, 4, 23, 30, tzinfo=UTC)
        first = _research_outcomes_spec(self._schedule(), occurrence)
        second = _research_outcomes_spec(self._schedule(), occurrence)
        assert first["task_key"] == second["task_key"]

    def test_the_resolvers_do_not_claim_each_other_s_schedules(self):
        """The chain in `_create_scheduled_job` is first-match-wins, so each
        resolver must answer None for a schedule that is not its own."""
        from app.jobs.scheduler import (_research_lifecycle_spec,
                                        _research_outcomes_spec)
        occurrence = datetime(2026, 9, 4, 23, 30, tzinfo=UTC)
        assert _research_lifecycle_spec(self._schedule(), occurrence) is None
        lifecycle = {"schedule_code": RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE,
                     "schedule_version": 1,
                     "job_type": RL.RESEARCH_LIFECYCLE_JOB_TYPE,
                     "payload_template": {}}
        assert _research_outcomes_spec(lifecycle, occurrence) is None

    def test_the_research_leader_owns_it_and_nobody_else_does(self):
        from app.config import settings
        from app.jobs.scheduler import _schedule_is_ownable
        sched = self._schedule()
        original = settings.JOB_SCHEDULER_OWNER
        try:
            settings.JOB_SCHEDULER_OWNER = "research_lifecycle"
            assert _schedule_is_ownable(sched)
            settings.JOB_SCHEDULER_OWNER = ""
            # The GENERAL leader (the pipeline driver) must NOT take it: its
            # role cannot write a research table, so a task it created would
            # be a task it could never carry out.
            assert not _schedule_is_ownable(sched)
        finally:
            settings.JOB_SCHEDULER_OWNER = original


class TestHandlerRegistration:

    def test_the_handler_is_registered_and_production_enabled(self):
        from app.jobs import registry as R
        spec = R.resolve_handler(RO.RESEARCH_OUTCOMES_TASK)
        assert spec.production_enabled is True
        assert spec.is_test_handler is False
        assert spec.queue_name == "research_lifecycle"
        assert spec.max_attempts == RO.RESEARCH_OUTCOMES_MAX_ATTEMPTS
        assert spec.probe_fn is not None

    def test_its_retry_budget_is_short_unlike_the_lifecycle_s(self):
        """The lifecycle defers for half an hour at a time because it is
        waiting out a refresh it asked for. This run has nothing to outlast."""
        assert len(RO.RESEARCH_OUTCOMES_BACKOFF_SECONDS) == \
            RO.RESEARCH_OUTCOMES_MAX_ATTEMPTS - 1
        assert max(RO.RESEARCH_OUTCOMES_BACKOFF_SECONDS) < \
            min(RL.RESEARCH_LIFECYCLE_BACKOFF_SECONDS)

    def test_the_lifecycle_handler_is_untouched(self):
        from app.jobs import registry as R
        spec = R.resolve_handler(RL.RESEARCH_LIFECYCLE_TASK)
        assert spec.max_attempts == RL.RESEARCH_LIFECYCLE_MAX_ATTEMPTS == 4
        assert list(spec.retry_backoff_schedule) == [1800, 1800, 1800]


# =========================================================================== #
# isolation
# =========================================================================== #

CANONICAL_RELATIONS = ("strategy_shadow_pairs", "strategy_shadow_run_pairs",
                       "strategy_shadow_runs", "strategy_shadow_evaluations",
                       "strategy_shadow_pair_outcomes",
                       "strategy_shadow_outcome_runs",
                       "prospective_campaign_registrations")

NEW_SOURCES = ("app/research_outcomes.py", "app/jobs/research_outcomes.py",
               "app/jobs/handlers/research_outcomes_worker.py")


class TestCanonicalIsolation:

    def test_no_new_module_names_a_canonical_relation(self):
        for path in NEW_SOURCES:
            source = pathlib.Path(path).read_text(encoding="utf-8")
            # Strip the docstrings/comments: the module headers legitimately
            # EXPLAIN what they do not touch, and that must not read as a use.
            code = _executable_source(path)
            for relation in CANONICAL_RELATIONS:
                assert relation not in code, f"{path} references {relation}"
            assert "prospective_campaign_registrations" not in code
            del source

    def test_the_migration_creates_no_reference_to_the_frozen_experiment(self):
        statements = MIGRATION_031.upper()
        for relation in CANONICAL_RELATIONS:
            assert f"REFERENCES PUBLIC.{relation.upper()}" not in statements
        # The only foreign key is to the research scan itself.
        assert MIGRATION_031.count("REFERENCES public.") == 1
        assert "REFERENCES public.research_scan_results(id)" in MIGRATION_031

    def test_the_migration_alters_nothing_that_already_existed(self):
        """Additive only. No ALTER, no DROP TABLE, no DELETE, no UPDATE of an
        existing row — the one DROP is of this migration's own trigger, so
        re-applying it converges."""
        upper = MIGRATION_031.upper()
        assert "ALTER TABLE PUBLIC.RESEARCH_SCAN_RESULTS" not in upper
        assert "ALTER TABLE PUBLIC.RESEARCH_SYMBOLS" not in upper
        assert "ALTER TABLE PUBLIC.DAILY_BARS" not in upper
        assert "DROP TABLE" not in upper
        assert "DELETE FROM" not in upper
        # As a STATEMENT. `truncated_by_limit` is a column on the run row, and
        # a substring match would read it as one.
        assert not [ln for ln in MIGRATION_031.splitlines()
                    if re.match(r"TRUNCATE\b", ln.strip().upper())]
        assert "UPDATE PUBLIC.JOB_SCHEDULES" not in upper
        # ENABLE ROW LEVEL SECURITY on the two NEW tables is the only ALTER.
        alters = [ln for ln in MIGRATION_031.splitlines()
                  if ln.strip().upper().startswith("ALTER TABLE")]
        assert len(alters) == 2
        assert all("ENABLE ROW LEVEL SECURITY" in ln for ln in alters)

    def test_the_role_gains_the_two_new_tables_and_no_delete(self):
        role = pathlib.Path(
            "ops/sql/create_smart_scanner_research_lifecycle.sql").read_text(
                encoding="utf-8")
        grants = [ln for ln in role.splitlines()
                  if ln.strip().upper().startswith("GRANT")]
        for relation in ("research_scan_outcomes", "research_outcome_runs"):
            lines = [ln for ln in grants if relation in ln]
            assert lines, relation
            for line in lines:
                assert "DELETE" not in line, line
        # And the canonical experiment is still granted nothing at all.
        for relation in CANONICAL_RELATIONS:
            assert not any(relation in ln for ln in grants), relation

    def test_the_product_reader_is_granted_nothing_here(self):
        """The licence boundary is an OMISSION, exactly as in 026 and 029."""
        assert "smart_scanner_product_reader" not in MIGRATION_031
        assert "internal_research_only" in MIGRATION_031


class TestP0LifecycleUntouched:

    def test_the_lifecycle_schedule_row_is_not_rewritten(self):
        assert "SMART-SCANNER-RESEARCH-LIFECYCLE" not in MIGRATION_031
        assert "SMART-SCANNER-DAILY-PIPELINE" not in MIGRATION_031

    def test_the_funnel_state_machine_is_unchanged(self):
        """The extraction of `scan_classification` was a refactor; the states
        and their meanings are exactly what P0 left."""
        assert rf.LIFECYCLE_STATES == (
            "admission_pending", "admission_rejected", "history_pending",
            "history_warming", "history_unavailable", "history_failed",
            "scan_pending", "scan_stale", "classification_pending",
            "scanned_not_candidate", "research_candidate")

    @pytest.mark.parametrize("row,expected", [
        ({"admission_state": None}, "admission_pending"),
        ({"admission_state": "rejected_before_history"}, "admission_rejected"),
        ({"admission_state": "eligible_for_history", "state": "research_ready",
          "has_current_scan": False, "has_any_scan": True}, "scan_stale"),
        ({"admission_state": "eligible_for_history", "state": "research_ready",
          "has_current_scan": False, "has_any_scan": False}, "scan_pending"),
        ({"admission_state": "eligible_for_history",
          "state": "research_scanned", "has_current_scan": True},
         "classification_pending"),
        ({"admission_state": "eligible_for_history",
          "state": "research_scanned", "has_current_scan": True,
          "rejection_reason": "price_below_minimum"}, "scanned_not_candidate"),
        ({"admission_state": "eligible_for_history",
          "state": "research_scanned", "has_current_scan": True,
          "structure_state": "unknown", "setup_state": "unknown",
          "benchmark_relative": "underperforming"}, "scanned_not_candidate"),
        ({"admission_state": "eligible_for_history",
          "state": "research_scanned", "has_current_scan": True,
          "structure_state": "recognized", "setup_state": "valid",
          "benchmark_relative": "outperforming"}, "research_candidate"),
    ])
    def test_lifecycle_state_still_answers_exactly_as_before(self, row, expected):
        assert rf.lifecycle_state(row) == expected

    def test_scan_classification_and_lifecycle_state_cannot_disagree(self):
        """One definition. If these ever diverge, the funnel and the ledger
        would classify the same scan differently."""
        for evidence in (
                {"rejection_reason": "price_below_minimum"},
                {"structure_state": "recognized", "setup_state": "valid"},
                {"structure_state": "unknown", "setup_state": "invalid",
                 "benchmark_relative": "underperforming"},
                {"benchmark_relative": "outperforming"},
                {}):
            row = {"admission_state": "eligible_for_history",
                   "state": "research_scanned", "has_current_scan": True,
                   **evidence}
            assert rf.lifecycle_state(row) == rf.scan_classification(row)

    def test_the_calendar_resolver_still_answers_as_before(self):
        """`nth_trading_session_after` was ADDED to prospective_session; the
        function the whole project already depends on must be untouched."""
        from app.prospective_session import resolve_latest_completed_session
        # After Friday's close.
        assert resolve_latest_completed_session(
            datetime(2026, 9, 4, 21, tzinfo=UTC)) == __import__(
                "datetime").date(2026, 9, 4)
        # During Friday's session — Thursday is the latest COMPLETED one.
        assert resolve_latest_completed_session(
            datetime(2026, 9, 4, 15, tzinfo=UTC)) == __import__(
                "datetime").date(2026, 9, 3)


# =========================================================================== #
# provenance
# =========================================================================== #

def _executable_source(path: str) -> str:
    """The module's code with every docstring removed.

    A module header that explains what it refuses to touch must not be
    mistaken for touching it — and equally, a test that greps raw source can
    be satisfied by a comment. Both directions are wrong, so the comparison is
    made against the code that actually runs.
    """
    tree = ast.parse(pathlib.Path(path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)) and ast.get_docstring(node):
            node.body = node.body[1:]
    return ast.unparse(tree)


class TestProvenance:

    def test_the_engine_makes_no_provider_call(self):
        """Forward bars come from the local store or the outcome is not
        measured. A measurement that could fetch its own data could fetch the
        WRONG data, later, silently."""
        code = _executable_source("app/research_outcomes.py")
        for forbidden in ("provider", "massive", "fmp", "httpx", "requests",
                          "aiohttp", "MASSIVE_API_KEY", "FMP_API_KEY"):
            assert forbidden.lower() not in code.lower(), forbidden

    def test_the_only_table_read_for_prices_is_daily_bars(self):
        code = _executable_source("app/research_outcomes.py")
        assert "public.daily_bars" in code
        assert "market_bars_4h" not in code
        assert "bars_4h" not in code

    def test_the_horizon_is_never_derived_from_a_bar_count(self):
        """The exit bar is looked up by DATE. A tolerant `<=` lookup is how a
        5D silently becomes a six-session measurement."""
        code = _executable_source("app/research_outcomes.py")
        assert "trading_date = $2" in code
        assert "ORDER BY trading_date DESC LIMIT" not in code

    def test_the_versions_are_the_shared_ones(self):
        from app.prospective_session import MARKET_CALENDAR_VERSION
        from app.workers.outcomes.calculator import CALCULATION_VERSION
        assert CALCULATION_VERSION == "outcome.v1"
        assert MARKET_CALENDAR_VERSION == "us_market_calendar.v1"
        assert ro.RESEARCH_OUTCOME_CONTRACT_VERSION == "research_scan_outcome.v1"

    def test_the_horizons_match_the_canonical_windows(self):
        from app.workers.outcomes.calculator import HOLDING_WINDOWS
        assert list(ro.HORIZONS) == HOLDING_WINDOWS == [1, 3, 5, 10, 20]

    def test_the_benchmark_is_the_designated_broad_reference(self):
        from app.reference_market import PRIMARY_BENCHMARK, is_reference_symbol
        assert PRIMARY_BENCHMARK == "SPY"
        assert is_reference_symbol("SPY")

    def test_every_status_is_enumerated_in_the_schema(self):
        for status in ro.OUTCOME_STATUSES:
            assert f"'{status}'" in MIGRATION_031

    def test_every_reason_code_is_bounded_and_secret_free(self):
        codes = [ro.REASON_HORIZON_NOT_COMPLETE, ro.REASON_MISSING_SYMBOL_ENTRY,
                 ro.REASON_MISSING_SYMBOL_EXIT,
                 ro.REASON_MISSING_BENCHMARK_ENTRY,
                 ro.REASON_MISSING_BENCHMARK_EXIT,
                 ro.REASON_NON_POSITIVE_PRICE, ro.REASON_GRACE_EXCEEDED,
                 ro.REASON_MEASURED]
        for code in codes:
            assert code.replace("_", "").isalnum()
            assert len(code) <= 60
        assert len(set(codes)) == len(codes)


class TestOperatorSurface:

    def test_the_cli_dispatches_through_the_canonical_enqueue(self):
        """No hand-written outcome row: the backfill takes the same three
        steps the schedule takes."""
        code = _executable_source("ops/analysis/research_outcomes.py")
        assert "enqueue_research_outcomes" in code
        assert "execute_research_outcomes" in code
        assert "INSERT INTO public.research_scan_outcomes" not in code
        assert "UPDATE public.research_scan_outcomes" not in code

    def test_the_cli_connects_as_the_research_identity(self):
        code = _executable_source("ops/analysis/research_outcomes.py")
        assert "research_connection" in code
        assert "intel_connection()" not in code
