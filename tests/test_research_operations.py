"""Research operations: cheap rejection, honest candidates, canonical config.

Three findings from the first live cohort drive this file, and each has a test
that would have caught it:

  * history was bought for three symbols that all failed the SAME hard gate;
  * those three were then reported as "worth a human look";
  * the research scan's config matched the experiment's by ACCIDENT, because
    the read failed and both fell back to the same defaults.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

import app.research_admission as ra
import app.research_ingest as ri
import app.research_universe as ru

UTC = timezone.utc
NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
REFERENCE = date(2026, 8, 28)


# =========================================================================== #
# P0 — the canonical configuration dependency
# =========================================================================== #

class TestCanonicalConfig:
    def test_min_price_is_read_from_the_canonical_config_not_hardcoded(self):
        # 5.0 is the strategy's default. Admission must READ it, so that an
        # operator changing pattern_configs changes both at once.
        from app.workers.strategies.wyckoff_v2.constants import (
            DEFAULT_CONFIG as WYCKOFF_DEFAULTS)
        assert WYCKOFF_DEFAULTS["min_price"] == 5.0
        assert ra.resolve_min_price(WYCKOFF_DEFAULTS) == 5.0
        # A different canonical value produces a different gate — no constant
        # of this module's own can be involved.
        assert ra.resolve_min_price({"min_price": 12.5}) == 12.5

    def test_an_unusable_minimum_disables_rejection_rather_than_guessing(self):
        for bad in ({}, {"min_price": None}, {"min_price": "x"},
                    {"min_price": 0}, {"min_price": -3}):
            assert ra.resolve_min_price(bad) is None
        verdict = ra.evaluate_admission(price=0.10, price_source="local_daily_bars",
                                        min_price=None)
        assert verdict["state"] == ra.ADMISSION_UNKNOWN
        assert ra.admits_history(verdict)

    def test_the_resolver_accepts_a_connection_and_can_fail_closed(self):
        import inspect
        from app.workers.patterns.config import resolve_pattern_config
        params = inspect.signature(resolve_pattern_config).parameters
        assert "conn" in params and "require_db" in params

    def test_a_config_read_failure_raises_instead_of_using_defaults(self):
        from app.workers.patterns.config import (ConfigUnavailable,
                                                 resolve_pattern_config)

        class Boom:
            async def fetch(self, *a, **k):
                raise RuntimeError("unreachable database")

        from app.workers.patterns.config import bound_config_connection

        async def go():
            with bound_config_connection(Boom(), require_db=True):
                return await resolve_pattern_config(
                    "wyckoff_mtf_v2", {"min_price": 5.0})

        with pytest.raises(ConfigUnavailable):
            asyncio.run(go())

    def test_a_CHANGED_canonical_config_propagates_to_the_research_scan(self):
        # The regression the mission asked for: if the canonical config
        # changes, research must resolve the CHANGED value, not safe defaults.
        from app.workers.patterns.config import resolve_pattern_config

        class Configured:
            async def fetch(self, *a, **k):
                return [{"key": "min_price", "value": "42.5"}]

        resolved = asyncio.run(resolve_pattern_config(
            "wyckoff_mtf_v2", {"min_price": 5.0, "other": 1},
            conn=Configured(), require_db=True))
        assert resolved["min_price"] == 42.5      # the DB value, not 5.0
        assert resolved["other"] == 1             # defaults still merged under
        assert ra.resolve_min_price(resolved) == 42.5

    def test_the_research_scan_binds_config_to_its_own_connection(self):
        source = open("app/research_scan.py", encoding="utf-8").read()
        assert "bound_config_connection(conn, require_db=True)" in source
        # BOTH arms are resolved inside the binding, not just the candidate.
        block = source.split("with bound_config_connection")[1]
        assert block.count("_resolve_arm") >= 2

    def test_the_canonical_execution_layer_is_left_untouched(self):
        # The parameter version of this fix would have had to thread `conn`
        # through `_resolve_arm`, which several phase-boundary tests assert is
        # unmodified — and rightly so. A scoped context variable keeps the
        # change inside the resolver.
        import subprocess
        diff = subprocess.run(
            ["git", "diff", "--", "app/workers/shadow/runner.py",
             "app/workers/persistence.py"],
            capture_output=True, text=True, check=False).stdout
        assert diff.strip() == ""

    def test_the_binding_is_scoped_and_restores_itself(self):
        from app.workers.patterns.config import (_BOUND_CONNECTION,
                                                 bound_config_connection)
        sentinel = object()
        assert _BOUND_CONNECTION.get() is None
        with bound_config_connection(sentinel, require_db=True):
            assert _BOUND_CONNECTION.get() is sentinel
        assert _BOUND_CONNECTION.get() is None


# =========================================================================== #
# P1 — admission, BEFORE any provider request
# =========================================================================== #

class TestAdmission:
    def test_price_below_minimum_is_rejected_before_warmup(self):
        v = ra.evaluate_admission(price=0.1814, price_source=ra.PRICE_SOURCE_DISCOVERY,
                                  min_price=5.0)
        assert v["state"] == ra.ADMISSION_REJECTED
        assert v["reason"] == ra.REASON_PRICE_BELOW_MINIMUM
        assert v["provider_request_avoided"] is True
        assert not ra.admits_history(v)

    def test_price_exactly_at_the_threshold_is_admitted(self):
        # The strategy's gate is `price < min_price`; admission must not be
        # stricter than the rule it anticipates.
        v = ra.evaluate_admission(price=5.0, price_source=ra.PRICE_SOURCE_LOCAL_BARS,
                                  min_price=5.0)
        assert v["state"] == ra.ADMISSION_ELIGIBLE
        assert ra.admits_history(v)

    def test_price_above_the_threshold_is_admitted(self):
        v = ra.evaluate_admission(price=217.55, price_source=ra.PRICE_SOURCE_LOCAL_BARS,
                                  min_price=5.0)
        assert v["state"] == ra.ADMISSION_ELIGIBLE

    def test_an_unknown_price_PROCEEDS_rather_than_being_rejected(self):
        # Rejecting on absent evidence would silently filter out exactly the
        # symbols our data is weakest on.
        v = ra.evaluate_admission(price=None, price_source=None, min_price=5.0)
        assert v["state"] == ra.ADMISSION_UNKNOWN
        assert v["reason"] == ra.REASON_NO_PRICE
        assert ra.admits_history(v)
        assert v["provider_request_avoided"] is False

    def test_the_three_live_rejections_would_all_have_been_caught(self):
        # CELU / NVD / PPCB, at the prices the discovery snapshot held.
        for symbol, price in (("CELU", 0.1814), ("NVD", 1.02), ("PPCB", 0.398)):
            v = ra.evaluate_admission(price=price,
                                      price_source=ra.PRICE_SOURCE_DISCOVERY,
                                      min_price=5.0)
            assert v["state"] == ra.ADMISSION_REJECTED, symbol

    def test_every_state_and_reason_is_in_the_declared_vocabulary(self):
        for price in (None, 0.5, 5.0, 500.0):
            v = ra.evaluate_admission(price=price, price_source=None,
                                      min_price=5.0)
            assert v["state"] in ra.ADMISSION_STATES
            assert v["reason"] in ra.ADMISSION_REASONS

    def test_no_score_is_produced(self):
        v = ra.evaluate_admission(price=9.0, price_source=ra.PRICE_SOURCE_LOCAL_BARS,
                                  min_price=5.0)
        for banned in ("score", "rank", "weight", "confidence", "quality"):
            assert banned not in v


class TestAdmissionLicensing:
    def test_the_price_source_is_recorded_because_it_changes_the_licence(self):
        restricted = ra.evaluate_admission(price=1.0, min_price=5.0,
                                           price_source=ra.PRICE_SOURCE_DISCOVERY)
        clean = ra.evaluate_admission(price=1.0, min_price=5.0,
                                      price_source=ra.PRICE_SOURCE_LOCAL_BARS)
        assert restricted["restricted_source"] is True
        assert clean["restricted_source"] is False

    def test_only_the_fmp_snapshot_is_treated_as_restricted(self):
        assert ra.RESTRICTED_PRICE_SOURCES == (ra.PRICE_SOURCE_DISCOVERY,)
        assert not ra.is_restricted_source(ra.PRICE_SOURCE_LOCAL_BARS)

    def test_local_bars_are_preferred_over_the_restricted_snapshot(self):
        # Cheapest AND least restricted first.
        sql = ri.ADMISSION_PRICE_SQL
        assert sql.index("local_close") < sql.index("discovery_price")
        source = open("app/research_ingest.py", encoding="utf-8").read()
        assert 'if row["local_close"] is not None' in source

    def test_neither_price_source_costs_a_provider_request(self):
        # Both come from rows we already hold — that is the entire point.
        assert "daily_bars" in ri.ADMISSION_PRICE_SQL
        assert "external_discovery_candidates" in ri.ADMISSION_PRICE_SQL
        source = open("app/research_admission.py", encoding="utf-8").read()
        assert "get_daily_bars" not in source
        assert "provider" not in source.split('"""')[2]

    def test_the_licensing_module_is_not_weakened(self):
        import app.source_licensing as lic
        assert lic.SOURCE_LICENSING["fmp"] == lic.LICENSING_INTERNAL_ONLY
        assert not lic.is_product_displayable(lic.LICENSING_UNKNOWN)


class TestAdmissionBlocksWarmup:
    def test_a_rejected_symbol_is_excluded_from_the_warmup_batch(self):
        assert "rejected_before_history" not in ri.WARMUP_SELECT_SQL
        assert "eligible_for_history" in ri.WARMUP_SELECT_SQL
        assert "insufficient_admission_data" in ri.WARMUP_SELECT_SQL

    def test_admission_is_a_hard_filter_that_runs_before_priority(self):
        # A $1 stock on three mover lists must not outrank a valid $20 one:
        # priority orders the SURVIVORS, it never readmits a rejection.
        # The SQL literal itself, not the code that follows it.
        assert "admission_state" in ri.WARMUP_SELECT_SQL
        assert ri.WARMUP_SELECT_SQL.index("admission_state") > \
            ri.WARMUP_SELECT_SQL.index("WHERE")
        source = open("app/research_ingest.py", encoding="utf-8").read()
        # and the ordering is applied to what the SQL already filtered
        assert "ru.prioritise(eligible" in source


class FakeAdmissionConn:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []

    async def fetch(self, sql, *args):
        return self.rows

    async def execute(self, sql, *args):
        self.updates.append(args)
        return "UPDATE 1"


def _price_row(symbol, *, local=None, discovery=None):
    return {"symbol": symbol, "local_close": local,
            "local_session": date(2026, 8, 28) if local is not None else None,
            "discovery_price": discovery,
            "discovery_session": REFERENCE if discovery is not None else None}


class TestAdmissionEvaluation:
    def test_a_rejected_symbol_costs_zero_provider_requests(self):
        conn = FakeAdmissionConn([_price_row("CELU", discovery=0.18),
                                  _price_row("BIGCO", local=210.0)])
        summary = asyncio.run(ri.evaluate_admissions(conn, min_price=5.0,
                                                     now=NOW))
        assert summary["states"][ra.ADMISSION_REJECTED] == 1
        assert summary["states"][ra.ADMISSION_ELIGIBLE] == 1
        assert summary["provider_requests_avoided"] == 1
        assert [r["symbol"] for r in summary["rejected_before_history"]] == ["CELU"]

    def test_restricted_source_decisions_are_counted(self):
        conn = FakeAdmissionConn([_price_row("A", discovery=0.5),
                                  _price_row("B", local=9.0)])
        summary = asyncio.run(ri.evaluate_admissions(conn, min_price=5.0,
                                                     now=NOW))
        assert summary["decisions_on_restricted_source"] == 1

    def test_the_decision_and_its_provenance_are_both_persisted(self):
        conn = FakeAdmissionConn([_price_row("CELU", discovery=0.18)])
        asyncio.run(ri.evaluate_admissions(conn, min_price=5.0, now=NOW))
        args = conn.updates[0]
        # symbol, state, reason, price, source, reference_session, min, at
        assert args[0] == "CELU"
        assert args[1] == ra.ADMISSION_REJECTED
        assert args[2] == ra.REASON_PRICE_BELOW_MINIMUM
        assert args[4] == ra.PRICE_SOURCE_DISCOVERY
        assert args[5] == REFERENCE          # the MARKET session it describes
        assert args[6] == 5.0                # the minimum in force, stored


# =========================================================================== #
# P2 — scanned is not the same as worth a look
# =========================================================================== #

def _scanned(**kw):
    base = {"state": ru.STATE_RESEARCH_SCANNED, "rejection_reason": None,
            "structure_state": None, "setup_state": None,
            "benchmark_relative": None, "discovery_reasons": [],
            "discovery_observation_count": 1,
            "latest_reference_session": REFERENCE}
    base.update(kw)
    return base


class TestCandidateSemantics:
    def test_a_hard_AVOID_is_scanned_but_NOT_a_candidate(self):
        # THE bug: three symbols with AVOID / price_below_minimum were called
        # "worth a human look" because their discovery reasons were strong.
        row = _scanned(rejection_reason="price_below_minimum",
                       discovery_reasons=["most_active", "top_gainers",
                                          "top_losers"],
                       discovery_observation_count=3)
        verdict = ru.classify_candidate(row)
        assert verdict["candidate_state"] == ru.CANDIDATE_SCANNED_NOT_CANDIDATE
        assert verdict["reason"] == "price_below_minimum"
        assert not ru.is_research_candidate(row)

    def test_discovery_strength_ALONE_can_never_produce_a_candidate(self):
        for count in (1, 3, 9):
            row = _scanned(rejection_reason="price_below_minimum",
                           discovery_reasons=["a"] * count,
                           discovery_observation_count=count)
            assert not ru.is_research_candidate(row)

    def test_a_survivor_with_evidence_is_a_candidate(self):
        row = _scanned(structure_state="accumulation",
                       setup_state="setup_confirmed",
                       benchmark_relative="outperforming")
        verdict = ru.classify_candidate(row)
        assert verdict["candidate_state"] == ru.CANDIDATE_RESEARCH_CANDIDATE
        assert verdict["reason"] is None
        assert set(verdict["screen"]) == {ru.SCREEN_STRUCTURE_PRESENT,
                                          ru.SCREEN_SETUP_PRESENT,
                                          ru.SCREEN_BENCHMARK_LEADING}

    def test_a_survivor_with_nothing_to_read_is_not_a_candidate(self):
        row = _scanned(structure_state="none", setup_state="absent")
        verdict = ru.classify_candidate(row)
        assert verdict["candidate_state"] == ru.CANDIDATE_SCANNED_NOT_CANDIDATE
        assert verdict["reason"] == ru.SCREEN_NO_EVIDENCE

    def test_a_hard_disqualifier_is_reported_ALONE(self):
        # Listing "structure present" beside "rejected on price" would invite
        # weighing one against the other.
        row = _scanned(rejection_reason="price_below_minimum",
                       structure_state="accumulation",
                       setup_state="setup_confirmed")
        assert ru.screen_findings(row) == [ru.SCREEN_HARD_DISQUALIFIED]

    def test_an_unscanned_symbol_is_insufficient_data(self):
        row = _scanned(state=ru.STATE_HISTORY_WARMING)
        assert ru.classify_candidate(row)["candidate_state"] \
            == ru.CANDIDATE_INSUFFICIENT_DATA

    def test_terminal_states_are_unavailable(self):
        for state in ru.TERMINAL_STATES:
            assert ru.classify_candidate(_scanned(state=state))["candidate_state"] \
                == ru.CANDIDATE_UNAVAILABLE

    def test_candidate_status_is_not_ENTER_or_WATCH(self):
        # A candidate is "the screen did not disqualify it", never a verdict.
        row = _scanned(structure_state="accumulation")
        assert ru.classify_candidate(row)["candidate_state"] \
            == ru.CANDIDATE_RESEARCH_CANDIDATE
        assert "ENTER" not in str(ru.CANDIDATE_STATES)
        assert "WATCH" not in str(ru.CANDIDATE_STATES)

    def test_the_two_halves_never_share_a_vocabulary(self):
        # WHY WE LOOKED and WHETHER IT SURVIVED must not be confusable.
        assert not set(ru.LOOKED_REASONS) & set(ru.SCREEN_REASONS)

    def test_why_we_looked_is_reported_even_for_a_rejected_symbol(self):
        row = _scanned(rejection_reason="price_below_minimum",
                       discovery_reasons=["most_active", "top_gainers"],
                       discovery_observation_count=2)
        looked = ru.looked_because(row, latest_reference_session=REFERENCE)
        assert ru.LOOKED_MULTIPLE_LISTS in looked
        assert ru.LOOKED_REPEATEDLY in looked
        # …and it still is not a candidate.
        assert not ru.is_research_candidate(row)

    def test_classification_is_deterministic(self):
        row = _scanned(structure_state="accumulation", setup_state="setup_forming")
        assert ru.classify_candidate(row) == ru.classify_candidate(dict(row))


# =========================================================================== #
# P3 — provider-history exhaustion reconciliation
# =========================================================================== #

class TestExhaustionReconciliation:
    SQL = open("app/db/migrations/027_research_admission.sql",
               encoding="utf-8").read()

    def test_the_reconciliation_is_evidence_based_not_symbol_based(self):
        # The executable predicate, not the prose. The comment names LGPS as
        # the case that motivated this; the SQL must not.
        block = self.SQL.split("UPDATE public.research_symbols r")[1]
        executable = "\n".join(ln for ln in block.splitlines()
                                if not ln.strip().startswith("--"))
        assert "LGPS" not in executable
        assert "state = 'failed'" in block
        assert "history_daily_bars > 0" in block
        assert "count(DISTINCT date_trunc('month'" in block

    def test_it_does_not_erase_the_audit_trail(self):
        block = self.SQL.split("UPDATE public.research_symbols r")[1]
        assert "warmup_attempts" not in block.split("WHERE")[0]
        assert "warmup_last_attempt_at" not in block.split("WHERE")[0]

    def test_it_marks_terminal_so_no_retry_is_attempted(self):
        block = self.SQL.split("UPDATE public.research_symbols r")[1]
        assert "warmup_last_error_class = 'terminal'" in block
        assert "state = 'unavailable'" in block

    def test_a_terminal_class_classifies_as_unavailable(self):
        assert ru.classify_history_state(
            daily_bars=359, attempts=3,
            last_error_class="terminal") == ru.STATE_UNAVAILABLE


# =========================================================================== #
# P4 — the lifecycle, and its gates
# =========================================================================== #

class TestLifecycle:
    """The lifecycle moved from ops/ to app/ so the CLI and the durable task
    handler are the SAME code path; the ordering guarantees this milestone
    established are re-asserted at the new location."""

    SOURCE = open("app/research_lifecycle.py", encoding="utf-8").read()

    def test_stale_core_history_BLOCKS_rather_than_continuing(self):
        import app.research_lifecycle as rl
        assert rl.STATUS_BLOCKED_STALE == "blocked_stale_core_history"
        gate = self.SOURCE.split('if not freshness["fresh"]:')[1]
        assert "return summary" in gate.split("# ---- 2.")[0]

    def test_an_unresolvable_canonical_config_blocks_the_run(self):
        import app.research_lifecycle as rl
        assert rl.STATUS_BLOCKED_CONFIG == "blocked_canonical_config_unavailable"

    def test_the_freshness_gate_covers_both_core_universes(self):
        import app.research_lifecycle as rl
        assert set(rl.CORE_UNIVERSES) == {
            "WYCKOFF-HISTORY-WARMUP-QUALIFICATION",
            "SMART-SCANNER-REFERENCE-MARKET-V1"}

    def test_admission_runs_before_warmup_in_the_lifecycle(self):
        assert self.SOURCE.index("evaluate_admissions") < \
            self.SOURCE.index("_run_warmup(conn")

    def test_a_dry_run_touches_no_provider(self):
        body = self.SOURCE.split("        if dry_run:")[1]
        assert "return summary" in body.split("# ---- 6.")[0]

    def test_enrichment_is_lazy_capped_and_survivor_only(self):
        # Still lazy, still capped, still survivors only — but it now FETCHES,
        # because migration 028 gave the freshness row a cohort. See
        # tests/test_research_lifecycle_automation.py::TestLazyEnrichment.
        import app.research_enrichment as re_
        assert re_.MAX_ENRICHED_SYMBOLS == 10
        assert "candidate_state = $1" in re_.CANDIDATE_SQL

    def test_enrichment_is_confined_to_the_research_cohort(self):
        # This replaces the previous milestone's "fetches nothing and says why".
        # The reason it fetched nothing was that `refresh_sec_filings` wrote the
        # SHARED `sec_edgar` freshness row the Product API reads for the frozen
        # 25. That row now carries a scope, so the fetch is safe — and the
        # thing that made it unsafe is now impossible rather than avoided.
        import app.research_enrichment as re_
        from app.source_scope import SCOPE_RESEARCH
        stage = open("app/research_enrichment.py", encoding="utf-8").read()
        assert "si.refresh_sec_filings" in stage
        assert f'scope=SCOPE_RESEARCH' in stage
        assert re_.SOURCE_SEC in re_.ENRICHMENT_SOURCES
        assert SCOPE_RESEARCH == "research"

    def test_every_enrichment_source_carries_an_explicit_status(self):
        import app.research_enrichment as re_

        class Empty:
            async def fetch(self, *a, **k):
                return []

        result = asyncio.run(re_.enrich_research_candidates(Empty()))
        assert result["provider_requests"] == 0
        assert set(result["sources"]) == set(re_.ENRICHMENT_SOURCES)
        assert all("status" in v for v in result["sources"].values())

    def test_the_lifecycle_invents_no_scheduler_of_its_own(self):
        # It did not GROW a scheduler; it joined the existing durable one. No
        # cron parser, no timer, no second dispatch mechanism lives here — the
        # schedule is a `job_schedules` row created disabled by migration 029.
        for forbidden in ("cron_expression", "APScheduler", "asyncio.sleep",
                          "while True"):
            assert forbidden not in self.SOURCE
        assert "job_schedules" not in self.SOURCE

    def test_it_never_mutates_a_universe(self):
        assert "INSERT INTO public.history_warmup" not in self.SOURCE
        assert "UPDATE public.history_warmup" not in self.SOURCE
