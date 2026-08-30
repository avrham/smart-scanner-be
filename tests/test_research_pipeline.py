"""The research domain: bounded, deterministic, and unable to reach the experiment.

Wave 2 measured the blind spot — 68 discovered symbols, 1 inside the frozen 25,
67 with too little history to analyse. This layer makes a bounded few of them
studiable. Every test here guards one of two things: that it works, or that it
cannot cross the line into the canonical experiment.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import app.research_ingest as ri
import app.research_universe as ru

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
NOW = datetime(2026, 8, 30, 14, 34, tzinfo=UTC)          # the real Sunday fetch
REFERENCE = date(2026, 8, 28)                            # the tape it describes
ACTIONABLE = date(2026, 8, 31)                           # first tradable session


# =========================================================================== #
# the requirement, verified from code rather than assumed
# =========================================================================== #

class TestHistoryRequirement:
    def test_the_binding_gate_is_read_from_the_canonical_module(self):
        from app.prospective_readiness import (
            CANDIDATE_MIN_DAILY_BARS, CANDIDATE_MIN_MONTHLY_PERIODS,
            CONTROL_MIN_DAILY_BARS, IMPLIED_DAILY_SESSIONS_FOR_MONTHLY)
        # The mission's remembered "~540" is close but is NOT the canonical
        # number, and the canonical one is not even a daily bar count: 24
        # completed MONTHS binds, and 504 daily sessions is the practical floor
        # that satisfies it. 175 and 200 never bind on their own.
        assert CANDIDATE_MIN_MONTHLY_PERIODS == 24
        assert IMPLIED_DAILY_SESSIONS_FOR_MONTHLY == 504
        assert ru.RESEARCH_MIN_DAILY_BARS == IMPLIED_DAILY_SESSIONS_FOR_MONTHLY
        assert CANDIDATE_MIN_DAILY_BARS < ru.RESEARCH_MIN_DAILY_BARS
        assert CONTROL_MIN_DAILY_BARS < ru.RESEARCH_MIN_DAILY_BARS

    def test_the_gate_is_periods_and_the_bar_count_is_only_a_fetch_target(self):
        # This distinction is load-bearing and was found in the live cohort:
        # the provider plan caps history at TWO YEARS (~500 sessions), so a
        # bar-count gate of 504 would have declared every freshly discovered
        # symbol permanently not-ready — while the real rule, 24 COMPLETED
        # MONTHS, is satisfiable at ~500 bars depending on where the listing
        # starts.
        assert ru.RESEARCH_FETCH_TARGET_SESSIONS == 504
        # 500 bars spanning 25 month-groups = 24 completed months -> READY,
        # even though 500 < 504.
        assert ru.is_research_ready(500, week_groups=105, month_groups=25,
                                    symbol="NVD")
        # The same 500 bars spanning only 24 groups = 23 completed -> NOT ready.
        assert not ru.is_research_ready(500, week_groups=105, month_groups=24,
                                        symbol="NVD")

    def test_period_counts_are_required_and_never_guessed_from_bars(self):
        # Without them the answer is "not yet", not an inference.
        assert not ru.is_research_ready(900)
        assert ru.readiness_gap(daily_bars=900, week_groups=None,
                                month_groups=None) == ["period_counts_unknown"]

    def test_readiness_delegates_to_the_canonical_evaluator(self):
        source = open("app/research_universe.py", encoding="utf-8").read()
        assert "from app.prospective_readiness import evaluate_symbol" in source
        assert "candidate_overall_ready" in source


class TestHistoryState:
    READY = {"week_groups": 110, "month_groups": 26}

    def test_a_discovered_symbol_with_no_bars_requires_history(self):
        assert ru.classify_history_state(daily_bars=0) == ru.STATE_HISTORY_REQUIRED

    def test_a_partly_warmed_symbol_is_warming(self):
        assert ru.classify_history_state(
            daily_bars=120, week_groups=26, month_groups=7) \
            == ru.STATE_HISTORY_WARMING

    def test_a_symbol_with_enough_history_is_ready(self):
        assert ru.classify_history_state(daily_bars=504, **self.READY) \
            == ru.STATE_RESEARCH_READY
        assert ru.classify_history_state(daily_bars=900, **self.READY) \
            == ru.STATE_RESEARCH_READY

    def test_a_discovered_symbol_that_is_ALREADY_ready_skips_warmup(self):
        # A symbol we already hold must not be queued for history it has.
        assert ru.is_research_ready(521, **self.READY)
        assert ru.classify_history_state(daily_bars=521, **self.READY) \
            == ru.STATE_RESEARCH_READY

    def test_exhausted_attempts_are_failed_not_unavailable(self):
        # Different sentences: "we could not get it" vs "there is nothing to
        # get". Only the second should stop us retrying forever.
        assert ru.classify_history_state(
            daily_bars=0, attempts=ru.MAX_WARMUP_ATTEMPTS) == ru.STATE_FAILED
        assert ru.classify_history_state(
            daily_bars=0, last_error_class="terminal") == ru.STATE_UNAVAILABLE

    def test_state_is_recomputed_not_trusted(self):
        # A stored state is a record of the last decision; the function is the
        # decision. This is what lets a crash mid-warmup self-correct.
        source = open("app/research_ingest.py", encoding="utf-8").read()
        assert "def refresh_states" in source
        assert "ru.classify_history_state" in source


# =========================================================================== #
# bounded operation
# =========================================================================== #

class TestBounds:
    def test_every_limit_is_conservative_and_derived_from_the_provider(self):
        # Massive Basic = 5 requests/minute; this repository paces warmup at 1
        # symbol / 75s behind a machine-wide lock. Limits that ignore that are
        # decoration.
        from app.config import settings
        assert settings.MASSIVE_REQUESTS_PER_MINUTE == 5
        assert settings.HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH == 1
        assert settings.HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS == 75
        assert ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN == 5
        assert ru.MAX_WARMUP_SYMBOLS_PER_RUN == 5
        assert ru.MAX_PROVIDER_REQUESTS_PER_RUN == 12
        assert ru.MAX_CONCURRENT_WARMUPS == 1

    def test_concurrency_is_one_because_the_lock_is_machine_wide(self):
        source = open("app/research_ingest.py", encoding="utf-8").read()
        assert "HISTORY_WARMUP_ADVISORY_LOCK_KEY" in source
        assert "pg_try_advisory_lock" in source

    def test_cooldown_parks_a_failed_symbol_for_an_hour(self):
        assert ru.WARMUP_COOLDOWN_MINUTES == 60
        until = ru.cooldown_until(NOW)
        assert until == NOW + timedelta(minutes=60)
        assert ru.is_in_cooldown(until, now=NOW)
        assert not ru.is_in_cooldown(until, now=NOW + timedelta(hours=2))
        assert not ru.is_in_cooldown(None, now=NOW)


# =========================================================================== #
# prioritisation — lexicographic, explainable, never a score
# =========================================================================== #

def _pool_row(symbol, *, reasons=("most_active",), observations=1, bars=0,
              reference=REFERENCE, rank=10):
    return {"symbol": symbol, "reasons": list(reasons),
            "observation_count": observations, "daily_bars": bars,
            "latest_reference_session": reference, "best_rank": rank}


class TestPrioritisation:
    def test_more_categories_wins_first(self):
        rows = [_pool_row("AAA", reasons=("most_active",)),
                _pool_row("BBB", reasons=("most_active", "top_gainers"))]
        assert [r["symbol"] for r in ru.prioritise(rows)] == ["BBB", "AAA"]

    def test_then_more_observations(self):
        rows = [_pool_row("AAA", observations=1), _pool_row("BBB", observations=3)]
        assert [r["symbol"] for r in ru.prioritise(rows)] == ["BBB", "AAA"]

    def test_then_partly_cached_because_it_is_cheaper_to_finish(self):
        rows = [_pool_row("AAA", bars=0), _pool_row("BBB", bars=200)]
        assert [r["symbol"] for r in ru.prioritise(rows)] == ["BBB", "AAA"]

    def test_then_recency_then_rank_then_alphabetical(self):
        rows = [_pool_row("BBB", reference=date(2026, 8, 27)),
                _pool_row("AAA", reference=date(2026, 8, 28))]
        assert [r["symbol"] for r in ru.prioritise(rows)] == ["AAA", "BBB"]
        tie = [_pool_row("ZZZ", rank=1), _pool_row("AAA", rank=9)]
        assert [r["symbol"] for r in ru.prioritise(tie)] == ["ZZZ", "AAA"]

    def test_the_order_is_total_so_a_rerun_picks_the_same_five(self):
        rows = [_pool_row(s) for s in ("DDD", "AAA", "CCC", "BBB")]
        first = [r["symbol"] for r in ru.prioritise(rows, limit=2)]
        second = [r["symbol"] for r in ru.prioritise(list(reversed(rows)), limit=2)]
        assert first == second == ["AAA", "BBB"]

    def test_it_produces_no_score(self):
        rows = [_pool_row("AAA", reasons=("a", "b"), observations=4)]
        picked = ru.prioritise(rows)[0]
        for banned in ("score", "rank_score", "opportunity", "weight",
                       "priority_value"):
            assert banned not in picked

    def test_the_choice_is_explainable_in_words(self):
        why = ru.explain_priority(_pool_row("CRM", reasons=("a", "b"),
                                            observations=3, bars=100))
        assert "in 2 discovery categories" in why
        assert "seen on 3 discovery observations" in why
        assert "partly cached already" in why

    def test_no_restricted_provider_value_is_used_for_ordering(self):
        # Price, market cap and volume are all in the FMP payload and none may
        # order anything — this ordering is not a place to launder them.
        dimensions = {d for d, _ in ru.PRIORITY_DIMENSIONS}
        assert not dimensions & {"price", "market_cap", "volume",
                                 "change_percent"}

    def test_the_cut_is_hard(self):
        rows = [_pool_row(f"S{i}") for i in range(20)]
        assert len(ru.prioritise(rows, limit=5)) == 5


# =========================================================================== #
# admission — with a fake connection
# =========================================================================== #

class FakeConn:
    """Routes by SQL substring, like the other DB fakes in this suite."""

    def __init__(self, *, pool=None, frozen=(), existing=(), bars=None):
        self.pool = pool or []
        self.frozen = list(frozen)
        self.existing = list(existing)
        self.bars = bars or {}
        self.upserts = []
        self.updates = []
        self.lock_granted = True

    async def fetch(self, sql, *args):
        if "history_warmup_universe_symbols" in sql:
            return [{"symbol": s} for s in self.frozen]
        if "FROM public.external_discovery_candidates" in sql:
            return self.pool
        if "SELECT symbol FROM public.research_symbols" in sql:
            return [{"symbol": s} for s in self.existing]
        if "FROM public.daily_bars" in sql:
            syms = args[0] if args else []
            return [{"symbol": s, "bars": self.bars[s]["bars"],
                     "first_session": self.bars[s].get("first_session"),
                     "latest_session": self.bars[s].get("latest_session"),
                     "month_groups": self.bars[s].get("month_groups"),
                     "week_groups": self.bars[s].get("week_groups")}
                    for s in syms if s in self.bars]
        if "FROM public.research_symbols" in sql:
            return []
        return []

    async def fetchrow(self, sql, *args):
        if "INSERT INTO public.research_symbols" in sql:
            self.upserts.append(args)
            return {"inserted": args[0] not in self.existing}
        return None

    async def fetchval(self, sql, *args):
        if "pg_try_advisory_lock" in sql:
            return self.lock_granted
        return None

    async def execute(self, sql, *args):
        self.updates.append((sql, args))
        return "UPDATE 1"


def _discovery(symbol, *, reasons=("most_active",), observations=1, rank=5):
    return {"symbol": symbol, "reasons": list(reasons),
            "observation_count": observations,
            "first_observed_at": NOW, "latest_observed_at": NOW,
            "first_reference_session": REFERENCE,
            "latest_reference_session": REFERENCE,
            "first_actionable_session": ACTIONABLE,
            "best_rank": rank, "discovery_source": "fmp"}


class TestAdmission:
    def test_a_discovered_symbol_with_no_history_needs_history(self):
        conn = FakeConn(pool=[_discovery("BTAI")])
        summary = asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        assert summary["admitted"] == 1
        # state argument (index 10) is the recomputed one
        assert conn.upserts[0][10] == ru.STATE_HISTORY_REQUIRED

    def test_a_discovered_symbol_we_already_hold_is_admitted_ready(self):
        conn = FakeConn(pool=[_discovery("XYZ")],
                        bars={"XYZ": {"bars": 600, "week_groups": 120,
                                      "month_groups": 28}})
        asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        assert conn.upserts[0][10] == ru.STATE_RESEARCH_READY

    def test_frozen_universe_members_are_never_admitted(self):
        # NVDA appears in the movers list AND is one of the 25. It is already
        # scanned properly; research is for what we cannot see.
        conn = FakeConn(pool=[_discovery("NVDA"), _discovery("BTAI")],
                        frozen=["NVDA"])
        summary = asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        assert [u[0] for u in conn.upserts] == ["BTAI"]
        assert summary["considered"] == 1

    def test_reference_market_symbols_are_never_admitted(self):
        conn = FakeConn(pool=[_discovery("SPY")], frozen=["SPY"])
        asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        assert conn.upserts == []

    def test_admission_is_bounded_to_the_configured_maximum(self):
        conn = FakeConn(pool=[_discovery(f"S{i:02d}") for i in range(67)])
        asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        # 67 discovered, 5 admitted. This is the explosion bound.
        assert len(conn.upserts) == ru.MAX_NEW_RESEARCH_SYMBOLS_PER_RUN

    def test_rediscovery_refreshes_and_does_not_consume_the_new_budget(self):
        pool = [_discovery("OLD")] + [_discovery(f"NEW{i}") for i in range(5)]
        conn = FakeConn(pool=pool, existing=["OLD"])
        summary = asyncio.run(ri.admit_from_discovery(conn, since=REFERENCE))
        assert summary["admitted"] == 5 and summary["refreshed"] == 1
        assert "OLD" in [u[0] for u in conn.upserts]

    def test_rediscovery_never_resets_warmup_progress(self):
        # The upsert must enrich, not overwrite: a symbol the market notices
        # every day would otherwise restart its own history forever.
        sql = ri.UPSERT_RESEARCH_SYMBOL_SQL
        assert "warmup_attempts" not in sql.split("DO UPDATE SET")[1]
        assert "history_daily_bars" not in sql.split("DO UPDATE SET")[1]
        assert "GREATEST" in sql and "array_agg(DISTINCT r" in sql


# =========================================================================== #
# warmup
# =========================================================================== #

class WarmConn(FakeConn):
    def __init__(self, batch, bars_after=None, **kw):
        super().__init__(**kw)
        self.batch = batch
        self.bars_after = bars_after or {}
        self._calls = 0
        #: What the attempt-counter UPDATE ... RETURNING hands back.
        self.attempt_number = 1

    async def fetchval(self, sql, *args):
        if "warmup_attempts=warmup_attempts+1" in sql:
            return self.attempt_number
        return await super().fetchval(sql, *args)

    async def fetch(self, sql, *args):
        if "FROM public.research_symbols" in sql and "state IN" in sql:
            return self.batch
        if "FROM public.daily_bars" in sql:
            self._calls += 1
            syms = args[0] if args else []
            table = self.bars_after if self._calls > 1 else self.bars
            return [{"symbol": s, "bars": table[s]["bars"],
                     "first_session": None, "latest_session": None,
                     "month_groups": table[s].get("month_groups"),
                     "week_groups": table[s].get("week_groups")}
                    for s in syms if s in table]
        return await super().fetch(sql, *args)


def canonical_bars(symbol, count=3, end=date(2026, 8, 27)):
    """The shape `provider.get_daily_bars` actually returns: a LIST of already
    canonical bar dicts. Getting this wrong in a fake is how a test proves the
    error path instead of the success path."""
    return [{"symbol": symbol, "trading_date": end - timedelta(days=i),
             "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
             "volume": 1_000_000.0}
            for i in range(count)]


class FakeProvider:
    name = "fake-massive"

    def __init__(self, *, fail=None, bars=None):
        self.fail = fail or {}
        self.bars = bars or {}
        self.calls = []

    async def get_daily_bars(self, symbol, frm, to):
        self.calls.append((symbol, frm, to))
        if symbol in self.fail:
            raise self.fail[symbol]
        return self.bars.get(symbol, canonical_bars(symbol))


def _batch_row(symbol, bars=0):
    return {"symbol": symbol, "reasons": ["most_active"],
            "observation_count": 1, "daily_bars": bars,
            "latest_reference_session": REFERENCE, "best_rank": 3,
            "warmup_attempts": 0, "warmup_cooldown_until": None}


class TestWarmup:
    def test_one_provider_request_per_symbol_and_it_is_counted(self):
        conn = WarmConn([_batch_row("AAA"), _batch_row("BBB")])
        provider = FakeProvider()
        summary = asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        assert len(provider.calls) == 2
        assert summary["provider_requests"] == 2

    def test_the_request_budget_is_enforced_not_hoped_for(self):
        conn = WarmConn([_batch_row(f"S{i}") for i in range(10)])
        provider = FakeProvider()
        summary = asyncio.run(ri.run_warmup(
            conn, provider, limit=10, max_requests=3, spacing_seconds=0))
        assert summary["provider_requests"] == 3
        assert summary["budget_exhausted"] is True
        assert len(provider.calls) == 3

    def test_one_bad_symbol_never_costs_the_others_their_warmup(self):
        conn = WarmConn([_batch_row("GOOD1"), _batch_row("BAD"),
                         _batch_row("GOOD2")])
        provider = FakeProvider(fail={"BAD": RuntimeError("provider exploded")})
        summary = asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        # All three attempted — the ORDER is the documented priority, not the
        # order they happened to arrive in.
        assert sorted(c[0] for c in provider.calls) == ["BAD", "GOOD1", "GOOD2"]
        assert [f["symbol"] for f in summary["failed"]] == ["BAD"]
        assert sorted(w["symbol"] for w in summary["warmed"]) == ["GOOD1", "GOOD2"]

    def test_a_failed_symbol_gets_a_cooldown_not_an_immediate_retry(self):
        conn = WarmConn([_batch_row("BAD")])
        provider = FakeProvider(fail={"BAD": RuntimeError("boom")})
        asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        cooldown_writes = [u for u in conn.updates
                           if "warmup_cooldown_until=$4" in u[0]]
        assert cooldown_writes and cooldown_writes[0][1][3] is not None

    def test_a_symbol_in_cooldown_is_skipped_without_consuming_the_batch(self):
        parked = _batch_row("PARKED")
        parked["warmup_cooldown_until"] = NOW + timedelta(minutes=30)
        conn = WarmConn([parked, _batch_row("READY")])
        picked = asyncio.run(ri.select_warmup_batch(conn, now=NOW))
        assert [r["symbol"] for r in picked] == ["READY"]

    def test_the_batch_is_ordered_by_the_documented_priority(self):
        conn = WarmConn([_batch_row("PLAIN"),
                         {**_batch_row("RICH"), "reasons": ["a", "b", "c"]}])
        picked = asyncio.run(ri.select_warmup_batch(conn, now=NOW))
        assert [r["symbol"] for r in picked] == ["RICH", "PLAIN"]

    def test_warmup_is_serialised_by_the_shared_machine_wide_lock(self):
        conn = WarmConn([_batch_row("AAA")])
        conn.lock_granted = False
        provider = FakeProvider()
        summary = asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        assert summary["locked"] is True
        assert provider.calls == []          # no provider call while locked

    def test_no_provider_is_reported_not_raised(self):
        conn = WarmConn([_batch_row("AAA")])
        summary = asyncio.run(ri.run_warmup(conn, None))
        assert summary["reason"] == "no_provider"

    def test_a_symbol_the_provider_cannot_fill_is_unavailable_not_failed(self):
        # Retrying will not conjure history that does not exist.
        conn = WarmConn([_batch_row("THIN")], bars_after={"THIN": {"bars": 12}})
        provider = FakeProvider()
        asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        terminal = [u for u in conn.updates
                    if "warmup_last_error_code=$2" in u[0]
                    and u[1][1] == "insufficient_provider_history"]
        assert terminal and terminal[0][1][2] == "terminal"

    def test_a_repeat_warmup_that_adds_nothing_is_provider_exhausted(self):
        # LGPS in the live cohort: 359 bars, listing younger than the 24-month
        # gate, and a second call returned NOTHING NEW. That is the provider
        # saying it has given us everything — `unavailable`, not `failed`,
        # which would read as our fault.
        conn = WarmConn([_batch_row("THIN", bars=359)],
                        bars={"THIN": {"bars": 359}},
                        bars_after={"THIN": {"bars": 359}})
        conn.attempt_number = 2
        provider = FakeProvider(bars={"THIN": canonical_bars("THIN", 1)})
        asyncio.run(ri.run_warmup(conn, provider, spacing_seconds=0))
        terminal = [u for u in conn.updates
                    if "warmup_last_error_code=$2" in u[0]
                    and u[1][1] == "provider_history_exhausted"]
        assert terminal and terminal[0][1][2] == "terminal"

    def test_warmup_writes_bars_through_the_canonical_upsert(self):
        # One way daily bars enter this database, not two.
        source = open("app/research_ingest.py", encoding="utf-8").read()
        assert "from app.history_warmup_execute import" in source
        assert "upsert_daily_bars" in source and "normalize_daily_bars" in source


# =========================================================================== #
# THE BOUNDARY — the part that must never be crossed
# =========================================================================== #

class TestExperimentBoundary:
    RESEARCH_MODULES = ("app/research_universe.py", "app/research_ingest.py",
                        "app/research_scan.py")

    def _code(self, path):
        import ast
        tree = ast.parse(open(path, encoding="utf-8").read())
        for node in ast.walk(tree):
            for field in ("body", "orelse", "finalbody"):
                statements = getattr(node, field, None)
                if not isinstance(statements, list):
                    continue          # IfExp.orelse is a single expression
                for stmt in statements:
                    if (isinstance(stmt, ast.Expr)
                            and isinstance(stmt.value, ast.Constant)
                            and isinstance(stmt.value.value, str)):
                        stmt.value.value = ""
        return ast.unparse(tree)

    def test_no_research_module_writes_a_universe(self):
        for path in self.RESEARCH_MODULES:
            code = self._code(path)
            assert "INSERT INTO public.history_warmup" not in code, path
            assert "UPDATE public.history_warmup" not in code, path

    def test_no_research_module_writes_an_experiment_relation(self):
        for path in self.RESEARCH_MODULES:
            code = self._code(path)
            for relation in ("strategy_shadow_runs", "strategy_shadow_pairs",
                             "strategy_shadow_run_pairs",
                             "strategy_shadow_evaluations"):
                assert relation not in code, f"{path}: {relation}"

    def test_no_research_module_creates_a_shadow_run(self):
        for path in self.RESEARCH_MODULES:
            code = self._code(path)
            assert "create_shadow_run" not in code, path
            assert "run_shadow_comparison" not in code, path
            assert "run_shadow_campaign" not in code, path

    def test_no_research_module_touches_outcomes_or_attention(self):
        for path in self.RESEARCH_MODULES:
            code = self._code(path)
            for forbidden in ("shadow_pair_outcomes", "classify_attention",
                              "attention_sort_key", "pattern_configs"):
                assert forbidden not in code, f"{path}: {forbidden}"

    def test_the_research_tables_carry_none_of_an_experiment_rows_columns(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        block = sql.split("CREATE TABLE IF NOT EXISTS public.research_scan_results")[1]
        for column in ("pair_id", "run_id", "arm_code", "experiment_code"):
            assert column not in block.split(");")[0], column

    def test_the_database_refuses_an_ENTER_research_row(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        assert "research_scan_no_enter_ck" in sql
        assert "verdict <> 'ENTER'" in sql

    def test_the_ingestion_role_gains_only_the_two_research_tables(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        grants = [ln.strip() for ln in sql.splitlines()
                  if ln.strip().upper().startswith("GRANT ")]
        targets = {ln.split(" ON ")[1].split(" TO ")[0].strip() for ln in grants}
        assert targets == {"public.research_symbols",
                           "public.research_scan_results", "public.daily_bars"}
        # And the shared bar table is confined by predicate to research symbols,
        # so the grant cannot reach a frozen-universe bar.
        assert "symbol IN (SELECT symbol FROM public.research_symbols)" in sql
        assert not any("DELETE" in ln.upper() for ln in grants)


class TestCanonicalStrategyReuse:
    def test_the_research_scan_calls_the_SAME_evaluator_as_the_experiment(self):
        source = open("app/research_scan.py", encoding="utf-8").read()
        assert "from app.workers.shadow.runner import _evaluate_arm, _resolve_arm" in source
        assert "build_canonical_frame" in source

    def test_there_is_no_second_strategy_implementation(self):
        source = open("app/research_scan.py", encoding="utf-8").read()
        for forbidden in ("def evaluate(", "class Wyckoff", "sma150",
                          "def _compute_structure", "def _score"):
            assert forbidden not in source, forbidden

    def test_the_strategy_identity_comes_from_the_campaign_constants(self):
        import app.research_scan as rscan
        from app.prospective_campaign import (CANDIDATE_STRATEGY_CODE,
                                              CONTROL_STRATEGY_CODE)
        assert rscan.CANDIDATE_STRATEGY_CODE == CANDIDATE_STRATEGY_CODE
        assert rscan.CONTROL_STRATEGY_CODE == CONTROL_STRATEGY_CODE

    def test_the_research_scan_reads_local_bars_only(self):
        # LocalHistoryProvider is the lookahead barrier the prospective
        # campaign already uses; a research scan makes no provider call.
        source = open("app/research_scan.py", encoding="utf-8").read()
        assert "LocalHistoryProvider" in source
        assert "get_market_data_provider" not in source


class TestSectorHandling:
    def test_a_frozen_universe_symbol_has_a_known_sector(self):
        assert ru.classify_sector_state(
            "NVDA", benchmark_available=True) == ru.SECTOR_KNOWN

    def test_a_discovered_symbol_is_sector_unknown_and_never_guessed(self):
        for symbol in ("BTAI", "FNGR", "CHAI", "XTNT"):
            assert ru.classify_sector_state(
                symbol, benchmark_available=True) == ru.SECTOR_UNKNOWN

    def test_no_benchmark_series_is_reference_unavailable(self):
        assert ru.classify_sector_state(
            "BTAI", benchmark_available=False) == ru.REFERENCE_UNAVAILABLE

    def test_benchmark_context_still_works_without_a_sector_mapping(self):
        # Comparing to SPY needs no mapping at all, so a discovered symbol gets
        # real market context rather than none.
        source = open("app/research_scan.py", encoding="utf-8").read()
        assert "PRIMARY_BENCHMARK" in source
        assert "symbol_not_in_sector_registry" in source


class TestResearchCandidates:
    """Rewritten for the V1 vocabulary: discovery strength explains why we
    looked, strategy evidence decides whether it survived. The previous
    version of this class asserted the old single-list behaviour that reported
    hard-AVOID symbols as candidates."""

    def _row(self, **kw):
        base = {"state": ru.STATE_RESEARCH_SCANNED, "history_daily_bars": 520,
                "discovery_reasons": ["most_active"],
                "discovery_observation_count": 1,
                "rejection_reason": None, "structure_state": None,
                "setup_state": None, "benchmark_relative": None,
                "latest_reference_session": REFERENCE}
        base.update(kw)
        return base

    def test_why_we_looked_is_discovery_only(self):
        looked = ru.looked_because(
            self._row(discovery_reasons=["most_active", "top_gainers"],
                      discovery_observation_count=3),
            latest_reference_session=REFERENCE)
        assert set(looked) <= set(ru.LOOKED_REASONS)
        assert ru.LOOKED_MULTIPLE_LISTS in looked
        assert ru.LOOKED_REPEATEDLY in looked

    def test_what_the_screen_found_is_strategy_evidence_only(self):
        findings = ru.screen_findings(
            self._row(structure_state="accumulation",
                      setup_state="setup_forming",
                      benchmark_relative="outperforming"))
        assert set(findings) <= set(ru.SCREEN_REASONS)
        assert ru.SCREEN_STRUCTURE_PRESENT in findings

    def test_the_two_vocabularies_never_overlap(self):
        assert not set(ru.LOOKED_REASONS) & set(ru.SCREEN_REASONS)

    def test_an_unscanned_symbol_is_never_a_candidate(self):
        assert not ru.is_research_candidate(
            self._row(state=ru.STATE_RESEARCH_READY))

    def test_a_hard_disqualified_symbol_is_never_a_candidate(self):
        assert not ru.is_research_candidate(
            self._row(rejection_reason="price_below_minimum",
                      discovery_reasons=["a", "b", "c"],
                      discovery_observation_count=5))

    def test_a_stale_discovery_loses_its_recency_reason(self):
        old = self._row(latest_reference_session=date(2026, 8, 1))
        assert ru.LOOKED_RECENTLY not in ru.looked_because(
            old, latest_reference_session=REFERENCE)

    def test_every_candidate_state_is_in_the_declared_vocabulary(self):
        for state in (ru.STATE_RESEARCH_SCANNED, ru.STATE_RESEARCH_READY,
                      ru.STATE_UNAVAILABLE, ru.STATE_FAILED):
            verdict = ru.classify_candidate(self._row(state=state))
            assert verdict["candidate_state"] in ru.CANDIDATE_STATES

    def test_no_score_is_produced_anywhere(self):
        verdict = ru.classify_candidate(
            self._row(structure_state="accumulation",
                      setup_state="setup_confirmed"))
        for banned in ("score", "rank", "weight", "confidence"):
            assert banned not in verdict


# =========================================================================== #
# temporal semantics carried forward from migration 025
# =========================================================================== #

class TestTemporalSemantics:
    def test_admission_reads_the_MARKET_session_not_the_actionable_one(self):
        # "Which session surfaced it" is a question about the tape. Grouping on
        # session_date would count a weekend fetch under a session that had not
        # happened — migration 025's finding.
        assert "reference_session_date" in ri.DISCOVERY_POOL_SQL
        assert "count(DISTINCT c.reference_session_date)" in ri.DISCOVERY_POOL_SQL

    def test_both_dates_survive_into_the_research_row(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        for column in ("first_reference_session", "latest_reference_session",
                       "first_actionable_session"):
            assert column in sql, column

    def test_the_row_cannot_claim_an_impossible_ordering(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        assert "first_reference_session <= first_actionable_session" in sql

    def test_a_weekend_discovery_keeps_the_two_apart(self):
        import app.external_discovery as ed
        sunday = datetime(2026, 8, 30, 10, 34, tzinfo=ET)
        assert ed.infer_reference_session(sunday) == REFERENCE      # Friday
        assert ed.resolve_session(sunday) == ACTIONABLE             # Monday

    def test_a_premarket_discovery_keeps_the_two_apart(self):
        import app.external_discovery as ed
        premarket = datetime(2026, 8, 28, 6, 24, tzinfo=ET)
        assert ed.infer_reference_session(premarket) == date(2026, 8, 27)
        assert ed.resolve_session(premarket) == date(2026, 8, 28)


# =========================================================================== #
# freshness lifecycle
# =========================================================================== #

class TestFreshnessLifecycle:
    def test_stale_bars_are_detected_against_the_latest_COMPLETED_session(self):
        from app.history_warmup_execute import classify_incremental_symbol_state
        target = date(2026, 8, 28)
        assert classify_incremental_symbol_state(date(2026, 8, 25), target) \
            == "incremental_refresh_needed"
        assert classify_incremental_symbol_state(date(2026, 8, 28), target) \
            == "incremental_current"

    def test_a_never_warmed_symbol_is_unverifiable_not_current(self):
        from app.history_warmup_execute import classify_incremental_symbol_state
        assert classify_incremental_symbol_state(None, date(2026, 8, 28)) \
            == "incremental_unverifiable"

    def test_the_missing_session_list_uses_the_shared_trading_calendar(self):
        from app.history_warmup_execute import missing_trading_sessions
        # 26th Wed, 27th Thu, 28th Fri — a weekend is never a missing session.
        assert missing_trading_sessions(date(2026, 8, 25), date(2026, 8, 28)) == [
            date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)]
        assert missing_trading_sessions(date(2026, 8, 28), date(2026, 8, 31)) == [
            date(2026, 8, 31)]

    def test_the_refresh_command_uses_the_canonical_service(self):
        source = open("ops/analysis/refresh_daily_history.py",
                      encoding="utf-8").read()
        assert "history_incremental_refresh_execute_service" in source
        # And paces itself at the provider's configured rate rather than a
        # number chosen here.
        assert "HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS" in source

    def test_the_refresh_command_never_enables_a_schedule(self):
        source = open("ops/analysis/refresh_daily_history.py",
                      encoding="utf-8").read()
        assert "job_schedules" not in source
        assert "UPDATE" not in source.split('"""')[2]


# =========================================================================== #
# licensing — unchanged and not weakened
# =========================================================================== #

class TestLicensing:
    def test_research_rows_inherit_internal_research_only(self):
        sql = open("app/db/migrations/026_research_symbols.sql",
                   encoding="utf-8").read()
        assert sql.count("DEFAULT 'internal_research_only'") == 2

    def test_the_product_reader_is_granted_neither_research_table(self):
        grants = open("ops/sql/create_smart_scanner_product_reader.sql",
                      encoding="utf-8").read()
        for relation in ("research_symbols", "research_scan_results"):
            assert f"GRANT SELECT ON public.{relation}" not in grants

    def test_no_router_names_a_research_relation(self):
        import pathlib
        for path in pathlib.Path("app/routers").glob("*.py"):
            source = path.read_text(encoding="utf-8")
            for relation in ("research_symbols", "research_scan_results"):
                assert relation not in source, f"{path.name}: {relation}"

    def test_the_three_layer_enforcement_is_still_intact(self):
        import app.source_licensing as lic
        assert lic.SOURCE_LICENSING["fmp"] == lic.LICENSING_INTERNAL_ONLY
        assert not lic.is_product_displayable(lic.LICENSING_UNKNOWN)
        assert "external_discovery_candidates" in lic.PRODUCT_FORBIDDEN_RELATIONS
