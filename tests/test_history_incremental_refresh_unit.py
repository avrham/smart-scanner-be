"""Fast, DB-free unit tests for history_incremental_refresh.v1: session-gap
math, per-symbol state classification, idempotency identity, bounded scope,
and provider isolation. Every completed-session decision reuses the EXISTING
app.prospective_session policy (never reimplemented); nothing here recomputes
launch_ready/both_ready.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.history_warmup_execute import (
    INCREMENTAL_REFRESH_CONTRACT_VERSION,
    MODE_INCREMENTAL,
    STATE_INCREMENTAL_CURRENT,
    STATE_INCREMENTAL_REFRESH_NEEDED,
    STATE_INCREMENTAL_STALE,
    STATE_INCREMENTAL_UNVERIFIABLE,
    build_incremental_preflight,
    classify_incremental_symbol_state,
    incremental_refresh_identity,
    missing_trading_sessions,
)
from app.prospective_session import is_trading_day, resolve_latest_completed_session


# --------------------------------------------------------------------------
# session resolution reuses the existing policy — sanity, not reimplementation
# --------------------------------------------------------------------------
def test_target_session_never_the_still_forming_current_session():
    from datetime import datetime, timezone
    # Wednesday 10:00 ET, well before the 16:00 close -> latest completed is
    # the PRIOR trading day, never "today".
    now = datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    target = resolve_latest_completed_session(now)
    assert target < date(2026, 7, 29)


def test_weekend_and_holiday_walked_past():
    # Friday 2026-07-24 is a trading day; Sat/Sun are not.
    assert is_trading_day(date(2026, 7, 24)) is True
    assert is_trading_day(date(2026, 7, 25)) is False
    assert is_trading_day(date(2026, 7, 26)) is False
    # July 4 (Saturday-observed or fixed) — at least confirm July 4 2026 (a
    # Saturday) is excluded either as a weekend or an observed holiday.
    assert is_trading_day(date(2026, 7, 4)) is False


# --------------------------------------------------------------------------
# missing_trading_sessions: bounded forward walk, excludes weekends/holidays
# --------------------------------------------------------------------------
def test_missing_sessions_excludes_weekend():
    # latest local = Friday 2026-07-24; target = Monday 2026-07-27 (skips Sat/Sun)
    missing = missing_trading_sessions(date(2026, 7, 24), date(2026, 7, 27))
    assert missing == [date(2026, 7, 27)]


def test_missing_sessions_empty_when_already_current():
    assert missing_trading_sessions(date(2026, 7, 28), date(2026, 7, 28)) == []
    assert missing_trading_sessions(date(2026, 7, 29), date(2026, 7, 28)) == []


def test_missing_sessions_none_when_no_local_baseline():
    assert missing_trading_sessions(None, date(2026, 7, 28)) == []


def test_first_and_last_missing_session_bounds():
    missing = missing_trading_sessions(date(2026, 7, 28), date(2026, 7, 31))
    # 2026-07-28 Tue -> 29 Wed, 30 Thu, 31 Fri all trading days
    assert missing[0] == date(2026, 7, 29)
    assert missing[-1] == date(2026, 7, 31)
    assert len(missing) == 3


def test_missing_sessions_bounded_never_unlimited_backfill():
    # A pathological multi-year gap still terminates (guarded walk), and the
    # result length is bounded by the actual number of trading days in range
    # — never silently truncated to a magic constant, never infinite.
    missing = missing_trading_sessions(date(2020, 1, 1), date(2020, 3, 1))
    assert 0 < len(missing) < 45  # ~2 months of trading days, sanity bound


# --------------------------------------------------------------------------
# per-symbol state classification — launch_ready is a SEPARATE, unread axis
# --------------------------------------------------------------------------
def test_current_state_when_latest_meets_target():
    assert classify_incremental_symbol_state(date(2026, 7, 28), date(2026, 7, 28)) == \
        STATE_INCREMENTAL_CURRENT


def test_refresh_needed_when_latest_behind_target():
    assert classify_incremental_symbol_state(date(2026, 7, 28), date(2026, 7, 29)) == \
        STATE_INCREMENTAL_REFRESH_NEEDED


def test_unverifiable_when_no_local_baseline():
    assert classify_incremental_symbol_state(None, date(2026, 7, 29)) == \
        STATE_INCREMENTAL_UNVERIFIABLE


def test_unverifiable_when_target_unresolvable():
    assert classify_incremental_symbol_state(date(2026, 7, 28), None) == \
        STATE_INCREMENTAL_UNVERIFIABLE


def test_launch_ready_does_not_block_incremental_classification():
    """A symbol that is ALREADY history_depth_ready (both_ready=True) still
    classifies as refresh_needed when its latest local bar is behind target —
    launch_ready and incremental freshness are independent axes."""
    depth_ready_but_stale = classify_incremental_symbol_state(date(2026, 7, 28), date(2026, 7, 29))
    assert depth_ready_but_stale == STATE_INCREMENTAL_REFRESH_NEEDED
    # (the caller is responsible for reporting history_depth_ready=True
    # alongside this state — build_incremental_preflight does exactly that,
    # exercised below.)


# --------------------------------------------------------------------------
# idempotency identity
# --------------------------------------------------------------------------
def test_identity_deterministic_for_same_inputs():
    a = incremental_refresh_identity(symbol="AAPL", latest_local_session=date(2026, 7, 28),
                                     target_completed_session=date(2026, 7, 29))
    b = incremental_refresh_identity(symbol="aapl", latest_local_session=date(2026, 7, 28),
                                     target_completed_session=date(2026, 7, 29))
    assert a == b  # case-insensitive symbol
    assert a.startswith("hwi:")


def test_identity_same_replay_is_identical():
    kwargs = dict(symbol="MSFT", latest_local_session=date(2026, 7, 28),
                  target_completed_session=date(2026, 7, 29))
    assert incremental_refresh_identity(**kwargs) == incremental_refresh_identity(**kwargs)


def test_identity_later_target_session_is_a_new_round():
    r1 = incremental_refresh_identity(symbol="MSFT", latest_local_session=date(2026, 7, 28),
                                      target_completed_session=date(2026, 7, 29))
    r2 = incremental_refresh_identity(symbol="MSFT", latest_local_session=date(2026, 7, 29),
                                      target_completed_session=date(2026, 7, 30))
    assert r1 != r2


def test_identity_different_symbol_is_different():
    a = incremental_refresh_identity(symbol="AAPL", latest_local_session=date(2026, 7, 28),
                                     target_completed_session=date(2026, 7, 29))
    b = incremental_refresh_identity(symbol="MSFT", latest_local_session=date(2026, 7, 28),
                                     target_completed_session=date(2026, 7, 29))
    assert a != b


def test_identity_none_latest_session_distinct_from_a_real_date():
    a = incremental_refresh_identity(symbol="AAPL", latest_local_session=None,
                                     target_completed_session=date(2026, 7, 29))
    b = incremental_refresh_identity(symbol="AAPL", latest_local_session=date(2026, 7, 28),
                                     target_completed_session=date(2026, 7, 29))
    assert a != b


def test_contract_version_pinned():
    assert INCREMENTAL_REFRESH_CONTRACT_VERSION == "history_incremental_refresh.v1"
    assert MODE_INCREMENTAL == "incremental"


# --------------------------------------------------------------------------
# build_incremental_preflight — bounded scope, no-provider, aggregate states
# --------------------------------------------------------------------------
def _cooldown(allowed=True):
    return {"execution_allowed_by_cooldown": allowed, "cooldown_remaining_seconds": 0,
            "next_execution_not_before": None}


def test_preflight_all_current_reports_current_state():
    pf = build_incremental_preflight(
        requested_symbol_count=2, normalized_symbols=["AAA", "BBB"], duplicates_removed=0,
        latest_by_symbol={"AAA": date(2026, 7, 28), "BBB": date(2026, 7, 28)},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={"AAA": True, "BBB": True},
        cooldown=_cooldown(), max_batch=1, provider_rate_limit_per_minute=5)
    assert pf["state"] == STATE_INCREMENTAL_CURRENT
    assert pf["symbols_current"] == ["AAA", "BBB"]
    assert pf["symbols_requiring_refresh"] == []
    assert pf["provider_called"] is False
    assert pf["estimated_provider_requests"] == 0


def test_preflight_stale_when_any_symbol_needs_refresh():
    pf = build_incremental_preflight(
        requested_symbol_count=2, normalized_symbols=["AAA", "BBB"], duplicates_removed=0,
        latest_by_symbol={"AAA": date(2026, 7, 28), "BBB": date(2026, 7, 27)},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={"AAA": True, "BBB": True},
        cooldown=_cooldown(), max_batch=5, provider_rate_limit_per_minute=5)
    assert pf["state"] == STATE_INCREMENTAL_STALE
    assert pf["symbols_requiring_refresh"] == ["BBB"]
    assert pf["estimated_provider_requests"] == 1
    assert pf["next_batch"]["symbols"] == ["BBB"]


def test_preflight_reports_history_depth_ready_as_a_separate_field():
    """A symbol can be BOTH history_depth_ready=True and state=refresh_needed
    — launch_ready is reported, never overwritten by incremental freshness."""
    pf = build_incremental_preflight(
        requested_symbol_count=1, normalized_symbols=["AAA"], duplicates_removed=0,
        latest_by_symbol={"AAA": date(2026, 7, 27)},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={"AAA": True},
        cooldown=_cooldown(), max_batch=1, provider_rate_limit_per_minute=5)
    row = pf["symbols"][0]
    assert row["history_depth_ready"] is True
    assert row["state"] == STATE_INCREMENTAL_REFRESH_NEEDED


def test_preflight_bounded_batch_never_exceeds_max_batch():
    pf = build_incremental_preflight(
        requested_symbol_count=5, normalized_symbols=["A", "B", "C", "D", "E"],
        duplicates_removed=0,
        latest_by_symbol={s: date(2026, 7, 20) for s in "ABCDE"},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={},
        cooldown=_cooldown(), max_batch=2, provider_rate_limit_per_minute=5)
    assert pf["next_batch"]["symbol_count"] == 2
    assert len(pf["symbols_requiring_refresh"]) == 5  # all 5 need refresh...
    assert len(pf["next_batch"]["symbols"]) == 2       # ...but the batch stays bounded


def test_preflight_unavailable_when_cooldown_active():
    pf = build_incremental_preflight(
        requested_symbol_count=1, normalized_symbols=["AAA"], duplicates_removed=0,
        latest_by_symbol={"AAA": date(2026, 7, 20)},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={},
        cooldown=_cooldown(allowed=False), max_batch=1, provider_rate_limit_per_minute=5)
    assert pf["next_batch"]["available"] is False
    assert "provider_cooldown_active" in pf["unavailable_reasons"]


def test_preflight_duplicates_removed_reported():
    pf = build_incremental_preflight(
        requested_symbol_count=3, normalized_symbols=["AAA", "BBB"], duplicates_removed=1,
        latest_by_symbol={"AAA": date(2026, 7, 28), "BBB": date(2026, 7, 28)},
        target_session=date(2026, 7, 28), depth_ready_by_symbol={},
        cooldown=_cooldown(), max_batch=1, provider_rate_limit_per_minute=5)
    assert pf["duplicates_removed"] == 1
    assert pf["requested_symbol_count"] == 3
    assert pf["normalized_symbol_count"] == 2


# --------------------------------------------------------------------------
# bounded symbol selection — no unbounded default anywhere in this module
# --------------------------------------------------------------------------
def test_normalize_universe_symbols_rejects_empty_and_over_cap():
    from app.history_warmup_execute import normalize_universe_symbols, UniverseError
    with pytest.raises(UniverseError):
        normalize_universe_symbols([], max_symbols=25)
    with pytest.raises(UniverseError):
        normalize_universe_symbols([f"S{i}" for i in range(30)], max_symbols=25)


def test_normalize_universe_symbols_dedupes():
    from app.history_warmup_execute import normalize_universe_symbols
    norm = normalize_universe_symbols(["aapl", "AAPL", "msft"], max_symbols=25)
    assert norm["symbols"] == ["AAPL", "MSFT"]
    assert norm["duplicates_removed"] == 1


# --------------------------------------------------------------------------
# provider isolation — no provider import in this module, or in the
# prospective/outcome-worker modules that must never see the provider client
# --------------------------------------------------------------------------
def _imported_names(module) -> set:
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
            names.update(a.name for a in node.names)
    return names


_FORBIDDEN_PROVIDER_IMPORTS = {
    "MassiveClient", "MassiveApiError", "get_market_data_provider", "providers",
    "httpx", "requests", "aiohttp",
}


def test_history_warmup_execute_module_constructs_no_provider_itself():
    """The pure module never imports/constructs a provider — it is injected
    by the caller (the admin route), exactly as the module's own docstring
    states ('This module is import-safe: it constructs NO provider')."""
    import app.history_warmup_execute as hwx
    hit = _imported_names(hwx) & _FORBIDDEN_PROVIDER_IMPORTS
    assert not hit, f"history_warmup_execute imports forbidden provider names: {hit}"


def test_prospective_local_provider_module_has_no_massive_import():
    import app.prospective_local_provider as lp
    hit = _imported_names(lp) & _FORBIDDEN_PROVIDER_IMPORTS
    assert not hit, f"prospective_local_provider imports forbidden provider names: {hit}"


def test_outcome_handler_module_has_no_massive_import():
    import app.jobs.handlers.prospective_outcome as oh
    hit = _imported_names(oh) & _FORBIDDEN_PROVIDER_IMPORTS
    assert not hit, f"outcome handler imports forbidden provider names: {hit}"


def test_outcome_local_reader_module_has_no_massive_import():
    import app.jobs.prospective_outcome_local_reader as reader
    hit = _imported_names(reader) & _FORBIDDEN_PROVIDER_IMPORTS
    assert not hit, f"outcome local reader imports forbidden provider names: {hit}"


def test_incremental_refresh_handler_only_registered_in_history_warmup_admin_routes():
    """The incremental-execute route (the ONLY place that calls
    _resolve_history_warmup_provider for this mode) lives exclusively in
    app.routers.admin, gated behind _require_history_warmup_mode — never
    exposed under PROSPECTIVE_CAMPAIGN_ONLY_MODE or the outcome-worker
    handler modules."""
    import ast
    import inspect
    import app.routers.admin as admin_module
    tree = ast.parse(inspect.getsource(admin_module))
    incremental_routes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            for dec in node.decorator_list:
                src = ast.unparse(dec) if hasattr(ast, "unparse") else ""
                if "incremental" in src:
                    incremental_routes.append(node.name)
    assert "history_warmup_incremental_preflight" in incremental_routes
    assert "history_warmup_incremental_execute" in incremental_routes
    # neither handler exists in the prospective outcome/evaluation modules
    import app.jobs.handlers.prospective_outcome as oh
    import app.jobs.handlers.prospective as ph
    assert not hasattr(oh, "history_warmup_incremental_execute")
    assert not hasattr(ph, "history_warmup_incremental_execute")
