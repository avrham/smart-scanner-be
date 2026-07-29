"""Fast, DB-free unit tests for the durable job-queue framework:
backoff, deterministic idempotency keys, typed payload, handler registry, the
minimal cron + market_daily occurrence math, and the prospective-mode allowlist.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from app.jobs import contracts as C
from app.jobs import identity as ident
from app.jobs.contracts import ProspectiveSymbolPayload, TerminalJobError


# --- backoff ---------------------------------------------------------------
def test_backoff_schedule_is_bounded_then_terminal():
    assert C.backoff_seconds(1, schedule=[60, 300]) == 60
    assert C.backoff_seconds(2, schedule=[60, 300]) == 300
    assert C.backoff_seconds(3, schedule=[60, 300]) is None
    assert C.backoff_seconds(0, schedule=[60, 300]) is None


def test_backoff_jitter_is_opt_in_and_bounded():
    assert C.backoff_seconds(1, schedule=[60, 300], jitter_seconds=5) == 65
    assert C.backoff_seconds(1, schedule=[60, 300], jitter_seconds=0) == 60


# --- deterministic idempotency keys ----------------------------------------
def test_job_key_is_deterministic_and_scoped():
    a = ident.job_idempotency_key(job_type="prospective_campaign",
                                  registration_identity="pcr:abc",
                                  campaign_execution_identity="pcx:def")
    b = ident.job_idempotency_key(job_type="prospective_campaign",
                                  registration_identity="pcr:abc",
                                  campaign_execution_identity="pcx:def")
    c = ident.job_idempotency_key(job_type="prospective_campaign",
                                  registration_identity="pcr:abc",
                                  campaign_execution_identity="pcx:zzz")
    assert a == b and a != c and a.startswith("job:")


def test_task_key_binds_symbol_and_identities():
    base = dict(registration_identity="pcr:abc", campaign_execution_identity="pcx:def",
                snapshot_session_date="2026-07-29", snapshot_cutoff_at="2026-07-29T20:00:00+00:00",
                candidate_strategy_identity="wyckoff_mtf_v2:wyckoff_mtf.v2:pre_rollout_enter_eligible.v1:allow_enter=false",
                control_strategy_identity="sma150_bounce:sma150.v2")
    k_aapl = ident.prospective_task_idempotency_key(symbol="AAPL", **base)
    k_msft = ident.prospective_task_idempotency_key(symbol="MSFT", **base)
    k_aapl2 = ident.prospective_task_idempotency_key(symbol="aapl", **base)
    assert k_aapl != k_msft
    assert k_aapl == k_aapl2  # symbol normalized to upper
    assert k_aapl.startswith("task:")


def test_schedule_occurrence_key_is_unique_per_occurrence():
    a = ident.schedule_occurrence_idempotency_key(schedule_code="X", schedule_version=1,
                                                  occurrence_iso="2026-07-29T20:30:00+00:00")
    b = ident.schedule_occurrence_idempotency_key(schedule_code="X", schedule_version=1,
                                                  occurrence_iso="2026-07-30T20:30:00+00:00")
    assert a != b


# --- typed payload ----------------------------------------------------------
def _payload_dict():
    return {
        "registration_id": "11111111-1111-1111-1111-111111111111",
        "registration_identity": "pcr:abc",
        "universe_id": "22222222-2222-2222-2222-222222222222",
        "universe_hash": "sha256:aaa",
        "history_readiness_manifest_hash": "sha256:bbb",
        "snapshot_session_date": "2026-07-29",
        "snapshot_cutoff_at": "2026-07-29T20:00:00+00:00",
        "symbol": "aapl",
        "ordinal": 0,
        "candidate_strategy_identity": "wyckoff_mtf_v2:wyckoff_mtf.v2:pre_rollout_enter_eligible.v1:allow_enter=false",
        "control_strategy_identity": "sma150_bounce:sma150.v2",
        "candidate_signal_definition": "pre_rollout_enter_eligible.v1",
    }


def test_payload_roundtrip_and_symbol_upper():
    p = ProspectiveSymbolPayload.from_dict(_payload_dict())
    assert p.symbol == "AAPL"
    assert p.ordinal == 0
    assert p.to_dict()["symbol"] == "AAPL"


def test_payload_missing_field_is_terminal():
    d = _payload_dict()
    del d["symbol"]
    with pytest.raises(TerminalJobError):
        ProspectiveSymbolPayload.from_dict(d)


# --- registry ---------------------------------------------------------------
def test_registry_resolves_prospective_and_rejects_unknown():
    from app.jobs import registry as R
    spec = R.resolve_handler(C.PROSPECTIVE_SYMBOL_EVALUATION_TASK)
    assert spec.production_enabled and spec.queue_name == "prospective"
    with pytest.raises(R.UnknownTaskType):
        R.resolve_handler("does_not_exist.v1")


def test_registry_rejects_test_handler_by_default(monkeypatch):
    from app.jobs import registry as R
    from app.jobs.handlers.synthetic import SYNTHETIC_TASK_TYPE
    from app.config import settings
    monkeypatch.setattr(settings, "JOB_ALLOW_TEST_HANDLERS", False, raising=False)
    with pytest.raises(TerminalJobError):
        R.resolve_handler(SYNTHETIC_TASK_TYPE)
    monkeypatch.setattr(settings, "JOB_ALLOW_TEST_HANDLERS", True, raising=False)
    assert R.resolve_handler(SYNTHETIC_TASK_TYPE).is_test_handler


# --- cron -------------------------------------------------------------------
def test_cron_parse_and_next_occurrence_in_tz():
    from app.jobs.scheduler import next_cron_occurrence
    after = datetime(2026, 7, 29, 0, 0, tzinfo=timezone.utc)  # a Wednesday
    nxt = next_cron_occurrence("30 14 * * *", after, "America/New_York")
    local = nxt.astimezone(ZoneInfo("America/New_York"))
    assert (local.hour, local.minute) == (14, 30)
    assert nxt > after


def test_cron_rejects_bad_expression():
    from app.jobs.scheduler import parse_cron
    with pytest.raises(ValueError):
        parse_cron("not a cron")
    with pytest.raises(ValueError):
        parse_cron("99 14 * * *")


# --- market_daily -----------------------------------------------------------
def test_market_daily_occurrence_is_future_trading_session():
    from app.jobs.scheduler import next_market_daily_occurrence
    from app.prospective_session import is_trading_day, session_cutoff_utc
    # a Saturday afternoon → next occurrence must land on a real trading day
    after = datetime(2026, 7, 25, 18, 0, tzinfo=timezone.utc)
    occ = next_market_daily_occurrence(after, delay_minutes=30)
    assert occ > after
    # occ == some session close (16:00 ET) + 30m; recover the session date
    session = (occ).astimezone(ZoneInfo("America/New_York")).date()
    assert is_trading_day(session)
    assert occ == session_cutoff_utc(session).astimezone(timezone.utc) + \
        __import__("datetime").timedelta(minutes=30)


def test_market_daily_skips_holiday():
    from app.jobs.scheduler import next_market_daily_occurrence
    from app.prospective_session import is_trading_day
    # July 3 2026 is the observed Independence Day holiday (July 4 is a Saturday).
    after = datetime(2026, 7, 2, 22, 0, tzinfo=timezone.utc)  # after Thu close
    occ = next_market_daily_occurrence(after, delay_minutes=0)
    session = occ.astimezone(ZoneInfo("America/New_York")).date()
    assert is_trading_day(session)
    assert session.isoformat() != "2026-07-03"  # never the holiday


# --- prospective-mode allowlist ---------------------------------------------
def test_prospective_allowlist_covers_queue_routes():
    from app.prospective_mode import is_prospective_route_allowed as ok
    assert ok("POST", "/api/admin/prospective/jobs")
    assert ok("GET", "/api/admin/jobs")
    assert ok("GET", "/api/admin/jobs/workers")
    assert ok("GET", "/api/admin/jobs/abc-123")
    assert ok("GET", "/api/admin/jobs/abc-123/tasks")
    assert ok("GET", "/api/admin/jobs/abc-123/events")
    assert ok("POST", "/api/admin/jobs/abc-123/cancel")
    assert ok("POST", "/api/admin/jobs/abc-123/retry-failed")
    assert ok("GET", "/api/admin/job-schedules")
    assert ok("POST", "/api/admin/job-schedules")
    assert ok("PATCH", "/api/admin/job-schedules/abc-123")
    assert ok("POST", "/api/admin/job-schedules/abc-123/pause")
    assert ok("POST", "/api/admin/job-schedules/abc-123/resume")
    assert ok("GET", "/api/admin/job-schedules/abc-123/preview")


def test_prospective_allowlist_denies_other_routes():
    from app.prospective_mode import is_prospective_route_allowed as ok
    assert not ok("POST", "/api/admin/scan")
    assert not ok("GET", "/api/admin/jobs/abc/cancel")   # GET on a POST-only suffix
    assert not ok("DELETE", "/api/admin/jobs/abc")
    assert not ok("POST", "/api/admin/jobs")             # list is GET-only
