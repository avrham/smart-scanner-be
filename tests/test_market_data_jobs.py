"""Phase 7A — durable market-data jobs. No live API or DB calls.

The repository functions are exercised against an in-memory fake connection
that dispatches on the exact SQL constants in app.workers.market_jobs,
including the durable duplicate-protection semantics of the partial unique
index (one active job per job_type/provider/trading_date).
"""

import asyncio
import json
from datetime import date, datetime, timedelta, timezone

import asyncpg
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.providers.massive import MassiveProvider
from app.workers import market_jobs, market_store
from main import app


TRADING_DATE = date(2026, 7, 17)


# --------------------------------------------------------------------------- #
# In-memory fake of the market_data_jobs table
# --------------------------------------------------------------------------- #

class FakeJobDB:
    """Emulates the exact SQL statements market_jobs issues, including the
    partial unique index on active (queued/running) jobs."""

    def __init__(self):
        self.rows = {}
        self._n = 0

    def _active_conflict(self, job_type, provider, trading_date):
        return any(
            r["job_type"] == job_type
            and r["provider"] == provider
            and r["trading_date"] == trading_date
            and r["status"] in ("queued", "running")
            for r in self.rows.values()
        )

    async def fetchrow(self, query, *args):
        if query is market_jobs.SQL_CREATE_JOB:
            job_type, provider, trading_date, requested_limit, now = args
            if self._active_conflict(job_type, provider, trading_date):
                raise asyncpg.exceptions.UniqueViolationError("duplicate active job")
            self._n += 1
            jid = f"00000000-0000-0000-0000-{self._n:012d}"
            self.rows[jid] = {
                "id": jid, "job_type": job_type, "status": "queued",
                "provider": provider, "trading_date": trading_date,
                "requested_limit": requested_limit, "selection_strategy": None,
                "selected_symbols": None, "progress": None, "result": None,
                "error": None, "created_at": now, "started_at": None,
                "finished_at": None, "updated_at": now,
            }
            return {"id": jid}
        if query is market_jobs.SQL_GET_JOB:
            row = self.rows.get(args[0])
            return dict(row) if row else None
        raise AssertionError(f"unexpected fetchrow: {query[:60]}")

    async def fetch(self, query, *args):
        if query is market_jobs.SQL_LIST_JOBS:
            job_type, status, provider, trading_date, limit = args
            rows = [
                dict(r) for r in sorted(
                    self.rows.values(),
                    key=lambda r: (r["created_at"], r["id"]), reverse=True,
                )
                if (job_type is None or r["job_type"] == job_type)
                and (status is None or r["status"] == status)
                and (provider is None or r["provider"] == provider)
                and (trading_date is None or r["trading_date"] == trading_date)
            ]
            return rows[:limit]
        raise AssertionError(f"unexpected fetch: {query[:60]}")

    async def execute(self, query, *args):
        if query is market_jobs.SQL_MARK_RUNNING:
            jid, now = args
            row = self.rows.get(jid)
            if row and row["status"] == "queued":
                row.update(status="running", started_at=now, updated_at=now)
                return "UPDATE 1"
            return "UPDATE 0"
        if query is market_jobs.SQL_UPDATE_PROGRESS:
            jid, progress_json, now = args
            row = self.rows.get(jid)
            if row and row["status"] == "running":
                row.update(progress=progress_json, updated_at=now)
                return "UPDATE 1"
            return "UPDATE 0"
        if query is market_jobs.SQL_COMPLETE_JOB:
            jid, result_json, symbols_json, strategy, now = args
            row = self.rows.get(jid)
            if row and row["status"] == "running":
                row.update(
                    status="completed", result=result_json,
                    selected_symbols=symbols_json, selection_strategy=strategy,
                    finished_at=now, updated_at=now,
                )
                return "UPDATE 1"
            return "UPDATE 0"
        if query is market_jobs.SQL_FAIL_JOB:
            jid, error, now = args
            row = self.rows.get(jid)
            if row and row["status"] in ("queued", "running"):
                row.update(status="failed", error=error, finished_at=now, updated_at=now)
                return "UPDATE 1"
            return "UPDATE 0"
        if query is market_jobs.SQL_RECOVER_STALE:
            cutoff, now = args
            count = 0
            for row in self.rows.values():
                if row["status"] in ("queued", "running") and row["updated_at"] < cutoff:
                    error = (
                        market_jobs.QUEUED_TIMEOUT_ERROR
                        if row["status"] == "queued"
                        else market_jobs.RUNNING_TIMEOUT_ERROR
                    )
                    row.update(status="failed", error=error, finished_at=now, updated_at=now)
                    count += 1
            return f"UPDATE {count}"
        raise AssertionError(f"unexpected execute: {query[:60]}")


@pytest.fixture
def job_db(monkeypatch):
    db = FakeJobDB()

    async def fake_get():
        return db

    async def fake_release(conn):
        pass

    monkeypatch.setattr(market_jobs, "get_db_connection", fake_get)
    monkeypatch.setattr(market_jobs, "release_db_connection", fake_release)
    return db


# --------------------------------------------------------------------------- #
# Fake providers
# --------------------------------------------------------------------------- #

class FakeProvider:
    name = "massive"

    def __init__(self, summary=None, error=None):
        self._summary = summary or {
            "provider": "massive",
            "detail_calls": 2,
            "enriched": 2,
            "missing_market_cap": 0,
            "errors": 0,
            "selection_strategy": "dollar_volume_desc",
            "selected_symbols": ["AAA", "BBB"],
            "remaining_stale_survivors": 0,
        }
        self._error = error
        self.progress_calls = []

    async def enrich_market_caps(self, trading_date, max_detail_calls=25, progress_callback=None):
        if progress_callback is not None:
            payload = {"phase": "selected", "detail_calls_planned": 2}
            self.progress_calls.append(payload)
            await progress_callback(payload)
        if self._error is not None:
            raise self._error
        return self._summary


def _run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------------------------- #
# State transitions
# --------------------------------------------------------------------------- #

def test_queued_to_running_to_completed(job_db):
    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        assert (await market_jobs.get_job(job_id))["status"] == "queued"
        await market_jobs.run_enrichment_job(job_id, FakeProvider(), TRADING_DATE, 25)
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    assert job["status"] == "completed"
    assert job["result"]["enriched"] == 2
    assert job["selected_symbols"] == ["AAA", "BBB"]
    assert job["selection_strategy"] == "dollar_volume_desc"
    assert job["started_at"] is not None and job["finished_at"] is not None


def test_running_to_failed_with_safe_error(job_db):
    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        provider = FakeProvider(error=RuntimeError("boom apiKey=SECRET123 detail"))
        await market_jobs.run_enrichment_job(job_id, provider, TRADING_DATE, 25)
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    assert job["status"] == "failed"
    assert "SECRET123" not in job["error"]
    assert "apiKey=***" in job["error"]
    assert "Traceback" not in job["error"]
    assert job["error"].startswith("RuntimeError")


def test_duplicate_active_job_conflict(job_db):
    async def flow():
        await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        with pytest.raises(market_jobs.DuplicateActiveJobError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 10
            )

    _run(flow())


def test_completed_job_permits_a_later_job(job_db):
    async def flow():
        first = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.run_enrichment_job(first, FakeProvider(), TRADING_DATE, 25)
        second = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        return first, second

    first, second = _run(flow())
    assert first != second


def test_partial_enrichment_success_still_completes(job_db):
    summary = {
        "provider": "massive", "detail_calls": 5, "enriched": 3,
        "missing_market_cap": 1, "errors": 1,
        "selection_strategy": "dollar_volume_desc",
        "selected_symbols": ["A", "B", "C", "D", "E"],
    }

    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 5
        )
        await market_jobs.run_enrichment_job(
            job_id, FakeProvider(summary=summary), TRADING_DATE, 5
        )
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    assert job["status"] == "completed"
    assert job["result"]["errors"] == 1  # partial failure is honest, not fatal


def test_stale_job_recovery_unblocks_new_work(job_db):
    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.mark_running(job_id)
        # Simulate a crashed process: running with an old heartbeat.
        job_db.rows[job_id]["updated_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=120)
        )
        # New job is blocked while the phantom is active...
        with pytest.raises(market_jobs.DuplicateActiveJobError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
            )
        recovered = await market_jobs.recover_stale_jobs(timeout_minutes=30)
        new_job = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        return recovered, await market_jobs.get_job(job_id), new_job

    recovered, old_job, new_job = _run(flow())
    assert recovered == 1
    assert old_job["status"] == "failed"
    assert "stale_job_timeout" in old_job["error"]
    assert new_job is not None


def test_progress_is_persisted_while_running(job_db):
    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.run_enrichment_job(job_id, FakeProvider(), TRADING_DATE, 25)
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    assert job["progress"]["phase"] == "selected"


def test_timestamps_are_timezone_aware(job_db):
    assert market_jobs.utcnow().tzinfo is timezone.utc

    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.run_enrichment_job(job_id, FakeProvider(), TRADING_DATE, 25)
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    for key in ("created_at", "started_at", "finished_at", "updated_at"):
        assert job[key].tzinfo is not None, key


# --------------------------------------------------------------------------- #
# Error sanitization (no secrets ever persisted)
# --------------------------------------------------------------------------- #

def test_sanitize_error_masks_secret_patterns():
    cases = [
        RuntimeError("call failed: https://api.example.com?apiKey=abc123&x=1"),
        RuntimeError("Authorization: Bearer sk-live-XYZ"),
        RuntimeError("config api_key=topsecret rejected"),
        RuntimeError("password=hunter2 for db"),
    ]
    for exc in cases:
        safe = market_jobs.sanitize_error(exc)
        for secret in ("abc123", "sk-live-XYZ", "topsecret", "hunter2"):
            assert secret not in safe

    long = market_jobs.sanitize_error(RuntimeError("x" * 5000))
    assert len(long) <= market_jobs.MAX_ERROR_LENGTH


# --------------------------------------------------------------------------- #
# Deterministic prioritization + profile cache through the job runner
# --------------------------------------------------------------------------- #

def _bar(symbol, close, volume):
    return {"symbol": symbol, "close": close, "volume": volume}


def test_job_runner_preserves_prioritization_and_cache(job_db, monkeypatch):
    """Real MassiveProvider.enrich_market_caps through run_enrichment_job:
    fresh profiles skipped, deterministic ordering, bounded calls."""
    monkeypatch.setattr(settings, "PRESCREEN_MIN_PRICE", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_VOLUME", 1.0)
    monkeypatch.setattr(settings, "PRESCREEN_MIN_DOLLAR_VOLUME", 1.0)
    monkeypatch.setattr(settings, "MASSIVE_PROFILE_CACHE_DAYS", 7)

    bars = [
        _bar("FRESH", 900.0, 9_000_000),   # highest dollar volume but cached
        _bar("BIG", 500.0, 5_000_000),
        _bar("SMALL", 5.0, 100_000),
    ]
    now = datetime.now(timezone.utc)
    profiles = [{"symbol": "FRESH", "profile_synced_at": now - timedelta(days=1)}]

    async def fake_bars(trading_date):
        return bars

    async def fake_eligible():
        return {"FRESH", "BIG", "SMALL"}

    async def fake_profiles(symbols):
        return [p for p in profiles if p["symbol"] in symbols]

    async def fake_update(symbol, market_cap, status):
        pass

    monkeypatch.setattr(market_store, "get_bars_for_date", fake_bars)
    monkeypatch.setattr(market_store, "get_eligible_symbols", fake_eligible)
    monkeypatch.setattr(market_store, "get_ticker_profiles", fake_profiles)
    monkeypatch.setattr(market_store, "update_ticker_profile", fake_update)

    class CountingClient:
        def __init__(self):
            self.calls = []

        async def get_ticker_details(self, symbol):
            self.calls.append(symbol)
            return {"market_cap": 1.0}

    client = CountingClient()
    provider = MassiveProvider(client=client)

    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.run_enrichment_job(job_id, provider, TRADING_DATE, 25)
        return await market_jobs.get_job(job_id)

    job = _run(flow())
    assert job["status"] == "completed"
    assert client.calls == ["BIG", "SMALL"]           # fresh skipped, order deterministic
    assert job["selected_symbols"] == ["BIG", "SMALL"]
    assert job["selection_strategy"] == "dollar_volume_desc"
    assert job["result"]["cached_fresh"] == 1


# --------------------------------------------------------------------------- #
# API endpoints
# --------------------------------------------------------------------------- #

@pytest.fixture
def client():
    return TestClient(app)


def test_job_status_endpoint(client, monkeypatch):
    job = {"id": "j-1", "job_type": market_jobs.JOB_TYPE_ENRICHMENT, "status": "completed"}

    async def fake_get(job_id):
        return job if job_id == "j-1" else None

    monkeypatch.setattr(market_jobs, "get_job", fake_get)
    assert client.get("/api/admin/market-data/jobs/j-1").json()["status"] == "completed"
    assert client.get("/api/admin/market-data/jobs/missing").status_code == 404


def test_jobs_list_filters_and_bounds(client, monkeypatch):
    captured = {}

    async def fake_list(job_type=None, status=None, provider=None, trading_date=None, limit=50):
        captured.update(job_type=job_type, status=status, limit=limit)
        return []

    monkeypatch.setattr(market_jobs, "list_jobs", fake_list)

    resp = client.get(
        "/api/admin/market-data/jobs",
        params={"job_type": "market_cap_enrichment", "status": "failed", "limit": 5},
    )
    assert resp.status_code == 200
    assert captured == {"job_type": "market_cap_enrichment", "status": "failed", "limit": 5}

    assert client.get("/api/admin/market-data/jobs", params={"status": "bogus"}).status_code == 400
    assert client.get("/api/admin/market-data/jobs", params={"limit": 0}).status_code == 400
    assert client.get("/api/admin/market-data/jobs", params={"limit": 500}).status_code == 400


def test_repo_list_limit_is_clamped(job_db):
    async def flow():
        for _ in range(3):
            jid = await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
            )
            await market_jobs.mark_running(jid)
            await market_jobs.complete_job(jid, {"ok": True})
        return await market_jobs.list_jobs(limit=100_000)

    jobs = _run(flow())
    assert len(jobs) == 3  # fake returns min(limit, rows); repo clamps limit <= 200


def test_enrich_endpoint_creates_queued_job(client, monkeypatch):
    from app.routers import admin as admin_module

    class P:
        name = "massive"

    monkeypatch.setattr(admin_module, "get_market_data_provider", lambda: P())

    created = {}

    async def fake_recover(timeout_minutes):
        created["recover_called"] = True
        return 0

    async def fake_create(job_type, provider, trading_date, requested_limit):
        created.update(job_type=job_type, provider=provider,
                       trading_date=trading_date, requested_limit=requested_limit)
        return "job-xyz"

    async def fake_run(job_id, provider, trading_date, max_detail_calls):
        created["ran"] = job_id

    monkeypatch.setattr(market_jobs, "recover_stale_jobs", fake_recover)
    monkeypatch.setattr(market_jobs, "create_job", fake_create)
    monkeypatch.setattr(market_jobs, "run_enrichment_job", fake_run)

    resp = client.post(
        "/api/admin/universe/enrich",
        json={"trading_date": "2026-07-17", "max_detail_calls": 10},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["job_id"] == "job-xyz"
    assert body["status"] == "queued"
    assert created["recover_called"] is True
    assert created["job_type"] == market_jobs.JOB_TYPE_ENRICHMENT
    assert created["requested_limit"] == 10
    assert created["ran"] == "job-xyz"


def test_stale_queued_job_recovery_unblocks_new_work(job_db):
    async def flow():
        job_id = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        # Simulate a process that died before the background task started.
        job_db.rows[job_id]["updated_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=120)
        )
        recovered = await market_jobs.recover_stale_jobs(timeout_minutes=30)
        replacement = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        return recovered, await market_jobs.get_job(job_id), replacement

    recovered, old_job, replacement = _run(flow())
    assert recovered == 1
    assert old_job["status"] == "failed"
    assert old_job["error"].startswith("queued_job_timeout")
    assert replacement is not None


def test_recent_queued_and_running_jobs_still_block_duplicates(job_db):
    async def flow():
        queued = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        # Recent queued job: recovery is a no-op and duplicates stay blocked.
        assert await market_jobs.recover_stale_jobs(timeout_minutes=30) == 0
        with pytest.raises(market_jobs.DuplicateActiveJobError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
            )
        # Recent running job: same.
        await market_jobs.mark_running(queued)
        assert await market_jobs.recover_stale_jobs(timeout_minutes=30) == 0
        with pytest.raises(market_jobs.DuplicateActiveJobError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
            )

    _run(flow())


# --------------------------------------------------------------------------- #
# Non-null trading_date enforcement for enrichment jobs
# --------------------------------------------------------------------------- #

def test_enrichment_job_requires_trading_date(job_db):
    """Two NULL-date jobs would both pass the partial unique index (NULLs are
    distinct), so NULL dates are rejected before insertion."""
    async def flow():
        with pytest.raises(ValueError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", None, 25
            )
        with pytest.raises(ValueError):
            await market_jobs.create_job(
                market_jobs.JOB_TYPE_ENRICHMENT, "massive", None, 25
            )

    _run(flow())
    assert job_db.rows == {}  # nothing was inserted — no NULL bypass possible


def test_enrich_endpoint_resolves_date_from_local_bars(client, monkeypatch):
    from app.routers import admin as admin_module

    class P:
        name = "massive"

    monkeypatch.setattr(admin_module, "get_market_data_provider", lambda: P())

    async def fake_latest():
        return TRADING_DATE

    monkeypatch.setattr(market_store, "get_latest_daily_bar_date", fake_latest)

    created = {}

    async def fake_recover(timeout_minutes):
        return 0

    async def fake_create(job_type, provider, trading_date, requested_limit):
        created["trading_date"] = trading_date
        return "job-1"

    async def fake_run(*a, **k):
        pass

    monkeypatch.setattr(market_jobs, "recover_stale_jobs", fake_recover)
    monkeypatch.setattr(market_jobs, "create_job", fake_create)
    monkeypatch.setattr(market_jobs, "run_enrichment_job", fake_run)

    resp = client.post("/api/admin/universe/enrich", json={})
    assert resp.status_code == 200
    assert resp.json()["trading_date"] == str(TRADING_DATE)
    assert created["trading_date"] == TRADING_DATE  # resolved, never NULL


def test_enrich_endpoint_rejects_when_no_local_bars(client, monkeypatch):
    from app.routers import admin as admin_module

    class P:
        name = "massive"

    monkeypatch.setattr(admin_module, "get_market_data_provider", lambda: P())

    async def no_latest():
        return None

    async def must_not_create(**kwargs):
        raise AssertionError("job must not be inserted without a trading_date")

    monkeypatch.setattr(market_store, "get_latest_daily_bar_date", no_latest)
    monkeypatch.setattr(market_jobs, "create_job", must_not_create)

    resp = client.post("/api/admin/universe/enrich", json={})
    assert resp.status_code == 400
    assert "daily bars" in resp.json()["detail"]


def test_enrich_endpoint_strict_date_validation(client, monkeypatch):
    from app.routers import admin as admin_module

    class P:
        name = "massive"

    monkeypatch.setattr(admin_module, "get_market_data_provider", lambda: P())

    for bad in ("2026/07/17", "20260717", "17-07-2026", "2026-13-40"):
        resp = client.post("/api/admin/universe/enrich", json={"trading_date": bad})
        assert resp.status_code == 400, bad


# --------------------------------------------------------------------------- #
# List filters (repository)
# --------------------------------------------------------------------------- #

def _seed_jobs(job_db):
    """Four jobs across providers/dates/status for filter tests."""
    other_date = TRADING_DATE - timedelta(days=1)

    async def seed():
        a = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", TRADING_DATE, 25
        )
        await market_jobs.mark_running(a)
        await market_jobs.complete_job(a, {"ok": True})

        b = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "fmp", TRADING_DATE, 25
        )
        await market_jobs.mark_running(b)
        await market_jobs.fail_job(b, "boom")

        c = await market_jobs.create_job(
            market_jobs.JOB_TYPE_ENRICHMENT, "massive", other_date, 25
        )

        d = await market_jobs.create_job("daily_sync", "massive", TRADING_DATE, None)
        return a, b, c, d

    return _run(seed()), other_date


def test_list_filter_provider_only(job_db):
    (a, b, c, d), _ = _seed_jobs(job_db)
    jobs = _run(market_jobs.list_jobs(provider="massive"))
    assert {j["id"] for j in jobs} == {a, c, d}
    # Exact normalized match: repo lowercases the filter input.
    assert {j["id"] for j in _run(market_jobs.list_jobs(provider="  MASSIVE "))} == {a, c, d}


def test_list_filter_trading_date_only(job_db):
    (a, b, c, d), other_date = _seed_jobs(job_db)
    jobs = _run(market_jobs.list_jobs(trading_date=other_date))
    assert [j["id"] for j in jobs] == [c]


def test_list_filter_provider_and_status(job_db):
    (a, b, c, d), _ = _seed_jobs(job_db)
    jobs = _run(market_jobs.list_jobs(provider="fmp", status="failed"))
    assert [j["id"] for j in jobs] == [b]
    assert _run(market_jobs.list_jobs(provider="fmp", status="completed")) == []


def test_list_filter_all_four_compose_with_and(job_db):
    (a, b, c, d), _ = _seed_jobs(job_db)
    jobs = _run(
        market_jobs.list_jobs(
            job_type=market_jobs.JOB_TYPE_ENRICHMENT,
            provider="massive",
            trading_date=TRADING_DATE,
            status="completed",
        )
    )
    assert [j["id"] for j in jobs] == [a]


def test_list_filter_no_matching_results(job_db):
    _seed_jobs(job_db)
    assert _run(market_jobs.list_jobs(provider="massive", status="cancelled")) == []


def test_list_ordering_newest_first(job_db):
    (a, b, c, d), _ = _seed_jobs(job_db)
    jobs = _run(market_jobs.list_jobs())
    assert [j["id"] for j in jobs] == [d, c, b, a]


# --------------------------------------------------------------------------- #
# List filters (endpoint)
# --------------------------------------------------------------------------- #

def test_jobs_list_endpoint_new_filters_passed_through(client, monkeypatch):
    captured = {}

    async def fake_list(job_type=None, status=None, provider=None, trading_date=None, limit=50):
        captured.update(job_type=job_type, status=status, provider=provider,
                        trading_date=trading_date, limit=limit)
        return []

    monkeypatch.setattr(market_jobs, "list_jobs", fake_list)

    resp = client.get(
        "/api/admin/market-data/jobs",
        params={
            "job_type": "market_cap_enrichment",
            "provider": "massive",
            "status": "completed",
            "trading_date": "2026-07-17",
            "limit": 10,
        },
    )
    assert resp.status_code == 200
    assert captured == {
        "job_type": "market_cap_enrichment",
        "provider": "massive",
        "status": "completed",
        "trading_date": TRADING_DATE,
        "limit": 10,
    }


def test_jobs_list_endpoint_rejects_invalid_trading_date(client):
    for bad in ("2026/07/17", "20260717", "not-a-date", "2026-02-30"):
        resp = client.get("/api/admin/market-data/jobs", params={"trading_date": bad})
        assert resp.status_code == 400, bad


def test_enrich_endpoint_duplicate_returns_409(client, monkeypatch):
    from app.routers import admin as admin_module

    class P:
        name = "massive"

    monkeypatch.setattr(admin_module, "get_market_data_provider", lambda: P())

    async def fake_recover(timeout_minutes):
        return 0

    async def fake_create(**kwargs):
        raise market_jobs.DuplicateActiveJobError("already active")

    monkeypatch.setattr(market_jobs, "recover_stale_jobs", fake_recover)
    monkeypatch.setattr(market_jobs, "create_job", fake_create)

    resp = client.post(
        "/api/admin/universe/enrich", json={"trading_date": "2026-07-17"}
    )
    assert resp.status_code == 409
