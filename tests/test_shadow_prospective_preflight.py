"""Prospective-campaign preflight (shadow_prospective_preflight.v1).

Proves safe completed-session resolution (partial latest bar steps back to the
prior real session; unknown refuses) and duplicate-campaign classification, at
both the pure layer and the read-only HTTP boundary — including worker-token
enforcement.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_worker_token
import app.routers.admin as admin_mod

from app.workers.shadow.prospective_preflight import (
    MATCH_COMPLETED,
    MATCH_MEMBERSHIP_MISMATCH,
    MATCH_MEMBERSHIP_UNVERIFIABLE,
    MATCH_NONE,
    MATCH_RESUMABLE,
    SESSION_RESOLVED_LATEST_COMPLETED,
    SESSION_RESOLVED_PRIOR,
    SESSION_UNRESOLVED,
    classify_existing_campaigns,
    resolve_latest_completed_session,
)
from app.workers.shadow.universe_identity import (
    compute_universe_hash,
    normalize_campaign_symbols,
)


CAL = [date(2026, 7, 20), date(2026, 7, 21), date(2026, 7, 22),
       date(2026, 7, 23), date(2026, 7, 24)]


def _hash(symbols: List[str]) -> str:
    return compute_universe_hash(normalize_campaign_symbols(symbols))


def _run(cid: str, aod: str, syms: List[str], status: str = "completed") -> Dict[str, Any]:
    return {
        "experiment_code": "wyckoff_v2_vs_baseline",
        "status": status,
        "requested_symbols": syms,
        "campaign": {"campaign_id": cid, "as_of_date": aod},
    }


class TestSessionResolution:
    def test_completed_latest_used_directly(self):
        r = resolve_latest_completed_session(
            latest_bar_date=date(2026, 7, 24),
            latest_bar_completion_state="completed",
            reference_session_dates=CAL,
        )
        assert r["resolved_session"] == "2026-07-24"
        assert r["resolution_reason"] == SESSION_RESOLVED_LATEST_COMPLETED
        assert r["is_valid_trading_session"] is True

    def test_partial_latest_steps_back_to_prior_session(self):
        r = resolve_latest_completed_session(
            latest_bar_date=date(2026, 7, 24),
            latest_bar_completion_state="partial",
            reference_session_dates=CAL,
        )
        # NOT 2026-07-23-minus-one-calendar-day — the prior real session.
        assert r["resolved_session"] == "2026-07-23"
        assert r["resolution_reason"] == SESSION_RESOLVED_PRIOR

    def test_unknown_latest_refuses(self):
        r = resolve_latest_completed_session(
            latest_bar_date=date(2026, 7, 24),
            latest_bar_completion_state="unknown",
            reference_session_dates=CAL,
        )
        assert r["resolved_session"] is None
        assert r["resolution_reason"] == SESSION_UNRESOLVED

    def test_no_bars_refuses(self):
        r = resolve_latest_completed_session(
            latest_bar_date=None, latest_bar_completion_state="unknown",
            reference_session_dates=[],
        )
        assert r["resolved_session"] is None


class TestCampaignClassification:
    def test_no_match_is_safe(self):
        r = classify_existing_campaigns(
            [], experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_NONE
        assert r["safe_to_create"] is True

    def test_completed_match_blocks_creation(self):
        r = classify_existing_campaigns(
            [_run("c1", "2026-07-24", ["AAPL", "MSFT"])],
            experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_COMPLETED
        assert r["safe_to_create"] is False
        assert r["campaign_id"] == "c1"

    def test_resumable_match(self):
        r = classify_existing_campaigns(
            [_run("c1", "2026-07-24", ["AAPL", "MSFT"], status="run_failed")],
            experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_RESUMABLE
        assert r["safe_to_create"] is False

    def test_same_session_different_membership_is_mismatch(self):
        r = classify_existing_campaigns(
            [_run("c1", "2026-07-24", ["AAPL", "NVDA"])],
            experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_MEMBERSHIP_MISMATCH
        assert r["safe_to_create"] is False

    def test_membership_unverifiable_is_not_safe(self):
        r = classify_existing_campaigns(
            [_run("c1", "2026-07-24", [])],
            experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_MEMBERSHIP_UNVERIFIABLE
        assert r["safe_to_create"] is False

    def test_different_session_ignored(self):
        r = classify_existing_campaigns(
            [_run("c1", "2026-07-23", ["AAPL", "MSFT"])],
            experiment_code="wyckoff_v2_vs_baseline",
            session_date="2026-07-24", universe_hash=_hash(["AAPL", "MSFT"]),
        )
        assert r["match"] == MATCH_NONE
        assert r["safe_to_create"] is True


class TestPreflightApi:
    @pytest.fixture
    def client(self):
        app.dependency_overrides[get_worker_token] = lambda: "test-token"
        try:
            yield TestClient(app, raise_server_exceptions=False)
        finally:
            app.dependency_overrides.pop(get_worker_token, None)

    def _patch_calendar(self, monkeypatch, *, latest, completion, reference,
                        campaigns):
        async def fake_inputs():
            return latest, completion, reference

        async def fake_runs(*, campaign_id=None, limit=100):
            return campaigns

        monkeypatch.setattr(
            admin_mod, "_latest_completed_session_inputs", fake_inputs
        )
        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_shadow_campaign_runs",
            fake_runs,
        )

    def test_safe_to_create(self, client, monkeypatch):
        self._patch_calendar(
            monkeypatch, latest=date(2026, 7, 24), completion="completed",
            reference=CAL, campaigns=[],
        )
        resp = client.get(
            "/api/admin/shadow-campaign-preflight",
            params={"symbols": "AAPL,MSFT", "expected_count": 2},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["resolved_session"] == "2026-07-24"
        assert body["creation_safe"] is True
        assert body["existing_campaign_match"] == MATCH_NONE

    def test_partial_session_and_existing_campaign_block(self, client, monkeypatch):
        self._patch_calendar(
            monkeypatch, latest=date(2026, 7, 24), completion="partial",
            reference=CAL,
            campaigns=[_run("c1", "2026-07-23", ["AAPL", "MSFT"])],
        )
        resp = client.get(
            "/api/admin/shadow-campaign-preflight",
            params={"symbols": "AAPL,MSFT", "expected_count": 2},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["resolved_session"] == "2026-07-23"
        assert body["creation_safe"] is False
        assert body["existing_campaign_match"] == MATCH_COMPLETED

    def test_symbols_required(self, client, monkeypatch):
        self._patch_calendar(
            monkeypatch, latest=date(2026, 7, 24), completion="completed",
            reference=CAL, campaigns=[],
        )
        resp = client.get(
            "/api/admin/shadow-campaign-preflight",
            params={"symbols": ""},
        )
        assert resp.status_code == 422

    def test_invalid_symbols_make_creation_unsafe(self, client, monkeypatch):
        self._patch_calendar(
            monkeypatch, latest=date(2026, 7, 24), completion="completed",
            reference=CAL, campaigns=[],
        )
        resp = client.get(
            "/api/admin/shadow-campaign-preflight",
            params={"symbols": "AAPL,bad!,MSFT", "expected_count": 3},
        )
        body = resp.json()
        assert body["creation_safe"] is False
        assert any(r.startswith("symbols:") for r in body["reasons"])


class TestPreflightAuthEnforced:
    """Token enforcement ON (REQUIRE_WORKER_TOKEN=true) for a new endpoint."""

    @pytest.fixture
    def client(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "unit-test-token")

        async def fake_inputs():
            return date(2026, 7, 24), "completed", CAL

        async def fake_runs(*, campaign_id=None, limit=100):
            return []

        monkeypatch.setattr(
            admin_mod, "_latest_completed_session_inputs", fake_inputs
        )
        monkeypatch.setattr(
            "app.workers.shadow.persistence.fetch_shadow_campaign_runs",
            fake_runs,
        )
        return TestClient(app, raise_server_exceptions=False)

    def _url(self):
        return ("/api/admin/shadow-campaign-preflight"
                "?symbols=AAPL,MSFT&expected_count=2")

    def test_missing_token_rejected(self, client):
        assert client.get(self._url()).status_code == 401

    def test_invalid_token_rejected(self, client):
        resp = client.get(self._url(), headers={"X-Worker-Token": "wrong"})
        assert resp.status_code == 401

    def test_valid_token_reaches_endpoint(self, client):
        resp = client.get(
            self._url(), headers={"X-Worker-Token": "unit-test-token"}
        )
        assert resp.status_code == 200
        assert resp.json()["creation_safe"] is True
