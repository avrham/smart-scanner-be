"""Phase 6 — candidate signals query builder tests (no DB).

Endpoint validation tests override the get_db dependency so no DB connection
is ever attempted.
"""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app.deps import get_db
from app.routers.public import VALID_VERDICTS, build_signals_query
from main import app


async def _no_db():
    yield None


@pytest.fixture
def client():
    app.dependency_overrides[get_db] = _no_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_default_is_enter_only_backward_compatible():
    query, params = build_signals_query()
    assert "s.verdict = $1" in query
    assert params[0] == "ENTER"
    assert params[-1] == 50  # limit last


def test_watch_filter():
    query, params = build_signals_query(verdict="WATCH")
    assert params[0] == "WATCH"


def test_all_means_enter_plus_watch_never_avoid():
    query, params = build_signals_query(verdict="ALL")
    assert "s.verdict IN ('ENTER', 'WATCH')" in query
    assert "AVOID" not in query
    assert params == [50]  # only limit


def test_all_filters_combined_ordered_params():
    since = datetime(2026, 1, 1)
    query, params = build_signals_query(
        verdict="ALL",
        pattern_code="wyckoff_mtf",
        side="LONG",
        min_score=0.6,
        since=since,
        limit=25,
    )
    assert params == ["wyckoff_mtf", "LONG", 0.6, since, 25]
    assert "s.pattern_code = $1" in query
    assert "s.details->>'side' = $2" in query
    assert "s.score >= $3" in query
    assert "s.created_at >= $4" in query
    assert "LIMIT $5" in query


def test_side_filter_uses_details_json():
    query, params = build_signals_query(side="SHORT")
    assert "s.details->>'side' = $2" in query
    assert params == ["ENTER", "SHORT", 50]


def test_invalid_verdict_rejected_by_endpoint(client):
    resp = client.get("/api/signals", params={"verdict": "BOGUS"})
    assert resp.status_code == 400
    assert "Invalid verdict" in resp.json()["detail"]


def test_invalid_side_rejected_by_endpoint(client):
    resp = client.get("/api/signals", params={"side": "SIDEWAYS"})
    assert resp.status_code == 400
    assert "Invalid side" in resp.json()["detail"]


def test_valid_verdicts_constant():
    assert VALID_VERDICTS == {"ENTER", "WATCH", "ALL"}
