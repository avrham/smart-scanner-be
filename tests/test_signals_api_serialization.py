"""Signals API JSONB serialization (regression for the live 7B smoke failure).

asyncpg returns JSONB columns as raw JSON strings unless a codec is
configured, so signals.details arrived as '{"symbol": ...}' and failed
SignalResponse's Dict validation with ResponseValidationError. Both signal
endpoints must normalize details to a real dict (or None) and must reject
corrupted persisted data safely without leaking its contents.
"""

import json
import uuid
from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.deps import get_db
from app.models.responses import SignalResponse
from app.routers.public import normalize_json_object
from main import app


DETAILS = {"symbol": "JBL", "snapshot_date": "2026-07-20", "side": "LONG"}
LEAK_MARKER = "SECRET-LEAK-MARKER-42"


def _row(signal_id, details):
    return {
        "id": signal_id,
        "symbol": "JBL",
        "pattern_code": "sma150_bounce",
        "verdict": "ENTER",
        "probability": None,
        "score": 0.9,
        "reason": "ok",
        "details": details,
        "snapshot_date": date(2026, 7, 20),
        "created_at": datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
    }


class _FakeDB:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        return self.rows

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        return self.rows[0] if self.rows else None


@pytest.fixture
def make_client():
    def _make(rows):
        db = _FakeDB(rows)

        async def _fake_db():
            yield db

        app.dependency_overrides[get_db] = _fake_db
        return TestClient(app, raise_server_exceptions=False), db

    yield _make
    app.dependency_overrides.pop(get_db, None)


# --------------------------------------------------------------------------- #
# Pure helper behavior
# --------------------------------------------------------------------------- #

def test_normalize_dict_passes_through_unchanged():
    assert normalize_json_object(DETAILS, "signals.details") is DETAILS


def test_normalize_none_stays_none():
    assert normalize_json_object(None, "signals.details") is None


def test_normalize_json_object_string_is_decoded():
    assert normalize_json_object(json.dumps(DETAILS), "signals.details") == DETAILS


def test_normalize_rejects_array_scalar_and_malformed():
    from fastapi import HTTPException

    for bad in ('["a", "b"]', '42', '"just a string"', "{not json", 3.14):
        with pytest.raises(HTTPException) as exc_info:
            normalize_json_object(bad, "signals.details", record_id="rec-1")
        assert exc_info.value.status_code == 500
        # Generic message only — no payload content.
        assert exc_info.value.detail == "Internal serialization error"


def test_normalize_error_does_not_leak_payload(caplog):
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        normalize_json_object(f'["{LEAK_MARKER}"]', "signals.details", "rec-1")
    with pytest.raises(HTTPException):
        normalize_json_object(f"{{bad {LEAK_MARKER}", "signals.details", "rec-1")
    assert LEAK_MARKER not in caplog.text
    # But the safe identifying facts ARE logged.
    assert "signals.details" in caplog.text
    assert "rec-1" in caplog.text


# --------------------------------------------------------------------------- #
# GET /api/signals
# --------------------------------------------------------------------------- #

def test_signals_list_details_already_dict(make_client):
    client, _ = make_client([_row(uuid.uuid4(), dict(DETAILS))])
    resp = client.get("/api/signals")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body[0]["details"], dict)
    assert body[0]["details"]["symbol"] == "JBL"


def test_signals_list_details_json_string(make_client):
    """The exact live failure: asyncpg JSONB arriving as a JSON string."""
    client, _ = make_client([_row(uuid.uuid4(), json.dumps(DETAILS))])
    resp = client.get("/api/signals")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body[0]["details"], dict)  # object, not string
    assert body[0]["details"] == DETAILS


def test_signals_list_details_none(make_client):
    client, _ = make_client([_row(uuid.uuid4(), None)])
    resp = client.get("/api/signals")
    assert resp.status_code == 200
    assert resp.json()[0]["details"] is None


def test_signals_list_malformed_details_safe_500(make_client):
    client, _ = make_client([_row(uuid.uuid4(), f"{{corrupt {LEAK_MARKER}")])
    resp = client.get("/api/signals")
    assert resp.status_code == 500
    assert LEAK_MARKER not in resp.text  # contents never leak to the client


def test_signals_list_array_details_safe_500(make_client):
    client, _ = make_client([_row(uuid.uuid4(), f'["{LEAK_MARKER}"]')])
    resp = client.get("/api/signals")
    assert resp.status_code == 500
    assert LEAK_MARKER not in resp.text


def test_signals_list_scan_run_filter_joins_occurrence_table(make_client):
    client, db = make_client([_row(uuid.uuid4(), json.dumps(DETAILS))])
    resp = client.get("/api/signals", params={"scan_run_id": str(uuid.uuid4())})
    assert resp.status_code == 200
    assert "JOIN scan_run_signals srs ON srs.signal_id = s.id" in db.queries[-1]


# --------------------------------------------------------------------------- #
# GET /api/signals/{signal_id}
# --------------------------------------------------------------------------- #

def test_signal_detail_handles_dict_and_string(make_client):
    sig_id = uuid.uuid4()
    for details in (dict(DETAILS), json.dumps(DETAILS)):
        client, _ = make_client([_row(sig_id, details)])
        resp = client.get(f"/api/signals/{sig_id}")
        assert resp.status_code == 200
        assert isinstance(resp.json()["details"], dict)
        assert resp.json()["details"] == DETAILS


def test_signal_detail_malformed_details_safe_500(make_client):
    sig_id = uuid.uuid4()
    client, _ = make_client([_row(sig_id, f"{{corrupt {LEAK_MARKER}")])
    resp = client.get(f"/api/signals/{sig_id}")
    assert resp.status_code == 500
    assert LEAK_MARKER not in resp.text


# --------------------------------------------------------------------------- #
# Response model contract
# --------------------------------------------------------------------------- #

def test_signal_response_validates_normalized_row():
    row = _row(uuid.uuid4(), json.dumps(DETAILS))
    validated = SignalResponse(
        id=str(row["id"]),
        symbol=row["symbol"],
        pattern_code=row["pattern_code"],
        verdict=row["verdict"],
        probability=row["probability"],
        score=row["score"],
        reason=row["reason"],
        details=normalize_json_object(row["details"], "signals.details"),
        snapshot_date=row["snapshot_date"],
        created_at=row["created_at"],
    )
    assert validated.details == DETAILS
