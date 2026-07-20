"""Phase 7B — provenance API contracts (no DB: fake connection via override).

Covers the GET /api/signals/{id}/provenance endpoint (linked / legacy_unlinked /
404) and the additive provenance filters on the signals list query builder.
"""

import json
import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.deps import get_db
from app.routers.public import build_signals_query
from main import app


SIGNAL_ID = uuid.uuid4()
LEGACY_ID = uuid.uuid4()
SCAN_RUN_ID = uuid.uuid4()

_PROV_ROW = {
    "signal_id": SIGNAL_ID,
    "scan_run_id": SCAN_RUN_ID,
    "source_path": "funnel",
    "scanner_mode": "funnel",
    "provider": "massive",
    "strategy_code": "wyckoff_mtf",
    "strategy_version": "wyckoff_mtf.v1",
    "decision_policy_version": "strategy_decision.v1",
    "provenance_version": "provenance.v1",
    "config_hash": "deadbeef",
    "config_snapshot": json.dumps({"strategy_config": {"min_price": 5.0}}),
    "market_data_as_of": datetime(2026, 7, 3, tzinfo=timezone.utc),
    "evidence_snapshot": json.dumps({"snapshot_date": "2026-07-03"}),
    "evidence_original_sha256": "a" * 64,
    "evidence_original_size_bytes": 123,
    "evidence_pruned": False,
    "evidence_pruned_keys": json.dumps([]),
    "external_observation_ids": json.dumps([]),
    "created_at": datetime(2026, 7, 3, 12, 0, tzinfo=timezone.utc),
}


FINGERPRINT = "f" * 64


class _FakeDB:
    """Answers the two fetchrow calls the provenance endpoint makes."""

    async def fetchrow(self, query, *args):
        if "FROM signals" in query and "signal_provenance" not in query:
            if args[0] == SIGNAL_ID:
                return {
                    "id": SIGNAL_ID,
                    "signal_fingerprint": FINGERPRINT,
                    "signal_fingerprint_version": "signal_fingerprint.v1",
                }
            if args[0] == LEGACY_ID:
                # Legacy identity is NULL-compatible: both fields NULL.
                return {
                    "id": LEGACY_ID,
                    "signal_fingerprint": None,
                    "signal_fingerprint_version": None,
                }
            return None
        if "FROM signal_provenance" in query:
            return dict(_PROV_ROW) if args[0] == SIGNAL_ID else None
        raise AssertionError(f"unexpected query: {query}")


async def _fake_db():
    yield _FakeDB()


@pytest.fixture
def client():
    app.dependency_overrides[get_db] = _fake_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


# --------------------------------------------------------------------------- #
# Provenance endpoint
# --------------------------------------------------------------------------- #

def test_provenance_endpoint_linked_signal(client):
    resp = client.get(f"/api/signals/{SIGNAL_ID}/provenance")
    assert resp.status_code == 200
    body = resp.json()
    assert body["provenance_status"] == "linked"
    assert body["signal_id"] == str(SIGNAL_ID)
    # Immutable identity exposed: fingerprint + algorithm version.
    assert body["signal_fingerprint"] == FINGERPRINT
    assert body["signal_fingerprint_version"] == "signal_fingerprint.v1"
    assert body["scan_run_id"] == str(SCAN_RUN_ID)
    assert body["source_path"] == "funnel"
    assert body["strategy_code"] == "wyckoff_mtf"
    assert body["strategy_version"] == "wyckoff_mtf.v1"
    assert body["decision_policy_version"] == "strategy_decision.v1"
    assert body["provenance_version"] == "provenance.v1"
    assert body["config_hash"] == "deadbeef"
    assert body["config_snapshot"] == {"strategy_config": {"min_price": 5.0}}
    assert body["evidence_snapshot"] == {"snapshot_date": "2026-07-03"}
    assert body["evidence_original_sha256"] == "a" * 64
    assert body["evidence_pruned"] is False
    assert body["evidence_pruned_keys"] == []
    assert body["external_observation_ids"] == []
    assert body["market_data_as_of"] is not None


def test_provenance_endpoint_legacy_unlinked(client):
    resp = client.get(f"/api/signals/{LEGACY_ID}/provenance")
    assert resp.status_code == 200
    body = resp.json()
    assert body["provenance_status"] == "legacy_unlinked"
    assert body["signal_id"] == str(LEGACY_ID)
    # Nothing fabricated for legacy rows: identity stays NULL-compatible.
    assert body["signal_fingerprint"] is None
    assert body["signal_fingerprint_version"] is None
    assert body["scan_run_id"] is None
    assert body["strategy_version"] is None
    assert body["config_hash"] is None
    assert body["config_snapshot"] is None


def test_provenance_endpoint_missing_signal_404(client):
    resp = client.get(f"/api/signals/{uuid.uuid4()}/provenance")
    assert resp.status_code == 404


def test_provenance_endpoint_invalid_uuid_404(client):
    resp = client.get("/api/signals/not-a-uuid/provenance")
    assert resp.status_code == 404


def test_signals_list_rejects_invalid_scan_run_id(client):
    resp = client.get("/api/signals", params={"scan_run_id": "nope"})
    assert resp.status_code == 400


# --------------------------------------------------------------------------- #
# Signals list query builder — provenance filters
# --------------------------------------------------------------------------- #

def test_no_provenance_filters_keeps_original_query_shape():
    query, params = build_signals_query()
    assert "signal_provenance" not in query
    assert params == ["ENTER", 50]


def test_provenance_filters_join_and_compose_with_and():
    query, params = build_signals_query(
        verdict="ALL",
        pattern_code="wyckoff_mtf",
        scan_run_id=str(SCAN_RUN_ID),
        strategy_version="wyckoff_mtf.v1",
        decision_policy_version="strategy_decision.v1",
        config_hash="deadbeef",
        limit=10,
    )
    # scan_run_id filters through the OCCURRENCE table (every detection),
    # version filters through the origin provenance.
    assert "JOIN scan_run_signals srs ON srs.signal_id = s.id" in query
    assert "JOIN signal_provenance sp ON sp.signal_id = s.id" in query
    assert "srs.scan_run_id = $2" in query
    assert "sp.strategy_version = $3" in query
    assert "sp.decision_policy_version = $4" in query
    assert "sp.config_hash = $5" in query
    # AND semantics with existing filters preserved.
    assert "s.pattern_code = $1" in query
    assert query.count(" AND ") >= 5
    assert params == [
        "wyckoff_mtf", str(SCAN_RUN_ID), "wyckoff_mtf.v1",
        "strategy_decision.v1", "deadbeef", 10,
    ]


def test_single_provenance_filter_joins_once():
    query, params = build_signals_query(config_hash="deadbeef")
    assert query.count("JOIN signal_provenance") == 1
    assert "scan_run_signals" not in query
    assert params == ["ENTER", "deadbeef", 50]


def test_scan_run_filter_uses_occurrence_table():
    """GET /api/signals?scan_run_id=... lists every signal the scan DETECTED
    (via scan_run_signals), including immutable signals originally created by
    an earlier scan — not only origin provenance rows."""
    query, params = build_signals_query(verdict="ALL", scan_run_id=str(SCAN_RUN_ID))
    assert "JOIN scan_run_signals srs ON srs.signal_id = s.id" in query
    assert "srs.scan_run_id = $1" in query
    assert "signal_provenance" not in query
    assert params == [str(SCAN_RUN_ID), 50]
