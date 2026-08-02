"""Focused HTTP tests for PATCH /api/admin/job-schedules/{id} payload_template
support — the mechanism used to extend the disabled daily-pipeline schedule's
preview to represent the full intended dependency chain (a documentation-only
JSONB field; never touches enabled/schedule_type/timezone). No DB — a fake
connection captures the issued SQL/params."""

from __future__ import annotations

import json
import uuid

import pytest
from fastapi.testclient import TestClient

from app.deps import get_db, get_worker_token
from main import app


class _FakeScheduleConn:
    def __init__(self):
        self.last_sql = None
        self.last_args = None

    async def fetchrow(self, q, *args):
        self.last_sql = q
        self.last_args = args
        if "UPDATE job_schedules" in q:
            return {
                "id": args[0], "schedule_code": "SMART-SCANNER-DAILY-PIPELINE",
                "schedule_version": 1, "schedule_type": "market_daily",
                "timezone": "America/New_York", "cron_expression": None,
                "market_close_delay_minutes": 30, "enabled": False, "paused": False,
                "next_run_at": None, "last_enqueued_at": None, "last_job_id": None,
                "idempotency_scope": "occurrence",
                "job_type": "smart_scanner_daily_pipeline",
                "job_contract_version": "smart_scanner_daily_pipeline.v1",
                "payload_template": args[-1] if "payload_template" in q else None,
                "created_at": None, "updated_at": None,
            }
        return None


@pytest.fixture
def client():
    app.dependency_overrides[get_worker_token] = lambda: "t"
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


def test_patch_updates_payload_template(client):
    conn = _FakeScheduleConn()

    async def fake_db():
        yield conn

    app.dependency_overrides[get_db] = fake_db
    sid = str(uuid.uuid4())
    template = {"stages": ["a", "b"], "documentation": "x"}
    resp = client.patch(f"/api/admin/job-schedules/{sid}", json={"payload_template": template})
    assert resp.status_code == 200
    assert "payload_template=$" in conn.last_sql
    assert json.loads(conn.last_args[-1]) == template


def test_patch_rejects_oversized_payload_template(client):
    conn = _FakeScheduleConn()

    async def fake_db():
        yield conn

    app.dependency_overrides[get_db] = fake_db
    sid = str(uuid.uuid4())
    big = {"blob": "x" * 9000}
    resp = client.patch(f"/api/admin/job-schedules/{sid}", json={"payload_template": big})
    assert resp.status_code == 422
    assert conn.last_sql is None  # rejected before any write attempt


def test_patch_null_payload_template_clears_it(client):
    conn = _FakeScheduleConn()

    async def fake_db():
        yield conn

    app.dependency_overrides[get_db] = fake_db
    sid = str(uuid.uuid4())
    resp = client.patch(f"/api/admin/job-schedules/{sid}", json={"payload_template": None})
    assert resp.status_code == 200
    assert conn.last_args[-1] is None
