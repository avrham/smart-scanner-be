"""Read-only DB access-check for the cohort closeout audit.

Proves the access-check recognizes a correctly least-privileged read-only role,
flags missing SELECT / unexpected write privileges / missing relations, handles
DB unavailability safely, and issues NO mutation SQL (runs in a read-only
transaction). No live database is used.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from main import app
from app.config import settings
from app.deps import get_db, get_worker_token
from app.audit_access import (
    REQUIRED_RELATIONS,
    WRITE_PRIVILEGES,
    evaluate_access,
    run_access_check,
)


def _clean_rows() -> List[Dict[str, Any]]:
    return [
        {"relation": r, "exists": True, "can_select": True,
         "can_insert": False, "can_update": False, "can_delete": False,
         "can_truncate": False, "can_trigger": False}
        for r in REQUIRED_RELATIONS
    ]


class TestEvaluateAccessPure:
    def test_clean_read_only_role_is_ready(self):
        v = evaluate_access(
            database_identity="smart_scanner_audit_reader",
            transaction_read_only="on", default_transaction_read_only="on",
            relation_privileges=_clean_rows(),
        )
        assert v["ready_for_closeout_audit"] is True
        assert v["reasons"] == []
        assert v["unexpected_write_privileges"] == []
        assert v["database_identity"] == "smart_scanner_audit_reader"
        assert v["default_transaction_read_only"] is True

    def test_missing_select_flagged(self):
        rows = _clean_rows(); rows[0]["can_select"] = False
        v = evaluate_access(
            database_identity="x", transaction_read_only="on",
            default_transaction_read_only="on", relation_privileges=rows,
        )
        assert v["ready_for_closeout_audit"] is False
        assert any("missing_select_privilege" in r for r in v["reasons"])

    @pytest.mark.parametrize("priv", ["insert", "update", "delete", "truncate"])
    def test_unexpected_write_privilege_flagged(self, priv):
        rows = _clean_rows(); rows[1][f"can_{priv}"] = True
        v = evaluate_access(
            database_identity="postgres", transaction_read_only="off",
            default_transaction_read_only="off", relation_privileges=rows,
        )
        assert v["ready_for_closeout_audit"] is False
        assert v["unexpected_write_privileges"]
        assert priv.upper() in v["unexpected_write_privileges"][0]["privileges"]

    def test_missing_relation_handled(self):
        rows = _clean_rows(); rows[2]["exists"] = False
        rows[2]["can_select"] = False
        v = evaluate_access(
            database_identity="x", transaction_read_only="on",
            default_transaction_read_only="on", relation_privileges=rows,
        )
        assert v["ready_for_closeout_audit"] is False
        assert any("missing_relations" in r for r in v["reasons"])

    def test_default_read_only_off_flagged(self):
        v = evaluate_access(
            database_identity="x", transaction_read_only="on",
            default_transaction_read_only="off",
            relation_privileges=_clean_rows(),
        )
        assert v["ready_for_closeout_audit"] is False
        assert "default_transaction_read_only_not_on" in v["reasons"]


class _FakeTxn:
    def __init__(self, readonly): self.readonly = readonly

    async def __aenter__(self):
        assert self.readonly is True   # audit txn MUST be read-only
        return self

    async def __aexit__(self, *a): return False


class _FakeConn:
    """Records every SQL statement and returns a clean read-only role."""

    def __init__(self, *, select=True, writes=False, exists=True,
                 rls=False, policies=()):
        self.sql: List[str] = []
        self._select, self._writes, self._exists = select, writes, exists
        self._rls, self._policies = rls, list(policies)

    def transaction(self, readonly=False):
        self.sql.append(f"transaction(readonly={readonly})")
        return _FakeTxn(readonly)

    async def fetchval(self, q, *a):
        self.sql.append(q.strip().splitlines()[0])
        if "current_user" in q:
            return "smart_scanner_audit_reader"
        if "SHOW transaction_read_only" in q:
            return "on"
        if "SHOW default_transaction_read_only" in q:
            return "on"
        if "to_regclass" in q:
            return a[0] if self._exists else None
        return None

    async def fetchrow(self, q, *a):
        if "rolsuper" in q:
            self.sql.append("role_attributes")
            # A clean audit role: no elevated attributes.
            return {
                "rolsuper": False, "rolcreaterole": False,
                "rolcreatedb": False, "rolreplication": False,
                "rolbypassrls": False,
            }
        if "relrowsecurity" in q:
            self.sql.append("rls_flags")
            # RLS disabled by default -> grant sufficiency (ready path).
            return {"rls_enabled": self._rls, "rls_forced": False,
                    "rls_active": self._rls}
        self.sql.append("has_table_privilege")
        return {
            "can_select": self._select, "can_insert": self._writes,
            "can_update": self._writes, "can_delete": self._writes,
            "can_truncate": self._writes, "can_trigger": self._writes,
        }

    async def fetch(self, q, *a):
        # pg_policies query -> no applicable policies by default.
        self.sql.append("applicable_policies")
        return list(self._policies)


class TestRunAccessCheckReadOnly:
    def test_uses_read_only_txn_and_no_mutation_sql(self):
        conn = _FakeConn()
        result = asyncio.run(run_access_check(conn))
        assert result["ready_for_closeout_audit"] is True
        assert any("transaction(readonly=True)" in s for s in conn.sql)
        forbidden = ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE",
                     "DROP", "ALTER ")
        issued = " ".join(conn.sql).upper()
        assert not any(k in issued for k in forbidden)

    def test_reports_unexpected_writes(self):
        conn = _FakeConn(writes=True)
        result = asyncio.run(run_access_check(conn))
        assert result["ready_for_closeout_audit"] is False
        assert result["unexpected_write_privileges"]


@pytest.fixture
def client():
    app.dependency_overrides[get_worker_token] = lambda: "t"
    try:
        yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.dependency_overrides.pop(get_worker_token, None)
        app.dependency_overrides.pop(get_db, None)


class TestAccessCheckEndpoint:
    def test_ready_when_role_clean(self, client, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)

        async def fake_db():
            yield _FakeConn()

        app.dependency_overrides[get_db] = fake_db
        resp = client.get("/api/admin/shadow-cohort/access-check")
        assert resp.status_code == 200
        assert resp.json()["ready_for_closeout_audit"] is True

    def test_db_unavailable_returns_bounded_error(self, client, monkeypatch):
        monkeypatch.setattr(settings, "AUDIT_ONLY_MODE", False)

        async def boom_db():
            raise HTTPException(status_code=503, detail="Database connection failed (X).")
            yield  # pragma: no cover

        app.dependency_overrides[get_db] = boom_db
        resp = client.get("/api/admin/shadow-cohort/access-check")
        assert resp.status_code == 503
        # bounded, no secret / no DSN
        body = resp.text.lower()
        assert "password" not in body and "postgresql://" not in body
