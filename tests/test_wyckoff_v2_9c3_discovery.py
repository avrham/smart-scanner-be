"""Phase 9C3: admin read-only strategy discovery catalog."""

from __future__ import annotations

import ast
import asyncio
import importlib
import pathlib
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.deps import get_db
from app.workers.strategies.discovery import (
    CONFIG_STATUS_CONFIGURED,
    CONFIG_STATUS_MISSING_PATTERN_ROW,
    build_discovery_from_sources,
    discover_all_strategies,
    discover_strategy,
)
from app.workers.strategies.registry import list_strategies
from app.workers.strategies.wyckoff_v2.constants import default_config as v2_default_config
from main import app


ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "app" / "db" / "migrations"
ADMIN = ROOT / "app" / "routers" / "admin.py"
PUBLIC = ROOT / "app" / "routers" / "public.py"
DISCOVERY = ROOT / "app" / "workers" / "strategies" / "discovery.py"


def _git_diff(*paths: str) -> str:
    result = subprocess.run(
        ["git", "diff", "--", *paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


class _FakeDB:
    """Read-only fake connection. Writes raise; tracks query kinds."""

    def __init__(
        self,
        patterns: Optional[Dict[str, Dict[str, Any]]] = None,
        configs: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.patterns = patterns or {}
        self.configs = configs or {}
        self.writes: List[str] = []
        self.queries: List[str] = []

    async def fetchrow(self, query: str, *args):
        self.queries.append(query)
        q = " ".join(query.split()).upper()
        if "INSERT" in q or "UPDATE" in q or "DELETE" in q or "ALTER" in q:
            self.writes.append(query)
            raise AssertionError(f"write attempted: {query}")
        if "FROM patterns" in query and "WHERE code" in query:
            code = args[0]
            row = self.patterns.get(code)
            return row
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetch(self, query: str, *args):
        self.queries.append(query)
        q = " ".join(query.split()).upper()
        if "INSERT" in q or "UPDATE" in q or "DELETE" in q or "ALTER" in q:
            self.writes.append(query)
            raise AssertionError(f"write attempted: {query}")
        if "FROM pattern_configs" in query:
            code = args[0]
            raw = self.configs.get(code, {})
            return [{"key": k, "value": v} for k, v in raw.items()]
        if "FROM patterns" in query and "is_enabled = true" in query:
            # public listing path
            rows = []
            for code, row in self.patterns.items():
                if row.get("is_enabled") is True:
                    rows.append(row)
            return rows
        raise AssertionError(f"unexpected fetch: {query}")

    async def execute(self, query: str, *args):
        self.writes.append(query)
        raise AssertionError(f"execute write attempted: {query}")


def _v2_configured_db() -> _FakeDB:
    cfg = v2_default_config()
    # Store JSON-ish string values similar to JSONB text encoding in migrations.
    raw = {}
    for key, value in cfg.items():
        if isinstance(value, bool):
            raw[key] = "true" if value else "false"
        elif isinstance(value, (int, float)):
            raw[key] = str(value) if not isinstance(value, bool) else value
        elif isinstance(value, str):
            raw[key] = f'"{value}"'
        else:
            import json

            raw[key] = json.dumps(value)
    created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return _FakeDB(
        patterns={
            "wyckoff_mtf_v2": {
                "code": "wyckoff_mtf_v2",
                "name": "Wyckoff MTF v2",
                "description": "v2",
                "is_enabled": False,
                "created_at": created,
            },
            "wyckoff_mtf": {
                "code": "wyckoff_mtf",
                "name": "Wyckoff MTF",
                "description": "v1",
                "is_enabled": False,
                "created_at": created,
            },
            "sma150_bounce": {
                "code": "sma150_bounce",
                "name": "SMA150",
                "description": "legacy",
                "is_enabled": True,
                "created_at": created,
            },
            "sma150_bounce_v3": {
                "code": "sma150_bounce_v3",
                "name": "SMA150 v3",
                "description": "v3",
                "is_enabled": False,
                "created_at": created,
            },
        },
        configs={"wyckoff_mtf_v2": raw},
    )


@pytest.fixture
def configured_client(monkeypatch):
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
    db = _v2_configured_db()

    async def _override():
        yield db

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app, raise_server_exceptions=False), db
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def missing_v2_client(monkeypatch):
    """Registry has v2; patterns row for v2 is absent (migration not applied)."""
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
    db = _FakeDB(
        patterns={
            "sma150_bounce": {
                "code": "sma150_bounce",
                "name": "SMA150",
                "description": "legacy",
                "is_enabled": True,
                "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            },
        },
        configs={},
    )

    async def _override():
        yield db

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app, raise_server_exceptions=False), db
    finally:
        app.dependency_overrides.pop(get_db, None)


class TestDiscoveryHelper:
    def test_missing_row_never_enabled(self):
        item = build_discovery_from_sources(
            pattern_code="wyckoff_mtf_v2",
            strategy_version="wyckoff_mtf.v2",
            decision_policy_version="wyckoff_mtf.policy.v1",
            defaults=v2_default_config(),
            pattern_row=None,
            raw_config={},
        )
        assert item.registered is True
        assert item.enabled is None
        assert item.db_configured is False
        assert item.config_status == CONFIG_STATUS_MISSING_PATTERN_ROW
        assert item.allow_enter is False
        assert item.enable_4h_trigger is False
        assert item.min_price == 5.0

    def test_configured_disabled_preserves_rollout_gates(self):
        item = build_discovery_from_sources(
            pattern_code="wyckoff_mtf_v2",
            strategy_version="wyckoff_mtf.v2",
            decision_policy_version="wyckoff_mtf.policy.v1",
            defaults=v2_default_config(),
            pattern_row={
                "code": "wyckoff_mtf_v2",
                "name": "Wyckoff MTF v2",
                "description": "x",
                "is_enabled": False,
            },
            raw_config={
                "allow_enter": "false",
                "enable_4h_trigger": "false",
                "min_price": "5.0",
            },
        )
        assert item.registered is True
        assert item.enabled is False
        assert item.db_configured is True
        assert item.config_status == CONFIG_STATUS_CONFIGURED
        assert item.allow_enter is False
        assert item.enable_4h_trigger is False
        assert item.min_price == 5.0
        assert item.registered is not item.enabled

    def test_discover_all_covers_registry_codes(self):
        db = _v2_configured_db()
        items = asyncio.run(discover_all_strategies(db))
        codes = [i.pattern_code for i in items]
        assert codes == list_strategies()
        assert "wyckoff_mtf_v2" in codes
        assert db.writes == []

    def test_discover_unknown_returns_none(self):
        db = _FakeDB()
        assert asyncio.run(discover_strategy(db, "not_a_strategy")) is None


class TestAdminStrategiesApi:
    def test_list_includes_all_registered_including_disabled(
        self, configured_client
    ):
        client, db = configured_client
        resp = client.get("/api/admin/strategies")
        assert resp.status_code == 200
        body = resp.json()
        codes = [row["pattern_code"] for row in body]
        assert codes == list_strategies()
        by_code = {row["pattern_code"]: row for row in body}

        v2 = by_code["wyckoff_mtf_v2"]
        assert v2["registered"] is True
        assert v2["enabled"] is False
        assert v2["db_configured"] is True
        assert v2["config_status"] == CONFIG_STATUS_CONFIGURED
        assert v2["allow_enter"] is False
        assert v2["enable_4h_trigger"] is False
        assert v2["min_price"] == 5.0
        assert v2["strategy_version"] == "wyckoff_mtf.v2"
        assert v2["registered"] is True and v2["enabled"] is False

        v1 = by_code["wyckoff_mtf"]
        assert v1["registered"] is True
        assert v1["enabled"] is False
        assert v1["strategy_version"] == "wyckoff_mtf.v1"

        assert db.writes == []

    def test_detail_endpoint_reuses_helper(self, configured_client):
        client, _ = configured_client
        resp = client.get("/api/admin/strategies/wyckoff_mtf_v2")
        assert resp.status_code == 200
        body = resp.json()
        assert body["pattern_code"] == "wyckoff_mtf_v2"
        assert body["registered"] is True
        assert body["enabled"] is False
        assert body["allow_enter"] is False
        assert body["enable_4h_trigger"] is False
        assert body["min_price"] == 5.0

    def test_detail_404_for_unregistered(self, configured_client):
        client, _ = configured_client
        resp = client.get("/api/admin/strategies/not_registered")
        assert resp.status_code == 404

    def test_missing_migration_row_fails_safely(self, missing_v2_client):
        client, db = missing_v2_client
        resp = client.get("/api/admin/strategies/wyckoff_mtf_v2")
        assert resp.status_code == 200
        body = resp.json()
        assert body["registered"] is True
        assert body["enabled"] is None
        assert body["db_configured"] is False
        assert body["config_status"] == CONFIG_STATUS_MISSING_PATTERN_ROW
        assert body["allow_enter"] is False
        assert body["enable_4h_trigger"] is False
        assert body["min_price"] == 5.0
        assert body["enabled"] is not True
        assert db.writes == []

    def test_unauthorized_when_token_required(self, configured_client, monkeypatch):
        client, _ = configured_client
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "phase9c3-secret")

        assert client.get("/api/admin/strategies").status_code == 401
        assert (
            client.get(
                "/api/admin/strategies",
                headers={"X-Worker-Token": "wrong"},
            ).status_code
            == 401
        )
        ok = client.get(
            "/api/admin/strategies",
            headers={"X-Worker-Token": "phase9c3-secret"},
        )
        assert ok.status_code == 200
        assert any(r["pattern_code"] == "wyckoff_mtf_v2" for r in ok.json())

    def test_public_patterns_still_hides_disabled(self, configured_client):
        client, _ = configured_client
        resp = client.get("/api/patterns")
        assert resp.status_code == 200
        codes = [row["code"] for row in resp.json()]
        assert "sma150_bounce" in codes
        assert "wyckoff_mtf_v2" not in codes
        assert "wyckoff_mtf" not in codes
        assert "sma150_bounce_v3" not in codes

    def test_no_provider_imports_in_discovery_module(self):
        tree = ast.parse(DISCOVERY.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert not mod.startswith("app.providers")
                assert "funnel" not in mod
                assert "scheduler" not in mod
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("app.providers")


class TestExecutionPathsUnchanged:
    def test_scheduler_still_hardcodes_sma150_bounce(self):
        text = (ROOT / "app" / "workers" / "scheduler.py").read_text(encoding="utf-8")
        assert 'pattern_code="sma150_bounce"' in text
        assert "wyckoff_mtf_v2" not in text

    def test_public_patterns_query_still_filters_enabled(self):
        text = PUBLIC.read_text(encoding="utf-8")
        assert "WHERE p.is_enabled = true" in text

    def test_package_import_still_no_registry_side_effect(self):
        before = list(list_strategies())
        importlib.reload(importlib.import_module("app.workers.strategies.wyckoff_v2"))
        assert list(list_strategies()) == before

    def test_v1_identity_unchanged(self):
        from app.workers.strategies.registry import get_strategy
        from app.workers.strategies.wyckoff import STRATEGY_VERSION

        v1 = get_strategy("wyckoff_mtf")
        assert v1.pattern_code == "wyckoff_mtf"
        assert v1.version == STRATEGY_VERSION == "wyckoff_mtf.v1"


class TestPhase9C3Boundaries:
    def test_no_migration_013(self):
        assert not list(MIGRATIONS.glob("013_*"))
        assert (MIGRATIONS / "012_wyckoff_mtf_v2.sql").exists()

    def test_forbidden_surfaces_unmodified(self):
        assert _git_diff(
            "app/workers/scanner/funnel.py",
            "app/workers/scheduler.py",
            "app/scheduler",
            "app/jobs",
            "app/workers/strategies/decision_card.py",
            "app/workers/outcomes",
            "app/workers/shadow",
            "app/providers",
            "app/workers/strategies/wyckoff",
            "app/workers/strategies/wyckoff_v2",
            "app/workers/strategies/registry.py",
            "app/db/migrations",
            "app/routers/public.py",
            "app/routers/outcomes.py",
            "app/routers/shadow.py",
        ) == ""

    def test_admin_diff_is_discovery_only(self):
        diff = _git_diff("app/routers/admin.py")
        assert diff
        assert "list_admin_strategies" in diff
        assert "discover_all_strategies" in diff
        assert "get_admin_strategy" in diff
        # Discovery endpoints must not introduce new write verbs.
        assert "@router.post(\"/strategies" not in diff
        assert "@router.put(\"/strategies" not in diff
        assert "@router.patch(\"/strategies" not in diff
        text = ADMIN.read_text(encoding="utf-8")
        assert '@router.get("/strategies"' in text
        assert '@router.get(\n    "/strategies/{pattern_code}"' in text or (
            '@router.get("/strategies/{pattern_code}"' in text
        )
        disc = DISCOVERY.read_text(encoding="utf-8")
        assert "INSERT" not in disc.upper()
        assert "UPDATE" not in disc.upper()
        assert "DELETE" not in disc.upper()

    def test_defaults_unchanged(self):
        cfg = v2_default_config()
        assert cfg["allow_enter"] is False
        assert cfg["enable_4h_trigger"] is False
        assert cfg["min_price"] == 5.0
