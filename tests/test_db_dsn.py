"""Deterministic tests for Supabase DSN construction (no live DB connections).

Root cause being locked in: Supabase's pooler (Supavisor) requires the tenant
in the username (`postgres.<project_ref>`); a bare `postgres` user fails with
"(ENOIDENTIFIER) no tenant identifier provided".
"""

import asyncio

import pytest
from fastapi import HTTPException

import app.deps as deps
from app.deps import build_connection_dsns, extract_project_ref


URL = "https://cbynqepcnfxdaxyambmi.supabase.co"
REF = "cbynqepcnfxdaxyambmi"
REGION = "eu-central-1"
PASSWORD = "s3cr3t-Passw0rd"


def test_pooler_dsn_uses_tenant_username():
    candidates = dict(build_connection_dsns(URL, REGION, PASSWORD))

    pooler_6543 = candidates["pooler:6543"]
    assert f"postgres.{REF}:" in pooler_6543  # tenant-qualified user
    assert f"@aws-0-{REGION}.pooler.supabase.com:6543/postgres" in pooler_6543
    assert "sslmode=require" in pooler_6543

    pooler_5432 = candidates["pooler:5432"]
    assert f"postgres.{REF}:" in pooler_5432
    assert f"@aws-0-{REGION}.pooler.supabase.com:5432/postgres" in pooler_5432


def test_fallback_order_pooler_6543_then_5432_then_direct():
    labels = [label for label, _ in build_connection_dsns(URL, REGION, PASSWORD)]
    assert labels == ["pooler:6543", "pooler:5432", "direct:5432"]


def test_direct_dsn_uses_plain_postgres_user():
    candidates = dict(build_connection_dsns(URL, REGION, PASSWORD))
    direct = candidates["direct:5432"]
    assert direct.startswith("postgresql://postgres:")  # NOT postgres.<ref>
    assert f"postgres.{REF}" not in direct
    assert f"@db.{REF}.supabase.co:5432/postgres" in direct
    assert "sslmode=require" in direct


def test_password_is_url_encoded():
    candidates = dict(build_connection_dsns(URL, REGION, "p@ss/word#1"))
    for dsn in candidates.values():
        assert "p%40ss%2Fword%231" in dsn


def test_missing_url_fails_clearly():
    for bad in ("", None, "not-a-url"):
        with pytest.raises(ValueError) as exc:
            build_connection_dsns(bad, REGION, PASSWORD)
        assert "SUPABASE_URL" in str(exc.value)
        assert PASSWORD not in str(exc.value)


def test_missing_region_fails_clearly():
    with pytest.raises(ValueError) as exc:
        build_connection_dsns(URL, "", PASSWORD)
    assert "SUPABASE_REGION" in str(exc.value)
    assert PASSWORD not in str(exc.value)


def test_missing_password_fails_clearly():
    with pytest.raises(ValueError) as exc:
        build_connection_dsns(URL, REGION, "")
    assert "SUPABASE_DB_PASSWORD" in str(exc.value)


def test_extract_project_ref():
    assert extract_project_ref(URL) == REF
    with pytest.raises(ValueError):
        extract_project_ref("")


def test_get_db_returns_json_503_without_secrets(monkeypatch):
    async def failing_pool():
        raise ValueError(f"boom with {PASSWORD}")  # simulate leaky low-level error

    monkeypatch.setattr(deps, "init_db_pool", failing_pool)

    async def drive():
        agen = deps.get_db()
        await agen.__anext__()

    with pytest.raises(HTTPException) as exc:
        asyncio.run(drive())

    assert exc.value.status_code == 503
    # JSON-able detail with a config hint, and the password never leaks even
    # when the underlying error message contains it.
    assert "SUPABASE_URL" in exc.value.detail
    assert PASSWORD not in exc.value.detail
