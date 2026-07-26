"""
Dependency injection for Smart Scanner Backend
"""
import logging
from typing import AsyncGenerator, List, Tuple

import asyncpg
from fastapi import Depends, HTTPException, Header
from fastapi.security import HTTPBearer
from urllib.parse import urlparse, quote

from .config import settings

logger = logging.getLogger(__name__)

_db_pool = None


def extract_project_ref(supabase_url: str) -> str:
    """Project ref from a Supabase API URL (https://<ref>.supabase.co)."""
    parsed = urlparse(supabase_url or "")
    hostname = parsed.hostname or ""
    project_ref = hostname.split(".")[0] if hostname else ""
    if not project_ref:
        raise ValueError(
            "SUPABASE_URL is missing or malformed; expected https://<project-ref>.supabase.co"
        )
    return project_ref


def build_connection_dsns(
    supabase_url: str, region: str, db_password: str
) -> List[Tuple[str, str]]:
    """Build the ordered Supabase DSN candidates as (label, dsn) pairs.

    Pooler (Supavisor) connections REQUIRE the tenant in the username:
    `postgres.<project_ref>` — a bare `postgres` user fails with
    "(ENOIDENTIFIER) no tenant identifier provided". The direct DB host keeps
    the plain `postgres` user.

    Validates inputs so we never construct malformed hosts. Never logs/embeds
    the password in error messages.
    """
    project_ref = extract_project_ref(supabase_url)
    if not (region or "").strip():
        raise ValueError("SUPABASE_REGION is missing/empty; e.g. eu-central-1")
    if not (db_password or "").strip():
        raise ValueError("SUPABASE_DB_PASSWORD is missing/empty")

    password = quote(db_password, safe="")  # URL-encode special characters
    pooler_user = f"postgres.{project_ref}"
    pooler_host = f"aws-0-{region}.pooler.supabase.com"
    direct_host = f"db.{project_ref}.supabase.co"

    return [
        (
            "pooler:6543",
            f"postgresql://{pooler_user}:{password}"
            f"@{pooler_host}:6543/postgres?sslmode=require",
        ),
        (
            "pooler:5432",
            f"postgresql://{pooler_user}:{password}"
            f"@{pooler_host}:5432/postgres?sslmode=require",
        ),
        (
            "direct:5432",
            f"postgresql://postgres:{password}"
            f"@{direct_host}:5432/postgres?sslmode=require",
        ),
    ]


_db_pool_mode = None


async def init_db_pool():
    """Create (once) the shared asyncpg pool.

    The DSN candidates are chosen by the audit-aware connection selector: in
    audit-only mode ONLY the explicit AUDIT_DATABASE_URL is used (fail closed if
    absent — never the legacy Supabase-derived identity); otherwise the existing
    Supabase-derived candidates. Only the connection MODE and host:port (never
    credentials or the audit DSN) are ever logged.
    """
    global _db_pool, _db_pool_mode
    # Import here to avoid a circular import (audit_db imports deps helpers).
    from app.audit_db import (
        MODE_AUDIT_EXPLICIT,
        select_connection_plan,
    )

    if _db_pool is None:
        mode, candidates, pool_kwargs = select_connection_plan()

        last_error = None
        for label, dsn in candidates:
            try:
                _db_pool = await asyncpg.create_pool(dsn, **pool_kwargs)
                async with _db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                if mode == MODE_AUDIT_EXPLICIT:
                    # Never log the audit host/DSN — only the mode.
                    logger.info("Connected OK (mode=%s)", mode)
                else:
                    logger.info(
                        "Connected OK via %s (%s) mode=%s",
                        label, dsn.split("@")[1].split("?")[0], mode,
                    )
                _db_pool_mode = mode
                break
            except Exception as e:
                last_error = e
                _db_pool = None
                logger.warning("Connect failed via %s: %s", label, type(e).__name__)
                continue

        if _db_pool is None:
            raise last_error or Exception("All connection attempts failed")
    return _db_pool


async def close_db_pool():
    """Close the shared pool and reset selection state (shutdown / tests)."""
    global _db_pool, _db_pool_mode
    if _db_pool is not None:
        try:
            await _db_pool.close()
        finally:
            _db_pool = None
            _db_pool_mode = None

async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    from app.audit_db import AuditDatabaseError

    try:
        pool = await init_db_pool()
    except AuditDatabaseError as exc:
        # Bounded: audit database not configured / invalid. The message is
        # already redacted (never contains the DSN, host, user or password).
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        # Clean JSON error instead of a plain-text 500. Never include the DSN
        # or password — only the failure class and a config hint.
        if settings.AUDIT_ONLY_MODE:
            raise HTTPException(
                status_code=503,
                detail=f"Audit database connection failed ({type(exc).__name__}).",
            )
        raise HTTPException(
            status_code=503,
            detail=(
                f"Database connection failed ({type(exc).__name__}). "
                "Check SUPABASE_URL / SUPABASE_REGION / SUPABASE_DB_PASSWORD."
            ),
        )
    async with pool.acquire() as connection:
        yield connection

async def get_worker_token(
    x_worker_token: str = Header(None, alias="X-Worker-Token")
) -> str:
    # Allow bypass in environments where token is not required
    if getattr(settings, "REQUIRE_WORKER_TOKEN", False):
        if not x_worker_token or x_worker_token != settings.WORKER_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid or missing worker token")
        return x_worker_token
    return x_worker_token or "disabled"

security = HTTPBearer()