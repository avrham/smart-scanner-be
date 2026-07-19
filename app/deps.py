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


async def init_db_pool():
    global _db_pool
    if _db_pool is None:
        candidates = build_connection_dsns(
            settings.SUPABASE_URL,
            settings.SUPABASE_REGION,
            settings.SUPABASE_DB_PASSWORD,
        )

        last_error = None
        for label, dsn in candidates:
            try:
                _db_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10, command_timeout=60)
                async with _db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                # Log host:port only — never credentials.
                logger.info("Connected OK via %s (%s)", label, dsn.split("@")[1].split("?")[0])
                break
            except Exception as e:
                last_error = e
                _db_pool = None
                logger.warning("Connect failed via %s: %s", label, type(e).__name__)
                continue

        if _db_pool is None:
            raise last_error or Exception("All connection attempts failed")
    return _db_pool

async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        pool = await init_db_pool()
    except Exception as exc:
        # Clean JSON error instead of a plain-text 500. Never include the DSN
        # or password — only the failure class and a config hint.
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