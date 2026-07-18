"""
Dependency injection for Smart Scanner Backend
"""
import asyncpg
from typing import AsyncGenerator
from fastapi import Depends, HTTPException, Header
from fastapi.security import HTTPBearer
from urllib.parse import urlparse

from .config import settings

_db_pool = None

async def init_db_pool():
    global _db_pool
    if _db_pool is None:
        parsed = urlparse(settings.SUPABASE_URL)
        project_ref = parsed.hostname.split('.')[0]
        pooler_host = f"aws-0-{settings.SUPABASE_REGION}.pooler.supabase.com"

        connection_strings = [
            # Pooler 6543 (recommended)
            f"postgresql://postgres:{settings.SUPABASE_DB_PASSWORD}"
            f"@{pooler_host}:6543/postgres?sslmode=require&options=project%3D{project_ref}",
            # Pooler 5432 (fallback)
            f"postgresql://postgres:{settings.SUPABASE_DB_PASSWORD}"
            f"@{pooler_host}:5432/postgres?sslmode=require&options=project%3D{project_ref}",
            # Direct DB host (fallback)
            f"postgresql://postgres:{settings.SUPABASE_DB_PASSWORD}"
            f"@db.{project_ref}.supabase.co:5432/postgres?sslmode=require",
        ]

        last_error = None
        for dsn in connection_strings:
            try:
                _db_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10, command_timeout=60)
                async with _db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                print(f"Connected OK via: {dsn.split('@')[1].split('?')[0]}")
                break
            except Exception as e:
                last_error = e
                _db_pool = None
                print(f"Connect failed: {e}")
                continue

        if _db_pool is None:
            raise last_error or Exception("All connection attempts failed")
    return _db_pool

async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await init_db_pool()
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