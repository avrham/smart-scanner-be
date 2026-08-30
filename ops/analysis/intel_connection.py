"""One connection helper for the three Wave 2 ingestion entry points.

The refreshes run OUTSIDE the FastAPI process and write to relations the
Product API cannot touch, so they connect as `smart_scanner_market_intel` when
a DSN for it is configured, and fall back to the ordinary connection selector
when it is not — which keeps every existing invocation working unchanged.

The role is ASSERTED after connecting, not assumed. A DSN that silently
authenticates as somebody more privileged is exactly the failure this pattern
exists to catch, and it is cheap to check once per run.

Nothing here logs a DSN, a host or a password.
"""

from __future__ import annotations

import contextlib
from typing import AsyncIterator, Optional

import asyncpg

from app.config import settings


class IntelConnectionRefused(RuntimeError):
    """The configured DSN did not authenticate as the expected role."""


@contextlib.asynccontextmanager
async def intel_connection(*, expected_role: Optional[str] = None,
                           ) -> AsyncIterator[asyncpg.Connection]:
    """Yield a connection for an ingestion run, then close it.

    With MARKET_INTEL_DATABASE_URL set: a single direct connection as the
    least-privilege ingestion role, with `current_user` verified.
    Without it: the shared pool, exactly as before.
    """
    dsn = (settings.MARKET_INTEL_DATABASE_URL or "").strip()
    if not dsn:
        from app.deps import close_db_pool, init_db_pool
        pool = await init_db_pool()
        try:
            async with pool.acquire() as conn:
                yield conn
        finally:
            await close_db_pool()
        return

    role = expected_role or settings.MARKET_INTEL_EXPECTED_DB_ROLE
    conn = await asyncpg.connect(dsn)
    try:
        actual = await conn.fetchval("SELECT current_user")
        if role and actual != role:
            # The role name is safe to print; the DSN is not, and is not.
            raise IntelConnectionRefused(
                f"connected as {actual!r}, expected {role!r}")
        yield conn
    finally:
        await conn.close()


__all__ = ["intel_connection", "IntelConnectionRefused"]
