#!/usr/bin/env python3
"""Bounded catalyst refresh for the isolated staging scanner universe.

Operator entry point for `app.catalyst_ingest.refresh_catalysts` — the same
function a daily-pipeline stage would call. It adds no scheduling, no polling
loop and no provider logic of its own; it opens one connection, runs one
idempotent refresh over the frozen candidate universe, and exits.

Credentials come from the environment and are never printed. This must only be
run by a component that already holds the provider key — never the Product API.

Usage:
    DATABASE_URL=...  MASSIVE_API_KEY=... \\
        .venv/bin/python ops/analysis/refresh_catalyst_events.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.catalyst_ingest as ci  # noqa: E402
import app.reference_market as rm  # noqa: E402
from app.workers.massive_client import MassiveClient  # noqa: E402


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not dsn or not key:
        print("DATABASE_URL and MASSIVE_API_KEY are required; neither is printed.",
              file=sys.stderr)
        return 2

    # The frozen CANDIDATE universe only. Reference symbols are market context
    # and carry no corporate catalysts of their own.
    symbols = sorted(rm.SYMBOL_SECTORS)
    client = MassiveClient(api_key=key)
    conn = await asyncpg.connect(dsn)
    try:
        summary = await ci.refresh_catalysts(conn, client, symbols)
    finally:
        await conn.close()

    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
