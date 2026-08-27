#!/usr/bin/env python3
"""Bounded company-news refresh for the isolated staging scanner universe.

Operator entry point for `app.news_ingest.refresh_company_news` — the same
function the `company_news_refresh.v1` pipeline stage would call. It adds no
scheduling, no polling loop and no provider logic of its own; it opens one
connection, runs one idempotent refresh over the frozen candidate universe,
and exits.

Credentials come from the environment and are never printed. This must only be
run by a component that already holds the provider key — never the Product API.

Usage:
    DATABASE_URL=...  MASSIVE_API_KEY=... \\
        .venv/bin/python ops/analysis/refresh_company_news.py [lookback_days]
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.news_ingest as ni  # noqa: E402
import app.reference_market as rm  # noqa: E402
from app.workers.massive_client import MassiveClient  # noqa: E402


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not dsn or not key:
        print("DATABASE_URL and MASSIVE_API_KEY are required; neither is printed.",
              file=sys.stderr)
        return 2

    lookback = int(sys.argv[1]) if len(sys.argv) > 1 else ni.DEFAULT_LOOKBACK_DAYS
    pages = int(sys.argv[2]) if len(sys.argv) > 2 else ni.MAX_PAGES_PER_SYMBOL

    # The frozen CANDIDATE universe only. Reference symbols (SPY/QQQ, sector
    # ETFs) are market context and carry no company catalysts of their own.
    symbols = sorted(rm.SYMBOL_SECTORS)
    client = MassiveClient(api_key=key)
    conn = await asyncpg.connect(dsn)
    try:
        summary = await ni.refresh_company_news(
            conn, client, symbols, lookback_days=lookback, max_pages=pages)
    finally:
        await conn.close()

    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
