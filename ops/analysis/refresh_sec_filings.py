#!/usr/bin/env python3
"""Bounded SEC 8-K refresh for the isolated staging scanner universe.

Operator entry point for `app.sec_ingest.refresh_sec_filings` — the same
function the `sec_filings_refresh.v1` pipeline stage would call. It adds no
scheduling and no source logic of its own; it opens one connection, runs one
idempotent refresh over the frozen candidate universe, and exits.

SEC_USER_AGENT is REQUIRED. The SEC asks automated clients to identify
themselves, and this refuses to run without one rather than sending a generic
default. Supply a real contact, e.g.:

    SEC_USER_AGENT="Smart Scanner research (you@example.com)"

Usage:
    DATABASE_URL=...  SEC_USER_AGENT=... \\
        .venv/bin/python ops/analysis/refresh_sec_filings.py [lookback_days]
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import asyncpg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import app.reference_market as rm  # noqa: E402
import app.sec_ingest as si  # noqa: E402


async def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    agent = os.environ.get("SEC_USER_AGENT", "").strip()
    if not dsn or not agent:
        print("DATABASE_URL and SEC_USER_AGENT are required. SEC access policy "
              "requires a declared identifiable User-Agent.", file=sys.stderr)
        return 2

    lookback = int(sys.argv[1]) if len(sys.argv) > 1 else si.DEFAULT_LOOKBACK_DAYS

    # The frozen CANDIDATE universe only. Reference symbols (SPY/QQQ, sector
    # ETFs) are market context; a fund files no current reports of its own.
    symbols = sorted(rm.SYMBOL_SECTORS)
    client = si.SecClient(agent)
    conn = await asyncpg.connect(dsn)
    try:
        summary = await si.refresh_sec_filings(
            conn, client, symbols, lookback_days=lookback)
    finally:
        await conn.close()

    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
