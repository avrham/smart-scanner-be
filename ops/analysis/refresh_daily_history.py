"""Bring the frozen universe and the reference market through the latest
completed session — audit first, then a bounded manual refresh.

    python -m ops.analysis.refresh_daily_history --audit
    python -m ops.analysis.refresh_daily_history --refresh [--limit 40]

WHY THIS EXISTS
---------------
The temporal audit found staging bars stopping at 2026-08-26 while the latest
completed session was 2026-08-28, and asked for the exact cause rather than two
missing dates. The cause is operational and provable:

    SMART-SCANNER-DAILY-PIPELINE   enabled = FALSE, next_run_at = NULL
    PROOF-DAILY-PIPELINE           enabled = true but PAUSED = true
    history-refresh worker app     SUSPENDED since 2026-08-25

Nothing was broken. Nothing had run. The last bars are exactly as old as the
last manual pipeline invocation (2026-08-26 08:38 UTC), and every one of the 25
symbols sits on the same date with the same bar count — the signature of "no
run", not of "a run that half-worked".

The 23 failed refresh tasks in the queue are all `history_refresh_http_409`,
the documented shared-cooldown code, from the 2026-08-23 job that the 2026-08-26
job then superseded. They are old evidence, not a live fault.

WHAT THIS FIXES, THEN
---------------------
The gap was not in the refresh logic — it was that an operator had no bounded
way to run it. Enabling a schedule is forbidden, and hand-driving the durable
queue means writing job rows by hand. So this is the missing third option: one
command that says exactly how stale each class of symbol is, and one that
brings them current through the SAME service the durable worker calls
(`history_incremental_refresh_execute_service`) — no second refresh path, no
second provider path, no schedule enabled.

PACING IS THE PROVIDER'S, NOT OURS
----------------------------------
Massive Basic allows five requests a minute and this repository paces warmup at
one symbol per 75 seconds behind a machine-wide advisory lock. The service
enforces that itself and answers 409 when asked sooner; this script surfaces
that as a skip rather than an error, and reports how many symbols remain.

RUN IT WITH THE SCHEDULER OFF
-----------------------------
The service refuses (409 `scheduler_enabled`) when `ENABLE_SCHEDULER` is true,
and that guard is correct: a process that schedules must not also hand-drive
the provider. So invoke it as

    ENABLE_SCHEDULER=false HISTORY_REFRESH_DATABASE_URL=... python -m ...

This never edits `.env` and never enables a schedule — it only declines to be
a scheduler for the length of one command.

CONNECTION
----------
`HISTORY_REFRESH_DATABASE_URL` — an operator-supplied DSN with the privilege to
write `daily_bars`. Deliberately NOT the market-intel role: that one is
confined by RLS to research symbols and must not be able to touch a
frozen-universe bar. Never logged.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

from app.history_warmup_execute import (INCREMENTAL_REFRESH_CONTRACT_VERSION_V2,
                                        classify_incremental_symbol_state,
                                        missing_trading_sessions)
from app.prospective_session import resolve_latest_completed_session

FROZEN_CODE = "WYCKOFF-HISTORY-WARMUP-QUALIFICATION"
REFERENCE_CODE = "SMART-SCANNER-REFERENCE-MARKET-V1"

FRESHNESS_SQL = """
SELECT u.universe_code, s.symbol,
       (SELECT max(b.trading_date) FROM public.daily_bars b WHERE b.symbol = s.symbol) AS latest_bar,
       (SELECT count(*)::int FROM public.daily_bars b WHERE b.symbol = s.symbol) AS bars
FROM public.history_warmup_universe_symbols s
JOIN public.history_warmup_universes u ON u.id = s.universe_id
WHERE u.universe_code = ANY($1::text[])
ORDER BY u.universe_code, s.symbol
"""


async def _connect() -> asyncpg.Connection:
    dsn = (os.environ.get("HISTORY_REFRESH_DATABASE_URL") or "").strip()
    if not dsn:
        raise SystemExit(
            "HISTORY_REFRESH_DATABASE_URL is required (an operator DSN able to "
            "write daily_bars). It is never read from the repository.")
    return await asyncpg.connect(dsn)


async def audit(now: Optional[datetime] = None) -> Dict[str, Any]:
    moment = now or datetime.now(timezone.utc)
    target = resolve_latest_completed_session(moment)
    conn = await _connect()
    try:
        rows = [dict(r) for r in await conn.fetch(
            FRESHNESS_SQL, [FROZEN_CODE, REFERENCE_CODE])]
    finally:
        await conn.close()

    out: Dict[str, Any] = {"target_completed_session": target.isoformat(),
                           "classes": {}, "stale_symbols": []}
    for row in rows:
        klass = row["universe_code"]
        bucket = out["classes"].setdefault(
            klass, {"symbols": 0, "current": 0, "stale": 0,
                    "latest_bar": None, "missing_sessions": 0})
        bucket["symbols"] += 1
        latest = row["latest_bar"]
        state = classify_incremental_symbol_state(latest, target)
        if bucket["latest_bar"] is None or (latest and latest > bucket["latest_bar"]):
            bucket["latest_bar"] = latest
        if state == "incremental_current":
            bucket["current"] += 1
            continue
        bucket["stale"] += 1
        missing = missing_trading_sessions(latest, target)
        bucket["missing_sessions"] = max(bucket["missing_sessions"], len(missing))
        out["stale_symbols"].append({
            "universe": klass, "symbol": row["symbol"],
            "latest_bar": latest.isoformat() if latest else None,
            "bars": row["bars"], "missing_sessions": len(missing),
            "state": state})
    for bucket in out["classes"].values():
        if isinstance(bucket["latest_bar"], date):
            bucket["latest_bar"] = bucket["latest_bar"].isoformat()
    return out


async def refresh(limit: int, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Bring stale symbols current through the canonical service.

    One symbol per call, exactly as the durable worker does it. A 409 is the
    provider cooldown speaking and is reported as `deferred`, not as a failure;
    a symbol that errors is reported and the batch continues.
    """
    from fastapi import HTTPException
    from app.routers.admin import history_incremental_refresh_execute_service

    moment = now or datetime.now(timezone.utc)
    target = resolve_latest_completed_session(moment)
    report = await audit(moment)
    stale = report["stale_symbols"][:max(0, limit)]

    from app.config import settings
    # The provider's pace, read from configuration rather than chosen here:
    # Massive Basic is five requests a minute and this repository already
    # settled on one symbol per 75 seconds behind a machine-wide lock. Sleeping
    # it is what turns 35 sequential 409s into 35 refreshed symbols.
    # Reported for context only. The ACTUAL wait comes from the server's own
    # `cooldown_remaining_seconds`, because the configured 75s floor does not
    # include the previous run's duration and a local sleep therefore always
    # undershoots it.
    interval = max(0, int(settings.HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS))

    conn = await _connect()
    summary: Dict[str, Any] = {
        "target_completed_session": target.isoformat(),
        "stale_total": len(report["stale_symbols"]),
        "attempted": len(stale), "refreshed": [], "deferred": [], "failed": [],
        "provider_requests": 0, "pacing_seconds": interval,
    }
    try:
        for row in stale:
            body = {"contract_version": INCREMENTAL_REFRESH_CONTRACT_VERSION_V2,
                    "symbol": row["symbol"],
                    "target_completed_session": target.isoformat(),
                    "latest_local_session": row["latest_bar"]}
            # The v2 contract binds the observed latest local 4H session as well
            # as the daily one — optimistic concurrency over BOTH series, so a
            # caller cannot refresh against a picture it never actually saw.
            # Rather than recompute the server's 4H session here (a second
            # implementation that could drift from its), ask: the first call is
            # rejected BEFORE any provider request and hands back the server's
            # own value, and the retry states it. Costs zero provider calls.
            result = None
            # Up to three attempts per symbol, and every retry is driven by
            # something the SERVER said rather than by a local guess:
            #   * it binds the 4H session too -> restate the value it reports
            #   * its cooldown is longer than any fixed sleep we could pick
            #     (it includes the previous run's own duration) -> wait exactly
            #     the remaining seconds it reports, then try again.
            # A fixed 75s sleep undershoots that cooldown by a few seconds, so
            # every symbol after the first defers forever. Asking is the fix.
            for attempt in range(3):
                try:
                    result = await history_incremental_refresh_execute_service(
                        conn, body=body, now=datetime.now(timezone.utc))
                    break
                except HTTPException as exc:
                    detail = exc.detail if isinstance(exc.detail, dict) else {}
                    error = detail.get("error")
                    if (exc.status_code == 409
                            and error == "stale_latest_local_4h_session"
                            and attempt < 2):
                        body["latest_local_4h_session"] = detail.get(
                            "server_latest_local_4h_session")
                        continue
                    if (exc.status_code == 409
                            and error == "provider_cooldown_active"
                            and attempt < 2):
                        wait = int(detail.get("cooldown_remaining_seconds") or 0)
                        await asyncio.sleep(max(1, wait) + 2)
                        continue
                    bucket = ("deferred" if exc.status_code == 409 else "failed")
                    summary[bucket].append({
                        "symbol": row["symbol"], "status": exc.status_code,
                        # Bounded and secret-free: the service's own safe detail.
                        "detail": str(exc.detail)[:200]})
                    break
                except Exception as exc:           # noqa: BLE001
                    summary["failed"].append({"symbol": row["symbol"],
                                              "error": type(exc).__name__})
                    break
            if result is None:
                continue
            summary["provider_requests"] += int(result.get("provider_request_count") or 0)
            summary["refreshed"].append({
                "symbol": row["symbol"],
                "before": result.get("latest_local_session_before"),
                "after": result.get("latest_local_session_after"),
                "requests": result.get("provider_request_count")})
    finally:
        await conn.close()
    return summary


def _print_audit(report: Dict[str, Any]) -> None:
    print(f"\nTarget completed session: {report['target_completed_session']}\n")
    print(f"  {'UNIVERSE':<40} {'SYMBOLS':>8} {'CURRENT':>8} {'STALE':>6} "
          f"{'LATEST BAR':>12} {'MISSING':>8}")
    for code, bucket in sorted(report["classes"].items()):
        print(f"  {code:<40} {bucket['symbols']:>8} {bucket['current']:>8} "
              f"{bucket['stale']:>6} {str(bucket['latest_bar']):>12} "
              f"{bucket['missing_sessions']:>8}")
    if report["stale_symbols"]:
        print(f"\n  {len(report['stale_symbols'])} symbols are behind the "
              f"latest completed session.")
    else:
        print("\n  Every symbol is current through the latest completed session.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--limit", type=int, default=40,
                        help="max symbols to refresh in one run (default 40)")
    args = parser.parse_args()
    if not (args.audit or args.refresh):
        parser.error("choose --audit, --refresh, or both")

    if args.audit:
        _print_audit(asyncio.run(audit()))

    if args.refresh:
        import json
        summary = asyncio.run(refresh(args.limit))
        print("\n" + json.dumps(summary, indent=2, default=str))
        print("\n  A `deferred` entry is the shared history-warmup cooldown "
              "(HTTP 409), not a failure: one symbol per 75 seconds is the "
              "configured pace. Re-run until `stale_total` reaches zero.")


if __name__ == "__main__":
    main()
