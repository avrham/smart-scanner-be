"""Operator entry point for the research lifecycle — through the REAL dispatcher.

    python -m ops.analysis.research_lifecycle --dispatch [--label proof]
    python -m ops.analysis.research_lifecycle --run     [--label proof]
    python -m ops.analysis.research_lifecycle --dry-run
    python -m ops.analysis.research_lifecycle --summary
    python -m ops.analysis.research_lifecycle --measure [--limit 20]
    python -m ops.analysis.research_lifecycle --schedule-preview

WHY THIS FILE IS NOW THIN
-------------------------
It used to BE the lifecycle. That was the problem: the scheduled path would
have been a job handler, and a handler that reimplements a script is a second
program that drifts from the one that was proven by hand. The lifecycle now
lives in `app/research_lifecycle.py`; the durable task identity lives in
`app/jobs/research_lifecycle.py`; and both the scheduler and this CLI reach it
through the SAME `enqueue_research_lifecycle`. So "run one lifecycle manually
through the exact same dispatcher used by scheduling" is a fact about the call
graph, not a claim in a report.

THE THREE MODES, AND WHEN EACH IS HONEST
----------------------------------------
  --dispatch  Enqueue the durable task and stop. The deployed research worker
              claims and executes it. This is the production-equivalent path
              and the one a staging proof should use.
  --run       Enqueue AND execute the task in this process, through the SAME
              handler entrypoint the worker calls. For an operator with no
              worker running. It is the same code; only the process differs.
  --dry-run   Stop after admission. Touches no provider at all.

CONNECTION
----------
`RESEARCH_LIFECYCLE_DATABASE_URL` — the dedicated least-privilege research
identity, verified after connecting. Without it, the market-intel connection is
used and the run says which role it actually got. Never logged.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone

import app.jobs.research_lifecycle as RL
import app.research_funnel as rf
import app.research_runs as rr
from ops.analysis.intel_connection import research_connection


async def dispatch(*, label: str, execute: bool, dry_run: bool) -> dict:
    """Enqueue one lifecycle through the canonical dispatcher; optionally run it.

    The payload is built by the SAME `task_payload_from_template` the scheduler
    uses, so an operator cannot hand-craft a run with limits the schedule could
    not have asked for — the bounds are clamped in one place.
    """
    from app.jobs.handlers.research_lifecycle_worker import (
        execute_research_lifecycle)

    now = datetime.now(timezone.utc)
    run_key = RL.manual_run_key(label=label, now=now)
    payload = RL.task_payload_from_template({}, run_key=run_key)
    if dry_run:
        payload["dry_run"] = True

    async with research_connection() as conn:
        role = await conn.fetchval("SELECT current_user")
        enqueued = await RL.enqueue_research_lifecycle(
            conn, run_key=run_key, payload=payload,
            requested_by=f"operator:{label}")
        out = {"db_role": role, "dispatch": enqueued, "run_key": run_key,
               "payload": {k: v for k, v in payload.items() if k != "run_key"}}
        if not execute:
            out["note"] = ("task queued on the research_lifecycle queue; the "
                           "dedicated research worker will claim it")
            return out
        if dry_run:
            # The dry run stops before the provider and is not worth a durable
            # task execution; call the service directly with the same limits.
            import app.research_lifecycle as svc
            out["result"] = await svc.run_lifecycle(
                conn, run_key=run_key, dry_run=True,
                refresh_discovery=False, enrich=False,
                admit_limit=payload["admit_limit"],
                warm_limit=payload["warm_limit"],
                provider_budget=payload["provider_budget"])
            return out
        out["result"] = await execute_research_lifecycle(conn, payload)
        return out


async def funnel() -> dict:
    async with research_connection() as conn:
        summary = await rf.load_funnel(conn)
        drops = [dict(r) for r in await conn.fetch(DROPOFF_SQL)]
    return {"funnel": summary, "dropoff": drops}


async def measure(limit: int) -> dict:
    async with research_connection() as conn:
        return await rr.measurement(conn, limit=limit)


async def schedule_preview() -> dict:
    from app.jobs.scheduler import compute_next_run_at, preview_occurrences
    async with research_connection() as conn:
        row = await conn.fetchrow(
            "SELECT schedule_code, schedule_version, schedule_type, timezone,"
            " market_close_delay_minutes, enabled, paused, next_run_at,"
            " payload_template, job_type "
            "FROM job_schedules WHERE schedule_code=$1",
            RL.RESEARCH_LIFECYCLE_SCHEDULE_CODE)
    if row is None:
        return {"schedule": None, "reason": "not_created"}
    s = dict(row)
    now = datetime.now(timezone.utc)
    return {"schedule": {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                         for k, v in s.items()},
            "next_occurrences_utc": preview_occurrences(s, now, 5),
            "next_run_at_computed": compute_next_run_at(s, now).isoformat()}


DROPOFF_SQL = """
SELECT coalesce(admission_reason, '-') AS admission_reason,
       coalesce(candidate_reason, '-') AS candidate_reason,
       coalesce(warmup_last_error_code, '-') AS warmup_error,
       count(*)::int AS symbols
FROM public.research_symbols
GROUP BY 1,2,3 ORDER BY 4 DESC
"""


def _print_funnel(payload: dict) -> None:
    f = payload["funnel"]
    states = f["states"]
    adm = f["admission"]
    print("\nRESEARCH FUNNEL — one state per symbol, and the totals add up\n")
    steps = [
        ("SELECTED FOR RESEARCH", f["selected_for_research"]),
        ("  admission passed", adm["passed"]),
        ("  admission unknown (proceeds)", adm["unknown"]),
        ("  admission pending (not evaluated)", adm["pending"]),
        ("  admission REJECTED (no provider call)", adm["rejected"]),
        ("ADMITTED TO HISTORY", f["admitted_to_history"]),
        ("  history pending", states[rf.LIFECYCLE_HISTORY_PENDING]),
        ("  history warming", states[rf.LIFECYCLE_HISTORY_WARMING]),
        ("  history unavailable", states[rf.LIFECYCLE_HISTORY_UNAVAILABLE]),
        ("  history failed", states[rf.LIFECYCLE_HISTORY_FAILED]),
        ("  scan pending", states[rf.LIFECYCLE_SCAN_PENDING]),
        ("SCANNED", f["scanned"]),
        ("  awaiting classification",
         states[rf.LIFECYCLE_CLASSIFICATION_PENDING]),
        ("  scanned, not a candidate",
         states[rf.LIFECYCLE_SCANNED_NOT_CANDIDATE]),
        ("  RESEARCH CANDIDATE", states[rf.LIFECYCLE_RESEARCH_CANDIDATE]),
    ]
    for label, value in steps:
        print(f"  {label:<42} {value:>5}")

    print("\n  RATES — each with its own denominator, and never one word\n")
    for name, r in f["rates"].items():
        pct = "n/a" if r["percent"] is None else f"{r['percent']}%"
        print(f"  {name:<28} {r['numerator']:>4} / {r['denominator']:<4} "
              f"= {pct:<7} of {r['of']}")

    cons = f["conservation"]
    print(f"\n  CONSERVATION: {'OK' if cons['ok'] else 'VIOLATED'}")
    for c in cons["checks"]:
        mark = "ok " if c["ok"] else "FAIL"
        print(f"    [{mark}] {c['invariant']:<44} {c['left']} == {c['right']}")

    print("\n  DROP-OFF BY REASON\n")
    print(f"  {'ADMISSION':<30} {'CANDIDATE':<24} {'WARMUP ERROR':<32} COUNT")
    for row in payload["dropoff"]:
        print(f"  {row['admission_reason']:<30} {row['candidate_reason']:<24} "
              f"{row['warmup_error']:<32} {row['symbols']:>5}")
    print("\n  Reason codes, never a score. `admission REJECTED` is the count "
          "of provider requests this gate did not have to spend.")


def _print_measurement(m: dict) -> None:
    agg = m["runs"]
    print(f"\nRESEARCH LIFECYCLE — {agg['runs']} measurable run(s)\n")
    if not agg["runs"]:
        print("  No completed runs yet. Conversion rates need several "
              "sessions before they mean anything.")
        return
    print(f"  window                {agg['first_run_at']} .. {agg['last_run_at']}")
    for name, r in m["rates"].items():
        pct = "n/a" if r["percent"] is None else f"{r['percent']}%"
        print(f"  {name:<28} {r['numerator']:>4} / {r['denominator']:<4} = {pct}")
    print(f"  median provider calls per candidate: "
          f"{m['median_provider_calls_per_candidate']}")
    p = m["symbol_persistence"]
    print(f"\n  unique symbols {p['unique_symbols']} | seen in >1 run "
          f"{p['repeated_symbols']} | ever a candidate "
          f"{p['symbols_ever_candidate']} | candidate in >1 run "
          f"{p['repeat_candidates']}")
    print(f"  runs not conserving {agg['runs_not_conserving']} | "
          f"runs hitting the provider budget {agg['runs_budget_exhausted']}")
    print("\n  BY DISCOVERY REASON\n")
    for row in m["by_discovery_reason"]:
        r = row["candidate_rate"]
        pct = "n/a" if r["percent"] is None else f"{r['percent']}%"
        print(f"    {row['reason']:<26} {row['candidates']:>3} / "
              f"{row['symbols']:<4} = {pct}")
    print("\n  LAST RUNS\n")
    print(f"    {'started':<28}{'status':<36}{'sel':>4}{'scan':>6}{'cand':>6}"
          f"{'used':>6}{'avoid':>7}")
    for run in m["recent_runs"]:
        print(f"    {str(run['started_at'])[:26]:<28}{run['status']:<36}"
              f"{run['symbols_selected']:>4}{run['research_scanned']:>6}"
              f"{run['research_candidates']:>6}{run['provider_calls_used']:>6}"
              f"{run['provider_calls_avoided']:>7}")
    print(f"\n  {m['interpretation']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dispatch", action="store_true",
                        help="enqueue the durable task and stop")
    parser.add_argument("--run", action="store_true",
                        help="enqueue AND execute through the same handler")
    parser.add_argument("--dry-run", action="store_true",
                        help="stop after admission; touch no provider")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--measure", action="store_true")
    parser.add_argument("--schedule-preview", action="store_true")
    parser.add_argument("--label", default="manual")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    if not (args.dispatch or args.run or args.dry_run or args.summary
            or args.measure or args.schedule_preview):
        parser.error("choose --dispatch, --run, --dry-run, --summary, "
                     "--measure or --schedule-preview")

    if args.dispatch or args.run or args.dry_run:
        print(json.dumps(asyncio.run(dispatch(
            label=args.label, execute=bool(args.run or args.dry_run),
            dry_run=bool(args.dry_run))), indent=2, default=str))
    if args.summary:
        _print_funnel(asyncio.run(funnel()))
    if args.measure:
        _print_measurement(asyncio.run(measure(args.limit)))
    if args.schedule_preview:
        print(json.dumps(asyncio.run(schedule_preview()), indent=2, default=str))


if __name__ == "__main__":
    main()
