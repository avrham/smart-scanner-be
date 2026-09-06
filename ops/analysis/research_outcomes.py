"""Operator entry point for research outcome maturation — through the REAL dispatcher.

    python -m ops.analysis.research_outcomes --dry-run
    python -m ops.analysis.research_outcomes --dispatch [--label backfill]
    python -m ops.analysis.research_outcomes --run      [--label backfill]
    python -m ops.analysis.research_outcomes --status
    python -m ops.analysis.research_outcomes --recheck  [--label audit]
    python -m ops.analysis.research_outcomes --schedule-preview
    python -m ops.analysis.research_outcomes --enable-schedule
    python -m ops.analysis.research_outcomes --disable-schedule

THE THREE MODES, AND WHEN EACH IS HONEST
----------------------------------------
  --dry-run   Reports what a real pass WOULD do — eligible, not yet eligible,
              measurable, missing data, per horizon — and writes NOTHING, not
              even the plan rows. Safe to point at staging or at production
              data before any decision.
  --dispatch  Enqueue the durable task and stop. The deployed research worker
              claims and executes it. This is the production-equivalent path
              and the one a staging backfill should use.
  --run       Enqueue AND execute in this process, through the SAME handler
              entrypoint the worker calls. For an operator with no worker
              running. Same code; only the process differs.

  --recheck   Re-reads settled rows too, purely to DETECT a corrected bar. It
              cannot repair one: a measured outcome is frozen by a database
              trigger, and a divergence becomes a bounded note beside the
              frozen numbers. Deliberately unavailable to the schedule.

WHY THERE IS NO `--measure-one` OR `--fix`
------------------------------------------
Because every hand-written outcome row is a row nobody can reproduce. The
backfill goes through `enqueue_research_outcomes` -> the registered handler ->
`run_outcome_maturation`, which is the same three steps the 19:30 ET schedule
takes, so "the historical rows and tonight's rows were produced by the same
program" is a fact about the call graph rather than an intention.

CONNECTION
----------
`RESEARCH_LIFECYCLE_DATABASE_URL` — the dedicated least-privilege research
identity, verified after connecting. Without it the market-intel connection is
used and, failing that, the process's configured pool; the run reports which
role it actually got. Never logged as a DSN.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone

import app.jobs.research_outcomes as RO
import app.research_outcomes as svc
from ops.analysis.intel_connection import research_connection


async def dispatch(*, label: str, execute: bool, include_settled: bool,
                   scan_limit: int, observation_limit: int) -> dict:
    """Enqueue one maturation run through the canonical dispatcher; optionally
    run it here.

    The payload is built by the SAME `task_payload_from_template` the scheduler
    uses, so an operator cannot hand-craft a run with bounds the schedule could
    not have asked for — every limit is clamped in one place.
    """
    from app.jobs.handlers.research_outcomes_worker import (
        execute_research_outcomes)

    run_key = RO.manual_run_key(label=label,
                                now=datetime.now(timezone.utc))
    template = {"scan_limit": scan_limit,
                "observation_limit": observation_limit}
    # Every operator-initiated pass is a `backfill`, whatever its label — the
    # mode column exists so a report can tell the nightly schedule's rows apart
    # from the ones somebody asked for by hand.
    payload = RO.task_payload_from_template(template, run_key=run_key,
                                            mode=svc.MODE_BACKFILL)
    # `include_settled` is not settable from a schedule template on purpose;
    # an operator asking for a recheck sets it here, explicitly.
    payload["include_settled"] = bool(include_settled)

    async with research_connection() as conn:
        role = await conn.fetchval("SELECT current_user")
        enqueued = await RO.enqueue_research_outcomes(
            conn, run_key=run_key, payload=payload, requested_by="operator")
        out = {"connected_as": role, "run_key": run_key, "enqueue": enqueued,
               "payload": payload}
        if execute:
            out["execution"] = await execute_research_outcomes(conn, payload)
        return out


async def dry_run(*, scan_limit: int, observation_limit: int) -> dict:
    async with research_connection() as conn:
        role = await conn.fetchval("SELECT current_user")
        summary = await svc.run_outcome_maturation(
            conn, run_key="dry-run", dry_run=True, scan_limit=scan_limit,
            observation_limit=observation_limit)
        return {"connected_as": role, **summary}


async def status() -> dict:
    async with research_connection() as conn:
        role = await conn.fetchval("SELECT current_user")
        return {"connected_as": role, **(await svc.outcome_status(conn))}


async def set_schedule(*, enable: bool) -> dict:
    """Enable or disable the outcome schedule, ALWAYS seeding `next_run_at`.

    WHY THIS EXISTS RATHER THAN A HAND-WRITTEN UPDATE
    -------------------------------------------------
    The durable scheduler treats a row with `next_run_at IS NULL` as DUE, and
    then does this (app/jobs/scheduler.py::_tick_as_leader):

        occurrence = s["next_run_at"] or compute_next_run_at(s, now)

    So a schedule enabled with a NULL next_run_at fires on its very next tick —
    but stamped with the identity of the next FUTURE occurrence, and it then
    advances past that occurrence. Measured on this schedule in staging: the
    tick at 2026-09-06T11:21:38Z materialised occurrence 2026-09-09T15:00:00Z
    (three days early) and set next_run_at to 2026-09-10T15:00:00Z, so the
    2026-09-09 slot produced no run at its own time and the durable record
    nonetheless claims it was served.

    Nothing was lost — an observation waits in the ledger until it is measured,
    so the only cost was one day of latency — but the audit trail was wrong
    about which occurrence a run answered, and that is not a property to leave
    in place.

    The scheduler is SHARED with the daily pipeline and the research lifecycle,
    so it is not this workstream's to change. What is ours is never handing it a
    NULL: `next_run_at` is computed here with the SAME resolver the scheduler
    uses, so the first tick takes the stored instant and the identity it stamps
    is the occurrence it actually ran for.
    """
    from app.jobs.scheduler import compute_next_run_at

    async with research_connection() as conn:
        role = await conn.fetchval("SELECT current_user")
        row = await conn.fetchrow(
            "SELECT * FROM job_schedules WHERE schedule_code=$1 "
            "ORDER BY schedule_version DESC LIMIT 1",
            RO.RESEARCH_OUTCOMES_SCHEDULE_CODE)
        if row is None:
            return {"connected_as": role, "error": "schedule_not_found",
                    "note": "migration 031 has not been applied here"}
        sched = dict(row)
        before = {"enabled": sched["enabled"], "paused": sched["paused"],
                  "next_run_at": (sched["next_run_at"].isoformat()
                                  if sched["next_run_at"] else None)}
        if not enable:
            await conn.execute(
                "UPDATE job_schedules SET enabled=FALSE, paused=TRUE,"
                " updated_at=NOW() WHERE id=$1", sched["id"])
            return {"connected_as": role, "action": "disabled", "before": before,
                    "after": {"enabled": False, "paused": True,
                              "next_run_at": before["next_run_at"]}}

        # Keep an already-scheduled occurrence; only a NULL is dangerous.
        next_run = sched["next_run_at"] or compute_next_run_at(
            sched, datetime.now(timezone.utc))
        await conn.execute(
            "UPDATE job_schedules SET enabled=TRUE, paused=FALSE,"
            " next_run_at=$2, updated_at=NOW() WHERE id=$1",
            sched["id"], next_run)
        return {"connected_as": role, "action": "enabled", "before": before,
                "after": {"enabled": True, "paused": False,
                          "next_run_at": next_run.isoformat()},
                "seeded_next_run_at": sched["next_run_at"] is None}


async def schedule_preview() -> dict:
    from app.jobs.scheduler import preview_occurrences
    async with research_connection() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM job_schedules WHERE schedule_code=$1 "
            "ORDER BY schedule_version DESC LIMIT 1",
            RO.RESEARCH_OUTCOMES_SCHEDULE_CODE)
        if row is None:
            return {"schedule": None,
                    "note": "migration 031 has not been applied here"}
        sched = dict(row)
        return {
            "schedule_code": sched["schedule_code"],
            "schedule_version": sched["schedule_version"],
            "enabled": sched["enabled"], "paused": sched["paused"],
            "schedule_type": sched["schedule_type"],
            "market_close_delay_minutes": sched["market_close_delay_minutes"],
            "next_run_at": (sched["next_run_at"].isoformat()
                            if sched["next_run_at"] else None),
            "next_occurrences": preview_occurrences(
                sched, datetime.now(timezone.utc), 5),
        }


def _print_status(report: dict) -> None:
    print(f"\n  research outcome ledger — {report['observations']} observations")
    print(f"  connected as {report.get('connected_as')}\n")
    for name, count in report["by_status"].items():
        print(f"    {name:<20} {count:>5}")
    print(f"\n  {'horizon':<8}{'classification':<24}{'status':<18}"
          f"{'n':>5}{'beat':>6}{'mean sym%':>12}{'mean exc%':>12}")
    for cell in report["cells"]:
        beat = cell.get("beat_benchmark")
        sym = cell.get("mean_symbol_return_pct")
        exc = cell.get("mean_excess_return_pct")
        print(f"    {cell['horizon']:<8}{cell['classification']:<24}"
              f"{cell['status']:<18}{cell['observations']:>5}"
              f"{('-' if beat is None else beat):>6}"
              f"{('-' if sym is None else f'{sym:+.2f}'):>12}"
              f"{('-' if exc is None else f'{exc:+.2f}'):>12}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what a run would do; write nothing")
    parser.add_argument("--dispatch", action="store_true",
                        help="enqueue the durable task and stop")
    parser.add_argument("--run", action="store_true",
                        help="enqueue AND execute through the same handler")
    parser.add_argument("--recheck", action="store_true",
                        help="with --run/--dispatch: re-read settled rows to "
                             "DETECT (never repair) a corrected bar")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--schedule-preview", action="store_true")
    parser.add_argument("--enable-schedule", action="store_true",
                        help="enable the outcome schedule, seeding next_run_at "
                             "so the first tick cannot consume a future "
                             "occurrence")
    parser.add_argument("--disable-schedule", action="store_true",
                        help="disable and pause the outcome schedule")
    parser.add_argument("--label", default="manual")
    parser.add_argument("--scan-limit", type=int, default=svc.DEFAULT_SCAN_LIMIT)
    parser.add_argument("--observation-limit", type=int,
                        default=svc.DEFAULT_OBSERVATION_LIMIT)
    args = parser.parse_args()

    if not (args.dry_run or args.dispatch or args.run or args.status
            or args.schedule_preview or args.enable_schedule
            or args.disable_schedule):
        parser.error("choose --dry-run, --dispatch, --run, --status, "
                     "--schedule-preview, --enable-schedule or "
                     "--disable-schedule")
    if args.enable_schedule and args.disable_schedule:
        parser.error("--enable-schedule and --disable-schedule are exclusive")

    if args.dry_run:
        print(json.dumps(asyncio.run(dry_run(
            scan_limit=args.scan_limit,
            observation_limit=args.observation_limit)),
            indent=2, default=str))
    if args.dispatch or args.run:
        print(json.dumps(asyncio.run(dispatch(
            label=args.label, execute=bool(args.run),
            include_settled=bool(args.recheck),
            scan_limit=args.scan_limit,
            observation_limit=args.observation_limit)),
            indent=2, default=str))
    if args.status:
        _print_status(asyncio.run(status()))
    if args.enable_schedule or args.disable_schedule:
        print(json.dumps(asyncio.run(set_schedule(
            enable=bool(args.enable_schedule))), indent=2, default=str))
    if args.schedule_preview:
        print(json.dumps(asyncio.run(schedule_preview()), indent=2,
                         default=str))


if __name__ == "__main__":
    main()
