"""PURE read-only prospective history-readiness evaluation (`shadow_prospective_readiness.v1`).

Evaluates a proposed frozen universe against LOCALLY AVAILABLE history only — it
never calls a provider. The endpoint runs one bounded, read-only aggregate query
over `daily_bars` (SELECT count / min / max / distinct-month / distinct-week per
symbol) and hands the per-symbol history rows to the pure builder here, which
compares them against the effective strategy thresholds resolved from the
wyckoff_mtf_v2 and sma150_bounce readiness code (Task Part 2):

  * wyckoff_mtf_v2 (candidate) HARD gates: >= 175 completed daily bars,
    >= 26 completed weekly periods, >= 24 completed monthly periods (the monthly
    gate binds, ~504 completed sessions / ~730+ calendar days). Weekly/monthly
    are RESAMPLED from daily, so daily depth drives them.
  * sma150_bounce (control) HARD gate: >= 200 completed daily bars.
  * 4H: there is NO local 4H bar table (frames are fetched live), so 4H
    readiness is reported as NOT LOCALLY VERIFIABLE — a launch blocker that must
    be resolved by provider pre-caching, never silently passed.

Deterministic for a fixed local dataset + symbol list; emits stable universe,
config and readiness-manifest hashes.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

PROSPECTIVE_READINESS_CONTRACT_VERSION = "shadow_prospective_readiness.v1"

# Effective HARD gates (traced Part 2 — NOT prefetch hints).
CANDIDATE_MIN_DAILY_BARS = 175          # required_daily_structure_bars
CANDIDATE_MIN_WEEKLY_PERIODS = 26       # weekly_min_periods
CANDIDATE_MIN_MONTHLY_PERIODS = 24      # monthly_min_periods (BINDS)
CANDIDATE_MIN_4H_BARS = 11              # trigger_lookback_4h + 1 (trigger, not readiness)
CONTROL_MIN_DAILY_BARS = 200            # sma150.v2 validate_dataframe (sma_window+50)

# Practical daily floor so the binding monthly-24 gate can be satisfied
# (~21 sessions/month × 24, trailing partial dropped).
IMPLIED_DAILY_SESSIONS_FOR_MONTHLY = 504

THRESHOLDS: Dict[str, Any] = {
    "candidate_min_daily_bars": CANDIDATE_MIN_DAILY_BARS,
    "candidate_min_weekly_periods": CANDIDATE_MIN_WEEKLY_PERIODS,
    "candidate_min_monthly_periods": CANDIDATE_MIN_MONTHLY_PERIODS,
    "candidate_min_4h_bars": CANDIDATE_MIN_4H_BARS,
    "control_min_daily_bars": CONTROL_MIN_DAILY_BARS,
    "binding_gate": "candidate_monthly_periods>=24 (~504 completed daily sessions)",
    "four_hour_local_history": "not_available_fetched_live",
}


def _sha(obj: Any) -> str:
    import json
    return "sha256:" + hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _completed_from_groups(distinct_groups: Optional[int]) -> int:
    """Completed periods = distinct present period-groups minus the (possibly
    partial) trailing one, mirroring `_period_is_complete` (partial dropped)."""
    if not distinct_groups or distinct_groups <= 0:
        return 0
    return max(0, int(distinct_groups) - 1)


def evaluate_symbol(history: Dict[str, Any]) -> Dict[str, Any]:
    """Pure per-symbol readiness. `history` is one aggregate row:
    {symbol, daily_bars, oldest, latest, month_groups, week_groups[, four_hour_bars]}."""
    symbol = str(history.get("symbol"))
    daily = int(history.get("daily_bars") or 0)
    month_groups = history.get("month_groups")
    week_groups = history.get("week_groups")
    completed_weeks = _completed_from_groups(week_groups)
    completed_months = _completed_from_groups(month_groups)
    # 4H is never available locally unless a genuine local column is provided.
    four_h = history.get("four_hour_bars")
    four_h_known = isinstance(four_h, int)

    cand_daily_ready = daily >= CANDIDATE_MIN_DAILY_BARS
    cand_weekly_ready = completed_weeks >= CANDIDATE_MIN_WEEKLY_PERIODS
    cand_monthly_ready = completed_months >= CANDIDATE_MIN_MONTHLY_PERIODS
    cand_4h_ready = (four_h >= CANDIDATE_MIN_4H_BARS) if four_h_known else None
    control_ready = daily >= CONTROL_MIN_DAILY_BARS
    # candidate overall readiness (daily+weekly+monthly). 4H availability is a
    # separate launch blocker surfaced below (never silently assumed ready).
    candidate_overall_ready = cand_daily_ready and cand_weekly_ready and cand_monthly_ready

    blocking: List[str] = []
    if not cand_daily_ready:
        blocking.append(f"candidate_insufficient_daily_history:{daily}<{CANDIDATE_MIN_DAILY_BARS}")
    if not cand_weekly_ready:
        blocking.append(f"candidate_insufficient_weekly_periods:{completed_weeks}<{CANDIDATE_MIN_WEEKLY_PERIODS}")
    if not cand_monthly_ready:
        blocking.append(f"candidate_insufficient_monthly_periods:{completed_months}<{CANDIDATE_MIN_MONTHLY_PERIODS}")
    if not control_ready:
        blocking.append(f"control_insufficient_daily_history:{daily}<{CONTROL_MIN_DAILY_BARS}")
    if cand_4h_ready is None:
        blocking.append("four_hour_history_not_locally_available")
    elif cand_4h_ready is False:
        blocking.append(f"candidate_insufficient_4h_bars:{four_h}<{CANDIDATE_MIN_4H_BARS}")

    return {
        "symbol": symbol,
        "available_completed_daily_bars": daily,
        "available_completed_weekly_periods": completed_weeks,
        "available_completed_monthly_periods": completed_months,
        "available_completed_4h_bars": four_h if four_h_known else None,
        "oldest_available": str(history.get("oldest")) if history.get("oldest") else None,
        "latest_completed": str(history.get("latest")) if history.get("latest") else None,
        "candidate_daily_ready": cand_daily_ready,
        "candidate_weekly_ready": cand_weekly_ready,
        "candidate_monthly_ready": cand_monthly_ready,
        "candidate_4h_ready": cand_4h_ready,
        "candidate_overall_ready": candidate_overall_ready,
        "control_ready": control_ready,
        "both_ready": candidate_overall_ready and control_ready,
        "missing_counts_by_timeframe": {
            "daily": max(0, CANDIDATE_MIN_DAILY_BARS - daily),
            "weekly": max(0, CANDIDATE_MIN_WEEKLY_PERIODS - completed_weeks),
            "monthly": max(0, CANDIDATE_MIN_MONTHLY_PERIODS - completed_months),
            "control_daily": max(0, CONTROL_MIN_DAILY_BARS - daily),
        },
        "blocking_reasons": blocking,
    }


def _percentile(sorted_vals: List[int], q: float) -> Optional[int]:
    if not sorted_vals:
        return None
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def build_prospective_readiness(
    symbols: List[str],
    history_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """`shadow_prospective_readiness.v1` over a bounded symbol list and the
    LOCAL history rows fetched for them (missing symbols → zero history)."""
    norm_symbols = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    by_symbol = {str(r.get("symbol", "")).upper(): r for r in history_rows}

    per_symbol: List[Dict[str, Any]] = []
    for sym in norm_symbols:
        row = by_symbol.get(sym, {"symbol": sym, "daily_bars": 0,
                                  "month_groups": 0, "week_groups": 0,
                                  "oldest": None, "latest": None})
        row = dict(row); row["symbol"] = sym
        per_symbol.append(evaluate_symbol(row))

    n = len(per_symbol)
    cand_ready = sum(1 for r in per_symbol if r["candidate_overall_ready"])
    ctrl_ready = sum(1 for r in per_symbol if r["control_ready"])
    both_ready = sum(1 for r in per_symbol if r["both_ready"])
    four_h_ready = sum(1 for r in per_symbol if r["candidate_4h_ready"] is True)
    four_h_unknown = sum(1 for r in per_symbol if r["candidate_4h_ready"] is None)
    not_ready = sum(1 for r in per_symbol if not r["both_ready"])

    depths = sorted(r["available_completed_daily_bars"] for r in per_symbol)
    threshold = CANDIDATE_MIN_MONTHLY_PERIODS  # binding gate is monthly, but daily depth drives it
    daily_threshold = max(CANDIDATE_MIN_DAILY_BARS, CONTROL_MIN_DAILY_BARS)
    depth_dist = {
        "minimum": depths[0] if depths else None,
        "p10": _percentile(depths, 0.10),
        "median": _percentile(depths, 0.50),
        "p90": _percentile(depths, 0.90),
        "maximum": depths[-1] if depths else None,
        "required_daily_threshold": daily_threshold,
        "implied_daily_sessions_for_monthly_gate": IMPLIED_DAILY_SESSIONS_FOR_MONTHLY,
        "count_meeting_daily_threshold": sum(1 for d in depths if d >= daily_threshold),
        "count_meeting_monthly_gate_proxy": sum(
            1 for r in per_symbol if r["candidate_monthly_ready"]),
    }

    readiness_manifest = [
        {"symbol": r["symbol"], "candidate_overall_ready": r["candidate_overall_ready"],
         "control_ready": r["control_ready"], "both_ready": r["both_ready"],
         "daily": r["available_completed_daily_bars"],
         "weekly": r["available_completed_weekly_periods"],
         "monthly": r["available_completed_monthly_periods"]}
        for r in per_symbol
    ]

    return {
        "contract_version": PROSPECTIVE_READINESS_CONTRACT_VERSION,
        "candidate_strategy": "wyckoff_mtf_v2",
        "control_strategy": "sma150_bounce",
        "thresholds": THRESHOLDS,
        "provider_called": False,
        "data_source": "local_daily_bars_only",
        "universe_size": n,
        "candidate_ready_count": cand_ready,
        "control_ready_count": ctrl_ready,
        "both_ready_count": both_ready,
        "four_hour_ready_count": four_h_ready,
        "four_hour_not_locally_verifiable_count": four_h_unknown,
        "not_ready_count": not_ready,
        "readiness_percentage": round(both_ready / n, 6) if n else None,
        "history_depth_distribution": depth_dist,
        "four_hour_launch_blocker": (
            "4H bars are fetched live and not stored locally; 4H readiness cannot "
            "be confirmed from local data and MUST be validated via provider "
            "pre-caching before a prospective campaign is frozen."),
        "universe_hash": _sha(norm_symbols),
        "config_hash": _sha(THRESHOLDS),
        "readiness_manifest_hash": _sha(readiness_manifest),
        "symbols": per_symbol,
    }


__all__ = [
    "PROSPECTIVE_READINESS_CONTRACT_VERSION",
    "THRESHOLDS",
    "CANDIDATE_MIN_DAILY_BARS",
    "CANDIDATE_MIN_WEEKLY_PERIODS",
    "CANDIDATE_MIN_MONTHLY_PERIODS",
    "CONTROL_MIN_DAILY_BARS",
    "evaluate_symbol",
    "build_prospective_readiness",
]


# =========================================================================== #
# v2: four-state per-timeframe readiness backed by local daily + 4H data.
# =========================================================================== #
from datetime import datetime, timezone, date as _date  # noqa: E402

PROSPECTIVE_READINESS_V2_CONTRACT_VERSION = "shadow_prospective_readiness.v2"

FOUR_HOUR_MIN_COMPLETED_BARS = 11          # trigger_lookback_4h(10) + 1
# Bounded freshness rule (pending a shared market-calendar abstraction): a
# timeframe is STALE if its latest completed bar/session is older than N calendar
# days from `now` — sized to absorb a weekend + a holiday, so a symbol with
# enough bars but no RECENT completed bar is NOT launch-ready.
DAILY_FRESHNESS_MAX_CALENDAR_DAYS = 5
FOUR_HOUR_FRESHNESS_MAX_CALENDAR_DAYS = 5

STATE_UNKNOWN = "unknown_no_local_storage"
STATE_NOT_READY = "not_ready_insufficient_count"
STATE_STALE = "stale_latest_bar_too_old"
STATE_READY = "ready"

THRESHOLDS_V2 = dict(THRESHOLDS, **{
    "candidate_min_4h_completed_bars": FOUR_HOUR_MIN_COMPLETED_BARS,
    "daily_freshness_max_calendar_days": DAILY_FRESHNESS_MAX_CALENDAR_DAYS,
    "four_hour_freshness_max_calendar_days": FOUR_HOUR_FRESHNESS_MAX_CALENDAR_DAYS,
})


def _to_date(v) -> Optional[_date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, _date):
        return v
    try:
        return datetime.fromisoformat(str(v)[:19]).date()
    except (ValueError, TypeError):
        try:
            return _date.fromisoformat(str(v)[:10])
        except (ValueError, TypeError):
            return None


def _timeframe_state(*, available: bool, count: int, required: int,
                     latest, now_date: _date, max_stale_days: int) -> Dict[str, Any]:
    """Four-state readiness for one timeframe (never silently 'ready')."""
    if not available:
        return {"state": STATE_UNKNOWN, "completed": None, "required": required,
                "missing": None, "latest": None, "stale_days": None}
    latest_d = _to_date(latest)
    if count < required:
        state = STATE_NOT_READY
    else:
        stale_days = (now_date - latest_d).days if latest_d else None
        state = STATE_STALE if (stale_days is None or stale_days > max_stale_days) else STATE_READY
    return {"state": state, "completed": count, "required": required,
            "missing": max(0, required - count),
            "latest": latest_d.isoformat() if latest_d else None,
            "stale_days": ((now_date - latest_d).days if latest_d else None)}


def evaluate_symbol_v2(*, symbol: str, daily: Dict[str, Any],
                       fourh: Optional[Dict[str, Any]], now: datetime) -> Dict[str, Any]:
    """Pure per-symbol v2 readiness. `daily` = aggregate daily row; `fourh` =
    aggregate 4H row, or None when the local 4H store is UNAVAILABLE (relation
    missing / not readable) → 4H state unknown_no_local_storage."""
    now_date = now.astimezone(timezone.utc).date()
    d_count = int(daily.get("daily_bars") or 0)
    completed_weeks = _completed_from_groups(daily.get("week_groups"))
    completed_months = _completed_from_groups(daily.get("month_groups"))

    daily_tf = _timeframe_state(available=True, count=d_count,
                                required=CANDIDATE_MIN_DAILY_BARS, latest=daily.get("latest"),
                                now_date=now_date, max_stale_days=DAILY_FRESHNESS_MAX_CALENDAR_DAYS)
    weekly_tf = _timeframe_state(available=True, count=completed_weeks,
                                 required=CANDIDATE_MIN_WEEKLY_PERIODS, latest=daily.get("latest"),
                                 now_date=now_date, max_stale_days=DAILY_FRESHNESS_MAX_CALENDAR_DAYS)
    monthly_tf = _timeframe_state(available=True, count=completed_months,
                                  required=CANDIDATE_MIN_MONTHLY_PERIODS, latest=daily.get("latest"),
                                  now_date=now_date, max_stale_days=31)
    fourh_available = fourh is not None
    fh_count = int((fourh or {}).get("completed_4h_bars") or 0)
    fourh_tf = _timeframe_state(available=fourh_available, count=fh_count,
                                required=FOUR_HOUR_MIN_COMPLETED_BARS,
                                latest=(fourh or {}).get("latest_4h"),
                                now_date=now_date, max_stale_days=FOUR_HOUR_FRESHNESS_MAX_CALENDAR_DAYS)
    control_tf = _timeframe_state(available=True, count=d_count,
                                  required=CONTROL_MIN_DAILY_BARS, latest=daily.get("latest"),
                                  now_date=now_date, max_stale_days=DAILY_FRESHNESS_MAX_CALENDAR_DAYS)

    candidate_states = [daily_tf["state"], weekly_tf["state"], monthly_tf["state"], fourh_tf["state"]]
    candidate_overall = STATE_READY if all(s == STATE_READY for s in candidate_states) else (
        STATE_UNKNOWN if STATE_UNKNOWN in candidate_states else (
            STATE_STALE if STATE_STALE in candidate_states else STATE_NOT_READY))
    both_ready = candidate_overall == STATE_READY and control_tf["state"] == STATE_READY

    blocking: List[str] = []
    for name, tf in (("daily", daily_tf), ("weekly", weekly_tf), ("monthly", monthly_tf),
                     ("four_hour", fourh_tf), ("control_daily", control_tf)):
        if tf["state"] != STATE_READY:
            blocking.append(f"{name}:{tf['state']}")

    return {
        "symbol": symbol,
        "daily": daily_tf, "weekly": weekly_tf, "monthly": monthly_tf,
        "four_hour": fourh_tf, "control": control_tf,
        "oldest_daily": _to_date(daily.get("oldest")).isoformat() if _to_date(daily.get("oldest")) else None,
        "latest_daily": daily_tf["latest"],
        "oldest_4h": (_to_date((fourh or {}).get("oldest_4h")).isoformat()
                      if fourh_available and _to_date((fourh or {}).get("oldest_4h")) else None),
        "latest_4h": fourh_tf["latest"],
        "candidate_overall_state": candidate_overall,
        "control_state": control_tf["state"],
        "both_ready": both_ready,
        "blocking_reasons": blocking,
    }


def build_prospective_readiness_v2(symbols: List[str], daily_rows: List[Dict[str, Any]],
                                   fourh_rows: Optional[List[Dict[str, Any]]], *,
                                   now: datetime) -> Dict[str, Any]:
    """shadow_prospective_readiness.v2. `fourh_rows=None` ⇒ local 4H store
    UNAVAILABLE (relation missing) ⇒ every symbol's 4H state is
    unknown_no_local_storage. `fourh_rows=[]` ⇒ store exists but no rows ⇒
    not_ready_insufficient_count."""
    norm = sorted({s.strip().upper() for s in symbols if s and s.strip()})
    daily_by = {str(r.get("symbol", "")).upper(): r for r in daily_rows}
    fourh_available = fourh_rows is not None
    fourh_by = {str(r.get("symbol", "")).upper(): r for r in (fourh_rows or [])}

    per: List[Dict[str, Any]] = []
    for sym in norm:
        d = dict(daily_by.get(sym, {"daily_bars": 0, "month_groups": 0, "week_groups": 0,
                                    "oldest": None, "latest": None}))
        fh = (fourh_by.get(sym, {"completed_4h_bars": 0, "oldest_4h": None, "latest_4h": None})
              if fourh_available else None)
        per.append(evaluate_symbol_v2(symbol=sym, daily=d, fourh=fh, now=now))

    n = len(per)
    both = sum(1 for r in per if r["both_ready"])
    fourh_ready = sum(1 for r in per if r["four_hour"]["state"] == STATE_READY)
    fourh_unknown = sum(1 for r in per if r["four_hour"]["state"] == STATE_UNKNOWN)
    fourh_stale = sum(1 for r in per if r["four_hour"]["state"] == STATE_STALE)
    fully_launch_ready = both
    depths = sorted(int(r["daily"]["completed"] or 0) for r in per)

    def _dist(vals):
        s = sorted(vals)
        return {"minimum": s[0] if s else None, "median": _percentile(s, 0.5),
                "p90": _percentile(s, 0.9), "maximum": s[-1] if s else None} if s else \
               {"minimum": None, "median": None, "p90": None, "maximum": None}

    daily_manifest = [{"s": r["symbol"], "st": r["daily"]["state"], "n": r["daily"]["completed"]} for r in per]
    fourh_manifest = [{"s": r["symbol"], "st": r["four_hour"]["state"], "n": r["four_hour"]["completed"]} for r in per]
    combined_manifest = [{"s": r["symbol"], "cand": r["candidate_overall_state"],
                          "ctrl": r["control_state"], "both": r["both_ready"]} for r in per]

    return {
        "contract_version": PROSPECTIVE_READINESS_V2_CONTRACT_VERSION,
        "candidate_strategy": "wyckoff_mtf_v2", "control_strategy": "sma150_bounce",
        "thresholds": THRESHOLDS_V2,
        "provider_called": False,
        "data_source": "local_daily_bars_and_market_bars_4h",
        "four_hour_local_store_available": fourh_available,
        "universe_size": n,
        "both_ready_count": both,
        "fully_launch_ready_count": fully_launch_ready,
        "four_hour_ready_count": fourh_ready,
        "four_hour_unknown_count": fourh_unknown,
        "four_hour_stale_count": fourh_stale,
        "not_ready_count": n - both,
        "readiness_percentage": round(both / n, 6) if n else None,
        "daily_distribution": _dist(depths),
        "weekly_distribution": _dist([int(r["weekly"]["completed"] or 0) for r in per]),
        "monthly_distribution": _dist([int(r["monthly"]["completed"] or 0) for r in per]),
        "four_hour_distribution": _dist([int(r["four_hour"]["completed"] or 0) for r in per
                                         if r["four_hour"]["completed"] is not None]),
        "universe_hash": _sha(norm),
        "config_hash": _sha(THRESHOLDS_V2),
        "daily_manifest_hash": _sha(daily_manifest),
        "four_hour_manifest_hash": _sha(fourh_manifest),
        "combined_readiness_manifest_hash": _sha(combined_manifest),
        "symbols": per,
    }
