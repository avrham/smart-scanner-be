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
