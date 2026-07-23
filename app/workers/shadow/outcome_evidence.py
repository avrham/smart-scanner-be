"""Outcome and benchmark evidence for shadow review (Phase 9F3).

Contract: shadow_outcome_evidence.v1.

Built ON TOP of the existing outcome model, never beside it:

  * input rows are the joined pair-outcome records produced by
    outcomes.persistence.fetch_pair_outcomes — returns, SPY/QQQ
    benchmark-relative values, both frozen arm identities and verdicts;
  * the pair outcome is ONE verdict-neutral market-path observation shared
    by both arms, so "candidate return" and "control return" are DECISION
    COHORTS over the same observations (pairs where that arm chose an
    actionable ENTER/WATCH), never fabricated per-arm return deltas;
  * the paired candidate-vs-control comparison reuses the existing
    action-divergence semantics verbatim (derive_enter_arm + the
    shadow_pair_resolution_metrics.v1 neutral bands): only pairs where
    exactly ONE arm chose ENTER are action-resolvable, and favorability is
    classified with the existing bands — no new statistical claims;
  * horizons are NEVER combined; groups are keyed by the full identity
    (experiment + candidate strategy/policy/config versions) and never
    pooled across it; a missing return stays missing (excluded WITH a
    count), never zero.

All functions are pure (no I/O).
"""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Dict, List, Optional, Tuple

from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label
from app.workers.shadow.outcomes.metrics import (
    classify_horizon_return,
    derive_enter_arm,
)


OUTCOME_EVIDENCE_CONTRACT_VERSION = "shadow_outcome_evidence.v1"

ACTIONABLE_VERDICTS = ("ENTER", "WATCH")

GROUP_FIELDS = (
    "experiment_code",
    "experiment_version",
    "candidate_strategy_code",
    "candidate_strategy_version",
    "candidate_decision_policy_version",
    "candidate_config_hash",
    "control_strategy_code",
    "control_strategy_version",
)


def _mean_or_none(values: List[float]) -> Optional[float]:
    return mean(values) if values else None


def _median_or_none(values: List[float]) -> Optional[float]:
    return median(values) if values else None


def _identity(row: Dict[str, Any]) -> Dict[str, Any]:
    pair = row.get("pair") or {}
    control = row.get("control") or {}
    candidate = row.get("candidate") or {}
    return {
        "experiment_code": pair.get("experiment_code"),
        "experiment_version": pair.get("experiment_version"),
        "candidate_strategy_code": candidate.get("strategy_code"),
        "candidate_strategy_version": candidate.get("strategy_version"),
        "candidate_decision_policy_version": candidate.get(
            "decision_policy_version"
        ),
        "candidate_config_hash": candidate.get("config_hash"),
        "control_strategy_code": control.get("strategy_code"),
        "control_strategy_version": control.get("strategy_version"),
    }


def _pair_return(row: Dict[str, Any], window: int) -> Optional[float]:
    returns = (row.get("outcome") or {}).get("returns") or {}
    return returns.get(window_label(window))


def _relative_return(
    row: Dict[str, Any], window: int, benchmark: str
) -> Optional[float]:
    rel = (row.get("relative_returns") or {}).get(benchmark) or {}
    return rel.get(window_label(window))


def _is_actionable(row: Dict[str, Any], arm: str) -> bool:
    verdict = (row.get(arm) or {}).get("verdict")
    return verdict in ACTIONABLE_VERDICTS


def _return_stats(values: List[float]) -> Dict[str, Any]:
    positive = sum(1 for v in values if v > 0)
    return {
        "return_count": len(values),
        "mean_return": _mean_or_none(values),
        "median_return": _median_or_none(values),
        "positive_return_count": positive,
        "positive_return_rate": (positive / len(values)) if values else None,
    }


def _horizon_evidence(
    rows: List[Dict[str, Any]], window: int
) -> Dict[str, Any]:
    """All per-horizon evidence for one identity group. One horizon only."""
    candidate_rows = [r for r in rows if _is_actionable(r, "candidate")]
    control_rows = [r for r in rows if _is_actionable(r, "control")]

    candidate_returns = [
        v for v in (_pair_return(r, window) for r in candidate_rows)
        if v is not None
    ]
    control_returns = [
        v for v in (_pair_return(r, window) for r in control_rows)
        if v is not None
    ]

    benchmark_block: Dict[str, Any] = {}
    for benchmark in ("SPY", "QQQ"):
        rel_values = [
            v for v in (
                _relative_return(r, window, benchmark)
                for r in candidate_rows
            )
            if v is not None
        ]
        better = sum(1 for v in rel_values if v > 0)
        benchmark_block[benchmark] = {
            "relative_return_count": len(rel_values),
            "relative_mean_return": _mean_or_none(rel_values),
            "relative_median_return": _median_or_none(rel_values),
            "candidate_better_count": better,
            "candidate_better_rate": (
                better / len(rel_values) if rel_values else None
            ),
        }

    # Paired candidate-vs-control resolution: ONLY action-divergent pairs
    # (exactly one arm chose ENTER) are resolvable, classified with the
    # existing shadow_pair_resolution_metrics.v1 neutral bands. Unmatched
    # observations are never mixed in.
    divergent = [
        (r, derive_enter_arm(
            (r.get("control") or {}).get("verdict"),
            (r.get("candidate") or {}).get("verdict"),
        ))
        for r in rows
    ]
    divergent = [(r, arm) for r, arm in divergent if arm is not None]
    candidate_favorable = 0
    control_favorable = 0
    flat = 0
    incomplete = 0
    for row, enter_arm in divergent:
        classification = classify_horizon_return(
            _pair_return(row, window), window
        )
        if classification == "incomplete":
            incomplete += 1
        elif classification == "flat_or_neutral":
            flat += 1
        elif classification == "enter_action_favorable":
            if enter_arm == "candidate":
                candidate_favorable += 1
            else:
                control_favorable += 1
        else:  # non_enter_action_favorable
            if enter_arm == "candidate":
                control_favorable += 1
            else:
                candidate_favorable += 1
    resolved = candidate_favorable + control_favorable + flat

    return {
        "window": window_label(window),
        "candidate": {
            "actionable_pair_count": len(candidate_rows),
            **_return_stats(candidate_returns),
        },
        "control": {
            "actionable_pair_count": len(control_rows),
            **_return_stats(control_returns),
        },
        "benchmarks": benchmark_block,
        "paired_action_divergence": {
            "divergent_pair_count": len(divergent),
            "resolved_count": resolved,
            "incomplete_count": incomplete,
            "candidate_favorable_count": candidate_favorable,
            "control_favorable_count": control_favorable,
            "flat_or_neutral_count": flat,
            "candidate_favorable_rate": (
                candidate_favorable / resolved if resolved else None
            ),
        },
    }


def build_outcome_evidence(
    rows: List[Dict[str, Any]],
    *,
    missing_outcome_count: Optional[int] = None,
) -> Dict[str, Any]:
    """Grouped per-horizon outcome and benchmark evidence.

    `rows` are joined pair-outcome records (pairs WITHOUT any outcome row
    never appear here — pass their count via `missing_outcome_count`, taken
    from the evaluation records, so the response never hides them).
    """
    groups: Dict[Tuple, Tuple[Dict[str, Any], List[Dict[str, Any]]]] = {}
    for row in rows:
        identity = _identity(row)
        key = tuple(identity[f] for f in GROUP_FIELDS)
        if key not in groups:
            groups[key] = (identity, [])
        groups[key][1].append(row)

    group_payloads: List[Dict[str, Any]] = []
    for key, (identity, group_rows) in sorted(
        groups.items(), key=lambda item: tuple(str(k) for k in item[0])
    ):
        statuses = [
            (r.get("outcome") or {}).get("outcome_status") for r in group_rows
        ]
        matured = sum(1 for s in statuses if s == "complete")
        errored = sum(1 for s in statuses if s == "error")
        pending = len(group_rows) - matured - errored
        group_payloads.append({
            **identity,
            "pair_count": len(group_rows),
            "matured_outcome_count": matured,
            "pending_outcome_count": pending,
            "error_outcome_count": errored,
            "by_window": [
                _horizon_evidence(group_rows, w) for w in HOLDING_WINDOWS
            ],
        })

    return {
        "contract_version": OUTCOME_EVIDENCE_CONTRACT_VERSION,
        "total_outcome_rows": len(rows),
        # Pairs with NO outcome row (from the evaluation records) — reported
        # explicitly, never converted to zero returns.
        "missing_outcome_count": missing_outcome_count,
        "groups": group_payloads,
    }
