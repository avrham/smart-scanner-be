"""WyckoffMTFV2Strategy orchestration — Phase 9C1 (unregistered).

Orchestrates readiness → aggregation → range → HTF → events → structure →
phases → invalidation → 4H trigger → ranking → policy → evidence.v1 →
StrategyResult.

Pure evaluation path: no providers, DB, persistence, registry or LLM.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from app.workers.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.wyckoff_v2.aggregation import (
    aggregate_completed_timeframes,
)
from app.workers.strategies.wyckoff_v2.constants import (
    DECISION_POLICY_VERSION,
    EVIDENCE_VERSION,
    RANKING_VERSION,
    STRATEGY_CODE,
    STRATEGY_VERSION,
    default_config,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.context_htf import measure_htf_context
from app.workers.strategies.wyckoff_v2.events import detect_event_candidates
from app.workers.strategies.wyckoff_v2.evidence_map import build_evidence_bundle
from app.workers.strategies.wyckoff_v2.phases import classify_phases, classify_structure
from app.workers.strategies.wyckoff_v2.policy import (
    compute_invalidation,
    compute_ranking,
    evaluate_policy,
)
from app.workers.strategies.wyckoff_v2.ranges import detect_trading_ranges
from app.workers.strategies.wyckoff_v2.readiness import assess_data_readiness
from app.workers.strategies.wyckoff_v2.trigger_4h import analyze_4h_trigger


# Injectable clock for deterministic tests (module-local).
_UTC_NOW = lambda: datetime.now(timezone.utc)  # noqa: E731


def set_utc_now_for_tests(fn) -> None:
    """Test-only hook to pin evaluation time when data_meta omits it."""
    global _UTC_NOW
    _UTC_NOW = fn


def _parse_eval_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime().astimezone(timezone.utc)


def _setup_type(
    structure,
    phase_result,
) -> str:
    if structure is None or structure.state != "recognized":
        return "unknown_structure"
    family = structure.classification
    if phase_result is None or phase_result.selected_phase is None:
        return f"{family}_unknown_phase"
    return f"{family}_phase_{phase_result.selected_phase.lower()}"


def _side_enum(side: str) -> StrategySide:
    if side == "LONG":
        return StrategySide.LONG
    if side == "SHORT":
        return StrategySide.SHORT
    return StrategySide.UNKNOWN


def _decision_enum(verdict: str) -> StrategyDecision:
    if verdict == "ENTER":
        return StrategyDecision.ENTER
    if verdict == "WATCH":
        return StrategyDecision.WATCH
    return StrategyDecision.AVOID


def _score_components(
    *,
    htf,
    selected_range,
    structure,
    phase_result,
    trigger,
) -> Dict[str, Any]:
    comps: Dict[str, Any] = {
        "monthly_slope_pct": None if htf is None else htf.monthly_slope_pct,
        "weekly_slope_pct": None if htf is None else htf.weekly_slope_pct,
        "range_width": None if selected_range is None else selected_range.width,
        "range_width_atr_multiple": (
            None if selected_range is None else selected_range.width_atr_multiple
        ),
        "support_touch_cluster_count": (
            None
            if selected_range is None
            else selected_range.support_touch_cluster_count
        ),
        "resistance_touch_cluster_count": (
            None
            if selected_range is None
            else selected_range.resistance_touch_cluster_count
        ),
        "containment_fraction": (
            None if selected_range is None else selected_range.containment_fraction
        ),
        "breakout_contamination_fraction": (
            None
            if selected_range is None
            else selected_range.breakout_contamination_fraction
        ),
        "range_volume_coverage": (
            None if selected_range is None else selected_range.volume_coverage
        ),
        "accumulation_confirmed_type_count": (
            None
            if structure is None
            else structure.accumulation_confirmed_type_count
        ),
        "distribution_confirmed_type_count": (
            None
            if structure is None
            else structure.distribution_confirmed_type_count
        ),
        "selected_phase_ordinal": (
            None
            if phase_result is None or phase_result.selected_phase is None
            else {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5}.get(
                phase_result.selected_phase
            )
        ),
        "four_hour_current_close": None if trigger is None else trigger.current_close,
        "four_hour_trigger_level": None if trigger is None else trigger.trigger_level,
    }
    return comps


class WyckoffMTFV2Strategy(Strategy):
    """Unregistered Phase 9C1 strategy plugin."""

    pattern_code = STRATEGY_CODE
    version = STRATEGY_VERSION
    decision_policy_version = DECISION_POLICY_VERSION
    required_timeframes = ["1d", "1w", "1M", "4h"]
    min_daily_bars = 200

    def default_config(self) -> Dict[str, Any]:
        return default_config()

    def evaluate(self, df: pd.DataFrame, context: StrategyContext) -> StrategyResult:
        cfg = resolve_config(context.config)
        meta = dict(context.data_meta or {})

        if "evaluation_time_utc" in meta and meta["evaluation_time_utc"] is not None:
            evaluation_time_utc = _parse_eval_time(meta["evaluation_time_utc"])
        else:
            evaluation_time_utc = _UTC_NOW()

        as_of_date = meta.get("as_of_date")
        explicit_completed = meta.get("explicit_completed")
        provider_hard_cap_bars = meta.get("provider_hard_cap_bars")
        df_4h = meta.get("df_4h")

        # 3. Daily readiness
        readiness = assess_data_readiness(
            df,
            config=cfg,
            evaluation_time_utc=evaluation_time_utc,
            explicit_completed=explicit_completed,
            provider_hard_cap_bars=provider_hard_cap_bars,
            as_of_date=as_of_date,
        )

        aggregation = None
        htf = None
        range_result = None
        selected_range = None
        event_result = None
        structure = None
        phase_result = None
        invalidation = None
        trigger = None
        ranking = None
        last_close = None
        completed_daily = readiness.completed_daily_frame
        market_data_as_of = readiness.market_data_as_of
        pinned_as_of = as_of_date or market_data_as_of

        if readiness.ready and completed_daily is not None and len(completed_daily) > 0:
            if pinned_as_of is None:
                pinned_as_of = (
                    pd.Timestamp(completed_daily["date"].iloc[-1]).date().isoformat()
                )
            market_data_as_of = pinned_as_of
            last_close = float(completed_daily["close"].iloc[-1])
            min_price = float(cfg["min_price"])

            # Early hard-filter: price below minimum — skip later phases.
            if last_close < min_price:
                pass
            else:
                # 4. Aggregation
                aggregation = aggregate_completed_timeframes(
                    completed_daily,
                    evaluation_time_utc=evaluation_time_utc,
                    exchange_timezone=str(cfg["exchange_timezone"]),
                    as_of_date=pinned_as_of,
                )

                # 5. Trading range
                range_result = detect_trading_ranges(
                    completed_daily, config=cfg, as_of_date=pinned_as_of
                )
                selected_range = range_result.selected_range
                if selected_range is not None and selected_range.as_of_date != pinned_as_of:
                    raise ValueError(
                        "selected_range.as_of_date must equal pinned as_of_date "
                        f"({selected_range.as_of_date!r} != {pinned_as_of!r})"
                    )

                # 6. HTF
                htf = measure_htf_context(
                    aggregation, as_of_date=pinned_as_of, config=cfg
                )

                if selected_range is not None and selected_range.valid:
                    # 7. Events — never reuse frozen ranges on the strategy path
                    event_result = detect_event_candidates(
                        completed_daily,
                        selected_range,
                        as_of_date=pinned_as_of,
                        config=cfg,
                        allow_frozen_range_reuse=False,
                    )
                    # 8. Structure
                    structure = classify_structure(
                        event_result,
                        as_of_date=pinned_as_of,
                        config=cfg,
                        htf_context=htf,
                    )
                    # 9. Phases
                    phase_result = classify_phases(
                        completed_daily,
                        selected_range,
                        event_result,
                        as_of_date=pinned_as_of,
                        config=cfg,
                        htf_context=htf,
                        structure=structure,
                    )
                    # 10. Invalidation
                    invalidation = compute_invalidation(
                        structure=structure,
                        selected_range=selected_range,
                        phase_result=phase_result,
                        as_of=pinned_as_of,
                        config=cfg,
                    )
                    # 11. 4H trigger
                    side_hint = "UNKNOWN"
                    if structure is not None and structure.state == "recognized":
                        if structure.classification == "accumulation":
                            side_hint = "LONG"
                        elif structure.classification == "distribution":
                            side_hint = "SHORT"
                    trigger = analyze_4h_trigger(
                        df_4h,
                        side=side_hint,
                        evaluation_time_utc=evaluation_time_utc,
                        daily_frame=completed_daily,
                        daily_market_data_as_of=pinned_as_of,
                        config=cfg,
                    )
                else:
                    invalidation = compute_invalidation(
                        structure=None,
                        selected_range=selected_range,
                        phase_result=None,
                        as_of=pinned_as_of or "",
                        config=cfg,
                    )

                # 12. Ranking (never gates)
                ranking = compute_ranking(
                    structure=structure,
                    selected_range=selected_range,
                    phase_result=phase_result,
                    htf=htf,
                    trigger=trigger,
                    config=cfg,
                )

        # 13. Policy
        policy = evaluate_policy(
            readiness=readiness,
            selected_range=selected_range,
            structure=structure,
            phase_result=phase_result,
            htf=htf,
            trigger=trigger,
            invalidation=invalidation,
            event_result=event_result,
            last_close=last_close,
            config=cfg,
        )

        # 14. Evidence
        evidence_bundle, bound_meta, included_events = build_evidence_bundle(
            symbol=context.symbol,
            policy=policy,
            readiness=readiness,
            aggregation=aggregation,
            htf=htf,
            selected_range=selected_range,
            event_result=event_result,
            structure=structure,
            phase_result=phase_result,
            trigger=trigger,
            invalidation=invalidation,
            ranking=ranking,
            market_data_as_of=market_data_as_of,
            last_close=last_close,
            config=cfg,
        )

        # Details event list uses the larger details cap.
        details_cap = int(cfg["max_event_candidates_in_details"])
        if event_result is not None:
            from app.workers.strategies.wyckoff_v2.evidence_map import (
                bound_event_candidates,
                decision_relevant_candidate_ids,
            )

            decision_ids = decision_relevant_candidate_ids(
                structure=structure,
                phase_result=phase_result,
                invalidation=invalidation,
                event_result=event_result,
            )
            details_events, details_bound = bound_event_candidates(
                event_result,
                decision_relevant=decision_ids,
                max_candidates=details_cap,
            )
        else:
            details_events, details_bound = [], bound_meta

        score_comps = _score_components(
            htf=htf,
            selected_range=selected_range,
            structure=structure,
            phase_result=phase_result,
            trigger=trigger,
        )

        snapshot_date = None
        if market_data_as_of:
            snapshot_date = str(market_data_as_of)[:10]

        waiting_reason = (
            None
            if not policy.waiting_reasons
            else ",".join(policy.waiting_reasons)
        )
        rejection_reason = (
            policy.reason_code if policy.verdict == "AVOID" else None
        )

        entry_price = None
        if policy.verdict == "ENTER" and trigger is not None:
            entry_price = trigger.trigger_price

        invalidation_level = (
            None if invalidation is None or not invalidation.available else invalidation.level
        )

        details: Dict[str, Any] = {
            "symbol": context.symbol,
            "snapshot_date": snapshot_date,
            "market_data_as_of": market_data_as_of,
            "score_version": RANKING_VERSION,
            "decision_policy_version": DECISION_POLICY_VERSION,
            "evidence_version": EVIDENCE_VERSION,
            "setup_state": policy.setup_state,
            "trigger_state": policy.trigger_state,
            "rejection_reason": rejection_reason,
            "waiting_reason": waiting_reason,
            "thresholds_used": {
                k: cfg[k]
                for k in (
                    "allow_enter",
                    "enable_4h_trigger",
                    "require_4h_trigger_for_enter",
                    "trigger_lookback_4h",
                    "max_4h_staleness_sessions",
                    "avoid_on_htf_contradiction",
                    "enter_eligible_phases",
                    "structure_quality_full_event_types",
                    "max_event_candidates_in_evidence",
                    "max_event_candidates_in_details",
                    "min_price",
                    "event_invalidation_buffer_atr_multiple",
                )
                if k in cfg
            },
            "score_components": score_comps,
            "readiness": readiness.to_dict(),
            "aggregation": None if aggregation is None else aggregation.to_dict(),
            "htf_context": None if htf is None else htf.to_dict(),
            "trading_range": None if range_result is None else range_result.to_dict(),
            "event_detection_summary": (
                None
                if event_result is None
                else {
                    "candidate_count": len(event_result.candidates),
                    "candidates_truncated": event_result.candidates_truncated,
                    "range_candidate_id": event_result.range_candidate_id,
                    "as_of_date": event_result.as_of_date,
                }
            ),
            "event_candidates": [c.to_dict() for c in details_events],
            "event_evidence_bounding": details_bound,
            "structure": None if structure is None else structure.to_dict(),
            "phase_state": None if phase_result is None else phase_result.phase_state,
            "phase_candidates": (
                None
                if phase_result is None
                else [c.to_dict() for c in phase_result.candidates]
            ),
            "selected_phase": None if phase_result is None else phase_result.selected_phase,
            "four_hour_trigger": None if trigger is None else trigger.to_dict(),
            "invalidation": None if invalidation is None else invalidation.to_dict(),
            "ranking": None if ranking is None else ranking.to_dict(),
            "contradictions": list(evidence_bundle.contradictions),
            "missing_data": list(evidence_bundle.missing_data),
            "candidates_truncated": bool(
                (event_result.candidates_truncated if event_result else False)
                or details_bound.get("candidates_truncated", False)
            ),
            "evidence": evidence_bundle.to_dict(),
            "policy": policy.to_dict(),
        }

        # Guard: no DataFrames in details
        for key, value in details.items():
            if isinstance(value, pd.DataFrame):
                raise ValueError(f"DataFrame leaked into details[{key!r}]")

        reason = (
            f"{policy.verdict}:{policy.reason_code}"
            if policy.reason_code
            else policy.verdict
        )

        return StrategyResult(
            decision=_decision_enum(policy.verdict),
            symbol=context.symbol,
            pattern_code=self.pattern_code,
            score=None if ranking is None else ranking.ranking_score,
            side=_side_enum(policy.side),
            reason=reason,
            rejection_reason=rejection_reason,
            details=details,
            score_components=score_comps,
            required_timeframes=list(self.required_timeframes),
            entry_price=entry_price,
            stop_price=None,
            target_price=None,
            invalidation=invalidation_level,
            setup_type=_setup_type(structure, phase_result),
            strategy_version=self.version,
        )
