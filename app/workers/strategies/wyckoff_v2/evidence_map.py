"""evidence.v1 mapping for wyckoff_mtf.v2 (Phase 9C1).

Uses existing EvidenceItem / EvidenceBundle without modifying evidence.py.
Decision-relevant event candidates are mandatory; optional candidates fill
remaining capacity deterministically. Ranking is soft evidence only.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.workers.strategies.evidence import EvidenceBundle, EvidenceItem
from app.workers.strategies.wyckoff_v2.constants import (
    DECISION_POLICY_VERSION,
    EVIDENCE_VERSION,
    STRATEGY_CODE,
    STRATEGY_VERSION,
    event_key,
    resolve_config,
)
from app.workers.strategies.wyckoff_v2.models import (
    CompletedAggregationResult,
    EventCandidate,
    EventDetectionResult,
    FourHourTriggerResult,
    HTFContextResult,
    InvalidationResult,
    PhaseClassificationResult,
    PolicyDecisionResult,
    RangeCandidate,
    RankingResult,
    ReadinessResult,
    StructureClassificationResult,
)


class EvidenceMappingError(ValueError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


_STATUS_ORDER = {
    "confirmed": 0,
    "confirmation_pending": 1,
    "candidate": 2,
    "unknown": 3,
    "contradicted": 4,
}


def _item(**kwargs: Any) -> EvidenceItem:
    return EvidenceItem(**kwargs)


def decision_relevant_candidate_ids(
    *,
    structure: Optional[StructureClassificationResult],
    phase_result: Optional[PhaseClassificationResult],
    invalidation: Optional[InvalidationResult],
    event_result: Optional[EventDetectionResult],
) -> Set[str]:
    ids: Set[str] = set()
    if structure is not None:
        ids.update(structure.accumulation_candidate_ids)
        ids.update(structure.distribution_candidate_ids)
    if phase_result is not None:
        from app.workers.strategies.wyckoff_v2.policy import PHASE_ORDINAL

        for cand in phase_result.candidates:
            if not cand.sequence_valid:
                continue
            if phase_result.selected_phase is not None:
                if PHASE_ORDINAL.get(cand.phase, 0) > PHASE_ORDINAL.get(
                    phase_result.selected_phase, 0
                ):
                    continue
            ids.update(cand.supporting_candidate_ids)
    if invalidation is not None:
        ids.update(invalidation.source_event_ids)

    # Closure: supporting dependencies of retained candidates.
    if event_result is not None:
        by_id = {c.candidate_id: c for c in event_result.candidates}
        frontier = set(ids)
        while frontier:
            nxt: Set[str] = set()
            for cid in frontier:
                c = by_id.get(cid)
                if c is None:
                    continue
                for sid in c.supporting_candidate_ids:
                    if sid not in ids:
                        ids.add(sid)
                        nxt.add(sid)
            frontier = nxt
    return ids


def bound_event_candidates(
    event_result: Optional[EventDetectionResult],
    *,
    decision_relevant: Set[str],
    max_candidates: int,
) -> Tuple[List[EventCandidate], Dict[str, Any]]:
    if event_result is None:
        return [], {
            "original_candidate_count": 0,
            "included_candidate_count": 0,
            "omitted_candidate_count": 0,
            "candidates_truncated": False,
            "included_decision_relevant_count": 0,
            "omitted_counts_by_status": {},
        }

    all_cands = list(event_result.candidates)
    relevant = [c for c in all_cands if c.candidate_id in decision_relevant]
    if len(relevant) > max_candidates:
        raise EvidenceMappingError(
            "decision_evidence_limit_exceeded",
            f"decision-relevant candidates {len(relevant)} exceed cap {max_candidates}",
        )

    optional = [c for c in all_cands if c.candidate_id not in decision_relevant]

    def _opt_key(c: EventCandidate) -> tuple:
        usable_rank = 0 if c.usable_for_structure else 1
        return (
            usable_rank,
            _STATUS_ORDER.get(c.status, 99),
            c.date,
            c.family,
            c.event_code,
            c.candidate_id,
        )

    optional_sorted = sorted(optional, key=_opt_key)
    remaining = max_candidates - len(relevant)
    included_optional = optional_sorted[:remaining]
    included = relevant + included_optional
    included.sort(key=lambda c: (c.date, c.index, c.family, c.event_code, c.candidate_id))

    omitted = [c for c in all_cands if c.candidate_id not in {x.candidate_id for x in included}]
    omitted_counts: Dict[str, int] = {}
    for c in omitted:
        omitted_counts[c.status] = omitted_counts.get(c.status, 0) + 1

    meta = {
        "original_candidate_count": len(all_cands),
        "included_candidate_count": len(included),
        "omitted_candidate_count": len(omitted),
        "candidates_truncated": len(omitted) > 0,
        "included_decision_relevant_count": len(relevant),
        "omitted_counts_by_status": dict(sorted(omitted_counts.items())),
    }
    return included, meta


def build_evidence_bundle(
    *,
    symbol: str,
    policy: PolicyDecisionResult,
    readiness: Optional[ReadinessResult],
    aggregation: Optional[CompletedAggregationResult],
    htf: Optional[HTFContextResult],
    selected_range: Optional[RangeCandidate],
    event_result: Optional[EventDetectionResult],
    structure: Optional[StructureClassificationResult],
    phase_result: Optional[PhaseClassificationResult],
    trigger: Optional[FourHourTriggerResult],
    invalidation: Optional[InvalidationResult],
    ranking: Optional[RankingResult],
    market_data_as_of: Optional[str],
    last_close: Optional[float] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[EvidenceBundle, Dict[str, Any], List[EventCandidate]]:
    """Build a complete evidence.v1 bundle for any verdict including AVOID."""
    cfg = resolve_config(config)
    evid_cap = int(cfg["max_event_candidates_in_evidence"])
    min_price = float(cfg["min_price"])
    as_of = market_data_as_of

    decision_ids = decision_relevant_candidate_ids(
        structure=structure,
        phase_result=phase_result,
        invalidation=invalidation,
        event_result=event_result,
    )
    included, bound_meta = bound_event_candidates(
        event_result, decision_relevant=decision_ids, max_candidates=evid_cap
    )

    items: List[EvidenceItem] = []
    missing: List[str] = []
    contradictions: List[str] = []
    hard: Dict[str, Any] = {}

    def add(item: EvidenceItem) -> None:
        items.append(item)
        if item.required:
            hard[item.code] = item.state

    # Readiness
    if readiness is not None:
        rd = readiness.to_dict()
        add(
            _item(
                code="latest_bar_completion",
                category="readiness",
                source_type="market_data",
                state="pass" if readiness.ready else (
                    "unknown"
                    if "unconfirmed_bar_completion" in readiness.reason_codes
                    else "fail"
                ),
                raw_value=dict(readiness.latest_bar_completion),
                required=True,
                timeframe="1d",
                as_of=as_of,
                reason_code=readiness.status,
            )
        )
        readiness_fields = (
            ("history_depth", readiness.available_completed_bars),
            ("monthly_period_sufficiency", readiness.available_completed_monthly_periods),
            ("weekly_period_sufficiency", readiness.available_completed_weekly_periods),
            ("daily_structure_sufficiency", readiness.available_completed_bars),
            ("volume_coverage", readiness.volume_coverage),
        )
        for code, raw in readiness_fields:
            add(
                _item(
                    code=code,
                    category="readiness",
                    source_type="market_data",
                    state="pass" if readiness.ready else "fail",
                    raw_value=raw,
                    required=True,
                    timeframe="1d",
                    as_of=as_of,
                    threshold={
                        "history_depth": readiness.desired_history_bars,
                        "monthly_period_sufficiency": readiness.required_monthly_periods,
                        "weekly_period_sufficiency": readiness.required_weekly_periods,
                        "daily_structure_sufficiency": readiness.required_daily_structure_bars,
                        "volume_coverage": None,
                    }.get(code),
                )
            )
        missing.extend(list(readiness.reason_codes))
        _ = rd
    else:
        missing.append("readiness")

    # Minimum-price hard filter
    price_pass = (
        last_close is not None
        and isinstance(last_close, (int, float))
        and not isinstance(last_close, bool)
        and float(last_close) >= min_price
    )
    add(
        _item(
            code="minimum_price",
            category="hard_filter",
            source_type="market_data",
            state="pass" if price_pass else "fail",
            raw_value=None if last_close is None else float(last_close),
            threshold=min_price,
            operator=">=",
            required=True,
            timeframe="1d",
            as_of=as_of,
            reason_code=None if price_pass else "price_below_minimum",
        )
    )

    # HTF
    if htf is not None:
        for code, raw, nv in (
            ("monthly_bias", htf.monthly_bias, None),
            ("monthly_slope", htf.monthly_slope_pct, htf.monthly_trend_quality),
            ("monthly_window_structure", htf.monthly_window_structure, None),
            ("weekly_bias", htf.weekly_bias, None),
            ("weekly_slope", htf.weekly_slope_pct, htf.weekly_trend_quality),
            ("weekly_window_structure", htf.weekly_window_structure, None),
            ("htf_alignment", htf.htf_alignment, None),
        ):
            state = "unknown" if raw in (None, "unknown") else "neutral"
            if code.endswith("bias"):
                state = {
                    "up": "positive",
                    "down": "negative",
                    "neutral": "neutral",
                    "unknown": "unknown",
                }.get(str(raw), "unknown")
            if code == "htf_alignment":
                state = {
                    "aligned_up": "positive",
                    "aligned_down": "negative",
                    "contradiction": "fail",
                    "mixed": "neutral",
                    "unknown": "unknown",
                }.get(str(raw), "unknown")
            add(
                _item(
                    code=code,
                    category="htf",
                    source_type="strategy",
                    state=state,
                    raw_value=raw,
                    normalized_value=nv,
                    required=code == "htf_alignment",
                    timeframe="1M" if code.startswith("monthly") else (
                        "1w" if code.startswith("weekly") else "multi"
                    ),
                    as_of=htf.as_of_date,
                )
            )
        contradictions.extend(list(htf.contradiction_codes))
        missing.extend(list(htf.missing_data))

    # Range
    if selected_range is not None:
        add(
            _item(
                code="selected_trading_range",
                category="range",
                source_type="strategy",
                state="pass" if selected_range.valid else "fail",
                raw_value=selected_range.to_dict(),
                required=True,
                timeframe="1d",
                as_of=selected_range.as_of_date,
            )
        )
        add(
            _item(
                code="range_touch_counts",
                category="range",
                source_type="strategy",
                state="neutral",
                raw_value={
                    "support": selected_range.support_touch_cluster_count,
                    "resistance": selected_range.resistance_touch_cluster_count,
                },
                timeframe="1d",
                as_of=selected_range.as_of_date,
            )
        )
        add(
            _item(
                code="range_containment",
                category="range",
                source_type="strategy",
                state="neutral" if selected_range.containment_fraction is not None else "unknown",
                raw_value=selected_range.containment_fraction,
                timeframe="1d",
                as_of=selected_range.as_of_date,
            )
        )
        add(
            _item(
                code="range_breakout_contamination",
                category="range",
                source_type="strategy",
                state=(
                    "neutral"
                    if selected_range.breakout_contamination_fraction is not None
                    else "unknown"
                ),
                raw_value=selected_range.breakout_contamination_fraction,
                timeframe="1d",
                as_of=selected_range.as_of_date,
            )
        )
        add(
            _item(
                code="range_quality",
                category="range",
                source_type="strategy",
                state="neutral" if selected_range.range_quality is not None else "unknown",
                raw_value=selected_range.range_quality,
                normalized_value=selected_range.range_quality,
                timeframe="1d",
                as_of=selected_range.as_of_date,
            )
        )

    # Events
    if event_result is not None:
        by_key = {
            k: [c.candidate_id for c in v]
            for k, v in event_result.candidates_by_code.items()
        }
        add(
            _item(
                code="event_detection_summary",
                category="events",
                source_type="strategy",
                state="neutral",
                raw_value={
                    "candidate_count": len(event_result.candidates),
                    "candidates_by_code": by_key,
                    "candidates_truncated": event_result.candidates_truncated,
                },
                timeframe="1d",
                as_of=event_result.as_of_date,
            )
        )
        add(
            _item(
                code="bounded_event_sequence",
                category="events",
                source_type="strategy",
                state="neutral",
                raw_value=[c.to_dict() for c in included],
                metadata=bound_meta,
                timeframe="1d",
                as_of=event_result.as_of_date,
            )
        )

    # Structure / phases
    if structure is not None:
        st_state = {
            "recognized": "pass",
            "ambiguous": "fail",
            "unknown": "unknown",
        }.get(structure.state, "unknown")
        add(
            _item(
                code="structure_classification",
                category="structure",
                source_type="strategy",
                state=st_state,
                raw_value=structure.to_dict(),
                required=True,
                timeframe="1d",
                as_of=structure.as_of_date,
            )
        )
        contradictions.extend(list(structure.contradiction_codes))

    if phase_result is not None:
        add(
            _item(
                code="phase_state",
                category="phases",
                source_type="strategy",
                state="pass" if phase_result.selected_phase else "unknown",
                raw_value=phase_result.phase_state,
                timeframe="1d",
                as_of=phase_result.as_of_date,
            )
        )
        add(
            _item(
                code="cumulative_phase_candidates",
                category="phases",
                source_type="strategy",
                state="neutral",
                raw_value=[c.to_dict() for c in phase_result.candidates],
                timeframe="1d",
                as_of=phase_result.as_of_date,
            )
        )
        add(
            _item(
                code="selected_phase",
                category="phases",
                source_type="strategy",
                state="pass" if phase_result.selected_phase else "unknown",
                raw_value=phase_result.selected_phase,
                timeframe="1d",
                as_of=phase_result.as_of_date,
            )
        )

    # 4H
    if trigger is not None:
        t_state = {
            "confirmed": "pass",
            "missing": "fail",
            "contradicted": "fail",
            "unknown": "unknown",
        }.get(trigger.state, "unknown")
        add(
            _item(
                code="four_hour_readiness",
                category="confirmation",
                source_type="market_data",
                state="unknown" if trigger.state == "unknown" else "pass",
                raw_value={
                    "available_completed_bars": trigger.available_completed_bars,
                    "staleness_sessions": trigger.staleness_sessions,
                    "enabled": trigger.enabled,
                },
                required=True,
                timeframe="4h",
                as_of=as_of,
            )
        )
        add(
            _item(
                code="four_hour_trigger",
                category="confirmation",
                source_type="strategy",
                state=t_state,
                raw_value=trigger.to_dict(),
                required=True,
                timeframe="4h",
                as_of=as_of,
                reason_code=trigger.reason_codes[0] if trigger.reason_codes else None,
            )
        )
        missing.extend(list(trigger.missing_data))

    # Invalidation
    if invalidation is not None:
        add(
            _item(
                code="invalidation_level",
                category="risk",
                source_type="risk",
                state="neutral" if invalidation.available else "unknown",
                raw_value=invalidation.to_dict(),
                required=True,
                timeframe="1d",
                as_of=invalidation.as_of,
                reason_code=invalidation.reason,
            )
        )

    # Policy
    add(
        _item(
            code="decision_policy_trace",
            category="policy",
            source_type="strategy",
            state="pass" if policy.verdict != "AVOID" else "fail",
            raw_value=policy.to_dict(),
            required=True,
            timeframe="multi",
            as_of=as_of,
            reason_code=policy.reason_code,
        )
    )
    add(
        _item(
            code="rollout_gate",
            category="policy",
            source_type="strategy",
            state="pass" if policy.allow_enter else "fail",
            raw_value={"allow_enter": policy.allow_enter},
            required=True,
            timeframe="multi",
            as_of=as_of,
            reason_code=None if policy.allow_enter else "enter_disabled_shadow_only",
        )
    )

    # Ranking components
    if ranking is not None:
        for name, value in ranking.components.items():
            add(
                _item(
                    code=name,
                    category="ranking",
                    source_type="strategy",
                    state="neutral" if value is not None else "unknown",
                    raw_value=value,
                    normalized_value=value,
                    timeframe="multi",
                    as_of=as_of,
                )
            )
        add(
            _item(
                code="ranking_score",
                category="ranking",
                source_type="strategy",
                state="neutral" if ranking.ranking_score is not None else "unknown",
                raw_value=ranking.ranking_score,
                normalized_value=ranking.ranking_score,
                timeframe="multi",
                as_of=as_of,
            )
        )

    # Merge hard filters from policy gates
    for gate, passed in policy.required_gate_results.items():
        hard.setdefault(gate, "pass" if passed else "fail")

    timeframe_summary = {
        "1d": {"market_data_as_of": as_of},
        "4h": {
            "enabled": None if trigger is None else trigger.enabled,
            "state": None if trigger is None else trigger.state,
        },
        "1w": {"bias": None if htf is None else htf.weekly_bias},
        "1M": {"bias": None if htf is None else htf.monthly_bias},
    }

    bundle = EvidenceBundle(
        strategy_code=STRATEGY_CODE,
        strategy_version=STRATEGY_VERSION,
        decision_policy_version=DECISION_POLICY_VERSION,
        symbol=symbol,
        verdict=policy.verdict,
        setup_state=policy.setup_state,
        trigger_state=policy.trigger_state,
        market_data_as_of=as_of,
        items=items,
        hard_filter_summary=hard,
        missing_data=sorted(set(missing)),
        contradictions=sorted(set(contradictions)),
        timeframe_summary=timeframe_summary,
        ranking_components={} if ranking is None else dict(ranking.components),
        ranking_score=None if ranking is None else ranking.ranking_score,
        evidence_version=EVIDENCE_VERSION,
    )
    return bundle, bound_meta, included
