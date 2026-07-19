"""Structured decision cards (Phase 5.2).

A decision card is a deterministic, data-only explanation of a StrategyResult:
what was interesting, why, what is missing for ENTER, and what should happen
next. Built ONLY from StrategyResult fields — no LLM, no invented values.

Cards are persisted inside `signals.details.decision_card` so the existing
table/API can serve them without a schema change.
"""

from typing import Any, Dict, List, Optional

from app.workers.strategies.base import (
    StrategyDecision,
    StrategyResult,
    StrategySide,
)


CARD_VERSION = "decision_card.v1"


def _timeframe_summary(result: StrategyResult) -> Dict[str, Any]:
    """Compact per-timeframe evidence, using only what the strategy reported."""
    details = result.details or {}
    summary: Dict[str, Any] = {"required_timeframes": list(result.required_timeframes)}
    # Wyckoff-style context, included only when the strategy produced it.
    for key in ("monthly_bias", "weekly_phase", "weekly_aligned"):
        if key in details:
            summary[key] = details[key]
    if isinstance(details.get("timeframes"), dict):
        summary["bars"] = details["timeframes"]
    return summary


def _wants_4h(result: StrategyResult) -> bool:
    return "4h" in (result.required_timeframes or [])


def _has_4h(result: StrategyResult) -> bool:
    details = result.details or {}
    tf = details.get("timeframes")
    if isinstance(tf, dict) and "has_4h" in tf:
        return bool(tf["has_4h"])
    sc = result.score_components or {}
    return bool(sc.get("has_4h", False))


def _next_action(result: StrategyResult) -> str:
    if result.decision == StrategyDecision.ENTER:
        if _wants_4h(result):
            return "Entry trigger confirmed on 4H. Review stop/invalidation before action."
        return "Signal criteria met. Review the evidence and risk before action."
    if result.decision == StrategyDecision.WATCH:
        if _wants_4h(result):
            return "Wait for 4H trigger confirmation. No ENTER signal yet."
        return "Setup valid but unconfirmed. Wait for the strategy's trigger."
    return "No action. Candidate did not qualify."


def _trigger_needed(result: StrategyResult) -> Optional[str]:
    if result.decision != StrategyDecision.WATCH:
        return None
    if _wants_4h(result):
        side = result.side.value if result.side != StrategySide.UNKNOWN else "directional"
        return f"4H close breaking local structure in the {side} direction"
    return "strategy trigger not yet confirmed"


def _missing_data(result: StrategyResult) -> List[str]:
    missing: List[str] = []
    if result.decision == StrategyDecision.WATCH and _wants_4h(result) and not _has_4h(result):
        missing.append("4h_data")
    return missing


def _risk_notes(result: StrategyResult) -> List[str]:
    notes: List[str] = []
    if result.decision == StrategyDecision.ENTER:
        if result.stop_price is None:
            notes.append("no stop_price defined by the strategy")
        if result.target_price is None:
            notes.append("no deterministic target_price (v1)")
    if result.decision == StrategyDecision.WATCH:
        notes.append("not an entry signal; trigger unconfirmed")
    notes.append("signal value unproven; compare against baselines via outcome tracking")
    return notes


def build_decision_card(result: StrategyResult) -> Dict[str, Any]:
    """Build a deterministic decision card from a StrategyResult.

    Never invents prices/direction: fields the strategy did not set stay None.
    """
    side = result.side.value if result.side else StrategySide.UNKNOWN.value
    setup = result.setup_type or "unknown_setup"
    title = f"{result.decision.value}: {result.symbol} {setup}"
    if result.side not in (None, StrategySide.UNKNOWN):
        title = f"{result.decision.value}: {result.symbol} {side} {setup}"

    return {
        "card_version": CARD_VERSION,
        "title": title,
        "decision": result.decision.value,
        "symbol": result.symbol,
        "pattern_code": result.pattern_code,
        "side": side,
        "setup_type": result.setup_type,
        "score": result.score,
        "why_now": result.reason,
        "timeframe_summary": _timeframe_summary(result),
        "trigger_needed": _trigger_needed(result),
        "confirmation_needed": _trigger_needed(result) is not None,
        "entry_price": result.entry_price,
        "stop_price": result.stop_price,
        "target_price": result.target_price,
        "invalidation": result.invalidation,
        "risk_notes": _risk_notes(result),
        "missing_data": _missing_data(result),
        "next_action": _next_action(result),
        "raw_evidence": dict(result.score_components or {}),
        "strategy_version": result.strategy_version,
    }
