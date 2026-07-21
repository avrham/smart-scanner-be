"""Admin-triggered shadow comparison runner (Phase 8.1B1).

Answers exactly one product question per symbol:

    "Given the exact same completed OHLCV frame, resolved at the same time,
     what decision did sma150.v2 make and what decision did sma150.v3 make?"

Bounded (max 25 explicit symbols), never scheduled, never touches normal
signals/outcomes, never alters strategy enablement, preserves ENTER, WATCH
and AVOID verbatim. Disagreement is recorded, never labeled as improvement
or regression.
"""

import logging
import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.workers.patterns.config import resolve_pattern_config
from app.workers.provenance import (
    EvidenceTooLargeError,
    _bound_evidence,
    _sha256,
    canonical_json,
    config_hash,
    sanitize_config,
)
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CANDIDATE_PATTERN_CODE,
    CONTROL_ARM_CODE,
    CONTROL_PATTERN_CODE,
    EVALUATION_FINGERPRINT_VERSION,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    FRAME_FETCH_MARGIN_BARS,
    FRAME_HARD_CAP_BARS,
    MAX_DETAILS_SNAPSHOT_BYTES,
    MAX_SHADOW_SYMBOLS,
    PAIR_FINGERPRINT_VERSION,
)
from app.workers.shadow.fingerprints import (
    compute_evaluation_fingerprint,
    compute_pair_fingerprint,
    disagreement_category,
)
from app.workers.shadow.frames import (
    CanonicalFrame,
    FrameRejection,
    build_canonical_frame,
    desired_history_bars,
    shared_required_history_bars,
)
from app.workers.shadow.persistence import (
    ShadowIntegrityError,
    create_shadow_run,
    finalize_shadow_run,
    persist_shadow_pair,
)
from app.workers.shadow.serialization import (
    ShadowSerializationError,
    normalize_json_safe,
)
from app.workers.strategies import StrategyContext, get_strategy


logger = logging.getLogger(__name__)


class ShadowRequestError(ValueError):
    """Invalid shadow-comparison request (empty/oversized symbol list)."""


def normalize_shadow_symbols(symbols: Any) -> List[str]:
    """Validate + normalize the explicit symbol list.

    Uppercases, strips, removes duplicates while PRESERVING requested order.
    Rejects empty lists and lists exceeding the hard MAX_SHADOW_SYMBOLS cap.
    """
    if not isinstance(symbols, list):
        raise ShadowRequestError("symbols must be a list of ticker strings")
    seen: set = set()
    normalized: List[str] = []
    for s in symbols:
        su = str(s or "").strip().upper()
        if su and su not in seen:
            seen.add(su)
            normalized.append(su)
    if not normalized:
        raise ShadowRequestError("symbols must contain at least one ticker")
    if len(normalized) > MAX_SHADOW_SYMBOLS:
        raise ShadowRequestError(
            f"at most {MAX_SHADOW_SYMBOLS} symbols per shadow run "
            f"(got {len(normalized)})"
        )
    return normalized


async def _resolve_arm(pattern_code: str, arm_code: str) -> Dict[str, Any]:
    """Resolve one arm's REAL strategy object + frozen config identity.

    Versions come from the registered strategy object (never inferred from
    names); config comes from the existing DB resolver (operator-modified
    pattern_configs respected) merged over the strategy's own defaults, then
    sanitized with the existing secret-removal policy and hashed
    deterministically. pattern_configs is never modified.
    """
    strategy = get_strategy(pattern_code)
    resolved = await resolve_pattern_config(pattern_code, strategy.default_config())
    # The persisted config snapshot crosses the same strict JSON boundary as
    # every other shadow JSONB field. config_hash stays on the existing
    # provenance path (sanitize + canonical JSON) — hashes are unchanged.
    snapshot = normalize_json_safe(sanitize_config(resolved))
    return {
        "arm_code": arm_code,
        "strategy": strategy,
        "config": resolved,
        "config_snapshot": snapshot,
        "strategy_code": pattern_code,
        "strategy_version": strategy.version,
        "decision_policy_version": strategy.decision_policy_version,
        "config_hash": config_hash(resolved),
    }


def _bound_details(details: Dict[str, Any]) -> Dict[str, Any]:
    """Bounded, deterministic, JSON-SAFE details snapshot for one evaluation.

    Order is load-bearing: raw strategy details are first normalized through
    the strict shadow JSON boundary (pandas.Timestamp bounce dates become ISO
    strings, numpy scalars become Python numbers, unsupported objects raise
    ShadowSerializationError). The canonical original hash, the bounded
    snapshot AND the evaluation fingerprint all derive from that SAME
    normalized value — we never hash one representation and persist another.

    Returns {"snapshot", "original_sha256"}. Within bound: the snapshot is
    the normalized details verbatim. Over bound: optional keys are pruned via
    the Phase 7B deterministic pruner (mandatory decision inputs survive) and
    a reproducible `_snapshot_meta` records the original hash/size and pruned
    keys. If mandatory content alone exceeds the bound, EvidenceTooLargeError
    propagates and the caller rejects the symbol's pair.
    """
    details = normalize_json_safe(details or {})
    original_json = canonical_json(details)
    original_sha = _sha256(original_json)
    if len(original_json.encode("utf-8")) <= MAX_DETAILS_SNAPSHOT_BYTES:
        return {"snapshot": details, "original_sha256": original_sha}

    bounded, meta = _bound_evidence(details)
    snapshot = dict(bounded)
    snapshot["_snapshot_meta"] = {
        "details_original_sha256": meta["evidence_original_sha256"],
        "details_original_size_bytes": meta["evidence_original_size_bytes"],
        "details_pruned": meta["evidence_pruned"],
        "details_pruned_keys": meta["evidence_pruned_keys"],
    }
    return {"snapshot": snapshot, "original_sha256": original_sha}


def _evaluate_arm(
    arm: Dict[str, Any],
    frame: CanonicalFrame,
    run_id: str,
    *,
    latest_bar_completed: bool,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Evaluate one arm on ITS OWN copy of the canonical frame.

    The returned verdict/score/reason/details are persisted verbatim — v2 is
    never normalized into v3 semantics, AVOID is never converted, missing
    scores stay None, and missing evidence.v1 (v2) stays absent.
    """
    data_meta: Dict[str, Any] = {}
    if latest_bar_completed:
        # Only set after the shared runner PROVED the canonical last bar is
        # completed (build_canonical_frame rejects anything unproven).
        data_meta["latest_bar_completed"] = True
    if now_utc is not None:
        data_meta["evaluation_time_utc"] = now_utc

    context = StrategyContext(
        symbol=frame.symbol,
        pattern_code=arm["strategy_code"],
        config=arm["config"],
        scanner_mode="shadow",
        scan_run_id=run_id,
        data_meta=data_meta or None,
    )
    result = arm["strategy"].evaluate(frame.dataframe(), context)
    return {
        "arm_code": arm["arm_code"],
        "strategy_code": arm["strategy_code"],
        "strategy_version": arm["strategy_version"],
        "decision_policy_version": arm["decision_policy_version"],
        "config_hash": arm["config_hash"],
        "config_snapshot": arm["config_snapshot"],
        "verdict": result.verdict,
        "score": result.score,
        "reason": result.reason,
        "rejection_reason": result.rejection_reason,
        "details": result.details or {},
    }


async def run_shadow_comparison(
    provider: Any,
    symbols: List[str],
    *,
    run_id: Optional[str] = None,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Run one bounded shadow comparison over an explicit symbol list.

    One symbol failing never aborts the others; a handled run-level failure
    finalizes the run as 'failed'; a run whose every symbol was honestly
    rejected for data readiness still completes with an explicit terminal
    reason. Returns the bounded run summary.
    """
    normalized = normalize_shadow_symbols(symbols)
    run_id = run_id or str(uuid.uuid4())
    provider_name = getattr(provider, "name", None) or "unknown"

    await create_shadow_run(
        run_id,
        provider=provider_name,
        requested_symbols=normalized,
        requested_limit=len(normalized),
    )

    counts = Counter()
    categories: Counter = Counter()
    rejected: Counter = Counter()
    rejected_symbols: Dict[str, List[str]] = {}
    completion_meta: Dict[str, Any] = {}
    pair_summaries: List[Dict[str, Any]] = []

    try:
        control = await _resolve_arm(CONTROL_PATTERN_CODE, CONTROL_ARM_CODE)
        candidate = await _resolve_arm(CANDIDATE_PATTERN_CODE, CANDIDATE_ARM_CODE)

        # Shared canonical history depth, DERIVED once per run from both
        # resolved configs (SMA warm-up counted). The UNCAPPED desired value
        # is preserved: depth completeness is always judged against it, so a
        # filled 600-bar cap never masquerades as a complete 800-bar
        # lookback. Only the capped requested value is fetched/stored. The
        # fetch adds a small margin so excluding a partial latest bar can
        # never shrink an otherwise-full completed frame where the provider
        # has the data.
        desired_bars = desired_history_bars(
            control["config"], candidate["config"]
        )
        requested_history_bars = shared_required_history_bars(
            control["config"], candidate["config"]
        )
        history_depth_capped = desired_bars > FRAME_HARD_CAP_BARS
        fetch_bars = requested_history_bars + FRAME_FETCH_MARGIN_BARS

        for symbol in normalized:
            try:
                # ONE fetch per symbol, shared by both arms.
                payload = await provider.get_daily_history(
                    symbol, timeseries=fetch_bars
                )
                counts["fetched_count"] += 1
            except Exception as exc:
                logger.warning("Shadow fetch failed for %s: %s", symbol, exc)
                rejected["fetch_error"] += 1
                rejected_symbols.setdefault("fetch_error", []).append(symbol)
                continue

            try:
                frame = build_canonical_frame(
                    symbol, payload,
                    max_bars=requested_history_bars, now_utc=now_utc,
                )
            except FrameRejection as rejection:
                rejected[rejection.reason_code] += 1
                rejected_symbols.setdefault(
                    rejection.reason_code, []
                ).append(symbol)
                continue

            # Honest depth accounting: completeness is judged against the
            # UNCAPPED desired requirement — a provider with less history
            # than the configured lookback (or a lookback beyond the hard
            # cap) is recorded, never inflated; both arms still evaluate the
            # same available completed frame and their own readiness rules
            # decide.
            completion_meta[symbol] = {
                **frame.completion,
                "desired_history_bars": desired_bars,
                "requested_history_bars": requested_history_bars,
                "available_completed_bars": frame.bar_count,
                "history_depth_capped": history_depth_capped,
                "history_depth_complete": frame.bar_count >= desired_bars,
            }

            try:
                control_eval = _evaluate_arm(
                    control, frame, run_id,
                    latest_bar_completed=True, now_utc=now_utc,
                )
                candidate_eval = _evaluate_arm(
                    candidate, frame, run_id,
                    latest_bar_completed=True, now_utc=now_utc,
                )

                pair_fingerprint = compute_pair_fingerprint(
                    symbol=symbol,
                    timeframe=frame.timeframe,
                    provider=provider_name,
                    frame_hash=frame.frame_hash,
                    snapshot_date=frame.snapshot_date,
                    market_data_as_of=frame.market_data_as_of,
                    control_identity=control,
                    candidate_identity=candidate,
                )

                evaluations = []
                for ev in (control_eval, candidate_eval):
                    bounded = _bound_details(ev["details"])
                    evaluations.append({
                        "arm_code": ev["arm_code"],
                        "strategy_code": ev["strategy_code"],
                        "strategy_version": ev["strategy_version"],
                        "decision_policy_version": ev["decision_policy_version"],
                        "config_hash": ev["config_hash"],
                        "config_snapshot": ev["config_snapshot"],
                        "verdict": ev["verdict"],
                        "score": ev["score"],
                        "reason": ev["reason"],
                        "rejection_reason": ev["rejection_reason"],
                        "details_snapshot": bounded["snapshot"],
                        "evidence_original_sha256": bounded["original_sha256"],
                        "evaluation_fingerprint": compute_evaluation_fingerprint(
                            pair_fingerprint=pair_fingerprint,
                            arm_code=ev["arm_code"],
                            strategy_code=ev["strategy_code"],
                            strategy_version=ev["strategy_version"],
                            decision_policy_version=ev["decision_policy_version"],
                            config_hash_value=ev["config_hash"],
                            verdict=ev["verdict"],
                            details_original_sha256=bounded["original_sha256"],
                        ),
                        "evaluation_fingerprint_version": EVALUATION_FINGERPRINT_VERSION,
                    })

                pair_record = {
                    "experiment_code": EXPERIMENT_CODE,
                    "experiment_version": EXPERIMENT_VERSION,
                    "symbol": symbol,
                    "timeframe": frame.timeframe,
                    "provider": provider_name,
                    "snapshot_date": frame.snapshot_date,
                    "market_data_as_of": frame.market_data_as_of,
                    "frame_snapshot_version": frame.frame_snapshot_version,
                    "frame_hash": frame.frame_hash,
                    "frame_bar_count": frame.bar_count,
                    "frame_first_date": frame.first_date,
                    "frame_last_date": frame.last_date,
                    "frame_snapshot": frame.bars,
                    "pair_fingerprint": pair_fingerprint,
                    "pair_fingerprint_version": PAIR_FINGERPRINT_VERSION,
                }

                persisted = await persist_shadow_pair(
                    run_id=run_id, pair=pair_record, evaluations=evaluations
                )
            except ShadowSerializationError as exc:
                # Deterministic bounded rejection: the exception text carries
                # only a machine-readable reason code and a key/index path —
                # never the offending object's repr or a raw payload.
                logger.warning(
                    "Shadow details not JSON-safe for %s: %s", symbol, exc
                )
                rejected["details_not_json_safe"] += 1
                rejected_symbols.setdefault(
                    "details_not_json_safe", []
                ).append(symbol)
                continue
            except EvidenceTooLargeError:
                rejected["details_snapshot_too_large"] += 1
                rejected_symbols.setdefault(
                    "details_snapshot_too_large", []
                ).append(symbol)
                continue
            except ShadowIntegrityError as exc:
                logger.error("Shadow integrity error for %s: %s", symbol, exc)
                rejected["integrity_error"] += 1
                rejected_symbols.setdefault("integrity_error", []).append(symbol)
                continue
            except Exception as exc:
                logger.error("Shadow pair failed for %s: %s", symbol, exc)
                rejected["pair_error"] += 1
                rejected_symbols.setdefault("pair_error", []).append(symbol)
                continue

            counts["pair_count"] += 1
            if persisted["created_new_pair"]:
                counts["pairs_created"] += 1
            else:
                counts["pairs_deduplicated"] += 1

            cv = control_eval["verdict"]
            xv = candidate_eval["verdict"]
            counts[f"control_{cv.lower()}_count"] += 1
            counts[f"candidate_{xv.lower()}_count"] += 1
            if cv == xv:
                counts["agreement_count"] += 1
            else:
                counts["disagreement_count"] += 1
            categories[disagreement_category(cv, xv)] += 1

            pair_summaries.append({
                "symbol": symbol,
                "pair_id": persisted["pair_id"],
                "created_new_pair": persisted["created_new_pair"],
                "control_verdict": cv,
                "candidate_verdict": xv,
                "agreement": cv == xv,
                "disagreement_category": disagreement_category(cv, xv),
            })

        telemetry: Dict[str, Any] = {
            "experiment_code": EXPERIMENT_CODE,
            "experiment_version": EXPERIMENT_VERSION,
            "requested_count": len(normalized),
            # Canonical history-depth contract (derived, bounded scalars).
            "desired_history_bars": desired_bars,
            "requested_history_bars": requested_history_bars,
            "canonical_frame_cap": FRAME_HARD_CAP_BARS,
            "history_depth_capped": history_depth_capped,
            "fetched_count": counts["fetched_count"],
            "pair_count": counts["pair_count"],
            "pairs_created": counts["pairs_created"],
            "pairs_deduplicated": counts["pairs_deduplicated"],
            "agreement_count": counts["agreement_count"],
            "disagreement_count": counts["disagreement_count"],
            "control_enter_count": counts["control_enter_count"],
            "control_watch_count": counts["control_watch_count"],
            "control_avoid_count": counts["control_avoid_count"],
            "candidate_enter_count": counts["candidate_enter_count"],
            "candidate_watch_count": counts["candidate_watch_count"],
            "candidate_avoid_count": counts["candidate_avoid_count"],
            "verdict_categories": dict(sorted(categories.items())),
            "rejected_counts": dict(sorted(rejected.items())),
            # Symbol lists stay small by construction (hard 25-symbol cap).
            "rejected_symbols": {
                k: v[:MAX_SHADOW_SYMBOLS]
                for k, v in sorted(rejected_symbols.items())
            },
            "completion": completion_meta,
            "control_identity": {
                "strategy_code": control["strategy_code"],
                "strategy_version": control["strategy_version"],
                "decision_policy_version": control["decision_policy_version"],
                "config_hash": control["config_hash"],
            },
            "candidate_identity": {
                "strategy_code": candidate["strategy_code"],
                "strategy_version": candidate["strategy_version"],
                "decision_policy_version": candidate["decision_policy_version"],
                "config_hash": candidate["config_hash"],
            },
        }
        if counts["pair_count"] == 0:
            # All symbols honestly rejected -> still a COMPLETED run with an
            # explicit terminal reason (failure is reserved for operational
            # exceptions).
            telemetry["terminal_reason"] = "no_valid_pairs"

        await finalize_shadow_run(run_id, status="completed", telemetry=telemetry)

        return {
            "run_id": run_id,
            "status": "completed",
            "telemetry": telemetry,
            "pairs": pair_summaries,
        }

    except Exception as exc:
        logger.error("Shadow run %s failed: %s", run_id, exc)
        try:
            await finalize_shadow_run(
                run_id,
                status="failed",
                error_code="shadow_run_exception",
                error_message=str(exc),
            )
        except Exception as finalize_exc:
            logger.error(
                "Failed to finalize failed shadow run %s: %s", run_id, finalize_exc
            )
        return {
            "run_id": run_id,
            "status": "failed",
            "error_code": "shadow_run_exception",
        }
