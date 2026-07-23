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
from datetime import date, datetime, timedelta, timezone
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
    EVALUATION_FINGERPRINT_VERSION,
    FRAME_FETCH_MARGIN_BARS,
    FRAME_HARD_CAP_BARS,
    MAX_DETAILS_SNAPSHOT_BYTES,
    MAX_SHADOW_SYMBOLS,
    PAIR_FINGERPRINT_VERSION,
)
from app.workers.shadow.experiments import DEFAULT_EXPERIMENT, ShadowExperiment
from app.workers.shadow.frames_4h import (
    FOUR_HOUR_FETCH_CALENDAR_DAYS,
    FOUR_HOUR_FRAME_CONTRACT_VERSION,
    FourHourFrameRejection,
    build_four_hour_frame,
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
from app.workers.shadow.typed_values import ShadowPersistenceTypeError
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


async def _resolve_arm(
    pattern_code: str,
    arm_code: str,
    *,
    config_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve one arm's REAL strategy object + frozen config identity.

    Versions come from the registered strategy object (never inferred from
    names); config comes from the existing DB resolver (operator-modified
    pattern_configs respected) merged over the strategy's own defaults, then
    sanitized with the existing secret-removal policy and hashed
    deterministically. pattern_configs is never modified.

    `config_overrides` (Phase 9E3) is an experiment-declared IMMUTABLE
    evaluation override applied on an in-memory COPY of the resolved config
    only — the database configuration and the strategy defaults are never
    mutated. Overridden values are visible verbatim in the frozen config
    snapshot, enter the config hash (and therefore every fingerprint), and
    are echoed separately so an operator can always see that an override
    was in effect.
    """
    strategy = get_strategy(pattern_code)
    resolved = await resolve_pattern_config(pattern_code, strategy.default_config())
    applied_overrides: Dict[str, Any] = {}
    if config_overrides:
        resolved = dict(resolved)
        for key, value in config_overrides.items():
            applied_overrides[key] = value
            resolved[key] = value
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
        "config_overrides": applied_overrides,
    }


async def _acquire_four_hour(
    provider: Any,
    symbol: str,
    frame: CanonicalFrame,
    *,
    evaluation_time_utc: Optional[datetime],
    as_of_date: Optional[date],
) -> Dict[str, Any]:
    """Fetch and build the canonical completed 4H frame for one symbol.

    Never aborts the pair: every failure mode is a TYPED state
    (unsupported_provider / fetch_error / frame_rejected) recorded in the
    frozen metadata and telemetry — the candidate still evaluates the daily
    frame and its own trigger analysis reports the missing 4H data honestly.
    Returns {"status", "frame" (or None), "meta"}.
    """
    base_meta: Dict[str, Any] = {
        "contract_version": FOUR_HOUR_FRAME_CONTRACT_VERSION,
        "frame_hash": None,
    }
    if not bool(getattr(provider, "supports_intraday_history", False)):
        return {
            "status": "unsupported_provider",
            "frame": None,
            "meta": {**base_meta, "state": "unsupported_provider"},
        }

    # As-of alignment: the 4H window ends at the DAILY frame's pinned as-of
    # session (plus one calendar day of fetch margin; the frame builder cuts
    # any bar ending on a later exchange session).
    as_of_session = as_of_date or date.fromisoformat(frame.last_date)
    fetch_end = as_of_session + timedelta(days=1)
    fetch_start = as_of_session - timedelta(days=FOUR_HOUR_FETCH_CALENDAR_DAYS)
    try:
        payload = await provider.get_intraday_history(
            symbol,
            multiplier=4,
            timespan="hour",
            start=fetch_start,
            end=fetch_end,
        )
    except Exception as exc:
        logger.warning(
            "Shadow 4H fetch failed for %s: %s", symbol, type(exc).__name__
        )
        return {
            "status": "fetch_error",
            "frame": None,
            "meta": {
                **base_meta,
                "state": "fetch_error",
                "reason_code": f"provider_{type(exc).__name__}",
            },
        }

    daily_sessions = [date.fromisoformat(b["date"]) for b in frame.bars]
    try:
        frame_4h = build_four_hour_frame(
            symbol,
            payload,
            evaluation_time_utc=evaluation_time_utc,
            as_of_session_date=as_of_session,
            daily_session_dates=daily_sessions,
        )
    except FourHourFrameRejection as rejection:
        return {
            "status": "frame_rejected",
            "frame": None,
            "meta": {
                **base_meta,
                "state": "frame_rejected",
                "reason_code": rejection.reason_code,
            },
        }
    return {
        "status": "built",
        "frame": frame_4h,
        "meta": normalize_json_safe(frame_4h.metadata()),
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
    data_meta_extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate one arm on ITS OWN copy of the canonical frame.

    The returned verdict/score/reason/details are persisted verbatim — v2 is
    never normalized into v3 semantics, AVOID is never converted, missing
    scores stay None, and missing evidence.v1 (v2) stays absent.

    `data_meta_extras` carries an experiment's per-arm completion vocabulary
    (e.g. wyckoff_mtf.v2 reads explicit_completed/as_of_date); the default
    keys below are never removed or overridden by extras.
    """
    data_meta: Dict[str, Any] = dict(data_meta_extras or {})
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
    experiment: Optional[ShadowExperiment] = None,
    as_of_date: Optional[date] = None,
    telemetry_extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run one bounded shadow comparison over an explicit symbol list.

    One symbol failing never aborts the others; a handled run-level failure
    finalizes the run as 'failed'; a run whose every symbol was honestly
    rejected for data readiness still completes with an explicit terminal
    reason. Returns the bounded run summary.

    `experiment` selects the declared comparison protocol; omitted, the
    historical sma150 v2-vs-v3 experiment runs unchanged. Whatever the
    experiment, running it never enables a strategy, never creates a signal,
    watch, alert, notification or decision card, and never touches ranking.

    `as_of_date` (Phase 9E6) pins the evaluation to a historical session:
    daily bars after the date are excluded before the canonical frame is
    built, the 4H window is aligned to the same session, and (when now_utc
    is not supplied) the evaluation time resolves deterministically to
    midnight UTC of the following day. Only real historical bars are used —
    nothing is fabricated.

    `telemetry_extras` merges bounded JSON-safe operator metadata (e.g. the
    campaign block) into the finalized run telemetry.
    """
    experiment = experiment or DEFAULT_EXPERIMENT
    normalized = normalize_shadow_symbols(symbols)
    run_id = run_id or str(uuid.uuid4())
    provider_name = getattr(provider, "name", None) or "unknown"
    if as_of_date is not None and now_utc is None:
        # Midnight UTC of the next day is deterministically AFTER the NY
        # session close of as_of_date, so the as-of bar counts as completed.
        next_day = as_of_date + timedelta(days=1)
        now_utc = datetime(
            next_day.year, next_day.month, next_day.day, tzinfo=timezone.utc
        )

    def _category(control_verdict: str, candidate_verdict: str) -> str:
        return disagreement_category(
            control_verdict,
            candidate_verdict,
            control_label=experiment.control_category_label,
            candidate_label=experiment.candidate_category_label,
        )

    await create_shadow_run(
        run_id,
        provider=provider_name,
        requested_symbols=normalized,
        requested_limit=len(normalized),
        experiment_code=experiment.experiment_code,
        experiment_version=experiment.experiment_version,
    )

    counts = Counter()
    categories: Counter = Counter()
    trigger_states: Counter = Counter()
    rejected: Counter = Counter()
    rejected_symbols: Dict[str, List[str]] = {}
    completion_meta: Dict[str, Any] = {}
    pair_summaries: List[Dict[str, Any]] = []

    try:
        control = await _resolve_arm(
            experiment.control_pattern_code, experiment.control_arm_code
        )
        candidate = await _resolve_arm(
            experiment.candidate_pattern_code,
            experiment.candidate_arm_code,
            config_overrides=experiment.candidate_config_overrides,
        )

        # Shared canonical history depth, DERIVED once per run from both
        # resolved configs (SMA warm-up counted). The UNCAPPED desired value
        # is preserved: depth completeness is always judged against it, so a
        # filled 600-bar cap never masquerades as a complete 800-bar
        # lookback. Only the capped requested value is fetched/stored. The
        # fetch adds a small margin so excluding a partial latest bar can
        # never shrink an otherwise-full completed frame where the provider
        # has the data.
        desired_bars = desired_history_bars(
            control["config"], candidate["config"],
            control_fn=experiment.control_history_bars,
            candidate_fn=experiment.candidate_history_bars,
        )
        requested_history_bars = shared_required_history_bars(
            control["config"], candidate["config"],
            control_fn=experiment.control_history_bars,
            candidate_fn=experiment.candidate_history_bars,
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

            if as_of_date is not None:
                # Pin the evaluation to the as-of session: only REAL bars on
                # or before the date survive (ISO date strings compare
                # lexicographically; malformed rows stay for the canonical
                # frame builder to reject explicitly).
                as_of_iso = as_of_date.isoformat()
                payload = {
                    **(payload or {}),
                    "historical": [
                        bar for bar in (payload or {}).get("historical") or []
                        if not isinstance(bar, dict)
                        or "date" not in bar
                        or str(bar.get("date"))[:10] <= as_of_iso
                    ],
                }

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

            # Canonical completed 4H frame (Phase 9E3) — a typed per-symbol
            # state, never a pair abort: the candidate's own trigger analysis
            # reports missing 4H data honestly.
            four_hour: Optional[Dict[str, Any]] = None
            four_hour_identity: Optional[Dict[str, Any]] = None
            if experiment.requires_four_hour_frame:
                four_hour = await _acquire_four_hour(
                    provider, symbol, frame,
                    evaluation_time_utc=now_utc, as_of_date=as_of_date,
                )
                counts[f"four_hour_{four_hour['status']}"] += 1
                completion_meta[symbol]["four_hour"] = four_hour["meta"]
                four_hour_identity = {
                    "contract_version": four_hour["meta"].get("contract_version"),
                    "frame_hash": four_hour["meta"].get("frame_hash"),
                    "state": four_hour["meta"].get("state"),
                }

            try:
                control_eval = _evaluate_arm(
                    control, frame, run_id,
                    latest_bar_completed=True, now_utc=now_utc,
                    data_meta_extras=(
                        experiment.control_data_meta_extras(frame)
                        if experiment.control_data_meta_extras is not None
                        else None
                    ),
                )
                candidate_extras: Optional[Dict[str, Any]] = (
                    experiment.candidate_data_meta_extras(frame)
                    if experiment.candidate_data_meta_extras is not None
                    else None
                )
                if four_hour is not None and four_hour["frame"] is not None:
                    candidate_extras = {
                        **(candidate_extras or {}),
                        "df_4h": four_hour["frame"].dataframe(),
                    }
                candidate_eval = _evaluate_arm(
                    candidate, frame, run_id,
                    latest_bar_completed=True, now_utc=now_utc,
                    data_meta_extras=candidate_extras,
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
                    experiment_code=experiment.experiment_code,
                    experiment_version=experiment.experiment_version,
                    four_hour=four_hour_identity,
                )

                evaluations = []
                for ev in (control_eval, candidate_eval):
                    bounded = _bound_details(ev["details"])
                    snapshot = bounded["snapshot"]
                    if (
                        four_hour is not None
                        and ev["arm_code"] == experiment.candidate_arm_code
                    ):
                        # Runner-added namespaced metadata (same convention
                        # as _snapshot_meta): injected AFTER the original
                        # details hash, so the strategy's own output stays
                        # pure while the frozen row carries queryable 4H
                        # frame evidence.
                        snapshot = dict(snapshot)
                        snapshot["_four_hour_frame_meta"] = four_hour["meta"]
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
                        "details_snapshot": snapshot,
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
                    "experiment_code": experiment.experiment_code,
                    "experiment_version": experiment.experiment_version,
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
            except ShadowPersistenceTypeError as exc:
                # Deterministic application-level type failure at the typed
                # persistence boundary — never collapsed into pair_error.
                # Logs only the symbol, safe reason code and field name.
                logger.error(
                    "Shadow persistence type error for %s: %s (field %s)",
                    symbol, exc.reason_code, exc.field,
                )
                rejected["persistence_type_error"] += 1
                rejected_symbols.setdefault(
                    "persistence_type_error", []
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
            if experiment.requires_four_hour_frame:
                trigger_record = (
                    (candidate_eval["details"] or {}).get("four_hour_trigger")
                    or {}
                )
                trigger_states[
                    str(trigger_record.get("state") or "not_evaluated")
                ] += 1
                if trigger_record.get("trigger_price") is not None:
                    counts["candidate_real_trigger_price_count"] += 1
            if cv == xv:
                counts["agreement_count"] += 1
            else:
                counts["disagreement_count"] += 1
            categories[_category(cv, xv)] += 1

            pair_summaries.append({
                "symbol": symbol,
                "pair_id": persisted["pair_id"],
                "created_new_pair": persisted["created_new_pair"],
                "control_verdict": cv,
                "candidate_verdict": xv,
                "agreement": cv == xv,
                "disagreement_category": _category(cv, xv),
            })

        telemetry: Dict[str, Any] = {
            "experiment_code": experiment.experiment_code,
            "experiment_version": experiment.experiment_version,
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
        # Experiment-only immutable evaluation overrides are ALWAYS visible
        # in frozen run telemetry (empty overrides add nothing, keeping the
        # sma150 telemetry byte-identical).
        if control["config_overrides"]:
            telemetry["control_identity"]["config_overrides"] = (
                normalize_json_safe(control["config_overrides"])
            )
        if candidate["config_overrides"]:
            telemetry["candidate_identity"]["config_overrides"] = (
                normalize_json_safe(candidate["config_overrides"])
            )
        if as_of_date is not None:
            telemetry["as_of_date"] = as_of_date.isoformat()
        if experiment.requires_four_hour_frame:
            telemetry["four_hour_contract_version"] = (
                FOUR_HOUR_FRAME_CONTRACT_VERSION
            )
            telemetry["four_hour_frames_built"] = counts["four_hour_built"]
            telemetry["four_hour_unsupported_provider"] = counts[
                "four_hour_unsupported_provider"
            ]
            telemetry["four_hour_fetch_error"] = counts["four_hour_fetch_error"]
            telemetry["four_hour_frame_rejected"] = counts[
                "four_hour_frame_rejected"
            ]
            telemetry["candidate_trigger_states"] = dict(
                sorted(trigger_states.items())
            )
            telemetry["candidate_real_trigger_price_count"] = counts[
                "candidate_real_trigger_price_count"
            ]
        if telemetry_extras:
            telemetry.update(normalize_json_safe(dict(telemetry_extras)))
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
