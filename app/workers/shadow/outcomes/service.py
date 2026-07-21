"""Bounded admin-triggered outcome calculation for frozen shadow pairs.

One durable strategy_shadow_outcome_runs row per admin run; every handled run
finalizes as completed or failed (never left 'running'). One pair failing
never aborts the run. No scheduler entry exists; no resumability is claimed.

Provider policy (shadow_pair_outcomes.v1):
  * forward data MUST come from the frozen pair's provider — the configured
    provider client name must equal strategy_shadow_pairs.provider, otherwise
    the pair is deterministically rejected as provider_mismatch (providers
    are never mixed, the outcome is not calculated);
  * the provider must support bounded DATE-RANGE retrieval
    (get_daily_bars(symbol, from_date, to_date) over an actual range) so old
    pairs stay calculable after they fall outside any latest-N window;
    otherwise provider_range_unsupported;
  * the requested range is bounded: snapshot_date through
    min(today, snapshot_date + FORWARD_CALENDAR_CAP_DAYS);
  * fetched bars are written through to the local daily_bars cache, but
    correctness NEVER depends on local coverage being complete — the
    bounded provider response is authoritative for forward alignment (a
    local gap could silently shift the 1D bar).

Isolation: this module writes ONLY migration-011 tables (via the outcomes
persistence layer) and the daily_bars cache. It never touches signals,
signal_provenance, scan_run_signals, signal_outcomes or pattern_runs; it
never calls save_signal, never re-runs v2/v3 and never mutates B1 rows.
"""

import logging
import uuid as uuid_lib
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.workers import market_store
from app.workers.outcomes.calculator import HOLDING_WINDOWS
from app.workers.shadow.outcomes.calculator import (
    ShadowOutcomeRejection,
    build_forward_sequence,
    check_reference_revision,
    compute_benchmark_returns_for_pair,
    compute_outcome_values,
    resolve_reference_price,
    status_for_bar_count,
)
from app.workers.shadow.outcomes.constants import (
    BENCHMARK_SYMBOLS,
    CALCULATION_VERSION,
    DEFAULT_CALCULATION_LIMIT,
    FORWARD_CALENDAR_CAP_DAYS,
    FORWARD_FRAME_VERSION,
    MAX_CALCULATION_LIMIT,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
    REASON_PROVIDER_MISMATCH,
    REASON_PROVIDER_RANGE_UNSUPPORTED,
    REASON_REFERENCE_REVISION,
    REASON_SNAPSHOT_BAR_MISSING,
    REFERENCE_PRICE_ROLE,
    STATUS_ERROR,
)
from app.workers.shadow.outcomes.fingerprints import (
    compute_forward_bars_hash,
    compute_outcome_fingerprint,
)
from app.workers.shadow.outcomes.persistence import (
    create_outcome_run,
    finalize_outcome_run,
    select_pairs_for_outcomes,
    upsert_pair_outcome,
)
from app.workers.shadow.typed_values import ShadowPersistenceTypeError


logger = logging.getLogger(__name__)


class ShadowOutcomeRequestError(ValueError):
    """Invalid outcome-calculation request (unbounded / malformed selectors)."""


def normalize_outcome_request(
    *,
    pair_ids: Any = None,
    symbols: Any = None,
    run_id: Any = None,
    pending: Any = False,
    limit: Any = None,
    include_recalc: Any = False,
) -> Dict[str, Any]:
    """Validate and normalize the admin request. Deterministic rejections.

    Rules: at least one selector (pair_ids / symbols / run_id) or
    pending=true — no unbounded all-history mode; limit defaults to 50 with
    a hard cap of 200; pair IDs and symbols are normalized and deduplicated;
    malformed UUIDs reject. Filters AND-compose downstream.
    """
    norm_pair_ids: List[str] = []
    if pair_ids is not None:
        if not isinstance(pair_ids, list):
            raise ShadowOutcomeRequestError("pair_ids must be a list of UUIDs")
        seen: set = set()
        for pid in pair_ids:
            try:
                canonical = str(uuid_lib.UUID(str(pid)))
            except (ValueError, TypeError, AttributeError):
                raise ShadowOutcomeRequestError(
                    "pair_ids contains a malformed UUID"
                )
            if canonical not in seen:
                seen.add(canonical)
                norm_pair_ids.append(canonical)

    norm_symbols: List[str] = []
    if symbols is not None:
        if not isinstance(symbols, list):
            raise ShadowOutcomeRequestError("symbols must be a list of tickers")
        seen_s: set = set()
        for s in symbols:
            su = str(s or "").strip().upper()
            if su and su not in seen_s:
                seen_s.add(su)
                norm_symbols.append(su)

    norm_run_id: Optional[str] = None
    if run_id is not None:
        try:
            norm_run_id = str(uuid_lib.UUID(str(run_id)))
        except (ValueError, TypeError, AttributeError):
            raise ShadowOutcomeRequestError("run_id is a malformed UUID")

    pending = bool(pending)
    if not (norm_pair_ids or norm_symbols or norm_run_id or pending):
        raise ShadowOutcomeRequestError(
            "at least one selector (pair_ids, symbols, run_id) or "
            "pending=true is required — no unbounded all-history mode"
        )

    if limit is None:
        norm_limit = DEFAULT_CALCULATION_LIMIT
    else:
        try:
            norm_limit = int(limit)
        except (TypeError, ValueError):
            raise ShadowOutcomeRequestError("limit must be an integer")
        if norm_limit < 1:
            raise ShadowOutcomeRequestError("limit must be at least 1")
        if norm_limit > MAX_CALCULATION_LIMIT:
            raise ShadowOutcomeRequestError(
                f"limit must not exceed {MAX_CALCULATION_LIMIT}"
            )

    return {
        "pair_ids": norm_pair_ids,
        "symbols": norm_symbols,
        "run_id": norm_run_id,
        "pending": pending,
        "limit": norm_limit,
        "include_recalc": bool(include_recalc),
    }


def provider_supports_bounded_range(provider: Any) -> bool:
    """Whether the provider performs REAL bounded date-range retrieval.

    Declared by the provider class (supports_bounded_daily_range). A
    latest-N shim that merely filters a fixed recent window (FMP) does NOT
    qualify: an old pair outside that window would silently lose bars and
    corrupt forward alignment.
    """
    return bool(getattr(provider, "supports_bounded_daily_range", False))


def forward_range_for(
    snapshot_date: date,
    *,
    today: date,
    calendar_cap_days: int = FORWARD_CALENDAR_CAP_DAYS,
) -> Tuple[date, date]:
    """Bounded retrieval range: snapshot_date through
    min(today, snapshot_date + calendar_cap_days)."""
    return snapshot_date, min(today, snapshot_date + timedelta(days=calendar_cap_days))


async def _fetch_daily_range(
    provider: Any,
    symbol: str,
    from_date: date,
    to_date: date,
) -> List[Dict[str, Any]]:
    """Bounded provider date-range fetch, written through to daily_bars.

    Returns provider bars converted to the pure layer's shape
    ({"date", open, high, low, close, volume}). The cache write is
    best-effort: a cache failure never fails the outcome.
    """
    bars = await provider.get_daily_bars(symbol, str(from_date), str(to_date))
    try:
        if bars:
            await market_store.bulk_upsert_daily_bars(
                bars, source=getattr(provider, "name", "unknown")
            )
    except Exception as exc:  # cache only — never fatal
        logger.warning(
            "daily_bars cache write failed for %s: %s", symbol, type(exc).__name__
        )
    return [
        {
            "date": str(b["trading_date"]),
            "open": b["open"],
            "high": b["high"],
            "low": b["low"],
            "close": b["close"],
            "volume": b["volume"],
        }
        for b in (bars or [])
    ]


def _error_record(
    pair: Dict[str, Any],
    provider_name: Optional[str],
    error_code: str,
    error_message: Optional[str],
    *,
    reference_revision_detected: bool = False,
    revision_notes: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Deterministic error outcome record (horizons untouched by the merge).

    The merge layer guarantees an error record can never erase previously
    matured evidence: frozen horizons stay frozen and a partial/complete
    status is never regressed to error.
    """
    return {
        "pair_id": pair["pair_id"],
        "outcome_fingerprint": compute_outcome_fingerprint(
            pair_fingerprint=pair["pair_fingerprint"],
            pair_fingerprint_version=pair["pair_fingerprint_version"],
        ),
        "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
        "forward_frame_version": FORWARD_FRAME_VERSION,
        "reference_price": None,
        "reference_price_role": REFERENCE_PRICE_ROLE,
        "forward_provider": provider_name,
        "outcome_status": STATUS_ERROR,
        "error_code": error_code,
        "error_message": error_message,
        "available_forward_bars": 0,
        "reference_revision_detected": bool(reference_revision_detected),
        "revision_notes": list(revision_notes or []),
    }


async def run_shadow_outcome_calculation(
    provider: Any,
    *,
    pair_ids: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    run_id: Optional[str] = None,
    pending: bool = False,
    limit: int = DEFAULT_CALCULATION_LIMIT,
    include_recalc: bool = False,
    outcome_run_id: Optional[str] = None,
    now_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Run one bounded outcome calculation over selected frozen pairs.

    Selection reads only strategy_shadow_pairs and existing outcome rows.
    One pair failure never aborts the run; a handled run-level failure
    finalizes the run as 'failed'. Returns the bounded run summary.
    """
    outcome_run_id = outcome_run_id or str(uuid_lib.uuid4())
    provider_name = getattr(provider, "name", None) or "unknown"
    now = now_utc or datetime.now(timezone.utc)
    today = now.date()

    selector = {
        "pair_ids": pair_ids or [],
        "symbols": symbols or [],
        "run_id": run_id,
        "pending": bool(pending),
        "include_recalc": bool(include_recalc),
    }
    await create_outcome_run(
        outcome_run_id,
        provider=provider_name,
        requested_selector=selector,
        requested_limit=limit,
    )

    counts: Dict[str, int] = {
        "pairs_selected": 0,
        "calculated": 0,
        "pending_forward_bars": 0,
        "partial": 0,
        "complete": 0,
        "errors": 0,
        "provider_mismatch": 0,
        "provider_range_unsupported": 0,
        "reference_revisions": 0,
        "snapshot_bar_missing": 0,
    }
    pair_results: List[Dict[str, Any]] = []
    # Fetched sequences cached per (provider, symbol, bounded range) within
    # this run: one bounded fetch per symbol/range, SPY/QQQ at most once per
    # range, repeated symbols reuse the same frame. Provider is part of the
    # key so a cache entry can never cross providers.
    range_cache: Dict[
        Tuple[str, str, date, date], Optional[List[Dict[str, Any]]]
    ] = {}
    # Built benchmark sequences per (provider, benchmark, range).
    benchmark_seq_cache: Dict[
        Tuple[str, str, date, date], Optional[Dict[str, Any]]
    ] = {}

    async def _cached_daily_range(
        symbol: str, from_date: date, to_date: date
    ) -> List[Dict[str, Any]]:
        key = (provider_name, symbol, from_date, to_date)
        if key not in range_cache:
            range_cache[key] = await _fetch_daily_range(
                provider, symbol, from_date, to_date
            )
        return range_cache[key]

    async def _persist_error(
        pair: Dict[str, Any], code: str, message: Optional[str],
        *,
        reference_revision_detected: bool = False,
        revision_notes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        counts["errors"] += 1
        try:
            result = await upsert_pair_outcome(
                _error_record(
                    pair, provider_name, code, message,
                    reference_revision_detected=reference_revision_detected,
                    revision_notes=revision_notes,
                )
            )
            pair_results.append({
                "pair_id": pair["pair_id"],
                "symbol": pair["symbol"],
                "outcome_status": result["outcome_status"],
                "error_code": code,
            })
        except Exception:
            logger.error(
                "Also failed to persist error outcome for pair %s",
                pair["pair_id"],
            )
            pair_results.append({
                "pair_id": pair["pair_id"],
                "symbol": pair["symbol"],
                "outcome_status": "error",
                "error_code": code,
            })

    try:
        pairs = await select_pairs_for_outcomes(
            pair_ids=pair_ids,
            symbols=symbols,
            run_id=run_id,
            pending=pending,
            include_recalc=include_recalc,
            limit=limit,
        )
        counts["pairs_selected"] = len(pairs)

        for pair in pairs:
            try:
                pair_provider = pair.get("provider")
                # Provider continuity is REQUIRED in shadow_pair_outcomes.v1:
                # never mix providers, never silently substitute.
                if pair_provider != provider_name:
                    counts["provider_mismatch"] += 1
                    await _persist_error(
                        pair,
                        REASON_PROVIDER_MISMATCH,
                        f"pair provider '{pair_provider}' != configured "
                        f"provider '{provider_name}'",
                    )
                    continue
                if not provider_supports_bounded_range(provider):
                    counts["provider_range_unsupported"] += 1
                    await _persist_error(
                        pair,
                        REASON_PROVIDER_RANGE_UNSUPPORTED,
                        f"provider '{provider_name}' does not support "
                        "bounded date-range daily retrieval",
                    )
                    continue

                # Frozen reference (B1 frame only; never provider data).
                try:
                    reference_price = resolve_reference_price(
                        frame_last_bar=pair.get("frame_last_bar"),
                        frame_bar_count=pair.get("frame_bar_count"),
                        snapshot_date=pair.get("snapshot_date"),
                        frame_last_date=pair.get("frame_last_date"),
                    )
                except ShadowOutcomeRejection as rejection:
                    await _persist_error(
                        pair, rejection.reason_code, rejection.detail
                    )
                    continue

                snapshot_date = pair["snapshot_date"]
                if isinstance(snapshot_date, str):
                    snapshot_date = date.fromisoformat(snapshot_date)
                from_date, to_date = forward_range_for(snapshot_date, today=today)

                try:
                    historical = await _cached_daily_range(
                        pair["symbol"], from_date, to_date
                    )
                except Exception as exc:
                    await _persist_error(
                        pair, "forward_fetch_error", type(exc).__name__
                    )
                    continue

                try:
                    sequence = build_forward_sequence(
                        historical, snapshot_date, now_utc=now
                    )
                except ShadowOutcomeRejection as rejection:
                    await _persist_error(
                        pair, rejection.reason_code, rejection.detail
                    )
                    continue

                forward_bars = sequence["forward_bars"]

                # Reference continuity gate. Forward returns are only
                # trustworthy when the provider's snapshot-date bar exists
                # AND its close matches the frozen reference within
                # tolerance. A missing bar (trimmed / suspended / delisted
                # history) or a diverging close (split / provider revision)
                # means the fetched forward price scale CANNOT be proven
                # compatible with the frozen reference — no new horizon may
                # be calculated from it. Previously frozen horizons stay
                # frozen (the merge layer never lets an error record erase
                # matured evidence); a split must never surface as a
                # legitimate +/-50% return.
                if sequence["snapshot_bar"] is None:
                    counts["snapshot_bar_missing"] += 1
                    await _persist_error(
                        pair,
                        REASON_SNAPSHOT_BAR_MISSING,
                        "bounded range has no bar on snapshot_date; "
                        "reference continuity unconfirmed",
                    )
                    continue

                revision_detected, revision_note = check_reference_revision(
                    reference_price,
                    sequence["snapshot_bar"],
                    provider=provider_name,
                )
                if revision_detected:
                    counts["reference_revisions"] += 1
                    await _persist_error(
                        pair,
                        REASON_REFERENCE_REVISION,
                        "snapshot-date close diverged from frozen reference; "
                        "forward price scale incompatible",
                        reference_revision_detected=True,
                        revision_notes=(
                            [{**revision_note, "detected_at": now.isoformat()}]
                            if revision_note else []
                        ),
                    )
                    continue

                # Benchmarks: same provider, same bounded range, completed-bar
                # policy identical. A benchmark failure NEVER fails the pair.
                benchmark_sequences: Dict[str, Optional[Dict[str, Any]]] = {}
                for bench in BENCHMARK_SYMBOLS:
                    cache_key = (provider_name, bench, from_date, to_date)
                    if cache_key not in benchmark_seq_cache:
                        try:
                            bench_bars = await _cached_daily_range(
                                bench, from_date, to_date
                            )
                            benchmark_seq_cache[cache_key] = (
                                build_forward_sequence(
                                    bench_bars, snapshot_date, now_utc=now
                                )
                            )
                        except Exception as exc:
                            logger.warning(
                                "Benchmark %s fetch failed: %s",
                                bench, type(exc).__name__,
                            )
                            benchmark_seq_cache[cache_key] = None
                    benchmark_sequences[bench] = benchmark_seq_cache[cache_key]

                values = compute_outcome_values(reference_price, forward_bars)
                benchmark_returns = compute_benchmark_returns_for_pair(
                    benchmark_sequences
                )

                record: Dict[str, Any] = {
                    "pair_id": pair["pair_id"],
                    "outcome_fingerprint": compute_outcome_fingerprint(
                        pair_fingerprint=pair["pair_fingerprint"],
                        pair_fingerprint_version=pair["pair_fingerprint_version"],
                    ),
                    "outcome_fingerprint_version": OUTCOME_FINGERPRINT_VERSION,
                    "calculation_version": CALCULATION_VERSION,
                    "outcome_coverage_version": OUTCOME_COVERAGE_VERSION,
                    "forward_frame_version": FORWARD_FRAME_VERSION,
                    "reference_price": reference_price,
                    "reference_price_role": REFERENCE_PRICE_ROLE,
                    "forward_provider": provider_name,
                    "forward_data_as_of": (
                        values["last_forward_date"] or None
                    ),
                    "available_forward_bars": values["available_forward_bars"],
                    "first_forward_date": values["first_forward_date"],
                    "last_forward_date": values["last_forward_date"],
                    "forward_bars_hash": compute_forward_bars_hash(
                        symbol=pair["symbol"],
                        provider=provider_name,
                        snapshot_date=snapshot_date,
                        forward_bars=forward_bars,
                    ),
                    "max_favorable_excursion": values["max_favorable_excursion"],
                    "max_adverse_excursion": values["max_adverse_excursion"],
                    "mfe_mae_bar_count": values["mfe_mae_bar_count"],
                    "benchmark_returns": benchmark_returns,
                    # Continuity was proven above (snapshot bar present and
                    # within tolerance) — a revision would have rejected the
                    # pair before this point.
                    "reference_revision_detected": False,
                    "revision_notes": [],
                    "outcome_status": status_for_bar_count(
                        values["available_forward_bars"]
                    ),
                    "error_code": None,
                    "error_message": None,
                }
                for w in HOLDING_WINDOWS:
                    record[f"ret_{w}d"] = values["ret_by_window"][w]

                result = await upsert_pair_outcome(record)
                counts["calculated"] += 1
                counts[result["outcome_status"]] = (
                    counts.get(result["outcome_status"], 0) + 1
                )
                pair_results.append({
                    "pair_id": pair["pair_id"],
                    "symbol": pair["symbol"],
                    "outcome_status": result["outcome_status"],
                    "available_forward_bars": values["available_forward_bars"],
                    "created_new": result["created_new"],
                })
            except ShadowPersistenceTypeError as exc:
                logger.error(
                    "Outcome persistence type error for pair %s: %s (field %s)",
                    pair["pair_id"], exc.reason_code, exc.field,
                )
                await _persist_error(
                    pair, "persistence_type_error", exc.reason_code
                )
            except Exception as exc:
                logger.error(
                    "Outcome calculation failed for pair %s: %s",
                    pair["pair_id"], exc,
                )
                await _persist_error(pair, "outcome_error", type(exc).__name__)

        telemetry: Dict[str, Any] = {
            "provider": provider_name,
            "requested_limit": limit,
            **counts,
        }
        await finalize_outcome_run(
            outcome_run_id, status="completed", telemetry=telemetry
        )
        return {
            "outcome_run_id": outcome_run_id,
            "status": "completed",
            "telemetry": telemetry,
            "pairs": pair_results,
        }

    except Exception as exc:
        logger.error("Shadow outcome run %s failed: %s", outcome_run_id, exc)
        try:
            await finalize_outcome_run(
                outcome_run_id,
                status="failed",
                error_code="shadow_outcome_run_exception",
                error_message=str(exc),
            )
        except Exception as finalize_exc:
            logger.error(
                "Failed to finalize failed outcome run %s: %s",
                outcome_run_id, finalize_exc,
            )
        return {
            "outcome_run_id": outcome_run_id,
            "status": "failed",
            "error_code": "shadow_outcome_run_exception",
        }
