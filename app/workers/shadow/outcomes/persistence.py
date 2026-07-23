"""DB persistence for shadow pair outcomes (Phase 8.1B2, migration 011).

Writes ONLY to strategy_shadow_pair_outcomes and strategy_shadow_outcome_runs.
This module must never touch signals, signal_provenance, scan_run_signals,
signal_outcomes, pattern_runs or any normal scan table, and it never UPDATEs
a B1 pair or evaluation row.

Write-once maturation contract:
  * one canonical outcome row per pair (pair_id UNIQUE);
  * a NULL horizon may become calculated exactly once; a calculated horizon
    is FROZEN — recalculation never overwrites, never resets to NULL;
  * benchmark horizons freeze per benchmark per window the same way;
  * MFE/MAE update only when the completed forward bar count INCREASES,
    and the bar count is always stored next to them;
  * divergent recalculations are recorded in bounded revision_notes — one
    value is never automatically declared correct;
  * a 'complete' row never regresses (not to partial and not to error).

All merge logic is a PURE function (merge_outcome_for_persistence) so the
freeze semantics are unit-testable without a database. The DB write itself
happens in one transaction with the merged, typed values.
"""

import json
import logging
import uuid as uuid_lib
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label
from app.workers.persistence import get_db_connection, release_db_connection
from app.workers.scan_runs import sanitize_scan_error
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
)
from app.workers.shadow.fingerprints import (
    category_label_for_arm,
    disagreement_category,
)
from app.workers.shadow.outcomes.constants import (
    BENCHMARK_SYMBOLS,
    MAX_ERROR_TEXT_LEN,
    MAX_OUTCOME_TELEMETRY_BYTES,
    MAX_REVISION_NOTES,
    MAX_REVISION_NOTES_BYTES,
    MAX_SELECTOR_BYTES,
    REFERENCE_ABS_TOL,
    REFERENCE_REL_TOL,
    STATUS_COMPLETE,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_PENDING,
)
from app.workers.shadow.serialization import normalize_json_safe, strict_json
from app.workers.shadow.typed_values import (
    as_bool_param,
    as_date_param,
    as_int_param,
    as_score_param,
    as_utc_datetime_param,
    as_uuid_param,
)


logger = logging.getLogger(__name__)

_RET_FIELDS = {w: f"ret_{w}d" for w in HOLDING_WINDOWS}


def _is_close(a: float, b: float) -> bool:
    import math

    return math.isclose(a, b, rel_tol=REFERENCE_REL_TOL, abs_tol=REFERENCE_ABS_TOL)


def _maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


# --------------------------------------------------------------------------- #
# PURE write-once merge (no I/O)
# --------------------------------------------------------------------------- #

def _note_identity(note: Dict[str, Any]) -> Tuple:
    """Deterministic identity of a note EXCLUDING its detection timestamp,
    so a repeated identical recalculation never appends a duplicate."""
    return tuple(sorted(
        (k, strict_json(normalize_json_safe(v)))
        for k, v in note.items()
        if k != "detected_at"
    ))


def _dedupe_notes(
    existing: List[Dict[str, Any]], new: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    seen = {_note_identity(n) for n in existing}
    out = list(existing)
    for note in new:
        key = _note_identity(note)
        if key not in seen:
            seen.add(key)
            out.append(note)
    return out


def _bounded_notes(notes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Cap revision notes by count, then by serialized size (oldest kept —
    the earliest divergences are the most forensically valuable)."""
    notes = notes[:MAX_REVISION_NOTES]
    while notes and len(
        strict_json(normalize_json_safe(notes)).encode("utf-8")
    ) > MAX_REVISION_NOTES_BYTES:
        notes = notes[:-1]
    return notes


def merge_outcome_for_persistence(
    existing: Optional[Dict[str, Any]],
    calculated: Dict[str, Any],
    *,
    detected_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Merge one freshly calculated outcome into the existing frozen row.

    `existing` is None for the first insert, otherwise a plain dict of the
    stored row (ret_1d.., benchmark_returns parsed, mfe/mae, bar counts,
    statuses, revision_notes parsed). `calculated` is the fresh calculation
    record. Returns the full merged record plus the combined bounded
    revision_notes. PURE: no I/O, deterministic for identical inputs.
    """
    if existing is None:
        merged = dict(calculated)
        merged["revision_notes"] = _bounded_notes(
            list(calculated.get("revision_notes") or [])
        )
        merged["reference_revision_detected"] = bool(
            calculated.get("reference_revision_detected")
        )
        return merged

    notes: List[Dict[str, Any]] = list(existing.get("revision_notes") or [])
    new_notes: List[Dict[str, Any]] = list(calculated.get("revision_notes") or [])
    merged: Dict[str, Any] = {}

    def _note(payload: Dict[str, Any]) -> None:
        if detected_at is not None:
            payload = {**payload, "detected_at": detected_at}
        new_notes.append(payload)

    # ---- frozen reference ------------------------------------------------ #
    existing_ref = existing.get("reference_price")
    calc_ref = calculated.get("reference_price")
    if existing_ref is not None:
        merged["reference_price"] = existing_ref
        if calc_ref is not None and not _is_close(existing_ref, calc_ref):
            _note({
                "reason_code": "reference_price_divergence",
                "existing_value": existing_ref,
                "observed_value": calc_ref,
            })
            merged["reference_revision_detected_extra"] = True
    else:
        merged["reference_price"] = calc_ref

    # ---- write-once horizons --------------------------------------------- #
    for w in HOLDING_WINDOWS:
        field = _RET_FIELDS[w]
        old = existing.get(field)
        new = calculated.get(field)
        if old is not None:
            merged[field] = old
            if new is not None and not _is_close(old, new):
                _note({
                    "reason_code": "horizon_value_divergence",
                    "horizon": window_label(w),
                    "existing_value": old,
                    "observed_value": new,
                })
        else:
            merged[field] = new

    # ---- benchmark freeze per benchmark per window ------------------------ #
    old_bench = existing.get("benchmark_returns") or {}
    new_bench = calculated.get("benchmark_returns") or {}
    merged_bench: Dict[str, Dict[str, Optional[float]]] = {}
    for bench in sorted(set(old_bench) | set(new_bench)):
        old_map = old_bench.get(bench) or {}
        new_map = new_bench.get(bench) or {}
        bench_out: Dict[str, Optional[float]] = {}
        for w in HOLDING_WINDOWS:
            label = window_label(w)
            old_v = old_map.get(label)
            new_v = new_map.get(label)
            if old_v is not None:
                bench_out[label] = old_v
                if new_v is not None and not _is_close(old_v, new_v):
                    _note({
                        "reason_code": "benchmark_value_divergence",
                        "benchmark": bench,
                        "horizon": label,
                        "existing_value": old_v,
                        "observed_value": new_v,
                    })
            else:
                bench_out[label] = new_v
        merged_bench[bench] = bench_out
    merged["benchmark_returns"] = merged_bench or None

    # ---- MFE/MAE: only with MORE completed bars, and MONOTONIC ------------ #
    # Excursions over an EXPANDING prefix of forward bars can only widen:
    # MFE never decreases, MAE never increases (LONG raw market path). A
    # larger bar count whose revised earlier highs/lows would violate
    # monotonicity keeps the trustworthy stored value and records a note.
    old_count = existing.get("mfe_mae_bar_count")
    new_count = calculated.get("mfe_mae_bar_count")
    old_mfe = existing.get("max_favorable_excursion")
    old_mae = existing.get("max_adverse_excursion")
    if new_count is not None and (old_count is None or new_count > old_count):
        new_mfe = calculated.get("max_favorable_excursion")
        new_mae = calculated.get("max_adverse_excursion")
        if old_mfe is not None and new_mfe is not None and new_mfe < old_mfe:
            _note({
                "reason_code": "mfe_monotonicity_violation",
                "existing_value": old_mfe,
                "observed_value": new_mfe,
                "bar_count": new_count,
            })
            merged["max_favorable_excursion"] = old_mfe
        else:
            merged["max_favorable_excursion"] = (
                new_mfe if new_mfe is not None else old_mfe
            )
        if old_mae is not None and new_mae is not None and new_mae > old_mae:
            _note({
                "reason_code": "mae_monotonicity_violation",
                "existing_value": old_mae,
                "observed_value": new_mae,
                "bar_count": new_count,
            })
            merged["max_adverse_excursion"] = old_mae
        else:
            merged["max_adverse_excursion"] = (
                new_mae if new_mae is not None else old_mae
            )
        merged["mfe_mae_bar_count"] = new_count
    else:
        # Same-or-smaller incoming bar count can never rewrite excursions,
        # and mfe_mae_bar_count never decreases.
        merged["max_favorable_excursion"] = old_mfe
        merged["max_adverse_excursion"] = old_mae
        merged["mfe_mae_bar_count"] = old_count

    # ---- forward-frame metadata ------------------------------------------- #
    old_avail = int(existing.get("available_forward_bars") or 0)
    new_avail = int(calculated.get("available_forward_bars") or 0)
    if new_avail > old_avail:
        merged["available_forward_bars"] = new_avail
        merged["first_forward_date"] = calculated.get("first_forward_date")
        merged["last_forward_date"] = calculated.get("last_forward_date")
        merged["forward_data_as_of"] = calculated.get("forward_data_as_of")
        merged["forward_bars_hash"] = calculated.get("forward_bars_hash")
        if (
            existing.get("forward_bars_hash") is not None
            and calculated.get("forward_bars_hash") is not None
            and existing["forward_bars_hash"] != calculated["forward_bars_hash"]
        ):
            # The superseded hash is preserved in the bounded history BEFORE
            # replacement (normal maturation replaces it as bars grow).
            _note({
                "reason_code": "forward_bars_hash_superseded",
                "existing_hash": existing["forward_bars_hash"],
                "observed_hash": calculated["forward_bars_hash"],
                "bar_count": new_avail,
            })
    else:
        merged["available_forward_bars"] = old_avail
        merged["first_forward_date"] = existing.get("first_forward_date")
        merged["last_forward_date"] = existing.get("last_forward_date")
        merged["forward_data_as_of"] = existing.get("forward_data_as_of")
        merged["forward_bars_hash"] = existing.get("forward_bars_hash")
        if new_avail < old_avail:
            _note({
                "reason_code": "forward_coverage_regression",
                "existing_value": old_avail,
                "observed_value": new_avail,
            })
        elif (
            calculated.get("forward_bars_hash") is not None
            and existing.get("forward_bars_hash") is not None
            and calculated["forward_bars_hash"] != existing["forward_bars_hash"]
        ):
            # Same bar count, different data: a provider revision. The
            # original hash stays frozen with the frozen horizons.
            _note({
                "reason_code": "forward_bars_revision",
                "existing_hash": existing["forward_bars_hash"],
                "observed_hash": calculated["forward_bars_hash"],
                "bar_count": new_avail,
            })

    # ---- monotonic status -------------------------------------------------- #
    order = {STATUS_PENDING: 0, STATUS_PARTIAL: 1, STATUS_COMPLETE: 2}
    old_status = existing.get("outcome_status")
    new_status = calculated.get("outcome_status")
    if old_status == STATUS_COMPLETE:
        # Complete never regresses — not to partial and not to error, even
        # if the provider temporarily returns fewer bars or a recheck fails.
        merged["outcome_status"] = STATUS_COMPLETE
    elif old_status == STATUS_ERROR:
        # A successful recalculation REPAIRS an error row (to pending,
        # partial or complete); a repeated error stays error.
        merged["outcome_status"] = new_status
    elif new_status == STATUS_ERROR:
        if old_status == STATUS_PARTIAL:
            # An operational/deterministic failure never erases matured
            # evidence: the partial state is retained and the failure is
            # recorded as a bounded note instead.
            merged["outcome_status"] = STATUS_PARTIAL
            _note({
                "reason_code": "recalculation_error",
                "error_code": calculated.get("error_code"),
            })
        else:
            merged["outcome_status"] = STATUS_ERROR
    else:
        merged["outcome_status"] = (
            new_status
            if order.get(new_status, 0) >= order.get(old_status, 0)
            else old_status
        )

    if merged["outcome_status"] == STATUS_ERROR:
        merged["error_code"] = calculated.get("error_code") or existing.get("error_code")
        merged["error_message"] = (
            calculated.get("error_message") or existing.get("error_message")
        )
    else:
        merged["error_code"] = None
        merged["error_message"] = None

    merged["reference_revision_detected"] = bool(
        existing.get("reference_revision_detected")
        or calculated.get("reference_revision_detected")
        or merged.pop("reference_revision_detected_extra", False)
    )
    # Identical repeated divergences never append duplicate notes; the
    # combined history stays bounded by count and byte size.
    merged["revision_notes"] = _bounded_notes(_dedupe_notes(notes, new_notes))

    # Contract identity never changes across maturation.
    for key in (
        "outcome_fingerprint",
        "outcome_fingerprint_version",
        "calculation_version",
        "outcome_coverage_version",
        "forward_frame_version",
        "reference_price_role",
        "forward_provider",
    ):
        merged[key] = existing.get(key) or calculated.get(key)

    return merged


# --------------------------------------------------------------------------- #
# Outcome runs (operational audit; completed/failed, never left running)
# --------------------------------------------------------------------------- #

def _bounded_json(value: Optional[Dict[str, Any]], max_bytes: int) -> Optional[str]:
    if value is None:
        return None
    normalized = normalize_json_safe(value)
    text = strict_json(normalized)
    if len(text.encode("utf-8")) <= max_bytes:
        return text
    scalars = {
        k: v for k, v in normalized.items()
        if isinstance(v, (int, float, str, bool)) or v is None
    }
    scalars["payload_truncated"] = True
    return strict_json(scalars)


async def create_outcome_run(
    run_id: str,
    *,
    provider: Optional[str],
    requested_selector: Optional[Dict[str, Any]],
    requested_limit: Optional[int],
) -> str:
    """Create the durable outcome-run row at start (status='running')."""
    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            INSERT INTO strategy_shadow_outcome_runs (
                id, status, requested_selector, requested_limit, provider,
                started_at, created_at, updated_at
            )
            VALUES ($1, 'running', $2, $3, $4, NOW(), NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            as_uuid_param(run_id, "run_id"),
            _bounded_json(requested_selector, MAX_SELECTOR_BYTES),
            None if requested_limit is None
            else as_int_param(requested_limit, "requested_limit"),
            provider,
        )
        return str(run_id)
    finally:
        await release_db_connection(conn)


async def finalize_outcome_run(
    run_id: str,
    *,
    status: str,
    telemetry: Optional[Dict[str, Any]] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Finalize the run row (completed OR failed) — never leaves 'running'
    after a handled exception. Error text is sanitized and bounded."""
    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            UPDATE strategy_shadow_outcome_runs
            SET status = $2,
                telemetry = $3,
                error_code = $4,
                error_message = $5,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            as_uuid_param(run_id, "run_id"),
            status,
            _bounded_json(telemetry, MAX_OUTCOME_TELEMETRY_BYTES),
            error_code,
            sanitize_scan_error(error_message, max_len=MAX_ERROR_TEXT_LEN),
        )
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Selection (reads ONLY strategy_shadow_pairs + existing outcome rows)
# --------------------------------------------------------------------------- #

async def select_pairs_for_outcomes(
    *,
    pair_ids: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    run_id: Optional[str] = None,
    pending: bool = False,
    include_recalc: bool = False,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Bounded AND-composed selection of frozen pairs to (re)calculate.

    Only the LAST frozen frame bar is read from the JSONB snapshot (the
    reference contract needs exactly that bar); the full frame snapshot is
    never pulled into memory. Failed B1 runs with no pairs are structurally
    excluded (selection starts from strategy_shadow_pairs).

    Status predicate:
      * include_recalc=False: only pairs without an outcome row or with a
        pending_forward_bars/partial row (frozen complete/error rows are
        never silently reprocessed);
      * include_recalc=True + pending=True: adds error rows (repair);
      * include_recalc=True + explicit selectors: also re-checks complete
        rows for revision diagnostics (frozen values stay frozen).
    """
    where: List[str] = []
    params: List[Any] = []

    def _add(clause: str, value: Any) -> None:
        params.append(value)
        where.append(clause.format(n=len(params)))

    if pair_ids:
        _add(
            "p.id = ANY(${n}::uuid[])",
            [as_uuid_param(pid, "pair_ids") for pid in pair_ids],
        )
    if symbols:
        _add("p.symbol = ANY(${n})", [s.upper() for s in symbols])
    if run_id is not None:
        _add(
            "p.id IN (SELECT pair_id FROM strategy_shadow_run_pairs "
            "WHERE run_id = ${n})",
            as_uuid_param(run_id, "run_id"),
        )

    if include_recalc:
        if pending:
            where.append(
                "(o.id IS NULL OR o.outcome_status <> 'complete')"
            )
    else:
        where.append(
            "(o.id IS NULL OR o.outcome_status IN "
            "('pending_forward_bars', 'partial'))"
        )

    params.append(int(limit))
    query = f"""
        SELECT p.id AS pair_id, p.symbol, p.provider, p.snapshot_date,
               p.frame_bar_count, p.frame_last_date,
               p.frame_snapshot->-1 AS frame_last_bar,
               p.pair_fingerprint, p.pair_fingerprint_version,
               o.id AS outcome_id, o.outcome_status AS existing_status
        FROM strategy_shadow_pairs p
        LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        ORDER BY p.snapshot_date ASC, p.created_at ASC
        LIMIT ${len(params)}
    """
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [
            {
                "pair_id": str(r["pair_id"]),
                "symbol": r["symbol"],
                "provider": r["provider"],
                "snapshot_date": r["snapshot_date"],
                "frame_bar_count": r["frame_bar_count"],
                "frame_last_date": r["frame_last_date"],
                "frame_last_bar": _maybe_json(r["frame_last_bar"]),
                "pair_fingerprint": r["pair_fingerprint"],
                "pair_fingerprint_version": r["pair_fingerprint_version"],
                "outcome_id": str(r["outcome_id"]) if r["outcome_id"] else None,
                "existing_status": r["existing_status"],
            }
            for r in rows
        ]
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Write-once upsert (one transaction; merge computed against the locked row)
# --------------------------------------------------------------------------- #

_OUTCOME_COLUMNS = """
    id, pair_id, outcome_fingerprint, outcome_fingerprint_version,
    calculation_version, outcome_coverage_version, forward_frame_version,
    reference_price, reference_price_role, forward_provider,
    forward_data_as_of, available_forward_bars, first_forward_date,
    last_forward_date, forward_bars_hash,
    ret_1d, ret_3d, ret_5d, ret_10d, ret_20d,
    max_favorable_excursion, max_adverse_excursion, mfe_mae_bar_count,
    benchmark_returns, revision_notes, reference_revision_detected,
    outcome_status, error_code, error_message,
    first_calculated_at, calculated_at, created_at, updated_at
"""


def _existing_row_to_dict(row: Any) -> Dict[str, Any]:
    return {
        "outcome_id": str(row["id"]),
        "outcome_fingerprint": row["outcome_fingerprint"],
        "outcome_fingerprint_version": row["outcome_fingerprint_version"],
        "calculation_version": row["calculation_version"],
        "outcome_coverage_version": row["outcome_coverage_version"],
        "forward_frame_version": row["forward_frame_version"],
        "reference_price": row["reference_price"],
        "reference_price_role": row["reference_price_role"],
        "forward_provider": row["forward_provider"],
        "forward_data_as_of": row["forward_data_as_of"],
        "available_forward_bars": row["available_forward_bars"],
        "first_forward_date": row["first_forward_date"],
        "last_forward_date": row["last_forward_date"],
        "forward_bars_hash": row["forward_bars_hash"],
        "ret_1d": row["ret_1d"],
        "ret_3d": row["ret_3d"],
        "ret_5d": row["ret_5d"],
        "ret_10d": row["ret_10d"],
        "ret_20d": row["ret_20d"],
        "max_favorable_excursion": row["max_favorable_excursion"],
        "max_adverse_excursion": row["max_adverse_excursion"],
        "mfe_mae_bar_count": row["mfe_mae_bar_count"],
        "benchmark_returns": _maybe_json(row["benchmark_returns"]),
        "revision_notes": _maybe_json(row["revision_notes"]),
        "reference_revision_detected": row["reference_revision_detected"],
        "outcome_status": row["outcome_status"],
        "error_code": row["error_code"],
        "error_message": row["error_message"],
        "first_calculated_at": row["first_calculated_at"],
    }


def _date_or_none(value: Any, field: str) -> Optional[date]:
    return None if value is None else as_date_param(value, field)


async def upsert_pair_outcome(record: Dict[str, Any]) -> Dict[str, Any]:
    """Insert or write-once-merge the canonical outcome row for one pair.

    In ONE transaction: the existing row is locked (FOR UPDATE), the pure
    merge is applied, and the merged record is written back. The merge
    preserves every previously calculated horizon, benchmark value, MFE/MAE
    (unless more bars) and the frozen reference. Returns
    {"outcome_id", "created_new", "outcome_status"}.
    """
    now = datetime.now(timezone.utc)
    conn = await get_db_connection()
    try:
        async with conn.transaction():
            existing_row = await conn.fetchrow(
                f"SELECT {_OUTCOME_COLUMNS} FROM strategy_shadow_pair_outcomes "
                "WHERE pair_id = $1 FOR UPDATE",
                as_uuid_param(record["pair_id"], "pair_id"),
            )
            existing = (
                _existing_row_to_dict(existing_row)
                if existing_row is not None
                else None
            )
            merged = merge_outcome_for_persistence(
                existing, record, detected_at=now.isoformat()
            )

            benchmark_json = (
                strict_json(normalize_json_safe(merged["benchmark_returns"]))
                if merged.get("benchmark_returns") is not None
                else None
            )
            notes_json = (
                strict_json(normalize_json_safe(merged["revision_notes"]))
                if merged.get("revision_notes")
                else None
            )
            error_message = sanitize_scan_error(
                merged.get("error_message"), max_len=MAX_ERROR_TEXT_LEN
            )

            if existing is None:
                outcome_id = uuid_lib.uuid4()
                # ON CONFLICT DO NOTHING closes the first-insert race: if a
                # concurrent transaction inserted this pair's row between
                # our FOR UPDATE probe and this INSERT, the speculative
                # insert waits for it, inserts nothing, and we fall through
                # to the locked merge-update path instead of raising.
                inserted = await conn.fetchval(
                    """
                    INSERT INTO strategy_shadow_pair_outcomes (
                        id, pair_id, outcome_fingerprint,
                        outcome_fingerprint_version, calculation_version,
                        outcome_coverage_version, forward_frame_version,
                        reference_price, reference_price_role,
                        forward_provider, forward_data_as_of,
                        available_forward_bars, first_forward_date,
                        last_forward_date, forward_bars_hash,
                        ret_1d, ret_3d, ret_5d, ret_10d, ret_20d,
                        max_favorable_excursion, max_adverse_excursion,
                        mfe_mae_bar_count, benchmark_returns, revision_notes,
                        reference_revision_detected, outcome_status,
                        error_code, error_message,
                        first_calculated_at, calculated_at,
                        created_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25, $26, $27, $28, $29,
                            $30, $31, NOW(), NOW())
                    ON CONFLICT (pair_id) DO NOTHING
                    RETURNING id
                    """,
                    outcome_id,
                    as_uuid_param(record["pair_id"], "pair_id"),
                    merged["outcome_fingerprint"],
                    merged["outcome_fingerprint_version"],
                    merged["calculation_version"],
                    merged["outcome_coverage_version"],
                    merged["forward_frame_version"],
                    as_score_param(merged.get("reference_price"), "reference_price"),
                    merged["reference_price_role"],
                    merged.get("forward_provider"),
                    _date_or_none(merged.get("forward_data_as_of"), "forward_data_as_of"),
                    as_int_param(merged.get("available_forward_bars") or 0,
                                 "available_forward_bars"),
                    _date_or_none(merged.get("first_forward_date"), "first_forward_date"),
                    _date_or_none(merged.get("last_forward_date"), "last_forward_date"),
                    merged.get("forward_bars_hash"),
                    as_score_param(merged.get("ret_1d"), "ret_1d"),
                    as_score_param(merged.get("ret_3d"), "ret_3d"),
                    as_score_param(merged.get("ret_5d"), "ret_5d"),
                    as_score_param(merged.get("ret_10d"), "ret_10d"),
                    as_score_param(merged.get("ret_20d"), "ret_20d"),
                    as_score_param(merged.get("max_favorable_excursion"),
                                   "max_favorable_excursion"),
                    as_score_param(merged.get("max_adverse_excursion"),
                                   "max_adverse_excursion"),
                    None if merged.get("mfe_mae_bar_count") is None
                    else as_int_param(merged["mfe_mae_bar_count"], "mfe_mae_bar_count"),
                    benchmark_json,
                    notes_json,
                    as_bool_param(bool(merged.get("reference_revision_detected")),
                                  "reference_revision_detected"),
                    merged["outcome_status"],
                    merged.get("error_code"),
                    error_message,
                    as_utc_datetime_param(now, "first_calculated_at"),
                    as_utc_datetime_param(now, "calculated_at"),
                )
                if inserted is not None:
                    return {
                        "outcome_id": str(outcome_id),
                        "created_new": True,
                        "outcome_status": merged["outcome_status"],
                    }
                # Lost the insert race: lock the winner's committed row and
                # re-merge against it so nothing it matured can be lost.
                existing_row = await conn.fetchrow(
                    f"SELECT {_OUTCOME_COLUMNS} "
                    "FROM strategy_shadow_pair_outcomes "
                    "WHERE pair_id = $1 FOR UPDATE",
                    as_uuid_param(record["pair_id"], "pair_id"),
                )
                existing = _existing_row_to_dict(existing_row)
                merged = merge_outcome_for_persistence(
                    existing, record, detected_at=now.isoformat()
                )
                benchmark_json = (
                    strict_json(normalize_json_safe(merged["benchmark_returns"]))
                    if merged.get("benchmark_returns") is not None
                    else None
                )
                notes_json = (
                    strict_json(normalize_json_safe(merged["revision_notes"]))
                    if merged.get("revision_notes")
                    else None
                )
                error_message = sanitize_scan_error(
                    merged.get("error_message"), max_len=MAX_ERROR_TEXT_LEN
                )

            # The Python merge (computed against the FOR UPDATE-locked row)
            # already enforces the freeze contract; the statement itself
            # ALSO enforces it so no interleaving can ever overwrite frozen
            # evidence at the database boundary:
            #   * ret_*d: existing-value-first (COALESCE(column, incoming));
            #   * reference_price / forward_provider: immutable once set;
            #   * identity/version columns: never in the SET list at all;
            #   * available_forward_bars: monotonic (GREATEST);
            #   * forward metadata + MFE/MAE: only replaced when the
            #     incoming completed bar count is strictly larger;
            #   * outcome_status: 'complete' never regresses;
            #   * reference_revision_detected: sticky OR.
            # benchmark_returns / revision_notes are merged per-key in
            # Python under the row lock (JSONB per-key SQL merge is not
            # expressible with simple column guards); the lock serializes
            # those writes.
            await conn.execute(
                """
                UPDATE strategy_shadow_pair_outcomes
                SET reference_price = COALESCE(reference_price, $2),
                    forward_provider = COALESCE(forward_provider, $3),
                    forward_data_as_of = CASE
                        WHEN $5 > available_forward_bars THEN $4
                        ELSE forward_data_as_of END,
                    first_forward_date = CASE
                        WHEN $5 > available_forward_bars THEN $6
                        ELSE first_forward_date END,
                    last_forward_date = CASE
                        WHEN $5 > available_forward_bars THEN $7
                        ELSE last_forward_date END,
                    forward_bars_hash = CASE
                        WHEN $5 > available_forward_bars THEN $8
                        ELSE forward_bars_hash END,
                    available_forward_bars =
                        GREATEST(available_forward_bars, $5),
                    ret_1d = COALESCE(ret_1d, $9),
                    ret_3d = COALESCE(ret_3d, $10),
                    ret_5d = COALESCE(ret_5d, $11),
                    ret_10d = COALESCE(ret_10d, $12),
                    ret_20d = COALESCE(ret_20d, $13),
                    max_favorable_excursion = CASE
                        WHEN COALESCE($16, -1) > COALESCE(mfe_mae_bar_count, -1)
                        THEN $14 ELSE max_favorable_excursion END,
                    max_adverse_excursion = CASE
                        WHEN COALESCE($16, -1) > COALESCE(mfe_mae_bar_count, -1)
                        THEN $15 ELSE max_adverse_excursion END,
                    mfe_mae_bar_count = CASE
                        WHEN COALESCE($16, -1) > COALESCE(mfe_mae_bar_count, -1)
                        THEN $16 ELSE mfe_mae_bar_count END,
                    benchmark_returns = $17,
                    revision_notes = $18,
                    reference_revision_detected =
                        (reference_revision_detected OR $19),
                    outcome_status = CASE
                        WHEN outcome_status = 'complete' THEN 'complete'
                        ELSE $20 END,
                    error_code = $21,
                    error_message = $22,
                    first_calculated_at = COALESCE(first_calculated_at, $23),
                    calculated_at = $23,
                    updated_at = NOW()
                WHERE pair_id = $1
                """,
                as_uuid_param(record["pair_id"], "pair_id"),
                as_score_param(merged.get("reference_price"), "reference_price"),
                merged.get("forward_provider"),
                _date_or_none(merged.get("forward_data_as_of"), "forward_data_as_of"),
                as_int_param(merged.get("available_forward_bars") or 0,
                             "available_forward_bars"),
                _date_or_none(merged.get("first_forward_date"), "first_forward_date"),
                _date_or_none(merged.get("last_forward_date"), "last_forward_date"),
                merged.get("forward_bars_hash"),
                as_score_param(merged.get("ret_1d"), "ret_1d"),
                as_score_param(merged.get("ret_3d"), "ret_3d"),
                as_score_param(merged.get("ret_5d"), "ret_5d"),
                as_score_param(merged.get("ret_10d"), "ret_10d"),
                as_score_param(merged.get("ret_20d"), "ret_20d"),
                as_score_param(merged.get("max_favorable_excursion"),
                               "max_favorable_excursion"),
                as_score_param(merged.get("max_adverse_excursion"),
                               "max_adverse_excursion"),
                None if merged.get("mfe_mae_bar_count") is None
                else as_int_param(merged["mfe_mae_bar_count"], "mfe_mae_bar_count"),
                benchmark_json,
                notes_json,
                as_bool_param(bool(merged.get("reference_revision_detected")),
                              "reference_revision_detected"),
                merged["outcome_status"],
                merged.get("error_code"),
                error_message,
                as_utc_datetime_param(now, "calculated_at"),
            )
            return {
                "outcome_id": existing["outcome_id"],
                "created_new": False,
                "outcome_status": merged["outcome_status"],
            }
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Read queries (bounded; list responses never include full B1 snapshots)
# --------------------------------------------------------------------------- #

# Arm joins are POSITIONAL: every declared experiment persists exactly one
# 'control*' and one 'candidate*' arm per pair (migration 010/013 CHECK), so
# the prefix join stays unique. The real arm codes are selected and echoed.
_OUTCOME_LIST_SQL = """
    SELECT p.id AS pair_id, p.experiment_code, p.experiment_version,
           p.symbol, p.timeframe, p.provider AS pair_provider,
           p.snapshot_date, p.frame_hash, p.frame_bar_count,
           p.pair_fingerprint, p.pair_fingerprint_version,
           c.arm_code AS control_arm_code,
           c.strategy_code AS control_strategy_code,
           c.strategy_version AS control_strategy_version,
           c.decision_policy_version AS control_decision_policy_version,
           c.config_hash AS control_config_hash,
           c.verdict AS control_verdict,
           x.arm_code AS candidate_arm_code,
           x.strategy_code AS candidate_strategy_code,
           x.strategy_version AS candidate_strategy_version,
           x.decision_policy_version AS candidate_decision_policy_version,
           x.config_hash AS candidate_config_hash,
           x.verdict AS candidate_verdict,
           o.id AS outcome_id, o.outcome_fingerprint,
           o.outcome_fingerprint_version, o.calculation_version,
           o.outcome_coverage_version, o.forward_frame_version,
           o.reference_price, o.reference_price_role, o.forward_provider,
           o.forward_data_as_of, o.available_forward_bars,
           o.first_forward_date, o.last_forward_date, o.forward_bars_hash,
           o.ret_1d, o.ret_3d, o.ret_5d, o.ret_10d, o.ret_20d,
           o.max_favorable_excursion, o.max_adverse_excursion,
           o.mfe_mae_bar_count, o.benchmark_returns,
           o.reference_revision_detected, o.outcome_status,
           o.error_code, o.first_calculated_at, o.calculated_at
    FROM strategy_shadow_pair_outcomes o
    JOIN strategy_shadow_pairs p ON p.id = o.pair_id
    JOIN strategy_shadow_evaluations c
      ON c.pair_id = p.id AND c.arm_code LIKE 'control%'
    JOIN strategy_shadow_evaluations x
      ON x.pair_id = p.id AND x.arm_code LIKE 'candidate%'
"""


def _outcome_fields(row: Any) -> Dict[str, Any]:
    return {
        "outcome_id": str(row["outcome_id"]),
        "outcome_fingerprint": row["outcome_fingerprint"],
        "outcome_fingerprint_version": row["outcome_fingerprint_version"],
        "calculation_version": row["calculation_version"],
        "outcome_coverage_version": row["outcome_coverage_version"],
        "forward_frame_version": row["forward_frame_version"],
        "reference_price": row["reference_price"],
        "reference_price_role": row["reference_price_role"],
        "forward_provider": row["forward_provider"],
        "forward_data_as_of": row["forward_data_as_of"],
        "available_forward_bars": row["available_forward_bars"],
        "first_forward_date": row["first_forward_date"],
        "last_forward_date": row["last_forward_date"],
        "forward_bars_hash": row["forward_bars_hash"],
        "returns": {
            window_label(w): row[f"ret_{w}d"] for w in HOLDING_WINDOWS
        },
        "max_favorable_excursion": row["max_favorable_excursion"],
        "max_adverse_excursion": row["max_adverse_excursion"],
        "mfe_mae_bar_count": row["mfe_mae_bar_count"],
        "benchmark_returns": _maybe_json(row["benchmark_returns"]),
        "reference_revision_detected": row["reference_revision_detected"],
        "outcome_status": row["outcome_status"],
        "error_code": row["error_code"],
        "first_calculated_at": row["first_calculated_at"],
        "calculated_at": row["calculated_at"],
    }


def _relative_returns(row: Any) -> Dict[str, Dict[str, Optional[float]]]:
    """pair_return - benchmark_return per benchmark per window (read-time)."""
    bench = _maybe_json(row["benchmark_returns"]) or {}
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for name in BENCHMARK_SYMBOLS:
        by_label = bench.get(name) or {}
        rel: Dict[str, Optional[float]] = {}
        for w in HOLDING_WINDOWS:
            label = window_label(w)
            pair_ret = row[f"ret_{w}d"]
            bench_ret = by_label.get(label)
            rel[label] = (
                pair_ret - bench_ret
                if pair_ret is not None and bench_ret is not None
                else None
            )
        out[name] = rel
    return out


def _outcome_list_item(row: Any) -> Dict[str, Any]:
    control_verdict = row["control_verdict"]
    candidate_verdict = row["candidate_verdict"]
    # Historical callers/fakes may omit the arm-code columns; the sma150
    # arms are the only rows that can predate arm-code selection.
    control_arm = row["control_arm_code"] or CONTROL_ARM_CODE
    candidate_arm = row["candidate_arm_code"] or CANDIDATE_ARM_CODE
    return {
        "pair": {
            "pair_id": str(row["pair_id"]),
            "experiment_code": row["experiment_code"],
            "experiment_version": row["experiment_version"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "provider": row["pair_provider"],
            "snapshot_date": row["snapshot_date"],
            "frame_hash": row["frame_hash"],
            "frame_bar_count": row["frame_bar_count"],
        },
        "control": {
            "arm_code": control_arm,
            "strategy_code": row["control_strategy_code"],
            "strategy_version": row["control_strategy_version"],
            "decision_policy_version": row["control_decision_policy_version"],
            "config_hash": row["control_config_hash"],
            "verdict": control_verdict,
        },
        "candidate": {
            "arm_code": candidate_arm,
            "strategy_code": row["candidate_strategy_code"],
            "strategy_version": row["candidate_strategy_version"],
            "decision_policy_version": row["candidate_decision_policy_version"],
            "config_hash": row["candidate_config_hash"],
            "verdict": candidate_verdict,
        },
        "agreement": control_verdict == candidate_verdict,
        "disagreement_category": disagreement_category(
            control_verdict,
            candidate_verdict,
            control_label=category_label_for_arm(control_arm),
            candidate_label=category_label_for_arm(candidate_arm),
        ),
        "outcome": _outcome_fields(row),
        "relative_returns": _relative_returns(row),
    }


async def fetch_pair_outcomes(
    *,
    pair_id: Optional[str] = None,
    symbol: Optional[str] = None,
    run_id: Optional[str] = None,
    experiment_code: Optional[str] = None,
    control_strategy_code: Optional[str] = None,
    candidate_strategy_code: Optional[str] = None,
    outcome_status: Optional[str] = None,
    forward_provider: Optional[str] = None,
    control_verdict: Optional[str] = None,
    candidate_verdict: Optional[str] = None,
    disagreement_category_filter: Optional[str] = None,
    control_strategy_version: Optional[str] = None,
    candidate_strategy_version: Optional[str] = None,
    control_decision_policy_version: Optional[str] = None,
    candidate_decision_policy_version: Optional[str] = None,
    control_config_hash: Optional[str] = None,
    candidate_config_hash: Optional[str] = None,
    min_snapshot_date: Optional[date] = None,
    max_snapshot_date: Optional[date] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Bounded joined outcome rows with AND-composed filters.

    Never returns the full B1 frame_snapshot or details_snapshot.
    """
    where: List[str] = []
    params: List[Any] = []

    def _add(clause: str, value: Any) -> None:
        params.append(value)
        where.append(clause.format(n=len(params)))

    if pair_id is not None:
        _add("p.id = ${n}", as_uuid_param(pair_id, "pair_id"))
    if symbol is not None:
        _add("p.symbol = ${n}", symbol.upper())
    if run_id is not None:
        _add(
            "p.id IN (SELECT pair_id FROM strategy_shadow_run_pairs "
            "WHERE run_id = ${n})",
            as_uuid_param(run_id, "run_id"),
        )
    if experiment_code is not None:
        _add("p.experiment_code = ${n}", experiment_code)
    if control_strategy_code is not None:
        _add("c.strategy_code = ${n}", control_strategy_code)
    if candidate_strategy_code is not None:
        _add("x.strategy_code = ${n}", candidate_strategy_code)
    if outcome_status is not None:
        _add("o.outcome_status = ${n}", outcome_status)
    if forward_provider is not None:
        _add("o.forward_provider = ${n}", forward_provider)
    if control_verdict is not None:
        _add("c.verdict = ${n}", control_verdict.upper())
    if candidate_verdict is not None:
        _add("x.verdict = ${n}", candidate_verdict.upper())
    if disagreement_category_filter is not None:
        # SQL twin of fingerprints.disagreement_category: historical sma150
        # arm codes keep 'v2'/'v3' labels; other arm codes map to neutral
        # positional labels.
        _add(
            "(CASE WHEN c.verdict = x.verdict "
            "THEN 'same_' || lower(c.verdict) "
            "ELSE (CASE WHEN c.arm_code = 'control_v2' "
            "THEN 'v2' ELSE 'control' END)"
            " || '_' || lower(c.verdict) || '_' || "
            "(CASE WHEN x.arm_code = 'candidate_v3' "
            "THEN 'v3' ELSE 'candidate' END)"
            " || '_' || lower(x.verdict) "
            "END) = ${n}",
            disagreement_category_filter,
        )
    if control_strategy_version is not None:
        _add("c.strategy_version = ${n}", control_strategy_version)
    if candidate_strategy_version is not None:
        _add("x.strategy_version = ${n}", candidate_strategy_version)
    if control_decision_policy_version is not None:
        _add("c.decision_policy_version = ${n}", control_decision_policy_version)
    if candidate_decision_policy_version is not None:
        _add("x.decision_policy_version = ${n}", candidate_decision_policy_version)
    if control_config_hash is not None:
        _add("c.config_hash = ${n}", control_config_hash)
    if candidate_config_hash is not None:
        _add("x.config_hash = ${n}", candidate_config_hash)
    if min_snapshot_date is not None:
        _add("p.snapshot_date >= ${n}", as_date_param(min_snapshot_date,
                                                      "min_snapshot_date"))
    if max_snapshot_date is not None:
        _add("p.snapshot_date <= ${n}", as_date_param(max_snapshot_date,
                                                      "max_snapshot_date"))

    params.append(int(limit))
    query = (
        _OUTCOME_LIST_SQL
        + (("WHERE " + " AND ".join(where)) if where else "")
        + f" ORDER BY p.snapshot_date DESC, p.created_at DESC LIMIT ${len(params)}"
    )
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [_outcome_list_item(r) for r in rows]
    finally:
        await release_db_connection(conn)


async def fetch_pair_outcome_detail(pair_id: str) -> Optional[Dict[str, Any]]:
    """Full bounded inspection of one pair's shared outcome.

    Includes the pair identity, a frame SUMMARY (never the full snapshot),
    both frozen evaluation identities/decisions, the full outcome row and
    the bounded revision diagnostics.
    """
    conn = await get_db_connection()
    try:
        pair_row = await conn.fetchrow(
            """
            SELECT id, origin_run_id, experiment_code, experiment_version,
                   symbol, timeframe, provider, snapshot_date,
                   market_data_as_of, frame_snapshot_version, frame_hash,
                   frame_bar_count, frame_first_date, frame_last_date,
                   pair_fingerprint, pair_fingerprint_version, created_at
            FROM strategy_shadow_pairs
            WHERE id = $1
            """,
            as_uuid_param(pair_id, "pair_id"),
        )
        if pair_row is None:
            return None

        eval_rows = await conn.fetch(
            """
            SELECT arm_code, strategy_code, strategy_version,
                   decision_policy_version, config_hash, verdict, score,
                   reason, rejection_reason, evaluation_fingerprint,
                   evaluation_fingerprint_version
            FROM strategy_shadow_evaluations
            WHERE pair_id = $1
            ORDER BY arm_code
            """,
            pair_row["id"],
        )
        outcome_row = await conn.fetchrow(
            f"SELECT {_OUTCOME_COLUMNS} FROM strategy_shadow_pair_outcomes "
            "WHERE pair_id = $1",
            pair_row["id"],
        )

        evaluations: Dict[str, Dict[str, Any]] = {}
        verdicts: Dict[str, str] = {}
        arm_codes: Dict[str, str] = {}
        for ev in eval_rows:
            verdicts[ev["arm_code"]] = ev["verdict"]
            # Positional roles: exactly one control* and one candidate* arm
            # per pair (arm-code CHECK constraint), any experiment.
            for role in ("control", "candidate"):
                if str(ev["arm_code"]).startswith(role):
                    arm_codes[role] = ev["arm_code"]
            evaluations[ev["arm_code"]] = {
                "arm_code": ev["arm_code"],
                "strategy_code": ev["strategy_code"],
                "strategy_version": ev["strategy_version"],
                "decision_policy_version": ev["decision_policy_version"],
                "config_hash": ev["config_hash"],
                "verdict": ev["verdict"],
                "score": ev["score"],
                "reason": ev["reason"],
                "rejection_reason": ev["rejection_reason"],
                "evaluation_fingerprint": ev["evaluation_fingerprint"],
                "evaluation_fingerprint_version":
                    ev["evaluation_fingerprint_version"],
            }

        outcome: Optional[Dict[str, Any]] = None
        if outcome_row is not None:
            existing = _existing_row_to_dict(outcome_row)
            outcome = {
                **existing,
                "returns": {
                    window_label(w): existing[f"ret_{w}d"]
                    for w in HOLDING_WINDOWS
                },
                "calculated_at": outcome_row["calculated_at"],
            }

        control_arm = arm_codes.get("control", CONTROL_ARM_CODE)
        candidate_arm = arm_codes.get("candidate", CANDIDATE_ARM_CODE)
        control_verdict = verdicts.get(control_arm)
        candidate_verdict = verdicts.get(candidate_arm)
        return {
            "pair_id": str(pair_row["id"]),
            "origin_run_id": (
                str(pair_row["origin_run_id"])
                if pair_row["origin_run_id"] else None
            ),
            "experiment_code": pair_row["experiment_code"],
            "experiment_version": pair_row["experiment_version"],
            "symbol": pair_row["symbol"],
            "timeframe": pair_row["timeframe"],
            "provider": pair_row["provider"],
            "snapshot_date": pair_row["snapshot_date"],
            "market_data_as_of": pair_row["market_data_as_of"],
            "frame_summary": {
                "frame_snapshot_version": pair_row["frame_snapshot_version"],
                "frame_hash": pair_row["frame_hash"],
                "frame_bar_count": pair_row["frame_bar_count"],
                "frame_first_date": pair_row["frame_first_date"],
                "frame_last_date": pair_row["frame_last_date"],
            },
            "pair_fingerprint": pair_row["pair_fingerprint"],
            "pair_fingerprint_version": pair_row["pair_fingerprint_version"],
            "evaluations": evaluations,
            "agreement": (
                control_verdict == candidate_verdict
                if control_verdict and candidate_verdict
                else None
            ),
            "disagreement_category": (
                disagreement_category(
                    control_verdict,
                    candidate_verdict,
                    control_label=category_label_for_arm(control_arm),
                    candidate_label=category_label_for_arm(candidate_arm),
                )
                if control_verdict and candidate_verdict
                else None
            ),
            "outcome": outcome,
            "created_at": pair_row["created_at"],
        }
    finally:
        await release_db_connection(conn)
