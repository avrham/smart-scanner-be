"""DB persistence for shadow runs, pairs and evaluations (Phase 8.1B1).

Writes ONLY to the strategy_shadow_* tables from migration 010. This module
must never touch signals, signal_provenance, scan_run_signals or
signal_outcomes — shadow evaluations are not signals.

Immutability contract (mirrors the Phase 7B signal semantics):
  * a new exact comparison inserts one pair + two evaluations;
  * a repeated exact comparison reuses the existing pair/evaluations and adds
    only a strategy_shadow_run_pairs occurrence link;
  * nothing ever UPDATEs an existing pair or evaluation, and the origin run
    of an existing pair is never replaced.
"""

import json
import logging
import uuid as uuid_lib
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from app.workers.persistence import get_db_connection, release_db_connection
from app.workers.scan_runs import sanitize_scan_error
from app.workers.shadow.constants import (
    CANDIDATE_ARM_CODE,
    CONTROL_ARM_CODE,
    EXPERIMENT_CODE,
    EXPERIMENT_VERSION,
    MAX_ERROR_TEXT_LEN,
    MAX_TELEMETRY_BYTES,
)
from app.workers.shadow.fingerprints import (
    category_label_for_arm,
    disagreement_category,
)
from app.workers.shadow.serialization import normalize_json_safe, strict_json
from app.workers.shadow.typed_values import (
    ShadowPersistenceTypeError,
    as_bool_param,
    as_date_param,
    as_int_param,
    as_score_param,
    as_utc_datetime_param,
    as_uuid_param,
)


logger = logging.getLogger(__name__)


class ShadowIntegrityError(RuntimeError):
    """An existing pair fingerprint is being reused with incompatible data."""


def _bounded_telemetry(telemetry: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialize telemetry strictly, deterministically dropping list payloads
    if the bound is exceeded (scalar counts always survive).

    Telemetry crosses the same explicit normalization boundary as every other
    shadow JSONB field: no default=str fallback that could silently stringify
    an unexpected object into the frozen row."""
    if telemetry is None:
        return None
    telemetry = normalize_json_safe(telemetry)
    text = strict_json(telemetry)
    if len(text.encode("utf-8")) <= MAX_TELEMETRY_BYTES:
        return text
    scalars = {
        k: v for k, v in telemetry.items()
        if isinstance(v, (int, float, str, bool)) or v is None
    }
    scalars["telemetry_truncated"] = True
    return strict_json(scalars)


async def create_shadow_run(
    run_id: str,
    *,
    provider: Optional[str],
    requested_symbols: List[str],
    requested_limit: Optional[int] = None,
    experiment_code: str = EXPERIMENT_CODE,
    experiment_version: str = EXPERIMENT_VERSION,
) -> str:
    """Create the canonical shadow-run row at start (status='running').

    Idempotent on id (ON CONFLICT DO NOTHING). The experiment identity
    defaults to the historical sma150 experiment for existing callers.
    """
    conn = await get_db_connection()
    try:
        await conn.execute(
            """
            INSERT INTO strategy_shadow_runs (
                id, experiment_code, experiment_version, status, provider,
                requested_symbols, requested_limit, started_at,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, 'running', $4, $5, $6, NOW(), NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            as_uuid_param(run_id, "run_id"),
            experiment_code,
            experiment_version,
            provider,
            strict_json([str(s) for s in requested_symbols]),
            None if requested_limit is None
            else as_int_param(requested_limit, "requested_limit"),
        )
        return str(run_id)
    finally:
        await release_db_connection(conn)


async def finalize_shadow_run(
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
            UPDATE strategy_shadow_runs
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
            _bounded_telemetry(telemetry),
            error_code,
            sanitize_scan_error(error_message, max_len=MAX_ERROR_TEXT_LEN),
        )
    finally:
        await release_db_connection(conn)


async def persist_shadow_pair(
    *,
    run_id: str,
    pair: Dict[str, Any],
    evaluations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Insert-or-link one immutable pair with its two arm evaluations.

    `pair` carries the canonical pair record (fingerprint already computed);
    `evaluations` carries exactly the control and candidate records. All
    writes happen in ONE transaction. Returns
    {"pair_id", "created_new_pair"}.
    """
    conn = await get_db_connection()
    try:
        async with conn.transaction():
            existing = await conn.fetchrow(
                """
                SELECT id, symbol, frame_hash
                FROM strategy_shadow_pairs
                WHERE pair_fingerprint = $1 AND pair_fingerprint_version = $2
                """,
                pair["pair_fingerprint"],
                pair["pair_fingerprint_version"],
            )

            if existing is not None:
                # Compatibility check: the same fingerprint MUST describe the
                # same comparison input. Anything else is a corruption signal
                # and must never silently reuse or overwrite frozen data.
                if (
                    existing["symbol"] != pair["symbol"]
                    or existing["frame_hash"] != pair["frame_hash"]
                ):
                    raise ShadowIntegrityError(
                        "pair fingerprint reuse with incompatible "
                        f"symbol/frame for {pair['symbol']}"
                    )
                pair_id = existing["id"]
                created_new = False
            else:
                pair_id = uuid_lib.uuid4()
                await conn.execute(
                    """
                    INSERT INTO strategy_shadow_pairs (
                        id, origin_run_id, experiment_code, experiment_version,
                        symbol, timeframe, provider, snapshot_date,
                        market_data_as_of, frame_snapshot_version, frame_hash,
                        frame_bar_count, frame_first_date, frame_last_date,
                        frame_snapshot, pair_fingerprint,
                        pair_fingerprint_version, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            $12, $13, $14, $15, $16, $17, NOW())
                    """,
                    pair_id,
                    as_uuid_param(run_id, "origin_run_id"),
                    pair["experiment_code"],
                    pair["experiment_version"],
                    pair["symbol"],
                    pair["timeframe"],
                    pair["provider"],
                    # asyncpg's DATE codec requires datetime.date (an ISO
                    # string raises "'str' object has no attribute
                    # 'toordinal'" — the live pair_error). The canonical
                    # frame keeps ISO strings for hashing/JSON; conversion
                    # to the driver type happens ONLY here, at the typed
                    # persistence boundary.
                    as_date_param(pair["snapshot_date"], "snapshot_date"),
                    as_utc_datetime_param(
                        pair["market_data_as_of"], "market_data_as_of"
                    ),
                    pair["frame_snapshot_version"],
                    pair["frame_hash"],
                    as_int_param(pair["frame_bar_count"], "frame_bar_count"),
                    as_date_param(pair["frame_first_date"], "frame_first_date"),
                    as_date_param(pair["frame_last_date"], "frame_last_date"),
                    strict_json(pair["frame_snapshot"]),
                    pair["pair_fingerprint"],
                    pair["pair_fingerprint_version"],
                )
                for ev in evaluations:
                    await conn.execute(
                        """
                        INSERT INTO strategy_shadow_evaluations (
                            id, pair_id, arm_code, strategy_code,
                            strategy_version, decision_policy_version,
                            config_hash, config_snapshot, verdict, score,
                            reason, rejection_reason, details_snapshot,
                            evidence_original_sha256, evaluation_fingerprint,
                            evaluation_fingerprint_version, created_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                                $11, $12, $13, $14, $15, $16, NOW())
                        """,
                        uuid_lib.uuid4(),
                        pair_id,
                        ev["arm_code"],
                        ev["strategy_code"],
                        ev["strategy_version"],
                        ev["decision_policy_version"],
                        ev["config_hash"],
                        strict_json(ev["config_snapshot"]),
                        ev["verdict"],
                        as_score_param(ev["score"], "score"),
                        ev["reason"],
                        ev["rejection_reason"],
                        strict_json(ev["details_snapshot"]),
                        ev["evidence_original_sha256"],
                        ev["evaluation_fingerprint"],
                        ev["evaluation_fingerprint_version"],
                    )
                created_new = True

            # Occurrence link for EVERY run that produced this exact pair.
            await conn.execute(
                """
                INSERT INTO strategy_shadow_run_pairs (
                    run_id, pair_id, created_new_pair, linked_at
                )
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (run_id, pair_id) DO NOTHING
                """,
                as_uuid_param(run_id, "run_id"),
                as_uuid_param(pair_id, "pair_id"),
                as_bool_param(created_new, "created_new_pair"),
            )

        return {"pair_id": str(pair_id), "created_new_pair": created_new}
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Read queries (bounded; list responses never include full snapshots)
# --------------------------------------------------------------------------- #

def _maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


async def fetch_shadow_run(run_id: str) -> Optional[Dict[str, Any]]:
    """One shadow run with its bounded telemetry (no pair payloads)."""
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            """
            SELECT id, experiment_code, experiment_version, status, provider,
                   requested_symbols, requested_limit, started_at, finished_at,
                   telemetry, error_code, error_message, created_at, updated_at
            FROM strategy_shadow_runs
            WHERE id = $1
            """,
            uuid_lib.UUID(str(run_id)),
        )
        if row is None:
            return None
        return {
            "run_id": str(row["id"]),
            "experiment_code": row["experiment_code"],
            "experiment_version": row["experiment_version"],
            "status": row["status"],
            "provider": row["provider"],
            "requested_symbols": _maybe_json(row["requested_symbols"]),
            "requested_limit": row["requested_limit"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "telemetry": _maybe_json(row["telemetry"]),
            "error_code": row["error_code"],
            "error_message": row["error_message"],
        }
    finally:
        await release_db_connection(conn)


# Arm joins are POSITIONAL: every declared experiment persists exactly one
# 'control*' and one 'candidate*' arm per pair (migration 010/013 CHECK), so
# the prefix join stays unique. The real arm codes are selected and echoed —
# never rewritten to the sma150 constants.
_PAIR_LIST_SQL = """
    SELECT p.id, p.origin_run_id, p.experiment_code, p.experiment_version,
           p.symbol, p.timeframe, p.provider, p.snapshot_date,
           p.market_data_as_of, p.frame_snapshot_version, p.frame_hash,
           p.frame_bar_count, p.frame_first_date, p.frame_last_date,
           p.pair_fingerprint, p.pair_fingerprint_version, p.created_at,
           c.arm_code AS control_arm_code,
           c.strategy_code AS control_strategy_code,
           c.strategy_version AS control_strategy_version,
           c.decision_policy_version AS control_decision_policy_version,
           c.config_hash AS control_config_hash,
           c.verdict AS control_verdict,
           c.score AS control_score,
           c.reason AS control_reason,
           c.rejection_reason AS control_rejection_reason,
           x.arm_code AS candidate_arm_code,
           x.strategy_code AS candidate_strategy_code,
           x.strategy_version AS candidate_strategy_version,
           x.decision_policy_version AS candidate_decision_policy_version,
           x.config_hash AS candidate_config_hash,
           x.verdict AS candidate_verdict,
           x.score AS candidate_score,
           x.reason AS candidate_reason,
           x.rejection_reason AS candidate_rejection_reason
    FROM strategy_shadow_pairs p
    JOIN strategy_shadow_evaluations c
      ON c.pair_id = p.id AND c.arm_code LIKE 'control%'
    JOIN strategy_shadow_evaluations x
      ON x.pair_id = p.id AND x.arm_code LIKE 'candidate%'
"""


def _pair_summary(row: Any) -> Dict[str, Any]:
    """Bounded pair summary (never the full frame/details snapshots)."""
    control_verdict = row["control_verdict"]
    candidate_verdict = row["candidate_verdict"]
    # Historical callers/fakes may omit the arm-code columns; the sma150
    # arms are the only rows that can predate arm-code selection.
    control_arm = row["control_arm_code"] or CONTROL_ARM_CODE
    candidate_arm = row["candidate_arm_code"] or CANDIDATE_ARM_CODE
    return {
        "pair_id": str(row["id"]),
        "origin_run_id": str(row["origin_run_id"]) if row["origin_run_id"] else None,
        "experiment_code": row["experiment_code"],
        "experiment_version": row["experiment_version"],
        "symbol": row["symbol"],
        "timeframe": row["timeframe"],
        "provider": row["provider"],
        "snapshot_date": row["snapshot_date"],
        "market_data_as_of": row["market_data_as_of"],
        "frame_snapshot_version": row["frame_snapshot_version"],
        "frame_hash": row["frame_hash"],
        "frame_bar_count": row["frame_bar_count"],
        "control": {
            "arm_code": control_arm,
            "strategy_code": row["control_strategy_code"],
            "strategy_version": row["control_strategy_version"],
            "decision_policy_version": row["control_decision_policy_version"],
            "config_hash": row["control_config_hash"],
            "verdict": control_verdict,
            "score": row["control_score"],
            "reason": row["control_reason"],
            "rejection_reason": row["control_rejection_reason"],
        },
        "candidate": {
            "arm_code": candidate_arm,
            "strategy_code": row["candidate_strategy_code"],
            "strategy_version": row["candidate_strategy_version"],
            "decision_policy_version": row["candidate_decision_policy_version"],
            "config_hash": row["candidate_config_hash"],
            "verdict": candidate_verdict,
            "score": row["candidate_score"],
            "reason": row["candidate_reason"],
            "rejection_reason": row["candidate_rejection_reason"],
        },
        "agreement": control_verdict == candidate_verdict,
        "disagreement_category": disagreement_category(
            control_verdict,
            candidate_verdict,
            control_label=category_label_for_arm(control_arm),
            candidate_label=category_label_for_arm(candidate_arm),
        ),
        "created_at": row["created_at"],
    }


# Deterministic SQL twin of fingerprints.disagreement_category: historical
# sma150 arm codes keep 'v2'/'v3' labels, every other arm code maps to its
# neutral positional label.
_CATEGORY_CASE_SQL = (
    "(CASE WHEN c.verdict = x.verdict "
    "THEN 'same_' || lower(c.verdict) "
    "ELSE (CASE WHEN c.arm_code = 'control_v2' THEN 'v2' ELSE 'control' END)"
    " || '_' || lower(c.verdict) || '_' || "
    "(CASE WHEN x.arm_code = 'candidate_v3' THEN 'v3' ELSE 'candidate' END)"
    " || '_' || lower(x.verdict) "
    "END)"
)


async def fetch_shadow_pairs(
    *,
    run_id: Optional[str] = None,
    symbol: Optional[str] = None,
    experiment_code: Optional[str] = None,
    control_strategy_code: Optional[str] = None,
    candidate_strategy_code: Optional[str] = None,
    control_verdict: Optional[str] = None,
    candidate_verdict: Optional[str] = None,
    agreement: Optional[bool] = None,
    disagreement_category_filter: Optional[str] = None,
    control_strategy_version: Optional[str] = None,
    candidate_strategy_version: Optional[str] = None,
    control_config_hash: Optional[str] = None,
    candidate_config_hash: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Bounded pair summaries with AND-composed filters."""
    where: List[str] = []
    params: List[Any] = []

    def _add(clause: str, value: Any) -> None:
        params.append(value)
        where.append(clause.format(n=len(params)))

    if run_id is not None:
        _add(
            "p.id IN (SELECT pair_id FROM strategy_shadow_run_pairs "
            "WHERE run_id = ${n})",
            uuid_lib.UUID(str(run_id)),
        )
    if symbol is not None:
        _add("p.symbol = ${n}", symbol.upper())
    if experiment_code is not None:
        _add("p.experiment_code = ${n}", experiment_code)
    if control_strategy_code is not None:
        _add("c.strategy_code = ${n}", control_strategy_code)
    if candidate_strategy_code is not None:
        _add("x.strategy_code = ${n}", candidate_strategy_code)
    if control_verdict is not None:
        _add("c.verdict = ${n}", control_verdict.upper())
    if candidate_verdict is not None:
        _add("x.verdict = ${n}", candidate_verdict.upper())
    if agreement is not None:
        where.append(
            "(c.verdict = x.verdict)" if agreement else "(c.verdict <> x.verdict)"
        )
    if disagreement_category_filter is not None:
        _add(
            _CATEGORY_CASE_SQL + " = ${n}",
            disagreement_category_filter,
        )
    if control_strategy_version is not None:
        _add("c.strategy_version = ${n}", control_strategy_version)
    if candidate_strategy_version is not None:
        _add("x.strategy_version = ${n}", candidate_strategy_version)
    if control_config_hash is not None:
        _add("c.config_hash = ${n}", control_config_hash)
    if candidate_config_hash is not None:
        _add("x.config_hash = ${n}", candidate_config_hash)

    params.append(limit)
    query = (
        _PAIR_LIST_SQL
        + (("WHERE " + " AND ".join(where)) if where else "")
        + f" ORDER BY p.created_at DESC LIMIT ${len(params)}"
    )

    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [_pair_summary(r) for r in rows]
    finally:
        await release_db_connection(conn)


async def fetch_shadow_pair_detail(pair_id: str) -> Optional[Dict[str, Any]]:
    """Full bounded inspection of one pair: canonical frame + both frozen
    evaluation snapshots + run occurrence links."""
    conn = await get_db_connection()
    try:
        pair_row = await conn.fetchrow(
            """
            SELECT id, origin_run_id, experiment_code, experiment_version,
                   symbol, timeframe, provider, snapshot_date,
                   market_data_as_of, frame_snapshot_version, frame_hash,
                   frame_bar_count, frame_first_date, frame_last_date,
                   frame_snapshot, pair_fingerprint, pair_fingerprint_version,
                   created_at
            FROM strategy_shadow_pairs
            WHERE id = $1
            """,
            uuid_lib.UUID(str(pair_id)),
        )
        if pair_row is None:
            return None

        eval_rows = await conn.fetch(
            """
            SELECT arm_code, strategy_code, strategy_version,
                   decision_policy_version, config_hash, config_snapshot,
                   verdict, score, reason, rejection_reason, details_snapshot,
                   evidence_original_sha256, evaluation_fingerprint,
                   evaluation_fingerprint_version, created_at
            FROM strategy_shadow_evaluations
            WHERE pair_id = $1
            ORDER BY arm_code
            """,
            pair_row["id"],
        )
        link_rows = await conn.fetch(
            """
            SELECT run_id, created_new_pair, linked_at
            FROM strategy_shadow_run_pairs
            WHERE pair_id = $1
            ORDER BY linked_at ASC
            """,
            pair_row["id"],
        )

        evaluations = {}
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
                "config_snapshot": _maybe_json(ev["config_snapshot"]),
                "verdict": ev["verdict"],
                "score": ev["score"],
                "reason": ev["reason"],
                "rejection_reason": ev["rejection_reason"],
                "details_snapshot": _maybe_json(ev["details_snapshot"]),
                "evidence_original_sha256": ev["evidence_original_sha256"],
                "evaluation_fingerprint": ev["evaluation_fingerprint"],
                "evaluation_fingerprint_version": ev["evaluation_fingerprint_version"],
                "created_at": ev["created_at"],
            }

        control_arm = arm_codes.get("control", CONTROL_ARM_CODE)
        candidate_arm = arm_codes.get("candidate", CANDIDATE_ARM_CODE)
        control_verdict = verdicts.get(control_arm)
        candidate_verdict = verdicts.get(candidate_arm)
        return {
            "pair_id": str(pair_row["id"]),
            "origin_run_id": (
                str(pair_row["origin_run_id"]) if pair_row["origin_run_id"] else None
            ),
            "experiment_code": pair_row["experiment_code"],
            "experiment_version": pair_row["experiment_version"],
            "symbol": pair_row["symbol"],
            "timeframe": pair_row["timeframe"],
            "provider": pair_row["provider"],
            "snapshot_date": pair_row["snapshot_date"],
            "market_data_as_of": pair_row["market_data_as_of"],
            "frame_snapshot_version": pair_row["frame_snapshot_version"],
            "frame_hash": pair_row["frame_hash"],
            "frame_bar_count": pair_row["frame_bar_count"],
            "frame_first_date": pair_row["frame_first_date"],
            "frame_last_date": pair_row["frame_last_date"],
            "frame_snapshot": _maybe_json(pair_row["frame_snapshot"]),
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
            "run_occurrences": [
                {
                    "run_id": str(l["run_id"]),
                    "created_new_pair": l["created_new_pair"],
                    "linked_at": l["linked_at"],
                }
                for l in link_rows
            ],
            "created_at": pair_row["created_at"],
        }
    finally:
        await release_db_connection(conn)


def _run_summary(row: Any) -> Dict[str, Any]:
    return {
        "run_id": str(row["id"]),
        "experiment_code": row["experiment_code"],
        "experiment_version": row["experiment_version"],
        "status": row["status"],
        "provider": row["provider"],
        "requested_symbols": _maybe_json(row["requested_symbols"]),
        "requested_limit": row["requested_limit"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "error_code": row["error_code"],
        "created_at": row["created_at"],
    }


async def fetch_shadow_runs(
    *,
    experiment_code: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Bounded newest-first shadow-run summaries (Phase 9D6).

    List rows never include telemetry or error text — use fetch_shadow_run
    for one run's bounded detail. Filters AND-compose.
    """
    where: List[str] = []
    params: List[Any] = []

    def _add(clause: str, value: Any) -> None:
        params.append(value)
        where.append(clause.format(n=len(params)))

    if experiment_code is not None:
        _add("experiment_code = ${n}", experiment_code)
    if status is not None:
        _add("status = ${n}", status)

    params.append(int(limit))
    query = (
        """
        SELECT id, experiment_code, experiment_version, status, provider,
               requested_symbols, requested_limit, started_at, finished_at,
               error_code, created_at
        FROM strategy_shadow_runs
        """
        + (("WHERE " + " AND ".join(where)) if where else "")
        + f" ORDER BY started_at DESC LIMIT ${len(params)}"
    )

    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [_run_summary(r) for r in rows]
    finally:
        await release_db_connection(conn)


# Bounded per-evaluation record for strategy-filtered shadow metrics
# (Phase 9D5/9D6). Deliberately extracts ONLY compact JSONB sub-documents
# (the policy record, the readiness status and the evidence item categories)
# — never the full details or frame snapshots.
_STRATEGY_EVALUATION_SQL = """
    SELECT e.id, e.pair_id, e.arm_code, e.strategy_code, e.strategy_version,
           e.decision_policy_version, e.config_hash, e.verdict, e.score,
           e.reason, e.rejection_reason, e.created_at,
           e.details_snapshot->'policy' AS policy,
           e.details_snapshot->'readiness'->>'status' AS readiness_status,
           e.details_snapshot->'four_hour_trigger' AS four_hour_trigger,
           e.details_snapshot->'_four_hour_frame_meta' AS four_hour_frame_meta,
           jsonb_path_query_array(
               e.details_snapshot, '$.evidence.items[*].category'
           ) AS evidence_categories,
           p.symbol, p.snapshot_date, p.experiment_code, p.experiment_version,
           p.frame_snapshot_version AS daily_frame_contract_version,
           p.provider,
           (o.id IS NOT NULL) AS has_outcome,
           o.outcome_status AS outcome_status,
           (SELECT jsonb_agg(DISTINCT r.telemetry->'campaign'->>'campaign_id')
            FROM strategy_shadow_run_pairs rp
            JOIN strategy_shadow_runs r ON r.id = rp.run_id
            WHERE rp.pair_id = p.id
              AND r.telemetry->'campaign' IS NOT NULL) AS campaign_ids
    FROM strategy_shadow_evaluations e
    JOIN strategy_shadow_pairs p ON p.id = e.pair_id
    LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
"""


def _evaluation_record(row: Any) -> Dict[str, Any]:
    categories = _maybe_json(row["evidence_categories"])
    return {
        "evaluation_id": str(row["id"]),
        "pair_id": str(row["pair_id"]),
        "arm_code": row["arm_code"],
        "strategy_code": row["strategy_code"],
        "strategy_version": row["strategy_version"],
        "decision_policy_version": row["decision_policy_version"],
        "config_hash": row["config_hash"],
        "verdict": row["verdict"],
        "score": row["score"],
        "reason": row["reason"],
        "rejection_reason": row["rejection_reason"],
        "policy": _maybe_json(row["policy"]),
        "readiness_status": row["readiness_status"],
        "four_hour_trigger": _maybe_json(row["four_hour_trigger"]),
        "four_hour_frame_meta": _maybe_json(row["four_hour_frame_meta"]),
        "evidence_categories": categories if isinstance(categories, list) else [],
        "symbol": row["symbol"],
        "snapshot_date": row["snapshot_date"],
        "experiment_code": row["experiment_code"],
        "experiment_version": row["experiment_version"],
        "daily_frame_contract_version": row["daily_frame_contract_version"],
        "provider": row["provider"],
        "has_outcome": bool(row["has_outcome"]),
        "outcome_status": row["outcome_status"],
        "campaign_ids": (
            [c for c in (_maybe_json(row["campaign_ids"]) or []) if c]
            if isinstance(_maybe_json(row["campaign_ids"]), list)
            else []
        ),
        "created_at": row["created_at"],
    }


async def fetch_strategy_shadow_evaluations(
    *,
    strategy_code: str,
    symbol: Optional[str] = None,
    strategy_version: Optional[str] = None,
    decision_policy_version: Optional[str] = None,
    experiment_code: Optional[str] = None,
    config_hash: Optional[str] = None,
    campaign_id: Optional[str] = None,
    min_snapshot_date: Optional[date] = None,
    max_snapshot_date: Optional[date] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Bounded newest-first per-evaluation records for ONE strategy code.

    Read-only aggregation source for the strategy shadow metrics — never
    returns full details or frame snapshots and never mutates anything.
    """
    where: List[str] = []
    params: List[Any] = []

    def _add(clause: str, value: Any) -> None:
        params.append(value)
        where.append(clause.format(n=len(params)))

    _add("e.strategy_code = ${n}", strategy_code)
    if symbol is not None:
        _add("p.symbol = ${n}", symbol.upper())
    if strategy_version is not None:
        _add("e.strategy_version = ${n}", strategy_version)
    if decision_policy_version is not None:
        _add("e.decision_policy_version = ${n}", decision_policy_version)
    if experiment_code is not None:
        _add("p.experiment_code = ${n}", experiment_code)
    if config_hash is not None:
        _add("e.config_hash = ${n}", config_hash)
    if campaign_id is not None:
        _add(
            "EXISTS (SELECT 1 FROM strategy_shadow_run_pairs rp2 "
            "JOIN strategy_shadow_runs r2 ON r2.id = rp2.run_id "
            "WHERE rp2.pair_id = p.id "
            "AND r2.telemetry->'campaign'->>'campaign_id' = ${n})",
            str(campaign_id),
        )
    if min_snapshot_date is not None:
        _add("p.snapshot_date >= ${n}",
             as_date_param(min_snapshot_date, "min_snapshot_date"))
    if max_snapshot_date is not None:
        _add("p.snapshot_date <= ${n}",
             as_date_param(max_snapshot_date, "max_snapshot_date"))

    params.append(int(limit))
    query = (
        _STRATEGY_EVALUATION_SQL
        + "WHERE " + " AND ".join(where)
        + f" ORDER BY e.created_at DESC LIMIT ${len(params)}"
    )

    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [_evaluation_record(r) for r in rows]
    finally:
        await release_db_connection(conn)


# --------------------------------------------------------------------------- #
# Campaign reads (Phase 9E8) — campaigns ARE shadow runs whose frozen
# telemetry carries a campaign block; no separate campaign table exists.
# --------------------------------------------------------------------------- #

_CAMPAIGN_RUN_SQL = """
    SELECT id, experiment_code, experiment_version, status, provider,
           requested_symbols, requested_limit, started_at, finished_at,
           error_code, created_at,
           telemetry->'campaign' AS campaign,
           telemetry->'pair_count' AS pair_count,
           telemetry->'pairs_created' AS pairs_created,
           telemetry->'pairs_deduplicated' AS pairs_deduplicated,
           telemetry->'rejected_symbols' AS rejected_symbols
    FROM strategy_shadow_runs
    WHERE telemetry->'campaign' IS NOT NULL
"""


def _campaign_run_row(row: Any) -> Dict[str, Any]:
    return {
        "run_id": str(row["id"]),
        "experiment_code": row["experiment_code"],
        "experiment_version": row["experiment_version"],
        "status": row["status"],
        "provider": row["provider"],
        "requested_symbols": _maybe_json(row["requested_symbols"]),
        "requested_limit": row["requested_limit"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "error_code": row["error_code"],
        "campaign": _maybe_json(row["campaign"]),
        "pair_count": _maybe_json(row["pair_count"]),
        "pairs_created": _maybe_json(row["pairs_created"]),
        "pairs_deduplicated": _maybe_json(row["pairs_deduplicated"]),
        "rejected_symbols": _maybe_json(row["rejected_symbols"]),
        "created_at": row["created_at"],
    }


async def fetch_shadow_campaign_runs(
    *,
    campaign_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Bounded newest-first campaign chunk runs (read-only)."""
    where: List[str] = []
    params: List[Any] = []

    if campaign_id is not None:
        params.append(str(campaign_id))
        where.append(
            f"telemetry->'campaign'->>'campaign_id' = ${len(params)}"
        )

    params.append(int(limit))
    query = (
        _CAMPAIGN_RUN_SQL
        + (("AND " + " AND ".join(where)) if where else "")
        + f" ORDER BY started_at DESC LIMIT ${len(params)}"
    )
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(query, *params)
        return [_campaign_run_row(r) for r in rows]
    finally:
        await release_db_connection(conn)


async def fetch_campaign_outcome_coverage(
    run_ids: List[str],
) -> Dict[str, Any]:
    """Outcome coverage for a campaign's persisted pairs (read-only).

    A pair without an outcome row is reported as 'missing_outcome' — never
    as a zero return.
    """
    if not run_ids:
        return {"pair_count": 0, "with_outcome_count": 0,
                "missing_outcome_count": 0, "outcome_status_distribution": {}}
    conn = await get_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT o.outcome_status AS outcome_status,
                   COUNT(DISTINCT p.id) AS n
            FROM strategy_shadow_run_pairs rp
            JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
            LEFT JOIN strategy_shadow_pair_outcomes o ON o.pair_id = p.id
            WHERE rp.run_id = ANY($1::uuid[])
            GROUP BY o.outcome_status
            """,
            [as_uuid_param(r, "run_ids") for r in run_ids],
        )
        distribution: Dict[str, int] = {}
        missing = 0
        total = 0
        for row in rows:
            n = int(row["n"])
            total += n
            if row["outcome_status"] is None:
                missing += n
            else:
                distribution[str(row["outcome_status"])] = n
        return {
            "pair_count": total,
            "with_outcome_count": total - missing,
            "missing_outcome_count": missing,
            "outcome_status_distribution": dict(sorted(distribution.items())),
        }
    finally:
        await release_db_connection(conn)
