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
from datetime import datetime, timezone
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
from app.workers.shadow.fingerprints import disagreement_category


logger = logging.getLogger(__name__)


class ShadowIntegrityError(RuntimeError):
    """An existing pair fingerprint is being reused with incompatible data."""


def _bounded_telemetry(telemetry: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialize telemetry, deterministically dropping list payloads if the
    bound is exceeded (scalar counts always survive)."""
    if telemetry is None:
        return None
    text = json.dumps(telemetry, default=str)
    if len(text.encode("utf-8")) <= MAX_TELEMETRY_BYTES:
        return text
    scalars = {
        k: v for k, v in telemetry.items()
        if isinstance(v, (int, float, str, bool)) or v is None
    }
    scalars["telemetry_truncated"] = True
    return json.dumps(scalars, default=str)


async def create_shadow_run(
    run_id: str,
    *,
    provider: Optional[str],
    requested_symbols: List[str],
    requested_limit: Optional[int] = None,
) -> str:
    """Create the canonical shadow-run row at start (status='running').

    Idempotent on id (ON CONFLICT DO NOTHING).
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
            uuid_lib.UUID(str(run_id)),
            EXPERIMENT_CODE,
            EXPERIMENT_VERSION,
            provider,
            json.dumps(list(requested_symbols)),
            requested_limit,
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
            uuid_lib.UUID(str(run_id)),
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
                    uuid_lib.UUID(str(run_id)),
                    pair["experiment_code"],
                    pair["experiment_version"],
                    pair["symbol"],
                    pair["timeframe"],
                    pair["provider"],
                    pair["snapshot_date"],
                    pair["market_data_as_of"],
                    pair["frame_snapshot_version"],
                    pair["frame_hash"],
                    pair["frame_bar_count"],
                    pair["frame_first_date"],
                    pair["frame_last_date"],
                    json.dumps(pair["frame_snapshot"]),
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
                        json.dumps(ev["config_snapshot"]),
                        ev["verdict"],
                        ev["score"],
                        ev["reason"],
                        ev["rejection_reason"],
                        json.dumps(ev["details_snapshot"]),
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
                uuid_lib.UUID(str(run_id)),
                pair_id,
                created_new,
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


_PAIR_LIST_SQL = """
    SELECT p.id, p.origin_run_id, p.experiment_code, p.experiment_version,
           p.symbol, p.timeframe, p.provider, p.snapshot_date,
           p.market_data_as_of, p.frame_snapshot_version, p.frame_hash,
           p.frame_bar_count, p.frame_first_date, p.frame_last_date,
           p.pair_fingerprint, p.pair_fingerprint_version, p.created_at,
           c.strategy_code AS control_strategy_code,
           c.strategy_version AS control_strategy_version,
           c.decision_policy_version AS control_decision_policy_version,
           c.config_hash AS control_config_hash,
           c.verdict AS control_verdict,
           c.score AS control_score,
           c.reason AS control_reason,
           c.rejection_reason AS control_rejection_reason,
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
      ON c.pair_id = p.id AND c.arm_code = '{control}'
    JOIN strategy_shadow_evaluations x
      ON x.pair_id = p.id AND x.arm_code = '{candidate}'
""".format(control=CONTROL_ARM_CODE, candidate=CANDIDATE_ARM_CODE)


def _pair_summary(row: Any) -> Dict[str, Any]:
    """Bounded pair summary (never the full frame/details snapshots)."""
    control_verdict = row["control_verdict"]
    candidate_verdict = row["candidate_verdict"]
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
            "arm_code": CONTROL_ARM_CODE,
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
            "arm_code": CANDIDATE_ARM_CODE,
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
            control_verdict, candidate_verdict
        ),
        "created_at": row["created_at"],
    }


async def fetch_shadow_pairs(
    *,
    run_id: Optional[str] = None,
    symbol: Optional[str] = None,
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
            "(CASE WHEN c.verdict = x.verdict "
            "THEN 'same_' || lower(c.verdict) "
            "ELSE 'v2_' || lower(c.verdict) || '_v3_' || lower(x.verdict) "
            "END) = ${n}",
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
        for ev in eval_rows:
            verdicts[ev["arm_code"]] = ev["verdict"]
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

        control_verdict = verdicts.get(CONTROL_ARM_CODE)
        candidate_verdict = verdicts.get(CANDIDATE_ARM_CODE)
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
                disagreement_category(control_verdict, candidate_verdict)
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
