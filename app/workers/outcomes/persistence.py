"""DB persistence for signal outcomes (Phase 2).

Thin async layer over the `signal_outcomes` table. Reuses the pooled-connection
helpers from app.workers.persistence (get/release) so the B11 pool-release
discipline is preserved.
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.workers.persistence import get_db_connection, release_db_connection
from app.workers.outcomes.calculator import HOLDING_WINDOWS, window_label


logger = logging.getLogger(__name__)


# Columns for the signal's own per-window returns, in DB naming (ret_1d..).
def _ret_column(days: int) -> str:
    return f"ret_{days}d"


async def get_signals_needing_outcomes(
    limit: int = 100,
    pattern_code: Optional[str] = None,
    include_recalc: bool = False,
) -> List[Dict[str, Any]]:
    """Return ENTER signals that have no calculated outcome yet.

    When include_recalc is True, also returns signals whose existing outcome is
    in a non-terminal/failed state ('pending', 'error', 'insufficient_data').
    """
    conn = await get_db_connection()
    try:
        where = ["s.verdict = 'ENTER'"]
        params: List[Any] = []

        if include_recalc:
            where.append(
                "(o.id IS NULL OR o.outcome_status IN "
                "('pending', 'error', 'insufficient_data'))"
            )
        else:
            where.append("o.id IS NULL")

        if pattern_code:
            params.append(pattern_code)
            where.append(f"s.pattern_code = ${len(params)}")

        params.append(limit)
        # Phase 7B: LEFT JOIN provenance so new outcomes can FREEZE the exact
        # signal version they evaluate. Legacy signals have no provenance row
        # -> these fields stay None and the outcome keeps NULL provenance.
        query = f"""
            SELECT s.id, s.symbol, s.pattern_code, s.snapshot_date,
                   s.created_at, s.details,
                   sp.scan_run_id, sp.strategy_code, sp.strategy_version,
                   sp.decision_policy_version, sp.config_hash,
                   sp.provenance_version
            FROM signals s
            LEFT JOIN signal_outcomes o ON o.signal_id = s.id
            LEFT JOIN signal_provenance sp ON sp.signal_id = s.id
            WHERE {' AND '.join(where)}
            ORDER BY s.snapshot_date ASC
            LIMIT ${len(params)}
        """
        rows = await conn.fetch(query, *params)
        result = []
        for r in rows:
            details = r["details"]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except (ValueError, TypeError):
                    details = {}
            result.append(
                {
                    "signal_id": str(r["id"]),
                    "symbol": r["symbol"],
                    "pattern_code": r["pattern_code"],
                    "snapshot_date": r["snapshot_date"],
                    "created_at": r["created_at"],
                    "details": details or {},
                    "provenance": {
                        "scan_run_id": str(r["scan_run_id"]) if r["scan_run_id"] else None,
                        "strategy_code": r["strategy_code"],
                        "strategy_version": r["strategy_version"],
                        "decision_policy_version": r["decision_policy_version"],
                        "config_hash": r["config_hash"],
                        "provenance_version": r["provenance_version"],
                    },
                }
            )
        return result
    except Exception as exc:
        logger.error("Failed to load signals needing outcomes: %s", exc)
        return []
    finally:
        await release_db_connection(conn)


async def upsert_signal_outcome(outcome: Dict[str, Any]) -> str:
    """Insert or update the outcome row for a signal (unique per signal_id).

    Expects `outcome` keys:
      signal_id, symbol, pattern_code, side, signal_timestamp,
      entry_price, stop_price, target_price, invalidation,
      ret_by_window (dict {days: pct|None}),
      benchmark_returns (dict|None), same_ticker_buy_hold (dict|None),
      max_favorable_excursion, max_adverse_excursion,
      hit_stop, hit_target, simulated_r,
      outcome_status, calculation_version
    """
    conn = await get_db_connection()
    try:
        ret = outcome.get("ret_by_window") or {}
        now = datetime.utcnow()
        row_id = uuid.uuid4()

        # Phase 7B: outcome provenance columns freeze the signal version being
        # evaluated (NULL for legacy signals without provenance — never faked).
        scan_run_id = outcome.get("scan_run_id")
        query = """
            INSERT INTO signal_outcomes (
                id, signal_id, symbol, pattern_code, side, signal_timestamp,
                entry_price, stop_price, target_price, invalidation,
                ret_1d, ret_3d, ret_5d, ret_10d, ret_20d,
                benchmark_returns, same_ticker_buy_hold,
                max_favorable_excursion, max_adverse_excursion,
                hit_stop, hit_target, simulated_r,
                outcome_status, calculation_version, created_at, updated_at,
                scan_run_id, strategy_code, strategy_version,
                decision_policy_version, config_hash, provenance_version
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12, $13, $14, $15,
                $16, $17,
                $18, $19,
                $20, $21, $22,
                $23, $24, $25, $26,
                $27, $28, $29, $30, $31, $32
            )
            ON CONFLICT (signal_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                pattern_code = EXCLUDED.pattern_code,
                side = EXCLUDED.side,
                signal_timestamp = EXCLUDED.signal_timestamp,
                entry_price = EXCLUDED.entry_price,
                stop_price = EXCLUDED.stop_price,
                target_price = EXCLUDED.target_price,
                invalidation = EXCLUDED.invalidation,
                ret_1d = EXCLUDED.ret_1d,
                ret_3d = EXCLUDED.ret_3d,
                ret_5d = EXCLUDED.ret_5d,
                ret_10d = EXCLUDED.ret_10d,
                ret_20d = EXCLUDED.ret_20d,
                benchmark_returns = EXCLUDED.benchmark_returns,
                same_ticker_buy_hold = EXCLUDED.same_ticker_buy_hold,
                max_favorable_excursion = EXCLUDED.max_favorable_excursion,
                max_adverse_excursion = EXCLUDED.max_adverse_excursion,
                hit_stop = EXCLUDED.hit_stop,
                hit_target = EXCLUDED.hit_target,
                simulated_r = EXCLUDED.simulated_r,
                outcome_status = EXCLUDED.outcome_status,
                calculation_version = EXCLUDED.calculation_version,
                updated_at = EXCLUDED.updated_at,
                scan_run_id = EXCLUDED.scan_run_id,
                strategy_code = EXCLUDED.strategy_code,
                strategy_version = EXCLUDED.strategy_version,
                decision_policy_version = EXCLUDED.decision_policy_version,
                config_hash = EXCLUDED.config_hash,
                provenance_version = EXCLUDED.provenance_version
            RETURNING id
        """

        benchmark_json = (
            json.dumps(outcome["benchmark_returns"])
            if outcome.get("benchmark_returns") is not None
            else None
        )
        same_ticker_json = (
            json.dumps(outcome["same_ticker_buy_hold"])
            if outcome.get("same_ticker_buy_hold") is not None
            else None
        )

        result = await conn.fetchrow(
            query,
            row_id,
            uuid.UUID(str(outcome["signal_id"])),
            outcome["symbol"],
            outcome.get("pattern_code"),
            outcome.get("side", "LONG"),
            outcome["signal_timestamp"],
            outcome.get("entry_price"),
            outcome.get("stop_price"),
            outcome.get("target_price"),
            outcome.get("invalidation"),
            ret.get(1),
            ret.get(3),
            ret.get(5),
            ret.get(10),
            ret.get(20),
            benchmark_json,
            same_ticker_json,
            outcome.get("max_favorable_excursion"),
            outcome.get("max_adverse_excursion"),
            outcome.get("hit_stop"),
            outcome.get("hit_target"),
            outcome.get("simulated_r"),
            outcome.get("outcome_status", "pending"),
            outcome.get("calculation_version", "outcome.v1"),
            now,
            now,
            uuid.UUID(str(scan_run_id)) if scan_run_id else None,
            outcome.get("strategy_code"),
            outcome.get("strategy_version"),
            outcome.get("decision_policy_version"),
            outcome.get("config_hash"),
            outcome.get("provenance_version"),
        )
        return str(result["id"])
    except Exception as exc:
        logger.error(
            "Failed to upsert outcome for signal %s: %s",
            outcome.get("signal_id"),
            exc,
        )
        raise
    finally:
        await release_db_connection(conn)


def _row_to_dict(row) -> Dict[str, Any]:
    """Map a DB row into a plain outcome dict (API + metrics shape)."""
    def _num(v):
        return float(v) if v is not None else None

    benchmark = row["benchmark_returns"]
    same_ticker = row["same_ticker_buy_hold"]
    if isinstance(benchmark, str):
        benchmark = json.loads(benchmark)
    if isinstance(same_ticker, str):
        same_ticker = json.loads(same_ticker)

    ret_by_window = {
        1: _num(row["ret_1d"]),
        3: _num(row["ret_3d"]),
        5: _num(row["ret_5d"]),
        10: _num(row["ret_10d"]),
        20: _num(row["ret_20d"]),
    }
    ret_labeled = {window_label(w): ret_by_window[w] for w in HOLDING_WINDOWS}

    return {
        "id": str(row["id"]),
        "signal_id": str(row["signal_id"]),
        "symbol": row["symbol"],
        "pattern_code": row["pattern_code"],
        "side": row["side"],
        "signal_timestamp": row["signal_timestamp"],
        "entry_price": _num(row["entry_price"]),
        "stop_price": _num(row["stop_price"]),
        "target_price": _num(row["target_price"]),
        "invalidation": _num(row["invalidation"]),
        "ret_by_window": ret_by_window,
        "returns": ret_labeled,
        "benchmark_returns": benchmark,
        "same_ticker_buy_hold": same_ticker,
        "mfe": _num(row["max_favorable_excursion"]),
        "mae": _num(row["max_adverse_excursion"]),
        "max_favorable_excursion": _num(row["max_favorable_excursion"]),
        "max_adverse_excursion": _num(row["max_adverse_excursion"]),
        "hit_stop": row["hit_stop"],
        "hit_target": row["hit_target"],
        "simulated_r": _num(row["simulated_r"]),
        "outcome_status": row["outcome_status"],
        "calculation_version": row["calculation_version"],
        # Phase 7B provenance columns (NULL for legacy outcomes — never faked).
        "scan_run_id": str(row["scan_run_id"]) if row["scan_run_id"] else None,
        "strategy_code": row["strategy_code"],
        "strategy_version": row["strategy_version"],
        "decision_policy_version": row["decision_policy_version"],
        "config_hash": row["config_hash"],
        "provenance_version": row["provenance_version"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def fetch_outcomes(
    pattern_code: Optional[str] = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch outcome rows with optional filters (for the read API + metrics)."""
    conn = await get_db_connection()
    try:
        where: List[str] = []
        params: List[Any] = []
        for col, val in (
            ("pattern_code", pattern_code),
            ("symbol", symbol),
            ("side", side),
            ("outcome_status", status),
        ):
            if val is not None:
                params.append(val)
                where.append(f"{col} = ${len(params)}")

        params.append(limit)
        query = f"""
            SELECT id, signal_id, symbol, pattern_code, side, signal_timestamp,
                   entry_price, stop_price, target_price, invalidation,
                   ret_1d, ret_3d, ret_5d, ret_10d, ret_20d,
                   benchmark_returns, same_ticker_buy_hold,
                   max_favorable_excursion, max_adverse_excursion,
                   hit_stop, hit_target, simulated_r,
                   outcome_status, calculation_version, created_at, updated_at,
                   scan_run_id, strategy_code, strategy_version,
                   decision_policy_version, config_hash, provenance_version
            FROM signal_outcomes
            {('WHERE ' + ' AND '.join(where)) if where else ''}
            ORDER BY signal_timestamp DESC
            LIMIT ${len(params)}
        """
        rows = await conn.fetch(query, *params)
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        logger.error("Failed to fetch outcomes: %s", exc)
        return []
    finally:
        await release_db_connection(conn)
