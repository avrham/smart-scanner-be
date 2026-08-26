"""Smart Scanner product API — the SMALLEST coherent UI-facing surface.

Read-only. Translates the existing prospective-campaign / shadow-evaluation
persistence (campaign = a strategy_shadow_runs row with a telemetry.campaign
block; per-symbol result = strategy_shadow_pairs + its two
strategy_shadow_evaluations rows) into a stable contract a frontend can build
against directly, without joining internal tables, understanding queue jobs,
occurrence IDs, or interpreting raw internal enum combinations.

Every route uses an exact static path (query params carry variable input, no
path params) so each one can be listed verbatim in app.audit_mode's
AUDIT_ONLY_ALLOWLIST for the read-only isolated staging app. Only reads the 5
relations the SELECT-only smart_scanner_product_reader role is granted on
(strategy_shadow_runs/run_pairs/pairs/evaluations, daily_bars) plus the pure
market-calendar resolver — see ops/sql/create_smart_scanner_product_reader.sql.
Decision-support only: never exposes allow_enter=true semantics,
never reinterprets pair-level outcomes as candidate/control-specific returns.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import get_db
import app.prospective_campaign as pc
from app.prospective_session import resolve_latest_completed_session
import app.scanner_view as sv

logger = logging.getLogger(__name__)

router = APIRouter()

_RECENT_BARS_LIMIT = 30


def _as_json(value: Any) -> Any:
    """asyncpg may return JSONB as str depending on codec config."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return value


# The session date is written at the telemetry ROOT by the shadow runner
# (app/workers/shadow/runner.py sets telemetry["as_of_date"]); the
# `telemetry.campaign` block is operator metadata merged in on top of it. The
# durable-worker path therefore never nests as_of_date under `campaign`, so the
# root value is the canonical source and the nested one is only a fallback for
# any writer that supplies it explicitly.
def _as_of_date_sql(alias: str = "") -> str:
    col = f"{alias}." if alias else ""
    return (
        f"COALESCE({col}telemetry->'campaign'->>'as_of_date', "
        f"{col}telemetry->>'as_of_date')"
    )


_AS_OF_DATE_SQL = _as_of_date_sql()


async def _fetch_latest_campaign(db: asyncpg.Connection, *, session: Optional[str]) -> Optional[Dict[str, Any]]:
    """Latest strategy_shadow_runs row carrying a telemetry.campaign block —
    optionally pinned to one `session` (as_of_date). Only the granted
    top-level + telemetry columns are read; never a DSN/role/secret."""
    where = "telemetry->'campaign' IS NOT NULL"
    params: List[Any] = []
    if session:
        params.append(session)
        where += f" AND {_AS_OF_DATE_SQL} = ${len(params)}"
    row = await db.fetchrow(
        f"""
        SELECT id, experiment_code, experiment_version, status, started_at,
               finished_at, error_code,
               telemetry->'campaign'->>'campaign_id' AS campaign_id,
               {_AS_OF_DATE_SQL} AS as_of_date,
               requested_symbols
        FROM strategy_shadow_runs
        WHERE {where}
        ORDER BY started_at DESC LIMIT 1
        """,
        *params,
    )
    if row is None:
        return None
    d = dict(row)
    d["requested_symbols"] = _as_json(d["requested_symbols"]) or []
    return d


async def _fetch_scan_universe(db: asyncpg.Connection, run_id: Any) -> List[str]:
    """The symbols this scan actually covered, taken from its persisted pairs.

    `strategy_shadow_runs.requested_symbols` is NOT a reliable universe: the
    durable worker evaluates one symbol per job against a single shared
    campaign run, so each job rewrites that column with its own single symbol
    and the last writer wins. The run's `strategy_shadow_run_pairs` rows are
    the authoritative membership, and they are exactly what `results` is built
    from — so deriving the universe here keeps the two consistent by
    construction.
    """
    rows = await db.fetch(
        """
        SELECT DISTINCT p.symbol
        FROM strategy_shadow_run_pairs rp
        JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
        WHERE rp.run_id = $1
        ORDER BY p.symbol
        """,
        run_id,
    )
    return [r["symbol"] for r in rows]


async def _resolve_universe(db: asyncpg.Connection, campaign: Dict[str, Any]) -> List[str]:
    """Persisted pair membership, falling back to the requested list only when
    the run has no pairs yet (a still-running or failed scan)."""
    symbols = await _fetch_scan_universe(db, campaign["id"])
    return symbols or list(campaign["requested_symbols"])


async def _fetch_campaign_results(db: asyncpg.Connection, run_id: Any) -> List[Dict[str, Any]]:
    """One bounded, single-query, batched join for every symbol in this
    campaign — never N+1 per symbol. candidate_details is only used
    internally to derive presentation fields; it is never returned verbatim
    on this (list) endpoint."""
    rows = await db.fetch(
        """
        SELECT p.symbol,
               x.verdict AS candidate_verdict, x.score AS candidate_score,
               x.details_snapshot AS candidate_details,
               c.verdict AS control_verdict, c.score AS control_score
        FROM strategy_shadow_run_pairs rp
        JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
        LEFT JOIN strategy_shadow_evaluations x
               ON x.pair_id = p.id AND x.arm_code = $2
        LEFT JOIN strategy_shadow_evaluations c
               ON c.pair_id = p.id AND c.arm_code = $3
        WHERE rp.run_id = $1
        ORDER BY p.symbol
        """,
        run_id, pc.CANDIDATE_ARM_CODE, pc.CONTROL_ARM_CODE,
    )
    out = []
    for r in rows:
        d = dict(r)
        d["candidate_details"] = _as_json(d["candidate_details"])
        out.append(d)
    return out


async def _fetch_data_freshness(db: asyncpg.Connection, symbols: List[str]) -> Dict[str, Any]:
    if not symbols:
        return {"oldest_bar_date": None, "latest_bar_date": None}
    row = await db.fetchrow(
        "SELECT MIN(trading_date) AS oldest, MAX(trading_date) AS latest "
        "FROM daily_bars WHERE symbol = ANY($1::text[])",
        symbols,
    )
    return {
        "oldest_bar_date": row["oldest"].isoformat() if row and row["oldest"] else None,
        "latest_bar_date": row["latest"].isoformat() if row and row["latest"] else None,
    }


@router.get("/scanner/overview")
async def scanner_overview(
    session: Optional[str] = Query(None, description="ISO session date (YYYY-MM-DD); defaults to the latest campaign"),
    db: asyncpg.Connection = Depends(get_db),
):
    """Main scanner screen: latest scan identity/state, universe, per-symbol
    result rows, and a state summary — everything item A of the product
    contract needs in one response."""
    now = datetime.now(timezone.utc)
    latest_completed_session = str(resolve_latest_completed_session(now))

    campaign = await _fetch_latest_campaign(db, session=session)
    scanner_state = sv.classify_scanner_state(
        campaign_status=campaign["status"] if campaign else None,
        campaign_as_of_date=campaign["as_of_date"] if campaign else None,
        latest_completed_session=latest_completed_session,
    )

    results: List[Dict[str, Any]] = []
    freshness = {"oldest_bar_date": None, "latest_bar_date": None}
    universe_symbols: List[str] = []
    if campaign is not None:
        raw_results = await _fetch_campaign_results(db, campaign["id"])
        results = [sv.build_overview_row(r, scanner_state=scanner_state) for r in raw_results]
        universe_symbols = await _resolve_universe(db, campaign)
        freshness = await _fetch_data_freshness(db, universe_symbols)

    return {
        "contract_version": sv.OVERVIEW_CONTRACT_VERSION,
        "generated_at": now.isoformat(),
        "scanner_state": scanner_state,
        "latest_completed_market_session": latest_completed_session,
        "scan": (
            {
                "scan_id": str(campaign["id"]),
                "campaign_id": campaign["campaign_id"],
                "session_date": campaign["as_of_date"],
                "status": campaign["status"],
                "started_at": campaign["started_at"].isoformat() if campaign["started_at"] else None,
                "finished_at": campaign["finished_at"].isoformat() if campaign["finished_at"] else None,
                "experiment_code": campaign["experiment_code"],
            }
            if campaign is not None else None
        ),
        "universe": (
            {
                "symbol_count": len(universe_symbols),
                "symbols": universe_symbols,
            }
            if campaign is not None else None
        ),
        "data_freshness": freshness,
        "results_summary": sv.summarize_results(results),
        "results": results,
        "strategy": {
            "candidate_strategy_code": pc.CANDIDATE_STRATEGY_CODE,
            "candidate_strategy_version": pc.CANDIDATE_STRATEGY_VERSION,
            "control_strategy_code": pc.CONTROL_STRATEGY_CODE,
            "control_strategy_version": pc.CONTROL_STRATEGY_VERSION,
            "allow_enter": pc.CANDIDATE_ALLOW_ENTER,
        },
    }


@router.get("/scanner/symbol")
async def scanner_symbol_detail(
    symbol: str = Query(..., description="Ticker symbol, e.g. AAPL"),
    session: Optional[str] = Query(None, description="ISO session date (YYYY-MM-DD); defaults to the latest campaign"),
    db: asyncpg.Connection = Depends(get_db),
):
    """Symbol detail screen: candidate result + evidence, control comparison,
    live readiness, and bounded recent bar context for one symbol in the
    selected (or latest) scan."""
    symbol = symbol.strip().upper()
    now = datetime.now(timezone.utc)
    latest_completed_session = str(resolve_latest_completed_session(now))

    campaign = await _fetch_latest_campaign(db, session=session)
    if campaign is None:
        raise HTTPException(status_code=404, detail={"error": "no_campaign_available"})
    if symbol not in await _resolve_universe(db, campaign):
        raise HTTPException(status_code=404, detail={"error": "unknown_symbol", "symbol": symbol})

    scanner_state = sv.classify_scanner_state(
        campaign_status=campaign["status"], campaign_as_of_date=campaign["as_of_date"],
        latest_completed_session=latest_completed_session,
    )

    row = await db.fetchrow(
        """
        SELECT x.verdict AS candidate_verdict, x.score AS candidate_score,
               x.reason AS candidate_reason, x.details_snapshot AS candidate_details,
               c.verdict AS control_verdict, c.score AS control_score,
               c.reason AS control_reason
        FROM strategy_shadow_run_pairs rp
        JOIN strategy_shadow_pairs p ON p.id = rp.pair_id
        LEFT JOIN strategy_shadow_evaluations x
               ON x.pair_id = p.id AND x.arm_code = $3
        LEFT JOIN strategy_shadow_evaluations c
               ON c.pair_id = p.id AND c.arm_code = $4
        WHERE rp.run_id = $1 AND p.symbol = $2
        """,
        campaign["id"], symbol, pc.CANDIDATE_ARM_CODE, pc.CONTROL_ARM_CODE,
    )
    if row is None:
        # Symbol is in the frozen universe but has no persisted pair for this
        # run (e.g. a still-running or partially-failed campaign).
        has_candidate = False
        candidate_details = None
        candidate_verdict = candidate_score = candidate_reason = None
        control_verdict = control_score = control_reason = None
    else:
        candidate_details = _as_json(row["candidate_details"])
        has_candidate = row["candidate_verdict"] is not None
        candidate_verdict, candidate_score, candidate_reason = (
            row["candidate_verdict"], row["candidate_score"], row["candidate_reason"])
        control_verdict, control_score, control_reason = (
            row["control_verdict"], row["control_score"], row["control_reason"])

    symbol_state = sv.classify_symbol_state(
        scanner_state=scanner_state, has_candidate_result=has_candidate,
        candidate_verdict=candidate_verdict,
    )

    daily = await db.fetchrow(
        "SELECT COUNT(*)::int AS n, MIN(trading_date) AS oldest, MAX(trading_date) AS latest "
        "FROM daily_bars WHERE symbol = $1",
        symbol,
    )
    bars = await db.fetch(
        "SELECT trading_date, open, high, low, close, volume FROM daily_bars "
        "WHERE symbol = $1 ORDER BY trading_date DESC LIMIT $2",
        symbol, _RECENT_BARS_LIMIT,
    )
    recent_bars = [
        {
            "date": b["trading_date"].isoformat(),
            "open": float(b["open"]), "high": float(b["high"]),
            "low": float(b["low"]), "close": float(b["close"]),
            "volume": float(b["volume"]),
        }
        for b in reversed(bars)
    ]

    return {
        "contract_version": sv.SYMBOL_DETAIL_CONTRACT_VERSION,
        "symbol": symbol,
        "symbol_state": symbol_state,
        "scan": {
            "scan_id": str(campaign["id"]),
            "campaign_id": campaign["campaign_id"],
            "session_date": campaign["as_of_date"],
            "status": campaign["status"],
        },
        "candidate": {
            "strategy_code": pc.CANDIDATE_STRATEGY_CODE,
            "strategy_version": pc.CANDIDATE_STRATEGY_VERSION,
            "verdict": candidate_verdict,
            "score": candidate_score,
            "reason": candidate_reason,
            "allow_enter": pc.CANDIDATE_ALLOW_ENTER,
            "evidence": sv.build_symbol_evidence(candidate_details),
        },
        "control": {
            "strategy_code": pc.CONTROL_STRATEGY_CODE,
            "strategy_version": pc.CONTROL_STRATEGY_VERSION,
            "verdict": control_verdict,
            "score": control_score,
            "reason": control_reason,
        },
        "readiness": {
            "daily_bar_count": daily["n"] if daily else 0,
            "oldest_daily_bar": daily["oldest"].isoformat() if daily and daily["oldest"] else None,
            "latest_daily_bar": daily["latest"].isoformat() if daily and daily["latest"] else None,
        },
        "recent_daily_bars": recent_bars,
    }


@router.get("/scanner/scans")
async def scanner_scans(
    limit: int = Query(20, ge=1, le=100),
    db: asyncpg.Connection = Depends(get_db),
):
    """Lightweight scan history — enough for a UI to let the user pick a
    previous completed scan; no analytics, no outcome/return data."""
    rows = await db.fetch(
        f"""
        SELECT r.id, r.status, r.started_at, r.finished_at,
               {_as_of_date_sql('r')} AS as_of_date,
               (SELECT count(*) FROM strategy_shadow_run_pairs rp
                 WHERE rp.run_id = r.id) AS pair_count
        FROM strategy_shadow_runs r
        WHERE r.telemetry->'campaign' IS NOT NULL
        ORDER BY r.started_at DESC LIMIT $1
        """,
        limit,
    )
    return {
        "contract_version": sv.SCAN_LIST_CONTRACT_VERSION,
        "scans": [
            {
                "scan_id": str(r["id"]),
                "session_date": r["as_of_date"],
                "status": r["status"],
                "pair_count": _as_json(r["pair_count"]),
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
            }
            for r in rows
        ],
    }
