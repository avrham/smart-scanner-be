"""Smart Scanner product API — the SMALLEST coherent UI-facing surface.

Read-only. Translates the existing prospective-campaign / shadow-evaluation
persistence (campaign = a strategy_shadow_runs row with a telemetry.campaign
block; per-symbol result = strategy_shadow_pairs + its two
strategy_shadow_evaluations rows) into a stable contract a frontend can build
against directly, without joining internal tables, understanding queue jobs,
occurrence IDs, or interpreting raw internal enum combinations.

Every route uses an exact static path (query params carry variable input, no
path params) so each one can be listed verbatim in app.audit_mode's
AUDIT_ONLY_ALLOWLIST for the read-only isolated staging app. Only reads the 13
relations the SELECT-only smart_scanner_product_reader role is granted on
(strategy_shadow_runs/run_pairs/pairs/evaluations, daily_bars,
symbol_catalyst_events, catalyst_source_state, company_news_articles,
company_news_symbols, sec_filings, sec_filing_symbols, external_signals,
external_signal_sources) plus the pure market-calendar resolver — see
ops/sql/create_smart_scanner_product_reader.sql.
Decision-support only: never exposes allow_enter=true semantics,
never reinterprets pair-level outcomes as candidate/control-specific returns.

External intelligence (022) is read here but WRITTEN somewhere else entirely:
the webhook gateway is a separate app with a separate role, because this one's
sessions are default_transaction_read_only and its route gate is GET-only. The
product surface reads normalised signals and the registry; it is deliberately
NOT granted `external_signal_deliveries`, which holds raw third-party payloads
and is operator data rather than product data.
"""

from __future__ import annotations

import json
import logging
from datetime import date as date_type, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import get_db
import app.prospective_campaign as pc
from app.prospective_session import resolve_latest_completed_session
import app.catalyst as cat
import app.catalyst_ingest as ci
import app.external_signals as ex
import app.market_context as mc
import app.news as nw
import app.news_ingest as ni
import app.reference_market as rm
import app.scanner_view as sv
import app.sec_events as se
import app.sec_ingest as si

logger = logging.getLogger(__name__)

router = APIRouter()

_RECENT_BARS_LIMIT = 30

# Longest context lookback (60D return needs 61 bars) plus a small margin so a
# holiday-shortened window still resolves. Bounded on purpose: one query for the
# whole universe, never per symbol.
_CONTEXT_BARS_LIMIT = mc.RS_HORIZONS[-1] + 5


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


def _parse_session(value: Optional[str]):
    """Campaign sessions arrive from telemetry as text."""
    from datetime import date as _date
    if not value:
        return None
    try:
        return _date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


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


async def _fetch_reference_bars(
    db: asyncpg.Connection, session: Optional[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Daily bars for the reference-market symbols, truncated at the same scan
    session as the candidates.

    ONE bounded query for all reference symbols — never per symbol, and never
    per candidate row. Reference symbols are read here purely as context; they
    are not part of the scanned universe and never enter `results`.
    """
    return await _fetch_universe_bars(db, rm.reference_symbols(), session)


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


async def _fetch_universe_bars(
    db: asyncpg.Connection, symbols: List[str], session: Optional[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Ascending daily bars per symbol, truncated AT the scan session.

    Truncating at `session` is what makes the context as-of-correct: a scan from
    an earlier session must be described by the market as it was then, never by
    bars that arrived afterwards. Falls back to the latest stored bars only when
    the run carries no session date at all.

    One bounded window-function query for the whole universe — never N+1.
    """
    if not symbols:
        return {}
    rows = await db.fetch(
        """
        SELECT symbol, trading_date, close, volume
        FROM (
            SELECT symbol, trading_date, close, volume,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol ORDER BY trading_date DESC
                   ) AS rn
            FROM daily_bars
            WHERE symbol = ANY($1::text[])
              AND ($2::text IS NULL OR trading_date <= $2::text::date)
        ) t
        WHERE rn <= $3
        ORDER BY symbol, trading_date
        """,
        symbols, session, _CONTEXT_BARS_LIMIT,
    )
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    for r in rows:
        out.setdefault(r["symbol"], []).append(
            {"close": float(r["close"]), "volume": float(r["volume"] or 0.0)}
        )
    return out

async def _fetch_catalyst_events(
    db: asyncpg.Connection, symbols: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Catalyst events for the whole universe in ONE bounded query.

    Never per symbol and never per HTTP request to a provider — the Product API
    reads persisted context only and holds no provider credential.
    """
    if not symbols:
        return {}
    rows = await db.fetch(
        """
        SELECT symbol, event_type, event_date, session_timing, certainty,
               fiscal_period, fiscal_year, source, source_reference, observed_at
        FROM symbol_catalyst_events
        WHERE symbol = ANY($1::text[])
        ORDER BY symbol, event_date DESC
        """,
        symbols,
    )
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    for r in rows:
        out.setdefault(r["symbol"], []).append(dict(r))
    return out


async def _fetch_catalyst_freshness(db: asyncpg.Connection) -> Dict[str, Dict[str, Any]]:
    """One row per source — what lets the product distinguish 'nothing
    scheduled' from 'we cannot see the schedule'."""
    rows = await db.fetch(
        "SELECT source, status, last_refresh_at, last_success_at, "
        "symbols_covered, events_upserted, detail FROM catalyst_source_state"
    )
    return {r["source"]: dict(r) for r in rows}


async def _load_catalysts(
    db: asyncpg.Connection, symbols: List[str], now: datetime
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any], Dict[str, Any]]:
    """Batch-load catalyst events + per-source freshness.

    Wrapped by the caller so that a catalyst failure degrades this dimension
    ONLY — the scanner, attention and market context must keep working.
    """
    events = await _fetch_catalyst_events(db, symbols)
    state = await _fetch_catalyst_freshness(db)
    earnings_fresh = cat.evaluate_freshness(
        state.get(ci.SOURCE_EARNINGS_CALENDAR), now=now)
    filings_fresh = cat.evaluate_freshness(
        state.get(ci.SOURCE_FINANCIAL_FILINGS), now=now)
    return events, earnings_fresh, filings_fresh


#: Calendar days of articles to load around a session. Deliberately WIDER than
#: the product window (7 trading sessions) so the SQL bound stays a pure
#: efficiency guard and `app.news` remains the only thing deciding what a
#: session may see.
_NEWS_QUERY_SPAN_DAYS = 16

NEWS_SQL = """
SELECT s.symbol, s.relevance, a.published_at, a.title, a.title_normalized,
       a.publisher, a.article_url, a.category, a.category_source,
       a.scope, a.ticker_breadth
FROM public.company_news_symbols s
JOIN public.company_news_articles a ON a.id = s.article_id
WHERE s.symbol = ANY($1::text[])
  AND a.published_at >= $2
  AND a.published_at <= $3
ORDER BY s.symbol, a.published_at DESC
"""


async def _fetch_news_articles(
    db: asyncpg.Connection, symbols: List[str], session_date: Optional[date_type]
) -> Dict[str, List[Dict[str, Any]]]:
    """Company news for the whole universe in ONE bounded query.

    Never per symbol, never paginated per row, and never a provider call — the
    Product API reads persisted context only and holds no provider credential.
    The upper bound is the session close, so the query itself cannot hand back
    an article the session was not allowed to see.
    """
    if not symbols or session_date is None:
        return {}
    upper = nw.session_close_utc(session_date)
    lower = upper - timedelta(days=_NEWS_QUERY_SPAN_DAYS)
    rows = await db.fetch(NEWS_SQL, symbols, lower, upper)
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    for r in rows:
        out.setdefault(r["symbol"], []).append(dict(r))
    return out


async def _load_news(
    db: asyncpg.Connection, symbols: List[str],
    session_date: Optional[date_type], now: datetime,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Batch-load news articles + the news source's freshness.

    Wrapped by the caller so that a news failure degrades this dimension ONLY —
    the scanner, attention, market context and earnings must keep working.
    """
    articles = await _fetch_news_articles(db, symbols, session_date)
    state = await _fetch_catalyst_freshness(db)
    freshness = nw.evaluate_freshness(state.get(ni.SOURCE_COMPANY_NEWS), now=now)
    return articles, freshness



#: Calendar days of filings to load around a session. Deliberately WIDER than
#: the product window (20 trading sessions) so the SQL bound stays a pure
#: efficiency guard and `app.sec_events` remains the only thing deciding what a
#: session may see.
_SEC_QUERY_SPAN_DAYS = 45

SEC_SQL = """
SELECT s.symbol, f.accession_number, f.cik, f.form, f.accepted_at,
       f.filing_date, f.period_of_report, f.item_codes, f.event_types,
       f.taxonomy_version, f.is_primary_event, f.amends_accession_number,
       f.filing_url
FROM public.sec_filing_symbols s
JOIN public.sec_filings f ON f.id = s.filing_id
WHERE s.symbol = ANY($1::text[])
  AND f.accepted_at >= $2
  AND f.accepted_at <= $3
ORDER BY s.symbol, f.accepted_at DESC
"""


async def _fetch_sec_filings(
    db: asyncpg.Connection, symbols: List[str], session_date: Optional[date_type]
) -> Dict[str, List[Dict[str, Any]]]:
    """SEC filings for the whole universe in ONE bounded query.

    Never per symbol and never a request to the SEC — the Product API reads
    persisted context only. The upper bound is the session close, so the query
    itself cannot hand back a filing the session was not allowed to see.
    """
    if not symbols or session_date is None:
        return {}
    upper = se.session_close_utc(session_date)
    lower = upper - timedelta(days=_SEC_QUERY_SPAN_DAYS)
    rows = await db.fetch(SEC_SQL, symbols, lower, upper)
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    for r in rows:
        out.setdefault(r["symbol"], []).append(dict(r))
    return out


async def _load_sec(
    db: asyncpg.Connection, symbols: List[str],
    session_date: Optional[date_type], now: datetime,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Batch-load SEC filings + the SEC source's freshness.

    Wrapped by the caller so that a SEC failure degrades this dimension ONLY —
    the scanner, attention, market context, earnings and news must keep working.
    """
    filings = await _fetch_sec_filings(db, symbols, session_date)
    state = await _fetch_catalyst_freshness(db)
    freshness = se.evaluate_freshness(state.get(si.SOURCE_SEC_EDGAR), now=now)
    return filings, freshness


_EXTERNAL_QUERY_SPAN_DAYS = 45

EXTERNAL_SIGNALS_SQL = """
SELECT id, source, symbol, source_signal_id,
       observed_at, received_at, effective_at, clock_skew_seconds,
       timeframe, timeframe_normalized,
       signal_type, signal_type_normalized,
       direction, direction_normalized,
       confidence, confidence_scale,
       indicator, indicator_version, alert_id,
       contract_version, source_payload_version, supersedes_signal_id
FROM public.external_signals
WHERE symbol = ANY($1::text[])
  AND symbol_scope = 'scanner_universe'
  AND effective_at >= $2
  AND effective_at <= $3
ORDER BY symbol, effective_at DESC
"""

EXTERNAL_SOURCES_SQL = """
SELECT source, display_name, transports, supports_realtime, supports_historical,
       supports_symbol_scan, supports_signal_events, emits_signals,
       requires_paid_plan, status, notes
FROM public.external_signal_sources
ORDER BY status <> 'live', source
"""


async def _fetch_external_signals(
    db: asyncpg.Connection, symbols: List[str], session_date: Optional[date_type]
) -> Dict[str, List[Dict[str, Any]]]:
    """External signals for the whole universe in ONE bounded query.

    Never per symbol — 25 symbols must cost one round trip, not 25. The upper
    bound is the session close, so the query itself cannot hand back a signal
    the session was not allowed to see, and `symbol_scope` keeps
    research-only discoveries out of the product surface entirely.
    """
    if not symbols or session_date is None:
        return {}
    upper = ex.session_close_utc(session_date)
    lower = upper - timedelta(days=_EXTERNAL_QUERY_SPAN_DAYS)
    rows = await db.fetch(EXTERNAL_SIGNALS_SQL, symbols, lower, upper)
    out: Dict[str, List[Dict[str, Any]]] = {s: [] for s in symbols}
    for r in rows:
        out.setdefault(r["symbol"], []).append(dict(r))
    return out


async def _load_external(
    db: asyncpg.Connection, symbols: List[str],
    session_date: Optional[date_type], now: datetime,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]], Dict[str, Any]]:
    """Batch-load external signals, the source registry, and freshness.

    Wrapped by the caller so an external-intelligence failure degrades this
    dimension ONLY — the scanner, attention, market context, earnings, news
    and SEC must all keep working when a third party goes quiet or the
    registry is unreadable. That is the whole point of a hub that sits beside
    the strategy rather than inside it.
    """
    signals = await _fetch_external_signals(db, symbols, session_date)
    source_rows = [dict(r) for r in await db.fetch(EXTERNAL_SOURCES_SQL)]
    state = await _fetch_catalyst_freshness(db)
    per_source = {
        row["source"]: ex.evaluate_freshness(
            state.get(ex.source_state_key(row["source"])), now=now,
            registry_status=row.get("status"))
        for row in source_rows
        if row.get("source") in ex.WEBHOOK_SOURCES
    }
    return signals, source_rows, ex.combine_freshness(per_source)


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
    universe_breadth: Dict[str, Any] = mc.build_universe_breadth({})
    market_regime: Dict[str, Any] = mc.build_market_regime({})
    catalyst_sources: Dict[str, Any] = {
        "earnings": {"status": cat.STATUS_UNAVAILABLE,
                     "reason": cat.REASON_NEVER_REFRESHED},
        "financial_reports": {"status": cat.STATUS_UNAVAILABLE,
                              "reason": cat.REASON_NEVER_REFRESHED}}
    # External intelligence is a TOP-LEVEL dimension, not a catalyst source:
    # a catalyst is something that happened to the company, an external signal
    # is another system's opinion about the same chart. It therefore gets its
    # own summary rather than a fourth entry in `catalyst_sources`.
    external_intelligence: Dict[str, Any] = {
        "contract_version": ex.EXTERNAL_INTELLIGENCE_CONTRACT_VERSION,
        "status": ex.STATUS_UNAVAILABLE,
        "reason": ex.REASON_NEVER_REFRESHED,
        "external_sources_present": [], "symbols_with_external_signal": 0,
        "recent_signal_count": 0, "agreement_symbol_count": 0,
        "disagreement_symbol_count": 0, "sources": []}

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
        # Server-side "inspect first" ordering, so every client agrees on it.
        results.sort(key=sv.attention_sort_key)
        universe_symbols = await _resolve_universe(db, campaign)
        freshness = await _fetch_data_freshness(db, universe_symbols)
        context_bars = await _fetch_universe_bars(
            db, universe_symbols, campaign["as_of_date"]
        )
        reference_bars = await _fetch_reference_bars(db, campaign["as_of_date"])
        for row in results:
            row["market_context"] = sv.build_row_context(
                row["symbol"], context_bars, reference_bars
            )
        universe_breadth = mc.build_universe_breadth(context_bars)
        market_regime = mc.build_market_regime(
            reference_bars, universe_breadth=universe_breadth
        )

        # Catalyst context is strictly additive: if it cannot be loaded, every
        # catalyst field says `unavailable` and the scan is served regardless.
        session_date = _parse_session(campaign["as_of_date"])
        try:
            events, earnings_fresh, filings_fresh = await _load_catalysts(
                db, universe_symbols, now)
            for row in results:
                ctx = cat.build_catalyst_context(
                    events.get(row["symbol"]) or [],
                    as_of_session=session_date,
                    earnings_freshness=earnings_fresh,
                    filings_freshness=filings_fresh)
                row["catalyst_context"] = cat.build_row_catalyst(ctx)
            catalyst_sources = {
                "earnings": earnings_fresh, "financial_reports": filings_fresh}
        except Exception:
            logger.warning("catalyst context unavailable for overview",
                           exc_info=False)
            empty = cat.empty_catalyst_context()
            for row in results:
                row["catalyst_context"] = cat.build_row_catalyst(empty)
            catalyst_sources = {
                "earnings": {"status": cat.STATUS_UNAVAILABLE,
                             "reason": cat.REASON_SOURCE_UNAVAILABLE},
                "financial_reports": {"status": cat.STATUS_UNAVAILABLE,
                                      "reason": cat.REASON_SOURCE_UNAVAILABLE}}

        # News is a SEPARATE dimension with its own try/except on purpose: a
        # news outage must not cost the product its earnings context, and an
        # earnings outage must not silence the news. Neither can take the
        # scanner down.
        try:
            articles, news_fresh = await _load_news(
                db, universe_symbols, session_date, now)
            for row in results:
                row["catalyst_context"]["news"] = nw.build_row_news(
                    nw.build_news_context(
                        articles.get(row["symbol"]) or [], symbol=row["symbol"],
                        as_of_session=session_date, freshness=news_fresh))
            catalyst_sources["news"] = news_fresh
        except Exception:
            logger.warning("news context unavailable for overview", exc_info=False)
            blank = nw.build_row_news(nw.empty_news_context())
            for row in results:
                row["catalyst_context"]["news"] = dict(blank)
            catalyst_sources["news"] = {
                "status": nw.STATUS_UNAVAILABLE,
                "reason": nw.REASON_SOURCE_UNAVAILABLE}

        # A THIRD independently-wrapped dimension. Earnings, news and SEC each
        # fail alone: a filing outage must not silence a headline, and neither
        # can take the scanner down.
        try:
            filings, sec_fresh = await _load_sec(
                db, universe_symbols, session_date, now)
            for row in results:
                row["catalyst_context"]["sec_events"] = se.build_row_sec(
                    se.build_sec_context(
                        filings.get(row["symbol"]) or [],
                        as_of_session=session_date, freshness=sec_fresh))
            catalyst_sources["sec_events"] = sec_fresh
        except Exception:
            logger.warning("sec context unavailable for overview", exc_info=False)
            blank_sec = se.build_row_sec(se.empty_sec_context())
            for row in results:
                row["catalyst_context"]["sec_events"] = dict(blank_sec)
            catalyst_sources["sec_events"] = {
                "status": se.STATUS_UNAVAILABLE,
                "reason": se.REASON_SOURCE_UNAVAILABLE}

        # A FOURTH independently-wrapped dimension, and the only one whose
        # data arrives from outside this system. A third party going quiet,
        # a registry that will not read, or an ingress that was never
        # configured must all cost exactly this block and nothing else.
        try:
            signals, source_rows, ext_fresh = await _load_external(
                db, universe_symbols, session_date, now)
            contexts = []
            for row in results:
                ctx = ex.build_external_context(
                    signals.get(row["symbol"]) or [],
                    as_of_session=session_date, sources=source_rows,
                    freshness=ext_fresh, attention=row.get("attention"))
                contexts.append(ctx)
                row["external_intelligence"] = ex.build_row_external(ctx)
            external_intelligence = {
                "contract_version": ex.EXTERNAL_INTELLIGENCE_CONTRACT_VERSION,
                "status": ext_fresh.get("status"),
                "reason": ext_fresh.get("reason"),
                **ex.summarize_sources(contexts),
                "sources": [ex.build_source_entry(r) for r in source_rows],
            }
        except Exception:
            logger.warning("external intelligence unavailable for overview",
                           exc_info=False)
            blank_ext = ex.build_row_external(ex.empty_external_context(
                reason=ex.REASON_SOURCE_UNAVAILABLE))
            for row in results:
                row["external_intelligence"] = dict(blank_ext)
            external_intelligence = {
                "contract_version": ex.EXTERNAL_INTELLIGENCE_CONTRACT_VERSION,
                "status": ex.STATUS_UNAVAILABLE,
                "reason": ex.REASON_SOURCE_UNAVAILABLE,
                "external_sources_present": [],
                "symbols_with_external_signal": 0, "recent_signal_count": 0,
                "agreement_symbol_count": 0, "disagreement_symbol_count": 0,
                "sources": []}

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
        "attention_summary": sv.summarize_attention(results),
        # Breadth of the SCANNED UNIVERSE — explicitly not market breadth.
        "scanner_universe_breadth": universe_breadth,
        # Broad environment from the benchmark. Context only; it changes no verdict.
        "market_regime": market_regime,
        # Per-source catalyst freshness, so the UI never shows stale event dates.
        "catalyst_sources": catalyst_sources,
        # THIRD-PARTY OPINIONS, deliberately outside both market_context and
        # catalyst_context. Compact evidence only — who is talking and about
        # how many symbols; the claims themselves live on the detail screen
        # where their provenance can travel with them.
        "external_intelligence": external_intelligence,
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

    universe_symbols = await _resolve_universe(db, campaign)
    context_bars = await _fetch_universe_bars(
        db, universe_symbols, campaign["as_of_date"]
    )
    reference_bars = await _fetch_reference_bars(db, campaign["as_of_date"])
    session_date = _parse_session(campaign["as_of_date"])
    try:
        events, earnings_fresh, filings_fresh = await _load_catalysts(
            db, [symbol], now)
        catalyst_context = cat.build_catalyst_context(
            events.get(symbol) or [], as_of_session=session_date,
            earnings_freshness=earnings_fresh, filings_freshness=filings_fresh)
    except Exception:
        logger.warning("catalyst context unavailable for symbol detail",
                       exc_info=False)
        catalyst_context = cat.empty_catalyst_context()

    # Independently wrapped — see the overview. `news` is added BESIDE the
    # earnings blocks rather than inside them: an article is not an earnings
    # date, and the two answer different questions.
    try:
        articles, news_fresh = await _load_news(db, [symbol], session_date, now)
        catalyst_context["news"] = nw.build_news_context(
            articles.get(symbol) or [], symbol=symbol,
            as_of_session=session_date, freshness=news_fresh)
    except Exception:
        logger.warning("news context unavailable for symbol detail", exc_info=False)
        catalyst_context["news"] = nw.empty_news_context(symbol=symbol)

    # Independently wrapped — see the overview. `sec_events` is a SIBLING of
    # `news`, never nested inside it: an article is somebody's account of an
    # event, a filing is the registrant's own formal disclosure of one, and
    # merging them would let commentary borrow the authority of a filing.
    try:
        filings, sec_fresh = await _load_sec(db, [symbol], session_date, now)
        catalyst_context["sec_events"] = se.build_sec_context(
            filings.get(symbol) or [], as_of_session=session_date,
            freshness=sec_fresh)
    except Exception:
        logger.warning("sec context unavailable for symbol detail", exc_info=False)
        catalyst_context["sec_events"] = se.empty_sec_context()

    market_context = mc.build_market_context(
        symbol, context_bars,
        as_of_session=campaign["as_of_date"],
        reference_bars=reference_bars,
        universe_breadth=mc.build_universe_breadth(context_bars),
    )

    # External intelligence is built AFTER the attention tier, because the
    # confluence reading needs to know what our own scanner thinks. Note the
    # direction of that dependency: confluence is derived FROM the attention
    # tier and can never feed back into it. The tier above was computed
    # without any knowledge that this block exists.
    evidence = sv.build_symbol_evidence(candidate_details)
    attention = sv.classify_attention(
        has_candidate_result=has_candidate,
        candidate_verdict=candidate_verdict,
        setup_state=evidence["setup_state"] if evidence else None,
        readiness_status=evidence["readiness_status"] if evidence else None,
        control_verdict=control_verdict,
    )
    try:
        signals, source_rows, ext_fresh = await _load_external(
            db, [symbol], session_date, now)
        external_intelligence = ex.build_external_context(
            signals.get(symbol) or [], as_of_session=session_date,
            sources=source_rows, freshness=ext_fresh, attention=attention)
    except Exception:
        logger.warning("external intelligence unavailable for symbol detail",
                       exc_info=False)
        external_intelligence = ex.empty_external_context(
            reason=ex.REASON_SOURCE_UNAVAILABLE)

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
        "attention": attention,
        "cross_arm": sv.classify_cross_arm(
            candidate_verdict=candidate_verdict, control_verdict=control_verdict
        ),
        # Where the strategy stopped, in evaluation order, and everything that
        # is not passed — the deterministic answer to "what would have to change".
        "gate_progress": sv.build_gate_progress(
            candidate_details, allow_enter=pc.CANDIDATE_ALLOW_ENTER
        ),
        "blockers": sv.build_blockers(
            candidate_details, allow_enter=pc.CANDIDATE_ALLOW_ENTER
        ),
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
            "structure_state": sv.structure_state(candidate_details),
            "reason_code": sv.reason_code(candidate_details),
            "evidence": evidence,
        },
        "control": {
            "strategy_code": pc.CONTROL_STRATEGY_CODE,
            "strategy_version": pc.CONTROL_STRATEGY_VERSION,
            "verdict": control_verdict,
            "score": control_score,
            "reason": control_reason,
        },
        # Context sits BESIDE the strategy result and never feeds into it.
        "market_context": market_context,
        # Known corporate events. Separate from market_context on purpose:
        # "this setup exists AND earnings are approaching", never "therefore".
        "catalyst_context": catalyst_context,
        # What systems OUTSIDE Smart Scanner claimed about this symbol, with
        # every claim's provenance attached. A sibling of catalyst_context and
        # never a member of it: an 8-K is a company's formal disclosure, an
        # external signal is a machine's opinion, and merging them would let
        # the opinion borrow the filing's authority.
        "external_intelligence": external_intelligence,
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
