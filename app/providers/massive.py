"""Massive implementation of MarketDataProvider (primary provider).

Cost discipline (Massive Basic, ~5 requests/minute):
  * Universe sync: ~12-13 paginated reference requests for the whole US market.
  * Daily grouped ingest: ONE request stores bars for the entire market.
  * Ticker details (market cap): survivor-only, AFTER the free local pre-screen,
    cached for MASSIVE_PROFILE_CACHE_DAYS.
  * Historical bars: local-first — only missing/stale ranges are fetched, and
    fetched bars are stored so the next scan is cheaper.

History honesty: Massive Basic serves ~2 years of daily history (~500 bars).
Strategies keep their own requirements (e.g. wyckoff_mtf needs 540 daily bars)
and report insufficiency explicitly — this provider never pads or fakes bars.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.config import settings
from app.providers.base import MarketDataProvider
from app.workers import market_store
from app.workers.massive_client import (
    MassiveApiError,
    MassiveClient,
    bars_to_fmp_payload,
    map_agg_bar,
    map_grouped_row,
)
from app.workers.screening import (
    ENRICHMENT_SELECTION_STRATEGY,
    MIC_TO_SHORT,
    classify_ticker,
    enrichment_status_for,
    needs_profile_refresh,
    prescreen_bars,
    prioritize_enrichment,
)


logger = logging.getLogger(__name__)

# Calendar days needed to cover N trading days (~252 trading days/year).
CALENDAR_FACTOR = 1.55
# Local history is considered fresh if its newest bar is at most this old.
STALE_AFTER_DAYS = 5


def _parse_provider_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class MassiveProvider(MarketDataProvider):
    name = "massive"
    # get_daily_bars maps directly onto Massive aggregates over an explicit
    # [from_date, to_date] window — genuine bounded range retrieval, so old
    # shadow pairs remain calculable after they leave any latest-N window.
    supports_bounded_daily_range = True
    # Massive aggregates serve REAL bounded intraday ranges too (Phase 9E1).
    supports_intraday_history = True

    def __init__(self, client: Optional[MassiveClient] = None):
        self.client = client or MassiveClient(api_key=settings.MASSIVE_API_KEY)

    # ------------------------------------------------------------------ #
    # Universe
    # ------------------------------------------------------------------ #

    async def sync_universe(self) -> Dict[str, Any]:
        """Fetch all reference pages, classify eligibility, upsert locally."""
        raw = await self.client.list_tickers(
            market="stocks", locale="us", active=True, limit=1000
        )

        rows: List[Dict[str, Any]] = []
        reason_counts: Dict[str, int] = {}
        eligible_count = 0

        for t in raw:
            symbol = (t.get("ticker") or "").strip().upper()
            if not symbol:
                continue
            eligible, reason = classify_ticker(
                t,
                allowed_exchanges=settings.UNIVERSE_ALLOWED_EXCHANGES,
                allowed_types=settings.UNIVERSE_ALLOWED_SECURITY_TYPES,
                include_otc=settings.UNIVERSE_INCLUDE_OTC,
            )
            if eligible:
                eligible_count += 1
            elif reason:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

            mic = (t.get("primary_exchange") or "").upper()
            rows.append(
                {
                    "symbol": symbol,
                    "name": t.get("name"),
                    "exchange": MIC_TO_SHORT.get(mic),  # legacy funnel column
                    "market": t.get("market"),
                    "locale": t.get("locale"),
                    "primary_exchange": mic or None,
                    "security_type": t.get("type"),
                    "currency": t.get("currency_name"),
                    "cik": t.get("cik"),
                    "composite_figi": t.get("composite_figi"),
                    "share_class_figi": t.get("share_class_figi"),
                    "is_active": bool(t.get("active", True)),
                    "eligible": eligible,
                    "provider_updated_at": _parse_provider_ts(t.get("last_updated_utc")),
                }
            )

        stored = await market_store.bulk_upsert_universe(rows)
        summary = {
            "provider": self.name,
            "fetched": len(raw),
            "stored": stored,
            "eligible": eligible_count,
            "ineligible_reasons": reason_counts,
        }
        logger.info("[massive] universe sync: %s", summary)
        return summary

    # ------------------------------------------------------------------ #
    # Daily grouped ingestion + survivor-only enrichment
    # ------------------------------------------------------------------ #

    async def get_daily_market_summary(self, trading_date: str) -> Dict[str, Any]:
        """Ingest the whole-market grouped daily snapshot (ONE request)."""
        raw = await self.client.get_grouped_daily(trading_date, adjusted=True)
        bars = [b for b in (map_grouped_row(r) for r in raw) if b is not None]
        stored = await market_store.bulk_upsert_daily_bars(bars)

        volumes_updated = 0
        if bars:
            volumes_updated = await market_store.update_last_volumes_from_bars(
                bars[0]["trading_date"]
            )

        summary = {
            "provider": self.name,
            "trading_date": trading_date,
            "records": len(raw),
            "bars_stored": stored,
            "ticker_volumes_updated": volumes_updated,
        }
        logger.info("[massive] daily ingest: %s", summary)
        return summary

    async def enrich_market_caps(
        self,
        trading_date: date,
        max_detail_calls: int = 25,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Survivor-only market-cap enrichment.

        1. Cheap local pre-screen on the stored grouped bars (FREE — no API).
        2. Detail calls ONLY for survivors whose cached profile is stale
           (older than MASSIVE_PROFILE_CACHE_DAYS), bounded by max_detail_calls.
        Missing market cap keeps NULL + enrichment_status='missing_market_cap'.

        progress_callback (Phase 7A, optional): async callable receiving small
        counter dicts after selection and periodically during the detail loop.
        Failures in the callback never fail the enrichment itself.
        """
        bars = await market_store.get_bars_for_date(trading_date)
        eligible = await market_store.get_eligible_symbols()

        survivors, reject_counts = prescreen_bars(
            bars,
            eligible,
            min_price=settings.PRESCREEN_MIN_PRICE,
            min_volume=settings.PRESCREEN_MIN_VOLUME,
            min_dollar_volume=settings.PRESCREEN_MIN_DOLLAR_VOLUME,
        )

        profiles = {p["symbol"]: p for p in await market_store.get_ticker_profiles(survivors)}
        now = datetime.now(timezone.utc)
        stale = [
            s for s in survivors
            if needs_profile_refresh(
                (profiles.get(s) or {}).get("profile_synced_at"),
                now,
                settings.MASSIVE_PROFILE_CACHE_DAYS,
            )
        ]
        # Deterministic priority: dollar volume desc, volume desc, symbol asc.
        bars_by_symbol = {b["symbol"]: b for b in bars if b.get("symbol")}
        prioritized = prioritize_enrichment(stale, bars_by_symbol)
        to_refresh = prioritized[:max_detail_calls]

        async def _report(payload: Dict[str, Any]) -> None:
            if progress_callback is None:
                return
            try:
                await progress_callback(payload)
            except Exception as exc:  # progress is observability, never fatal
                logger.warning("[massive] progress callback failed: %s", type(exc).__name__)

        await _report(
            {
                "phase": "selected",
                "prescreen_survivors": len(survivors),
                "stale_candidates": len(prioritized),
                "detail_calls_planned": len(to_refresh),
            }
        )

        enriched = missing = errors = processed = 0
        for symbol in to_refresh:
            try:
                details = await self.client.get_ticker_details(symbol)
                market_cap = (details or {}).get("market_cap")
                status = enrichment_status_for(market_cap)
                await market_store.update_ticker_profile(
                    symbol,
                    float(market_cap) if market_cap is not None else None,
                    status,
                )
                if market_cap is not None:
                    enriched += 1
                else:
                    missing += 1
            except MassiveApiError as exc:
                errors += 1
                logger.warning("[massive] enrichment failed for %s: %s", symbol, exc)
            processed += 1
            if processed % 5 == 0 or processed == len(to_refresh):
                await _report(
                    {
                        "phase": "enriching",
                        "processed": processed,
                        "planned": len(to_refresh),
                        "enriched": enriched,
                        "missing_market_cap": missing,
                        "errors": errors,
                    }
                )

        summary = {
            "provider": self.name,
            "trading_date": str(trading_date),
            "prescreen_survivors": len(survivors),
            "prescreen_rejects": reject_counts,
            "detail_calls": len(to_refresh),
            "enriched": enriched,
            "missing_market_cap": missing,
            "errors": errors,
            "cached_fresh": len(survivors) - len(stale),
            "selection_strategy": ENRICHMENT_SELECTION_STRATEGY,
            "selected_symbols": to_refresh[:25],
            "remaining_stale_survivors": len(prioritized) - len(to_refresh),
        }
        logger.info("[massive] enrichment: %s", summary)
        return summary

    # ------------------------------------------------------------------ #
    # Historical bars
    # ------------------------------------------------------------------ #

    async def get_daily_bars(
        self, symbol: str, from_date: str, to_date: str
    ) -> List[Dict[str, Any]]:
        raw = await self.client.get_aggs(symbol, 1, "day", from_date, to_date)
        return [b for b in (map_agg_bar(symbol, r) for r in raw) if b is not None]

    async def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        return await self.client.get_ticker_details(symbol)

    async def _daily_history_for(self, symbol: str, timeseries: int) -> Dict[str, Any]:
        """Local-first daily history with incremental top-up.

        Fetches from Massive only when local bars are missing or stale; new bars
        are stored so subsequent scans are cheaper. Never raises — on provider
        errors it returns whatever exists locally.
        """
        today = datetime.now(timezone.utc).date()
        local = await market_store.get_local_daily_bars(symbol, limit=timeseries + 30)
        latest_local: Optional[date] = local[-1]["trading_date"] if local else None

        have_enough = len(local) >= timeseries
        fresh = latest_local is not None and (today - latest_local).days <= STALE_AFTER_DAYS

        if not (have_enough and fresh):
            if latest_local is not None and have_enough:
                fetch_from = latest_local + timedelta(days=1)  # incremental only
            else:
                fetch_from = today - timedelta(days=int(timeseries * CALENDAR_FACTOR))
            try:
                fetched = await self.get_daily_bars(symbol, str(fetch_from), str(today))
                if fetched:
                    await market_store.bulk_upsert_daily_bars(fetched)
                    merged = {b["trading_date"]: b for b in local}
                    merged.update({b["trading_date"]: b for b in fetched})
                    local = [merged[d] for d in sorted(merged.keys())][-(timeseries + 30):]
            except MassiveApiError as exc:
                logger.warning("[massive] history fetch failed for %s: %s", symbol, exc)

        return bars_to_fmp_payload(symbol, local[-timeseries:] if local else [])

    async def get_daily_history(self, symbol: str, timeseries: int = 400) -> Dict[str, Any]:
        """Single-symbol daily history (local-first, incremental top-up)."""
        return await self._daily_history_for(symbol, timeseries)

    async def batch_historical_data(
        self, symbols: List[str], timeseries: int = 350
    ) -> Dict[str, Dict[str, Any]]:
        """Sequential per-symbol history (rate limiter paces the actual calls)."""
        results: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            try:
                results[symbol] = await self._daily_history_for(symbol, timeseries)
            except Exception as exc:  # never let one symbol abort the batch
                logger.warning("[massive] history failed for %s: %s", symbol, exc)
                results[symbol] = {"symbol": symbol, "historical": []}
        return results

    async def get_intraday_history(
        self,
        symbol: str,
        *,
        multiplier: int,
        timespan: str,
        start=None,
        end=None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Normalized typed intraday bars over an explicit bounded range.

        Maps directly onto Massive aggregates (epoch-ms UTC bar starts).
        Requests MUST be bounded: both `start` and `end` are required — a
        missing bound would silently become an unbounded window. Bars are
        returned oldest-first with tz-aware UTC start timestamps; malformed
        rows are skipped and counted; exact-duplicate rows (same start and
        identical OHLCV) are dropped keep-first and counted; rows sharing a
        start with DIFFERENT values are preserved for the canonical frame
        layer to reject. Completed-bar semantics are NOT applied here.
        Provider errors propagate as MassiveApiError — never silently empty.
        """
        if int(multiplier) <= 0:
            raise ValueError("multiplier must be a positive integer")
        if timespan not in ("minute", "hour", "day"):
            raise ValueError(f"unsupported timespan {timespan!r}")
        if start is None or end is None:
            raise ValueError(
                "get_intraday_history requires explicit start and end bounds"
            )

        def _as_date_str(value) -> str:
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc).date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            return str(value)[:10]

        start_str = _as_date_str(start)
        end_str = _as_date_str(end)
        raw = await self.client.get_aggs(
            symbol, int(multiplier), timespan, start_str, end_str
        )

        bars: List[Dict[str, Any]] = []
        skipped = 0
        for bar in raw:
            try:
                start_utc = datetime.fromtimestamp(
                    float(bar["t"]) / 1000.0, tz=timezone.utc
                )
                bars.append({
                    "start_utc": start_utc,
                    "open": float(bar["o"]),
                    "high": float(bar["h"]),
                    "low": float(bar["l"]),
                    "close": float(bar["c"]),
                    "volume": float(bar["v"]),
                })
            except (KeyError, TypeError, ValueError):
                skipped += 1
                continue

        bars.sort(key=lambda b: b["start_utc"])
        deduped: List[Dict[str, Any]] = []
        dropped = 0
        for bar in bars:
            if deduped and deduped[-1] == bar:
                dropped += 1
                continue
            deduped.append(bar)
        if limit is not None:
            deduped = deduped[-int(limit):]

        return {
            "symbol": symbol,
            "provider": self.name,
            "multiplier": int(multiplier),
            "timespan": timespan,
            "requested_start": start_str,
            "requested_end": end_str,
            "bars": deduped,
            "skipped_malformed": skipped,
            "dropped_exact_duplicates": dropped,
        }

    async def fetch_historical_4h(
        self, symbol: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """4H bars via aggregates (last ~30 days). Empty when unavailable."""
        today = datetime.now(timezone.utc).date()
        try:
            raw = await self.client.get_aggs(
                symbol, 4, "hour", str(today - timedelta(days=30)), str(today)
            )
        except MassiveApiError as exc:
            logger.warning("[massive] 4H unavailable for %s: %s", symbol, exc)
            return {"symbol": symbol, "historical": []}

        rows = []
        for bar in raw:
            try:
                ts = datetime.fromtimestamp(float(bar["t"]) / 1000.0, tz=timezone.utc)
                rows.append(
                    {
                        "date": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": float(bar["o"]),
                        "high": float(bar["h"]),
                        "low": float(bar["l"]),
                        "close": float(bar["c"]),
                        "volume": float(bar["v"]),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue
        rows.reverse()  # newest-first, matching the FMP payload convention
        if limit is not None:
            rows = rows[: int(limit)]
        return {"symbol": symbol, "historical": rows}

    # ------------------------------------------------------------------ #
    # Health
    # ------------------------------------------------------------------ #

    async def health_check(self) -> Dict[str, Any]:
        """Light connectivity probe (1 request). Never returns credentials."""
        try:
            await self.client._request("/v3/reference/tickers", {"limit": 1})
            return {"provider": self.name, "connectivity": "ok"}
        except MassiveApiError as exc:
            return {
                "provider": self.name,
                "connectivity": "error",
                "status_code": exc.status_code,
            }
