"""Massive API client (primary market data provider).

Low-level async client responsible for authentication, rate limiting, retries
with exponential backoff, JSON parsing, structured errors and pagination.

SAFETY:
  * The API key is sent via the Authorization header AND the apiKey query param
    (pagination `next_url`s come back without credentials, so auth is re-applied
    to every request including follow-ups).
  * The key is NEVER logged and is stripped from any error text/excerpt.
  * Rate limited to MASSIVE_REQUESTS_PER_MINUTE (Basic plan default: 5/min).

Also contains the PURE mapping helpers from Massive's abbreviated aggregate
fields (T/o/h/l/c/v/vw/t/n) into the project's canonical daily-bar model.
"""

import asyncio
import json
import logging
import re
import time
from collections import deque
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import aiohttp

from app.config import settings


logger = logging.getLogger(__name__)

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 4
EXCERPT_LEN = 200


class MassiveApiError(Exception):
    """Structured provider error. Never contains the API key."""

    def __init__(self, endpoint: str, status_code: Optional[int], message: str, excerpt: str = ""):
        self.provider = "massive"
        self.endpoint = endpoint
        self.status_code = status_code
        self.excerpt = excerpt
        super().__init__(
            f"[massive] {endpoint} -> {status_code}: {message}"
            + (f" | body: {excerpt}" if excerpt else "")
        )


# --------------------------------------------------------------------------- #
# Pure mapping helpers (canonical bar model)
# --------------------------------------------------------------------------- #

def ms_to_trading_date(ms: Any) -> Optional[date]:
    """Massive `t` is the bar window start in milliseconds (UTC)."""
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).date()
    except (TypeError, ValueError, OSError):
        return None


def map_grouped_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map one grouped-daily record (T/o/h/l/c/v/vw/t/n) to a canonical bar.

    Returns None when required fields are missing/invalid (never fabricates).
    """
    try:
        symbol = (row.get("T") or "").strip().upper()
        trading_date = ms_to_trading_date(row.get("t"))
        if not symbol or trading_date is None:
            return None
        return {
            "symbol": symbol,
            "trading_date": trading_date,
            "open": float(row["o"]),
            "high": float(row["h"]),
            "low": float(row["l"]),
            "close": float(row["c"]),
            "volume": float(row["v"]),
            "vwap": float(row["vw"]) if row.get("vw") is not None else None,
            "transaction_count": int(row["n"]) if row.get("n") is not None else None,
        }
    except (KeyError, TypeError, ValueError):
        return None


def map_agg_bar(symbol: str, bar: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map one per-ticker aggregate bar (o/h/l/c/v/vw/t/n) to a canonical bar."""
    row = dict(bar)
    row["T"] = symbol
    return map_grouped_row(row)


def bars_to_fmp_payload(symbol: str, bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert canonical bars into the FMP-shaped payload the scanner consumes.

    ({"symbol", "historical": [{date, open, high, low, close, volume}, ...]}).
    Order does not matter downstream (`to_dataframe` re-sorts by date).
    """
    historical = [
        {
            "date": str(b["trading_date"]),
            "open": b["open"],
            "high": b["high"],
            "low": b["low"],
            "close": b["close"],
            "volume": b["volume"],
        }
        for b in bars
    ]
    return {"symbol": symbol, "historical": historical}


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #

class MassiveClient:
    """Async Massive API client with rate limiting, retries and pagination."""

    def __init__(
        self,
        api_key: str,
        base_url: Optional[str] = None,
        requests_per_minute: Optional[int] = None,
        retry_base_delay: float = 0.5,
    ):
        self.api_key = api_key
        self.base_url = (base_url or settings.MASSIVE_BASE_URL).rstrip("/")
        self._rpm = max(1, requests_per_minute or settings.MASSIVE_REQUESTS_PER_MINUTE)
        self._retry_base_delay = retry_base_delay
        # Rolling 60s window: up to _rpm requests may START inside any 60s
        # span (bursts allowed). This does NOT sleep after every request.
        self._request_times: deque = deque()
        self._lock = asyncio.Lock()
        # Injectable hooks so tests can drive the limiter deterministically.
        self._clock = time.monotonic
        self._sleep = asyncio.sleep

    # ---- internals ------------------------------------------------------- #

    def _sanitize(self, text: str) -> str:
        """Strip the API key from any outgoing log/error text."""
        if not text:
            return ""
        cleaned = text
        if self.api_key:
            cleaned = cleaned.replace(self.api_key, "***")
        return re.sub(r"apiKey=[^&\s\"']+", "apiKey=***", cleaned)

    def _endpoint_of(self, url: str) -> str:
        """Safe endpoint label (path only, no query string)."""
        return urlparse(url).path or url

    async def _throttle(self) -> None:
        """Rolling-window limiter: allow up to _rpm request starts per 60s.

        A burst of _rpm requests goes through immediately; the next request
        waits only until the oldest start falls out of the 60s window.
        """
        async with self._lock:
            while True:
                now = self._clock()
                while self._request_times and now - self._request_times[0] >= 60.0:
                    self._request_times.popleft()
                if len(self._request_times) < self._rpm:
                    self._request_times.append(now)
                    return
                wait = 60.0 - (now - self._request_times[0])
                await self._sleep(max(wait, 0.01))

    def _build_url(self, path_or_url: str, params: Optional[Dict[str, Any]]) -> str:
        """Build the request URL with auth ALWAYS included.

        Handles both API paths and absolute `next_url`s (which Massive returns
        without credentials — auth must be re-applied).
        """
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))
        query.update({k: str(v) for k, v in (params or {}).items()})
        query["apiKey"] = self.api_key
        return urlunparse(parsed._replace(query=urlencode(query)))

    async def _raw_get(self, url: str) -> Tuple[int, str]:
        """One HTTP GET. Isolated so tests can fake it (no real calls)."""
        timeout = aiohttp.ClientTimeout(total=45, connect=10, sock_read=30)
        headers = {"Authorization": f"Bearer {self.api_key}"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                return response.status, await response.text()

    async def _request(self, path_or_url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Rate-limited GET with retries on 429/5xx/timeouts; parsed JSON out."""
        url = self._build_url(path_or_url, params)
        endpoint = self._endpoint_of(url)
        last_error: Optional[MassiveApiError] = None

        for attempt in range(MAX_ATTEMPTS):
            await self._throttle()
            try:
                status, text = await self._raw_get(url)
            except asyncio.TimeoutError:
                last_error = MassiveApiError(endpoint, None, "timeout")
                await self._sleep(self._retry_base_delay * (2 ** attempt))
                continue
            except aiohttp.ClientError as exc:
                last_error = MassiveApiError(endpoint, None, f"network error: {type(exc).__name__}")
                await self._sleep(self._retry_base_delay * (2 ** attempt))
                continue

            excerpt = self._sanitize(text[:EXCERPT_LEN])

            if status in RETRYABLE_STATUSES:
                last_error = MassiveApiError(endpoint, status, "retryable error", excerpt)
                logger.warning("[massive] %s -> %s (attempt %d/%d)", endpoint, status, attempt + 1, MAX_ATTEMPTS)
                await self._sleep(self._retry_base_delay * (2 ** attempt))
                continue

            if status in (401, 403):
                # Auth errors never resolve by retrying.
                raise MassiveApiError(endpoint, status, "authentication/authorization failed", excerpt)

            if status >= 400:
                raise MassiveApiError(endpoint, status, "request failed", excerpt)

            try:
                return json.loads(text)
            except (ValueError, TypeError):
                raise MassiveApiError(endpoint, status, "malformed JSON response", excerpt)

        raise last_error or MassiveApiError(endpoint, None, "request failed after retries")

    # ---- endpoints ------------------------------------------------------- #

    async def list_tickers(
        self,
        market: str = "stocks",
        locale: str = "us",
        active: bool = True,
        limit: int = 1000,
        max_pages: int = 50,
    ) -> List[Dict[str, Any]]:
        """All reference tickers, following `next_url` pagination with auth."""
        results: List[Dict[str, Any]] = []
        payload = await self._request(
            "/v3/reference/tickers",
            {"market": market, "locale": locale, "active": str(active).lower(), "limit": limit},
        )
        pages = 1
        while True:
            results.extend(payload.get("results") or [])
            next_url = payload.get("next_url")
            if not next_url or pages >= max_pages:
                break
            payload = await self._request(next_url)  # auth re-applied in _build_url
            pages += 1
        logger.info("[massive] reference tickers: %d rows over %d page(s)", len(results), pages)
        return results

    async def get_grouped_daily(self, trading_date: str, adjusted: bool = True) -> List[Dict[str, Any]]:
        """Whole-market daily OHLCV for one date (1 request)."""
        payload = await self._request(
            f"/v2/aggs/grouped/locale/us/market/stocks/{trading_date}",
            {"adjusted": str(adjusted).lower()},
        )
        return payload.get("results") or []

    async def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Reference details for one ticker (market cap etc.). None on 404."""
        try:
            payload = await self._request(f"/v3/reference/tickers/{symbol}")
        except MassiveApiError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload.get("results") or None

    async def get_aggs(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        from_date: str,
        to_date: str,
        adjusted: bool = True,
        limit: int = 50000,
    ) -> List[Dict[str, Any]]:
        """Per-ticker aggregate bars over a range."""
        payload = await self._request(
            f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
            {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": limit},
        )
        return payload.get("results") or []
