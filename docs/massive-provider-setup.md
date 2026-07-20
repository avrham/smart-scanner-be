# Massive provider setup

Massive is the **primary** market data provider (discovery + market data). FMP
remains available as a fallback via `MARKET_DATA_PROVIDER=fmp` and is still used
by the legacy scanner path and outcome calculation.

## 1. Configure

In `.env` (see `.env.example`):

```env
MARKET_DATA_PROVIDER=massive        # default; set fmp to fall back
MASSIVE_API_KEY=your-massive-api-key
# Optional (defaults shown):
# MASSIVE_BASE_URL=https://api.massive.com
# MASSIVE_REQUESTS_PER_MINUTE=5     # Basic plan
# MASSIVE_PROFILE_CACHE_DAYS=7
```

Apply migration `app/db/migrations/005_massive_provider.sql` in the Supabase SQL
editor (after 004). It is additive and idempotent: it extends `tickers` with
reference metadata and creates the `daily_bars` table.

The key is sent via the Authorization header + `apiKey` query param, is re-applied
to paginated `next_url` requests, and is never logged or echoed in errors.
`GET /api/health` now reports the provider block (name, credentials configured,
rate-limit mode, latest universe/daily sync) — never the key itself.

## 2. Data flow on Massive Basic (5 requests/minute)

| Step | Endpoint | Cost |
| --- | --- | --- |
| Universe sync | `/v3/reference/tickers` (paginated) | ~12–13 requests (~3 min) |
| Daily market ingest | `/v2/aggs/grouped/.../{date}` | **1 request** for the whole market |
| Local pre-screen | none (local bars: min price / volume / close×volume dollar volume) | free |
| Market-cap enrichment | `/v3/reference/tickers/{ticker}` survivors-only, 7-day cache | bounded by `max_detail_calls` |
| Historical backfill | `/v2/aggs/ticker/{t}/range/1/day/{from}/{to}` | 1 request per symbol needing bars; local-first + incremental |

Eligibility classification uses the provider's `type`/`primary_exchange`
metadata (common stock, XNAS/XNYS/XASE, OTC excluded) — never ticker suffixes.
Missing market cap is preserved as NULL with `enrichment_status =
'missing_market_cap'`, never coerced to 0.

## 3. Commands

```bash
# 1) Universe sync (background, ~3 min at 5 rpm):
curl -s -X POST "$BASE/api/admin/universe/sync" -H "X-Worker-Token: $TOKEN" | jq .

# 2) Ingest one grouped daily snapshot (1 request, synchronous):
curl -s -X POST "$BASE/api/admin/market/daily-sync" \
  -H "X-Worker-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"trading_date":"2026-07-17"}' | jq .

# 3) Survivor-only market-cap enrichment (background; 25 calls ≈ 5 min):
curl -s -X POST "$BASE/api/admin/universe/enrich" \
  -H "X-Worker-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"trading_date":"2026-07-17","max_detail_calls":25}' | jq .

# 4) Verify with the FMP/Massive-free funnel dry run:
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "X-Worker-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"scanner_mode":"funnel","pattern_code":"sma150_bounce","dry_run":true,"limit":25}' | jq '.stage_counts'

# 5) Small real funnel scan (historical bars are backfilled locally as it runs):
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "X-Worker-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"scanner_mode":"funnel","pattern_code":"sma150_bounce","limit":5}' | jq .
```

Historical bars are stored in `daily_bars` on first fetch; later scans reuse
local data and only top up missing days (incremental from the latest stored
trading date). Repeat the daily-sync each trading day to keep bars fresh with a
single request.

## 4. Massive Basic limitations

- **5 requests/minute (rolling window)** — the limiter allows a burst of 5
  requests, then waits for capacity; it does NOT sleep after every request.
  Initial history backfill costs exactly **1 aggregates request per symbol**
  (one request returns the full ~2-year range; profile enrichment is a separate
  survivor-only step). Expected throughput ≈ **5 symbols/minute**:

  | Symbols | Expected duration (no retries) |
  | --- | --- |
  | 5 | seconds (single burst) |
  | 50 | ~9 minutes |
  | 500 | ~99 minutes |
  | 1,000 | ~199 minutes |

  Retries on 429/5xx add time only when they occur. Once bars are stored
  locally, re-scans cost zero backfill requests (incremental top-up only), and
  the daily grouped sync keeps the whole market fresh for 1 request/day.
- **~2 years of daily history (~500 trading bars)**:
  - `sma150_bounce` needs 200 bars → fine on Basic.
  - `wyckoff_mtf` needs **540** bars (≥24 monthly bars) → **NOT satisfiable on
    Basic**. The threshold was deliberately not reduced; wyckoff will reject
    every symbol with `insufficient_daily_data` and report
    `daily_bars` vs `daily_bars_required` explicitly. No silent claims.
- 4H aggregates are implemented (`/range/4/hour`) but unverified on Basic; when
  unavailable the wyckoff trigger simply stays WATCH (no fake data).

## 5. When Massive Starter becomes necessary

- To run `wyckoff_mtf` at all (needs >2 years of daily history).
- For meaningfully faster scans (Basic's 5 rpm makes broad backfills slow).
- For dependable intraday (4H) data for the wyckoff ENTER trigger.

Until then: run sma150_bounce scans on Massive Basic, accumulate `daily_bars`
via the daily grouped sync, and switch `MARKET_DATA_PROVIDER=fmp` if you need
the old flow (legacy scans + outcome calculation still use FMP directly).
