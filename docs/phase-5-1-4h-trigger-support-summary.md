# Phase 5.1 — Survivor-only 4H trigger support (summary)

Phase 5 left a gap: through the funnel, `wyckoff_mtf` could only ever return
WATCH because 4H data had to be injected manually. Phase 5.1 adds a **safe,
bounded, survivor-only** 4H data path so wyckoff can produce real ENTER signals
— without broad expensive FMP usage.

**This still does not prove alpha.** ENTER signals must accumulate Phase 2
outcomes vs. baselines before any value claim.

---

## What changed

| File | Change |
| --- | --- |
| `app/workers/fmp_client.py` | New `fetch_historical_4h(symbol, limit=None)` using `/historical-chart/4hour/{symbol}`. Returns the same `{"symbol", "historical": [...]}` shape as daily (newest-first), so `to_dataframe` works unchanged. **Never raises**: unsupported endpoint / error dict / empty response yield an empty `historical` list. |
| `app/workers/scanner/funnel.py` | Stage 4 is now real: after Stage 3, WATCH survivors of a "4h"-declaring strategy get ONE 4H fetch and ONE re-evaluation with `data_meta["df_4h"]` injected (daily data reused, not refetched). New telemetry: `stage_counts.stage_4_4h_fetched`, `api_call_counts.four_hour_fetches`. |
| `app/workers/strategies/wyckoff/strategy.py` | WATCH reason now says explicitly: "MTF + daily setup valid, no 4H trigger yet". |
| `tests/test_funnel_4h.py` | Deterministic tests (all FMP mocked). |

No migration was needed; the gating uses existing config keys.

---

## When is 4H fetched? (ALL must be true)

1. The scan is **not** a dry run (dry_run never touches FMP).
2. Expensive gate explicitly enabled: scanner-level `enable_expensive_stages=true`
   **or** pattern-level `enable_4h_trigger=true` (both default false).
3. The strategy declares `"4h"` in `required_timeframes` — only `wyckoff_mtf`
   does; `sma150_bounce` never triggers a 4H fetch.
4. The candidate already survived Stage 1 (liquidity) and Stage 2 (daily
   prefilter), and Stage 3 returned **WATCH** — meaning monthly, weekly, and
   daily are all valid and only the trigger is missing. Rejected candidates
   never get a 4H fetch.
5. The candidate is inside the `limit` / `max_universe_size` bound (4H fetches
   are a subset of the bounded daily fetch set).

If the 4H response is empty/unsupported, the candidate simply **stays WATCH** —
no fake data is ever created and the scan does not fail.

## ENTER behavior (when the 4H trigger confirms)

- `decision = ENTER`, `side` from the monthly bias (LONG/SHORT)
- `entry_price` = 4H trigger close
- `stop_price` / `invalidation` = recent 4H local swing (deterministic)
- `target_price` = null in v1 (no deterministic rule)
- `setup_type` preserved from the daily setup (spring/sos/utad/sow/…)
- `details` include side, prices, and the timeframe summary — Phase 2 outcome
  tracking can consume these directly. Outcomes are still not auto-calculated.

---

## FMP endpoint / plan assumptions

- Endpoint: `GET /historical-chart/4hour/{symbol}` (stable FMP API v3 shape,
  bare JSON list of bars, newest-first).
- Availability of intraday history **depends on the FMP plan**. If the plan
  doesn't include it, the client logs a warning and returns empty — wyckoff
  candidates then remain WATCH. Verify with a single-symbol validation run
  before assuming ENTER signals are reachable.
- Each 4H fetch is one API call per WATCH survivor per scan, on top of the one
  daily-history call per bounded survivor.

---

## How to run a tiny validation scan

```bash
# 1) FMP-free sanity check (no writes, no fetches):
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pattern_code":"wyckoff_mtf","scanner_mode":"funnel","dry_run":true,"limit":10}' | jq .

# 2) Tiny real run (bounded: <=5 daily fetches + <=5 4H fetches).
#    Requires enable_4h_trigger=true in wyckoff pattern_configs (or passing
#    scanner-level enable_expensive_stages) — keep the limit small:
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pattern_code":"wyckoff_mtf","scanner_mode":"funnel","limit":5}' | jq .
```

Then inspect `pattern_runs.notes` for `stage_4_4h_fetched` and
`four_hour_fetches` to confirm the fetch count matched expectations.

---

## Tests (deterministic, no live FMP/Supabase)

`tests/test_funnel_4h.py`:
- 4H normalization: path, newest-first `limit`, `to_dataframe` sorts ascending
- error-dict and exception responses return empty safely
- dry_run never fetches 4H even with expensive stages enabled
- expensive-disabled default: no 4H fetch, candidate stays WATCH
- 4H fetched only for WATCH survivors (rejects skipped) and converts WATCH→ENTER
  with side/entry/stop/setup_type persisted in details
- empty 4H keeps WATCH (no fake data)
- sma150_bounce never fetches 4H even with expensive stages enabled
- `limit` bounds the number of 4H fetches

---

## Unresolved / next

- The real FMP 4hour endpoint has not been hit live yet (plan-dependent).
- WATCH candidates are counted in telemetry but still not persisted as signals.
- **No alpha claim** — value must come from Phase 2 outcomes vs. baselines.
