# Phase 1 - Foundation & Correctness (Implementation Summary)

Scope: correctness only. No Wyckoff, no funnel scanner, no outcome tracking, no
backtests, no LLM, no UI redesign. The existing `sma150_bounce` scanner is now
correct, configurable, measurable, and trustworthy.

## What was fixed

| ID | Fix | Where |
|----|-----|-------|
| B1 | Pattern config is now loaded from the DB and passed into evaluation. New resolver merges DB `pattern_configs` over safe defaults; logs clearly when falling back. | `app/workers/patterns/config.py`, `scan_runner.py` |
| B2 | `score_components` now hold RAW measured values (proximity, price_vs_sma, deduped bounce count, avg rebound, volume ratio) plus `score_version`, `thresholds_used`, `trend_context`, `rejection_reason`. No more `score * weight`. | `app/workers/patterns/sma150.py` |
| B11 | DB connections are released back to the pool via `release_db_connection()` instead of `conn.close()`, which had been destroying pooled connections. | `app/workers/persistence.py` |
| B12 | `sma150_bounce` thresholds are stricter by default and fully config-driven (touch 3%, min_bounces 2, min_avg_rebound 5%, min_vol_ratio 1.0, min_price 5.0, score_threshold 0.5). | `sma150.py`, `002_phase1_sma150_config.sql` |
| - | Bounce counting deduplicated: a contiguous in-band run is ONE touch event, not one per day. | `sma150.py::find_historical_bounces` |
| B9 | `filter_by_liquidity` no longer computes an unused fake market cap; it enforces real avg volume + min price and returns a rejection reason. Market-cap filtering stays at the universe level. | `app/workers/tickers.py` |
| B10 | Ticker cache refresh sources REAL market cap + volume from the FMP screener. Volume is never fabricated; unknown volume is stored as NULL. | `tickers.py::refresh_tickers_cache` |
| B6 | Removed the duplicate docker-compose `scheduler` curl service. The in-process APScheduler is the single authoritative scheduler (guarded by `ENABLE_SCHEDULER`, `max_instances=1`, `coalesce=True`). | `docker-compose.yml`, `scheduler.py` |
| B7 | `run_maintenance_tasks` no longer misuses `async with get_db()` (an async generator); it acquires a pooled connection and releases it. | `scan_runner.py` |
| B8 | Added `/api/health` alias so the UI (which prefixes `/api`) reports health correctly, without changing the frontend. | `main.py` |
| - | Minimal reject-telemetry foundation: each scan run persists JSON in `pattern_runs.notes` (totals, top rejection reasons, config used, score_version, runtime). No new schema. | `scan_runner.py` |

## Files changed

Modified:
- `app/workers/patterns/sma150.py`
- `app/workers/persistence.py`
- `app/workers/scan_runner.py`
- `app/workers/scheduler.py`
- `app/workers/tickers.py`
- `main.py`
- `docker-compose.yml`

Added:
- `app/workers/patterns/config.py` (config resolver)
- `app/db/migrations/002_phase1_sma150_config.sql` (authoritative stricter config)
- `pytest.ini`, `requirements-dev.txt`
- `tests/` (see below)

## Tests added (23, all passing)

- `tests/test_bounce_dedup.py` - consecutive in-band days = 1 bounce; separated touches = multiple; no touch = 0; proximity threshold changes count.
- `tests/test_score_components.py` - score_components keys are raw; equal measured values, not `score*weight`; `score_version` present.
- `tests/test_config_wiring.py` - changing config changes verdict; injected thresholds reflected in output.
- `tests/test_config_resolver.py` - JSONB coercion, merge/fallback, DB overrides via monkeypatch.
- `tests/test_liquidity_filter.py` - real volume enforced; low volume/price rejected; unknown volume not fabricated.
- `tests/test_health_route.py` - `/api/health` and `/health` both registered.

Run:
```bash
python3 -m venv --system-site-packages .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

## Config change note (operational)

Apply `app/db/migrations/002_phase1_sma150_config.sql` in Supabase. It upserts
the stricter values (with `DO UPDATE`) and adds the two new keys the evaluator
reads: `score_threshold` and `min_price`. Until it is applied, the code falls
back to the same conservative defaults, so behavior is safe either way.

## Tradeoffs / TODOs (not in Phase 1 scope)

- The existing UI `SignalDrawer` renders `details.score_components` through
  `formatScore()`, which is tuned for 0..1 scores. Raw values (e.g. a 3% distance
  or a volume ratio of 1.4) will display with score formatting. This is cosmetic
  and deferred to the Phase 6 UI work; the persisted data is correct.
- `process_single_symbol` (non-batch path) was updated for consistency but the
  scheduler/admin flows use the batch path.
- Reject telemetry currently lives in `pattern_runs.notes` (JSON). The richer
  `scan_runs` / `scan_rejects` tables are Phase 3.
- Market-cap enforcement depends on the ticker cache being refreshed from the
  screener; the DB-level filter in `get_candidate_tickers` uses `last_volume`
  and `market_cap` which are now real.

## What remains for Phase 2

- `signal_outcomes` + `market_regime` tables.
- Outcome tracker (1/3/5/10/20D returns, MFE/MAE, realized R).
- Baselines (SPY/QQQ buy&hold, ticker buy&hold, momentum, mean-reversion, sma150).
- Backtest harness + metrics (expectancy, avg R, profit factor, drawdown, sample size).
- Comparison endpoints proving whether a strategy beats baselines before enabling it.
