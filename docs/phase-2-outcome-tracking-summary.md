# Phase 2 — Outcome Tracking & Baseline Comparison (summary)

This phase is the product's **value gate**: it lets us measure, after the fact,
how each generated signal actually performed over fixed holding windows and
compare it honestly to simple baselines. No strategy should be called "useful"
without both a **sample size** and a **baseline comparison**.

Phase 2 changes are additive. **No existing signal-generation logic was
changed** — the existing `signals` table already carries the stable IDs and
timestamps outcome tracking needs.

---

## What was implemented

- A new `signal_outcomes` table (one row per signal).
- A pure, unit-tested numeric core for outcomes and metrics.
- An outcome calculation service that fetches historical data and persists
  results, resilient to per-symbol failures.
- Read-only inspection endpoints and one admin (token-protected) calculation
  endpoint.
- 33 new tests (56 total, all passing).

## Schema added

Migration: `app/db/migrations/003_phase2_signal_outcomes.sql`

`public.signal_outcomes`:

- `signal_id` (UNIQUE, FK → `signals.id`, ON DELETE CASCADE), `symbol`,
  `pattern_code`, `side` (`LONG`/`SHORT`), `signal_timestamp`.
- Trade levels (nullable): `entry_price`, `stop_price`, `target_price`,
  `invalidation`.
- Signal returns per window (PERCENT, side-adjusted): `ret_1d`, `ret_3d`,
  `ret_5d`, `ret_10d`, `ret_20d` (NULL when not enough future bars).
- Baselines (JSONB, per-window labels `"1D".."20D"`):
  `benchmark_returns` (`{"SPY": {...}, "QQQ": {...}}`) and
  `same_ticker_buy_hold` (naive LONG hold).
- `max_favorable_excursion`, `max_adverse_excursion` (PERCENT).
- `hit_stop`, `hit_target` (nullable), `simulated_r` (nullable).
- `outcome_status` (`pending|calculated|insufficient_data|error`),
  `calculation_version`, `created_at`, `updated_at`.
- Indexes on `(pattern_code, side)`, `(outcome_status)`, `(symbol)`.

**Design note (simplicity):** the signal's own returns are explicit numeric
columns (easy to aggregate); the SPY/QQQ/same-ticker breakdowns are compact
JSONB maps rather than ~20 extra numeric columns.

**Entry price:** derived deterministically as the close of the signal's
`snapshot_date` bar. Forward windows use subsequent daily closes. `sma150_bounce`
has no stop/target/side, so `side` defaults to `LONG` and stop/target-derived
fields stay NULL (and are excluded from metrics) until a strategy provides them.

## Code layout

Pure (no I/O, fully unit-tested):
- `app/workers/outcomes/calculator.py` — signed returns, forward returns,
  buy&hold, MFE/MAE, stop/target hits, simplified R.
- `app/workers/outcomes/baselines.py` — benchmark buy&hold + signal-vs-baseline
  deltas.
- `app/workers/outcomes/metrics.py` — aggregation (sample size, win rate,
  avg/median return, avg R, profit factor, avg MFE/MAE, baseline deltas,
  grouping).

I/O:
- `app/workers/outcomes/persistence.py` — CRUD, `get_signals_needing_outcomes`,
  `fetch_outcomes` (reuses the pooled-connection release discipline).
- `app/workers/outcomes/service.py` — `build_outcome_from_frames` (pure) and
  `calculate_outcomes_for_signals` (async orchestration: fetch symbol + SPY/QQQ
  OHLCV, build, persist; a single symbol failing never aborts the run).

API:
- `app/routers/outcomes.py` — `GET /api/outcomes`, `GET /api/outcomes/metrics`.
- `app/routers/admin.py` — `POST /api/admin/outcomes/calculate` (worker token).
- Registered in `main.py`.

## How to run outcome calculation

Outcome calculation is **on-demand only** — it is NOT scheduled and NOT enabled
automatically. It performs FMP calls (affected symbols + SPY + QQQ), so run it
deliberately. Keep `ENABLE_SCHEDULER=false` during validation.

```bash
# Compute outcomes for up to 50 signals that don't have one yet.
curl -s -X POST http://localhost:8000/api/admin/outcomes/calculate \
  -H "Content-Type: application/json" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  -d '{"limit": 50, "run_in_background": false}'

# Inspect results
curl -s "http://localhost:8000/api/outcomes?pattern_code=sma150_bounce&limit=20" | jq .

# Metrics (all windows), and grouped
curl -s "http://localhost:8000/api/outcomes/metrics?pattern_code=sma150_bounce" | jq .
curl -s "http://localhost:8000/api/outcomes/metrics?window=5&group_by=pattern_code,side" | jq .
```

Request body for `/api/admin/outcomes/calculate`:
`limit` (default 50), `pattern_code` (optional), `include_recalc` (default
false; also reprocesses `pending`/`error`/`insufficient_data`),
`run_in_background` (default true).

## What baselines exist

- **Same-ticker buy & hold** — naive LONG hold of the signal's own symbol over
  the same window. "Would just holding it have done better?"
- **SPY buy & hold** — market baseline over the same window.
- **QQQ buy & hold** — tech/growth baseline (when data is available).

Deferred: momentum, mean-reversion, random-sector, and any regime-conditioned
baselines.

## Tests

New (33): `test_outcome_returns.py`, `test_outcome_mfe_mae.py`,
`test_outcome_stop_target.py`, `test_outcome_baselines.py`,
`test_outcome_metrics.py`, `test_outcome_builder.py`, `test_outcome_routes.py`.
Run: `.venv/bin/python -m pytest` → **56 passed**.

DB-touching persistence functions are covered indirectly (pure builder + route
wiring). Live DB round-trip tests are deferred until a test DB fixture exists.

## What is still NOT proven

- **End-to-end run against real data has not happened.** All outcome math is
  verified on synthetic data via unit tests; it has not been executed against
  real generated signals + live FMP history.
- **Controlled scan smoke remains a validation debt** (intentionally skipped
  before Phase 2). Until it is run, we have no real signals to compute outcomes
  for, and therefore no real evidence of signal value yet.
- No claim about whether `sma150_bounce` beats its baselines can be made until
  there is a meaningful sample of calculated outcomes.

## Safety / scope honored

- No Wyckoff, no LLM, no prediction logic, no UI, no funnel scanner.
- No scheduler changes; no automatic outcome jobs; `ENABLE_SCHEDULER` stays
  `false` for validation.
- No broad FMP jobs run as part of this change.
- No change to existing signal-generation logic.
