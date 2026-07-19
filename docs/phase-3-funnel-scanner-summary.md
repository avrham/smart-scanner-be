# Phase 3 — Hierarchical Funnel Scanner (summary)

Phase 3 replaces random batch scanning with a **staged, telemetried funnel**:
start from a liquid universe, apply cheap filters first, evaluate strategies only
on survivors, and persist clear telemetry — producing fewer, more explainable
candidates. Changes are additive and reviewable.

**This phase does NOT prove signal alpha.** It makes scanning disciplined and
observable. Whether `sma150_bounce` (or any strategy) actually adds value is
answered by Phase 2 outcome tracking once real signals exist — see "still not
proven" below.

---

## What changed

- New package `app/workers/scanner/` with `funnel.py` (pure stages + async
  orchestrator).
- New read-only persistence helper `get_universe_tickers()` in
  `app/workers/persistence.py` (returns real cached `market_cap`/`last_volume`,
  NULLs preserved; never fabricates).
- `app/routers/admin.py`: `POST /api/admin/scan/start` gains `scanner_mode`
  (`legacy` default | `funnel`), `limit`, and `dry_run`. Legacy behavior is
  unchanged.
- Docs updated (`evidence-engine-architecture-plan.md`) + this summary.
- **No migration.** Telemetry is stored as JSON in the existing
  `pattern_runs.notes` (TEXT).

## How the funnel works

- **Stage 0 — Universe build.** Load candidate tickers from the ticker cache
  (`get_universe_tickers`), ordered by market cap. Real values only; unknown
  market cap/volume are represented as NULL.
- **Stage 1 — Liquidity filter (cheap, no FMP).** `classify_liquidity` rejects
  with explicit reasons: `market_cap_unknown`, `market_cap_below_min`,
  `volume_unknown` (unless `allow_unknown_volume`), `volume_below_min`. Runs
  BEFORE any history fetch. Survivors are then bounded by `limit` /
  `max_universe_size` before the expensive stage.
- **Stage 2 — Cheap daily prefilters.** `cheap_prefilter` on fetched OHLCV:
  `no_data`, `missing_columns`, `insufficient_history` (<200 bars),
  `invalid_ohlcv` (shape/positivity), `price_below_min`.
- **Stage 3 — Strategy evaluation.** Only survivors are evaluated, using the
  Phase 1 config resolver + `evaluate_sma150_bounce`. Verdicts: `ENTER` saved via
  the existing pipeline; non-ENTER counted as `reject_count` with reasons.
  `WATCH` is **not supported** by `sma150_bounce` and is not forced (future work
  via the Phase 4 strategy interface).
- **Stage 4 — Expensive data gate.** A documented, DISABLED no-op hook
  (`enable_expensive_stages=false`). No 4H / FMP-heavy calls are added in Phase 3.

### Telemetry (persisted in `pattern_runs.notes`)

`scanner_version`, `pattern_code`, `config_summary`, `started_at`/`finished_at`/
`runtime_seconds`, `universe_count`, `stage_counts`
(`stage_0_universe`, `stage_1_liquidity_passed`, `stage_2_prefilter_passed`,
`stage_3_evaluated`, `enter_count`, `watch_count`, `reject_count`),
`rejection_reason_counts`, `sample_rejections` (capped, default 25),
`api_call_counts` (`historical_fetches`), `data_source`, `dry_run`, `notes`.
Per-symbol logs are intentionally NOT stored in bulk; only a capped sample. A
dedicated `scan_rejects` table remains a documented future option, not added now.

## Scanner configuration

Strategy thresholds come from the pattern config (`min_liquidity_filters`,
`min_price`, `score_threshold`). Minimal scanner-level defaults live in
`funnel.DEFAULT_SCANNER_CONFIG`:

- `max_universe_size = 500`
- `sample_rejections_limit = 25`
- `allow_unknown_volume = false`
- `enable_expensive_stages = false`
- `scanner_version = "funnel_v1"`

## How to run a safe validation scan

**Dry run (no FMP, no writes)** — recommended first. Runs Stages 0-1 and returns
telemetry synchronously:

```bash
curl -s -X POST http://localhost:8000/api/admin/scan/start \
  -H "Content-Type: application/json" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  -d '{"scanner_mode":"funnel","dry_run":true,"limit":25}' | jq .
```

**Small live funnel (bounded FMP)** — only after you approve FMP usage. `limit`
caps how many liquidity survivors get a history fetch:

```bash
curl -s -X POST http://localhost:8000/api/admin/scan/start \
  -H "Content-Type: application/json" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  -d '{"scanner_mode":"funnel","dry_run":false,"limit":10}'
```

Keep `ENABLE_SCHEDULER=false` during validation. The legacy scan is still the
default (`scanner_mode` omitted or `"legacy"`).

## How Phase 3 connects to Phase 2 outcome tracking

Funnel ENTER signals are written through the same `save_signal` pipeline into the
`signals` table (stable `id`, `symbol`, `pattern_code`, `score`, `reason`,
`details`, `snapshot_date`). Phase 2's `get_signals_needing_outcomes` therefore
picks them up automatically — no glue code. `side` is **not** invented for
`sma150_bounce`; outcome tracking defaults it to LONG, consistent with the
long-only ENTER semantics. A clean per-strategy `side`/stop/target contract is
deferred to the Phase 4 strategy interface.

## What is still NOT proven

- **No alpha claim.** The funnel improves scan discipline and observability, not
  signal quality. Value is only demonstrable via Phase 2 outcomes on a real
  sample.
- **Not run end-to-end on real data.** All stages are verified on synthetic data
  via unit tests; the funnel has not been executed against the live universe +
  FMP. (Carried validation debt: the controlled scan smoke is still pending.)
- **Universe depends on a fresh ticker cache.** `get_universe_tickers` reads
  whatever `refresh_tickers_cache` last stored; a stale/empty cache yields a
  small universe.

## What remains before Wyckoff

- Phase 4 strategy interface (pluggable strategies; real `side`/stop/target;
  `WATCH` verdict), then Wyckoff MTF as one strategy module. Not started.

## Tests

New (19): `test_funnel_liquidity.py`, `test_funnel_prefilter.py`,
`test_funnel_telemetry.py`, `test_funnel_scan.py`. They cover stage counting,
liquidity filtering (real values only), unknown-volume/market-cap rejection,
cheap prefilter rejection, rejection aggregation, sample cap, config used in
evaluation, no FMP call in dry_run, `limit` bounding the fetch, and telemetry
shape. All deterministic — no live FMP/Supabase. `pytest` → **75 passed**.

## Safety / scope honored

- No broad scans; dry_run is FMP-free; live funnel is bounded by `limit`.
- No scheduler changes; nothing enabled automatically.
- No Wyckoff, no LLM, no UI, no broker execution.
- No fake score, no fabricated volume/market cap.
- Legacy scan preserved; existing endpoint contract intact (default `legacy`).
- Outcome calculation logic unchanged.
