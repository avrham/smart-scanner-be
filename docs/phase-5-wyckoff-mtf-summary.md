# Phase 5 — Wyckoff MTF v1 (summary)

Phase 5 adds `wyckoff_mtf`: a **deterministic** multi-timeframe strategy plugin
behind the Phase 4 Strategy interface. It evaluates monthly → weekly → daily and
(optionally) 4H, using measurable rules only. There is **no subjective chart
interpretation, no LLM, and no fake confidence score.**

**This phase does not prove alpha.** It produces fewer, explainable candidates
whose value must still be measured by Phase 2 outcome tracking. Wyckoff is one
plugin among strategies — not the product.

---

## What changed

| File | Purpose |
| --- | --- |
| `app/workers/timeframes.py` | Pure resampling: normalize daily OHLCV; daily→weekly (`W-FRI`) and daily→monthly (`ME`) preserving open=first/high=max/low=min/close=last/volume=sum. Never fetches data. |
| `app/workers/strategies/wyckoff/structure.py` | `monthly_bias` (LONG/SHORT/NEUTRAL) and `weekly_alignment` (aligned + rough phase). Pure. |
| `app/workers/strategies/wyckoff/events.py` | `detect_daily_setup` (spring/utad/sos/sow/range breakout/breakdown) and optional `four_hour_trigger`. Pure. |
| `app/workers/strategies/wyckoff/strategy.py` | `WyckoffMTFStrategy` orchestrator returning a `StrategyResult`; `DEFAULT_CONFIG`. |
| `app/workers/strategies/wyckoff/__init__.py` | Package exports. |
| `app/workers/strategies/registry.py` | Registers `wyckoff_mtf` (in addition to `sma150_bounce`). |
| `app/workers/strategies/base.py` | Added `Strategy.default_config()` and `Strategy.min_daily_bars` (used by the funnel). |
| `app/workers/strategies/sma150_adapter.py` | Declares `min_daily_bars = 200` (behavior unchanged). |
| `app/workers/scanner/funnel.py` | Resolves the strategy first, uses `strategy.default_config()` for the config resolver, and sizes the bounded history fetch + cheap prefilter to `strategy.min_daily_bars`. No sma150 import. |
| `app/db/migrations/004_phase5_wyckoff_mtf_config.sql` | Registers the pattern **disabled** + its config. Additive & idempotent. |
| `tests/test_wyckoff_mtf.py` | Deterministic tests (see below). |
| `tests/test_funnel_scan.py` | Doubles updated for the strategy-aware funnel. |

---

## Deterministic rules implemented

### Monthly bias (`monthly_bias`)
Requires ≥ `monthly_min_bars` (24). Using SMA20 and its slope over
`monthly_slope_lookback` (3):
- **LONG** — close > SMA, slope > 0, and not 3 consecutive lower lows.
- **SHORT** — close < SMA, slope < 0, and not 3 consecutive higher highs.
- **NEUTRAL** otherwise → **REJECT**.

### Weekly alignment (`weekly_alignment`)
Requires ≥ `weekly_min_bars` (26). Rough phase from price side vs SMA20 and slope
sign: markup / markdown / distribution / accumulation / unknown.
- **LONG aligned** — monthly LONG + weekly slope up + phase ∈ {accumulation, markup}.
- **SHORT aligned** — monthly SHORT + weekly slope down + phase ∈ {distribution, markdown}.
- Not aligned → **REJECT**.

### Daily setup (`detect_daily_setup`)
Range measured over `daily_range_lookback` (60) bars **excluding** the current
bar; range height must be ≥ `min_range_atr_multiple` × ATR (else rejected as
noise). Current bar is the trigger:
- **LONG** — `spring` (pierce below range low by ≥ `pierce_atr_multiple`×ATR then
  close back inside), `sos` (close > range high with volume ratio ≥
  `min_breakout_volume_ratio`), else `range_breakout` (close > range high).
- **SHORT** — `utad`, `sow`, `range_breakdown` (mirror image).
- A setup that contradicts the side is forced to `none`. `none` → **REJECT**.

### 4H trigger (`four_hour_trigger`) — optional
Only used when `enable_4h_trigger` is true **and** 4H data is injected via
`StrategyContext.data_meta["df_4h"]`. LONG triggers when the last 4H close breaks
the prior local high (SHORT: prior local low). Sets `entry_price` (trigger
close), `stop_price`/`invalidation` (local swing). `target_price` stays null in
v1.

### Decision
Valid monthly + weekly + daily + `structure_score ≥ score_threshold`:
- `require_4h_for_enter` true (default): **ENTER** only if a 4H trigger fired,
  otherwise **WATCH**.
- `require_4h_for_enter` false: **ENTER** on the daily setup alone.

---

## Subjective concepts intentionally deferred (OUT OF SCOPE)

- LPS / LPSY (last point of support/supply) — too dependent on subjective
  labeling of prior events in v1.
- Effort-vs-result / relative-volume nuance beyond a single breakout ratio.
- Composite-operator narrative, cause-and-effect count objectives, exact
  phase-substep labeling (Phase A–E).
- Automatic deterministic `target_price` (left null unless a rule is added).

---

## Config keys (`DEFAULT_CONFIG` / migration 004)

`monthly_sma_window`, `monthly_min_bars`, `monthly_slope_lookback`,
`weekly_sma_window`, `weekly_min_bars`, `weekly_slope_lookback`,
`daily_range_lookback`, `atr_window`, `min_range_atr_multiple`,
`pierce_atr_multiple`, `volume_sma_window`, `min_breakout_volume_ratio`,
`trigger_lookback_4h`, `enable_4h_trigger`, `require_4h_for_enter`,
`score_threshold`, `min_price`, `min_daily_bars`, `min_liquidity_filters`.

DB config (via `pattern_configs`) overrides these through the Phase 1 resolver.
sma150 config is untouched.

---

## Integration

- **Strategy interface (Phase 4):** `WyckoffMTFStrategy.evaluate(df, context)`
  returns a `StrategyResult` with `decision/side/score/reason/rejection_reason/
  details/score_components/required_timeframes/entry_price/stop_price/
  target_price/invalidation/setup_type/strategy_version`.
- **Funnel (Phase 3):** selectable via `pattern_code="wyckoff_mtf"`; evaluated
  through the registry. Funnel remains **opt-in and non-default**; expensive 4H
  stages stay disabled, so via the funnel wyckoff returns at most WATCH (nothing
  is saved). `dry_run` still makes zero FMP calls and no DB writes. `limit` still
  bounds the history fetch, now sized to wyckoff's deep-history need.
- **Outcome tracking (Phase 2):** when a wyckoff signal is created it carries
  `side`, `entry_price`, `stop_price`, `invalidation`, `setup_type`, and a
  timeframe summary in `details`, making outcomes more useful. Outcomes are still
  **not** calculated automatically.

---

## How to run a safe validation scan

```bash
# FMP-free, no writes — cheap stages only:
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pattern_code":"wyckoff_mtf","scanner_mode":"funnel","dry_run":true,"limit":25}' | jq .
```

A non-dry-run funnel scan for wyckoff fetches deep daily history for the bounded
survivors and will emit at most WATCH (4H disabled). Apply migration 004 first if
you want DB-authoritative config; flip `patterns.is_enabled` to true only when you
deliberately want it scanned.

---

## Tests (deterministic, no live FMP/Supabase)

`tests/test_wyckoff_mtf.py` covers: daily→weekly/monthly resampling; monthly
LONG/SHORT/NEUTRAL/insufficient; weekly alignment pass/fail; spring/sos/utad/sow
detection + none-inside-range + side-consistency guard; 4H trigger present/absent;
strategy WATCH (no 4H) vs ENTER (4H trigger enabled); monthly-neutral and
insufficient-daily rejections; raw score components; registry inclusion; funnel
evaluates wyckoff via the registry (WATCH, deep-history fetch sizing, nothing
saved); funnel dry-run makes no FMP calls.

---

## What remains unproven / not done

- **No proof of alpha.** Requires Phase 2 outcomes accumulating vs. baselines.
- 4H entries are untested against a live intraday feed (no FMP intraday endpoint
  exists yet). ENTER via wyckoff currently needs externally injected 4H data.
- Weekly phase and swing-structure rules are coarse v1 heuristics; LPS/LPSY and
  richer Wyckoff logic are deferred.
- No scheduler, no UI, no broker execution, no LLM were added.
