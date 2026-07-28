# Wyckoff v2 — Measurement Semantics & 4H Readiness Architecture

Companion to `wyckoff-v2-prospective-experiment-runbook.md`. Resolves the signal
and outcome measurement semantics and specifies (DESIGN ONLY — no migration in
this task) the local 4H readiness store, the arm-conditioned outcome store, and a
dedicated least-privilege history-warmer.

## 1. Candidate signal state machine (traced, cited)

`evaluate_policy` (`app/workers/strategies/wyckoff_v2/policy.py:275-496`) runs gates
A→F and writes intermediate state into `strategy_shadow_evaluations.details_snapshot`.
Load-bearing persisted fields (JSON paths): `policy.setup_state`,
`policy.trigger_confirmed`, `policy.enter_eligible_without_rollout_gate`,
`policy.allow_enter`, `policy.required_gate_results.*`, `policy.waiting_reasons`,
`readiness.status`, `four_hour_trigger.state`/`.trigger_price`, `ranking.ranking_score`
(persisted, NOT a gate).

- **ENTER** (one branch, `policy.py:459-477`): `enter_structurally AND allow_enter`.
  `enter_structurally` requires readiness ready + recognized structure + phase
  eligible + **confirmed 4H trigger with trigger_price>0** + invalidation. ENTER
  therefore always implies a confirmed 4H trigger. `enter_eligible_without_rollout_gate=True`.
- **WATCH** (one return, `policy.py:479-496`): `setup_state=="valid"` but NOT
  (`enter_structurally AND allow_enter`). `enter_eligible_without_rollout_gate =
  enter_structurally`.
- **AVOID** (13 branches, `policy.py:312-391`): readiness / price / structure /
  invalidation / HTF failures. `setup_state` never "valid";
  `enter_eligible_without_rollout_gate=False`.

## 2. Exact meanings

| Term | Meaning | Persisted signal |
|---|---|---|
| setup_present | daily structure recognized, valid setup | `policy.setup_state=="valid"` |
| trigger_confirmed | completed 4H bar broke the level | `four_hour_trigger.state=="confirmed"` |
| pre_rollout_enter_eligible | passed every gate except (possibly) the rollout gate | `policy.enter_eligible_without_rollout_gate==true` |
| rollout_blocked | would-ENTER, blocked ONLY by allow_enter=false | `enter_eligible==true AND allow_enter==false` |
| ENTER | all gates incl. rollout passed (impossible while allow_enter=false) | `verdict=="ENTER"` |
| WATCH | valid setup, not a final ENTER — **ambiguous, must be decomposed** | `verdict=="WATCH"` |

## 3. WATCH decomposition (never one signal)

`reason_code` is always `watch_setup_valid` and does NOT distinguish; classify from
persisted fields (`v2` `candidate.watch_classification`):

| Class | Test | Meaning |
|---|---|---|
| `trigger_confirmed_rollout_blocked` | `enter_eligible_without_rollout_gate==true AND allow_enter==false` (`is_rollout_blocked`) | the strategy's true "would-enter" — this is the primary signal |
| `trigger_confirmed_other` | `four_hour_trigger.state=="confirmed"` but not eligible (phase/entry-ref gate failed) | confirmed trigger, still not enterable |
| `valid_setup_trigger_unconfirmed` | `setup_state=="valid" AND enter_eligible==false` | waiting for the 4H trigger |
| `watch_other` | residual | (none currently produced) |

WATCH is **not** partial evidence (→ AVOID) and **not** a threshold-edge score
(ranking never gates). All sub-states are fully determinable from persisted
`details_snapshot`.

## 4. v1 actionable-definition flaw & the v2 correction

**Flaw:** `shadow_paired_*.v1` exposed `candidate_actionable = WATCH or ENTER or
pre-rollout`, collapsing materially different states into one signal.

**v2 correction (implemented, read-only):** contracts bumped to
`shadow_paired_comparison.v2` / `shadow_paired_metrics.v2`. The bare `actionable`
field is REMOVED. Instead:

- `candidate_signal_definition = "pre_rollout_enter_eligible.v1"` (the recommended
  primary signal — the would-enter decision while `allow_enter=false`).
- Per-row candidate flags: `setup_present`, `trigger_confirmed`,
  `pre_rollout_enter_eligible`, `rollout_blocked`, `final_enter`, `watch`,
  `watch_classification`, `primary_signal` (= pre_rollout).
- Separate populations: `candidate_setup_population`, `candidate_trigger_population`,
  `candidate_pre_rollout_entry_population`, `candidate_rollout_blocked_entry_population`,
  `candidate_final_enter_population`, `candidate_watch_population`,
  `candidate_ready_population`, `control_signal_population`.
- `candidate_watch_breakdown` (counts per `watch_classification`).

No existing field's meaning was silently changed (version bumped).

## 5. Shared market-path (Concept A) vs arm-conditioned (Concept B)

- **Concept A — `shared_market_path_outcome`:** the forward movement of the symbol
  after the common frozen snapshot. **One outcome per pair** (`strategy_shadow_pair_outcomes`
  is PAIR-level, `UNIQUE(pair_id)`, no `arm_code`; reference_price is the
  verdict-neutral snapshot close). Identical for candidate and control. Descriptive
  of *market opportunity*, i.e. **selection quality**.
- **Concept B — `arm_conditioned_outcome`:** return measured from an arm-specific
  ENTRY event (entry price + timestamp), may differ between arms, may include
  stop/target execution. **Requires arm-specific entry semantics.**

The v2 contracts label outcomes `concept: shared_market_path_outcome`,
`shared_across_arms: true`, `arm_conditioned_available: false`.

## 6. Corrected paired-metric semantics (v2)

For SHARED outcomes the surface does **not** emit `candidate_return − control_return`
(definitionally ~0; it is in `prohibited`). Instead it reports
`selection_conditioned_market_path_metrics` — the market-path return distribution
for each SELECTION population: `candidate_selected` (primary signal),
`control_selected`, `both_selected`, `candidate_only`, `control_only`,
`neither_selected`, `unconditional`. These are **selection-quality** analyses, not
strategy P&L. For `both_selected` the forward returns are IDENTICAL for both arms
by construction — a zero cross-arm difference is explicitly **not** evidence of
equivalence.

## 7. Arm-entry field audit & conclusion (Part 6)

| Field | Candidate `wyckoff_mtf_v2` | Control `sma150_bounce` |
|---|---|---|
| entry_price | persisted-but-optional (= `four_hour_trigger.trigger_price`, only on ENTER/confirmed) | not-available (adapter sets None) |
| trigger_price | persisted-but-optional (confirmed only) | not-available |
| entry_timestamp/session | persisted (`latest_completed_4h_end`/`_session_date`) | not-available (arm-specific) |
| stop_price | not-available (v1 → None) | not-available |
| target_price | not-available (v1 → None) | not-available |
| holding start | derivable (trigger session) | shared snapshot only |

**Conclusion: arm-conditioned outcomes are NOT derivable today without a schema
change and without fabrication.** The control has no entry/trigger/stop/target
concept at all; the candidate has an authoritative entry reference only on ENTER
(impossible while `allow_enter=false`), lacks stop/target, and its persisted
returns are measured from the daily snapshot close (not the 4H entry). **Arm-specific
outcome persistence is required** for a true entry-conditioned comparison.

## 8. Proposed `strategy_shadow_arm_outcomes` (DESIGN ONLY — do not migrate)

```sql
-- Migration NNN_shadow_arm_outcomes.sql  (NOT created in this task)
CREATE TABLE IF NOT EXISTS public.strategy_shadow_arm_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pair_id UUID NOT NULL REFERENCES public.strategy_shadow_pairs(id) ON DELETE CASCADE,
  evaluation_id UUID NOT NULL REFERENCES public.strategy_shadow_evaluations(id) ON DELETE CASCADE,
  arm_code TEXT NOT NULL,                 -- candidate_wyckoff_v2 | control_baseline
  strategy_code TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  entry_semantics_version TEXT NOT NULL,  -- e.g. wyckoff_4h_trigger_entry.v1
  outcome_horizon TEXT NOT NULL,          -- 1d|3d|5d|10d|20d
  entry_eligible BOOLEAN NOT NULL,
  entry_confirmed BOOLEAN NOT NULL,
  entry_price NUMERIC,                    -- NULL when not entry-confirmed (never fabricated)
  entry_timestamp TIMESTAMPTZ,
  entry_source TEXT,                      -- four_hour_trigger | none
  stop_price NUMERIC,
  target_price NUMERIC,
  exit_or_horizon_price NUMERIC,
  gross_return NUMERIC,
  benchmark_return NUMERIC,
  relative_return NUMERIC,
  mfe NUMERIC, mae NUMERIC,
  stop_hit BOOLEAN, target_hit BOOLEAN,
  status TEXT NOT NULL CHECK (status IN ('entry_ineligible','pending','complete','error')),
  error_code TEXT, error_message TEXT,
  calculation_version TEXT NOT NULL,
  first_calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (pair_id, arm_code, outcome_horizon, entry_semantics_version, calculation_version)
);
CREATE INDEX ... ON strategy_shadow_arm_outcomes (arm_code, strategy_code, outcome_horizon);
CREATE INDEX ... ON strategy_shadow_arm_outcomes (pair_id);
CREATE INDEX ... ON strategy_shadow_arm_outcomes (status);
```

- **Unique / immutability:** bounded upsert on the UNIQUE tuple (recalculation
  bumps `calculation_version`); never overwrites `strategy_shadow_pair_outcomes`
  (shared market path is retained separately).
- **RLS:** enable; audit reader gets a full-row SELECT policy; the arm-outcome
  maintainer gets campaign-scoped INSERT/UPDATE (same predicate style as the
  existing outcome maintainer) and NO DELETE/DDL. Owner-run policy script,
  rerunnable, fail-closed.
- **Audit-reader privileges:** SELECT only. **Maintainer privileges:** SELECT on
  read relations + INSERT/UPDATE on `strategy_shadow_arm_outcomes` only.
- **Backfill:** compute arm outcomes for existing pairs where the arm has
  authoritative entry semantics (candidate ENTER/confirmed-trigger only); leave
  `entry_eligible=false`/`status=entry_ineligible` (NULL prices) where not — never
  fabricate. Control rows are `entry_ineligible` until sma150 gains entry semantics.
- **Failure semantics:** provider/compute failure → `status=error` + safe
  `error_code`; retryable via recalculation (bump `calculation_version`).
- **API contracts:** extend `shadow_paired_metrics` to `v3` (arm-conditioned) ONLY
  once this table exists; keep v2 (shared-path) contract for descriptive analysis.

## 9. Existing 4H data-flow audit & local-store confirmation

4H bars come from `MassiveProvider.get_intraday_history` (`massive.py:325`) via a
single `get_aggs(symbol,4,"hour",…)` call at shadow-runner
`_build_4h_frame_for_symbol` (`runner.py:183`), window
`FOUR_HOUR_FETCH_CALENDAR_DAYS=30` (+1). Fetched **live per evaluation**; only
frame metadata (hash + bounded counts) is persisted in `details_snapshot` — **raw
4H bars are never stored**. **Confirmed: no local 4H bar table exists** across all
migrations; `daily_bars` is the only local bar table. One request retrieves all
required 4H history for a symbol (no pagination, `limit=50000`, ≥ the 11-bar need,
cap 240).

## 10. Proposed `market_bars_4h` local store (DESIGN ONLY)

```sql
-- Migration NNN_market_bars_4h.sql  (NOT created in this task)
CREATE TABLE IF NOT EXISTS public.market_bars_4h (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,
  bar_start TIMESTAMPTZ NOT NULL,         -- UTC; exchange-session aligned
  bar_end TIMESTAMPTZ NOT NULL,
  open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL,
  close NUMERIC NOT NULL, volume NUMERIC NOT NULL,
  is_completed BOOLEAN NOT NULL,          -- only completed bars count toward readiness
  provider TEXT NOT NULL DEFAULT 'massive',
  provider_adjustment TEXT,               -- split/adjustment basis
  source_timestamp TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  content_fingerprint TEXT NOT NULL,      -- sha256 of OHLCV → correction detection
  UNIQUE (symbol, bar_start)
);
CREATE INDEX ... ON market_bars_4h (symbol, bar_start DESC);
CREATE INDEX ... ON market_bars_4h (symbol, is_completed);
```

- **Completed-bar rule:** a 4H bar is completed only when its `bar_end` ≤ the last
  completed exchange session boundary (mirror `frames_4h` session-cut logic);
  partial trailing bar `is_completed=false`, excluded from readiness counts.
- **Timezone/session:** store UTC; align to NY exchange sessions; regular-session
  bars only (match the frame builder).
- **Split/adjustment:** store `provider_adjustment`; a changed `content_fingerprint`
  for an existing `(symbol,bar_start)` is a CORRECTION → upsert new values +
  retain a correction note; never silently diverge.
- **Duplicate handling:** UNIQUE(symbol,bar_start) → idempotent upsert.
- **Retention:** keep ≥ candidate lookback (≥ ~90 calendar days of 4H comfortably
  covers 11+ completed bars; retain more for regime spread). **Indexing:** as above.

## 11. Dedicated history-warmer (DESIGN ONLY — do not create role/mode/migration)

A NEW least-privilege role `smart_scanner_history_warmer` + app mode
`HISTORY_WARMUP_ONLY_MODE` (mutually exclusive with audit/maintenance modes). It
must NOT reuse the outcome-maintainer role (which has no market-data write grant
by design).

- **Grants (minimum):** INSERT/UPDATE on `daily_bars` and `market_bars_4h`
  (upsert), INSERT/UPDATE on a new `history_warmup_runs` bookkeeping table, SELECT
  on universe/strategy-config read relations. **NO** access to campaigns,
  evaluations, pair outcomes, arm outcomes; **NO** strategy execution; **NO**
  scheduler; **NO** DDL/DELETE/TRUNCATE.
- **Endpoints (warmup-only mode):** `access-check` (proves role + mode + provider +
  scheduler-off), `preflight` (universe lock + history-readiness hash + provider
  budget estimate), `POST warmup/execute` (bounded symbol batch; server-enforced
  pacing + advisory lock + idempotent replay; per-run telemetry; failure/retry
  plan), mirroring the maintenance-execute contract shape (cooldown, advisory
  lock, no-secret telemetry).
- **Pacing:** server-enforced ≥ the Massive minute window; batch ≤ 3–5 symbols;
  cooldown persisted (same pattern as maintenance). Idempotent replay via a
  request/batch hash; a repeat with unchanged inputs is a no-op.
- Not implemented/deployed here (requires a new role + tables + migration).

## 12. Provider-budget estimates (Massive Basic ≈ 5 req/min)

One request retrieves all required 4H for one symbol, and one request retrieves
daily for one symbol (local-first + incremental top-up). Benchmarks (SPY, QQQ)
are 2 shared daily requests for the whole universe. Per symbol = 1 daily + 1 4H = 2.

`base_requests = 2N + 2`; `worst_case = 2×base` (one retry each). At a safe pacing
of ~15 s/request (4/min, margin under the 5/min cap):

| N | base req | base @5/min | worst req | worst @15s pacing |
|---|---|---|---|---|
| 10 | 22 | ~4.4 min | 44 | ~11 min |
| 25 | 52 | ~10.4 min | 104 | ~26 min |
| 50 | 102 | ~20.4 min | 204 | ~51 min |
| 100 | 202 | ~40.4 min | 404 | ~101 min |

Safe cooldown: ≥12 s between request starts (60/5); recommend 15 s. Worst-case time
above assumes one retry per request at 15 s spacing. (No provider call made here.)

## 13. `shadow_prospective_readiness.v2` design (with local 4H)

Per symbol adds, to v1: `completed_4h_count`, `oldest/latest_4h_timestamp`,
`candidate_4h_ready` (≥11 completed 4H bars), `data_freshness` (latest completed
bar age per timeframe), and a four-state `readiness_state` per timeframe:
`unknown_no_local_storage` | `not_ready_insufficient_count` | `stale_latest_bar_too_old`
| `ready`. Aggregate adds: `four_hour_ready_count`, `fully_launch_ready_count`
(daily+weekly+monthly+4H all ready & fresh), `four_hour_manifest_hash`, plus the
v1 hashes. v2 replaces v1's "4H not locally verifiable" blocker with a real
`market_bars_4h`-backed count once that table exists.

## 14. Frozen-universe selection prerequisites (do not freeze yet)

Before choosing a ~50-symbol universe: local daily readiness (≥~504 completed
sessions), local 4H readiness (≥11 completed 4H bars, fresh), liquidity/price
minimum (≥ sma150 `min_price` 5.0), exchange eligibility, symbol activity status,
provider support (`supports_intraday_history`), unique-symbol count (no duplicate
share classes), and sector / market-cap concentration limits. **Proposed process:**
(1) draw a candidate pool from liquid, provider-supported, price-eligible symbols;
(2) run the warmer to populate local daily + 4H; (3) run `prospective-readiness.v2`
until 100% both-ready + 4H-ready + fresh; (4) apply concentration caps and dedupe
share classes; (5) freeze → pin `universe_hash`/`config_hash`/`readiness_manifest_hash`/
`4h_manifest_hash`. **Do not hardcode symbols from the historical cohort.** The
final universe is selected only AFTER the 4H-readiness mechanism exists.
