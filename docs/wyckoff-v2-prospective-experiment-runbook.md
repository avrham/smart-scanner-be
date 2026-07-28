# Wyckoff v2 Prospective Experiment & Analytical Surface Runbook

Contract: **`wyckoff_v2_prospective_experiment.v1`**. Prepares a *valid* prospective
Wyckoff v2 (candidate `wyckoff_mtf_v2`) vs SMA150 (control `sma150_bounce`)
experiment, and documents the read-only analytical surface added to the audit
app. This task adds **no** campaign, **no** provider call, **no** strategy change.

## 1. Why the first cohort was descriptive-only

The first fully-matured cohort (`wyckoff_v2_vs_baseline`, 502 campaign pairs,
502/502 complete) received verdict **READY_ONLY_FOR_DESCRIPTIVE_AUDIT**:

- **Candidate insufficient history on 502/502 campaign pairs.** wyckoff_mtf_v2's
  readiness gate was not met on any campaign pair; the 2 `readiness=ready`
  records are the excluded manual non-campaign records, not campaign pairs.
- **`allow_enter=false` (rollout gate).** The candidate STRUCTURALLY cannot emit
  `ENTER` in the shadow config (default false, migration-seeded, override
  forbidden). Its maximum verdict is `WATCH`. So `503 AVOID / 1 WATCH / 0 ENTER`
  and `trigger_confirmed_count=0`, `score_sample_count=1` are **operational
  defaults / data artifacts**, not investment conclusions.
- A candidate that produced ~0 valid actionable signals cannot be compared to
  the control on outcomes. No outperformance claim is supportable from it.

## 2. Effective history requirements (resolved — Part 2)

Weekly and monthly frames are **resampled from the daily frame**
(`aggregation.py`, pandas `W-FRI` / `ME`), so daily depth drives all three
gates. A trailing partial period is dropped (`_period_is_complete`).

| Strategy (version) | Min completed daily bars | Min weekly periods | Min monthly periods | Min completed 4H bars | Binds first |
|---|---|---|---|---|---|
| candidate `wyckoff_mtf_v2` (`wyckoff_mtf.v2`) | **175** (`range_max 120 + range_end 20 + atr 14 + vol_baseline 20 + margin 1`) | **26** | **24** | 11 (trigger; 4H enabled in shadow) | **monthly 24** |
| control `sma150_bounce` (`sma150.v2`) | **200** (`sma_window 150 + 50`) | — | — | — | 200-bar gate |

- **175 vs 540 reconciliation:** `540` is the **v1** strategy (`wyckoff_mtf`) hard
  `min_daily_bars` gate (migration 004) — **legacy, v1 only**. v2 replaced the
  single 540-bar gate with the three completed-period gates above. `200` is v2's
  coarse prefetch hint (`min_daily_bars`, not a readiness gate) and the control's
  hard gate. `~562` is v2's data-**request** target (`desired_history_bars`,
  monthly 24×23 + margin), not a gate.
- **True binding constraint (candidate):** 24 completed monthly periods ≈
  **~504 completed trading sessions ≈ ~730+ calendar days (~2 years+)**. This
  dwarfs the 130-session weekly and 175-bar daily gates. **"200 daily bars are
  enough" is FALSE for the candidate** — 200 bars is < 10 months and cannot yield
  24 completed months.
- **Practical daily floor to launch both arms:** ≥ **~504** completed daily
  sessions (candidate monthly gate) AND ≥ 200 (control) → the candidate's monthly
  gate governs.
- **4H history is fetched live; there is NO local 4H bar table.** 4H readiness
  cannot be confirmed from local data — it is a launch blocker that must be
  resolved by provider pre-caching.

## 3. Prospective experiment contract (`wyckoff_v2_prospective_experiment.v1`)

```
experiment_code            wyckoff_v2_prospective_experiment (new; NOT registered here)
candidate                  wyckoff_mtf_v2 / wyckoff_mtf.v2 / policy wyckoff_mtf.policy.v1
control                    sma150_bounce / sma150.v2
frozen_universe            explicit symbol list, frozen at launch; universe_hash (sha256 of sorted uppercased symbols)
strategy_config_hash       config_hash of each arm's resolved config (candidate override enable_4h_trigger=true only)
history_readiness_hash     readiness_manifest_hash from shadow_prospective_readiness.v1 at freeze time
campaign_cadence           scheduled sessions spanning multiple market regimes (NOT adjacent duplicate sessions)
snapshot_rules             one frozen daily+4H snapshot per (symbol, session); market_data_as_of pinned
market_session_rules       completed-session (ny_session_close.v1) frames only; no partial trailing bar
pair_identity              one strategy_shadow_pairs row per (symbol, snapshot); shared pair_fingerprint; both arms persisted
candidate_setup_detected   policy.setup_state == "valid"
candidate_trigger_confirmed four_hour_trigger.state == "confirmed"
candidate_enter_eligible   policy.enter_eligible_without_rollout_gate == true  (pre-rollout signal)
candidate_final_verdict    ENTER | WATCH | AVOID (ENTER impossible while allow_enter=false)
actionable_signal          candidate: final verdict in {ENTER,WATCH} OR pre-rollout enter-eligible.
                           control: verdict in {ENTER,WATCH} (sma150 emits ENTER/AVOID only)
rollout_gate_treatment     see §7 (recommended Option A: keep allow_enter=false, use pre-rollout eligibility)
entry_price_semantics      MUST be defined before any trade-conditioned claim (currently undefined → signal-level only)
benchmark_identity         SPY/QQQ (already fetched per pair); benchmark_returns + relative_returns per horizon
outcome_horizons           1D, 3D, 5D, 10D, 20D
duplicate_symbol_controls  cap repeated observations per symbol within an adjacent-session window; cluster by symbol
campaign_stop_conditions   any readiness regression, universe-hash drift, provider 429 storm, or history-blocker
minimum_actionable_sample  see §9
success_criteria / failure_criteria  see §9
```

The contract **distinguishes** setup-detected → trigger-confirmed →
enter-eligible-before-rollout → final verdict. Candidate actionability is **NOT**
defined as final `ENTER` while `allow_enter=false`.

> **v2 update (measurement semantics):** the paired contracts are now
> `shadow_paired_comparison.v2` / `shadow_paired_metrics.v2`. The broad
> `actionable` field is removed; the versioned primary signal is
> `candidate_signal_definition = pre_rollout_enter_eligible.v1`, and WATCH is
> decomposed (`watch_classification`). Outcomes are labelled as a SHARED market
> path (Concept A); no candidate−control strategy-return difference is emitted.
> See `wyckoff-v2-measurement-and-4h-readiness-runbook.md` for the full signal
> state machine, arm-conditioned-outcome (Concept B) design, local `market_bars_4h`
> store, history-warmer role, provider budget, and `prospective_readiness.v2`.

## 4–6. Analytical surface (read-only, audit-app allowlist)

New GET endpoints (all read-only, GET-only so the audit-only method gate holds;
they reuse the frozen evidence/outcome readers — no new SQL, no schema change):

- `GET /api/admin/shadow-cohort/paired-comparison` → **`shadow_paired_comparison.v1`**.
  Bounded, cursor-paginated pair-level dataset. Filters: `experiment_code`,
  `campaign_id?`, `campaign_scope`, `symbol?`, `strategy_version?`, `config_hash?`,
  `horizon?`, `decision_population?`, `cursor`, `limit`. Per pair: identity +
  candidate {verdict, readiness_status, setup_present, trigger_confirmed,
  pre_rollout_enter_eligible, rollout_blocked, score, four_hour_frame_state,
  actionable}, control {verdict, score, actionable}, outcome {status, ret_1d..20d,
  benchmark_returns, relative_returns, MFE, MAE, stop/target (null unless
  genuinely present)}, structure flags. **Null semantics:** a null return /
  benchmark / stop / target means not present in the stored outcome — NEVER
  coerced to zero.
- `GET /api/admin/shadow-cohort/paired-metrics` → **`shadow_paired_metrics.v1`**.
  Symmetric population counts + per-population, per-horizon effect sizes and
  (min-sample-gated) paired sign / Wilcoxon / t / bootstrap statistics.
- `GET /api/admin/shadow-cohort/prospective-readiness?symbols=...` →
  **`shadow_prospective_readiness.v1`**. Local-only history readiness (below).

**Symmetry & reconciliation (Part 6).** The new surface fetches BOTH arms
(`wyckoff_mtf_v2` and `sma150_bounce`) — unlike the closeout which aggregated
only the candidate. It returns: `raw_pair_rows`, `raw_candidate_evaluation_rows`,
`raw_control_evaluation_rows`, `valid_paired_rows`, `missing_candidate_rows`,
`missing_control_rows`, `duplicate_candidate_rows`, `duplicate_control_rows`,
`missing_outcome_rows`, `excluded_manual_rows`, with bounded pair-id samples. No
malformed record is silently dropped — each is counted and sampled.

**Denominator distinction (must be stated with every rate):**

| Denominator | Meaning |
|---|---|
| pair rows | one `strategy_shadow_pairs` row per (symbol, snapshot) |
| candidate evaluation rows | `wyckoff_mtf_v2` arm rows (= pairs + any manual candidate-only records) |
| control evaluation rows | `sma150_bounce` arm rows |
| outcome rows | one matured outcome per pair (shared by both arms) |
| excluded manual rows | manual non-campaign records (retained, outside campaign arithmetic) |

**Shared-outcome caveat.** One matured outcome per pair is shared by both arms, so
a *raw* candidate−control return difference is definitionally ~0. A meaningful
paired difference requires an **entry-conditioned** model the current experiment
does not define. The surface therefore reports population counts + per-arm
descriptive, actionable-conditioned returns; it does not manufacture a spurious
paired difference. Inferential stats are suppressed below `MIN_INFERENTIAL_SAMPLE=30`.

## 7. Prospective readiness workflow & rollout-signal option

`shadow_prospective_readiness.v1` evaluates a proposed frozen universe against
**local `daily_bars` only** (no provider call). Per symbol: available completed
daily bars / weekly periods / monthly periods, oldest/latest, candidate
daily/weekly/monthly-ready, control-ready, both-ready, 4H (not locally
verifiable → blocker), blocking reasons, missing counts. Aggregates: universe
size, candidate/control/both-ready counts, readiness %, history-depth
distribution (min/p10/median/p90/max + required threshold + count meeting), and
stable `universe_hash` / `config_hash` / `readiness_manifest_hash`.

**Rollout-signal options (recommend A):**

- **Option A (RECOMMENDED): keep `allow_enter=false`; measure the pre-rollout
  eligibility signal `enter_eligible_without_rollout_gate`.** No production
  trading risk; requires no strategy/rollout change; the candidate's actionable
  set is defined by setup+trigger+eligibility rather than a gated final `ENTER`.
  This is the smallest, safest change that yields a non-degenerate actionable
  population. The paired-comparison surface already exposes
  `pre_rollout_enter_eligible` / `rollout_blocked` / `actionable`.
- **Option B: a controlled shadow-only `allow_enter=true` candidate arm** (no
  order execution). Yields native `ENTER` verdicts but requires changing the
  experiment's forbidden-override rule and rollout config — larger surface, more
  review, and not needed to answer the outperformance question.

**Recommendation: Option A.** It is sufficient (the pre-rollout signal is the
strategy's true "would enter" decision), reversible, and touches no rollout/
strategy code. Neither option is enabled in this task.

## 8. Launch gates (all must pass before a prospective campaign is created)

1. 100% of the frozen universe **both-strategy ready** (`both_ready_count == universe_size`).
2. 100% required daily/weekly/monthly completed frames (candidate monthly-24 gate met for every symbol).
3. 100% required completed 4H availability — **verified via provider pre-cache**, since 4H is not locally stored (this gate cannot pass on local data alone).
4. 0 unresolved history blockers (`not_ready_count == 0`).
5. 0 duplicate universe symbols.
6. Stable `universe_hash`, `config_hash`, `readiness_manifest_hash` (recorded at freeze).
7. Audit `paired-comparison` / `paired-metrics` / `prospective-readiness` endpoints healthy.
8. Scheduler disabled during manual launch.
9. Provider budget documented (Massive Basic 5 req/min; pre-cache daily+4H; pace).

**A symbol that is history-ineligible must be EXCLUDED before the universe is
frozen** — not carried into the campaign. Symbols may be excluded before freeze;
**after freeze the universe must not mutate silently** (universe_hash pins it).

## 9. Sampling & power (design targets — not total evaluated rows)

Do **not** use total evaluated rows as the sample target (the first cohort's 504
were ~all not-ready). Targets are on the ACTIONABLE population:

| Target | Value |
|---|---|
| candidate actionable observations | **≥ 100 (min), ≥ 200 preferred** |
| both-actionable intersection (population F) | ≥ 30 (min inferential sample) |
| candidate setup-present | tracked; expect ≥ candidate-actionable |
| candidate trigger-confirmed | tracked (>0 required to prove the trigger path works) |
| distinct symbols | ≥ 40 |
| distinct campaign sessions | ≥ 6 (regime spread) |
| market regimes | ≥ 2 |
| max repeated obs per symbol / adjacent-session window | ≤ 2 |

These are proposed design targets; refine from an assumed effect size + variance
once a pilot yields a variance estimate. **Cluster by symbol, campaign/session
and market regime** — repeated observations of the same symbol are NOT
independent and must not be treated as such. Inferential claims across the 5
horizons apply the Bonferroni family size (`bonferroni_family_size=5`), and
always report effect sizes + denominators, not p-values alone. Significance is
not, by itself, strategy validation.

## 10. Portfolio-claim limitations

The prospective experiment is **decision-support research, not automated
trading**. Supported vs prohibited claims:

- **Signal-level forward-return comparison** — supported (per-arm, per-horizon,
  actionable-conditioned, with denominators).
- **Entry-conditioned trade comparison** — only if the contract defines entry
  price + timestamp.
- **Portfolio backtest / live trading performance** — NOT supported.

Unless the contract defines ALL of {entry price, entry timestamp, position
sizing, capital allocation, overlapping-position handling, transaction costs,
slippage, stop execution, target execution, cash treatment, benchmark
allocation}, the system MUST NOT report: portfolio return, Sharpe ratio, capital
growth, realized portfolio drawdown, or strategy P&L. Pair-level forward
outcomes are not automatically a backtest.

## 11. Post-maturation analytical workflow

1. Mature the prospective cohort via the existing bounded maintenance path
   (3-pair batches, 75s cooldown), then retries.
2. `GET closeout` (completeness), then `GET paired-comparison` (reconcile arms),
   then `GET paired-metrics` (population counts FIRST, then effect sizes).
3. Report population counts before any performance. If population F (both
   actionable) or D (candidate actionable) is empty/small, say so directly —
   READY_ONLY_FOR_DESCRIPTIVE_AUDIT again.
4. Only claim outperformance when population F ≥ the min sample, denominators are
   defensible, and the effect survives multi-horizon correction.
