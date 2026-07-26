# Wyckoff MTF v2 — Historical Cohort Closeout Runbook

Operator sequence to CLOSE OUT the existing historical shadow-evidence cohort
(the campaigns run with historical `as_of_date` values; operationally called
"Phase 9G"). Everything here is shadow-only and read-first: the closeout audit
is strictly read-only, and maturation is the existing bounded, idempotent,
rate-limited endpoint — no new maturation path is introduced.

The stored rollout defaults never change:

```text
patterns.is_enabled = false
allow_enter        = false
enable_4h_trigger  = false
min_price          = 5.0
```

All admin calls require the worker token header:

```text
X-Worker-Token: <operator token from the deployment secret store>
```

> This runbook does not assume the current production counts. Run the read-only
> audit first to learn the real cohort state; every command below is bounded.

---

## 1. Read-only closeout audit (ALWAYS run this first)

A cohort selector is **required** — the closeout never scans all shadow
history. Supply at least one of `experiment_code`, `campaign_id`,
`strategy_version`, `config_hash`, `symbol`, `min_snapshot_date`,
`max_snapshot_date`. A bare call (only the default `pattern_code`) returns a
422 validation error by design.

Scope to the experiment (the whole historical cohort):

```bash
GET /api/admin/shadow-cohort/closeout?experiment_code=wyckoff_v2_vs_baseline
```

Scope to one campaign, or a date window:

```bash
GET /api/admin/shadow-cohort/closeout?campaign_id=<id>
GET /api/admin/shadow-cohort/closeout?experiment_code=wyckoff_v2_vs_baseline&min_snapshot_date=2026-05-01&max_snapshot_date=2026-06-30
```

Reads are bounded by `limit` (default 1000, hard cap 2000). A valid selector
that matches nothing returns a clean empty report (`total_evaluations == 0`) —
that is distinct from the 422 an absent selector produces.

The report is `shadow_cohort_closeout.v1` and includes:

* `total_evaluations`, `total_outcome_rows`;
* `outcome_status_distribution` and `matured_outcomes_by_horizon`
  (matured horizon counts for 1D/3D/5D/10D/20D);
* `eligibility.counts` — **the key distinction**: `matured`, `eligible`,
  `not_yet_eligible`, `retryable_failure`, `terminal_failure`,
  `missing_market_session_data`, `eligibility_unknown`. Eligibility is
  measured in **completed trading sessions** (from the local `daily_bars`
  calendar), never in calendar days;
* `eligible_not_yet_matured_count` vs `not_yet_eligible_count` — a
  not-yet-eligible outcome is **correct to leave alone**, not a failure;
* `unresolved_action_required_count` + a bounded `unresolved_sample`
  (eligible + retryable pairs that still need a maturation/retry pass);
* `provider_failure_count` / `provider_failure_rows` and
  `forward_fetch_error_count` / `forward_fetch_error_rows`;
* `outcome_coverage`, `missing_outcome_count`;
* duplicate detection: `duplicate_outcome_pair_count`,
  `duplicate_symbol_session_count`;
* `campaign_ids` included in the cohort;
* the reused `quality_audit` (`shadow_evidence_quality.v1`) and
  `decision_metrics` (`strategy_shadow_metrics.v2`) blocks.

Use `unresolved_sample` to drive the bounded maturation below. If
`eligibility.counts.eligibility_unknown` is non-zero, the local trading
calendar (SPY `daily_bars`) is incomplete — sync daily bars before trusting
session-based eligibility.

## 2. Bounded maturation of the eligible outcomes

Maturation is the EXISTING endpoint (do not re-implement it):

```bash
POST /api/admin/shadow/outcomes/calculate
```

It is bounded by `limit` (default 50, hard cap 200), requires at least one
selector (`pair_ids` / `symbols` / `run_id`) or `pending=true`, is idempotent
(write-once merge on the pair fingerprint — never a duplicate outcome, never a
regression of frozen horizons), and every provider call is paced by the
Massive rate limiter (5/min on Basic). It is never scheduled.

Small bounded batch by campaign chunk run (safe between scheduler windows):

```bash
POST /api/admin/shadow/outcomes/calculate
{"run_id": "<chunk run id from the cohort audit>", "limit": 50}
```

Maturation sweep of everything still pending, one bounded page at a time:

```bash
POST /api/admin/shadow/outcomes/calculate
{"pending": true, "limit": 200}
```

Target specific eligible pairs from `unresolved_sample`:

```bash
POST /api/admin/shadow/outcomes/calculate
{"pair_ids": ["<pair-uuid>", "..."], "limit": 50}
```

Successful (`complete`) outcomes are NOT recalculated by default — the default
selector only picks pairs with no outcome row or a `pending_forward_bars` /
`partial` row. Re-run step 1 after each batch; repeat only while
`unresolved_action_required_count > 0`.

## 3. Targeted `forward_fetch_error` retry (`include_recalc`)

A `forward_fetch_error` is a transient/retryable fetch failure. Re-run ONLY
the affected pair(s) with `include_recalc=true` so the write-once merge repairs
the existing error row in place (no duplicate row is created):

```bash
POST /api/admin/shadow/outcomes/calculate
{"pair_ids": ["<the forward_fetch_error pair id>"],
 "include_recalc": true, "limit": 1}
```

Or, to repair every error row inside one chunk run:

```bash
POST /api/admin/shadow/outcomes/calculate
{"run_id": "<chunk run id>", "pending": true,
 "include_recalc": true, "limit": 50}
```

`terminal_failure` rows (e.g. `reference_revision_detected`) and
`missing_market_session_data` (`snapshot_bar_missing`) are NOT retried — a
plain re-run cannot fix a frozen split/revision or a truly absent snapshot
session. Record them in the review notes instead.

## 4. Confirm the closeout

> The actual runtime cohort counts (how many campaigns, evaluations, matured
> outcomes, and whether the single `forward_fetch_error` is still present) are
> **unknown until an authorized deployment call is made**. This tooling has not
> been run against any live cohort, and no live cohort has been closed. The
> numbers below are read live by step 1 at that time.

Re-run step 1. The cohort is cleanly closed for audit when:

* `unresolved_action_required_count == 0` (no `eligible` and no
  `retryable_failure` remain);
* every remaining non-matured pair is honestly one of `not_yet_eligible`,
  `terminal_failure` or `missing_market_session_data`, each explained in the
  review notes;
* `duplicate_outcome_pair_count == 0`;
* the `quality_audit` `blocking_count == 0`.

Export the evidence package for the record with the existing endpoint:

```bash
GET /api/admin/shadow-evidence/export?pattern_code=wyckoff_mtf_v2
```

Store the JSON with its `content_sha256`.

## Guardrails

* This closeout never enables anything: `allow_enter` and `patterns.is_enabled`
  stay `false`, and no scheduler entry is added. The maturation endpoint writes
  only the migration-011 outcome tables + the `daily_bars` cache.
* Do NOT run a new large retrospective cohort and do NOT change the Massive
  subscription — the closeout only matures pairs that already exist.
* Every provider fetch is bounded and rate-limited; keep `limit` small
  (≤ 200) so runs finish comfortably between scheduler windows.
