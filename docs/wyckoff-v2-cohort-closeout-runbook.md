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

## 1a. Bounded maturation PLAN (build the exact manifest before any mutation)

```bash
GET /api/admin/shadow-cohort/maturation-plan?experiment_code=wyckoff_v2_vs_baseline
```

This read-only endpoint (contract `shadow_maturation_plan.v1`) is the bridge
between the closeout report and the mutation endpoint. It is worker-token
protected, allowed in `AUDIT_ONLY_MODE`, uses the same fail-closed audit access
gate as closeout, and requires at least one cohort selector (never all-history).

### Why the closeout counts were complete but its unresolved IDs were sampled

The closeout `eligibility.counts` are exact — every one of the 504 evaluations
is classified. But its `unresolved_sample` is deliberately capped at 200 rows
(`unresolved_sample_truncated=true` when more exist) so a large cohort can never
grow the response without bound. The exact **counts** are trustworthy; the
enumerated **IDs** are only a sample. You therefore cannot drive a mutation from
the closeout body alone.

### Why an exact pair-ID manifest is required before mutation

A maturation run mutates outcome rows. To keep it bounded, provable and
reversible-in-reasoning, you must feed the calculation endpoint the EXACT set of
`pair_ids` it will touch — not a broad selector that might sweep unrelated pairs.
The plan endpoint returns that complete, de-duplicated, deterministically ordered
manifest (ordering `snapshot_date_asc_symbol_asc_pair_id_asc`) plus a stable
`manifest_hash`, so the pair set is fixed and auditable before and after the run.

### Why `pending=true` must NOT be used unless cohort isolation is proven

`POST /api/admin/shadow/outcomes/calculate` supports only `pair_ids` / `symbols`
/ `run_id` / `pending` selectors — **NOT** `experiment_code` or `campaign_id`.
Its `select_pairs_for_outcomes` applies the pending status predicate with no
experiment or strategy filter, so `pending=true` (without a narrowing selector)
sweeps EVERY incomplete pair across ALL experiments and strategies — cross-cohort
leakage. Never use `pending=true` for cohort maturation unless you have proven
isolation (e.g. only this experiment has any incomplete pairs). Use the explicit
`pair_ids` manifest instead.

### How to request every manifest page

The manifest is paginated (`limit` ≤ 500, `offset`). The 329-pair cohort fits in
one page, but always confirm completeness by paging until `has_more=false`:

```bash
GET .../maturation-plan?experiment_code=wyckoff_v2_vs_baseline&limit=500&offset=0
# then, while has_more: offset = next_offset
GET .../maturation-plan?experiment_code=wyckoff_v2_vs_baseline&limit=500&offset=500
```

### How to verify the combined count and the manifest hash

* Concatenate every page's `eligible_manifest`; assert the unique `pair_id`
  count equals `manifest_total` and equals `eligible_unmatured_count`
  (the authoritative eligibility count). No pair may appear twice; none may be
  missing.
* `manifest_hash` is computed over IMMUTABLE identity only (cohort identity +
  per-pair `pair_id` / `snapshot_date` / strategy + experiment identity),
  canonicalized so it is **identical across page sizes and row order**. Record it
  before maturation and re-request the plan afterwards: an unchanged hash proves
  the cohort identity set did not drift. Changing any pair id or the cohort
  identity changes the hash.

### How duplicate symbol-sessions are interpreted

The plan classifies every duplicate `(symbol, session)` group:

* `benign_cross_campaign_overlap` — two DISTINCT pair IDs from two DIFFERENT
  legitimate campaigns. **Does not block** maturation: overlapping campaign
  windows can legitimately evaluate the same symbol on the same session, and the
  two distinct pairs each mature independently.
* `duplicate_within_same_campaign` / `duplicate_within_same_run` /
  `identity_mismatch` / `unverifiable` — **block** maturation
  (`safe_to_execute=false`), because they indicate double-counting, drift or
  unattributable records that must be investigated first (never merged or
  deleted from here).

### Why the audit staging app cannot execute maturation

`smart-scanner-be-staging` is intentionally audit-only: `AUDIT_ONLY_MODE=true`
(only the read-only allowlist is exposed — the calculate route returns 404), the
database connects through the SELECT-only `smart_scanner_audit_reader` role, and
there are no provider credentials. The plan endpoint states this explicitly in
its `planning.cannot_execute_reason`. It PLANS; it never matures.

### Prerequisites for a later maintenance-only execution environment

Maturation must run in a SEPARATE, short-lived maintenance environment with:

* a write-capable but still bounded database role (INSERT/UPDATE only on the
  migration-011 outcome tables + `daily_bars` cache — never the audit reader);
* a real Massive provider credential (Basic = 5 requests/min);
* `ENABLE_SCHEDULER=false` (never background work);
* a mutation-route allowlist that exposes ONLY the calculate endpoint;
* worker-token authentication;
* the exact `manifest_hash` recorded from this plan.

Do not build that environment as part of planning.

### Exact separation between normal maturation and `include_recalc`

* **Normal maturation** (`planning.requires_include_recalc=false`): feed the
  `eligible_manifest` `pair_ids` in batches. These are pairs with no outcome row
  yet — `include_recalc` is neither needed nor used.
* **Targeted recalculation** (`retry_plan`, `requires_include_recalc=true`): the
  retryable failures are EXISTING error rows and are kept OUT of the eligible
  manifest. They are repaired separately with `include_recalc=true`, one bounded
  request, so a normal batch never silently reprocesses a frozen row. Terminal
  failures (`retryable=false`) are never retried.

Recommended order when both are needed: (1) normal eligible maturation →
(2) read-only plan/closeout → (3) targeted `include_recalc` retry →
(4) final read-only plan/closeout, re-checking the manifest hash.

### Cohort scope: `campaign` vs `experiment` (REQUIRED)

`GET /api/admin/shadow-cohort/maturation-plan` now REQUIRES an explicit
`cohort_scope` (omitting it returns 422 — the endpoint never silently
reinterprets a bare call as the executable manifest). Two deliberately-limited
values:

```bash
# The EXECUTABLE campaign maturation manifest (campaign-linked records only):
GET .../maturation-plan?experiment_code=wyckoff_v2_vs_baseline&cohort_scope=campaign
# The broad read-only experiment-evidence view (manual/legacy records included):
GET .../maturation-plan?experiment_code=wyckoff_v2_vs_baseline&cohort_scope=experiment
```

**Experiment evidence and campaign evidence are different scopes.** A pair is
*campaign-linked* only when at least one persisted linked run carries a VALID
`telemetry.campaign` block — validated on `campaign_id`, `experiment_code`
(must match the cohort experiment) and `as_of_date`. Campaign membership is
NEVER inferred from symbol, snapshot date, nearby campaigns, requested-symbol
overlap, run creation time, or strategy identity alone. A pair legitimately
linked to more than one campaign is kept ONCE in the manifest with all campaign
ids reported — cross-campaign reuse is not a blocker.

`cohort_scope=campaign` excludes valid manual `/shadow-run` records and legacy
non-campaign evidence into a separate `excluded_non_campaign_evidence` block —
those records remain fully visible for audit, are never deleted or relabelled,
and never block campaign maturation. Only *campaign-intended* pairs whose
telemetry is invalid/ambiguous (`campaign_conflicting_eligible_count > 0`) block
the campaign scope. `cohort_scope=experiment` keeps every eligible pair and
therefore stays `safe_to_execute=false` while any non-campaign membership
exists; it must NOT be used as the execution manifest.

**Why the old 329-pair manifest is invalid for campaign execution.** The two
manual AAPL/MSFT pairs (`62438e8b-…` and `fb01b6d2-…`, snapshot 2026-07-23,
classified `manual_non_campaign_shadow_run` by the lineage audit) are valid
experiment evidence with no campaign telemetry by design. The old
experiment-wide 329 manifest mixed them in and was `safe_to_execute=false`; its
v1 hash `sha256:65eb5471…` no longer describes any executable manifest. The
campaign scope excludes exactly those two → a **327-pair** manifest with a new
scope-stamped v2 hash.

**Requesting every page and verifying.** Page with `limit`≤500 and `offset`;
page until `has_more=false`; assert the combined unique `pair_id` count equals
`manifest_total` == `campaign_eligible_unmatured_count`. The `manifest_hash`
(`shadow_maturation_manifest_hash.v2`) includes the scope, is identical across
page sizes, and differs between `campaign` and `experiment` scopes so the two
manifests can never be confused. Excluded non-campaign records never affect the
campaign manifest hash. Record the new hash before any maturation and re-check
it after.

`pending=true` remains FORBIDDEN (it is not cohort-isolated — see below).
Normal eligible maturation and the `include_recalc` retry remain separate. The
execution prerequisites (a write-capable maintenance environment with a bounded
write role, a Massive credential, scheduler disabled, a mutation-route
allowlist, worker-token auth, and the recorded manifest hash) remain UNBUILT.

### Progressive locked maturation (stable cohort lock vs dynamic remaining)

The initial eligible-manifest hash CANNOT be a fixed execution lock across
batches: after the first 10 outcomes complete they leave the eligible-unmatured
set, the count drops 327→317 and the manifest hash changes — so a protocol that
required the original hash for later batches would stall after batch 0. The
maintenance protocol therefore separates three identities (all from the same
pure builder, exposed by the audit planner AND the maintenance preflight):

* **Stable cohort lock** — `cohort_lock_hash` (`shadow_maturation_cohort_lock.v1`),
  `cohort_pair_count`. Covers EVERY campaign-linked pair of the cohort
  regardless of outcome status (complete + eligible + retryable ≈ 502), over
  immutable identity only (`pair_id, symbol, snapshot_date, experiment_code,
  experiment_version, strategy_code, strategy_version, decision_policy_version,
  config_hash, sorted campaign_ids`). It NEVER changes on a valid normal/retry
  outcome write; it changes only if a campaign pair is added/removed or an
  identity/membership field changes. Excluded manual/non-campaign records never
  affect it. Installed as the `MAINTENANCE_LOCKED_COHORT_HASH` secret after
  independent audit verification.
* **Dynamic remaining manifest** — `remaining_manifest_hash`,
  `remaining_pair_count`. Only the normal campaign pairs still needing
  maturation (starts at 327 / `sha256:f8947f83…`). It MUST change after every
  successful batch: 327 → 317 → … → 0.
* **Deterministic next batch** — `next_batch` = the first slice of the CURRENT
  remaining manifest (`snapshot_date_asc_symbol_asc_pair_id_asc`) with
  `next_batch_hash` over `cohort_lock_hash + remaining_manifest_hash + ordered
  pair_ids + batch_size + mode`. Fixed `batch_index` into the shrinking manifest
  is no longer used.

**Every batch requires a FRESH preflight.** The v2 execute request
(`shadow_maintenance_execute.v2`) carries `cohort_lock_hash` +
`remaining_manifest_hash` + `next_batch_hash` + the exact current `pair_ids`;
the server recomputes all three (before AND again under the advisory lock) and
rejects any drift or arbitrary slice. Progression: preflight → execute the
returned `next_batch` → preflight again (new remaining hash + new next_batch,
same cohort lock) → repeat until `normal_execution_complete=true` /
`next_batch.available=false`.

**Stop on stable cohort drift.** If `cohort_lock_hash` ever differs from the
recorded `MAINTENANCE_LOCKED_COHORT_HASH`, the access-check/preflight/execute
report `cohort_lock_drift` — stop and investigate; do NOT overwrite the lock.
**Replay:** re-sending a just-completed batch (stale remaining/next-batch hash,
cohort lock unchanged, its pairs now complete) returns `already_applied` with no
provider call; a partially-complete batch returns 409 `stale_partial_batch`.
**Retry** (`include_recalc`, server-set) is permitted only once
`normal_execution_complete=true`, still gated on the stable cohort lock. The
manifest-hash the audit planner surfaces as `manifest_hash` remains the campaign
remaining hash (alias of `remaining_manifest_hash`).

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
