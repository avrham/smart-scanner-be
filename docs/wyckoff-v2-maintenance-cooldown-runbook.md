# Wyckoff v2 — Maintenance Provider Cooldown Runbook

Server-enforced pacing between provider-backed outcome-maturation batches on the
`smart-scanner-be-maintenance` app. This complements the progressive
locked-maturation protocol (stable `cohort_lock_hash`, dynamic
`remaining_manifest_hash`, deterministic `next_batch`).

## Why a cooldown exists — the live evidence

The Massive **Basic** plan allows only **~5 requests/minute**
(`MASSIVE_REQUESTS_PER_MINUTE=5`). A single 3-pair batch consumes roughly five
cache-cold provider requests: three pair symbols plus the shared SPY/QQQ
benchmarks (fetched once per request and cached only within that one run).

Observed on `ed7f433`, batch size 3:

- **Batch A** (MS, MSFT, NEE): 3 complete outcomes, ~2.61s, ~5 successful
  provider requests — **consumed the minute's request budget**.
- **Batch B** started **~4 seconds later** (NKE, NVDA, ORCL): every symbol hit
  **HTTP 429 across all four retry attempts**, the batch took **127.34s**, and
  all three pairs persisted as `error` with `error_code=forward_fetch_error`
  (retryable). `retryable_failure_count` went 1 → 4.

### Duration is NOT the pacing signal

Batch A finished in ~2.6s. Response duration therefore tells you nothing about
whether the provider's rolling request window is clear — a cache-warm batch can
finish fast yet still have spent the whole minute budget. **The safety signal is
the provider's rolling request window, not observed response time.** Never lower
the interval below 60s because a batch "was fast".

### Why cache warmth is misleading

The `smart_scanner_outcome_maintainer` role intentionally has **no `daily_bars`
INSERT/UPDATE grant** (least privilege). The outcome service's best-effort cache
write therefore always fails with `InsufficientPrivilegeError` and is caught —
outcome correctness uses the provider response directly; cache persistence is
non-essential in maintenance mode. Consequence: maintenance runs never warm the
read-through cache, so back-to-back batches re-fetch from the provider and are
rate-limited. This grant is **not** broadened to improve pacing.

In maintenance mode that specific, expected `daily_bars cache write failed …`
WARNING is downgraded to DEBUG so logs do not imply an outcome failure (a
maintenance-only logging filter; no grant, no cache behaviour, and no normal
deployment are affected).

## The guard

`MAINTENANCE_MIN_BATCH_INTERVAL_SECONDS` — default **75**; floored to **60**
whenever maintenance mode is active on the Massive provider; clamped to a **600**
maximum; not sensitive (no secret treatment); overridable via Fly runtime config;
ignored outside maintenance mode.

Cooldown timing is derived from the **latest persisted maintenance outcome-run
row** (`strategy_shadow_outcome_runs`), so it survives process restart, Fly
auto-stop, Machine replacement and worker-token rotation — never process memory.

- Maintenance runs are tagged distinctly: the execute route pre-creates the run
  row with a bounded `maintenance` block in `requested_selector` (set at
  creation, so it survives a crash even if the run never finalizes). Generic
  runs from the calc endpoint / scheduler carry no marker and never trigger the
  maintenance cooldown.
- **Reference-timestamp precedence:** `finished_at` → `updated_at` →
  `started_at` → `created_at`. A failed / 429-dominated batch still has one of
  these set, so it establishes cooldown exactly like a success.
- A maintenance run identifiable but somehow without a usable timestamp is
  treated conservatively as **cooldown required** — never permission to run.

### Preflight fields (`GET .../shadow-maintenance/preflight`)

```
min_batch_interval_seconds
last_execution_finished_at
next_execution_not_before
cooldown_remaining_seconds
execution_allowed_by_cooldown
```

During an active cooldown: `safe_to_execute` stays **true** (cohort + manifest
are safe), `blocking_reasons` includes **`provider_cooldown_active`**, and
`execution_available` is **false** (temporarily unavailable — distinct from
environment readiness).

### Execute behaviour (`POST .../shadow-maintenance/outcomes/execute`)

Validation order: mode/auth → stable cohort lock → current plan + exact next
batch/retry plan → **cooldown check** → advisory lock → **recompute plan +
cooldown under the lock** → provider construction → provider calls + writes.

During an active cooldown the route returns **HTTP 409**:

```json
{ "error": "provider_cooldown_active",
  "min_batch_interval_seconds": 75,
  "next_execution_not_before": "…Z",
  "cooldown_remaining_seconds": 42 }
```

with a `Retry-After` header rounded up to whole seconds. On this path: no
provider is constructed, no provider call occurs, no outcome run is created, no
outcome row is inserted/updated, no advisory lock remains held, no progression
hash changes, and no retryable count changes.

If the window reopens while a request waits on the advisory lock, the under-lock
recheck rejects with `provider_cooldown_activated_under_lock` (same safe fields),
again without constructing the provider.

**A fresh preflight is required after a cooldown elapses** (the manifest / next
batch may have moved). No operator-side batch loop may ignore
`execution_available`.

### Idempotency & retry are preserved

- A stale replay whose supplied pairs are already `complete` (stable cohort lock
  matches) returns **`already_applied`** — never blocked by cooldown, never a
  provider call, no run, no cooldown mutation.
- A partial stale batch still returns **`stale_partial_batch`**.
- Retry mode uses the **same** cooldown and still requires
  `normal_execution_complete = true`. After normal maturation reaches zero,
  retries are paced one at a time through this mechanism.

### Access-check (`GET .../shadow-maintenance/access-check`)

Reports `min_batch_interval_seconds` and
`cooldown_persistence_source = strategy_shadow_outcome_runs`.
`ready_for_maintenance_execution` is an **environment capability** verdict and is
**not** a function of the current clock — it may remain true during a cooldown.
Temporary executability lives in the preflight (`execution_available`).

## Current live state (as of this change)

```
cohort_pair_count       = 502
remaining_pair_count    = 317 normal remaining
retryable_failure_count = 4
terminal_failure_count  = 0
```

## Operating procedure

1. `GET preflight`. Proceed only if `execution_available = true`.
2. If `execution_allowed_by_cooldown = false`, wait until
   `next_execution_not_before` (respect `Retry-After` on a 409), then fetch a
   **fresh** preflight — do not reuse the old next-batch hashes.
3. Execute exactly the server-returned `next_batch.pair_ids`.

### Stop procedure on provider faults

If a batch returns any pair with `outcome_status = error` /
`error_code = forward_fetch_error` (or any non-`complete`, non-null
`error_code`): **stop immediately**. Do not start the next batch, do not retry.
The failed batch has established the cooldown; obtain a fresh preflight after the
window clears and re-evaluate. Pace multi-batch work ≥ the configured interval
(≥ 60s) apart.
