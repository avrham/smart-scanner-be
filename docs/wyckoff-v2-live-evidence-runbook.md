# Wyckoff MTF v2 — Live Shadow Evidence Runbook (Phase 9F)

Operator sequence for the FIRST authorized live evidence campaign and its
review. Everything below is shadow-only: at no point does any step enable
the strategy, and the stored rollout defaults must remain:

```text
patterns.is_enabled = false
allow_enter = false
enable_4h_trigger = false
min_price = 5.0
```

The only 4H-trigger analysis that ever runs is the frozen experiment-local
override inside `wyckoff_v2_vs_baseline` shadow runs. A confirmed trigger
can produce at most a rollout-blocked WATCH.

All admin calls require the worker token header:

```text
X-Worker-Token: <operator token from the deployment secret store>
```

Never place the token, provider keys or database credentials in this file,
in tickets, or in exported evidence.

---

## 1. Verify the deployment

```bash
git rev-parse HEAD          # must equal the reviewed/approved SHA
git status --short          # must be clean
```

Confirm `GET /health` reports the expected provider block and a connected
database.

## 2. Apply migration 013 (manual, once)

In the Supabase SQL editor, after `012_wyckoff_mtf_v2.sql`, run
`app/db/migrations/013_wyckoff_v2_shadow_arms.sql` verbatim. It only
extends the shadow arm-code CHECK constraint; it changes no data, no
defaults and no enablement. It is idempotent (safe to re-run).

Verification query (read-only):

```sql
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conname = 'strategy_shadow_evaluations_arm_code_check';
```

The definition must list `control_baseline` and `candidate_wyckoff_v2`.

If this step is skipped, wyckoff shadow persistence fails at the CHECK
constraint: runs report `pair_error` rejections and no pairs persist. That
failure is non-corrupting — apply the migration and re-run (idempotent by
pair-fingerprint dedupe).

## 3. Verify the provider

`MARKET_DATA_PROVIDER=massive` with valid credentials is REQUIRED: only
Massive serves honest bounded 4H ranges. FMP deployments record a typed
`unsupported_provider` 4H state and cannot produce trigger evidence.
Check `GET /health` → `market_data.provider == "massive"` and
`credentials_configured == true`.

## 4. Verify worker-token protection

`REQUIRE_WORKER_TOKEN=true` must be set in the deployment. Verify that an
un-tokened call is rejected:

```bash
curl -s -o /dev/null -w "%{http_code}" \
  "$HOST/api/admin/shadow-runs"          # expect 401
```

## 5. Dry-run one symbol (no persistence)

```bash
POST /api/admin/strategies/wyckoff_mtf_v2/dry-run
{"symbol": "AAPL"}
```

Expect `persisted=false`, `status="evaluated"`, rollout flags
`allow_enter=false`, `enable_4h_trigger=false`.

## 6. Shadow-run one or two symbols

```bash
POST /api/admin/strategies/wyckoff_mtf_v2/shadow-run
{"symbols": ["AAPL", "MSFT"]}
```

Expect `status="completed"`, `experiment_code="wyckoff_v2_vs_baseline"`,
`four_hour_frames_built >= 1`, `candidate_enter_count == 0`, and
`rejected_counts` free of `pair_error` (a `pair_error` here usually means
migration 013 was not applied).

## 7. Inspect the persisted pairs

```bash
GET /api/admin/shadow-runs?pattern_code=wyckoff_mtf_v2
GET /api/admin/shadow-runs/{run_id}
```

Confirm both arms persisted (`control_baseline`, `candidate_wyckoff_v2`),
the candidate details carry `_four_hour_frame_meta`, and the config
snapshot shows the frozen override (`enable_4h_trigger: true`) while the
strategy defaults remain false.

## 8. Plan and start a bounded campaign

Generate the exact payloads first (no execution):

```bash
POST /api/admin/shadow-campaign-plan
{
  "experiment_code": "wyckoff_v2_vs_baseline",
  "candidate_symbols": ["...explicit bounded list..."],
  "as_of_sessions": ["2026-07-21", "2026-07-22"],
  "max_symbols_per_campaign": 50,
  "target_unique_symbols": 50,
  "target_trigger_confirmed": 20,
  "target_matured_outcomes": 100
}
```

Then submit each returned payload verbatim:

```bash
POST /api/admin/shadow-campaigns
{"experiment_code": "wyckoff_v2_vs_baseline",
 "symbols": [...], "max_symbols": 50, "as_of_date": "2026-07-21"}
```

`max_symbols` is a required safety bound; campaigns are chunked at 25
symbols per run and never run an implicit universe.

## 9. Inspect campaign completion

```bash
GET /api/admin/shadow-campaigns
GET /api/admin/shadow-campaigns/{campaign_id}
```

Review per-symbol statuses and re-submit the SAME campaign payload for
failed chunks — retries are idempotent (identical inputs dedupe onto the
same immutable pairs).

## 10. Mature outcomes

After each horizon's trading sessions have passed (20 sessions for 20D),
run bounded outcome calculation per campaign run:

```bash
POST /api/admin/shadow/outcomes/calculate
{"run_id": "<chunk run id>", "limit": 50}
# or maturation sweeps: {"pending": true, "limit": 200}
```

Repeat until `GET /api/admin/shadow-comparison?pattern_code=wyckoff_mtf_v2`
shows the expected matured counts. Never treat a missing outcome as a zero
return — missing stays missing.

## 11. Run the evidence-quality audit

```bash
GET /api/admin/shadow-evidence/quality?pattern_code=wyckoff_mtf_v2
```

Resolve every BLOCKING issue before any readiness discussion
(`missing_db_pattern_row`, `confirmed_trigger_missing_price`). Warnings
(missing outcomes, partial campaigns, mixed versions) need explanation in
the review notes.

## 12. Run the advisory readiness policy

```bash
GET /api/admin/shadow-evidence/readiness?pattern_code=wyckoff_mtf_v2
```

The response is ADVISORY ONLY (`wyckoff_v2_rollout_readiness.v1`):
`not_ready | continue_shadow | review_required |
eligible_for_controlled_read_only_rollout`. It never enables anything and
echoes every threshold and observed value. Operator threshold overrides
are bounded query parameters; out-of-bounds overrides are rejected.

## 13. Export the evidence package

```bash
GET /api/admin/shadow-evidence/export?pattern_code=wyckoff_mtf_v2
```

Store the JSON with its `content_sha256` alongside the review. Identical
stored data + filters reproduce the identical hash.

## 14. Document the human decision

Record in the review notes: deployment SHA, campaign ids, export
`content_sha256`, the advisory status, every failed/review condition, and
the human decision (remain in shadow / collect more / revise / proceed to
a separately designed controlled read-only phase). The advisory status is
never self-executing.

## 15–16. Rollout guardrails

* `allow_enter` stays `false`. Nothing in this runbook changes it.
* `patterns.is_enabled` stays `false`; the public `/api/patterns` listing
  must continue to exclude `wyckoff_mtf_v2`.
* The production scheduler stays on `sma150_bounce`; no shadow or campaign
  scheduling may be added.

## Failure handling and rollback

* **Chunk/campaign failure**: statuses are typed per symbol; re-submit the
  same payload (idempotent). A process restart mid-campaign leaves
  completed chunks persisted; re-running the campaign resumes coverage.
* **`pair_error` rejections**: almost always the unapplied migration 013 —
  apply it and re-run.
* **Provider outage**: 4H failures are recorded as typed `fetch_error`
  states; the daily evaluation still persists. Re-run after recovery to
  create the missing 4H-bearing pairs (they get new fingerprints because
  the 4H input state is material).
* **Bad evidence discovered later**: never edit frozen rows. Record the
  issue in the quality audit review, re-collect under a new campaign, and
  filter reviews by campaign/date range.
* **Rollback**: shadow evidence is additive and isolated to
  `strategy_shadow_*` tables; production behavior does not depend on it.
  Reverting the deployment SHA is always safe; migration 013 does not need
  to be rolled back (it only widens a CHECK constraint).
