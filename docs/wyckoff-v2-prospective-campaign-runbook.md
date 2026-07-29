# Wyckoff v2 — Prospective Campaign Runbook (frozen-universe, local-only)

Bounded prospective campaign pipeline: register + execute exactly one
candidate (`wyckoff_mtf_v2`) vs control (`sma150_bounce`) evaluation per frozen
symbol, using ONLY locally persisted daily + 4H bars pinned to a completed US
market session. NO provider calls, NO forward outcomes, NO strategy-math change.

## Registration contract — `prospective_campaign_registration.v1`
Immutable identities pinned at registration: experiment_code
(`wyckoff_v2_vs_baseline`), experiment_contract_version
(`wyckoff_v2_prospective_experiment.v1`), frozen universe
(id/code/version/hash), history_config_hash, history_readiness_manifest_hash,
candidate (code `wyckoff_mtf_v2` / version `wyckoff_mtf.v2` / signal
`pre_rollout_enter_eligible.v1` / **allow_enter=false**), control (`sma150_bounce`
/ `sma150.v2`), snapshot_session_date, snapshot_cutoff_at, market_calendar_version.
Persisted in `prospective_campaign_registrations` (migration 017), RLS-enabled.
Status: draft→registered→executing→completed→failed.

## Completed-session resolution (`us_market_calendar.v1`)
`app/prospective_session.py`: rule-based NYSE calendar (weekends + observed
federal market holidays + Good Friday). A session is FULLY completed only
at/after the regular 16:00 America/New_York close (early-close days count only
after 16:00 — conservative, can only exclude the current day). The current
in-progress session is never used. `snapshot_cutoff_at` = 16:00 ET of the
snapshot session, as UTC.

## Universe + history pinning
The server independently loads the frozen universe by `universe_id`, recomputes
its hash from membership and proves it equals the pinned hash; recomputes the
history readiness manifest (`shadow_prospective_readiness.v2`) and requires all
symbols both-ready. Registration/execution reject any drift
(stale_universe / stale_history_manifest / history_not_ready).

## Candidate/control identities
Reuses the closed experiment `WYCKOFF_V2_VS_BASELINE`: arm codes
`candidate_wyckoff_v2` / `control_baseline`. The candidate primary signal is
`enter_eligible_without_rollout_gate` (pre-rollout); a fully-eligible setup with
`allow_enter=false` surfaces as final **WATCH** with waiting reason
`enter_disabled_shadow_only` — WATCH is NOT reinterpreted as an entry.

## Local-only data access + lookahead prevention
`app/prospective_local_provider.LocalHistoryProvider` is injected into the reused
`run_shadow_campaign`/`run_shadow_comparison` in place of a market-data provider.
It reads ONLY `daily_bars` (trading_date ≤ snapshot_session_date) and
`market_bars_4h` (is_completed AND bar_end ≤ snapshot_cutoff_at). No provider is
ever constructed; no network access. Future bars in the DB are excluded by the
cutoff barrier (proven: a seeded future daily/4H bar never enters a pair frame).

## Pair + evaluation identity
Each pair is a `strategy_shadow_pairs` row (symbol, snapshot_date=session,
market_data_as_of, frame_hash = daily manifest fingerprint, pair_fingerprint
that also binds the 4H frame hash) linked to the campaign run; reference price =
close of the last completed daily bar ≤ cutoff (in the frame snapshot). The
campaign/universe/snapshot binding + reference are reconciled in the audit +
registration. Exactly one candidate + one control evaluation per pair
(`strategy_shadow_evaluations`, UNIQUE (pair_id, arm_code)).

## Idempotency + locking
- `registration_identity` (`pcr:` sha256) = experiment + universe + config +
  snapshot ⇒ duplicate registration returns the existing id (`already_registered`).
- `campaign_execution_identity` (`pcx:` sha256) binds registration + hashes +
  snapshot. Execute is `pg_try_advisory_lock(0x50524F53)`-guarded; identical
  completed replay returns `already_applied` (same campaign_id, same pair/eval
  counts, no new writes, no provider); a concurrent execute returns 409
  `prospective_campaign_execution_locked`.

## Crash recovery
The registration row is the durable marker (status `executing` + lease). On
re-execute under the lock: if the campaign run already fully persisted (pairs +
both arms per symbol) → reconcile to `completed` with no re-run; else re-run
(pair-fingerprint dedup + UNIQUE(pair_id,arm_code) make it idempotent — no
duplicate pairs/evaluations). No permanently `executing` row (lease expiry).

## Failure taxonomy
retryable: (none provider-side — no provider). terminal/operator:
stale_universe, stale_history_manifest, history_not_ready, local_4h_stale,
invalid_snapshot_session, duplicate_registration_conflict,
prospective_campaign_execution_locked, candidate_evaluation_error,
control_evaluation_error, pair_persistence_error, evaluation_persistence_error.
A partial campaign is marked `failed` (`campaign_count_mismatch`), never completed.

## Audit — `prospective_campaign_audit.v1`
`GET /api/admin/prospective/audit` reconciles registration↔campaign↔pairs↔
evaluations + descriptive candidate/control distributions (readiness, decisions,
setup/trigger/pre-rollout-entry/rollout-blocked counts, WATCH classifications,
control ENTER count, both-signal intersection, 4H states). Always
`provider_called=false`, `outcome_count=0`.

## Prospective-only mode
`PROSPECTIVE_CAMPAIGN_ONLY_MODE=true` connects via `PROSPECTIVE_DATABASE_URL` as
`smart_scanner_prospective_runner`; mutually exclusive with the audit/maintenance/
warmup modes; scheduler disabled; no provider credential. Allowlist: GET
health/version/access-check/preflight/audit + POST register/execute only.

## Outcome-maturation handoff
This pipeline creates NO forward outcomes. The persisted campaign (25 pairs, 50
evaluations) is the frozen prospective decision snapshot; outcome maturation is a
SEPARATE future task (forward daily bars after the snapshot → returns/excursions),
never run here. The candidate signal for analysis is
`enter_eligible_without_rollout_gate` (pre_rollout_enter_eligible.v1).

## Operator workflow (isolated cloud)
1. `GET /api/admin/prospective/access-check` (expect ready, provider_constructed=false).
2. `GET /api/admin/prospective/preflight?universe_id=<frozen>&experiment_code=wyckoff_v2_vs_baseline`
   → note snapshot session + hashes.
3. `POST /api/admin/prospective/register` with the pinned hashes/snapshot.
4. `POST /api/admin/prospective/execute` with the registration id + pinned hashes.
5. `GET /api/admin/prospective/audit` → verify 25 pairs, 25+25 evaluations, 0 outcomes.
6. Replay register + execute → already_registered / already_applied. Stop.
