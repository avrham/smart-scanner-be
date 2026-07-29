# Wyckoff v2 — 4H Persistence & History-Warmer Foundation Runbook

Foundation ONLY. No provider-backed execute, no provider client, no live fetch,
no campaign. Adds a canonical local 4H bar store, a least-privilege history-warmer
role/mode, read-only access-check + preflight, and `shadow_prospective_readiness.v2`
backed by local daily + 4H data.

## 1. Canonical 4H bar semantics (Option A — provider-native adjusted bars)

Massive returns **split/dividend-adjusted** (`adjusted=true`) provider-native 4H
aggregates as **epoch-ms UTC bar starts** (`get_intraday_history` → single
`get_aggs(...,4,"hour",...)`, no pagination). The existing frame builder
(`frames_4h.py`) already consumes those NATIVE boundaries, assigns a session date
from the bar END in `America/New_York`, applies a completed-cut, and never
synthesizes missing bars. Persisting provider-native adjusted bars and letting
that existing layer reapply the session/completed cut is the **least-drift**
choice (Option A) — the strategy's behavior is unchanged.

Definitions:
- `bar_start` / `bar_end`: provider-native UTC bounds (`bar_end = bar_start + 4h`).
- `session_date`: exchange (`America/New_York`) date of `bar_end`.
- `exchange timezone`: `America/New_York`.
- `is_regular_session`: whether the bar sits in the 09:30–16:00 ET regular
  session — **provenance only, NOT used in the readiness gate** (the strategy
  consumes all provider bars, no regular-session filter), so it cannot drift.
- `is_completed`: the bar has fully closed and does not end in the future
  (write-time invariant; a `now()` CHECK is not immutable). Only completed bars
  count toward readiness.
- `provider_adjustment`: `split_dividend_adjusted` (adjusted=true) vs
  `unadjusted` — part of the uniqueness identity so a future unadjusted feed
  cannot silently collide.

**Early close / holidays:** the final 4H bar of a shortened session is a shorter
observed bar; missing bars are never synthesized (matches `frames_4h`). Readiness
counts OBSERVED completed bars.

**11-bar readiness rule:** candidate 4H-ready ⟺ **completed 4H bar count ≥ 11**
(`trigger_lookback_4h(10) + 1`).

## 2. `market_bars_4h` schema (migration `014_market_bars_4h.sql`)

Columns: `id`, `symbol` (upper), `bar_start`/`bar_end` (tz-aware, `bar_end >
bar_start`), `session_date`, `open/high/low/close` (NOT NULL, OHLC CHECK),
`volume` (≥0), `is_completed`, `is_regular_session`, `provider` (∈ massive),
`provider_adjustment` (∈ split_dividend_adjusted|unadjusted), `source_timestamp`,
`ingested_at`, `updated_at`, `content_fingerprint`. **Uniqueness:** `UNIQUE
(symbol, bar_start, provider, provider_adjustment)`. **Indexes:**
`(symbol, is_completed, bar_start DESC)` (latest completed + count),
`(symbol, session_date)` (range + manifest), `(bar_start)` (oldest/newest).
RLS enabled by the migration.

## 3. Idempotency & correction model (upsert; smallest provenance)

Repeated ingestion is an **upsert on the uniqueness identity**: an identical
replay is a no-op; a provider CORRECTION (changed OHLCV / source_timestamp /
completion) updates the canonical row's values + `updated_at` +
`content_fingerprint` (sha256 of canonical OHLCV) — never a duplicate row.
**Tradeoff:** chosen the smallest design (single canonical row + fingerprint +
updated_at) over an immutable-version audit table — it preserves correction
detectability for reproducibility without an unbounded raw-payload archive. If
full version history is later required, add a bounded `market_bars_4h_history`
audit table; not needed for the foundation.

## 4. `history_warmup_runs` (bookkeeping only)

`id`, `mode`, `status` (planned|running|completed|failed|cancelled),
`universe_hash`, `readiness_manifest_hash`, `requested_symbols` (jsonb),
`requested_symbol_count`, `processed_symbol_count`, `provider_request_count`,
`idempotency_key` (UNIQUE), `cooldown_last_finished_at`,
`cooldown_next_not_before`, `error_code`/`error_message`, `started_at`/
`finished_at`/`created_at`/`updated_at`. **Never** stores provider keys, DSNs,
tokens or raw payloads. Populated by a FUTURE warmup execute (not this task).

## 5. History-warmer role (`ops/sql/create_shadow_history_warmer.sql`)

`smart_scanner_history_warmer`: LOGIN, NOINHERIT, NOSUPERUSER, NOCREATEDB,
NOCREATEROLE, NOREPLICATION, NOBYPASSRLS; `default_transaction_read_only = off`;
bounded timeouts. Distinct from — never reuses — the outcome-maintainer or audit
reader. RLS policies in `create_shadow_history_warmer_rls_policies.sql`.

## 6. Privilege matrix

| Relation | owner | audit_reader | outcome_maintainer | history_warmer |
|---|---|---|---|---|
| daily_bars | all | SELECT | SELECT | SELECT, INSERT, UPDATE |
| market_bars_4h | all | SELECT | — (none) | SELECT, INSERT, UPDATE |
| history_warmup_runs | all | SELECT | — | SELECT, INSERT, UPDATE |
| patterns / pattern_configs | all | SELECT | SELECT | SELECT |
| strategy_shadow_evaluations | all | SELECT | SELECT | — (none) |
| strategy_shadow_pairs / run_pairs / runs | all | SELECT | SELECT | — |
| strategy_shadow_pair_outcomes | all | SELECT | SELECT, INSERT, UPDATE | — |
| strategy_shadow_outcome_runs | all | SELECT | SELECT, INSERT, UPDATE | — |
| DELETE / TRUNCATE / DDL anywhere | owner only | never | never | never |

No role receives DELETE via grant or RLS. RLS policies: audit reader SELECT-only;
history warmer SELECT/INSERT/UPDATE on the two new tables (no DELETE policy).

## 7. `HISTORY_WARMUP_ONLY_MODE`

Defaults false; mutually exclusive with `AUDIT_ONLY_MODE` and
`MAINTENANCE_ONLY_MODE` (startup RuntimeError otherwise); requires
`ENABLE_SCHEDULER=false`; scheduler + background work disabled. Route allowlist
(`app/history_warmup_mode.py`, GET/HEAD/OPTIONS only): `/`, `/version`,
`/api/version`, `/health`, `/api/health`,
`/api/admin/history-warmup/{access-check, preflight}`. No strategy / campaign /
outcome / execute route is reachable. Testable locally (no new Fly app created).

**Connection identity (`HISTORY_WARMUP_DATABASE_URL`).** In warmup mode the app
connects using ONLY the explicit `HISTORY_WARMUP_DATABASE_URL` DSN (the dedicated
`smart_scanner_history_warmer` role), selected by
`app.audit_db.select_connection_plan` — mode `history_warmup_explicit`. It FAILS
CLOSED when the DSN is absent (never falls back to `AUDIT_DATABASE_URL`,
`MAINTENANCE_DATABASE_URL` or the Supabase-derived default identity — that
default would target the shared store as `postgres`). The DSN is a Fly secret on
the dedicated (isolated) warmup app only; never logged, never returned, never
committed. Same DSN validation/redaction as the audit/maintenance URLs.

## 7a. Isolated-environment foundation validation (this task)

The full foundation was provisioned + validated against a genuinely isolated
non-production PostgreSQL cluster (local Docker `postgres:16-alpine`, a distinct
physical cluster — different host `127.0.0.1`, different db `warmupdb`, distinct
`system_identifier`, empty of any real cohort). Cloud provisioning (a dedicated
Fly Postgres cluster + the dedicated `smart-scanner-be-history-warmup-staging`
Fly app) was intentionally DEFERRED — see §14. The shared Supabase store was
never migrated, written, or connected to.

Two bounded foundation fixes were made during isolated live validation:

1. **Warmup connection wiring** (`HISTORY_WARMUP_DATABASE_URL`, §7). Before the
   fix, warmup mode had no connection branch and fell through to the
   Supabase-derived default identity — it could not connect to the isolated DB
   as the warmer role at all.
2. **Warmer `daily_bars` RLS policies.** The live DB enables RLS on `daily_bars`
   (one of the eight audit relations). The warmer HELD the `daily_bars`
   SELECT/INSERT/UPDATE grant but, with RLS on and no warmer policy, the grant
   was inert: the warmer read ZERO daily rows (breaking readiness run as the
   warmer) and was refused writes. `create_shadow_history_warmer_rls_policies.sql`
   now adds guarded warmer SELECT/INSERT/UPDATE policies on `daily_bars` (NO
   DELETE), only when RLS is enabled there. No new privilege beyond the existing
   grant. Access-check also now exposes `foundation_ready` /
   `provider_execution_supported` / `provider_execution_ready` to make explicit
   that a missing provider credential NEVER makes the DB foundation unusable.

## 8. Access-check (`GET /api/admin/history-warmup/access-check`, `history_warmup_access_check.v1`)

Read-only privilege verdict via PostgreSQL privilege functions
(`current_user`, `to_regclass`, `has_table_privilege`). `provider_constructed`
is always **false** — the endpoint never constructs a provider. Reports `ready`,
`reasons`, `database_identity`, `history_warmup_only_mode`, `scheduler_enabled`,
`provider_name`, `provider_credential_configured`, `provider_constructed=false`,
required relations/functions/privileges, `market_bars_4h_readable/writable`,
`daily_bars_readable/writable`, `campaign_writes_forbidden`,
`outcome_writes_forbidden`, `delete_forbidden`.

## 9. Preflight (`GET /api/admin/history-warmup/preflight`, `history_warmup_preflight.v1`)

Read-only, local-data-only. Embeds `shadow_prospective_readiness.v2` per symbol +
a **provider-request ESTIMATE** (never a call): `daily_symbols_requiring_warmup`,
`four_hour_symbols_requiring_warmup`, `shared_benchmark_requests`,
`estimated_base_requests`, `estimated_worst_case_requests`,
`estimated_min_duration_seconds`, `estimated_safe_duration_seconds`.
`provider_called=false`, `provider_constructed=false`.

## 10. `shadow_prospective_readiness.v2` (audit endpoint upgraded)

Reads `daily_bars` + `market_bars_4h`. Per timeframe (daily/weekly/monthly/4H/
control) a **four-state** value: `unknown_no_local_storage` |
`not_ready_insufficient_count` | `stale_latest_bar_too_old` | `ready`. When the
4H table exists but has no rows for a symbol → `not_ready_insufficient_count`;
`unknown_no_local_storage` is reserved for the unavailable-deployment state (4H
table not present / not readable). Aggregates: both-ready / fully-launch-ready /
4H-ready / 4H-unknown / 4H-stale / not-ready counts, readiness %, per-timeframe
distributions, `universe_hash`, `config_hash`, `daily_manifest_hash`,
`four_hour_manifest_hash`, `combined_readiness_manifest_hash`, `provider_called=false`.

## 11. 4H freshness

Bounded session-date rule (pending a shared market-calendar abstraction): a
timeframe is `stale_latest_bar_too_old` when its latest completed bar is older
than N calendar days from now (daily/4H = 5, monthly = 31) — sized to absorb a
weekend + holiday. A symbol with ≥11 bars but no RECENT completed bar is STALE,
NOT launch-ready. When a market-calendar module is added, swap the calendar-day
delta for a completed-session delta.

## 12. Provider-budget estimation

Per symbol = 1 daily request + 1 4H request (each a single call retrieves the
full required window); +2 shared benchmark daily. `base = daily_needed +
fourh_needed + 2`; `worst = base×2`; rate limit 5/min; safe pacing 15 s/request.
Estimate only — the endpoint never constructs or calls the provider.

## 13. Bounded warmup EXECUTE (`POST /api/admin/history-warmup/execute`)

Implemented (`history_warmup_execute.v1`) and validated ENTIRELY against isolated
Docker Postgres + a deterministic FAKE provider — no live provider construction
or network call in this task. `app/history_warmup_execute.py` holds the pure
logic; the endpoint (`app/routers/admin.py`) mirrors the maintenance execute
pattern.

**Preflight v2** (`history_warmup_preflight.v2`) is now server-authoritative: it
returns `universe_hash`, `config_hash`, `combined_readiness_manifest_hash`,
`normal_pending_symbols`, `retryable_symbols`, `terminal_symbols`,
`normal_complete`, `retry_plan_hash`, cooldown (`execution_allowed_by_cooldown`,
`next_execution_not_before`, `cooldown_remaining_seconds`), and a server-selected
`next_batch` (`available`, `mode`, `symbol_count`, `symbols`, `next_batch_hash`,
`daily_required`, `four_hour_required`, `estimated_provider_requests`).

**Server-selected batch algorithm (retry-first).** `HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH=1`.
If any retryable item exists → next batch is the first retryable symbol in RETRY
mode (normal progression hard-stops until retry drains). Else the first
normal-pending symbol (not-ready, not-retryable, not-terminal) in NORMAL mode.
Else unavailable (`all_symbols_launch_ready` / `only_terminal_symbols_remain`).
Terminal symbols are never auto-selected — they block launch readiness and
require operator investigation. The client can NEVER widen or choose a symbol
outside the server-selected batch.

**Execute contract (`history_warmup_execute.v1`).** Body: `contract_version`,
`mode` (normal|retry), `universe_hash`, `config_hash`, `readiness_manifest_hash`
(normal) / `retry_plan_hash` (retry), `next_batch_hash`, `symbols` (exactly the
server-selected batch), `limit==len(symbols)`, `limit<=1`. Forbidden fields
(422): provider/provider_options, pacing/spacing, retry_count, table/table_name,
adjustment, from/to/start/end/date_range/timeseries, run_id, batch_index,
pending, background.

**Server-enforced sequence:** worker token → warmup mode → scheduler off →
basic shape validation → compute execution identity → completed-idempotency
short-circuit → recompute live preflight + strict hash/batch validation →
cooldown gate (before any provider) → advisory lock → double-check
(recompute/validate/cooldown/idempotency under lock) → durable pre-provider run
marker (`idempotency_key`) → provider obtained via the existing abstraction and
called OUTSIDE any open transaction → normalize + canonical daily/4H upserts →
finalize run + run item → recompute readiness → safe response. Lock released in
`finally`.

**Advisory lock:** `HISTORY_WARMUP_ADVISORY_LOCK_KEY = 0x57524D55`
(`pg_try_advisory_lock`, session-scoped, released in `finally`). A second holder
→ 409 `history_warmup_execution_locked`; no provider call or write occurs after a
lock rejection.

**Idempotency identity:** `hwx:` + sha256 of
`contract_version|mode|universe_hash|config_hash|(readiness|retry_plan hash)|next_batch_hash|sorted(symbols)`,
stored as `history_warmup_runs.idempotency_key`. Identical completed replay → 200
`already_applied`, same `run_id`, no provider call, no new bars/run/item. A
different payload yields a different identity (new request, strictly re-validated).

**Cooldown:** `compute_cooldown` (reused, pure) over the latest
`history_warmup_runs` row (precedence finished_at→updated_at→started_at→created_at)
survives restart / auto-stop / token rotation. Defaults:
`HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS=75` (floored to 60 in warmup mode on
Massive — the rolling 1-minute request window),
`HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS=15` (between a symbol's daily and
4H request, applied outside locks). Justification: one symbol = 1 daily + 1 4H
(+ bounded client retries) ≈ the Massive Basic 5/min budget, so a second batch
inside the window would be 429-throttled. An execute during cooldown → 409
`provider_cooldown_active` (+ `Retry-After`), BEFORE provider construction.

**Daily persistence:** reuses the canonical `UPSERT_DAILY_BAR_SQL`
(ON CONFLICT (symbol,trading_date)) on the warmer connection; completed sessions
only (trading_date < today UTC); inserted/updated/unchanged telemetry via a
pre-read compare; no DELETE, no raw payload, no second implementation.

**4H persistence (Option A):** provider-native adjusted bars → validate
OHLC/volume/timestamps, compute `bar_end`, `session_date` (ET of bar_end),
`is_regular_session`, `is_completed` (only completed bars stored; the forming
bucket is excluded), `content_fingerprint`; fingerprint-guarded upsert
(ON CONFLICT (symbol,bar_start,provider,provider_adjustment)) → identical replay
is a no-op, a correction updates in place (fingerprint + updated_at change, no
duplicate row). An invalid provider row → bounded `provider_invalid_payload`
(never coerced).

**Failure taxonomy** (`FAILURE_TAXONOMY`): retryable = provider_rate_limited,
provider_unavailable, provider_timeout; terminal = provider_invalid_payload,
daily_persistence_error, four_hour_persistence_error; operator_error =
provider_auth_error, stale_manifest, stale_retry_plan, stale_next_batch,
history_warmup_execution_locked, provider_cooldown_active. Auth/config are NEVER
retryable.

**Retry plan:** `history_warmup_run_items` (migration 015) records one scalar row
per (run, symbol, attempt). The retry plan = symbols whose LATEST item failed
retryably; `retry_plan_hash` changes when an item is added / completes / becomes
terminal. Retry execution is one server-selected symbol, RETRY mode, fresh
retry-plan + retry-batch hashes, same cooldown + advisory lock.

**Normal vs retry progression:** retry-first hard-stop — normal progression stops
while any retryable item exists; the operator must run retry mode. Terminal
failures block launch readiness and require investigation.

**Crash recovery** (bounded points a–d): the durable pre-provider run marker +
idempotent daily/4H upserts + `ON CONFLICT DO NOTHING` run item guarantee no
duplicate bars, items or runs on re-entry. A crashed `running` run with the same
identity is safely re-driven (the advisory lock precludes a concurrent live
attempt); a crash that persisted some bars changes readiness → the operator
obtains a fresh preflight (new identity) and re-executes, still without
duplication. Reconciliation rule: a `running` run older than the cooldown
interval with no completed twin is stale and may be marked failed by an operator;
it never blocks a fresh-identity execute.

**Success response** (`history_warmup_execute_result.v1`): contract_version,
status, mode, run_id, batch_identity, symbols, provider_request_count,
daily{inserted,updated,unchanged,completed_count},
four_hour{inserted,updated,unchanged,completed_count}, readiness_before,
readiness_after, cooldown. Never exposes provider payloads, credentialed URLs,
tokens, DSNs, passwords or raw traces.

**Provider injection:** production obtains the provider via
`get_market_data_provider()` (`app/routers/admin._resolve_history_warmup_provider`).
Tests monkeypatch that resolver with a deterministic fake (`tests/support/fake_provider.py`)
that performs no network access; a socket guard fails any non-loopback connection.
The fake is never selectable via `MARKET_DATA_PROVIDER`.

## 13b. Crash-safe execution + frozen-universe identity (migration 016)

**Frozen universes.** `history_warmup_universes` + `history_warmup_universe_symbols`
(migration 016) are the server-authoritative executable symbol sets. Lifecycle:
`draft` → `frozen` → `superseded`. `POST /api/admin/history-warmup/universes`
(`history_warmup_universe_create.v1`) creates one bounded universe (uppercased,
deduped symbols, deterministic `universe_hash = sha256(code|version|ordered
members)`, capped at `HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS`) and freezes it
atomically when `freeze:true`. **Immutability is DB-enforced** by triggers
(`history_warmup_universe_symbols_guard`, `history_warmup_universes_guard`):
once a universe is not `draft`, membership INSERT/UPDATE/DELETE is denied and the
identity/hash/count are frozen; `frozen`→`draft` is denied; `superseded` is
terminal — regardless of role, above RLS/grants. Only a `frozen` universe is
executable. The warmer may create+freeze (SELECT/INSERT/UPDATE on the universe,
SELECT/INSERT its draft membership) but never mutate frozen membership, never
DELETE. Audit reader: SELECT only. Outcome maintainer: no access.

**Preflight v3** (`history_warmup_preflight.v3`) takes `universe_id` (or
`universe_code`+`universe_version`), loads all members from the DB, recomputes
the universe hash and proves it equals the pinned hash, then returns the frozen
identity + readiness v2 + retry plan + provider cooldown + `execution_state`
(active/abandoned lease) + the server-selected next batch (≤1 symbol). A
`?symbols=` list is an EXPLICITLY NON-EXECUTABLE preview (`executable:false`, no
next batch).

**Execute v2** (`history_warmup_execute.v2`) requires `universe_id`; the server
independently loads the frozen universe, recomputes membership/hash/readiness/
retry/next-batch, and rejects unknown / draft / superseded / hash-mismatch /
membership-mismatch / symbol-not-in-universe / stale-hash — all BEFORE any
provider construction. The execution identity binds the `universe_id`.

**Provider-activity markers + fail-closed cooldown.** Before the first provider
call the run's `provider_activity_state` is set to `started` +
`provider_activity_started_at` and COMMITTED; `last_provider_activity_at` +
`provider_request_count_attempted` update around each request. The provider
cooldown source of truth is the latest run with `provider_activity_state <>
'none'`, using `last_provider_activity_at` (or `provider_activity_started_at`)
+ interval — NOT run start / `finished_at`. So a crash AFTER provider activity
keeps the cooldown fail-closed even if the run never finalizes, while a crash
BEFORE provider activity (`state='none'`) never establishes cooldown and is
immediately re-drivable. A request rejected before provider construction
(stale/locked/cooldown/auth/batch) creates no activity and never extends cooldown.

**Execution leases.** The pre-provider run marker sets
`execution_lease_expires_at = now + HISTORY_WARMUP_EXECUTION_LEASE_SECONDS`
(default 120). Same-identity request while the lease is valid → 409
`history_warmup_execution_in_progress` (with `lease_expires_at` + `Retry-After`).
Once the lease expires the run is ABANDONED and never a permanent blocker.

**Completed-replay precedence.** Identity is derived and looked up BEFORE any
stale/cooldown check: a completed (or reconciled) run with the same identity
returns `already_applied` / `reconciled_complete` (200) with no provider call —
even during active cooldown and even if readiness changed after it succeeded.
A FRESH next-batch request during cooldown returns 409 `provider_cooldown_active`.

**Abandoned-run recovery.** For an expired-lease `running` run with the same
identity (under the advisory lock): (a) if the stored symbols now satisfy the
exact requested readiness (`both_ready`), finalize `reconciled_complete` from the
persisted local bars — NO provider call (Part 15); (b) else if provider activity
occurred and the cooldown is active → 409 `provider_cooldown_active` (re-drive
after expiry); (c) else re-drive reusing the same run marker (durable idempotency
+ idempotent daily/4H upserts + `ON CONFLICT DO NOTHING` run item → no duplicate
bars/items/runs). Reconciliation never infers success from the mere presence of
bars — it requires the full `both_ready` target.

## 13a. One-symbol live cloud pilot (future — do NOT run now)

1. provision an isolated non-production cloud Postgres (Fly Postgres cluster);
2. apply migrations 001→016 and the role + RLS scripts to it;
3. deploy the dedicated `smart-scanner-be-history-warmup-staging` Fly app
   (`HISTORY_WARMUP_ONLY_MODE=true`, `ENABLE_SCHEDULER=false`,
   `HISTORY_WARMUP_DATABASE_URL` → isolated DB as `smart_scanner_history_warmer`,
   dedicated `WORKER_TOKEN`);
4. install the provider credential (`MASSIVE_API_KEY`) on that app ONLY;
5. `GET access-check` (expect `foundation_ready=true`);
6. `POST /history-warmup/universes` with `{universe_code, symbols:[...one or a few],
   freeze:true}` → note `universe_id`;
7. `GET preflight?universe_id=<id>` → note the server-selected `next_batch` + hashes;
8. `POST execute` (v2) with `universe_id` + exactly the server-selected single
   symbol + fresh hashes;
9. verify persisted daily/4H bars + `readiness_after`;
10. replay the identical body → `already_applied` (no provider call), even though
    cooldown is now active;
11. attempt a FRESH next-symbol execute → 409 `provider_cooldown_active`;
12. stop.

## 14. Migration application (deferred — see report)

The migration + role/RLS scripts are validated on isolated Docker Postgres (all
integration tests green). They are NOT applied to the shared live Supabase store
in this task: the staging audit app reads the same store that holds the real 502
cohort, so applying there would target production data — a hard-stop. Live
application requires a confirmed isolated non-production database + admin
credentials + explicit authorization. The deployed code degrades gracefully:
readiness v2 reports 4H `unknown_no_local_storage` until `market_bars_4h` exists.
