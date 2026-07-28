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

## 13. Why there is no execute endpoint yet

This foundation deliberately ships NO `POST /history-warmup/execute`, no provider
client, no background job, and no new Fly app/credentials. The NEXT task will
implement bounded provider-backed execution (server-enforced pacing, advisory
lock, idempotent replay, run telemetry) and live-test it after this foundation
passes and the migration + role are applied to an isolated non-production DB.

## 14. Migration application (deferred — see report)

The migration + role/RLS scripts are validated on isolated Docker Postgres (all
integration tests green). They are NOT applied to the shared live Supabase store
in this task: the staging audit app reads the same store that holds the real 502
cohort, so applying there would target production data — a hard-stop. Live
application requires a confirmed isolated non-production database + admin
credentials + explicit authorization. The deployed code degrades gracefully:
readiness v2 reports 4H `unknown_no_local_storage` until `market_bars_4h` exists.
