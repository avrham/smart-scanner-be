# Evidence Engine Roadmap (Authoritative)

Status: authoritative implementation roadmap. Supersedes phase planning in
`docs/evidence-engine-architecture-plan.md` for all *future* work; historical
phase summaries (Phase 1–6, 5.1, 5.2) remain the record of what was built.

Last audited against the repository at commit `5158501`
("fix timezone-aware profile freshness comparison in enrichment").

---

## 1. Product boundaries

Smart Scanner is a **decision-support system**, not a prediction engine and
not a trading bot.

Hard boundaries — these override any future feature request until explicitly
revised here:

- **No execution.** The system never places, sizes, or manages orders.
- **No free-form LLM prediction.** The LLM explains deterministic evidence;
  it never generates a direction, level, or verdict.
- **No indicator is truth.** AI Edge, Lorentzian Classification, TradingView
  alerts, or any single commercial indicator are *evidence features*. None of
  them may directly create ENTER. An ENTER can only come from a **versioned
  decision policy** that was separately researched and validated.
- **Everything must be outcome-measurable.** A strategy, filter, or external
  signal that cannot be evaluated against baselines through outcome tracking
  does not belong in the decision path.
- **Uncertainty stays uncertain.** Missing data, ambiguous structure, and
  unknown phases are first-class states, never coerced into a value.

## 2. Current implemented state (verified in code, not from plans)

Verified at commit `5158501`. Where the code differs from earlier planning
documents, this section is authoritative.

### Data layer

- **Provider abstraction** — `app/providers/base.py` defines
  `MarketDataProvider` (`sync_universe`, `get_daily_market_summary`,
  `get_daily_bars`, `get_ticker_details`, `health_check`,
  `batch_historical_data`, `get_daily_history`, `fetch_historical_4h`,
  `enrich_market_caps`). Factory in `app/providers/__init__.py`
  (`MARKET_DATA_PROVIDER`, default `massive`, `fmp` fallback,
  `ProviderConfigError` on missing keys). No runtime code outside the
  providers imports `FMPClient` directly.
- **Massive client** — `app/workers/massive_client.py`: rolling-window rate
  limiter (default 5 rpm), exponential backoff on 429/5xx, fail-fast on
  401/403, `next_url` re-authentication, API keys scrubbed from errors/logs.
- **Universe sync** — paginated `/v3/reference/tickers`, classified by
  provider `type`/`primary_exchange` fields (never ticker suffixes) in
  `app/workers/screening.py::classify_ticker`, idempotent upsert into the
  extended `tickers` table (migration 005; `eligible`, `security_type`,
  `enrichment_status`, `profile_synced_at`, ...).
- **Daily grouped ingestion** — `/v2/aggs/grouped/...` → canonical bars in
  `daily_bars` (`UNIQUE(symbol, trading_date)`), volumes propagated to
  `tickers.last_volume`.
- **Historical bars** — local-first, incremental from the latest stored
  trading date; Massive Basic ≈ 2 years history. Strategies report required
  vs. available bars; Wyckoff keeps `min_daily_bars = 540` and returns an
  insufficient-history rejection rather than silently passing.
- **Market-cap enrichment** — `MassiveProvider.enrich_market_caps`: local
  pre-screen (price/volume/dollar volume) → stale-profile filter
  (`MASSIVE_PROFILE_CACHE_DAYS`, timezone-safe comparison) → deterministic
  priority (`dollar_volume desc, volume desc, symbol asc` —
  `prioritize_enrichment`) → bounded detail calls (`max_detail_calls`).
  Missing market cap stays NULL + `enrichment_status='missing_market_cap'`.
  Telemetry: `selected_symbols` (≤25), `remaining_stale_survivors`,
  `selection_strategy`, `cached_fresh`.
  **Limitation (pre-7A): runs as a fire-and-forget FastAPI BackgroundTask,
  no durable job record, no duplicate protection, no progress visibility.**

### Scanning

- **Funnel scanner** — `app/workers/scanner/funnel.py`, opt-in via
  `scanner_mode="funnel"`. Stages: eligible universe (eligible=true,
  is_active=true, configured exchanges) → liquidity (real market cap/volume
  only; unknown ⇒ honest rejection) → cheap daily prefilter → strategy
  evaluation via the registry → optional survivor-only 4H stage. Telemetry
  persisted as JSON in `pattern_runs.notes` (stage counts, rejection reasons,
  capped samples, `market_data_provider`, provider-aware `data_source`,
  bounded `result_symbols`).
- **Legacy scanner** — `app/workers/scan_runner.py` random-batch path is
  preserved (default for the scheduler); it also resolves the provider via
  the factory.
- **4H stage** — fetched only for WATCH survivors of monthly/weekly/daily
  checks, only when `enable_expensive_stages`/`enable_4h_trigger` is on,
  bounded by the scan limit, never in dry-run.

### Strategies

- **Interface** — `app/workers/strategies/base.py`: `Strategy` ABC,
  `StrategyContext` (includes `scan_run_id`, `scanner_mode`, `data_meta`),
  `StrategyResult` (decision, side, score, `score_components` (raw values
  only), entry/stop/target/invalidation, `setup_type`, `strategy_version`).
- **Registry** — `app/workers/strategies/registry.py`; registered:
  `sma150_bounce` (adapter, version `sma150.v2`) and `wyckoff_mtf`
  (version `wyckoff_mtf.v1`, deterministic monthly→weekly→daily→4H rules).
- **Decision cards** — `app/workers/strategies/decision_card.py`: pure,
  deterministic builder from `StrategyResult` fields only (`card_version`),
  persisted at `signals.details.decision_card`.

### Persistence and API

- **Signals** — existing `signals` table (migration 001): id, symbol,
  pattern_code, verdict (ENTER/WATCH/AVOID), probability, score, reason,
  details JSONB, snapshot_date, created_at.
  **Resolved in 7B:** signals are IMMUTABLE, identified by
  `signal_fingerprint` (migration 007 replaces the destructive
  symbol/pattern/date upsert); every new signal gets a 1:1
  `signal_provenance` row (origin scan_run_id, exact
  strategy/policy/provenance versions, config snapshot+hash, bounded
  evidence snapshot + pruning metadata, market-data as-of) plus a
  `scan_run_signals` occurrence link, all written in the same transaction.
  Repeated exact detections reuse the existing signal (link only). Pre-7B
  rows remain readable as `legacy_unlinked` with NULL fingerprints.
- **Outcomes** — migration 003 `signal_outcomes` (forward returns 1/3/5/10/20D,
  MFE/MAE, stop/target hits, `calculation_version='outcome.v1'`), pure
  calculator + baselines (same-ticker buy-hold, SPY, QQQ), aggregation
  metrics, `/api/outcomes*` endpoints, admin-triggered calculation only.
  Outcomes are computed for ENTER only; WATCH is excluded.
- **Admin API** — `/api/admin/scan/start` (legacy + funnel + dry-run),
  `/tickers/refresh`, `/universe/sync`, `/market/daily-sync`,
  `/universe/enrich` (pre-7A: non-durable), `/outcomes/calculate`,
  maintenance endpoints, scan WebSocket. Worker-token protected.
- **Health** — `/health` + `/api/health` include provider name, credential
  status (boolean), rate limit, latest universe/daily sync.
- **Frontend** — `lib/api.ts::getSignals` (verdict/pattern_code/side/
  min_score/limit filters, ENTER default), `Signal.verdict` includes WATCH,
  `DecisionCard` type, signals page filter bar, drawer renders the decision
  card, WATCH visually distinct from ENTER.

### Version fields that already exist

| Concern | Where | Value |
|---|---|---|
| sma150 scoring | `details.score_version`, adapter `version` | `sma150.v2` |
| Wyckoff strategy | `strategy.py::STRATEGY_VERSION` | `wyckoff_mtf.v1` |
| Outcome math | `signal_outcomes.calculation_version` | `outcome.v1` |
| Decision card | `details.decision_card.card_version` | card builder version |
| Funnel | telemetry `scanner_version` | `funnel_v1` |
| Decision policy (7B) | `signal_provenance.decision_policy_version` | `strategy_decision.v1` |
| Provenance record (7B) | `signal_provenance.provenance_version` | `provenance.v1` |
| Fingerprint algorithm (7B) | `signals.signal_fingerprint_version` | `signal_fingerprint.v1` |
| Evidence contract (8) | `evidence_snapshot.evidence.evidence_version` | `evidence.v1` |
| sma150 v3 strategy (8) | `strategies/sma150_v3.py::STRATEGY_VERSION` | `sma150.v3` |
| sma150 v3 policy (8) | `signal_provenance.decision_policy_version` | `sma150_bounce.policy.v1` |
| sma150 v3 ranking (8) | `details.ranking.ranking_version` | `sma150.v3.rank.v1` |

These remain SEPARATE identities by design: strategy version ≠ decision-card
version ≠ outcome-calculation version ≠ decision-policy version ≠
provenance version.

### Known validation debts

- Controlled scan smoke before Phase 2 was intentionally skipped (recorded).
- No signal alpha has been demonstrated; outcome sample sizes are tiny.
- Migration 007 is written and tested but not yet applied to Supabase.

## 3. Target pipeline

```
market data (provider abstraction: Massive primary, FMP fallback)
  -> data readiness            (coverage, freshness, sufficient history — hard gates)
  -> internal strategy candidates  (sma150.vN, wyckoff_mtf.vN via registry)
  -> external observations     (TradingView / AI Edge / Lorentzian / ... webhooks)
  -> normalized evidence       (uniform evidence contract; raw + normalized + state)
  -> hard filters              (liquidity, price, cap readiness, history, staleness)
  -> decision policy           (versioned; the ONLY thing that can produce ENTER)
  -> decision card             (deterministic narrative of the evidence)
  -> signal provenance         (scan_run_id, versions, config snapshot, evidence ids)
  -> outcomes                  (forward returns, MFE/MAE, R, per-version)
  -> baselines                 (SPY/QQQ/buy-hold/momentum/sector-relative)
  -> experiments               (ablations on frozen versions + immutable configs)
  -> LLM explanation           (analyst layer only; consumes, never creates, evidence)
```

Hard filters and soft evidence are architecturally separate: a failed hard
filter can never be outvoted by soft evidence or an LLM narrative.

## 4. Required future phases

Historical phases 1–6 (plus 5.1/5.2 and the Massive migration) keep their
numbers and are not reopened.

### Phase 7A — durable market-data jobs and coverage observability *(COMPLETE, live-validated)*

- `market_data_jobs` table (migration 006): queued/running/completed/failed/
  cancelled; initially `market_cap_enrichment`. Persists provider, trading
  date, requested limit, selection strategy, selected symbols (bounded),
  progress, result summary, safe error text, timestamps.
- `/api/admin/universe/enrich` becomes job-backed: validates the provider,
  creates a queued job, returns `job_id`, runs asynchronously with state
  transitions and bounded progress updates.
- Durable duplicate protection: a partial unique index prevents two active
  (queued/running) enrichment jobs for the same provider + trading date —
  not an in-memory lock.
- `GET /api/admin/market-data/jobs/{job_id}`, `GET /api/admin/market-data/jobs`
  (bounded filters), `GET /api/admin/market-data/coverage` (local DB only —
  universe/eligibility counts, bar coverage, prescreen results, profile
  freshness, next enrichment preview, selection strategy).
- Stale-job recovery with a configurable timeout so restarts cannot leave a
  phantom `running` job blocking new work.
- Naming keeps `job_type` open for future jobs (`daily_sync`,
  `universe_sync`, `historical_backfill`) without schema changes.

**Execution semantics (7A, explicit):**

- **Job state is durable** (the `market_data_jobs` row survives restarts);
  **job execution is not** — jobs currently run as in-process FastAPI
  BackgroundTasks in the API process.
- Jobs are **not resumed automatically** after process death. A job that was
  queued or running when the process died stays in that state until recovery.
- Stale recovery (runs before every new job creation, timeout
  `MARKET_DATA_JOB_STALE_MINUTES`, default 30, timezone-aware): stale
  `queued` jobs are marked `failed` with error code `queued_job_timeout`;
  stale `running` jobs are marked `failed` with error code
  `stale_job_timeout`. Both states block the active-job unique index, so
  both must be recoverable. Recent queued/running jobs continue to block
  duplicates.
- After recovery, a **replacement job can be started safely**.
- **Partially completed enrichment remains valid**: ticker profiles are
  written per symbol, so a mid-run failure keeps everything enriched so far,
  and a rerun skips fresh profiles.
- `market_cap_enrichment` jobs always carry a **resolved, non-NULL
  trading_date** (application guard + migration CHECK constraint), because
  Postgres unique indexes treat NULLs as distinct and a NULL date would
  bypass duplicate protection.
- A **future external worker** (separate process pulling queued jobs) can
  reuse the same table and API contract unchanged; no new queue system
  (Celery/Redis/pg-boss) is introduced in this phase.

### Phase 7B — scan and signal provenance *(COMPLETE — implemented contracts below)*

Implemented (migration 007 `007_scan_signal_provenance.sql`, additive and
idempotent; no backfill of legacy rows):

**Canonical scan-run identity** — `pattern_runs` IS the canonical scan-run
table (no second identity was created). The UUID returned by
`POST /api/admin/scan/start` — the same one the scan WebSocket subscribes
to — is now the `pattern_runs.id`, created at scan START
(`app/workers/scan_runs.py::create_scan_run`, status `running`) and
finalized at scan end (`finalize_scan_run`: status `completed`/`failed`,
counts, `finished_at`, telemetry JSONB + legacy `notes`). New columns:
`scanner_mode`, `status`, `provider`, `dry_run`, `requested_limit`,
`scan_date`, `finished_at`, `telemetry`, `created_at`, `updated_at`.
Funnel, legacy and scheduled scans all create/finalize their run; a run id
is generated internally when a caller does not supply one.

**Signal provenance** — one-to-one `signal_provenance` table (PK
`signal_id` FK→signals ON DELETE CASCADE; `scan_run_id` FK→pattern_runs):
`source_path` (`funnel` | `legacy` | `scheduled` | `manual`),
`scanner_mode`, `provider`, `strategy_code`, `strategy_version` (from the
real `StrategyResult`, e.g. `sma150.v2`, `wyckoff_mtf.v1`),
`decision_policy_version` (`strategy_decision.v1` — the strategies' implicit
decision rules, named so future explicit policies version separately),
`provenance_version` (`provenance.v1`), `config_hash` + `config_snapshot`,
`market_data_as_of`, `evidence_snapshot`, `external_observation_ids`
(always `[]` until Phase 10 — never placeholder IDs). Indexed on
scan_run_id, (strategy_code, strategy_version), decision_policy_version,
config_hash.

**Immutable signal identity** — `signals.signal_fingerprint` (nullable
TEXT): SHA-256 over the canonical decision inputs (fingerprint algorithm
version, symbol, strategy code+version, decision-policy version, config
hash, snapshot date, market-data as-of in UTC, verdict,
`evidence_original_sha256` — the hash of the COMPLETE canonical evidence
BEFORE size pruning, so decisions differing only in later-pruned optional
evidence stay distinct — and sorted external-observation ids; recursively
sorted keys; deliberately NO scan_run_id and no secrets/LLM prose). The
fingerprint ALGORITHM is explicitly versioned:
`signals.signal_fingerprint_version = 'signal_fingerprint.v1'`, persisted
with every new fingerprint, included in the hashed payload, compared during
deduplication, and exposed by the provenance endpoint; a CHECK constraint
forbids partial identity states (fingerprint without version or vice
versa), and legacy rows keep both NULL. The pre-7B
`UNIQUE(symbol, pattern_code, snapshot_date)` constraint — which made
different strategy versions/config hashes/data snapshots mutually
DESTRUCTIVE on the same day (the old upsert overwrote both the signal row
and its provenance) — is dropped and replaced by a unique partial index on
`(signal_fingerprint, signal_fingerprint_version)` for non-null
fingerprints. Legacy rows keep `signal_fingerprint` NULL (never
fabricated), stay readable, and keep their historical dedup semantics via a
partial legacy index (`WHERE signal_fingerprint IS NULL`). This lets
`sma150.v2`/`sma150.v3`, `wyckoff_mtf.v1`/`v2`, Wyckoff with/without
Lorentzian, different config hashes, and different market-data snapshots
coexist as distinct immutable signals on the same symbol and date.

**Centralized transactional persistence** — `save_signal`
(`app/workers/persistence.py`) is the ONLY `INSERT INTO signals` path
(enforced by a source-scanning test) and REQUIRES a provenance record.
Semantics: a NEW fingerprint inserts signal + origin provenance + occurrence
link in ONE transaction; an exact repeated fingerprint returns the existing
`signal_id` (deduplicated) — the signal row, evidence, provenance, and the
origin `scan_run_id` are NEVER overwritten (a compatibility check refuses a
fingerprint reuse whose stored identity disagrees). Returns
`{signal_id, created_new_signal, deduplicated, signal_fingerprint,
signal_fingerprint_version}`. Builders live in `app/workers/provenance.py`.

**Scan-run occurrence links** — `scan_run_signals` (PK
`(scan_run_id, signal_id)`, `source_path`, `created_new_signal`,
`linked_at`): EVERY scan that detects a signal records a link, including
re-detections of an immutable signal created by an earlier scan.
Semantics: `signal_provenance.scan_run_id` = the ORIGIN scan that first
created the signal; `scan_run_signals` = every detection. Scan telemetry
distinguishes `signals_created` / `signals_deduplicated` / `signals_linked`
(a repeated exact signal is never counted as newly created).

**Configuration snapshot/hash** — sanitized (secret-shaped keys and
credential-looking values stripped before hashing AND persistence),
canonical JSON (recursively sorted keys, semantic list order, compact),
SHA-256. Same logical config in any dict order ⇒ same hash; any meaningful
change ⇒ different hash.

**Evidence snapshot** — deterministic evidence only (score_components,
thresholds_used, bounces_detail, trend_context, decision-card evidence,
timeframe summary, missing data/trigger/confirmation, rejection reason).
Missing fields are never invented; no LLM prose. Bounded to 64 KiB with
DETERMINISTIC pruning: only optional keys may be pruned (largest serialized
size first, key name as tiebreak); mandatory decision inputs
(verdict/decision, score_components, thresholds_used, trigger/confirmation
needed, missing_data, rejection/waiting reason, timeframe summary,
snapshot/as-of info, decision-card evidence) can never be pruned. The
original snapshot's `evidence_original_sha256` and
`evidence_original_size_bytes` plus `evidence_pruned` /
`evidence_pruned_keys` are persisted for reproducibility. If even the
mandatory-only snapshot exceeds the bound, persistence is REJECTED
(`EvidenceTooLargeError`) and the whole signal transaction is aborted — a
snapshot missing its core decision inputs is never stored.

**Market-data as-of** — latest bar actually present in the evaluated
dataframe, normalized to UTC; NULL + explicit
`market_data_as_of_missing_reason` when no trustworthy timestamp exists.
Never insertion/server/provider-response time.

**Outcome linkage** — `signal_outcomes` gains `scan_run_id`,
`strategy_code`, `strategy_version`, `decision_policy_version`,
`config_hash`, `provenance_version`, frozen at outcome creation from the
signal's provenance row (legacy signals ⇒ NULLs, never inferred). Historical
outcomes untouched.

**APIs** — `GET /api/signals/{id}/provenance` (legacy rows return
`provenance_status="legacy_unlinked"` with no fabricated fields);
`GET /api/signals` gains additive AND-composing filters `scan_run_id`,
`strategy_version`, `decision_policy_version`, `config_hash` (the
`scan_run_id` filter is the chosen scan→signals listing path; no redundant
admin endpoint was added). The `scan_run_id` filter queries the
`scan_run_signals` OCCURRENCE table, so it returns every signal a scan
detected — including immutable signals originally created by an earlier
scan — not only origin provenance rows.

**Scan failure lifecycle** — every handled exception in a scan entry path
(admin funnel, legacy batch, scheduled batch — all of which funnel through
`run_funnel_scan` / `run_scan_batch`) finalizes the canonical run:
`status='failed'`, `finished_at`, sanitized+bounded `error_code` /
`error_message` columns, partial telemetry when available. No handled
exception leaves a scan in `running`. **Zero candidates is NOT a failure:**
a scan that executes normally and finds nothing to evaluate (empty universe
/ empty candidate pool) finalizes as `completed` with zero counts, no error
identity, and `telemetry.terminal_reason='no_candidates'` — `failed` is
reserved for operational/configuration/data-readiness/strategy exceptions.
**Process-death limitation
(documented, by design):** an abrupt process death cannot execute
finalization, so a stale `running` row may remain — it is kept for forensic
visibility, never blocks new scans (scan runs are independent rows with no
active-run uniqueness), and scans are NOT resumable; a stale-run sweeper is
possible later but is deliberately not claimed here.

**Outcome semantics under immutable identity** — outcomes stay keyed 1:1 to
the immutable signal variant: an exact repeated scan reuses the same
`signal_id` and therefore cannot create a second outcome; a different
strategy version, config hash, market-data as-of, or external-observation
set is a different fingerprint ⇒ distinct signal ⇒ distinct outcome row,
each frozen with its own version identity.

**How this enables the later phases:**

- `sma150.v3` vs `sma150.v2` and `wyckoff_mtf.v2` vs `wyckoff_mtf.v1`:
  both versions can run side by side; every signal and outcome carries the
  exact `strategy_version`, so outcome grouping/comparison is a WHERE
  clause, immune to later version changes.
- Wyckoff ± Lorentzian / internal strategies ± AI Edge (Phases 10–11):
  `external_observation_ids` is structurally ready; an ablation is
  "same strategy_version + decision_policy_version, different evidence
  set/config_hash", each arm immutably identified.
- Immutable ablation experiments: `config_snapshot` + `config_hash` freeze
  the exact resolved configuration per signal — experiments never depend on
  the mutable pattern-config rows.
- Outcome grouping by strategy version, decision policy and config hash:
  the frozen columns on `signal_outcomes` make per-version expectancy /
  baseline-delta reports simple aggregations.

### Phase 8 — evidence contracts and sma150.v3 *(COMPLETE — implemented contracts below)*

**Normalized evidence contract `evidence.v1`**
(`app/workers/strategies/evidence.py`): typed `EvidenceItem`
(code, category, source_type, state, raw_value, normalized_value, unit,
threshold, operator, required, timeframe, as_of, reason_code, metadata) and
`EvidenceBundle` (evidence_version, strategy identity + decision policy,
symbol, market_data_as_of, items, hard_filter_summary, setup_state,
trigger_state, verdict, missing_data, contradictions, timeframe_summary,
ranking_components, ranking_score). States:
`pass|fail|positive|negative|neutral|unknown`; source types reserve
`market_data|strategy|external|fundamental|event|risk` (external kinds are
Phase 10 — never fabricated now). Serialization is deterministic (items
sorted by category+code; lists sorted), JSON-safe by validation (non-JSON
values are rejected, not coerced), raw values are never replaced by
normalized ones, and unknown stays unknown. **No new table**: the bundle is
persisted under the `evidence` key of the immutable
`signal_provenance.evidence_snapshot` and is a MANDATORY evidence key
(size pruning can never remove it), so it participates in the original
pre-pruning evidence hash and the signal fingerprint.

**`sma150_bounce_v3` / `sma150.v3`** (`app/workers/strategies/sma150_v3.py`)
— registered SEPARATELY beside `sma150_bounce` (still `sma150.v2`,
byte-identical behavior) with `decision_policy_version =
sma150_bounce.policy.v1`. Four layers, in authority order:

- **A. Data readiness** — configurable `min_history_bars` (200), SMA/slope/
  volume-average availability. Insufficient history ⇒ `AVOID`,
  `setup_state=unknown`, `trigger_state=unknown`,
  `reason_code=insufficient_history`, unknown evidence (no fabricated zeros),
  `ranking_score=NULL`.
- **B. Setup validity** — (1) current proximity band: close within
  `max_close_above_sma_pct` (3.0) above / `max_close_below_sma_pct` (1.0)
  below the SMA-150; (2) **independent** historical bounce events:
  contiguous in-band runs are one event; runs closer than the EFFECTIVE
  separation `max(min_event_separation_bars=15, rebound_window_bars+1=11)`
  merge into one cluster (rebound windows can never overlap); one
  deterministic representative per cluster (min |distance to SMA|, earliest
  bar on ties); events with incomplete rebound windows are EXCLUDED; at
  least `min_independent_bounces` (2) required; (3) rebound quality gated on
  the **median** rebound (`min_median_rebound_pct` 5.0) — median AND mean
  persisted; per-event touch date/index/price, SMA, distance, max favorable
  rebound, bars-to-max and age are persisted in `details.bounce_events`.
  Setup failure ⇒ `AVOID`, `setup_state=invalid`; confirmations and score
  can never override it.
- **C. Entry confirmation** (all required for ENTER): close above SMA-150;
  SMA slope over `slope_lookback_bars` (20) STRICTLY above
  `min_sma_slope_pct` (0.0 — flat blocks ENTER); deterministic bullish
  trigger (close > prior bar high [persisted as `trigger_level`] AND close >
  open AND close-location value ≥ `min_close_location_value` 0.65 — a
  zero-range bar is `unknown`, never a division by zero or a pass); volume
  ratio = current volume / COMPLETED 20-bar average ≥
  `min_trigger_volume_ratio` (1.20 — 1.07 fails). Valid setup + any
  missing/failed confirmation ⇒ `WATCH` (`trigger_state=missing` or
  `contradicted`); negative slope / close below SMA / bearish candle are
  recorded as contradictions.
- **D. Ranking `sma150.v3.rank.v1`** — ordering only, never authorization:
  unweighted arithmetic mean of the fixed set {proximity_quality,
  trend_quality, independent_bounce_quality, rebound_quality,
  volume_quality, trigger_quality, bounce_recency_quality}, each an
  explicit unit-tested [0,1] formula; recency is exponential decay
  `0.5^(age_bars / recency_half_life_bars=126)`; score is NULL when any
  component is unknown; raw + normalized values are both persisted; NOT a
  probability; the v2 `score_threshold` does not exist in v3.

**Deterministic invalidation** (no invented targets):
`daily_close_below_sma150_pct` with `invalidation_below_sma_pct` (2.0);
level, rule code and threshold persisted in `details.invalidation` and on
the decision card. **Decision card** gains ADDITIVE v3 fields (setup/trigger
states, slope, median/mean rebound, trigger conditions, failed
confirmations, contradictions, invalidation rule, ranking components) —
v2/wyckoff cards unchanged.

**Known regression case (JBL-like)** — price ~2.3% above a declining SMA,
3 v2-counted bounces (2 clustered), strong rebounds, volume ratio ~1.07, no
trigger: v2 says ENTER; v3 says **WATCH** naming the trend, volume and
trigger gaps (regression-tested).

**Completed-daily-bar policy `ny_session_close.v1`** — a provider daily
aggregate may represent the still-open US session (Massive incremental
top-ups fetch through "today"; provider bars are never assumed completed).
v3 evaluates COMPLETED bars only: explicit caller/provider metadata wins
when present; otherwise a bar dated before the current exchange-session
date (America/New_York) is completed, a bar dated today is completed only
at/after the configured session close (16:00 — never a bare wall-clock
check; early-close days are conservatively treated as incomplete until the
regular close, which can only exclude, never include a partial bar). A
partial latest bar is EXCLUDED (one safe deterministic exclusion) and the
prior completed bar becomes the trigger bar; if completion still cannot be
proven (future-dated bar, corrupt feed), readiness is unknown and the
verdict is `AVOID` with `reason_code=unconfirmed_bar_completion`. The
volume baseline already excludes the trigger bar; slope, bounce and rebound
windows use completed bars only; `market_data_as_of` is the latest
COMPLETED bar actually evaluated (the strategy declares it in
`details.market_data_as_of` and both funnel and legacy provenance builders
prefer it over the raw frame). Policy config (`bar_completion_policy`,
`exchange_timezone`, `session_close_time`) lives in the strategy config —
persisted per signal via the config snapshot — and the per-evaluation
decision is recorded as the `latest_bar_completion` evidence item (with
`trigger_bar_date` and `volume_baseline_end_date`). sma150.v2 is untouched
by this policy.

**Evidence ordering rules** — evidence items serialize sorted by the FULL
identity key `(category, code, source_type, timeframe, as_of)`; duplicate
identities are rejected (never silently collapsed — the same check on 1d
and 4h is two items). Only SET-LIKE lists are sorted (`missing_data`,
`contradictions`, external observation ids); semantic sequences are never
reordered: bounce events stay chronological, declared timeframe sequences
keep their order, trigger conditions keep their documented order, and
ranking components are stable named keys.

**Outcome coverage (honest current state)** — the outcome service selects
`verdict = 'ENTER'` only (`get_signals_needing_outcomes`). Therefore:
sma150.v3 **ENTER** signals flow through `outcome.v1` unchanged; **WATCH**
signals are persisted with full immutable provenance but do **not** yet
receive outcome rows. This means trigger false negatives (WATCH candidates
that would have performed) and the value of waiting are currently
unmeasured. Phase 8.1 adds outcome tracking for both ENTER and WATCH before
any strategy-effectiveness conclusion is drawn. **No claim about v3
effectiveness can be made until enough frozen outcomes exist for both
verdicts.**

**Registration/rollout**: migration `008_sma150_v3.sql` (additive,
idempotent) registers `sma150_bounce_v3` **disabled by default**
(`is_enabled=false`) with its full default config; manual/explicit scans can
select it via the registry; the scheduled default pattern is unchanged. The
config seed uses `ON CONFLICT DO NOTHING`: a rerun never resets
operator-modified v3 configuration (002-style `DO UPDATE` is reserved for
explicit corrections of live values). The legacy scan path keeps its direct
v2 call for `sma150_bounce` and routes any other explicitly selected
pattern through the registry. v2 and v3 signals for the same symbol/date
coexist as separate immutable signals (different strategy_version + policy
⇒ different fingerprints) with separate outcomes.

### Phase 8.1 — candidate outcome coverage and v2/v3 shadow comparison *(NEXT — not implemented)*

Prerequisite for any effectiveness judgement of sma150.v3 (and any later
strategy version):

- outcome rows for **both ENTER and WATCH** signals (WATCH outcomes measure
  trigger false negatives and the value of waiting);
- the signal `verdict` preserved on each outcome row;
- metrics reported separately by `strategy_version`, `verdict`,
  `decision_policy_version` and `config_hash` — never pooled across
  versions or verdicts;
- no historical inference and no fabricated provenance: only signals
  persisted with real provenance get outcomes, from their snapshot forward;
- v2 versus v3 **shadow comparison** on the frozen outcome data (same
  symbols/dates where both versions produced signals);
- **no parameter tuning** — observation only.

### Phase 9 — deterministic Wyckoff technical engine v2

- New, separately versioned **`wyckoff_mtf.v2`**; v1 remains registered and
  untouched. Full requirements in §5.
- Emits `evidence.v1` bundles (the Phase 8 contract); daily setup vs. 4H
  trigger separation preserved; explicit `UNKNOWN_PHASE` and
  `AMBIGUOUS_STRUCTURE` states.

### Phase 10 — external signal intake (AI Edge, Lorentzian, ...)

- Generic external-observation contract (§6), secure webhook ingestion,
  idempotency, symbol/timeframe normalization, freshness policy.
- External observations become evidence features only — no decision rules in
  this phase.

### Phase 11 — evidence fusion, ablations and experiment reporting

- Versioned decision policies consuming evidence snapshots.
- Ablation harness: strategy ± each external feature vs. baselines on frozen
  versions and immutable config snapshots (§8).

### Phase 12 — shadow validation and evidence gates

- New strategy/policy versions run in shadow (persisted, marked, excluded
  from the default UI) until outcome evidence passes explicit gates
  (sample size, baseline delta, stability) — only then eligible for ENTER.

## 5. Wyckoff v2 requirements (Phase 9 target)

`wyckoff_mtf.v2` is a **new registered strategy version**. `wyckoff_mtf`
(v1) is neither replaced nor silently changed; both can be evaluated and
outcome-compared side by side.

Deterministic evidence v2 must support:

- monthly and weekly market context (bias, trend quality, structure);
- daily trading-range detection with explicit support, resistance and
  midpoint levels;
- structure classification: accumulation, distribution, or **unknown** —
  the engine must not force every chart into a Wyckoff phase;
- Phase A–E candidates with explicit `UNKNOWN_PHASE` and
  `AMBIGUOUS_STRUCTURE` states;
- accumulation event candidates: PS, SC, AR, ST, Spring, Test, SOS, LPS;
- distribution event candidates where supported: PSY, BC, AR, ST, UT, UTAD,
  SOW, LPSY;
- price spread and volume relationships; relative volume; effort-vs-result
  measurements;
- event dates and price levels persisted as evidence;
- event confidence computed from deterministic components only (no opaque
  scores);
- daily setup vs. 4H trigger separation (v1 semantics preserved);
- invalidation evidence (structure level + reason);
- insufficient-history handling (required vs. available bars, explicit skip
  reason).

Uncertain evidence remains uncertain — `UNKNOWN_PHASE` is a valid, persisted
result. Point-and-figure cause analysis is **out of scope** for the first v2
implementation; it may only arrive later as an isolated optional evidence
plugin.

## 6. External evidence architecture (Phase 10 target)

A generic external-observation contract is designed **before** any
indicator-specific decision rule. Normalized observation fields (minimum):

```
id, source, indicator_code, indicator_version, configuration_hash,
symbol, timeframe (canonical), direction, value (numeric | categorical),
source_confidence (optional), observed_at, received_at, expires_at,
idempotency_key, raw_payload (safe subset), created_at
```

Expected sources: TradingView, AI Edge, Lorentzian Classification, Future
Pivots, Wavetrend 3D, TrendSpider, manual observations.

Ingestion requirements:

- secure webhook authentication (per-source secrets; constant-time compare);
- idempotent ingestion via `idempotency_key`;
- symbol normalization (provider symbol table as canonical reference);
- timeframe normalization to canonical codes (1d, 4h, 1w, 1M, ...);
- freshness and expiration policy (`expires_at`; stale observations become
  `unknown` evidence, never silently reused);
- payload schema validation per source/indicator;
- no secrets in payload storage or logs;
- configuration hash + indicator version tracked on every observation;
- the raw source value is preserved separately from any derived feature.

Every observation must be linkable to: the scan run, the signal, the
evidence snapshot, the strategy/decision-policy version, and later outcomes.

### Normalized evidence contract (Phase 8 target)

Each evidence item carries:

```
feature_code, source_type (market_data | strategy | external | fundamental |
event | risk), raw_value, normalized_value (when applicable),
state (pass | fail | positive | negative | neutral | unknown),
timestamp, timeframe, freshness, provenance (source id / observation id),
missing_data_reason, explanation (deterministically generated text)
```

**Hard filters vs. soft evidence** — kept separate by construction:

| Hard filters (gates) | Soft evidence (features) |
|---|---|
| liquidity | Lorentzian direction |
| minimum price | relative strength |
| market-cap readiness | volume confirmation |
| sufficient history | Wyckoff event quality |
| stale data | trend quality |
| unsupported exchange | Future Pivot proximity |
| earnings/event window (when available) | |

Scoring rules:

- no arbitrary point weights ("Lorentzian = 25 points" is forbidden);
- raw components always persisted;
- every scoring formula versioned;
- continuous where appropriate, not bucketed for convenience;
- no hidden duplicated features (e.g., dollar volume both as filter and
  disguised score input without declaration);
- testable through ablations (§8);
- a failed hard gate can never be hidden or compensated by score.

## 7. Provenance requirements (Phase 7B onward)

Every persisted signal must eventually be reproducible from:

```
scan_run_id, scanner_mode, provider, strategy_code, strategy_version,
decision_policy_version, config snapshot (or stable configuration hash),
market-data as-of timestamp, external observation IDs used,
evidence snapshot, decision, created_at
```

Outcomes must preserve the strategy and policy versions being evaluated so a
later version change cannot silently contaminate historical statistics.

Status: implemented in Phase 7B for all signals persisted after migration
007 (see the Phase 7B section for the concrete contracts). Legacy rows are
reported as `legacy_unlinked` and are never backfilled with fabricated
provenance.

## 8. Ablation and experiment requirements (Phase 11 target)

The architecture must support comparisons such as:

- `sma150.v3` without Lorentzian vs. with Lorentzian;
- `wyckoff_mtf.v2` alone vs. + Lorentzian vs. + Future Pivots;
- each external indicator alone;
- simple momentum baseline;
- SPY / QQQ window baselines (already implemented in Phase 2);
- sector-relative baseline where sector data is available.

Rules:

- experiments run only on **frozen strategy/policy versions** and
  **immutable configuration snapshots**;
- no weight optimization during normal production scanning;
- results reported with sample size, baseline delta, and win/loss
  distribution — never a bare "score".

## 9. LLM boundary

The LLM **may**: explain deterministic evidence; summarize verified news or
filings; identify contradictions among supplied facts; write the
decision-card narrative; describe what information is missing.

The LLM **must not**: create technical values; infer an uncomputed Wyckoff
phase; override a failed hard filter; convert WATCH to ENTER; invent entry,
stop, target, or invalidation; provide an uncited free-form direction
prediction.

Enforcement is structural, not prompt-based: the LLM layer consumes the
evidence snapshot and decision card as read-only input and its output is
stored as narrative text only — never parsed back into decisions, levels, or
evidence.
