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
  **Gap (7B): no `scan_run_id`, no dedicated strategy/policy version columns
  — versions live only inside `details` (`score_version`,
  `strategy_version` on cards). Signals are not yet fully reproducible.**
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

### Known validation debts

- Controlled scan smoke before Phase 2 was intentionally skipped (recorded).
- No signal alpha has been demonstrated; outcome sample sizes are tiny.
- Signals lack explicit scan-run provenance (see Phase 7B).

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

### Phase 7A — durable market-data jobs and coverage observability *(this session)*

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

### Phase 7B — scan and signal provenance

- Additive migration: `signals.scan_run_id`, `signals.strategy_version`,
  `signals.decision_policy_version`, config snapshot hash (or snapshot JSONB),
  market-data as-of timestamp.
- `pattern_runs` gains a stable `scan_run_id` correlation to signals.
- Outcomes copy the strategy/policy versions they evaluated.
- Goal: every persisted signal reproducible from stored provenance
  (see §7 Provenance requirements).

### Phase 8 — evidence contracts and sma150.v3

- Introduce the normalized evidence contract (§6) and an evidence snapshot
  persisted per signal.
- Re-express sma150 checks as evidence features; ship as **`sma150.v3`**, a
  new version beside `sma150.v2` (no silent replacement).
- Separate hard filters from soft evidence explicitly in the funnel.

### Phase 9 — deterministic Wyckoff technical engine v2

- New, separately versioned **`wyckoff_mtf.v2`**; v1 remains registered and
  untouched. Full requirements in §5.

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

Current gap (audited): signals persist strategy versions only inside
`details`; `scan_run_id` is available in `StrategyContext` but not persisted.
Phase 7B closes this.

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
