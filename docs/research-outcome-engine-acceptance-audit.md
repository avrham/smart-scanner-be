# Research Outcome Engine — acceptance audit

**Audited:** 2026-09-06 · **Branch:** `feat/research-outcomes` · **Audited revision:** `f231b828e20db0bb4b7a29bab3a19a14f5346a62`
**Staging as-of for every count below:** `2026-09-06T13:36Z` (database clock)
**Deployed identity at audit time:** `smart-scanner-be-research-lifecycle-staging`, image `deployment-01M1V6M1GA5DTKSVFDESETHPX2`, machine version 7, git SHA `f231b828…`

This document records what was **measured**, not what was expected. Where the
previous session's report asserted something it had derived rather than
observed, the durable record was re-read and the assertion re-tested. Two of
the five concerns were confirmed as real defects and repaired; one is a real
limitation that belongs to the research lifecycle's acquisition path and is
reported rather than repaired; one was a documentation defect; one was
disproved.

---

## Summary table

| # | Concern | Verdict | Action |
|---|---|---|---|
| A | Schedule occurrence correctness | **Confirmed** — shared scheduler property | Outcome-specific mitigation + regression coverage; shared code untouched |
| B | Missing-data completion | **Confirmed limitation** — not an engine defect | Reported precisely; remedy proposed as an owner decision |
| C | Immutable scan provenance | **Two real gaps confirmed, one risk disproved** | Repaired (migration 032 + atomic planning) |
| D | Shared-worker interference | **Documentation defect** | Wording corrected; no queue added |
| E | Return / excursion contract | **No defect** | Contract stated explicitly |

---

## A — Schedule occurrence correctness

### What was claimed
That enabling the schedule with `next_run_at` NULL immediately consumed the
2026-09-09 occurrence and advanced the next fire to 2026-09-10T15:00Z.

### What the durable record says

| Fact | Value | Source |
|---|---|---|
| Schedule row created | `2026-09-06T11:10:18.493Z` | `job_schedules.created_at` |
| Scheduler tick that fired | `2026-09-06T11:21:38.176Z` | `job_runs.created_at`, `requested_by='scheduler'` |
| **Occurrence it stamped** | **`2026-09-09T15:00:00+00:00`** | `job_events.metadata->>'occurrence'` **and** `job_tasks.payload->>'occurrence_scheduled_at'` — two independent records agree |
| `next_run_at` after | `2026-09-10T15:00:00+00` (11:00 ET) | `job_schedules.next_run_at` |
| The run's actual data horizon | `as_of_session = 2026-09-04` | `research_outcome_runs.as_of_session` |
| Execution | started `11:21:38.221Z`, finished `11:21:39.140Z` (777 ms) | `job_task_attempts` |

Independently re-derived with the production resolver:
`compute_next_run_at(schedule, 2026-09-06T11:21:38Z) = 2026-09-09T15:00:00Z`
and `compute_next_run_at(schedule, 2026-09-09T15:00Z) = 2026-09-10T15:00:00Z`.
Both match the stored values exactly.

### Conclusion — CONFIRMED, and it is a shared property

The bootstrap run executed **3 days 3 h 38 min before** the occurrence it was
stamped with. The occurrence identity `2026-09-09T15:00Z` is therefore spent:
no pass will run at 11:00 ET on 2026-09-09, and the durable record nonetheless
claims that occurrence was served — by a run whose `as_of_session` was
2026-09-04 rather than the 2026-09-08 that occurrence would have seen.

The cause is one line of **shared** scheduler code:

```python
# app/jobs/scheduler.py::_tick_as_leader
occurrence = s["next_run_at"] or compute_next_run_at(s, now)
```

A row with `next_run_at IS NULL` is selected as *due*, and then labelled with
the next **future** occurrence. Reproduced for the research lifecycle's own
live delay of 960 minutes: from the same instant it too resolves to an
occurrence three days out. So this is not outcome-specific logic.

**What is not claimed:** that the lifecycle schedule was actually affected. Its
durable history shows its first observed scheduler job (2026-08-31T22:30:24Z)
already carried `occurrence = 2026-08-31T22:30:00Z` — 24 seconds in the *past* —
so `next_run_at` was already populated before that tick. How it came to be
populated is not recoverable from what is stored.

### Practical consequence

Bounded and non-destructive. An observation stays in the ledger until it is
measured, so nothing is lost; the cost is one day of latency, once. Session
2026-09-08's horizons will be measured by the 2026-09-10 pass instead of the
2026-09-09 one. The audit-trail error — a run claiming an occurrence it did not
answer — is the part worth preventing.

### Repair

The shared scheduler is **not changed**: editing `_tick_as_leader` would alter
dispatch for `SMART-SCANNER-RESEARCH-LIFECYCLE` and `SMART-SCANNER-DAILY-PIPELINE`,
which is outside this workstream's authorization.

Instead the outcome schedule is kept away from the hazard:
`ops/analysis/research_outcomes.py --enable-schedule` computes `next_run_at`
with the **same resolver the scheduler uses** and writes it in the same
statement that sets `enabled`. A seeded schedule takes the stored instant, so
the identity it stamps is the occurrence it actually ran for. `--disable-schedule`
is the matching rollback.

The already-spent 2026-09-09 occurrence is **not** repaired. Its idempotency key
is consumed; re-pointing `next_run_at` at it would produce a tick that creates
nothing and then advances anyway. Inventing a replacement identity would be
worse than the one day of latency it would save.

### Calendar handling (verified, no defect)

* Labor Day 2026-09-07 produces **no occurrence**; the cadence steps
  Fri 09-04 → Sat 09-05 → Wed 09-09.
* The Saturday firing is correct, not a bug: `market_daily` walks trading
  **sessions** and adds the delay, so Friday's session is measured ~19 h after
  its close. Every trading session receives exactly **one** occurrence — proven
  over a 12-occurrence window with no duplicate and no skipped session.
* Across the 2026-11-01 DST change the local time stays **11:00 ET**
  (15:00Z EDT → 16:00Z EST).

### Coverage added
`tests/test_research_outcomes_contract.py::TestScheduleBootstrapHazard` (5),
`::TestCadenceAcrossTheAwkwardDates` (3).

---

## B — Missing-data completion

### Natural immaturity vs. missing data

At 2026-09-06T13:36Z the ledger holds **120 observations**: 13 `measured`,
31 `waiting_for_data`, 76 `not_yet_eligible`, 0 `failed_terminal`.

* **76 `not_yet_eligible`** are natural immaturity — their horizon session has
  not happened yet. Nothing is missing.
* **31 `waiting_for_data`** are eligible and blocked. Every one of them needs a
  session in **2026-08-31 … 2026-09-04** — all in the **past**. Breakdown by
  reason: `missing_symbol_exit_bar` 29, `missing_symbol_entry_bar` 2.
  **Zero** are blocked on the benchmark.

### The acquisition path, traced

`research_ingest.warm_symbol` fetches

```python
frm = moment.date() - timedelta(days=int(target_sessions * 1.75))   # ≈ today − 882 d
to  = moment.date()
```

so a single successful top-up **backfills interior and older sessions**, not
just the newest bar. Missing exit bars are recoverable *in principle*.

Selection (`research_ingest.WARMUP_SELECT_SQL`) gates on three things:

1. `warmup_attempts < 3` (`ru.MAX_WARMUP_ATTEMPTS`)
2. `admission_state IN ('eligible_for_history','insufficient_admission_data')`
3. for the freshness top-up branch, `state IN ('research_ready','research_scanned')`

`warmup_attempts` is incremented on **every** call to `warm_symbol` and
decremented **only** when the symbol is still maturing
(`warmup_attempts = GREATEST(0, warmup_attempts - $5)`, `$5 = 1 if maturing`).
A grep of the whole application confirms these are the only two writes: **there
is no reset on success.**

### What that means, measured

| Symbol | state | admission | attempts | last bar | measured | waiting | not-yet | can top up? |
|---|---|---|---|---|---|---|---|---|
| AAL | research_scanned | eligible | **3** | 2026-09-04 | 0 | 0 | 5 | **no — attempt ceiling** |
| CELU | research_scanned | **rejected_before_history** | 2 | 2026-08-28 | 0 | 3 | 2 | **no — admission** |
| NVD | research_scanned | **rejected_before_history** | 1 | 2026-08-28 | 0 | 5 | 5 | **no — admission** |
| PPCB | research_scanned | **rejected_before_history** | 2 | 2026-08-28 | 0 | 3 | 2 | **no — admission** |
| BITO, IBIT, ONDS | research_scanned | eligible | 2 | 2026-09-03 | 10 | 6 | 24 | yes (1 attempt left) |
| INTC, TSLL | research_scanned | eligible | 2 | 2026-09-01 | 3 | 8 | 14 | yes (1 attempt left) |
| ARTL, BANL, IREN, NU, PATH, TQQQ | research_scanned | eligible | 1 | 09-01…09-04 | 0 | 6 | 24 | yes (2 attempts left) |

**25 of the 107 unfinished observations (23 %) sit behind a symbol that cannot
currently be topped up at all.** The remaining 82 sit behind symbols with one
or two attempts left each.

### Conclusion — completion is NOT guaranteed

This is a **property of the P0 research lifecycle's acquisition path**, not a
defect in the outcome engine. Stated precisely:

> Every research symbol has a **lifetime budget of three warm attempts**.
> Freshness top-ups consume that budget. Once it is exhausted the symbol is
> never selected again, its bars freeze permanently, and every outcome horizon
> that needs a later bar becomes unmeasurable — it will sit `waiting_for_data`
> for 60 trading sessions and then be recorded `failed_terminal`.

AAL is the existing proof: healthy, eligible, `research_scanned`, 504 bars,
history current to 2026-09-04, and already at the ceiling with five unfinished
observations.

A second, softer gate: a symbol scanned while `eligible_for_history` and later
re-admitted as `rejected_before_history` (price fell below the minimum) stops
being selectable. This is **conditionally** recoverable — admission is
re-evaluated from a fresh price each run, so a recovery in price restores
selectability and the wide fetch window then backfills the gap. CELU, NVD and
PPCB are in this state.

### What IS guaranteed

* **The benchmark leg advances automatically.** SPY is *not* a research symbol
  (so the research role's RLS cannot write it); it is refreshed through the
  `history_incremental_refresh` queue that the lifecycle enqueues when it finds
  core history stale. Last written `2026-09-05T12:03Z`, source `massive`; 202
  succeeded refresh tasks to date. Zero observations are blocked on the
  benchmark. The dependency to note: SPY advances *because* the lifecycle runs,
  not on a schedule of its own.
* **Interior gaps are recoverable** for any still-selectable symbol, in one
  request, because the fetch window is ~882 calendar days ending today.
* **`failed_terminal` is not a dead end.** The automatic pass ignores terminal
  rows, but an operator `--recheck` (`include_settled`) re-reads them and a
  terminal row transitions to `measured` if the bars finally arrive. Proven in
  `TestTerminalIsRecoverable`.

### The 60-session abandonment rule

* **Origin:** `app/research_outcomes.py::MISSING_DATA_GRACE_SESSIONS = 60`.
* **Anchor:** the observation's own `horizon_session`, counted in **trading
  sessions** via `trading_sessions_between(horizon_session, as_of_session)` —
  not calendar days, and not the first attempt.
* **Affected statuses:** only `waiting_for_data` (and `not_yet_eligible` rows
  that become eligible). A `measured` row is never re-classified.
* **Retry:** the row is re-examined on every pass until it settles; each pass
  increments `attempt_count` and stamps `last_attempt_at`.
* **Later-arriving data:** recoverable only through an explicit operator
  `--recheck`. A consequence worth stating: an observation whose horizon is
  already more than 60 sessions old when it is first planned goes straight to
  `failed_terminal` on its first sight, with `attempt_count = 1`.

### Proposed remedy — **owner decision, not applied**

The smallest change that would restore a completion guarantee is one line in
`app/research_ingest.py::warm_symbol`: roll back the attempt increment on a
**successful** top-up of an already-ready symbol, exactly as it is already
rolled back for a maturing one — i.e. treat `warmup_attempts` as the *failure*
budget its name implies rather than a lifetime service budget.

It is **not applied here** because it changes which symbols the research
lifecycle selects, which is validated P0 selection semantics and outside this
audit's authorization. It spends no additional provider budget (the per-run
`warm_limit` and `provider_budget` are unchanged); it changes *which* symbols
those requests are spent on.

---

## C — Immutable scan provenance

### Finding 1 — all five horizons share one revision · **proven by construction**

`plan_observations(scan)` reads the scan row **once** into a dict and all five
horizon plans copy from that dict. Within one planning pass the five rows
cannot disagree.

### Finding 2 — a partial plan could mix revisions · **CONFIRMED, repaired**

The five INSERTs were five independent statements. A worker that died after two
of them would leave a scan holding 1D and 3D from revision A; the next pass
re-reads `research_scan_results`, and because
`research_scan.py::UPSERT_SCAN_SQL` rewrites a scan **in place**
(`ON CONFLICT (symbol, scan_session) DO UPDATE SET verdict…, setup_state…`),
5D/10D/20D could be written from revision B. One scan, five horizons, two
classifications, and nothing on the row to say so.

**Repair:** `plan_missing_observations` now wraps each scan's five INSERTs in
one transaction — per **scan**, not per pass, so a bounded read-mostly job
never holds row locks across two hundred scans. Proven against real Postgres by
injecting a failure into the fifth INSERT and asserting zero rows survive
(`TestPlanningAtomicity`).

### Finding 3 — the trigger did not guard the meaning · **CONFIRMED, repaired**

Migration 031's `research_scan_outcomes_freeze` guarded 21 columns — the
numbers, the status and the horizon identity. **Ten attribution columns were
outside it**, plus the three version columns:

`benchmark_symbol`, `strategy_code`, `strategy_version`, `scan_verdict`,
`scan_structure_state`, `scan_setup_state`, `scan_reason_code`,
`scan_rejection_reason`, `scan_benchmark_relative`, `scan_scanned_at`,
`contract_version`, `calculation_version`, `market_calendar_version`.

`benchmark_symbol` alone is sufficient to demonstrate the consequence: flipping
a measured row from `SPY` to `QQQ` leaves an internally consistent row that
passes every CHECK and the trigger, and now claims an excess return against a
benchmark it was never computed from. `scan_scanned_at` is worse in kind — 031's
own header names it as *the* detector for a silent re-scan, and a detector that
can be edited detects nothing.

**Repair:** `app/db/migrations/032_research_outcome_freeze_attribution.sql` —
a pure widening of an existing refusal via `CREATE OR REPLACE FUNCTION` plus a
trigger recreate. No table is altered, no row is read or written. 14 real-Postgres
parametrized cases assert each column is now refused on a `measured` row, and
matching cases assert that `attempt_count`, `last_attempt_at`,
`revision_detected` and `revision_notes` remain writable and that a
**non-measured** row is still freely updatable.

> **Operational note discovered while testing this.** 031 and 032 both
> `CREATE OR REPLACE` the same function, so **replaying 031 on a database that
> already has 032 silently reinstates 031's narrower guard.** A replay must
> always continue forward through the rest of the chain. This is now encoded in
> `test_the_migration_is_idempotent`, which replays both in order — the audit
> found it by replaying only 031 and watching thirteen 032 assertions stop
> holding.

### Finding 4 — pending retries replacing attribution · **DISPROVED**

`UPDATE_PENDING_SQL` writes only `status`, `status_reason`, `attempt_count`
and `last_attempt_at`. No retry path touches the snapshot.

### Finding 5 — historical provenance · **partly unrecoverable**

The snapshot is taken at **plan** time, not at scan time. Every observation in
staging was planned on 2026-09-06, after every scan had been written, so each
row captured whatever revision was current then.

`research_scan_results` keeps exactly **one row per `(symbol, scan_session)`**
by design and stores no revision history. Session 2026-09-01 was scanned twice
(the blocked scheduled run and the manual `sessionfix0901` run both report five
scans for that target). **Which values those rows held before the second scan
cannot be reconstructed** — the information does not exist anywhere. What *is*
recoverable and now protected: the revision that was current at plan time,
frozen on the observation and guarded by the widened trigger, with
`scan_scanned_at` as an immutable tell for any later divergence.

### Uniqueness constraints and the source-scan identity contract

Two constraints, deliberately stating the same fact in two vocabularies:

* `UNIQUE (scan_id, horizon_sessions)` — the identity the engine writes through.
* `UNIQUE (symbol, scan_session, horizon_sessions)` — the same fact as a reader
  states it, and what makes "two scans of one symbol on different sessions are
  independent" a schema guarantee.

**The contract today:** a `(symbol, scan_session)` pair is **one mutable
research record**, not a series of distinct research events. A re-scan with a
different configuration overwrites it and the outcome ledger keeps the
revision it snapshotted. This is P0's design and is **not** changed here; a
multi-revision or multi-strategy scan identity would be a schema change to
`research_scan_results` and a product decision, not an audit repair.

---

## D — Shared-worker interference

### Measured facts

| Property | Value | Source |
|---|---|---|
| Worker concurrency | 1 | `settings.JOB_WORKER_CONCURRENCY`, `fly.research-lifecycle.toml` |
| Task lease | 900 s | `JOB_TASK_LEASE_SECONDS` |
| Heartbeat / lease renewal | every 30 s, **for as long as the child runs** | `app/jobs/worker.py` |
| Child wall-clock timeout | **none** | `app/jobs/worker.py` — the loop renews indefinitely |
| Per-statement ceiling | 120 s | role `statement_timeout` |
| Claim predicate | `AND available_at <= NOW()` | `app/jobs/queue.py:112` |
| Retry sets | `available_at = NOW() + backoff` | `app/jobs/queue.py:258` |
| Observed outcome-pass duration | 0.68 s and 1.23 s | `research_outcome_runs.duration_seconds` |

### Conclusion

* **Retry backoff DOES release execution capacity.** A task in backoff is
  `retryable` with `available_at` in the future and the claim query excludes
  it, so it holds neither the lease nor the executor. Proven by the predicate
  and pinned in `TestSharedWorkerClaimsAreAccurate`.
* **Failure-status isolation is complete.** An outcome failure settles on its
  own job/task/run row; there is no code path to a lifecycle status and no
  shared row.
* **Resource isolation is NOT complete, and the previous wording overstated
  it.** One worker at concurrency 1 with no child wall clock means whichever
  task is executing holds the executor until it returns. A separate queue would
  not change this — same worker, same executor.
* **The pass cannot monopolise the worker indefinitely** because the *work* is
  bounded by construction: at most `scan_limit ≤ 200` scans × 5 inserts plus
  `observation_limit ≤ 400` observations × 6 small point queries, each capped at
  120 s by the role. What is *not* bounded is an explicit wall clock; the
  guarantee is over work, not time.
* **Residual delay to lifecycle work:** normally zero — the schedules are three
  hours apart (08:00 ET vs 11:00 ET). The residual case is a lifecycle *retry*
  (30-minute backoff) landing on the same minute as an outcome pass, which then
  waits one pass duration (observed < 1.3 s).

**Repair:** documentation only. The sentence *"As a separate job it structurally
cannot [defer today's lifecycle]"* has been replaced in
`app/jobs/research_outcomes.py` with an explicit split between failure
isolation and resource isolation, the measured numbers, and the residual case.
No queue was added.

---

## E — Return and excursion contract

### Entry semantics — descriptive, not executable

`entry_close` is the close of the scan's **own session**. Every measured scan
was produced *after* that close — e.g. `scan_session = 2026-08-28` scanned at
`2026-08-31 06:05 ET`. So the price was fully knowable when the view was formed
(no lookahead), and equally **it is not a price anything could have been bought
at**. These are scan-session reference returns, a market-path observation. The
module and migration already say "not a trade… no side, no stop, no target, no
simulated R"; a test now pins that the module docstring never uses trade
vocabulary.

### MFE / MAE window — correct

`_window_bars` selects `trading_date > scan_session AND trading_date <= horizon_session`,
so the entry session's own high and low are **excluded** and the window is
exactly the intended completed sessions. All 13 measured rows carry
`excursion_basis = daily_high_low.v1` with `window_sessions_present ==
window_sessions_expected` (8 rows at 1/1, 5 rows at 3/3) and no NULL excursions.

Daily bars cannot order intrabar extremes, so MFE/MAE are excursion **extremes**
and never a claim that a level was reached before another one. This is recorded
on every row rather than in a comment.

### Adjustment basis — consistent, and detectable-but-not-classifiable

Every symbol in the ledger, and SPY, has exactly **one** `source` (`massive`);
no symbol mixes sources across any measurement window. `daily_bars` carries
**no adjustment column** (`id, symbol, trading_date, open, high, low, close,
volume, vwap, transaction_count, source, created_at`), so a retroactive split
or dividend adjustment is **detectable** — `bars_hash` changes and a
`--recheck` records a `bars_revised` note — but **not classifiable** as a
corporate action. 0 revisions detected to date.

### NULL excursions are permanent — stated, not repaired

A row measured with an incomplete window is frozen with `mfe_pct`/`mae_pct`
NULL and `excursion_basis = incomplete_window`. Because the row is immutable,
**those excursions stay NULL forever even if the interior bars later arrive.**
That is the immutable-snapshot policy applied consistently, and it is the
correct behaviour under it — but it is a real, permanent loss of information
for such rows, and it is recorded here rather than quietly enriched. No such
row exists in staging today.

---

## Evidence paths

| Artifact | Path |
|---|---|
| This report | `docs/research-outcome-engine-acceptance-audit.md` |
| Migration (repair C3) | `app/db/migrations/032_research_outcome_freeze_attribution.sql` |
| Atomic planning (repair C2) | `app/research_outcomes.py::plan_missing_observations` |
| Isolation wording (repair D) | `app/jobs/research_outcomes.py` module header |
| Safe enable (mitigation A) | `ops/analysis/research_outcomes.py::set_schedule`, `--enable-schedule` / `--disable-schedule` |
| Schedule + cadence tests | `tests/test_research_outcomes_contract.py::TestScheduleBootstrapHazard`, `::TestCadenceAcrossTheAwkwardDates` |
| Trigger coverage tests | `tests/test_research_outcomes_integration.py::TestFreezeGuardsTheAttribution` |
| Atomicity test | `tests/test_research_outcomes_integration.py::TestPlanningAtomicity` |
| Terminal-recovery test | `tests/test_research_outcomes_integration.py::TestTerminalIsRecoverable` |
| Contract tests (C/D/E) | `tests/test_research_outcomes_contract.py::TestFreezeCoversTheWholeMeaning`, `::TestPlanningIsAtomicPerScan`, `::TestSharedWorkerClaimsAreAccurate`, `::TestReturnContractIsDescriptive` |

Staging queries used for every runtime fact in this document were run read-only
against the isolated Fly Postgres `warmup` as `flypgadmin` over `flyctl proxy`,
and are reproducible from the SQL quoted inline above.
