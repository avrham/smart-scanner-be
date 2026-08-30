# Dynamic discovery → research pipeline

Wave 2 measured the scanner's blind spot exactly: of 68 symbols the market
noticed in one session, **one** was inside the frozen 25 and **67** had too
little local history to analyse at all. We could see what we were missing and
could do nothing with it.

This closes that for a bounded few, and stops there.

```
DISCOVERED → HISTORY_REQUIRED → HISTORY_WARMING → RESEARCH_READY → RESEARCH_SCANNED
                                          (UNAVAILABLE / FAILED alongside)
```

---

## 1. The line, and where it is actually enforced

A research symbol never becomes a universe member, an experiment pair, a
canonical outcome, an attention rank or an ENTER. Not by convention — by four
independent mechanisms:

| Guarantee | Enforced by |
|---|---|
| never a universe member | the ingestion role holds **no privilege** on `history_warmup_universe_symbols`; proven live with a denied INSERT |
| never an experiment row | `research_scan_results` has no `pair_id`, `run_id`, `arm_code` or `experiment_code` — the columns an experiment row needs are absent |
| never ENTER-eligible | `CHECK (verdict <> 'ENTER')` on the table, plus the same `allow_enter=false` config the experiment runs |
| never a frozen-bar writer | the role's `daily_bars` grant is confined by RLS to `symbol IN (SELECT symbol FROM research_symbols)` |

Tests assert all four, and `tests/test_research_pipeline.py::TestExperimentBoundary`
parses the modules (docstrings stripped) so a future edit that reaches for a
`strategy_shadow_*` relation fails a test rather than a review.

## 2. Why a new table instead of a warmup universe

`history_warmup_universes` is immutable **by database trigger** the moment a
universe leaves `draft`, and the warmup path pins a hash at freeze. That is
right for a cohort whose interpretability depends on never changing, and wrong
for a research set that must grow. Reusing it would force either freezing a set
we need to extend, or parking a universe permanently in `draft` and quietly
weakening the guarantee the frozen 25 depend on.

## 3. The history requirement — verified, not assumed

The remembered figure was "~540". The canonical requirement is not a daily bar
count at all:

```
CANDIDATE_MIN_DAILY_BARS          175
CONTROL_MIN_DAILY_BARS            200
CANDIDATE_MIN_MONTHLY_PERIODS      24   <- BINDS
IMPLIED_DAILY_SESSIONS_FOR_MONTHLY 504   <- the practical daily floor
```

`RESEARCH_MIN_DAILY_BARS = 504`, read from `app/prospective_readiness.py`
rather than restated. The frozen 25 hold 521 completed sessions, which exceeds
504 by roughly the sessions elapsed since they were warmed — that agreement is
the check that 504 is the real number.

## 4. Prioritisation — lexicographic, explainable, never a score

Which discovered symbol gets the next ~90 seconds of provider time:

1. appears in **more discovery categories** at once
2. seen across **more separate market sessions**
3. **already partly cached** (cheaper to finish than to start)
4. more **recent** market session
5. better **rank** in the list that surfaced it
6. **alphabetical** — so the order is total and a re-run picks the same five

Absent on purpose: price, market cap, volume and change percent. All are in the
FMP payload; none may order anything, because this is not a place to launder a
restricted provider's values into a decision.

## 5. Bounds, and where the numbers come from

Every limit derives from one measured constraint, not from caution:
`MASSIVE_REQUESTS_PER_MINUTE = 5`, and this repository already paces warmup at
`HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH = 1` per
`HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS = 75` behind a machine-wide advisory
lock.

| Limit | Value | Why |
|---|---|---|
| new research symbols / run | 5 | ~6 minutes of provider wall time; more is a queue that grows |
| warmup symbols / run | 5 | same |
| provider requests / run | 12 | one fetch per symbol plus headroom for one retry each — counted and enforced |
| concurrent warmups | 1 | the warmup advisory lock is machine-wide; a second warmer would only collide |
| warmup attempts | 3 | matches `JOB_MAX_ATTEMPTS_DEFAULT` so operators reason about one number |
| cooldown | 60 min | longer than any transient provider condition, short enough to recover in a session |

## 6. Eager vs lazy

**Eager** (cheap, local, no provider): admission, state recomputation,
prioritisation, benchmark-relative context from stored SPY bars, the research
scan itself.

**Costly and therefore bounded**: history warmup — the only provider-touching
step, one symbol at a time.

**Lazy — deliberately not built**: earnings, news, SEC and analyst context for
research symbols. Fetching every catalyst source for every discovered symbol is
exactly the cost explosion this design exists to avoid; the staged order is
*history → research scan → only then* consider expensive context, and only for
symbols that survived. Nothing in this milestone fetches it.

## 7. Three concepts, still separate (migrations 025/026)

```
EVIDENCE PROVENANCE   observed_at + reference_session_date  (which tape)
ACTIONABILITY         session_date / first_actionable_session
OUTCOME ANCHOR        the actionable session, and only once it has CLOSED
```

Research outcomes live in their own descriptive domain
(`wave2_descriptive --research`) and never read or write a canonical pair
outcome. The anchor is the actionable session, never the reference session it
describes — anchoring a weekend snapshot on the Friday it describes would
measure a move we only learned about on Sunday.

## 8. Operating it

```bash
export MARKET_INTEL_DATABASE_URL='postgresql://smart_scanner_market_intel:…'

python -m ops.analysis.research_pipeline --admit          # bounded, no provider
python -m ops.analysis.research_pipeline --warm           # the only costly step
python -m ops.analysis.research_pipeline --scan           # local bars only
python -m ops.analysis.research_pipeline --report
python -m ops.analysis.wave2_descriptive --research
```

## 9. Daily-bar freshness — root cause and the missing third option

The audit found bars at 2026-08-26 against a latest completed session of
2026-08-28. The cause was operational and provable, not a defect:

```
SMART-SCANNER-DAILY-PIPELINE   enabled = FALSE, next_run_at = NULL
PROOF-DAILY-PIPELINE           enabled = true but PAUSED = true
history-refresh worker app     SUSPENDED since 2026-08-25
```

All 25 symbols sat on the same date with the same bar count — the signature of
"no run", not of "a run that half-worked". The 23 failed queue tasks are all
`history_refresh_http_409` (the documented shared cooldown) from the 2026-08-23
job that the 2026-08-26 job then superseded: old evidence, not a live fault.

The gap was that an operator had **no bounded way to run it** — enabling a
schedule is forbidden and hand-driving the durable queue means writing job rows
by hand. `ops/analysis/refresh_daily_history.py` is the missing third option:
it audits, then drives the **same** service the durable worker calls
(`history_incremental_refresh_execute_service`), with no second refresh path,
no second provider path and no schedule enabled.

Two things it learned from the server rather than guessing:

* the v2 contract binds the observed latest local **4H** session as well as the
  daily one, so the first call is rejected before any provider request and
  hands back the server's own value to restate;
* the provider cooldown is **longer than the configured 75s floor** (it
  includes the previous run's duration), so a fixed local sleep undershoots it
  and every symbol after the first defers forever. The command now waits
  exactly the `cooldown_remaining_seconds` the server reports.

```bash
ENABLE_SCHEDULER=false HISTORY_REFRESH_DATABASE_URL=... \
  python -m ops.analysis.refresh_daily_history --audit
ENABLE_SCHEDULER=false HISTORY_REFRESH_DATABASE_URL=... \
  python -m ops.analysis.refresh_daily_history --refresh
```

`ENABLE_SCHEDULER=false` for the length of one command — the service refuses
otherwise, correctly, and no schedule is ever enabled.

## 10. What is deliberately not here

* **No product surface.** Every research symbol exists because of an FMP
  discovery, whose plan is internal-research-only. A symbol becoming more
  interesting does not move a licence boundary, so neither research table is
  granted to the Product API's role and no router names them.
* **No UI.** There is no staging UI deploy target to deploy to — no Fly app, no
  `.vercel` link, no CI workflow. Documented rather than invented.
* **No catalyst fetching for research symbols** (§6).
* **No TradingView watchlist change.** AI Edge stays exactly as configured.


---

# Research operations V1 — admission, candidate quality, lifecycle

## 11. The canonical configuration is now an invariant, not a coincidence

The first research scan's `config_hash` matched the canonical experiment's, and
that was luck: the config read went through the global pool (a different
database for an operator-run process), the read failed, `resolve_pattern_config`
fell back to strategy defaults — and staging happens to store no override for
`wyckoff_mtf_v2`, so both landed on the same defaults.

`bound_config_connection(conn, require_db=True)` binds resolution to the
connection the caller already holds and turns the silent fallback into
`ConfigUnavailable`. A context variable rather than a parameter, deliberately:
the parameter would have to thread through `_resolve_arm` in
`app/workers/shadow/runner.py`, which several phase-boundary tests assert is
unmodified — correctly, since it is the canonical execution layer. The change
lives entirely in `app/workers/patterns/config.py`.

**It worked the first time it ran.** The lifecycle stopped with
`blocked_canonical_config_unavailable` because `smart_scanner_market_intel` had
no privilege on `pattern_configs`. Under the old lenient resolver that would
have been invisible.

## 12. Admission — reject before spending a request

Three of three symbols in the first cohort failed the same hard gate,
`price_below_minimum`, after their history had been bought. All three were
knowable beforehand.

`min_price` is **read from the canonical resolved config** (5.0 by default);
this module carries no threshold of its own, so admission and the strategy
cannot drift apart. The comparison is `price < min_price` — a price exactly at
the minimum is admitted, matching the strategy's own gate.

Price sources, tried in order, neither costing a request:

| source | licence |
|---|---|
| `local_daily_bars` — last close we already hold | ours, unrestricted |
| `discovery_snapshot` — the price FMP already sent | **internal research only** |

Three outcomes, and "unknown" **proceeds**: rejecting on absent evidence would
silently filter out the symbols our data is weakest on.

**Licensing consequence, stated rather than discovered later:** when admission
rejects on a discovery-snapshot price, the *decision itself* is derived from
restricted data. Contained today because the research domain has no product
surface; `admission_price_source` is stored on the row so that if research is
ever exposed, the affected decisions are identifiable rather than needing
re-derivation.

## 13. Scanned is not "worth a look"

The previous report called three hard-AVOID symbols candidates because their
discovery reasons were strong. Two columns now, never one:

```
looked_because   discovery facts  -> WHY WE LOOKED
screen_findings  strategy evidence -> WHETHER IT SURVIVED
```

`candidate_state` ∈ `research_candidate` | `scanned_not_candidate` |
`insufficient_data` | `unavailable`. A `rejection_reason` ends the matter and
is reported alone — listing "structure present" beside "rejected on price"
would invite weighing one against the other. Being a candidate is **not** ENTER
or WATCH; it means the screen did not disqualify it and there is something to
read.

## 14. Staging scheduling — DISABLED, and why

The mission permitted a recurring staging lifecycle only if it cannot reach
production. It cannot be separated safely, so it is not enabled:

* `job_schedules` is shared with the production-equivalent
  `SMART-SCANNER-DAILY-PIPELINE`, which must stay off;
* the scheduler leader is the pipeline-driver app, and its role holds **no**
  privilege on the research tables — verified live:
  `research_symbols` SELECT `f`, INSERT `f`, `daily_bars` INSERT `f`. It could
  materialise a research occurrence and never execute it.

So the **dispatch path** is built and proven — one bounded, idempotent,
lease-protected command — and scheduling stays off. Schedules were left exactly
as found: `SMART-SCANNER-DAILY-PIPELINE` disabled, both proof schedules paused.

## 15. The lifecycle, and its gates

```
latest completed session -> [gate] core bars fresh? -> discovery -> admit
  -> admission (0 provider calls) -> bounded warmup (the ONLY provider stage)
  -> readiness -> scan (local bars only) -> candidate classification -> summary
```

The freshness gate BLOCKS. A research scan against benchmark bars that end
three sessions early produces a relative-strength category that looks like
evidence and is not, so `blocked_stale_core_history` stops the run rather than
degrading it.

```bash
python -m ops.analysis.research_lifecycle --run --dry-run   # admission only
python -m ops.analysis.research_lifecycle --run --admit-limit 20 --warm-limit 7
python -m ops.analysis.research_lifecycle --summary
```

## 16. Measured funnel (live, 2026-08-30, session 2026-08-28)

```
DISCOVERED (admitted to research)             45
  ADMISSION PASSED                            15
  admission REJECTED (no provider call)       30
HISTORY READY                                  8
RESEARCH SCANNED                               7
  RESEARCH CANDIDATE                           1     <- ONDS
  scanned, not a candidate                     6
unavailable                                    2
```

7 provider requests spent, **30 avoided**, 3,045 bars inserted, 393 s
wall-clock. Admission changed the *character* of the cohort, not just its size:
the survivors are INTC, BITO, IBIT, TSLL, ONDS rather than sub-dollar movers.

## 17. Lazy catalyst enrichment — DEFERRED, with the boundary found

The stage exists, is capped at 10, and runs only on survivors. It fetches
nothing, and the reason is architectural rather than cautious: the live run
called `refresh_sec_filings` for its one survivor and row-level security
**correctly** refused. `refresh_sec_filings` writes `catalyst_source_state`
under `sec_edgar` — the SHARED freshness row the Product API reads to decide
whether the SEC dimension is trustworthy *for the frozen 25*. A research run
over one symbol writing "sec_edgar: ok, symbols_covered=1" would tell the
product its coverage was fresh when it had been refreshed for a symbol the
product cannot see.

So the catalyst infrastructure does assume one cohort — not in its symbol list,
which is arbitrary, but in its freshness accounting, which is the part that
matters. Enabling this needs a per-cohort source-state identity
(`sec_edgar:research`), which is a catalyst-domain contract change and is worth
doing when there is more than one survivor to enrich.
