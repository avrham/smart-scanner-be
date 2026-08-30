# External intelligence — Wave 2

Companion to `docs/external-intelligence-hub-runbook.md` (Wave 1: pushed
signals). Wave 1 answered *"what did another system claim about this symbol"*.
Wave 2 answers three questions it structurally could not.

| Question | Answer | Where it lands | Displayable? |
|---|---|---|---|
| What is the wider market watching that our 25 cannot see? | FMP market movers | `external_discovery_candidates` | **No** — internal research only |
| What scheduled market-wide event is close? | Federal Reserve + BEA calendars | `macro_events` | **Yes** |
| Who changed their mind about one of our 25? | FMP analyst grade changes | `analyst_grade_events` | **No** — internal research only |

Everything here sits **beside** the strategy. Nothing in this wave changes a
verdict, an attention tier, an ordering, a confluence reading, a market-regime
classification, or the frozen 25-symbol universe.

---

## 1. Measured entitlements (2026-08-30, against the live key/network)

Recorded because the next person to ask should not have to re-probe, and
because two of these results decided the shape of the whole wave.

### Financial Modeling Prep — `/stable` (`/api/v3` is dead: HTTP 403 "Legacy Endpoint")

| Endpoint | Status | Used |
|---|---|---|
| `biggest-gainers` / `biggest-losers` / `most-actives` | 200 | yes — discovery |
| `grades?symbol=` | 200 (full history, back to 2012) | **yes — analyst change events** |
| `grades-consensus`, `grades-historical`, `ratings-snapshot` | 200 | no (state, not change) |
| `price-target-summary`, `price-target-consensus` | 200 | no (state, not change) |
| `price-target-latest-news` | 200 but `limit` capped at **10** | no — cannot cover 25 symbols |
| `price-target-news?symbol=` | **402** | blocked |
| `analyst-estimates` | 200 | deferred (needs accumulated snapshots) |
| `ratios`, `ratios-ttm`, `key-metrics`, `key-metrics-ttm`, `financial-growth`, `profile` | 200 | no — see §4 |
| `holidays-by-exchange`, `all-exchange-market-hours`, `treasury-rates`, `earnings-calendar`, `dividends-calendar`, `sector-performance-snapshot` | 200 | no |
| `company-screener`, `stock-list`, `economic-calendar` | **402** | blocked |

No unusual-volume feed exists on this plan; the screener that could express one
is 402.

### Macro publishers

| Source | Status | Used |
|---|---|---|
| `federalreserve.gov/monetarypolicy/fomccalendars.htm` | 200 | **yes** |
| `bea.gov/news/schedule` | 200 | **yes** |
| `census.gov/economic-indicators/calendar.html` | 200 but JS-rendered; the XML feed is *released*, not scheduled | deferred |
| `www.bls.gov` — **every path**, including `/robots.txt`, from two independent clients | **403** | **blocked** |
| `api.bls.gov/publicAPI/v2` | 200 — time series only, **no release calendar** | not a substitute |

**CPI, PPI, nonfarm payrolls and the unemployment rate are therefore not
modelled.** They are blocked, not deferred: the primary publisher refuses this
network and its API carries no schedule. They are deliberately absent from the
`macro_events.event_type` CHECK constraint — an event type we cannot populate
would read as coverage.

---

## 2. Licensing: two classes of source, enforced three ways

Every source carries `licensing_visibility` ∈
`product_display_allowed` | `internal_research_only` | `unknown_restriction`.
**Only the first may be displayed.** `unknown_restriction` is treated exactly
like internal-only — the absence of an established position is not permission.

| Source | Class | Why |
|---|---|---|
| `tradingview`, `ai_edge` | product_display_allowed | the owner's own alert, on their own chart, reaching their own single-tenant deployment |
| `federal_reserve`, `bea` | product_display_allowed | works of the U.S. Government are not subject to copyright protection in the United States (17 U.S.C. §105) |
| `fmp` | **internal_research_only** | individual plans are personal/non-commercial and forbid integrating the data into tools accessible by third parties |
| `trendspider` | internal_research_only | terms forbid redistribution and third-party access |
| `finviz`, `koyfin`, `openbb` | unknown_restriction | no published terms / no established position |

Enforced in three independent places, because any one can be forgotten:

1. **The database role.** `smart_scanner_product_reader` holds no privilege on
   `external_discovery_candidates`, `analyst_grade_events` or
   `external_signal_deliveries`. A router that tried would get
   `permission denied for table`.
2. **The registry filter.** `lic.product_visible_rows` runs in the Product
   API's `_load_external`, so a restricted source is not even *named* to a
   reader.
3. **A test that reads the code.** `tests/test_source_licensing.py` parses the
   routers (docstrings stripped, SQL literals kept) and asserts the forbidden
   relation names do not appear.

Paraphrasing a restricted value into a product field breaches the same term —
the licence restricts the data, not the spelling.

---

## 3. What was built

```
app/source_licensing.py     the vocabulary, the predicate, the forbidden relations
app/macro_calendar.py       PURE reading layer: proximity, point-in-time, the block
app/macro_ingest.py         the two calendar parsers + refresh (network + DB)
app/analyst_events.py       grade-change normalisation, persistence, research reads
app/external_discovery.py   + provenance, + A3 aggregation, + A4 cross-reference
app/db/migrations/024_market_calendar_and_analyst.sql
ops/sql/create_smart_scanner_market_intel.sql   least-privilege ingestion role
ops/sql/verify_smart_scanner_market_intel.sql
ops/analysis/refresh_macro_calendar.py
ops/analysis/refresh_analyst_grades.py
ops/analysis/wave2_descriptive.py               descriptive outcomes, never alpha
```

Three separate tables rather than three columns on `external_signals`, because
they are three different kinds of object:

* a **signal** is an opinion about a price series — it has a direction and can
  agree or disagree with our reading;
* a **scheduled event** is a fact about the calendar — no direction, no symbol,
  true for all 25 rows at once;
* a **grade change** is a third party's published action on one company — a
  subject and a date, no timeframe and no view of the chart.

Folding any into `external_signals` would let a calendar entry acquire a
`direction` it does not have, and "CPI is Thursday" would start counting as a
source agreeing with the scanner.

### AI Edge semantic hygiene (the small fix)

`ai_edge` is `live` and has never fired. `evaluate_freshness` reported that as
`unavailable / never_refreshed`, rendered identically to a source we cannot
see. Wave 2 adds `STATUS_CONNECTED` + `REASON_AWAITING_FIRST_SIGNAL`, so the
product says **"Connected — no signal has fired yet."**

Deliberately unchanged: no signal presence is fabricated, and **confluence is
identical to the unavailable case** (`CONFLUENCE_UNAVAILABLE`). There is no
external reading; letting the connected path fall through to the normal branch
would turn "we have heard nothing" into `internal_only`, which is a claim about
the evidence rather than about the plumbing. No ranking or attention change.

---

## 4. What was deliberately NOT built

* **Static fundamentals** (P/E, margins, growth, market cap). Entitled and
  cheap. Not built: they answer "what is true this quarter", not "what changed
  today", and with FMP restricted to internal use they would have had no
  product surface to justify the weight on a screen already carrying six
  evidence dimensions.
* **Price-target change events.** Per-symbol endpoint is 402; the market-wide
  feed caps `limit` at 10, which cannot cover 25 symbols. Blocked by plan.
* **Estimate revisions.** `analyst-estimates` is entitled, but a *revision*
  requires accumulated daily snapshots. Deferred, not blocked.
* **A `/api/scanner/discovery` endpoint.** Would have returned FMP data.
  Blocked by licence, not by effort — if a display licence is acquired,
  surfacing it is additive and nothing has to move.
* **Any opportunity/macro/master score.** No.

---

## 5. Operating model

All three refreshes are pull-based and run on the **internal worker path** —
never through the internet-facing ingress app, whose role stays append-only and
untouched.

| Source | Transport | Cadence |
|---|---|---|
| `ai_edge` / `tradingview` | webhook (Wave 1) | event-driven; no refresh exists |
| `macro_calendar_refresh.v1` | outbound HTTPS, no credential | daily |
| `analyst_grades_refresh.v1` | outbound HTTPS, FMP key | daily, after the campaign |
| `external_discovery_refresh.v1` | outbound HTTPS, FMP key | market-session |

Declared in the **disabled** `SMART-SCANNER-DAILY-PIPELINE` template's `stages`
list, which is documentation of intent rather than executable dispatch. **No
schedule is enabled by this wave.**

```bash
# one-off, against the isolated staging Postgres
export MARKET_INTEL_DATABASE_URL='postgresql://smart_scanner_market_intel:…'

python -m ops.analysis.refresh_macro_calendar   --refresh --report
python -m ops.analysis.refresh_analyst_grades   --refresh --lookback-days 730
python -m ops.analysis.refresh_analyst_grades   --report --days 30
python -m ops.analysis.refresh_discovery_candidates --refresh --cross-reference
python -m ops.analysis.wave2_descriptive --analyst --macro --discovery
```

`MARKET_INTEL_DATABASE_URL` is a SECRET and is never committed or logged; the
connection helper asserts `current_user` and refuses anything else.

---

## 6. Failure isolation

Each dimension fails alone, and each failure is *named* rather than folded into
a count:

* one calendar publisher changing its markup costs that publisher's events and
  nothing else — the other calendar, the scan, the catalysts and the external
  signals all keep working;
* one FMP symbol failing costs that symbol, not the other twenty-four;
* an empty parse **raises** `unparseable` rather than returning an empty
  calendar, because "no meetings found" and "the Fed has no meetings" must
  never be the same answer;
* a missing credential is `unavailable`, never an error;
* the Product API loads the calendar in its own `try/except`, separate from the
  external-signals one — they fail for unrelated reasons.

---

## 7. Point-in-time

| Layer | Gate | Why |
|---|---|---|
| Macro events | `first_observed_at <= session close` | the schedule is public, but we only know what we actually read; a meeting added on Tuesday must not appear in Monday's context |
| Analyst grades | `session_date` = first trading session **strictly after** the event date | the provider publishes a date and no clock, so same-session actionability cannot be established and is never assumed |
| Discovery | `session_date` from the shared market clock | unchanged from Wave 1 |

The analyst rule forfeits up to a day of edge on purpose, in exchange for a
guarantee: no measurement built on those rows can be reading an outcome it
could not have traded.

---

## 8. Descriptive measurement — and its honest limit

`ops/analysis/wave2_descriptive.py` reports 1D/3D/5D/10D/20D forward behaviour
for analyst upgrades/downgrades (on the 25), for `SPY` around macro events, and
for discovered movers. It tunes no threshold, claims no alpha, and feeds
nothing.

The limit is structural and is printed beside every figure: `daily_bars` holds
the frozen 25 plus the reference market and nothing else, so **most discovered
symbols cannot be measured at all**. That is not a gap to paper over — it is
precisely the finding the discovery path exists to surface.
