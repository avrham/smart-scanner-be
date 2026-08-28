# External Intelligence Hub V1 — runbook

The first Smart Scanner surface that accepts data **pushed in from the public
internet**. Everything else in this system either pulls from a provider we
chose, or serves a read-only view. This one has a socket a stranger can reach,
and every decision below follows from that.

**What it is for:** recording what systems outside Smart Scanner claim about a
symbol, with enough provenance and point-in-time honesty that we can later
*measure* whether those claims were worth anything.

**What it is not for:** improving the scanner's verdict. An external signal
cannot reach the Wyckoff verdict, the attention tier, the ordering or ENTER
eligibility. There is no confluence score, and there is no broker.

---

## 1. Architecture in one picture

```
TradingView alert (AI Edge, or any indicator)
        │  HTTPS POST, fixed body, no custom headers possible
        ▼
smart-scanner-be-external-ingest-staging     EXTERNAL_INGEST_ONLY_MODE
  POST /api/external/signals                 ← the ONLY write in the system
        │  token → size → rate limit → parse → contract → clock → replay
        ▼
  role: smart_scanner_external_ingest        INSERT on 2 tables, no DELETE,
                                             zero privilege on any scanner table
        ▼
  external_signal_deliveries  (raw audit, replay-protected)
  external_signals            (normalised, APPEND-ONLY)
        │
        ▼  read-only, different app, different role
smart-scanner-be-staging                     AUDIT_ONLY_MODE (GET only)
  GET /api/scanner/overview  → external_intelligence summary
  GET /api/scanner/symbol    → external_intelligence block
        ▼
smart-scanner-ui                             External Signals panel
```

### Why two apps

The Product API runs `AUDIT_ONLY_MODE` with a GET-only allowlist, and connects
as `smart_scanner_product_reader`, whose sessions are
`default_transaction_read_only`. A webhook is a POST that INSERTs. Putting them
together would mean either widening the product role to write, or widening the
audit gate to accept POST. Both undo guarantees that took real work to
establish, so the ingress is its own app with its own role — the same isolation
pattern already used for audit, maintenance, warmup, prospective and the worker.

---

## 2. The alert contract

`smart_scanner_tradingview_signal.v1`

| field | required | notes |
|---|---|---|
| `contract_version` | **yes** | must equal `smart_scanner_tradingview_signal.v1` |
| `source` | no | `ai_edge` or `tradingview`; defaults to `tradingview` |
| `symbol` | **yes** | `{{ticker}}`. `EXCHANGE:SYMBOL` is accepted and split |
| `signal_type` | **yes** | the source's own word for what the alert IS |
| `direction` | no | the source's own word; never forced into buy/sell |
| `timeframe` | no | `{{interval}}` |
| `source_timestamp` | no | `{{timenow}}` — the FIRE time, not the bar time |
| `indicator`, `indicator_version`, `alert_id` | no | provenance |
| `confidence` + `confidence_scale` | no | **both or neither** (see §6) |
| `source_signal_id` | no | the source's own identity, if it has one |
| `metadata` | no | bounded free-form object |

Anything unrecognised is refused with a short stable code. The gateway does not
accept a bare text alert and does not accept arbitrary JSON: an unvalidated
payload produces rows whose meaning nobody can reconstruct later.

### Confirmed TradingView placeholders

Verified against official documentation. Use only these:

`{{ticker}}` `{{exchange}}` `{{interval}}` `{{time}}` `{{timenow}}` `{{open}}`
`{{high}}` `{{low}}` `{{close}}` `{{volume}}` `{{plot_0}}`…`{{plot_19}}`
`{{plot("title")}}` `{{syminfo.currency}}` `{{syminfo.basecurrency}}`

Strategy alerts additionally have `{{strategy.order.action}}`,
`{{strategy.order.contracts}}`, `{{strategy.position_size}}` and friends.

> **Do not** use `{{syminfo.description}}`, `{{syminfo.country}}`,
> `{{barstate.*}}`, `{{session.*}}` or `{{earnings.*}}`. Those circulate widely
> on forums but are Pine **built-in variables**, not alert placeholders — they
> will not expand, and the gateway will refuse the resulting body.

Two behaviours worth knowing before debugging a bad row:

* `{{time}}` is the **bar open** time; `{{timenow}}` is when the alert fired.
  Only `{{timenow}}` belongs in `source_timestamp` — using `{{time}}` would
  place a 4H signal up to four hours in the past.
* `{{exchange}}` gains a `_DL` / `_DLY` suffix on delayed data. The gateway
  records it verbatim, and also stores `exchange_venue` and `data_delayed`.

---

## 3. TradingView setup (the account owner must do this)

Indicator alerts cannot be created through any API — TradingView exposes no
write API at all — so this part is manual, once per condition per symbol.

**Prerequisites**

* A paid TradingView plan (Essential or above). Webhooks are not on Free.
* Two-factor authentication enabled on the TradingView account. This is a hard
  requirement for webhook alerts, not a recommendation.
* The ingress URL and token from §4.

**Steps**

1. Open the chart, add the indicator, set the timeframe.
2. Right-click → **Add alert** (or the alarm-clock icon).
3. **Condition**: the indicator, then the specific alert condition.
4. **Trigger**: `Once Per Bar Close`. Anything else fires on an unconfirmed bar
   and will deliver signals that the chart later erases.
5. **Expiration**: below Premium, alerts expire after two months. Diarise it —
   a silently expired alert looks exactly like an indicator that stopped firing.
6. **Notifications → Webhook URL**:
   `https://smart-scanner-be-external-ingest-staging.fly.dev/api/external/signals?token=<INGRESS_TOKEN>`
7. **Message**: paste one template from §3.1 or §3.2 verbatim.

### 3.1 AI Edge — the eight open-source alert conditions

The indicator uses `alertcondition()`, which means the message is fully
user-editable and there is **no** "Any alert() function call" option. Each
condition needs its own alert.

<details open>
<summary><strong>Open Long</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"open_long","direction":"bullish","indicator":"lorentzian_classification","alert_id":"aiedge-open-long","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Close Long</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"close_long","direction":"bearish","indicator":"lorentzian_classification","alert_id":"aiedge-close-long","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Open Short</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"open_short","direction":"bearish","indicator":"lorentzian_classification","alert_id":"aiedge-open-short","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Close Short</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"close_short","direction":"bullish","indicator":"lorentzian_classification","alert_id":"aiedge-close-short","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Open Position</strong> (direction-agnostic)</summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"open_position","indicator":"lorentzian_classification","alert_id":"aiedge-open-position","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```

No `direction` field on purpose: the condition fires either way, and supplying
one would record a claim the indicator never made. It normalises to `unknown`.
</details>

<details>
<summary><strong>Close Position</strong> (direction-agnostic)</summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"close_position","indicator":"lorentzian_classification","alert_id":"aiedge-close-position","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Kernel Bullish Color Change</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"kernel_bullish_color_change","direction":"bullish","indicator":"lorentzian_kernel","alert_id":"aiedge-kernel-bull","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

<details>
<summary><strong>Kernel Bearish Color Change</strong></summary>

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"ai_edge","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"kernel_bearish_color_change","direction":"bearish","indicator":"lorentzian_kernel","alert_id":"aiedge-kernel-bear","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}"}
```
</details>

A kernel colour change normalises to `trend`, not to an entry — it is a regime
reading that flipped, and promoting a filter to a trade signal would misstate
what the indicator said.

### 3.2 Any other TradingView indicator

`source: "tradingview"` needs no backend change, no new endpoint and no
deployment. Set `indicator` and `indicator_version` so sources stay
distinguishable.

```json
{"contract_version":"smart_scanner_tradingview_signal.v1","source":"tradingview","symbol":"{{ticker}}","exchange":"{{exchange}}","timeframe":"{{interval}}","signal_type":"breakout","direction":"bullish","indicator":"my_script_name","indicator_version":"v1","alert_id":"tv-breakout-daily","source_timestamp":"{{timenow}}","bar_time":"{{time}}","price":"{{close}}","metadata":{"plot":"{{plot_0}}"}}
```

---

## 4. Deployment

### 4.1 Database (once, as the Postgres admin)

```bash
export DBNAME=warmup
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f app/db/migrations/022_external_signals.sql

psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 \
  -v ingest_password="$INGEST_DB_PASSWORD" -v db_name="$DBNAME" \
  -f ops/sql/create_smart_scanner_external_ingest.sql

# The Product API role gains SELECT on the two new product-facing relations.
# Re-runnable; it does NOT change the existing password.
psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -v db_name="$DBNAME" \
  -f ops/sql/create_smart_scanner_product_reader.sql
```

Verify — **every assertion section must return zero rows**:

```bash
psql "$INGEST_DSN"  -f ops/sql/verify_smart_scanner_external_ingest.sql
psql "$PRODUCT_DSN" -f ops/sql/verify_smart_scanner_product_reader.sql
```

The ingest verifier proves the thing that actually matters: the role holds no
privilege of any kind on any of the 19 scanner relations, holds no DELETE
anywhere, and its policies on the shared freshness table are confined to the
`external_` namespace — so a leaked ingress credential cannot mark the SEC or
news dimension as failed.

### 4.2 The ingress app

```bash
export APP=smart-scanner-be-external-ingest-staging
fly apps create $APP --org <org>

fly secrets set --app $APP --stage \
  EXTERNAL_INGEST_DATABASE_URL="$INGEST_DSN" \
  EXTERNAL_INGEST_TOKEN="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 48)" \
  WORKER_TOKEN="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 40)"

fly deploy --config fly.external-ingest.toml --app $APP \
  --build-arg APP_GIT_SHA="$(git rev-parse HEAD)"
```

`min_machines_running = 1` and `auto_stop_machines = 'off'` are deliberate and
must not be "optimised". TradingView allows a webhook three seconds and
documents no retry, so a cold start is a lost alert.

---

## 5. Verifying it works

```bash
curl -s https://$APP.fly.dev/api/external/health | jq
curl -s https://$APP.fly.dev/api/external/sources | jq '.sources[] | {source,status}'

# A real delivery.
curl -s -X POST "https://$APP.fly.dev/api/external/signals" \
  -H 'Content-Type: application/json' \
  -H "X-Smart-Scanner-Token: $EXTERNAL_INGEST_TOKEN" \
  -d '{"contract_version":"smart_scanner_tradingview_signal.v1",
       "source":"tradingview","symbol":"AAPL","timeframe":"240",
       "signal_type":"breakout","direction":"bullish",
       "indicator":"ingress_smoke_test"}' | jq
```

Then confirm it reaches the product:

```bash
curl -s "https://smart-scanner-be-staging.fly.dev/api/scanner/symbol?symbol=AAPL" \
  | jq '.external_intelligence | {status, in_window_count, confluence, items}'
```

### Reading a refusal

| code | HTTP | what happened |
|---|---|---|
| `unauthorized` | 401 | bad/absent token — identical for every cause, on purpose |
| `rate_limited` | 429 | per-process sliding window |
| `payload_too_large` | 413 | body above `EXTERNAL_INGEST_MAX_PAYLOAD_BYTES` |
| `malformed_json` | 400 | not JSON |
| `unsupported_contract_version` | 422 | wrong or missing `contract_version` |
| `unknown_source` | 422 | `source` not a webhook-capable registry entry |
| `invalid_symbol` | 422 | failed the symbol shape check |
| `unsubstituted_placeholder` | 422 | `{{ticker}}` arrived literally |
| `missing_signal_type` | 422 | no `signal_type` and no `event` |
| `timestamp_out_of_window` | 422 | `source_timestamp` more than 30 min from arrival |
| `ingest_unavailable` | 503 | authentic delivery, storage failed — retry is meaningful |

401/429/413 are refused **before** anything is written; everything else is
recorded in `external_signal_deliveries` with its reason, which is what makes
"the alert fired, why is nothing showing?" answerable.

`status: duplicate` is a **success**, returned 200. A repeated delivery has
already been recorded, and erroring would invite a retry storm that changes
nothing.

---

## 6. The rules that are not negotiable

**The gate is arrival, not the source's clock.** `effective_at = received_at`,
always. The source's `observed_at` is recorded as evidence and displayed, and
the disagreement is stored in `clock_skew_seconds` rather than hidden. A
backdated `source_timestamp` — malicious, or just a chart in the wrong timezone
— would otherwise manufacture lookahead that looks exactly like a finding.

**Confidence is never invented.** If a source supplies no confidence, the
column is NULL and the product says `unavailable`. It is not 0.5, not derived
from the signal type, not back-filled. A `confidence` sent without a
`confidence_scale` is preserved in metadata as `unscaled_confidence` and is
**not** promoted — a number whose meaning is unknown is not a measurement.

For AI Edge specifically, confidence is unavailable as a **measured fact**: the
indicator's internal vote score is rendered with `label.new()`, and
`{{plot_N}}` can only read `plot()` output. There is no path by which a real
confidence reaches an alert message.

**The signal table is append-only.** Duplicates collapse on a UNIQUE
constraint. A correction is a NEW row carrying `supersedes_signal_id`, and
supersession is derived at read time. There is no `superseded_at` column to
UPDATE, and that single omission is what lets the ingest role hold INSERT and
nothing else. A correction only hides its target if the correction itself was
visible to the session being viewed — otherwise a later fix would retroactively
erase what we were actually looking at on the day.

**Confluence is a word, never a number.** `agreement`, `disagreement`, `mixed`,
`external_only`, `internal_only`, `no_external_signal`, `unavailable`. The
point of this milestone is to *measure* whether confluence has value;
publishing "3/4 confirmations" would answer that question by assertion, and the
number would immediately be read as a probability. A chatty indicator firing
five bullish alerts does not outvote one bearish source — distinct readings are
counted, not signals.

**Symbols outside the frozen universe are quarantined, not rejected.** An
external scanner will name symbols our 25-symbol experiment never inspects.
Rejecting them throws away "what else should we investigate?"; accepting them
into the scanner surface corrupts a live experiment. So they are stored with
`symbol_scope = 'external_discovery'` and never reach the product surface. If
the universe lookup fails, classification degrades to `external_discovery` —
the conservative direction.

---

## 7. Measuring it (the reason this exists)

`external_signal_session_links` is a VIEW, not a table: no writer, no backfill,
no cached association that could drift from the rule. It attaches a signal to a
scanner session iff it arrived before that session's 16:00 America/New_York
close — the same clock `app.news.session_close_utc` uses, so the SQL and Python
paths cannot disagree.

Joining it to the existing outcome infrastructure gives the comparison this
whole milestone was built for, with **no change to canonical market-path
semantics**:

```sql
SELECT l.source, l.direction_normalized,
       e.verdict AS candidate_verdict,
       o.return_5d, o.return_10d, o.return_20d
FROM public.external_signal_session_links l
JOIN public.strategy_shadow_evaluations e ON e.pair_id = l.pair_id
JOIN public.strategy_shadow_pair_outcomes o ON o.pair_id = l.pair_id
WHERE l.session_date = e.…;
```

The cohorts to accumulate: Wyckoff WATCH alone, external signal alone, both
agreeing, both disagreeing. **Do not tune the windows against the outcomes.**
The windows are fixed a priori from product meaning and the raw timing stays
visible precisely so that a fitted result cannot be mistaken for a finding.

---

## 8. Known limits

* **At-most-once delivery.** TradingView documents no retry. A missed delivery
  is missed; there is no backfill, because TradingView has no read API.
* **Alert expiry.** Below Premium, alerts die after two months and must be
  re-armed. This looks identical to a quiet indicator.
* **Per-process rate limiting.** Honest, and adequate for a single-machine
  ingress. The UNIQUE constraints, not the limiter, protect data integrity.
* **Byte-identical distinct alerts collapse.** Two genuinely different firings
  with identical bodies are indistinguishable from a retry. Every practical
  alert body carries a timestamp, so this costs nothing real.
* **No historical import.** Every signal starts from the day the alert was
  armed.
* **A quiet source is not a broken source.** An indicator that has not fired is
  behaving correctly. Freshness only reports `stale` after seven days.
