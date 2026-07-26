# Wyckoff MTF v2 — Prospective Shadow Campaign Runbook

Manual, copy-paste-safe procedure for running ONE prospective Wyckoff v2
shadow campaign after a completed US market session, plus the three-session
acceptance checklist. Everything is shadow-only and manual — there is NO daily
scheduler and no automation in this runbook.

The stored rollout defaults never change (verify before and after every run):

```text
patterns.is_enabled = false
allow_enter        = false
enable_4h_trigger  = false   # global default stays false
min_price          = 5.0
```

The only 4H-trigger analysis that ever runs is the frozen experiment-local
override inside `wyckoff_v2_vs_baseline` (`enable_4h_trigger: true` applied to
the CANDIDATE arm inside the shadow run only). `allow_enter` is never
overridable. A confirmed trigger produces, at most, a rollout-blocked WATCH.

All admin calls require the worker token header:

```text
X-Worker-Token: <operator token from the deployment secret store>
```

---

## Prerequisite — supply and freeze the exact 50-symbol experiment file

The repository intentionally has **no implicit universe and no hidden default
list**. Before the first prospective campaign you MUST create the frozen
experiment universe file yourself. Do not use placeholder or guessed tickers.

1. Create `experiment_universe.txt` (newline- or comma-delimited, `#` comments
   allowed) containing the EXACT 50 tickers for the experiment.
2. **Validate** it. This step FAILS (non-zero exit) when the file is empty,
   has invalid tickers, contains duplicates, or does not hold exactly 50 unique
   valid symbols. It uses the same canonicalization campaign creation applies
   (trim, upper-case, reject malformed, dedupe, sort) and reports duplicates
   explicitly rather than silently cleaning them:

   ```bash
   python -c "
   import sys
   from app.workers.shadow.universe_identity import (
       parse_symbol_file_text, inspect_universe_symbols)
   text = open('experiment_universe.txt').read()
   report = inspect_universe_symbols(
       parse_symbol_file_text(text), expected_count=50)
   print('unique_count       :', report['unique_count'])
   print('duplicates_supplied:', report['duplicates_supplied'])
   print('invalid_tokens     :', report['invalid_tokens'])
   print('universe_hash      :', report['universe_hash'])
   print('symbols            :', ' '.join(report['symbols']))
   if not report['ok']:
       print('VALIDATION FAILED:', report['problems'], file=sys.stderr)
       sys.exit(1)
   print('OK')
   "
   ```

3. Confirm the command printed `OK`. **Record `universe_hash`** — this is the
   frozen universe identity. Keep the file byte-unchanged across the first
   three sessions and pass it explicitly to every campaign.

The first prospective campaign is not part of the frozen-universe evidence
cohort until this file exists, validates, and its hash is recorded.

## 1. Resolve the session and run the duplicate preflight (one call)

Do NOT use `GET /health`'s `latest_daily_bar_date` as the session — that is a
bare `MAX(trading_date)` that may be an in-progress partial bar. Instead use the
read-only preflight, which resolves the latest COMPLETED session via the frozen
`ny_session_close.v1` policy (stepping back to the prior real session if the
latest bar is still partial) AND checks for an equivalent existing campaign:

```bash
SYMBOLS_CSV=$(python -c "
from app.workers.shadow.universe_identity import parse_symbol_file_text
print(','.join(parse_symbol_file_text(open('experiment_universe.txt').read())))
")

curl -s -G "$HOST/api/admin/shadow-campaign-preflight" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  --data-urlencode "experiment_code=wyckoff_v2_vs_baseline" \
  --data-urlencode "symbols=$SYMBOLS_CSV" \
  --data-urlencode "expected_count=50"
```

Read from the response:

* `resolved_session` — use this as `AS_OF` (a valid, completed trading session);
* `resolution_reason` — `latest_bar_completed` or
  `latest_bar_partial_used_prior_session`;
* `universe_hash` — must equal your frozen hash;
* `existing_campaign_match` and `matching_campaign_id`;
* `creation_safe` and `reasons`.

Proceed to create ONLY when `creation_safe == true`. Otherwise:

* `matching_completed_campaign` → audit it (step 4), do not recreate;
* `matching_resumable_campaign` → resume it (step 3);
* `same_session_membership_mismatch` → your file differs from the persisted
  campaign for that session — reconcile before doing anything;
* `matching_session_membership_unverifiable` → an existing campaign for that
  session has no persisted symbols; investigate, do not assume safe.

```bash
AS_OF=<resolved_session from the preflight, e.g. 2026-07-24>
```

## 2. Launch the campaign (explicit symbols, one session)

Submit the frozen universe explicitly. `max_symbols=50` is the required safety
bound; the runner chunks at 25 symbols/run automatically. No date range, no
backfill, no implicit universe.

```bash
SYMBOLS=$(python -c "
from app.workers.shadow.universe_identity import parse_symbol_file_text
import json
print(json.dumps(parse_symbol_file_text(open('experiment_universe.txt').read())))
")

curl -s -X POST "$HOST/api/admin/shadow-campaigns" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"experiment_code\":\"wyckoff_v2_vs_baseline\",
       \"symbols\":$SYMBOLS,
       \"max_symbols\":50,
       \"as_of_date\":\"$AS_OF\"}"
```

Record the returned `campaign_id`. Provider MUST be
`MARKET_DATA_PROVIDER=massive` (FMP records a typed `unsupported_provider` 4H
state and cannot produce trigger evidence). Migration 013 must already be
applied (otherwise `pair_error` rejections; re-run after applying — idempotent).

## 3. Resume a partial campaign safely

Re-submit the SAME payload with the SAME `campaign_id` semantics — because
symbols are normalized/sorted/deduped identically and pairs dedupe by
fingerprint, completed chunks are not re-created and only the missing coverage
is filled. Resuming never alters completed results.

```bash
# identical payload to step 2 — idempotent; fills only missing chunks
curl -s -X POST "$HOST/api/admin/shadow-campaigns" \
  -H "X-Worker-Token: $WORKER_TOKEN" -H "Content-Type: application/json" \
  -d "{\"experiment_code\":\"wyckoff_v2_vs_baseline\",
       \"symbols\":$SYMBOLS,\"max_symbols\":50,\"as_of_date\":\"$AS_OF\"}"
```

## 4. Audit the campaign after completion

```bash
GET /api/admin/shadow-campaigns/{campaign_id}/audit
```

Cross-check membership against the frozen file (strongest membership proof).
Build a clean comma-separated `expected_symbols` value with the same parser the
endpoint uses (this correctly ignores comments/blank lines — do NOT pipe the
raw file through `tr`, which would turn a leading `#` comment into junk):

```bash
EXPECTED_CSV=$(python -c "
from app.workers.shadow.universe_identity import parse_symbol_file_text
print(','.join(parse_symbol_file_text(open('experiment_universe.txt').read())))
")

curl -s -G "$HOST/api/admin/shadow-campaigns/$CAMPAIGN_ID/audit" \
  -H "X-Worker-Token: $WORKER_TOKEN" \
  --data-urlencode "expected_symbols=$EXPECTED_CSV"
```

The audit is `shadow_campaign_audit.v1` and returns a `verdict`:

* `valid` — terminal-successful, exact expected membership, no duplicates, no
  side effects, no systemic failure. **Zero confirmed triggers is valid.**
* `incomplete` — still resumable (some chunk not terminal). Go to step 3.
* `invalid` — a hard invariant failed; read `verdict_reasons`.
* `membership_unverifiable` — no persisted set and no explicit list supplied.

Confirm: `verdict == "valid"`, `unique_symbol_count == 50`,
`missing_symbols == []`, `duplicate_evaluations == {}`,
`membership_source == "persisted_campaign_symbols"` (and, with the frozen file
supplied, `explicit_vs_persisted_mismatch == false`).

## 5. Confirm no production side effects

From the same audit response, confirm all of:

```text
watches_created          == 0
decision_cards_created   == 0
allow_enter_true_count   == 0
pair_error_count         == 0
systemic_provider_failure == false
```

And confirm the rollout defaults are untouched:

```bash
GET /api/patterns            # must NOT list wyckoff_mtf_v2 (is_enabled=false)
```

The production scheduler stays on `sma150_bounce`; nothing here schedules a
shadow run or changes `allow_enter` / `enable_4h_trigger` globals.

## 6. Save the campaign identity for cohort analysis

Record in the review notes:

```text
campaign_id          : <from step 2>
session_date         : <AS_OF>
experiment_code      : wyckoff_v2_vs_baseline
experiment_version   : <audit.experiment_code/version>
config identity      : <audit identity_group_count == 1 expected>
universe_hash        : <frozen file hash from the prerequisite>
evaluated_universe_hash : <audit.evaluated_universe_hash — must match>
verdict              : valid
```

`evaluated_universe_hash` MUST equal the frozen `universe_hash`. Maturation of
this session's outcomes later uses the cohort closeout runbook
(`wyckoff-v2-cohort-closeout-runbook.md`).

---

## Three-session manual validation checklist

Run the procedure above for the first three completed prospective sessions,
using the SAME frozen `experiment_universe.txt` (same `universe_hash`) each
time. A session is **green** only when ALL of the following hold:

- [ ] the campaign completed (`campaign_status == "completed"`,
      `verdict == "valid"`);
- [ ] 50 of 50 symbols evaluated (`unique_symbol_count == 50`,
      `missing_symbols == []`);
- [ ] no duplicate campaign for the session, no duplicate evaluations
      (`duplicate_evaluations == {}`);
- [ ] no systemic pair errors (`pair_error_count == 0`);
- [ ] no fetch cascade (`provider_failure_count` within expected transient
      noise; `systemic_provider_failure == false`);
- [ ] daily readiness behaved as expected (`daily_ready_count` +
      `daily_not_ready_count == 50`, insufficient-history symbols explained);
- [ ] 4H frames were built where data allowed (`four_hour_ready_count`
      consistent with available intraday history; the rest are typed states,
      not silent gaps);
- [ ] rerunning / resuming did not alter completed results (re-submit the same
      payload; the audit verdict and counts are unchanged);
- [ ] no watches were created (`watches_created == 0`);
- [ ] no decision cards were created (`decision_cards_created == 0`);
- [ ] no entry permission was enabled (`allow_enter_true_count == 0`;
      `/api/patterns` still excludes `wyckoff_mtf_v2`).

Zero confirmed triggers across all three sessions is an acceptable result — do
NOT require any confirmed trigger.

Do NOT implement the daily scheduler or any Phase 9H automation as part of this
validation. After three green sessions, the human decision to design a separate
automation phase is made outside this runbook.
