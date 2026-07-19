# Phase 5.2 — Persist WATCH candidates + structured decision cards (summary)

Phase 5.1 made ENTER reachable, but valid multi-timeframe setups that lack a 4H
trigger (WATCH) were only counted in telemetry and then lost. For a
decision-support system these are the most interesting rows: "what was
interesting, why, and what has to happen before it becomes an entry."

Phase 5.2 persists WATCH candidates and attaches a deterministic **decision
card** to every persisted signal (WATCH and ENTER).

**No alpha claim.** WATCH rows are inspectable history; only ENTER signals flow
into Phase 2 outcome tracking.

---

## Why WATCH matters / how it differs from ENTER

| | WATCH | ENTER |
| --- | --- | --- |
| Meaning | Monthly + weekly + daily all valid, trigger unconfirmed | Trigger confirmed (for wyckoff: 4H break) |
| entry/stop | **null — never faked** | From the 4H trigger (deterministic) |
| Outcome tracking | Skipped (loader filters `verdict='ENTER'`) | Eligible |
| Purpose | Track candidate quality over time; see whether WATCH later converts to ENTER (same symbol+pattern, later snapshot) | Actionable signal for review |

## Storage approach

**No migration.** The existing `signals` table already fits: `verdict` is free
TEXT (now 'ENTER' | 'WATCH' | debug 'AVOID'), and `details` is JSONB. WATCH rows
store side/setup_type/prices (null when unknown) inside `details`, and the
decision card at `details.decision_card`. The unique `(symbol, pattern_code,
snapshot_date)` constraint upserts — so if a symbol upgrades WATCH→ENTER on the
same snapshot the row simply upgrades.

The public `GET /api/signals` endpoints filter `verdict='ENTER'`, so existing UI
behavior is unchanged; surfacing WATCH is deliberate Phase 6 UI work.

## Decision card (`app/workers/strategies/decision_card.py`)

`build_decision_card(result: StrategyResult)` — pure, deterministic, built ONLY
from StrategyResult fields. No LLM, no invented values. Fields:

`card_version, title, decision, symbol, pattern_code, side, setup_type, score,
why_now (strategy reason), timeframe_summary (monthly bias / weekly phase / bars
when the strategy reported them), trigger_needed, confirmation_needed,
entry_price, stop_price, target_price, invalidation, risk_notes, missing_data,
next_action, raw_evidence (raw score_components), strategy_version`.

- Wyckoff WATCH → `next_action`: "Wait for 4H trigger confirmation. No ENTER
  signal yet."; `missing_data: ["4h_data"]` when 4H was unavailable.
- Wyckoff ENTER → `next_action`: "Entry trigger confirmed on 4H. Review
  stop/invalidation before action."
- sma150 → simple card; no Wyckoff context is invented (no monthly/weekly keys).
- Every card carries the honest risk note that signal value is unproven.

## Funnel behavior

- WATCH results are persisted when `persist_watch_candidates` is true
  (**default true** — WATCH persistence is cheap, DB-only, and the whole point
  of this phase). Disable per run via scanner config or the admin param.
- ENTER results are persisted as before, now with a decision card attached.
- AVOID/REJECT are still never persisted unless `DEBUG_SAVE_AVOID`.
- New telemetry counter: `stage_counts.watch_saved_count` (alongside
  `watch_count`, `enter_count`, `reject_count`).
- `dry_run` still writes nothing and calls no FMP.

## Admin

`POST /api/admin/scan/start` accepts `persist_watch: true|false` (funnel mode
only; default true; legacy mode unaffected):

```bash
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pattern_code":"wyckoff_mtf","scanner_mode":"funnel","limit":5,"persist_watch":true}' | jq .
```

Inspect persisted WATCH rows directly (SQL) until Phase 6 UI:

```sql
SELECT symbol, verdict, score, details->'decision_card'->>'next_action'
FROM signals WHERE verdict = 'WATCH' ORDER BY created_at DESC LIMIT 20;
```

## Outcome tracking compatibility

Untouched math. `get_signals_needing_outcomes` already filters
`s.verdict = 'ENTER'`, so WATCH rows are never treated as entries — verified by
a test that inspects the generated query.

## How this prepares the decision-support UI (Phase 6)

Every persisted signal now carries a self-contained, render-ready explanation:
what/why/what's missing/next action plus raw evidence. A UI can list WATCH
candidates, show their cards, and track WATCH→ENTER conversion without any new
backend computation.

## Tests (deterministic, no live FMP/Supabase)

`tests/test_watch_persistence.py`: WATCH and ENTER cards for wyckoff (content +
no fakes); sma150 card stays simple (no Wyckoff context); minimal-result card;
WATCH persisted by default with card; not persisted when disabled;
AVOID/REJECT not persisted; ENTER also gets a card; telemetry includes
`watch_saved_count`; outcome loader query filters ENTER only.
One Phase 5 test updated: WATCH candidates are now expected to be saved.

## What remains unproven

- No proof of alpha; WATCH→ENTER conversion rates and ENTER outcomes must
  accumulate via Phase 2 before any value claim.
- WATCH rows are not yet surfaced in the UI (Phase 6).
- The live FMP 4hour endpoint remains unvalidated (plan-dependent).
