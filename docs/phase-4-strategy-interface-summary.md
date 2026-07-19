# Phase 4 — Strategy Interface (summary)

Phase 4 introduces a small, typed strategy/plugin contract so every strategy is
evaluated the same way and integrates cleanly with the funnel scanner, signal
persistence, and Phase 2 outcome tracking. It is the seam that later lets
Wyckoff MTF plug in as just another strategy module.

**This phase does not change `sma150_bounce` behavior and does not prove signal
alpha.** It is structural: one contract, one registry, one adapter.

---

## What changed

### New package: `app/workers/strategies/`

| File | Purpose |
| --- | --- |
| `base.py` | The interface: `StrategyDecision`, `StrategySide` enums; `StrategyContext`, `StrategyResult` dataclasses; `Strategy` ABC; `decision_from_verdict`. |
| `registry.py` | Static registry: `register_strategy`, `get_strategy`, `list_strategies`, `UnknownStrategyError`. Registers `sma150_bounce` at import. |
| `sma150_adapter.py` | `Sma150BounceStrategy` — wraps the existing `evaluate_sma150_bounce`. |
| `__init__.py` | Public exports. |

### Funnel integration (`app/workers/scanner/funnel.py`)
- Stage 3 no longer imports `evaluate_sma150_bounce` directly. It resolves the
  strategy via `get_strategy(pattern_code)` (fails fast on an unknown pattern)
  and calls `strategy.evaluate(df, StrategyContext(...))`.
- The Phase 1 config resolver is still used; the resolved config is passed
  through `StrategyContext.config`.
- Telemetry, `dry_run` safety (no FMP, no writes), and the disabled 4H stage are
  unchanged. `WATCH` is now counted if a strategy ever returns it (sma150 does
  not today).

### Legacy scanner
- `app/workers/scan_runner.py` is unchanged and still calls `evaluate_sma150_bounce`
  directly. The legacy path keeps working; routing it through the registry is
  optional and deferred (low value, avoids risk).

---

## The interface

```python
class StrategyDecision(str, Enum): ENTER, WATCH, AVOID, REJECT
class StrategySide(str, Enum):     LONG, SHORT, UNKNOWN

@dataclass
class StrategyContext:
    symbol; pattern_code; config
    scanner_mode=None; scan_run_id=None; data_meta=None

@dataclass
class StrategyResult:
    decision; symbol; pattern_code
    score=None; side=UNKNOWN; reason=None; rejection_reason=None
    details={}; score_components={}; required_timeframes=["1d"]
    entry_price=None; stop_price=None; target_price=None; invalidation=None
    setup_type=None; strategy_version=None
    # .verdict -> decision.value (legacy string for persistence)

class Strategy(ABC):
    pattern_code; version; required_timeframes
    def evaluate(self, df, context) -> StrategyResult: ...
```

---

## How to add a new strategy

1. Implement a subclass of `Strategy` (set `pattern_code`, `version`,
   `required_timeframes`, and `evaluate`).
2. Return a `StrategyResult`. Only populate `side/stop_price/target_price/...`
   if the strategy genuinely defines them — never invent them.
3. Put anything you want persisted (and later read by outcome tracking) into
   `details`. To make direction/stop/target visible to Phase 2, write `side`,
   `stop_price`, `target_price` into `details`.
4. Register it: `register_strategy(MyStrategy())` (or add it to
   `registry._register_defaults`).

No changes to the funnel are required — Stage 3 already routes every pattern
through the registry.

---

## How `sma150_bounce` is now wrapped

`Sma150BounceStrategy.evaluate` calls `evaluate_sma150_bounce(symbol, df, config)`
and maps the result:

| Legacy field | StrategyResult |
| --- | --- |
| `verdict` | `decision` (via `decision_from_verdict`) |
| `score` | `score` |
| `reason` | `reason` |
| `details` | `details` (verbatim) |
| `details.rejection_reason` | `rejection_reason` |
| `details.score_components` | `score_components` |

**Side:** sma150_bounce is a long-only rebound setup, and Phase 2 already
defaults these signals to LONG. The adapter sets `side=LONG` at the interface
level but keeps `details` byte-identical (no injected side/stop/target), so the
persisted signal, UI, and outcome tracking are unchanged. Formalizing
per-strategy direction/stop/target is left to future strategies.

---

## Outcome compatibility

The funnel persists `StrategyResult.details` verbatim via the existing
`save_signal(...)`. Because sma150's `details` are unchanged, Phase 2 outcome
tracking behaves exactly as before (it reads `details.side` and defaults to
LONG). No outcome-calculation logic was changed. Outcomes are still **not**
calculated automatically.

---

## Tests (deterministic, no live FMP/Supabase)

`tests/test_strategy_interface.py`:
- registry returns the `sma150_bounce` strategy; `list_strategies` includes it
- unknown pattern raises `UnknownStrategyError` (a `KeyError`) with a clear message
- adapter output equals the legacy evaluator on the same df/config
  (decision/score/reason/details/score_components) — full path and default-config path
- config override flows through the context into evaluation (`price_below_min`)
- `score_components` stay raw (no weighted `score` key)
- no side/stop/target invented (`side=LONG`, prices `None`, details not mutated)
- `verdict` property + `is_actionable`
- legacy scan path still exists (`scan_runner.run_scan_batch`, direct sma150 call)

`tests/test_funnel_scan.py` updated to prove Stage 3 routes through the registry
(monkeypatches `funnel.get_strategy`), and that `dry_run` still makes zero FMP
calls.

---

## How to run a safe validation scan

Unchanged from Phase 3 — the funnel dry-run is FMP-free and now runs through the
strategy interface:

```bash
curl -s -X POST "$BASE/api/admin/scan/start" \
  -H "Authorization: Bearer $WORKER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"pattern_code":"sma150_bounce","scanner_mode":"funnel","dry_run":true,"limit":50}' | jq .
```

---

## What is still NOT done

- No Wyckoff MTF (Phase 5). The interface only prepares for it.
- No LLM, no UI redesign, no broker execution.
- Legacy `scan_runner` not routed through the registry (intentional).
- `WATCH`/`REJECT` decisions, `side`/`stop`/`target` semantics are defined in the
  interface but not produced by sma150.
- **This phase does not claim signal alpha.** Value still depends on Phase 2
  outcome tracking accumulating enough samples vs. baselines.
