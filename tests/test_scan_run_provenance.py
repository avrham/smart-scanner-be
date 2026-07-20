"""Phase 7B — canonical scan-run identity is linked to persisted signals.

Verifies (all mocked, no DB, no providers):
  * funnel ENTER/WATCH signals persist the SAME scan_run_id the caller passed
    (the UUID the admin endpoint returns / the WebSocket uses)
  * the canonical scan-run row is created at scan start and finalized at end
  * exact strategy identity comes from the real StrategyResult
  * legacy/scheduled batch signals link to their real scan run
  * dry runs perform no signal writes but still get a canonical identity
"""

import asyncio
import uuid
from datetime import datetime, timezone

import app.workers.scanner.funnel as funnel
import app.workers.scan_runner as scan_runner
from app.workers.strategies.base import StrategyDecision, StrategyResult


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


def _history(n=210, price=50.0):
    rows = []
    for i in range(n):
        rows.append({
            "date": f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "open": price, "high": price + 1, "low": price - 1,
            "close": price, "volume": 1_000_000,
        })
    return {"historical": rows}


class _FakeFMP:
    name = "massive"

    def __init__(self, payloads):
        self._payloads = payloads

    async def batch_historical_data(self, symbols, timeseries=350):
        return {s: self._payloads.get(s, {"historical": []}) for s in symbols}


_PATTERN_CONFIG = {
    "min_liquidity_filters": {"min_market_cap": 2e8, "min_daily_volume": 2e5},
    "min_price": 5.0,
    "score_threshold": 0.5,
}


class _Strategy:
    pattern_code = "sma150_bounce"
    version = "sma150.v2"
    min_daily_bars = 200

    def __init__(self, decision):
        self.decision = decision

    def default_config(self):
        return {}

    def evaluate(self, df, context):
        return StrategyResult(
            decision=self.decision,
            symbol=context.symbol,
            pattern_code=context.pattern_code,
            score=0.9,
            reason="ok",
            details={"snapshot_date": "2023-06-01",
                     "score_components": {"proximity_to_sma150_pct": 1.0}},
            score_components={"proximity_to_sma150_pct": 1.0},
            strategy_version=self.version,
        )


def _run_funnel(monkeypatch, decision, scan_id):
    runs = {"created": [], "finalized": []}
    saved = []

    async def fake_create(**kwargs):
        runs["created"].append(kwargs)
        return kwargs["scan_run_id"]

    async def fake_finalize(**kwargs):
        runs["finalized"].append(kwargs)

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return {"signal_id": "sig-id", "created_new_signal": True, "deduplicated": False}

    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(dict(_PATTERN_CONFIG)))
    monkeypatch.setattr(
        funnel, "get_universe_tickers",
        _async([{"symbol": "AAA", "market_cap": 5e9, "last_volume": 1e6}]),
    )
    monkeypatch.setattr(funnel, "was_seen_today", _async(False))
    monkeypatch.setattr(funnel, "mark_seen_today", _async(None))
    monkeypatch.setattr(funnel, "create_scan_run", fake_create)
    monkeypatch.setattr(funnel, "finalize_scan_run", fake_finalize)
    monkeypatch.setattr(funnel, "save_signal", fake_save)
    monkeypatch.setattr(funnel, "get_strategy", lambda pc: _Strategy(decision))

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=_FakeFMP({"AAA": _history()}),
            pattern_code="sma150_bounce",
            dry_run=False,
            scan_id=scan_id,
        )
    )
    return summary, runs, saved


def test_funnel_enter_signal_links_canonical_scan_run(monkeypatch):
    scan_id = str(uuid.uuid4())
    summary, runs, saved = _run_funnel(monkeypatch, StrategyDecision.ENTER, scan_id)

    assert len(saved) == 1
    prov = saved[0]["provenance"]
    # The SAME UUID the endpoint/WebSocket used, not a new identity.
    assert prov["scan_run_id"] == scan_id
    assert runs["created"][0]["scan_run_id"] == scan_id
    assert runs["created"][0]["scanner_mode"] == "funnel"
    assert runs["finalized"][0]["scan_run_id"] == scan_id
    assert runs["finalized"][0]["status"] == "completed"
    assert prov["source_path"] == "funnel"
    assert prov["provider"] == "massive"


def test_funnel_watch_signal_links_canonical_scan_run(monkeypatch):
    scan_id = str(uuid.uuid4())
    summary, runs, saved = _run_funnel(monkeypatch, StrategyDecision.WATCH, scan_id)

    assert summary["stage_counts"]["watch_saved_count"] == 1
    assert saved[0]["provenance"]["scan_run_id"] == scan_id
    assert saved[0]["verdict"] == "WATCH"


def test_funnel_persists_exact_strategy_and_policy_versions(monkeypatch):
    scan_id = str(uuid.uuid4())
    _, _, saved = _run_funnel(monkeypatch, StrategyDecision.ENTER, scan_id)

    prov = saved[0]["provenance"]
    # Exact version from the REAL StrategyResult, not derived from names.
    assert prov["strategy_code"] == "sma150_bounce"
    assert prov["strategy_version"] == "sma150.v2"
    # Separate identities, both persisted.
    assert prov["decision_policy_version"] == "strategy_decision.v1"
    assert prov["provenance_version"] == "provenance.v1"
    assert prov["config_hash"]
    assert prov["config_snapshot"]["strategy_config"]["score_threshold"] == 0.5


def test_funnel_as_of_from_latest_evaluated_bar(monkeypatch):
    scan_id = str(uuid.uuid4())
    _, _, saved = _run_funnel(monkeypatch, StrategyDecision.ENTER, scan_id)

    as_of = saved[0]["provenance"]["market_data_as_of"]
    assert as_of is not None and as_of.tzinfo is not None
    # From the evaluated 2023 dataframe — never the wall clock.
    assert as_of.year == 2023


def test_funnel_evidence_snapshot_has_deterministic_evidence(monkeypatch):
    scan_id = str(uuid.uuid4())
    _, _, saved = _run_funnel(monkeypatch, StrategyDecision.ENTER, scan_id)

    evidence = saved[0]["provenance"]["evidence_snapshot"]
    assert evidence["score_components"] == {"proximity_to_sma150_pct": 1.0}
    assert evidence["snapshot_date"] == "2023-06-01"
    # Decision-card evidence captured; nothing invented.
    assert "decision_card_evidence" in evidence
    assert saved[0]["provenance"]["external_observation_ids"] == []


def test_funnel_dry_run_creates_run_but_writes_no_signals(monkeypatch):
    runs = {"created": [], "finalized": []}

    async def fake_create(**kwargs):
        runs["created"].append(kwargs)
        return kwargs["scan_run_id"]

    async def fake_finalize(**kwargs):
        runs["finalized"].append(kwargs)

    async def must_not_save(**kwargs):
        raise AssertionError("dry_run must not persist signals")

    monkeypatch.setattr(funnel, "resolve_pattern_config", _async(dict(_PATTERN_CONFIG)))
    monkeypatch.setattr(funnel, "get_universe_tickers", _async([]))
    monkeypatch.setattr(funnel, "create_scan_run", fake_create)
    monkeypatch.setattr(funnel, "finalize_scan_run", fake_finalize)
    monkeypatch.setattr(funnel, "save_signal", must_not_save)

    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=None, pattern_code="sma150_bounce", dry_run=True)
    )
    assert summary["dry_run"] is True
    assert len(runs["created"]) == 1
    assert runs["created"][0]["dry_run"] is True
    assert runs["finalized"][0]["status"] == "completed"


# --------------------------------------------------------------------------- #
# Legacy / scheduled batch path
# --------------------------------------------------------------------------- #

def _run_batch(monkeypatch, source_path):
    runs = {"created": [], "finalized": []}
    saved = []

    async def fake_create(**kwargs):
        runs["created"].append(kwargs)
        return kwargs["scan_run_id"]

    async def fake_finalize(**kwargs):
        runs["finalized"].append(kwargs)

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return {"signal_id": "sig-id", "created_new_signal": True, "deduplicated": False}

    def fake_eval(symbol, df, config):
        return {
            "verdict": "ENTER",
            "score": 0.9,
            "reason": "ok",
            "details": {"snapshot_date": "2023-06-01", "score_version": "sma150.v2"},
        }

    monkeypatch.setattr(scan_runner, "create_scan_run", fake_create)
    monkeypatch.setattr(scan_runner, "finalize_scan_run", fake_finalize)
    monkeypatch.setattr(scan_runner, "save_signal", fake_save)
    monkeypatch.setattr(scan_runner, "evaluate_sma150_bounce", fake_eval)
    monkeypatch.setattr(scan_runner, "resolve_pattern_config", _async({"min_price": 1.0}))
    monkeypatch.setattr(scan_runner, "was_seen_today", _async(False))
    monkeypatch.setattr(scan_runner, "mark_seen_today", _async(None))

    summary = asyncio.run(
        scan_runner.run_scan_batch(
            _FakeFMP({"AAA": _history()}),
            batch_size=1,
            symbols=["AAA"],
            ignore_seen=True,
            source_path=source_path,
        )
    )
    return summary, runs, saved


def test_legacy_batch_signal_links_generated_scan_run(monkeypatch):
    summary, runs, saved = _run_batch(monkeypatch, source_path="legacy")

    assert summary["success"] is True
    assert len(saved) == 1
    prov = saved[0]["provenance"]
    # scan_id generated inside run_scan_batch; signal links to that SAME run.
    assert prov["scan_run_id"] == summary["scan_id"]
    assert prov["scan_run_id"] == runs["created"][0]["scan_run_id"]
    assert prov["source_path"] == "legacy"
    assert prov["strategy_version"] == "sma150.v2"
    assert runs["finalized"][0]["status"] == "completed"


def test_scheduled_batch_signal_links_real_scan_run(monkeypatch):
    summary, runs, saved = _run_batch(monkeypatch, source_path="scheduled")

    prov = saved[0]["provenance"]
    assert prov["source_path"] == "scheduled"
    assert prov["scan_run_id"] == summary["scan_id"]
    assert runs["created"][0]["scanner_mode"] == "scheduled"
