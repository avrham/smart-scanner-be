"""Phase 5.2 — WATCH persistence + structured decision cards tests.

No live FMP or Supabase. Reuses the deterministic OHLCV builders from the
Phase 5.1 test module.
"""

import asyncio

import numpy as np
import pandas as pd

import app.workers.outcomes.persistence as outcomes_persistence
import app.workers.scanner.funnel as funnel
from app.workers.strategies import get_strategy
from app.workers.strategies.base import (
    StrategyContext,
    StrategyDecision,
    StrategyResult,
    StrategySide,
)
from app.workers.strategies.decision_card import build_decision_card
from app.workers.strategies.wyckoff.strategy import WyckoffMTFStrategy

from test_funnel_4h import (
    _4h_bars_newest_first,
    _daily_payload,
    _FakeFMP,
    _flat_daily,
    _ohlcv,
    _patch_db,
    _TICKER,
    _wyckoff_daily,
)


def _ctx(symbol="AAA", config=None, data_meta=None):
    return StrategyContext(
        symbol=symbol,
        pattern_code="wyckoff_mtf",
        config=config,
        scanner_mode="funnel",
        data_meta=data_meta,
    )


def _async(value):
    async def _f(*a, **k):
        return value
    return _f


# --------------------------------------------------------------------------- #
# Decision card builder
# --------------------------------------------------------------------------- #

def test_wyckoff_watch_builds_decision_card():
    res = get_strategy("wyckoff_mtf").evaluate(_wyckoff_daily(), _ctx())
    assert res.decision == StrategyDecision.WATCH

    card = build_decision_card(res)
    assert card["decision"] == "WATCH"
    assert card["symbol"] == "AAA"
    assert card["pattern_code"] == "wyckoff_mtf"
    assert card["side"] == "LONG"
    assert card["setup_type"] == "sos"
    assert card["next_action"] == "Wait for 4H trigger confirmation. No ENTER signal yet."
    assert card["confirmation_needed"] is True
    assert "4H close breaking local structure" in card["trigger_needed"]
    assert card["missing_data"] == ["4h_data"]
    # Timeframe summary carries the MTF evidence.
    ts = card["timeframe_summary"]
    assert ts["monthly_bias"] == "LONG"
    assert ts["weekly_phase"] == "markup"
    # Nothing invented.
    assert card["entry_price"] is None
    assert card["stop_price"] is None
    assert card["target_price"] is None
    # Raw evidence = raw score components.
    assert "monthly_close_vs_sma_pct" in card["raw_evidence"]


def test_wyckoff_enter_builds_decision_card():
    cfg = WyckoffMTFStrategy().default_config()
    cfg["enable_4h_trigger"] = True
    df_4h = funnel.to_dataframe({"historical": _4h_bars_newest_first()})
    res = get_strategy("wyckoff_mtf").evaluate(
        _wyckoff_daily(), _ctx(config=cfg, data_meta={"df_4h": df_4h})
    )
    assert res.decision == StrategyDecision.ENTER

    card = build_decision_card(res)
    assert card["decision"] == "ENTER"
    assert card["next_action"] == (
        "Entry trigger confirmed on 4H. Review stop/invalidation before action."
    )
    assert card["confirmation_needed"] is False
    assert card["trigger_needed"] is None
    assert card["missing_data"] == []
    assert card["entry_price"] == res.entry_price
    assert card["stop_price"] == res.stop_price
    # v1 has no deterministic target -> flagged as a risk note, not faked.
    assert card["target_price"] is None
    assert any("target_price" in n for n in card["risk_notes"])


def test_sma150_card_stays_simple_no_wyckoff_context():
    # Simple daily uptrend; sma150 will AVOID, which is fine for the card shape.
    dates = pd.date_range("2022-01-01", periods=360, freq="B")
    df = _ohlcv(dates, 50.0 + np.arange(360) * 0.05)
    ctx = StrategyContext(symbol="AAA", pattern_code="sma150_bounce", config=None)
    res = get_strategy("sma150_bounce").evaluate(df, ctx)

    card = build_decision_card(res)
    assert card["pattern_code"] == "sma150_bounce"
    # No Wyckoff context invented for sma150.
    ts = card["timeframe_summary"]
    assert "monthly_bias" not in ts
    assert "weekly_phase" not in ts
    assert card["missing_data"] == []  # sma150 does not need 4H
    assert card["entry_price"] is None
    assert card["stop_price"] is None


def test_watch_card_from_minimal_result_no_fakes():
    res = StrategyResult(
        decision=StrategyDecision.WATCH,
        symbol="XYZ",
        pattern_code="wyckoff_mtf",
        side=StrategySide.SHORT,
        setup_type="utad",
        required_timeframes=["1d", "1w", "1M", "4h"],
    )
    card = build_decision_card(res)
    assert card["side"] == "SHORT"
    assert "SHORT direction" in card["trigger_needed"]
    assert card["entry_price"] is None
    assert card["invalidation"] is None


# --------------------------------------------------------------------------- #
# Funnel persistence behavior
# --------------------------------------------------------------------------- #

def _run_watch_scan(monkeypatch, scanner_config=None):
    """Wyckoff funnel run (no 4H) -> one WATCH candidate. Returns (summary, saved)."""
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return "sig-id"

    monkeypatch.setattr(funnel, "save_signal", fake_save)
    fake = _FakeFMP({"AAA": _daily_payload(_wyckoff_daily())})
    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="wyckoff_mtf",
            dry_run=False,
            scanner_config=scanner_config,
        )
    )
    return summary, saved


def test_watch_persisted_by_default_with_card(monkeypatch):
    summary, saved = _run_watch_scan(monkeypatch)

    sc = summary["stage_counts"]
    assert sc["watch_count"] == 1
    assert sc["watch_saved_count"] == 1
    assert len(saved) == 1

    row = saved[0]
    assert row["verdict"] == "WATCH"
    card = row["details"]["decision_card"]
    assert card["decision"] == "WATCH"
    assert card["next_action"] == "Wait for 4H trigger confirmation. No ENTER signal yet."
    # No fake prices anywhere in the persisted payload.
    assert row["details"]["entry_price"] is None
    assert row["details"]["stop_price"] is None
    assert card["entry_price"] is None


def test_watch_not_persisted_when_disabled(monkeypatch):
    summary, saved = _run_watch_scan(
        monkeypatch, scanner_config={"persist_watch_candidates": False}
    )
    sc = summary["stage_counts"]
    assert sc["watch_count"] == 1
    assert sc["watch_saved_count"] == 0
    assert saved == []


def test_rejects_not_persisted_by_default(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "FLAT", **_TICKER}], cfg)

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return "sig-id"

    monkeypatch.setattr(funnel, "save_signal", fake_save)
    fake = _FakeFMP({"FLAT": _daily_payload(_flat_daily())})
    summary = asyncio.run(
        funnel.run_funnel_scan(fmp=fake, pattern_code="wyckoff_mtf", dry_run=False)
    )

    assert summary["stage_counts"]["reject_count"] == 1
    assert saved == []  # AVOID/REJECT never persisted unless DEBUG_SAVE_AVOID


def test_enter_signal_also_gets_decision_card(monkeypatch):
    cfg = WyckoffMTFStrategy().default_config()
    _patch_db(monkeypatch, [{"symbol": "AAA", **_TICKER}], cfg)

    saved = []

    async def fake_save(**kwargs):
        saved.append(kwargs)
        return "sig-id"

    monkeypatch.setattr(funnel, "save_signal", fake_save)
    fake = _FakeFMP(
        {"AAA": _daily_payload(_wyckoff_daily())}, payload_4h=_4h_bars_newest_first()
    )
    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=fake,
            pattern_code="wyckoff_mtf",
            dry_run=False,
            scanner_config={"enable_expensive_stages": True},
        )
    )

    assert summary["stage_counts"]["enter_count"] == 1
    assert summary["stage_counts"]["watch_saved_count"] == 0
    assert len(saved) == 1
    row = saved[0]
    assert row["verdict"] == "ENTER"
    card = row["details"]["decision_card"]
    assert card["decision"] == "ENTER"
    assert card["entry_price"] is not None


def test_telemetry_includes_watch_saved_count(monkeypatch):
    summary, _ = _run_watch_scan(monkeypatch)
    assert "watch_saved_count" in summary["telemetry"]["stage_counts"]


# --------------------------------------------------------------------------- #
# Outcome tracking compatibility
# --------------------------------------------------------------------------- #

def test_outcome_loader_only_selects_enter(monkeypatch):
    """The Phase 2 loader must never treat WATCH candidates as entries."""
    captured = {}

    class _FakeConn:
        async def fetch(self, query, *params):
            captured["query"] = query
            return []

    monkeypatch.setattr(
        outcomes_persistence, "get_db_connection", _async(_FakeConn())
    )
    monkeypatch.setattr(
        outcomes_persistence, "release_db_connection", _async(None)
    )

    asyncio.run(outcomes_persistence.get_signals_needing_outcomes())
    assert "s.verdict = 'ENTER'" in captured["query"]
