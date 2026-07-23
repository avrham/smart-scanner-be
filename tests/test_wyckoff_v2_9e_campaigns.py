"""Phase 9E6/9E7: bounded operator shadow campaigns."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any, Dict, List

import pytest

from app.workers.shadow.campaigns import (
    CAMPAIGN_CHUNK_SIZE,
    CAMPAIGN_CONTRACT_VERSION,
    MAX_CAMPAIGN_SYMBOLS,
    CampaignRequestError,
    plan_shadow_campaign,
    run_shadow_campaign,
)
from app.workers.shadow.experiments import UnknownShadowExperimentError

from test_shadow_comparison import NOW_UTC, default_configs, store  # noqa: F401
from test_wyckoff_v2_9d_shadow import _long_daily_payload
from test_wyckoff_v2_9e_shadow_mtf import MtfFakeProvider, _intraday_bars


def _run(coro):
    return asyncio.run(coro)


def _plan(**overrides) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = dict(
        experiment_code="wyckoff_v2_vs_baseline",
        symbols=["bbbx", "AAAX", "aaax"],
        max_symbols=25,
    )
    kwargs.update(overrides)
    return plan_shadow_campaign(**kwargs)


class TestCampaignPlanning:
    def test_explicit_bound_is_required(self):
        with pytest.raises(CampaignRequestError, match="max_symbols"):
            _plan(max_symbols=None)

    def test_unbounded_or_oversized_bounds_rejected(self):
        with pytest.raises(CampaignRequestError):
            _plan(max_symbols=0)
        with pytest.raises(CampaignRequestError):
            _plan(max_symbols=MAX_CAMPAIGN_SYMBOLS + 1)
        with pytest.raises(CampaignRequestError):
            _plan(max_symbols="lots")

    def test_symbol_list_is_explicit_and_never_truncated(self):
        with pytest.raises(CampaignRequestError):
            _plan(symbols=None)
        with pytest.raises(CampaignRequestError):
            _plan(symbols=[])
        with pytest.raises(CampaignRequestError, match="never silently"):
            _plan(symbols=[f"S{i}" for i in range(30)], max_symbols=10)

    def test_deterministic_sorted_deduped_symbols(self):
        plan = _plan()
        assert plan["symbols"] == ["AAAX", "BBBX"]
        assert plan["requested_count"] == 2
        assert plan["campaign_contract_version"] == CAMPAIGN_CONTRACT_VERSION

    def test_malformed_symbols_rejected(self):
        with pytest.raises(CampaignRequestError):
            _plan(symbols=["GOOD", "not a symbol!"])

    def test_unknown_experiment_rejected(self):
        with pytest.raises(UnknownShadowExperimentError):
            _plan(experiment_code="no_such_experiment")

    def test_chunking_respects_run_cap(self):
        symbols = [f"S{i:03d}" for i in range(60)]
        plan = _plan(symbols=symbols, max_symbols=100)
        assert plan["chunk_count"] == 3
        assert all(len(c) <= CAMPAIGN_CHUNK_SIZE for c in plan["chunks"])
        # Chunks preserve deterministic global ordering.
        flattened = [s for chunk in plan["chunks"] for s in chunk]
        assert flattened == sorted(symbols)

    def test_as_of_date_parsing(self):
        plan = _plan(as_of_date="2026-07-10")
        assert plan["as_of_date"] == "2026-07-10"
        plan2 = _plan(as_of_date=date(2026, 7, 10))
        assert plan2["as_of_date"] == "2026-07-10"
        with pytest.raises(CampaignRequestError):
            _plan(as_of_date="July 10")


class TestCampaignExecution:
    # 220 daily bars: enough for the sma150 control to evaluate and for the
    # wyckoff candidate to report insufficient_history quickly — campaign
    # mechanics (chunking, statuses, dedupe) are verdict-agnostic, so the
    # cheap frame keeps this suite fast.
    def _provider(self, symbols: List[str]):
        return MtfFakeProvider(
            {s: _long_daily_payload(bars=220) for s in symbols},
            intraday_bars={
                s: _intraday_bars(breakout_close=None) for s in symbols
            },
        )

    def test_bounded_sequential_execution_with_per_symbol_status(
        self, store, default_configs
    ):
        symbols = [f"SY{i:02d}" for i in range(30)]
        plan = _plan(symbols=symbols, max_symbols=50)
        provider = self._provider(sorted(symbols))
        summary = _run(run_shadow_campaign(provider, plan, now_utc=NOW_UTC))

        assert summary["status"] == "completed"
        assert summary["campaign_id"] == plan["campaign_id"]
        assert summary["chunk_count"] == 2
        assert summary["evaluated_count"] == 30
        assert summary["rejected_count"] == 0
        assert summary["unresolved_count"] == 0
        assert len(summary["runs"]) == 2
        assert set(summary["symbol_statuses"]) == set(sorted(symbols))
        for status in summary["symbol_statuses"].values():
            assert status["status"] == "evaluated"
            assert status["candidate_verdict"] in ("ENTER", "WATCH", "AVOID")
        # The runner's own 25-symbol cap was never exceeded per chunk.
        assert all(len(r["symbols"]) <= 25 for r in summary["runs"])

    def test_partial_failure_is_typed_not_fatal(self, store, default_configs):
        symbols = [f"SY{i:02d}" for i in range(30)]
        plan = _plan(symbols=symbols, max_symbols=50)

        class FlakyProvider(MtfFakeProvider):
            async def get_daily_history(self, symbol, timeseries=400):
                if symbol == "SY03":
                    raise RuntimeError("boom")
                return await super().get_daily_history(symbol, timeseries)

        provider = FlakyProvider(
            {s: _long_daily_payload(bars=220) for s in sorted(symbols)},
            intraday_bars={
                s: _intraday_bars(breakout_close=None) for s in symbols
            },
        )
        summary = _run(run_shadow_campaign(provider, plan, now_utc=NOW_UTC))
        assert summary["status"] == "completed"
        assert summary["evaluated_count"] == 29
        assert summary["rejected_count"] == 1
        assert summary["symbol_statuses"]["SY03"] == {
            "status": "rejected",
            "reason_code": "fetch_error",
            "run_id": summary["runs"][0]["run_id"],
            "chunk_index": 0,
        }

    def test_failed_chunk_marks_symbols_and_continues(
        self, store, default_configs, monkeypatch
    ):
        import app.workers.shadow.campaigns as campaigns_mod

        symbols = [f"SY{i:02d}" for i in range(30)]
        plan = _plan(symbols=symbols, max_symbols=50)
        real_run = campaigns_mod.run_shadow_comparison
        calls = {"n": 0}

        async def flaky_run(provider, chunk, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("chunk exploded")
            return await real_run(provider, chunk, **kwargs)

        monkeypatch.setattr(
            campaigns_mod, "run_shadow_comparison", flaky_run
        )
        provider = self._provider(sorted(symbols))
        summary = _run(run_shadow_campaign(provider, plan, now_utc=NOW_UTC))
        assert summary["status"] == "completed_with_failures"
        assert summary["failed_chunk_count"] == 1
        failed = [
            s for s, st in summary["symbol_statuses"].items()
            if st["status"] == "run_failed"
        ]
        assert len(failed) == 25
        assert summary["evaluated_count"] == 5

    def test_retry_is_idempotent_through_pair_dedupe(
        self, store, default_configs
    ):
        symbols = ["AAAX", "BBBX"]
        plan = _plan(symbols=symbols, as_of_date="2026-07-17")
        provider = self._provider(symbols)
        first = _run(run_shadow_campaign(provider, plan, now_utc=NOW_UTC))
        assert first["evaluated_count"] == 2
        assert len(store.pairs) == 2

        retry = _run(run_shadow_campaign(
            self._provider(symbols), plan, now_utc=NOW_UTC,
        ))
        assert retry["evaluated_count"] == 2
        # Identical inputs -> the SAME immutable pairs, only linked again.
        assert len(store.pairs) == 2
        for status in retry["symbol_statuses"].values():
            assert status["created_new_pair"] is False

    def test_campaign_block_frozen_into_run_telemetry(
        self, store, default_configs
    ):
        symbols = ["AAAX"]
        plan = _plan(symbols=symbols, as_of_date="2026-07-17")
        _run(run_shadow_campaign(
            self._provider(symbols), plan, now_utc=NOW_UTC,
        ))
        run_row = list(store.runs.values())[0]
        campaign = run_row["telemetry"]["campaign"]
        assert campaign["campaign_id"] == plan["campaign_id"]
        assert campaign["campaign_contract_version"] == (
            CAMPAIGN_CONTRACT_VERSION
        )
        assert campaign["chunk_index"] == 0
        assert campaign["chunk_count"] == 1
        assert campaign["as_of_date"] == "2026-07-17"

    def test_no_production_effects(self, store, default_configs, monkeypatch):
        import app.workers.persistence as persistence_mod
        import app.workers.strategies.decision_card as card_mod

        def _bomb(*args, **kwargs):
            raise AssertionError("production persistence invoked by campaign")

        monkeypatch.setattr(persistence_mod, "save_signal", _bomb)
        monkeypatch.setattr(card_mod, "build_decision_card", _bomb)
        symbols = ["AAAX"]
        plan = _plan(symbols=symbols)
        summary = _run(run_shadow_campaign(
            self._provider(symbols), plan, now_utc=NOW_UTC,
        ))
        assert summary["status"] == "completed"

    def test_no_scheduling_hook_exists(self):
        import app.workers.shadow.campaigns as campaigns_mod
        import inspect

        source = inspect.getsource(campaigns_mod)
        for forbidden in ("apscheduler", "add_job", "CronTrigger",
                          "start_scheduler"):
            assert forbidden not in source
