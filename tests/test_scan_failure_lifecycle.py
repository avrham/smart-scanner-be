"""Phase 7B — scan failure lifecycle (no DB, no providers).

Every HANDLED exception in a scan entry path must finalize the canonical
pattern_runs row: status='failed', finished_at populated (inside
finalize_scan_run), safe error_code + error_message, partial telemetry when
available. No handled exception may leave a scan stuck in 'running'.

Process-death limitation (documented in the roadmap, not testable here): an
abrupt kill cannot execute finalization, so a stale 'running' row may remain
for forensic visibility; it never blocks new scans and scans are NOT
resumable.
"""

import asyncio
import uuid

import pytest

import app.workers.scanner.funnel as funnel
import app.workers.scan_runner as scan_runner
from app.workers.scan_runs import sanitize_scan_error


def _async(value):
    async def _f(*args, **kwargs):
        return value
    return _f


class _ExplodingFMP:
    name = "massive"

    async def batch_historical_data(self, symbols, timeseries=350):
        raise RuntimeError("provider exploded mid-scan")


def _capture_runs(monkeypatch, module):
    runs = {"created": [], "finalized": []}

    async def fake_create(**kwargs):
        runs["created"].append(kwargs)
        return kwargs["scan_run_id"]

    async def fake_finalize(**kwargs):
        runs["finalized"].append(kwargs)

    monkeypatch.setattr(module, "create_scan_run", fake_create)
    monkeypatch.setattr(module, "finalize_scan_run", fake_finalize)
    return runs


# --------------------------------------------------------------------------- #
# Funnel scan failures
# --------------------------------------------------------------------------- #

def test_failed_funnel_scan_is_finalized_as_failed(monkeypatch):
    runs = _capture_runs(monkeypatch, funnel)
    monkeypatch.setattr(
        funnel, "resolve_pattern_config",
        _async({"min_liquidity_filters": {}, "min_price": 5.0}),
    )

    async def exploding_universe(*args, **kwargs):
        raise RuntimeError("universe query exploded")

    monkeypatch.setattr(funnel, "get_universe_tickers", exploding_universe)

    scan_id = str(uuid.uuid4())
    with pytest.raises(RuntimeError, match="universe query exploded"):
        asyncio.run(
            funnel.run_funnel_scan(
                fmp=None, pattern_code="sma150_bounce",
                dry_run=False, scan_id=scan_id,
            )
        )

    # The run was created, then finalized as failed — never left 'running'.
    assert len(runs["created"]) == 1
    assert len(runs["finalized"]) == 1
    final = runs["finalized"][0]
    assert final["scan_run_id"] == scan_id
    assert final["status"] == "failed"
    assert final["error_code"] == "funnel_scan_exception"
    assert "universe query exploded" in final["error_message"]
    # Partial telemetry: whatever stage counts existed before the failure.
    assert final["telemetry"]["partial"] is True
    assert "stage_counts" in final["telemetry"]


def test_failed_funnel_scan_keeps_partial_stage_counts(monkeypatch):
    runs = _capture_runs(monkeypatch, funnel)
    monkeypatch.setattr(
        funnel, "resolve_pattern_config",
        _async({"min_liquidity_filters": {}, "min_price": 5.0}),
    )
    # Universe loads fine (stage 0 counted), then the provider explodes.
    monkeypatch.setattr(
        funnel, "get_universe_tickers",
        _async([{"symbol": "AAA", "market_cap": 5e9, "last_volume": 1e6}]),
    )

    with pytest.raises(RuntimeError, match="provider exploded"):
        asyncio.run(
            funnel.run_funnel_scan(
                fmp=_ExplodingFMP(), pattern_code="sma150_bounce", dry_run=False,
            )
        )

    final = runs["finalized"][0]
    assert final["status"] == "failed"
    counts = final["telemetry"]["stage_counts"]
    assert counts["stage_0_universe"] == 1  # progress before failure preserved


# --------------------------------------------------------------------------- #
# Legacy / scheduled batch failures
# --------------------------------------------------------------------------- #

def _run_failing_batch(monkeypatch, source_path):
    runs = _capture_runs(monkeypatch, scan_runner)
    monkeypatch.setattr(
        scan_runner, "resolve_pattern_config", _async({"min_price": 1.0})
    )

    result = asyncio.run(
        scan_runner.run_scan_batch(
            _ExplodingFMP(),
            batch_size=1,
            symbols=["AAA"],
            ignore_seen=True,
            source_path=source_path,
        )
    )
    return result, runs


def test_failed_scheduled_scan_is_finalized_as_failed(monkeypatch):
    result, runs = _run_failing_batch(monkeypatch, source_path="scheduled")

    assert result["success"] is False
    assert len(runs["finalized"]) == 1
    final = runs["finalized"][0]
    assert final["status"] == "failed"
    assert final["error_code"] == "batch_scan_exception"
    assert "provider exploded" in final["error_message"]
    assert runs["created"][0]["scanner_mode"] == "scheduled"


def test_failed_legacy_scan_is_finalized_as_failed(monkeypatch):
    result, runs = _run_failing_batch(monkeypatch, source_path="legacy")

    assert result["success"] is False
    assert runs["finalized"][0]["status"] == "failed"
    assert runs["finalized"][0]["error_code"] == "batch_scan_exception"


# --------------------------------------------------------------------------- #
# Zero-candidate scans are NORMAL completed outcomes, never failures
# --------------------------------------------------------------------------- #

def _run_zero_candidate_batch(monkeypatch, source_path):
    runs = _capture_runs(monkeypatch, scan_runner)
    monkeypatch.setattr(
        scan_runner, "resolve_pattern_config", _async({"min_price": 1.0})
    )
    monkeypatch.setattr(scan_runner, "load_candidate_pool", _async([]))

    result = asyncio.run(
        scan_runner.run_scan_batch(
            _ExplodingFMP(), batch_size=1, source_path=source_path
        )
    )
    return result, runs


def test_zero_candidate_batch_completes_successfully(monkeypatch):
    """A scan that executes normally and finds nothing to evaluate completes
    with zero counts and NO error identity."""
    result, runs = _run_zero_candidate_batch(monkeypatch, source_path="legacy")

    assert result["success"] is True
    assert result["enter_count"] == 0
    final = runs["finalized"][0]
    assert final["status"] == "completed"
    assert final["scanned_count"] == 0
    assert final["enter_count"] == 0
    assert final.get("error_code") is None
    assert final.get("error_message") is None
    # The terminal reason lives in telemetry, not in the error identity.
    assert final["telemetry"]["terminal_reason"] == "no_candidates"
    assert final["telemetry"]["signals_created"] == 0
    assert final["telemetry"]["signals_linked"] == 0


def test_zero_candidate_scheduled_scan_completes_successfully(monkeypatch):
    result, runs = _run_zero_candidate_batch(monkeypatch, source_path="scheduled")

    assert result["success"] is True
    assert result["terminal_reason"] == "no_candidates"
    final = runs["finalized"][0]
    assert final["status"] == "completed"
    assert final.get("error_code") is None
    assert final.get("error_message") is None
    assert runs["created"][0]["scanner_mode"] == "scheduled"


def test_zero_candidate_funnel_scan_completes_successfully(monkeypatch):
    """Empty universe: the funnel evaluated nothing but ran normally."""
    runs = _capture_runs(monkeypatch, funnel)
    monkeypatch.setattr(
        funnel, "resolve_pattern_config",
        _async({"min_liquidity_filters": {}, "min_price": 5.0}),
    )
    monkeypatch.setattr(funnel, "get_universe_tickers", _async([]))

    class _UnusedFMP:
        name = "massive"

        async def batch_historical_data(self, symbols, timeseries=350):
            return {}

    summary = asyncio.run(
        funnel.run_funnel_scan(
            fmp=_UnusedFMP(), pattern_code="sma150_bounce", dry_run=False,
        )
    )

    assert summary["stage_counts"]["enter_count"] == 0
    assert summary["stage_counts"]["watch_count"] == 0
    assert summary["stage_counts"]["signals_created"] == 0
    assert summary["stage_counts"]["signals_linked"] == 0
    final = runs["finalized"][0]
    assert final["status"] == "completed"
    assert final.get("error_code") is None
    assert final.get("error_message") is None
    assert final["telemetry"]["terminal_reason"] == "no_candidates"


# --------------------------------------------------------------------------- #
# Error message safety
# --------------------------------------------------------------------------- #

def test_scan_error_messages_are_sanitized_and_bounded():
    assert sanitize_scan_error(None) is None
    masked = sanitize_scan_error("request failed: apiKey=super-secret-123 oops")
    assert "super-secret-123" not in masked
    long = sanitize_scan_error("x" * 10_000)
    assert len(long) <= 500
