"""Phase 9D1: explicit persistence-free strategy dry-run."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.deps import get_db
from app.routers import admin as admin_mod
from app.workers.strategies.dry_run import (
    DRY_RUN_CONTRACT_VERSION,
    DryRunRequestError,
    normalize_dry_run_symbol,
    parse_evaluation_time,
    required_daily_history_bars,
)
from app.workers.strategies.registry import get_strategy
from app.workers.strategies.wyckoff_v2.constants import (
    default_config as v2_default_config,
)
from main import app

from test_wyckoff_v2_9c3_discovery import _FakeDB, _v2_configured_db


EVAL_TIME = "2025-07-15T12:00:00+00:00"


def _daily_payload(bars: int, *, price: float = 50.0) -> Dict[str, Any]:
    """Synthetic completed daily history ending well in the past."""
    dates = pd.bdate_range(end="2025-06-30", periods=bars)
    historical = []
    for i, d in enumerate(dates):
        px = price + (i % 7) * 0.25
        historical.append({
            "date": d.date().isoformat(),
            "open": px,
            "high": px + 0.5,
            "low": px - 0.5,
            "close": px + 0.1,
            "volume": 1_000_000 + (i % 5) * 10_000,
        })
    return {"historical": historical}


class FakeProvider:
    name = "fake_provider"

    def __init__(self, payload: Any):
        self.payload = payload
        self.calls: List[Dict[str, Any]] = []

    async def get_daily_history(self, symbol: str, timeseries: int = 400):
        self.calls.append({"symbol": symbol, "timeseries": timeseries})
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


def _forbid_production_writes(monkeypatch):
    """Any signal/watch/decision-card persistence attempt fails the test."""
    import app.workers.persistence as persistence_mod
    import app.workers.strategies.decision_card as card_mod

    def _bomb(*args, **kwargs):
        raise AssertionError("production persistence invoked by dry-run")

    monkeypatch.setattr(persistence_mod, "save_signal", _bomb)
    monkeypatch.setattr(card_mod, "build_decision_card", _bomb)


@pytest.fixture
def dry_run_client(monkeypatch):
    """Configured fake DB + fake provider; production writes are bombed."""
    monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
    _forbid_production_writes(monkeypatch)
    db = _v2_configured_db()
    provider = FakeProvider(_daily_payload(600))
    monkeypatch.setattr(admin_mod, "get_market_data_provider", lambda: provider)

    async def _override():
        yield db

    app.dependency_overrides[get_db] = _override
    try:
        yield TestClient(app, raise_server_exceptions=False), db, provider
    finally:
        app.dependency_overrides.pop(get_db, None)


def _post_dry_run(client, pattern_code="wyckoff_mtf_v2", **body):
    payload = {"symbol": "AAPL", "evaluation_time_utc": EVAL_TIME}
    payload.update(body)
    return client.post(
        f"/api/admin/strategies/{pattern_code}/dry-run", json=payload
    )


class TestDryRunRequestValidation:
    def test_symbol_normalization(self):
        assert normalize_dry_run_symbol(" brk.b ") == "BRK.B"

    @pytest.mark.parametrize("bad", ["", None, "TOO_LONG_SYMBOL", "AA PL", "a$b"])
    def test_bad_symbols_reject(self, bad):
        with pytest.raises(DryRunRequestError):
            normalize_dry_run_symbol(bad)

    def test_naive_timestamp_rejects(self):
        with pytest.raises(DryRunRequestError):
            parse_evaluation_time("2025-07-15T12:00:00")

    def test_timestamp_normalized_to_utc(self):
        parsed = parse_evaluation_time("2025-07-15T14:00:00+02:00")
        assert parsed == datetime(2025, 7, 15, 12, 0, tzinfo=timezone.utc)

    def test_none_timestamp_means_now(self):
        assert parse_evaluation_time(None) is None


class TestRequiredHistoryBars:
    def test_wyckoff_v2_uses_its_own_derivation(self):
        strategy = get_strategy("wyckoff_mtf_v2")
        cfg = v2_default_config()
        from app.workers.strategies.wyckoff_v2.readiness import (
            derive_history_requirement,
        )

        expected = derive_history_requirement(cfg)["desired_history_bars"]
        assert required_daily_history_bars("wyckoff_mtf_v2", cfg, strategy) == min(
            expected, 600
        )

    def test_sma150_arms_use_frame_derivations(self):
        from app.workers.shadow.frames import (
            required_history_bars_v2,
            required_history_bars_v3,
        )

        v2 = get_strategy("sma150_bounce")
        v3 = get_strategy("sma150_bounce_v3")
        assert required_daily_history_bars(
            "sma150_bounce", v2.default_config(), v2
        ) == min(required_history_bars_v2(v2.default_config()), 600)
        assert required_daily_history_bars(
            "sma150_bounce_v3", v3.default_config(), v3
        ) == min(required_history_bars_v3(v3.default_config()), 600)


class TestDryRunEndpoint:
    def test_authorized_dry_run_succeeds(self, dry_run_client):
        client, db, provider = dry_run_client
        resp = _post_dry_run(client)
        assert resp.status_code == 200
        body = resp.json()
        assert body["dry_run_contract_version"] == DRY_RUN_CONTRACT_VERSION
        assert body["persisted"] is False
        assert body["status"] == "evaluated"
        assert body["pattern_code"] == "wyckoff_mtf_v2"
        assert body["symbol"] == "AAPL"
        assert body["decision"] in ("ENTER", "WATCH", "AVOID")
        assert body["strategy_version"] == "wyckoff_mtf.v2"
        assert body["decision_policy_version"] == "wyckoff_mtf.policy.v1"
        assert body["provider"] == "fake_provider"
        assert len(provider.calls) == 1
        assert db.writes == []

    def test_unauthorized_request_fails_with_required_token(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post(
            "/api/admin/strategies/wyckoff_mtf_v2/dry-run",
            json={"symbol": "AAPL"},
        )
        assert resp.status_code == 401

    def test_authorized_token_passes_when_required(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", True)
        monkeypatch.setattr(settings, "WORKER_TOKEN", "test-worker-token")
        _forbid_production_writes(monkeypatch)
        db = _v2_configured_db()
        provider = FakeProvider(_daily_payload(600))
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", lambda: provider
        )

        async def _override():
            yield db

        app.dependency_overrides[get_db] = _override
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post(
                "/api/admin/strategies/wyckoff_mtf_v2/dry-run",
                json={"symbol": "AAPL", "evaluation_time_utc": EVAL_TIME},
                headers={"X-Worker-Token": "test-worker-token"},
            )
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_unknown_strategy_returns_404(self, dry_run_client):
        client, db, provider = dry_run_client
        resp = _post_dry_run(client, pattern_code="no_such_strategy")
        assert resp.status_code == 404
        # No fallback strategy was invoked and no data was fetched.
        assert provider.calls == []

    def test_registered_disabled_strategy_is_evaluable(self, dry_run_client):
        client, db, provider = dry_run_client
        resp = _post_dry_run(client)
        body = resp.json()
        # DB row exists with is_enabled=false — dry-run still evaluates but
        # reports the true enablement state.
        assert body["enabled"] is False
        assert body["db_configured"] is True
        assert body["config_status"] == "configured"
        assert body["status"] == "evaluated"

    def test_missing_db_row_fails_safely(self, monkeypatch):
        monkeypatch.setattr(settings, "REQUIRE_WORKER_TOKEN", False)
        _forbid_production_writes(monkeypatch)
        db = _FakeDB(patterns={}, configs={})
        provider = FakeProvider(_daily_payload(600))
        monkeypatch.setattr(
            admin_mod, "get_market_data_provider", lambda: provider
        )

        async def _override():
            yield db

        app.dependency_overrides[get_db] = _override
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post(
                "/api/admin/strategies/wyckoff_mtf_v2/dry-run",
                json={"symbol": "AAPL", "evaluation_time_utc": EVAL_TIME},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["enabled"] is None
            assert body["db_configured"] is False
            assert body["config_status"] == "missing_pattern_row"
            assert body["status"] == "evaluated"
            assert body["persisted"] is False
            assert db.writes == []
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_rollout_flags_preserved(self, dry_run_client):
        client, db, provider = dry_run_client
        body = _post_dry_run(client).json()
        assert body["rollout_flags"] == {
            "allow_enter": False,
            "enable_4h_trigger": False,
            "min_price": 5.0,
        }
        # Nothing was mutated to produce the answer.
        assert db.writes == []

    def test_rollout_blocked_state_is_explicit_not_enter(self, dry_run_client):
        client, db, provider = dry_run_client
        body = _post_dry_run(client).json()
        # With allow_enter=false a dry-run can never return a production
        # ENTER unless the policy itself allowed it (it cannot).
        assert body["decision"] != "ENTER"
        if body["enter_eligible_without_rollout_gate"]:
            assert body["rollout_blocked"] is True

    def test_insufficient_history_is_typed(self, monkeypatch, dry_run_client):
        client, db, provider = dry_run_client
        provider.payload = _daily_payload(30)
        body = _post_dry_run(client).json()
        assert body["status"] == "evaluated"
        assert body["decision"] == "AVOID"
        assert body["readiness_status"] == "insufficient_history"
        assert body["insufficient_data"] is True

    def test_empty_payload_is_frame_rejected(self, dry_run_client):
        client, db, provider = dry_run_client
        provider.payload = {"historical": []}
        body = _post_dry_run(client).json()
        assert body["status"] == "frame_rejected"
        assert body["error_reason_code"] == "no_data"
        assert body["decision"] is None
        assert body["persisted"] is False

    def test_provider_error_handled_safely(self, dry_run_client):
        client, db, provider = dry_run_client
        provider.payload = RuntimeError("boom")
        resp = _post_dry_run(client)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "provider_error"
        assert body["error_reason_code"] == "provider_RuntimeError"
        assert "boom" not in str(body)
        assert body["decision"] is None

    def test_invalid_symbol_returns_422(self, dry_run_client):
        client, db, provider = dry_run_client
        resp = _post_dry_run(client, symbol="not a symbol!")
        assert resp.status_code == 422
        assert provider.calls == []

    def test_naive_evaluation_time_returns_422(self, dry_run_client):
        client, db, provider = dry_run_client
        resp = _post_dry_run(client, evaluation_time_utc="2025-07-15T12:00:00")
        assert resp.status_code == 422

    def test_dry_run_never_persists_anything(self, dry_run_client):
        client, db, provider = dry_run_client
        body = _post_dry_run(client).json()
        assert body["persisted"] is False
        # The fake DB raises on any write; reaching here with zero recorded
        # writes proves no signals/watches/cards/config rows were touched.
        assert db.writes == []

    def test_missing_trigger_stays_missing_and_nothing_fabricated(
        self, dry_run_client
    ):
        client, db, provider = dry_run_client
        body = _post_dry_run(client).json()
        # No 4H data was supplied and the trigger is disabled by rollout:
        # the dry-run must never invent stop/target or a trigger price.
        assert body["stop_price"] is None
        assert body["target_price"] is None
        if body["decision"] != "ENTER":
            assert body["entry_price"] is None

    def test_deterministic_repeat(self, dry_run_client):
        client, db, provider = dry_run_client
        first = _post_dry_run(client).json()
        second = _post_dry_run(client).json()
        assert first == second
