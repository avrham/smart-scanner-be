"""Pure unit tests for the bounded history-warmup execute logic (no DB, no
provider, no network): server-selected batch, retry plan, idempotency identity,
request validation, failure taxonomy, and bar normalization."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.prospective_readiness import build_prospective_readiness_v2
from app.history_warmup_execute import (
    EXECUTE_CONTRACT_VERSION, MODE_NORMAL, MODE_RETRY, FAILURE_TAXONOMY,
    RETRYABLE, TERMINAL, OPERATOR_ERROR, error_class, is_retryable,
    map_provider_error, HistoryWarmupPayloadError, compute_retry_plan,
    compute_progress, select_next_batch, build_preflight_v2, execution_identity,
    validate_execute_request, normalize_daily_bars, normalize_4h_bars,
)
from tests.support.fake_provider import (
    make_ready_4h, make_invalid_4h, rate_limited_error, auth_error)

NOW = datetime(2026, 7, 29, 20, 0, tzinfo=timezone.utc)
COOLDOWN_OK = {"execution_allowed_by_cooldown": True, "min_interval_seconds": 75,
               "next_execution_not_before": None, "cooldown_remaining_seconds": 0}


def _daily_row(sym, n, months, weeks, latest="2026-07-28"):
    return {"symbol": sym, "daily_bars": n, "month_groups": months,
            "week_groups": weeks, "oldest": "2024-01-01", "latest": latest}


def _readiness(symbols, daily_rows, fourh_rows):
    return build_prospective_readiness_v2(symbols, daily_rows, fourh_rows, now=NOW)


class TestRetryPlanAndProgress:
    def test_retryable_and_terminal_classification(self):
        items = {
            "AAA": {"status": "failed", "error_code": "provider_rate_limited",
                    "error_class": "retryable", "retryable": True, "attempt": 1},
            "BBB": {"status": "failed", "error_code": "provider_invalid_payload",
                    "error_class": "terminal", "retryable": False, "attempt": 1},
            "CCC": {"status": "completed", "retryable": False},
        }
        rp = compute_retry_plan(items)
        assert rp["retryable_symbols"] == ["AAA"]
        assert rp["terminal_symbols"] == ["BBB"]
        assert rp["retry_plan_hash"].startswith("sha256:")

    def test_retry_plan_hash_changes_with_membership(self):
        a = compute_retry_plan({"AAA": {"status": "failed", "error_class": "retryable",
                                        "error_code": "provider_timeout", "retryable": True}})
        b = compute_retry_plan({})
        assert a["retry_plan_hash"] != b["retry_plan_hash"]

    def test_progress_excludes_retryable_and_terminal_from_normal(self):
        rd = _readiness(["AAA", "BBB"], [_daily_row("AAA", 10, 2, 3),
                                         _daily_row("BBB", 10, 2, 3)], [])
        rp = {"retryable_symbols": ["AAA"], "terminal_symbols": ["BBB"],
              "retry_plan_hash": "sha256:x", "entries": []}
        prog = compute_progress(rd, rp)
        assert prog["normal_pending_symbols"] == []   # both excluded
        assert prog["normal_complete"] is True


class TestNextBatchSelection:
    def test_normal_next_batch_one_symbol(self):
        rd = _readiness(["AAA", "BBB"], [_daily_row("AAA", 10, 2, 3),
                                         _daily_row("BBB", 10, 2, 3)], [])
        rp = compute_retry_plan({})
        prog = compute_progress(rd, rp)
        nb = select_next_batch(rd, rp, prog, max_batch=1)
        assert nb["available"] and nb["mode"] == MODE_NORMAL
        assert nb["symbols"] == ["AAA"] and nb["symbol_count"] == 1
        assert nb["estimated_provider_requests"] == 2  # 1 daily + 1 4H

    def test_retry_first_hard_stops_normal(self):
        rd = _readiness(["AAA", "BBB"], [_daily_row("AAA", 10, 2, 3),
                                         _daily_row("BBB", 10, 2, 3)], [])
        rp = {"retryable_symbols": ["BBB"], "terminal_symbols": [],
              "retry_plan_hash": "sha256:r", "entries": []}
        prog = compute_progress(rd, rp)
        nb = select_next_batch(rd, rp, prog, max_batch=1)
        assert nb["mode"] == MODE_RETRY and nb["symbols"] == ["BBB"]

    def test_unavailable_when_all_ready(self):
        rd = _readiness(["AAA"], [_daily_row("AAA", 520, 26, 54)],
                        [{"symbol": "AAA", "completed_4h_bars": 12,
                          "oldest_4h": "2026-07-20", "latest_4h": "2026-07-28"}])
        rp = compute_retry_plan({})
        prog = compute_progress(rd, rp)
        nb = select_next_batch(rd, rp, prog, max_batch=1)
        assert nb["available"] is False and nb["reason"] == "all_symbols_launch_ready"


def _live_preflight(symbols=("AAA",)):
    rd = _readiness(list(symbols), [_daily_row(s, 10, 2, 3) for s in symbols], [])
    return build_preflight_v2(rd, {}, COOLDOWN_OK, max_batch=1)


def _good_body(pf, mode=MODE_NORMAL):
    nb = pf["next_batch"]
    body = {"contract_version": EXECUTE_CONTRACT_VERSION, "mode": mode,
            "universe_hash": pf["universe_hash"], "config_hash": pf["config_hash"],
            "next_batch_hash": nb["next_batch_hash"], "symbols": list(nb["symbols"]),
            "limit": len(nb["symbols"])}
    if mode == MODE_NORMAL:
        body["readiness_manifest_hash"] = pf["combined_readiness_manifest_hash"]
    else:
        body["retry_plan_hash"] = pf["retry_plan_hash"]
    return body


class TestValidateExecuteRequest:
    def test_valid_normal(self):
        pf = _live_preflight()
        v = validate_execute_request(_good_body(pf), pf, max_batch=1)
        assert v["ok"] and v["mode"] == MODE_NORMAL and v["symbols"] == ["AAA"]
        assert v["execution_identity"].startswith("hwx:")

    @pytest.mark.parametrize("field", ["provider", "adjustment", "from_date",
                                        "to_date", "timeseries", "table_name", "retries"])
    def test_forbidden_fields_rejected(self, field):
        pf = _live_preflight()
        body = _good_body(pf); body[field] = "x"
        v = validate_execute_request(body, pf, max_batch=1)
        assert not v["ok"] and v["reason"].startswith("forbidden_request_fields")

    def test_bad_contract_version(self):
        pf = _live_preflight(); body = _good_body(pf); body["contract_version"] = "v9"
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "bad_contract_version"

    def test_stale_manifest(self):
        pf = _live_preflight(); body = _good_body(pf)
        body["readiness_manifest_hash"] = "sha256:stale"
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "stale_manifest"

    def test_stale_next_batch(self):
        pf = _live_preflight(); body = _good_body(pf); body["next_batch_hash"] = "sha256:stale"
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "stale_next_batch"

    def test_unauthorized_symbol_rejected(self):
        pf = _live_preflight(); body = _good_body(pf)
        body["symbols"] = ["ZZZ"]; body["limit"] = 1
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "symbols_not_server_selected_batch"

    def test_batch_size_gt_1_rejected(self):
        pf = _live_preflight(); body = _good_body(pf)
        body["symbols"] = ["AAA", "BBB"]; body["limit"] = 2
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "batch_size_out_of_range"

    def test_limit_must_equal_count(self):
        pf = _live_preflight(); body = _good_body(pf); body["limit"] = 5
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "limit_must_equal_symbol_count"

    def test_retry_mode_when_normal_batch_current_rejected(self):
        pf = _live_preflight(); body = _good_body(pf, mode=MODE_RETRY)
        # current batch is normal -> retry request mismatches
        assert validate_execute_request(body, pf, max_batch=1)["reason"] == "mode_not_current_batch"


class TestExecutionIdentity:
    def test_deterministic(self):
        kw = dict(mode="normal", universe_hash="u", config_hash="c", plan_hash="p",
                  next_batch_hash="n", symbols=["AAA"])
        assert execution_identity(**kw) == execution_identity(**kw)

    def test_payload_sensitive(self):
        base = dict(mode="normal", universe_hash="u", config_hash="c", plan_hash="p",
                    next_batch_hash="n", symbols=["AAA"])
        assert execution_identity(**base) != execution_identity(**{**base, "symbols": ["BBB"]})
        assert execution_identity(**base) != execution_identity(**{**base, "next_batch_hash": "n2"})


class TestFailureTaxonomy:
    def test_all_codes_classified(self):
        for code, klass in FAILURE_TAXONOMY.items():
            assert klass in (RETRYABLE, TERMINAL, OPERATOR_ERROR)
        assert is_retryable("provider_rate_limited")
        assert not is_retryable("provider_auth_error")   # never retryable
        assert error_class("provider_invalid_payload") == TERMINAL
        assert error_class("provider_auth_error") == OPERATOR_ERROR

    def test_map_provider_error(self):
        assert map_provider_error(rate_limited_error()) == ("provider_rate_limited", RETRYABLE)
        assert map_provider_error(auth_error()) == ("provider_auth_error", OPERATOR_ERROR)
        assert map_provider_error(HistoryWarmupPayloadError("ohlc_envelope")) == (
            "provider_invalid_payload", TERMINAL)


class TestBarNormalization:
    def test_daily_drops_incomplete_today(self):
        from datetime import date
        today = NOW.date()
        bars = [{"symbol": "aaa", "trading_date": today - timedelta(days=1),
                 "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 9},
                {"symbol": "aaa", "trading_date": today,  # not completed
                 "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 9}]
        out = normalize_daily_bars(bars, now=NOW)
        assert len(out) == 1 and out[0]["symbol"] == "AAA"
        assert out[0]["trading_date"] == today - timedelta(days=1)

    def test_4h_computes_fields_and_skips_forming(self):
        payload = make_ready_4h("AAA", now=NOW, bars=12)
        # add a currently-forming bar (bar_end in the future)
        payload["bars"].append({"start_utc": NOW - timedelta(hours=1),
                                 "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 5})
        rows = normalize_4h_bars(payload, symbol="AAA", now=NOW)
        assert len(rows) == 12  # forming bar excluded
        r = rows[0]
        assert r["bar_end"] - r["bar_start"] == timedelta(hours=4)
        assert r["is_completed"] is True
        assert r["provider"] == "massive" and r["provider_adjustment"] == "split_dividend_adjusted"
        assert r["content_fingerprint"].startswith("sha256:")
        assert r["session_date"] is not None

    def test_4h_invalid_envelope_rejected(self):
        with pytest.raises(HistoryWarmupPayloadError):
            normalize_4h_bars(make_invalid_4h("AAA", now=NOW), symbol="AAA", now=NOW)

    def test_4h_missing_volume_rejected(self):
        payload = {"bars": [{"start_utc": NOW - timedelta(hours=9),
                             "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": None}]}
        with pytest.raises(HistoryWarmupPayloadError) as e:
            normalize_4h_bars(payload, symbol="AAA", now=NOW)
        assert e.value.code == "missing_volume"

    def test_4h_correction_changes_fingerprint(self):
        p1 = make_ready_4h("AAA", now=NOW, bars=1, close=1.5)
        p2 = make_ready_4h("AAA", now=NOW, bars=1, close=1.9)
        r1 = normalize_4h_bars(p1, symbol="AAA", now=NOW)[0]
        r2 = normalize_4h_bars(p2, symbol="AAA", now=NOW)[0]
        assert r1["bar_start"] == r2["bar_start"]
        assert r1["content_fingerprint"] != r2["content_fingerprint"]
