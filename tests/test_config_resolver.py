"""Config parsing/merge/resolution (B1 support)."""

import asyncio

import app.workers.patterns.config as config_mod
from app.workers.patterns.config import (
    coerce_config_value,
    merge_config,
    parse_config_values,
    resolve_pattern_config,
)


DEFAULTS = {"touch_tolerance_pct": 3.0, "min_bounces": 2, "score_threshold": 0.5}


def test_coerce_json_encoded_values():
    assert coerce_config_value("150") == 150
    assert coerce_config_value("5.0") == 5.0
    assert coerce_config_value('{"min_market_cap": 200000000}') == {
        "min_market_cap": 200000000
    }
    # Already-native values pass through unchanged.
    assert coerce_config_value(150) == 150
    # Non-JSON strings are preserved, never invented.
    assert coerce_config_value("abc") == "abc"


def test_parse_config_values():
    parsed = parse_config_values({"a": "1", "b": "2.5"})
    assert parsed == {"a": 1, "b": 2.5}


def test_merge_falls_back_when_empty():
    merged, used_fallback = merge_config({}, DEFAULTS)
    assert used_fallback is True
    assert merged == DEFAULTS
    assert merged is not DEFAULTS  # must be a copy


def test_merge_overrides_defaults():
    merged, used_fallback = merge_config({"min_bounces": "7"}, DEFAULTS)
    assert used_fallback is False
    assert merged["min_bounces"] == 7
    assert merged["touch_tolerance_pct"] == 3.0  # untouched default


def test_resolve_uses_defaults_when_db_empty(monkeypatch):
    async def fake_get_config(_code):
        return {}

    monkeypatch.setattr(config_mod, "get_pattern_config", fake_get_config)
    resolved = asyncio.run(resolve_pattern_config("sma150_bounce", DEFAULTS))
    assert resolved == DEFAULTS


def test_resolve_applies_db_overrides(monkeypatch):
    async def fake_get_config(_code):
        return {"min_bounces": "9", "score_threshold": "0.9"}

    monkeypatch.setattr(config_mod, "get_pattern_config", fake_get_config)
    resolved = asyncio.run(resolve_pattern_config("sma150_bounce", DEFAULTS))
    assert resolved["min_bounces"] == 9
    assert resolved["score_threshold"] == 0.9
