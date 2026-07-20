"""Massive client tests — retries, auth, pagination, errors. All HTTP mocked."""

import asyncio
import json

import pytest

from app.workers.massive_client import MAX_ATTEMPTS, MassiveApiError, MassiveClient


KEY = "SECRET-API-KEY-123"


def _client(responses):
    """Client with fake transport. `responses` is a list of (status, text)."""
    client = MassiveClient(
        api_key=KEY,
        base_url="https://api.massive.com",
        requests_per_minute=1_000_000,  # no throttling in tests
        retry_base_delay=0.001,
    )
    calls = []

    async def fake_raw_get(url):
        calls.append(url)
        if not responses:
            raise AssertionError("unexpected extra HTTP call")
        status, text = responses.pop(0)
        return status, text

    client._raw_get = fake_raw_get
    return client, calls


def _page(results, next_url=None):
    payload = {"results": results}
    if next_url:
        payload["next_url"] = next_url
    return 200, json.dumps(payload)


def test_pagination_follows_next_url_with_auth():
    # next_url comes back WITHOUT credentials — auth must be re-applied.
    next_url = "https://api.massive.com/v3/reference/tickers?cursor=abc123"
    client, calls = _client([
        _page([{"ticker": "AAA"}], next_url=next_url),
        _page([{"ticker": "BBB"}]),
    ])

    results = asyncio.run(client.list_tickers())
    assert [r["ticker"] for r in results] == ["AAA", "BBB"]
    assert len(calls) == 2
    # First call authenticated:
    assert f"apiKey={KEY}" in calls[0]
    # Follow-up call keeps cursor AND re-applies the key:
    assert "cursor=abc123" in calls[1]
    assert f"apiKey={KEY}" in calls[1]


def test_429_retries_then_succeeds():
    client, calls = _client([
        (429, "rate limited"),
        _page([{"ticker": "AAA"}]),
    ])
    results = asyncio.run(client.list_tickers())
    assert len(results) == 1
    assert len(calls) == 2


def test_5xx_retries_then_succeeds():
    client, calls = _client([
        (503, "unavailable"),
        (500, "boom"),
        _page([{"ticker": "AAA"}]),
    ])
    results = asyncio.run(client.list_tickers())
    assert len(results) == 1
    assert len(calls) == 3


def test_persistent_429_raises_after_max_attempts():
    client, calls = _client([(429, "rate limited")] * MAX_ATTEMPTS)
    with pytest.raises(MassiveApiError) as exc:
        asyncio.run(client.list_tickers())
    assert exc.value.status_code == 429
    assert len(calls) == MAX_ATTEMPTS


@pytest.mark.parametrize("status", [401, 403])
def test_auth_errors_fail_fast_without_retry(status):
    client, calls = _client([(status, '{"error": "unauthorized"}')])
    with pytest.raises(MassiveApiError) as exc:
        asyncio.run(client.list_tickers())
    err = exc.value
    assert err.status_code == status
    assert err.provider == "massive"
    assert err.endpoint == "/v3/reference/tickers"
    assert len(calls) == 1  # no retries on auth errors


def test_malformed_json_raises_structured_error():
    client, _ = _client([(200, "<html>not json</html>")])
    with pytest.raises(MassiveApiError) as exc:
        asyncio.run(client.get_grouped_daily("2026-07-17"))
    assert "malformed JSON" in str(exc.value)
    assert exc.value.endpoint.startswith("/v2/aggs/grouped")


def test_api_key_never_leaks_into_errors():
    # Body echoes the key (worst case); it must be stripped from the excerpt.
    body = f'{{"error": "bad", "url": "https://x?apiKey={KEY}", "raw": "{KEY}"}}'
    client, _ = _client([(400, body)])
    with pytest.raises(MassiveApiError) as exc:
        asyncio.run(client.list_tickers())
    assert KEY not in str(exc.value)
    assert KEY not in exc.value.excerpt


def test_empty_results_handled():
    client, _ = _client([(200, json.dumps({"results": []}))])
    assert asyncio.run(client.get_grouped_daily("2026-07-17")) == []

    client, _ = _client([(200, json.dumps({}))])
    assert asyncio.run(client.get_grouped_daily("2026-07-17")) == []


def test_ticker_details_404_returns_none():
    client, _ = _client([(404, '{"status": "NOT_FOUND"}')])
    assert asyncio.run(client.get_ticker_details("ZZZZ")) is None
