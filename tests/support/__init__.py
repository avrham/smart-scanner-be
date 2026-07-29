"""Test-only support helpers (deterministic fake provider, network guard).

Nothing in this package is importable/selectable by production code paths — the
provider factory (`app.providers.get_market_data_provider`) only knows 'massive'
and 'fmp'. The fake is injected explicitly by tests.
"""
