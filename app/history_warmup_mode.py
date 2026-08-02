"""HISTORY_WARMUP_ONLY_MODE route allowlist (read-only foundation).

When HISTORY_WARMUP_ONLY_MODE is on, the app exposes ONLY liveness/version and
the two read-only history-warmup FOUNDATION routes. There is intentionally NO
execute route and NO strategy/campaign/outcome route in this task. Mutually
exclusive with audit-only and maintenance-only modes.
"""

from __future__ import annotations

from typing import Set

HISTORY_WARMUP_ONLY_ALLOWLIST: Set[str] = frozenset({
    "/",
    "/version",
    "/api/version",
    "/health",
    "/api/health",
    "/api/admin/history-warmup/access-check",
    "/api/admin/history-warmup/preflight",
    "/api/admin/history-warmup/incremental/preflight",
})

# Read-only methods for the allowlist above (HEAD/OPTIONS for infra + CORS).
HISTORY_WARMUP_ONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})

# The bounded mutation routes reachable in warmup mode (POST only, exact paths):
#   * execute — the single provider-backed INITIAL-DEPTH warmup batch;
#   * incremental/execute — the single provider-backed INCREMENTAL-REFRESH
#     batch (distinct mode, distinct contract, shares the provider/rate-limit
#     boundary and advisory lock — never a second execute path with its own
#     provider client);
#   * universes — create/freeze a bounded frozen universe (no provider).
HISTORY_WARMUP_EXECUTE_PATH: str = "/api/admin/history-warmup/execute"
HISTORY_WARMUP_INCREMENTAL_EXECUTE_PATH: str = "/api/admin/history-warmup/incremental/execute"
HISTORY_WARMUP_UNIVERSES_PATH: str = "/api/admin/history-warmup/universes"
HISTORY_WARMUP_POST_PATHS = frozenset({
    HISTORY_WARMUP_EXECUTE_PATH, HISTORY_WARMUP_INCREMENTAL_EXECUTE_PATH,
    HISTORY_WARMUP_UNIVERSES_PATH,
})


def is_history_warmup_route_allowed(method: str, path: str) -> bool:
    m = (method or "").upper()
    p = path or ""
    # POST is permitted ONLY for the bounded mutation routes; OPTIONS is allowed
    # on them for CORS preflight but never GET/HEAD (not readable resources).
    if p in HISTORY_WARMUP_POST_PATHS:
        return m in ("POST", "OPTIONS")
    if m not in HISTORY_WARMUP_ONLY_METHODS:
        return False
    return p in HISTORY_WARMUP_ONLY_ALLOWLIST


__all__ = [
    "HISTORY_WARMUP_ONLY_ALLOWLIST",
    "HISTORY_WARMUP_ONLY_METHODS",
    "HISTORY_WARMUP_EXECUTE_PATH",
    "HISTORY_WARMUP_INCREMENTAL_EXECUTE_PATH",
    "HISTORY_WARMUP_UNIVERSES_PATH",
    "HISTORY_WARMUP_POST_PATHS",
    "is_history_warmup_route_allowed",
]
