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
})

# Read-only methods only (HEAD/OPTIONS for infra + CORS). No POST — no execute.
HISTORY_WARMUP_ONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})


def is_history_warmup_route_allowed(method: str, path: str) -> bool:
    if (method or "").upper() not in HISTORY_WARMUP_ONLY_METHODS:
        return False
    return (path or "") in HISTORY_WARMUP_ONLY_ALLOWLIST


__all__ = [
    "HISTORY_WARMUP_ONLY_ALLOWLIST",
    "HISTORY_WARMUP_ONLY_METHODS",
    "is_history_warmup_route_allowed",
]
