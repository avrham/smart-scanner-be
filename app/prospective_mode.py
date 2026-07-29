"""PROSPECTIVE_CAMPAIGN_ONLY_MODE route allowlist.

When on, the app exposes ONLY liveness/version + the prospective foundation
routes: read-only access-check / preflight / audit (GET) and the two bounded
mutation routes register + execute (POST). No generic campaign/outcome/warmup/
maintenance/audit/scheduler route is reachable. Mutually exclusive with the
audit-only, maintenance-only and history-warmup-only modes.
"""

from __future__ import annotations

from typing import Set

PROSPECTIVE_ONLY_ALLOWLIST: Set[str] = frozenset({
    "/",
    "/version",
    "/api/version",
    "/health",
    "/api/health",
    "/api/admin/prospective/access-check",
    "/api/admin/prospective/preflight",
    "/api/admin/prospective/audit",
})

PROSPECTIVE_ONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})

# The two bounded mutation routes (POST only; register creates a registration,
# execute is the ONE campaign-mutation route). OPTIONS allowed for CORS.
PROSPECTIVE_REGISTER_PATH: str = "/api/admin/prospective/register"
PROSPECTIVE_EXECUTE_PATH: str = "/api/admin/prospective/execute"
PROSPECTIVE_POST_PATHS = frozenset({PROSPECTIVE_REGISTER_PATH, PROSPECTIVE_EXECUTE_PATH})


def is_prospective_route_allowed(method: str, path: str) -> bool:
    m = (method or "").upper()
    p = path or ""
    if p in PROSPECTIVE_POST_PATHS:
        return m in ("POST", "OPTIONS")
    if m not in PROSPECTIVE_ONLY_METHODS:
        return False
    return p in PROSPECTIVE_ONLY_ALLOWLIST


__all__ = [
    "PROSPECTIVE_ONLY_ALLOWLIST",
    "PROSPECTIVE_ONLY_METHODS",
    "PROSPECTIVE_REGISTER_PATH",
    "PROSPECTIVE_EXECUTE_PATH",
    "PROSPECTIVE_POST_PATHS",
    "is_prospective_route_allowed",
]
