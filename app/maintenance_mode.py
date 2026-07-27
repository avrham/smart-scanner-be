"""Maintenance-only route gate (Shadow Outcome Maintenance Environment).

When MAINTENANCE_ONLY_MODE is enabled, the running API must expose ONLY a
narrow allowlist: revision/liveness plus the three shadow-maintenance routes
(read-only access-check, read-only preflight, and the ONE tightly-validated
mutation route). Every other route — including the generic outcome-calculation
endpoint, campaign create/resume, scans, universe/ticker refresh and the
OpenAPI/docs routes — is rejected BEFORE its handler executes, so no broad
mutation path can be reached even with a valid worker token.

Unlike audit-only mode, exactly ONE mutation route is permitted, and only for
POST. Every other allowlisted route is read-only. This is a single gate in
front of the existing app (one middleware over the existing router tree) —
never a second FastAPI app or a parallel router. A valid worker token never
bypasses this gate.
"""

from __future__ import annotations

from typing import Dict, Set


# Read-only allowlisted paths (GET/HEAD/OPTIONS only).
MAINTENANCE_READONLY_ALLOWLIST: Set[str] = frozenset({
    "/",
    "/version",
    "/api/version",
    "/health",
    "/api/health",
    "/api/admin/shadow-maintenance/access-check",
    "/api/admin/shadow-maintenance/preflight",
})

# The single mutation path, permitted for POST only.
MAINTENANCE_EXECUTE_PATH = "/api/admin/shadow-maintenance/outcomes/execute"

# Read-only methods permitted for the read allowlist (HEAD for infra probes,
# OPTIONS for CORS preflight).
READONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})
# Methods permitted for the single mutation path.
EXECUTE_METHODS: Set[str] = frozenset({"POST", "OPTIONS"})

# Exact method→path map (for reporting only; the predicate is authoritative).
MAINTENANCE_ALLOWED_ROUTES: Dict[str, str] = {
    "GET /": "liveness",
    "GET /version": "revision",
    "GET /api/version": "revision",
    "GET /health": "liveness",
    "GET /api/health": "liveness",
    "GET /api/admin/shadow-maintenance/access-check": "access-check (read-only)",
    "GET /api/admin/shadow-maintenance/preflight": "preflight (read-only)",
    "POST /api/admin/shadow-maintenance/outcomes/execute": "execute (single mutation)",
}


def is_maintenance_route_allowed(method: str, path: str) -> bool:
    """Whether one request may proceed under maintenance-only mode.

    Deterministic and side-effect free; the caller (middleware) blocks
    everything else with the stable hidden-route 404.
    """
    m = (method or "").upper()
    p = path or ""
    if p == MAINTENANCE_EXECUTE_PATH:
        return m in EXECUTE_METHODS
    if p in MAINTENANCE_READONLY_ALLOWLIST:
        return m in READONLY_METHODS
    return False


__all__ = [
    "MAINTENANCE_READONLY_ALLOWLIST",
    "MAINTENANCE_EXECUTE_PATH",
    "READONLY_METHODS",
    "EXECUTE_METHODS",
    "MAINTENANCE_ALLOWED_ROUTES",
    "is_maintenance_route_allowed",
]
