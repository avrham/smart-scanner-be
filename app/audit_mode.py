"""Audit-only route gate (Deployment Readiness).

When AUDIT_ONLY_MODE is enabled, the running API must expose ONLY a narrow,
read-only allowlist: revision/liveness plus the shadow-cohort audit routes.
Every other route — including all mutation endpoints, provider/universe/scan
endpoints, campaign create/resume, outcome calculation, and the OpenAPI/docs
routes — is rejected BEFORE its handler executes, so no provider or database
mutation path can be reached even with a valid worker token.

This is a single gate in front of the existing app (one middleware over the
existing router tree) — never a second FastAPI app or a parallel router.
"""

from __future__ import annotations

from typing import Set


# Exact, static paths allowed in audit-only mode. All are read-only:
#   * revision proof + liveness (public);
#   * the two shadow-cohort audit routes (worker-token protected — their own
#     dependencies still enforce the token; this gate never bypasses auth).
AUDIT_ONLY_ALLOWLIST: Set[str] = frozenset({
    "/",
    "/version",
    "/api/version",
    "/health",
    "/api/health",
    "/api/admin/shadow-cohort/access-check",
    "/api/admin/shadow-cohort/closeout",
})

# Read-only HTTP methods permitted for allowlisted routes (HEAD supported so
# Fly/infra probes work; OPTIONS for CORS preflight).
AUDIT_ONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})


def is_audit_route_allowed(method: str, path: str) -> bool:
    """Whether one request may proceed under audit-only mode.

    Exact path match against the allowlist AND a read-only method. Deterministic
    and side-effect free; the caller (middleware) blocks everything else.
    """
    if (method or "").upper() not in AUDIT_ONLY_METHODS:
        return False
    return (path or "") in AUDIT_ONLY_ALLOWLIST


__all__ = [
    "AUDIT_ONLY_ALLOWLIST",
    "AUDIT_ONLY_METHODS",
    "is_audit_route_allowed",
]
