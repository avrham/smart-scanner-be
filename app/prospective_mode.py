"""PROSPECTIVE_CAMPAIGN_ONLY_MODE route allowlist.

When on, the app exposes ONLY liveness/version + the prospective foundation
routes: read-only access-check / preflight / audit (GET) and the bounded
mutation route register (POST). Durable-queue additions: the prospective
ENQUEUE route, the generic read-only job-management routes, the generic
job/schedule mutation routes (cancel / retry-failed / schedule create-pause-
resume-patch), and read-only schedule listing/preview. The synchronous execute
route remains reachable for backward compatibility but is NOT the primary
execution system (the durable queue + worker is). No generic campaign/outcome/
warmup/maintenance/audit route is reachable. Mutually exclusive with the
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
    "/api/admin/prospective/outcomes/preflight",
    # generic job-management listing (read-only)
    "/api/admin/jobs",
    "/api/admin/jobs/workers",
    # generic schedule listing (read-only)
    "/api/admin/job-schedules",
})

PROSPECTIVE_ONLY_METHODS: Set[str] = frozenset({"GET", "HEAD", "OPTIONS"})

# Bounded POST mutation routes. register creates a registration; the prospective
# ENQUEUE route creates the durable campaign job (the primary execution path);
# execute remains for backward compatibility. OPTIONS allowed for CORS.
PROSPECTIVE_REGISTER_PATH: str = "/api/admin/prospective/register"
PROSPECTIVE_EXECUTE_PATH: str = "/api/admin/prospective/execute"
PROSPECTIVE_ENQUEUE_PATH: str = "/api/admin/prospective/jobs"
PROSPECTIVE_OUTCOME_ENQUEUE_PATH: str = "/api/admin/prospective/outcomes/jobs"
PROSPECTIVE_POST_PATHS = frozenset({
    PROSPECTIVE_REGISTER_PATH,
    PROSPECTIVE_EXECUTE_PATH,
    PROSPECTIVE_ENQUEUE_PATH,
    PROSPECTIVE_OUTCOME_ENQUEUE_PATH,
    "/api/admin/job-schedules",  # create a schedule (POST)
})

# Read-only GET routes with a path parameter (prefix-matched):
#   /api/admin/jobs/{job_id}, .../tasks, .../events
#   /api/admin/job-schedules/{id}/preview
_GET_PREFIXES = ("/api/admin/jobs/", "/api/admin/job-schedules/")

# Job/schedule POST mutations with a path parameter (suffix-matched):
#   /api/admin/jobs/{job_id}/cancel, /retry-failed
#   /api/admin/job-schedules/{schedule_id}/pause, /resume
_POST_SUFFIXES = ("/cancel", "/retry-failed", "/pause", "/resume")


def _is_job_scoped(path: str) -> bool:
    return path.startswith("/api/admin/jobs/") or path.startswith("/api/admin/job-schedules/")


def is_prospective_route_allowed(method: str, path: str) -> bool:
    m = (method or "").upper()
    p = path or ""
    # bounded POST mutation routes (register / execute / enqueue / schedule-create)
    if m in ("POST", "OPTIONS") and p in PROSPECTIVE_POST_PATHS:
        return True
    # scoped POST mutations (cancel / retry-failed / pause / resume)
    if (m in ("POST", "OPTIONS") and _is_job_scoped(p)
            and any(p.endswith(s) for s in _POST_SUFFIXES)):
        return True
    # PATCH a schedule: /api/admin/job-schedules/{id}
    if m == "PATCH":
        return (p.startswith("/api/admin/job-schedules/")
                and not any(p.endswith(s) for s in _POST_SUFFIXES))
    # read-only routes (fixed allowlist + scoped GET detail/preview)
    if m in PROSPECTIVE_ONLY_METHODS:
        if p in PROSPECTIVE_ONLY_ALLOWLIST:
            return True
        if p.startswith(_GET_PREFIXES) and not any(p.endswith(s) for s in _POST_SUFFIXES):
            return True
    return False


__all__ = [
    "PROSPECTIVE_ONLY_ALLOWLIST",
    "PROSPECTIVE_ONLY_METHODS",
    "PROSPECTIVE_REGISTER_PATH",
    "PROSPECTIVE_EXECUTE_PATH",
    "PROSPECTIVE_ENQUEUE_PATH",
    "PROSPECTIVE_OUTCOME_ENQUEUE_PATH",
    "PROSPECTIVE_POST_PATHS",
    "is_prospective_route_allowed",
]
