"""Explicit read-only audit database connection selection (Deployment Readiness).

The legacy connector derives the PostgreSQL username from the Supabase project
ref (`postgres.<ref>` on the pooler, `postgres` direct) — so changing only the
password can never log in as a custom least-privilege role. Audit-only
deployments therefore supply a COMPLETE connection identity via
`AUDIT_DATABASE_URL` (a secret), and this module selects it.

Rules (all enforced here so `deps.init_db_pool` stays a thin caller):
  * AUDIT_ONLY_MODE=false  -> the legacy Supabase-derived path (unchanged).
  * AUDIT_ONLY_MODE=true + AUDIT_DATABASE_URL set -> use ONLY that DSN.
  * AUDIT_ONLY_MODE=true + no AUDIT_DATABASE_URL -> FAIL CLOSED (no fallback to
    any default/derived identity).

The DSN is validated and never logged, never returned by an endpoint, and
never placed in an error message — only redacted, bounded diagnostics escape.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from app.config import settings


SUPPORTED_SCHEMES = ("postgres", "postgresql")

# Documented allowlist of safe query parameters an operator may include in
# AUDIT_DATABASE_URL. Anything else is rejected (never silently honored).
ALLOWED_QUERY_PARAMS = ("sslmode", "application_name", "connect_timeout")

# Connection-mode labels (safe to log / report — never the DSN).
MODE_DEFAULT_SUPABASE = "default_supabase"
MODE_AUDIT_EXPLICIT = "audit_explicit"
MODE_AUDIT_UNCONFIGURED = "audit_unconfigured"
MODE_MAINTENANCE_EXPLICIT = "maintenance_explicit"
MODE_MAINTENANCE_UNCONFIGURED = "maintenance_unconfigured"
MODE_HISTORY_WARMUP_EXPLICIT = "history_warmup_explicit"
MODE_HISTORY_WARMUP_UNCONFIGURED = "history_warmup_unconfigured"
MODE_PROSPECTIVE_EXPLICIT = "prospective_explicit"
MODE_PROSPECTIVE_UNCONFIGURED = "prospective_unconfigured"
MODE_JOB_WORKER_EXPLICIT = "job_worker_explicit"
MODE_JOB_WORKER_UNCONFIGURED = "job_worker_unconfigured"

# asyncpg pool kwargs for the AUDIT pool. statement_cache_size=0 disables
# prepared statements so the pool is safe behind a Supavisor pooler (and
# harmless in session mode); the legacy pool keeps its existing behavior.
AUDIT_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 30,
    "statement_cache_size": 0,
}
# Maintenance pool: WRITE-capable (never read-only). statement_cache_size=0 for
# Supavisor safety; a bounded command_timeout matches the role's server-side
# statement_timeout so a stuck write can never hang a batch indefinitely.
MAINTENANCE_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 60,
    "statement_cache_size": 0,
}
# History-warmup pool: WRITE-capable (the dedicated warmer role performs the
# narrowly-authorized 4H/daily writes). statement_cache_size=0 for Supavisor
# safety; bounded command_timeout matches the role's server-side statement_timeout.
HISTORY_WARMUP_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 60,
    "statement_cache_size": 0,
}
# Prospective pool: WRITE-capable (campaign/pairs/evaluations + registration).
# statement_cache_size=0 for pooler safety; a larger command_timeout since one
# execute persists 25 pairs + 50 evaluations locally (no provider waits).
PROSPECTIVE_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 120,
    "statement_cache_size": 0,
}
# Job-worker pool: WRITE-capable as the least-privilege
# smart_scanner_prospective_worker role. Used by the dedicated non-HTTP worker
# process (parent claim/lease/finalize AND the child evaluation subprocess).
# statement_cache_size=0 for pooler safety; individual SQL statements are fast
# (the multi-minute cost is CPU in the child, not any single query).
JOB_WORKER_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 120,
    "statement_cache_size": 0,
}
DEFAULT_POOL_KWARGS = {"min_size": 1, "max_size": 10, "command_timeout": 60}


class AuditDatabaseError(ValueError):
    """Invalid or missing audit database configuration. Messages are bounded
    and NEVER contain the DSN, username, password or host."""


def _audit_url() -> str:
    return (getattr(settings, "AUDIT_DATABASE_URL", "") or "").strip()


def _maintenance_url() -> str:
    return (getattr(settings, "MAINTENANCE_DATABASE_URL", "") or "").strip()


def _history_warmup_url() -> str:
    return (getattr(settings, "HISTORY_WARMUP_DATABASE_URL", "") or "").strip()


def _prospective_url() -> str:
    return (getattr(settings, "PROSPECTIVE_DATABASE_URL", "") or "").strip()


def _job_worker_url() -> str:
    return (getattr(settings, "JOB_WORKER_DATABASE_URL", "") or "").strip()


def maintenance_database_configured() -> bool:
    return bool(_maintenance_url())


def history_warmup_database_configured() -> bool:
    return bool(_history_warmup_url())


def validate_audit_database_url(url: str) -> Dict[str, Any]:
    """Validate a complete PostgreSQL DSN without exposing its secrets.

    Returns a parsed structure (never containing the password). Raises
    AuditDatabaseError with a redacted, field-only message on any problem.
    """
    raw = (url or "").strip()
    if not raw:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is empty")

    try:
        parsed = urlparse(raw)
    except Exception:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is not a parseable URL")

    if parsed.scheme not in SUPPORTED_SCHEMES:
        raise AuditDatabaseError(
            "AUDIT_DATABASE_URL scheme must be postgresql:// or postgres://"
        )
    if parsed.fragment:
        raise AuditDatabaseError("AUDIT_DATABASE_URL must not contain a fragment")
    if not parsed.username:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is missing a username")
    if not parsed.password:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is missing a password")
    if not parsed.hostname:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is missing a host")
    try:
        port = parsed.port
    except ValueError:
        raise AuditDatabaseError("AUDIT_DATABASE_URL has an invalid port")
    if port is None or not (1 <= int(port) <= 65535):
        raise AuditDatabaseError("AUDIT_DATABASE_URL is missing a valid port")
    dbname = (parsed.path or "").lstrip("/")
    if not dbname:
        raise AuditDatabaseError("AUDIT_DATABASE_URL is missing a database name")

    query = parse_qs(parsed.query or "", keep_blank_values=False)
    unknown = sorted(k for k in query if k not in ALLOWED_QUERY_PARAMS)
    if unknown:
        raise AuditDatabaseError(
            f"AUDIT_DATABASE_URL has unsupported query parameters: {unknown} "
            f"(allowed: {list(ALLOWED_QUERY_PARAMS)})"
        )
    sslmode = query.get("sslmode", [None])[0]

    return {
        "scheme": parsed.scheme,
        "user": parsed.username,     # role name is not a secret (worker-token
                                     # gated); host/password never returned here
        "port": int(port),
        "dbname": dbname,
        "sslmode": sslmode,
        "host_present": True,
        "database_present": True,
    }


def audit_dsn_diagnostic() -> Dict[str, Any]:
    """Safe, bounded diagnostic — NO host value, NO password, NO full DSN."""
    raw = _audit_url()
    if not raw:
        return {"configured": False}
    try:
        info = validate_audit_database_url(raw)
    except AuditDatabaseError as exc:
        return {"configured": True, "valid": False, "error": str(exc)}
    return {
        "configured": True,
        "valid": True,
        "host_present": True,
        "port": info["port"],
        "database_present": True,
        "user": info["user"],
        "ssl_mode": info["sslmode"],
    }


def get_connection_mode() -> str:
    """The selected connection mode (safe to log/report; never the DSN)."""
    if getattr(settings, "JOB_WORKER_ENABLED", False):
        return (MODE_JOB_WORKER_EXPLICIT if _job_worker_url()
                else MODE_JOB_WORKER_UNCONFIGURED)
    if getattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", False):
        return (MODE_PROSPECTIVE_EXPLICIT if _prospective_url()
                else MODE_PROSPECTIVE_UNCONFIGURED)
    if getattr(settings, "HISTORY_WARMUP_ONLY_MODE", False):
        return (MODE_HISTORY_WARMUP_EXPLICIT if _history_warmup_url()
                else MODE_HISTORY_WARMUP_UNCONFIGURED)
    if getattr(settings, "MAINTENANCE_ONLY_MODE", False):
        return (MODE_MAINTENANCE_EXPLICIT if _maintenance_url()
                else MODE_MAINTENANCE_UNCONFIGURED)
    if not settings.AUDIT_ONLY_MODE:
        return MODE_DEFAULT_SUPABASE
    return MODE_AUDIT_EXPLICIT if _audit_url() else MODE_AUDIT_UNCONFIGURED


def audit_database_configured() -> bool:
    return bool(_audit_url())


def select_connection_plan() -> Tuple[str, List[Tuple[str, str]], Dict[str, Any]]:
    """Return (mode, [(label, dsn), ...], pool_kwargs) for init_db_pool.

    In audit mode this NEVER returns the legacy Supabase-derived candidates —
    it uses only the explicit audit DSN, or fails closed when none is set.
    """
    from app.deps import build_connection_dsns

    if getattr(settings, "JOB_WORKER_ENABLED", False):
        raw = _job_worker_url()
        if not raw:
            raise AuditDatabaseError(
                "job worker database not configured: JOB_WORKER_ENABLED=true "
                "requires JOB_WORKER_DATABASE_URL (no fallback to the prospective, "
                "audit, maintenance, warmup or default database)")
        validate_audit_database_url(raw)
        return (MODE_JOB_WORKER_EXPLICIT, [("job_worker_explicit", raw)],
                dict(JOB_WORKER_POOL_KWARGS))

    if getattr(settings, "PROSPECTIVE_CAMPAIGN_ONLY_MODE", False):
        raw = _prospective_url()
        if not raw:
            raise AuditDatabaseError(
                "prospective database not configured: PROSPECTIVE_CAMPAIGN_ONLY_MODE"
                "=true requires PROSPECTIVE_DATABASE_URL (no fallback to the audit, "
                "maintenance, warmup or default database)")
        validate_audit_database_url(raw)
        return (MODE_PROSPECTIVE_EXPLICIT, [("prospective_explicit", raw)],
                dict(PROSPECTIVE_POOL_KWARGS))

    if getattr(settings, "HISTORY_WARMUP_ONLY_MODE", False):
        raw = _history_warmup_url()
        if not raw:
            raise AuditDatabaseError(
                "history-warmup database not configured: HISTORY_WARMUP_ONLY_MODE"
                "=true requires HISTORY_WARMUP_DATABASE_URL (no fallback to the "
                "audit, maintenance or default database)"
            )
        validate_audit_database_url(raw)  # same DSN validation; raises on invalid
        return (MODE_HISTORY_WARMUP_EXPLICIT, [("history_warmup_explicit", raw)],
                dict(HISTORY_WARMUP_POOL_KWARGS))

    if getattr(settings, "MAINTENANCE_ONLY_MODE", False):
        raw = _maintenance_url()
        if not raw:
            raise AuditDatabaseError(
                "maintenance database not configured: MAINTENANCE_ONLY_MODE=true "
                "requires MAINTENANCE_DATABASE_URL (no fallback to the audit or "
                "default database)"
            )
        validate_audit_database_url(raw)  # same DSN validation; raises on invalid
        return (MODE_MAINTENANCE_EXPLICIT, [("maintenance_explicit", raw)],
                dict(MAINTENANCE_POOL_KWARGS))

    if settings.AUDIT_ONLY_MODE:
        raw = _audit_url()
        if not raw:
            raise AuditDatabaseError(
                "audit database not configured: AUDIT_ONLY_MODE=true requires "
                "AUDIT_DATABASE_URL (no fallback to the default database)"
            )
        validate_audit_database_url(raw)  # raises AuditDatabaseError if invalid
        return MODE_AUDIT_EXPLICIT, [("audit_explicit", raw)], dict(AUDIT_POOL_KWARGS)

    candidates = build_connection_dsns(
        settings.SUPABASE_URL, settings.SUPABASE_REGION,
        settings.SUPABASE_DB_PASSWORD,
    )
    return MODE_DEFAULT_SUPABASE, candidates, dict(DEFAULT_POOL_KWARGS)


__all__ = [
    "SUPPORTED_SCHEMES",
    "ALLOWED_QUERY_PARAMS",
    "MODE_DEFAULT_SUPABASE",
    "MODE_AUDIT_EXPLICIT",
    "MODE_AUDIT_UNCONFIGURED",
    "MODE_MAINTENANCE_EXPLICIT",
    "MODE_MAINTENANCE_UNCONFIGURED",
    "MODE_HISTORY_WARMUP_EXPLICIT",
    "MODE_HISTORY_WARMUP_UNCONFIGURED",
    "MODE_PROSPECTIVE_EXPLICIT",
    "MODE_PROSPECTIVE_UNCONFIGURED",
    "MODE_JOB_WORKER_EXPLICIT",
    "MODE_JOB_WORKER_UNCONFIGURED",
    "AuditDatabaseError",
    "validate_audit_database_url",
    "audit_dsn_diagnostic",
    "get_connection_mode",
    "audit_database_configured",
    "maintenance_database_configured",
    "history_warmup_database_configured",
    "select_connection_plan",
]
