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

# asyncpg pool kwargs for the AUDIT pool. statement_cache_size=0 disables
# prepared statements so the pool is safe behind a Supavisor pooler (and
# harmless in session mode); the legacy pool keeps its existing behavior.
AUDIT_POOL_KWARGS = {
    "min_size": 1,
    "max_size": 5,
    "command_timeout": 30,
    "statement_cache_size": 0,
}
DEFAULT_POOL_KWARGS = {"min_size": 1, "max_size": 10, "command_timeout": 60}


class AuditDatabaseError(ValueError):
    """Invalid or missing audit database configuration. Messages are bounded
    and NEVER contain the DSN, username, password or host."""


def _audit_url() -> str:
    return (getattr(settings, "AUDIT_DATABASE_URL", "") or "").strip()


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
    "AuditDatabaseError",
    "validate_audit_database_url",
    "audit_dsn_diagnostic",
    "get_connection_mode",
    "audit_database_configured",
    "select_connection_plan",
]
