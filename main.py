"""
Smart Scanner Backend - Main FastAPI Application
"""

import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.audit_mode import is_audit_route_allowed
from app.maintenance_mode import is_maintenance_route_allowed
from app.build_info import build_provenance, startup_log_fields
from app.deps import get_db
from app.routers import public, admin, outcomes, shadow
from app.utils.logging import setup_logging
from app.workers.scheduler import start_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    # Concise, secret-free build-provenance line so logs prove the running
    # revision (never a token, DB URL, provider key or full env dump).
    logger.info("Starting Smart Scanner Backend", extra={
        "extra_data": startup_log_fields()
    })

    # Audit-only mode is incompatible with background processing: a misconfig
    # that enables both must fail fast rather than quietly run the scheduler.
    # The message carries no secrets.
    if settings.AUDIT_ONLY_MODE and settings.ENABLE_SCHEDULER:
        raise RuntimeError(
            "invalid configuration: AUDIT_ONLY_MODE=true requires "
            "ENABLE_SCHEDULER=false (audit-only mode never runs background work)"
        )

    # Maintenance-only mode guards (Shadow Outcome Maintenance Environment):
    #   * mutually exclusive with audit-only mode;
    #   * never runs the scheduler / background work.
    if settings.MAINTENANCE_ONLY_MODE and settings.AUDIT_ONLY_MODE:
        raise RuntimeError(
            "invalid configuration: MAINTENANCE_ONLY_MODE and AUDIT_ONLY_MODE "
            "are mutually exclusive"
        )
    if settings.MAINTENANCE_ONLY_MODE and settings.ENABLE_SCHEDULER:
        raise RuntimeError(
            "invalid configuration: MAINTENANCE_ONLY_MODE=true requires "
            "ENABLE_SCHEDULER=false (maintenance mode never runs background work)"
        )

    # Start scheduler if enabled — never in audit-only or maintenance-only mode.
    if (settings.ENABLE_SCHEDULER and not settings.AUDIT_ONLY_MODE
            and not settings.MAINTENANCE_ONLY_MODE):
        start_scheduler()
        logger.info("Scheduler started")
    elif settings.MAINTENANCE_ONLY_MODE:
        logger.info("Maintenance-only mode: scheduler and background work disabled")
        # The maintainer role intentionally has NO daily_bars write grant (least
        # privilege). The outcome service's best-effort cache write therefore
        # always fails with InsufficientPrivilegeError and logs a WARNING that
        # can be mis-read as an outcome failure. Outcome correctness uses the
        # provider response directly; cache persistence is non-essential in this
        # mode. Downgrade ONLY that specific, expected line to DEBUG — no key,
        # no outcome behaviour, no cache behaviour and no normal deployment are
        # affected (this filter is installed only in maintenance-only mode).
        class _MaintenanceCacheWriteNoise(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                try:
                    msg = record.getMessage()
                except Exception:
                    return True
                if (record.levelno == logging.WARNING
                        and msg.startswith("daily_bars cache write failed")):
                    record.levelno = logging.DEBUG
                    record.levelname = "DEBUG"
                    return logger.isEnabledFor(logging.DEBUG)
                return True
        logging.getLogger("app.workers.shadow.outcomes.service").addFilter(
            _MaintenanceCacheWriteNoise())
    elif settings.AUDIT_ONLY_MODE:
        logger.info("Audit-only mode: scheduler and background work disabled")

    yield
    
    logger.info("Shutting down Smart Scanner Backend")


# Create FastAPI app
app = FastAPI(
    title="Smart Scanner API",
    description="Stock pattern scanning and signal generation",
    version="1.1.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(public.router, prefix="/api", tags=["public"])
app.include_router(outcomes.router, prefix="/api", tags=["outcomes"])
app.include_router(shadow.router, prefix="/api", tags=["shadow"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])


@app.middleware("http")
async def audit_only_gate(request, call_next):
    """Reject any non-allowlisted route BEFORE its handler when audit-only OR
    maintenance-only mode is on. Blocked routes get a stable 404 (never a
    healthy handler, never a hint at the allowlist). Allowlisted routes still
    run their own worker-token dependency — this gate never bypasses
    authentication. When both modes are false (the default), every request
    passes through unchanged. Maintenance mode allows exactly one mutation route
    (POST execute); every other write path — including the generic calculate
    endpoint — is rejected here before its handler."""
    if settings.MAINTENANCE_ONLY_MODE and not is_maintenance_route_allowed(
        request.method, request.url.path
    ):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})
    if settings.AUDIT_ONLY_MODE and not is_audit_route_allowed(
        request.method, request.url.path
    ):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})
    return await call_next(request)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Smart Scanner API v1.1", "status": "healthy"}


def _provider_health() -> dict:
    """Safe provider status block. NEVER includes API keys."""
    provider = (settings.MARKET_DATA_PROVIDER or "massive").lower()
    if provider == "massive":
        credentials = bool((settings.MASSIVE_API_KEY or "").strip())
        rate_limit = f"{settings.MASSIVE_REQUESTS_PER_MINUTE}/min (basic)"
    else:
        credentials = bool((settings.FMP_API_KEY or "").strip())
        rate_limit = f"{settings.FMP_RATE_LIMIT_PER_MIN}/min"
    return {
        "provider": provider,
        "credentials_configured": credentials,
        "rate_limit": rate_limit,
    }


async def _health_payload(db) -> JSONResponse | dict:
    """Shared health logic: DB connectivity + safe provider status."""
    provider_block = _provider_health()
    try:
        from app.workers.market_store import get_provider_sync_status

        sync_status = await get_provider_sync_status()
        provider_block.update(sync_status)
    except Exception:
        pass  # never fail health because of the sync-status lookup

    # Source revision (short SHA or "unknown"). Additive and non-breaking: the
    # application `version` is unchanged and remains a separate concept from
    # the git revision. Full provenance lives on GET /version.
    revision = build_provenance()["git_sha_short"]
    try:
        await db.execute("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "version": "1.1.0",
            "revision": revision,
            "market_data": provider_block,
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
                "revision": revision,
                "market_data": provider_block,
            }
        )


@app.get("/health")
async def health_check(db=Depends(get_db)):
    """Detailed health check with database connectivity (infra/liveness path)."""
    return await _health_payload(db)


@app.get("/api/health")
async def api_health_check(db=Depends(get_db)):
    """Alias of /health under the /api prefix.

    Fixes B8: the UI api client prepends `/api`, so it calls `/api/health`.
    Exposing this alias keeps the frontend unchanged while making the Settings
    health check report correctly.
    """
    return await _health_payload(db)


@app.get("/version")
async def version():
    """Read-only deployment provenance: which source revision is running.

    Returns ONLY safe build metadata (service, application version, git SHA,
    build time, environment, release). It never touches the database, never
    constructs a market-data provider, never calls Massive/FMP/Supabase, and
    never exposes tokens, credentials, URLs, paths or environment dumps. When
    the build SHA was not embedded, git_sha and git_sha_short report
    "unknown" rather than a misleading value.
    """
    return build_provenance()


@app.get("/api/version")
async def api_version():
    """Alias of /version under the /api prefix (the UI api client prepends
    `/api`). Identical read-only provenance payload."""
    return build_provenance()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True if os.getenv("ENVIRONMENT") == "development" else False
    )