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
from app.deps import get_db
from app.routers import public, admin, outcomes
from app.utils.logging import setup_logging
from app.workers.scheduler import start_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting Smart Scanner Backend")
    
    # Start scheduler if enabled
    if settings.ENABLE_SCHEDULER:
        start_scheduler()
        logger.info("Scheduler started")
    
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
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])


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

    try:
        await db.execute("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "version": "1.1.0",
            "market_data": provider_block,
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True if os.getenv("ENVIRONMENT") == "development" else False
    )