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
from app.routers import public, admin
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
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Smart Scanner API v1.1", "status": "healthy"}


@app.get("/health")
async def health_check(db=Depends(get_db)):
    """Detailed health check with database connectivity"""
    try:
        # Test database connection
        await db.execute("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "version": "1.1.0"
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy", 
                "database": "disconnected",
                "error": str(e)
            }
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True if os.getenv("ENVIRONMENT") == "development" else False
    )