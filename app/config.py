"""
Configuration management for Smart Scanner Backend
"""

import os
from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Environment
    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    # Deployment build provenance (Deployment Readiness - Build Provenance).
    # Embedded at build/release time so a running backend can prove exactly
    # which source revision it is executing. These are OPTIONAL and default to
    # safe local values — startup and tests never require them, and they are
    # NEVER derived by running git inside the container at runtime.
    APP_GIT_SHA: str = "unknown"
    APP_BUILD_TIME: str = "unknown"   # ISO 8601 UTC build timestamp
    APP_ENVIRONMENT: str = "local"    # local | development | staging | production
    APP_RELEASE: str = "unknown"      # optional human/image release identifier

     # Database
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str
    SUPABASE_ANON_KEY: str
    SUPABASE_DB_PASSWORD: str
    SUPABASE_REGION: str = "eu-central-1"
    
    # Market data provider selection ("massive" | "fmp")
    MARKET_DATA_PROVIDER: str = "massive"

    # FMP API (fallback provider). Optional: startup must not require an FMP
    # key when MARKET_DATA_PROVIDER=massive (the factory validates at use time).
    FMP_API_KEY: str = ""
    FMP_BASE_URL: str = "https://financialmodelingprep.com/api/v3"
    FMP_MAX_CONCURRENT: int = 10
    FMP_RATE_LIMIT_PER_MIN: int = 250

    # Massive API (primary provider)
    MASSIVE_API_KEY: str = ""
    MASSIVE_BASE_URL: str = "https://api.massive.com"
    MASSIVE_REQUESTS_PER_MINUTE: int = 5   # Massive Basic plan
    MASSIVE_PROFILE_CACHE_DAYS: int = 7    # ticker-details (market cap) cache

    # Phase 7A: a 'running' market-data job whose last update is older than
    # this is considered stale (crashed process) and auto-failed so it never
    # blocks new work.
    MARKET_DATA_JOB_STALE_MINUTES: int = 30

    # Universe eligibility (Massive reference data). Classification uses the
    # provider's type/exchange fields, never ticker suffixes.
    UNIVERSE_ALLOWED_EXCHANGES: List[str] = ["XNAS", "XNYS", "XASE"]  # MIC codes
    UNIVERSE_ALLOWED_SECURITY_TYPES: List[str] = ["CS"]  # common stock
    UNIVERSE_INCLUDE_OTC: bool = False

    # Cheap local pre-screen (before any per-ticker detail calls). Dollar volume
    # is computed locally as close * volume (documented in screening.py).
    PRESCREEN_MIN_PRICE: float = 1.0
    PRESCREEN_MIN_VOLUME: float = 100_000
    PRESCREEN_MIN_DOLLAR_VOLUME: float = 1_000_000
    
    # Worker settings
    WORKER_TOKEN: str
    REQUIRE_WORKER_TOKEN: bool = False
    ENABLE_SCHEDULER: bool = True

    # Audit-only mode (Deployment Readiness). When true, the app exposes ONLY a
    # narrow read-only allowlist (revision/liveness + the shadow-cohort audit
    # routes); every other API and docs route is rejected before its handler
    # runs, and no scheduler/background work may start. Default false keeps
    # local, test and normal deployments completely unchanged.
    AUDIT_ONLY_MODE: bool = False

    # Explicit read-only audit database identity. A COMPLETE PostgreSQL DSN
    # (postgresql://<role>:<password>@<host>:<port>/<db>?sslmode=require) used
    # ONLY when AUDIT_ONLY_MODE=true. It supplies the custom least-privilege
    # role directly — the legacy Supabase-derived username cannot. SECRET:
    # never logged, never returned by any endpoint, never in .env.example with a
    # real value. Empty by default; supplied via `fly secrets set`.
    AUDIT_DATABASE_URL: str = ""
    # The PostgreSQL role the audit connection MUST authenticate as. Required
    # once AUDIT_DATABASE_URL is set in audit mode; the access-check refuses
    # readiness if current_user differs or is broader.
    AUDIT_EXPECTED_DB_ROLE: str = ""
    SCAN_BATCH_SIZE: int = 150
    SCAN_TIMES: List[str] = ["10:00", "14:00", "18:00"]  # UTC times
    
    # CORS
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001", 
        "https://*.vercel.app"
    ]
    
    # Debug flags
    DEBUG_SAVE_AVOID: bool = False
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
