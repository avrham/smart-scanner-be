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
