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
    
    # FMP API
    FMP_API_KEY: str
    FMP_BASE_URL: str = "https://financialmodelingprep.com/api/v3"
    FMP_MAX_CONCURRENT: int = 10
    FMP_RATE_LIMIT_PER_MIN: int = 250
    
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
