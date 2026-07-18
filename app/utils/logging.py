"""
Logging configuration for Smart Scanner
"""

import logging
import sys
import json
from datetime import datetime
from typing import Dict, Any

from app.config import settings


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, "extra_data"):
            log_entry["extra"] = record.extra_data
        
        return json.dumps(log_entry)


class TextFormatter(logging.Formatter):
    """Human-readable text formatter"""
    
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


def setup_logging():
    """Configure application logging"""
    
    # Get root logger
    root_logger = logging.getLogger()
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set log level
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    root_logger.setLevel(log_level)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Set formatter based on configuration
    if settings.LOG_FORMAT.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = TextFormatter()
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers
    
    # Reduce noise from external libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    
    # Set our application logger levels
    logging.getLogger("app").setLevel(log_level)
    
    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured: level={settings.LOG_LEVEL}, "
        f"format={settings.LOG_FORMAT}, environment={settings.ENVIRONMENT}"
    )


def get_logger_with_extra(name: str, extra_data: Dict[str, Any] = None):
    """Get logger with extra context data"""
    
    class ExtraLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            if self.extra:
                # Add extra data to the record
                if "extra" not in kwargs:
                    kwargs["extra"] = {}
                kwargs["extra"]["extra_data"] = self.extra
            return msg, kwargs
    
    logger = logging.getLogger(name)
    return ExtraLoggerAdapter(logger, extra_data or {})
