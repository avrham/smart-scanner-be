"""
Logging configuration for Smart Scanner
"""

import logging
import re
import sys
import json
from datetime import datetime
from typing import Dict, Any

from app.config import settings


#: Query parameters whose VALUE is a credential and must never be logged.
#:
#: `token` is not a hypothetical: it is the ONLY authentication mechanism the
#: external-signal ingress can offer TradingView, because a TradingView webhook
#: posts a fixed body to a fixed URL and cannot set a custom HTTP header (see
#: app/routers/external.py). Uvicorn's access logger writes the full request
#: line — path AND query string — so without this filter every genuine alert
#: delivery would print the live ingress credential into the application log,
#: where it is retained, shipped and readable by anyone with log access.
#:
#: Matching is on the PARAMETER NAME rather than on the value, because a value
#: that looks random is usually a legitimate identifier and blanking it would
#: destroy the diagnostic the line exists for.
_CREDENTIAL_QUERY_PARAMS = ("token", "secret", "api_key", "apikey",
                            "password", "signature", "auth", "access_token")

_CREDENTIAL_QUERY_RE = re.compile(
    r"(?i)([?&](?:" + "|".join(_CREDENTIAL_QUERY_PARAMS) + r")=)[^&\s\"']*")

REDACTED_QUERY_VALUE = "[redacted]"


def redact_query_credentials(text: str) -> str:
    """Replace credential query-parameter VALUES in a URL or request line.

    Pure and total: anything that is not a string comes back unchanged, and a
    string with no credential parameter is returned as-is, so this is safe to
    run on every log record.
    """
    if not isinstance(text, str) or "=" not in text:
        return text
    return _CREDENTIAL_QUERY_RE.sub(r"\1" + REDACTED_QUERY_VALUE, text)


class CredentialQueryRedactor(logging.Filter):
    """Strip credential query values from a log record before it is emitted.

    Installed on the ROOT logger and on `uvicorn.access` separately, because
    uvicorn configures its own handler with `propagate=False` before this
    application's logging setup runs — a root-only filter would therefore never
    see the access line, which is the one line that actually carries the
    credential.

    Rewrites `record.args` in place where uvicorn puts the request path
    (element 2 of its 5-tuple) and falls back to the formatted message for any
    other shape, so a future uvicorn log format cannot silently reopen the leak.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and args:
            redacted = tuple(redact_query_credentials(a) for a in args)
            if redacted != args:
                record.args = redacted
        if isinstance(record.msg, str):
            cleaned = redact_query_credentials(record.msg)
            if cleaned != record.msg:
                record.msg = cleaned
        return True


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

    # The credential redactor sits on the HANDLER rather than on the logger so
    # it applies to everything this handler emits, including records that
    # propagated from a library logger we never configured.
    redactor = CredentialQueryRedactor()
    console_handler.addFilter(redactor)
    root_logger.addHandler(console_handler)

    # Uvicorn's access logger keeps its OWN handler and does not propagate, so
    # the filter above never sees it. This is the line that carries `?token=`,
    # so it is attached explicitly and to the handlers uvicorn already
    # installed — attaching to the logger alone would miss nothing today but
    # would break the moment a handler is added with `propagate=False`.
    for _name in ("uvicorn.access", "uvicorn.error", "uvicorn"):
        _logger = logging.getLogger(_name)
        _logger.addFilter(CredentialQueryRedactor())
        for _handler in _logger.handlers:
            _handler.addFilter(CredentialQueryRedactor())
    
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
