"""Deployment build provenance (Deployment Readiness - Build Provenance).

Provider-neutral, read-only helpers that let a running backend prove exactly
which source revision it is executing. The revision is EMBEDDED at build/
release time via settings (APP_GIT_SHA / APP_BUILD_TIME / APP_ENVIRONMENT /
APP_RELEASE) — it is never derived by running `git` inside the container at
runtime.

Three separate concepts are kept distinct and never conflated:
  * application_version — the API/product version (main.APP version string);
  * git_sha            — the exact source revision the image was built from;
  * release            — an optional human/image release identifier.

Nothing here touches the database, a market-data provider, Supabase, secrets
or the scheduler.
"""

from __future__ import annotations

import re
from typing import Any, Dict

from app.config import settings


SERVICE_NAME = "smart-scanner-be"

# Keep in sync with the FastAPI `version=` in main.py. This is the APPLICATION
# version and is intentionally SEPARATE from the source git_sha.
APPLICATION_VERSION = "1.1.0"

UNKNOWN = "unknown"

# A git SHA is a lowercase hex string: 7..40 for sha1, up to 64 for sha256.
# Anything else (a branch name, "latest", an env dump, empty) is rejected so a
# misleading value is never presented as a trusted revision.
_SHA_RE = re.compile(r"^[0-9a-f]{7,64}$")


def normalize_git_sha(raw: Any) -> str:
    """Return a validated full git SHA, or 'unknown' when it cannot be trusted.

    Trims and lower-cases; accepts only 7–64 hex chars. Never raises.
    """
    if raw is None:
        return UNKNOWN
    candidate = str(raw).strip().lower()
    if candidate and candidate != UNKNOWN and _SHA_RE.match(candidate):
        return candidate
    return UNKNOWN


def short_git_sha(full_sha: str) -> str:
    """Derive the 7-char short SHA from a validated full SHA, else 'unknown'."""
    normalized = normalize_git_sha(full_sha)
    return normalized[:7] if normalized != UNKNOWN else UNKNOWN


def _clean_str(raw: Any) -> str:
    value = str(raw).strip() if raw is not None else ""
    return value or UNKNOWN


def build_provenance() -> Dict[str, Any]:
    """Assemble the safe, read-only build-provenance payload from settings.

    Contains ONLY non-sensitive deployment metadata: no tokens, no database
    URLs, no provider keys, no env dumps, no paths.
    """
    git_sha = normalize_git_sha(getattr(settings, "APP_GIT_SHA", UNKNOWN))
    return {
        "service": SERVICE_NAME,
        "application_version": APPLICATION_VERSION,
        "git_sha": git_sha,
        "git_sha_short": short_git_sha(git_sha),
        "build_time": _clean_str(getattr(settings, "APP_BUILD_TIME", UNKNOWN)),
        "environment": _clean_str(
            getattr(settings, "APP_ENVIRONMENT", "local")
        ),
        "release": _clean_str(getattr(settings, "APP_RELEASE", UNKNOWN)),
    }


def startup_log_fields() -> Dict[str, Any]:
    """Concise, secret-free provenance fields for one startup log line."""
    prov = build_provenance()
    return {
        "service": prov["service"],
        "application_version": prov["application_version"],
        "git_sha": prov["git_sha"],
        "environment": prov["environment"],
        "release": prov["release"],
    }


__all__ = [
    "SERVICE_NAME",
    "APPLICATION_VERSION",
    "UNKNOWN",
    "normalize_git_sha",
    "short_git_sha",
    "build_provenance",
    "startup_log_fields",
]
