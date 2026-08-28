"""External-signal ingress route gate (External Intelligence Hub V1).

When EXTERNAL_INGEST_ONLY_MODE is enabled, the running API must expose ONLY
liveness/version plus the external-signal ingress surface. Every other route —
the whole admin surface, the scanner Product API, campaign/outcome/provider
paths and the OpenAPI/docs routes — is rejected BEFORE its handler executes,
so no scanner or provider path can be reached from the internet-facing app
even with a valid worker token.

WHY THIS GATE IS SHAPED DIFFERENTLY FROM THE OTHERS
---------------------------------------------------
Audit, maintenance, warmup and prospective mode all share one method rule for
the whole allowlist (`AUDIT_ONLY_METHODS` is read-only; maintenance permits a
single POST). This gate is PER-PATH instead, and the difference matters: the
ingress app has exactly one write route, and it is the only one in this
repository that an anonymous caller can reach. Expressing "POST is allowed"
as a mode-wide fact would also permit a POST to `/version` — harmless today,
but it would mean the gate no longer states which route is the write.

So the allowlist maps path -> permitted methods, and the write is visible as
a single line that a reviewer can check against the router.

This is a single middleware in front of the existing app — never a second
FastAPI app and never a parallel router.
"""

from __future__ import annotations

from typing import Dict, FrozenSet

#: The ingress endpoint. ONE static path, and the only POST in this mode.
#:
#: Static because TradingView sends to a fixed URL it cannot vary, and because
#: an exact string is what makes an allowlist checkable by eye. The variable
#: input (the credential) travels as a query parameter, which leaves the PATH
#: constant — see EXTERNAL_INGEST_TOKEN in app/config.py for why a URL-borne
#: credential is the only option the platform offers.
EXTERNAL_SIGNAL_INGRESS_PATH = "/api/external/signals"

#: Read-only companions. `/sources` lets an operator confirm the registry the
#: gateway is validating against without opening a database session, and
#: `/health` proves the ingress is up without proving anything about the data.
EXTERNAL_SOURCES_PATH = "/api/external/sources"
EXTERNAL_INGRESS_HEALTH_PATH = "/api/external/health"

_READ_ONLY: FrozenSet[str] = frozenset({"GET", "HEAD", "OPTIONS"})
#: OPTIONS is included so a browser preflight against the ingress does not 404
#: in a way that looks like the endpoint is missing. HEAD is not: a HEAD on the
#: ingress would have to run the handler to be truthful, and a write handler
#: must never run for a method that promises no body.
_INGRESS_METHODS: FrozenSet[str] = frozenset({"POST", "OPTIONS"})

#: Exact path -> the methods that path permits. Nothing else is reachable.
EXTERNAL_INGEST_ALLOWLIST: Dict[str, FrozenSet[str]] = {
    # Liveness / revision proof (public, read-only) — identical to every other
    # bounded mode so infra probes and deploy verification are unchanged.
    "/": _READ_ONLY,
    "/version": _READ_ONLY,
    "/api/version": _READ_ONLY,
    "/health": _READ_ONLY,
    "/api/health": _READ_ONLY,
    # The read-only ingress companions.
    EXTERNAL_INGRESS_HEALTH_PATH: _READ_ONLY,
    EXTERNAL_SOURCES_PATH: _READ_ONLY,
    # THE ONE WRITE. Token-authenticated inside its own handler — this gate
    # decides reachability, never authentication, and never bypasses it.
    EXTERNAL_SIGNAL_INGRESS_PATH: _INGRESS_METHODS,
}


def is_external_ingest_route_allowed(method: str, path: str) -> bool:
    """Whether one request may proceed under external-ingest-only mode.

    Exact path match AND a method that path permits. Deterministic and
    side-effect free; the caller (middleware) blocks everything else.
    """
    permitted = EXTERNAL_INGEST_ALLOWLIST.get(path or "")
    if permitted is None:
        return False
    return (method or "").upper() in permitted


__all__ = [
    "EXTERNAL_SIGNAL_INGRESS_PATH",
    "EXTERNAL_SOURCES_PATH",
    "EXTERNAL_INGRESS_HEALTH_PATH",
    "EXTERNAL_INGEST_ALLOWLIST",
    "is_external_ingest_route_allowed",
]
