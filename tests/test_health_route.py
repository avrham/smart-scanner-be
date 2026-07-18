"""Health endpoint alignment (B8): UI calls /api/health."""

from main import app


def _paths():
    return {getattr(route, "path", None) for route in app.routes}


def test_api_health_alias_registered():
    # The UI api client prefixes /api, so /api/health must exist.
    assert "/api/health" in _paths()


def test_legacy_health_still_registered():
    assert "/health" in _paths()
