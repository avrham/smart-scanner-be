"""Phase 2 outcome endpoints are registered and wired."""

from main import app


def _paths():
    return {getattr(route, "path", None) for route in app.routes}


def test_outcomes_read_routes_registered():
    paths = _paths()
    assert "/api/outcomes" in paths
    assert "/api/outcomes/metrics" in paths


def test_admin_calculate_route_registered():
    assert "/api/admin/outcomes/calculate" in _paths()
