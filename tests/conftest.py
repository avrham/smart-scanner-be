"""Shared test setup.

Sets dummy environment variables BEFORE any `app.*` import so that
pydantic Settings() (required fields) can instantiate without real
credentials. No test in this suite performs real DB or network I/O.
"""

import os

_DUMMY_ENV = {
    "SUPABASE_URL": "https://test-project.supabase.co",
    "SUPABASE_SERVICE_KEY": "test-service-key",
    "SUPABASE_ANON_KEY": "test-anon-key",
    "SUPABASE_DB_PASSWORD": "test-password",
    "FMP_API_KEY": "test-fmp-key",
    "WORKER_TOKEN": "test-worker-token",
}

for _k, _v in _DUMMY_ENV.items():
    os.environ.setdefault(_k, _v)
