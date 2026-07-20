"""Market data provider abstraction + factory.

The scanner/admin layers call `get_market_data_provider()` and depend only on
the `MarketDataProvider` interface — never on Massive or FMP directly.
Massive is the default; FMP remains available via MARKET_DATA_PROVIDER=fmp.
"""

from typing import Optional

from app.config import settings
from app.providers.base import MarketDataProvider


class ProviderConfigError(ValueError):
    """Raised when the configured provider cannot be constructed safely."""


def get_market_data_provider(name: Optional[str] = None) -> MarketDataProvider:
    """Build the configured provider. Fails fast with a clear, secret-free error."""
    provider = (name or settings.MARKET_DATA_PROVIDER or "massive").lower()

    if provider == "massive":
        if not (settings.MASSIVE_API_KEY or "").strip():
            raise ProviderConfigError(
                "MARKET_DATA_PROVIDER=massive but MASSIVE_API_KEY is not set. "
                "Set it in .env or switch MARKET_DATA_PROVIDER=fmp."
            )
        from app.providers.massive import MassiveProvider

        return MassiveProvider()

    if provider == "fmp":
        if not (settings.FMP_API_KEY or "").strip():
            raise ProviderConfigError("MARKET_DATA_PROVIDER=fmp but FMP_API_KEY is not set.")
        from app.providers.fmp_provider import FMPProvider

        return FMPProvider()

    raise ProviderConfigError(
        f"Unknown MARKET_DATA_PROVIDER '{provider}'. Allowed: massive, fmp."
    )


__all__ = ["MarketDataProvider", "ProviderConfigError", "get_market_data_provider"]
