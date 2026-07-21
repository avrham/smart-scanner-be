"""Deterministic fingerprints for pair outcomes and forward frames (8.1B2).

Two independent identities:

  * outcome fingerprint  - identifies the canonical outcome CONTRACT for one
    pair. STABLE while horizons mature from NULL to calculated: it excludes
    the current forward bar count, forward hash, run ids, statuses,
    timestamps and error text.
  * forward bars hash    - identifies the exact ordered completed forward
    bars used in ONE calculation. It CHANGES whenever a forward date, any
    OHLCV value, the bar order or the canonical contract version changes.

The forward hash is deliberately NOT part of the B1 pair fingerprint — B1
identity remains unchanged.
"""

from typing import Any, Dict, List

from app.workers.provenance import canonical_json, _sha256
from app.workers.shadow.outcomes.constants import (
    CALCULATION_VERSION,
    FORWARD_FRAME_VERSION,
    OUTCOME_COVERAGE_VERSION,
    OUTCOME_FINGERPRINT_VERSION,
)


def compute_outcome_fingerprint(
    *,
    pair_fingerprint: str,
    pair_fingerprint_version: str,
    outcome_fingerprint_version: str = OUTCOME_FINGERPRINT_VERSION,
    calculation_version: str = CALCULATION_VERSION,
    outcome_coverage_version: str = OUTCOME_COVERAGE_VERSION,
    forward_frame_version: str = FORWARD_FRAME_VERSION,
) -> str:
    """SHA-256 identity of one pair's canonical outcome contract."""
    payload = {
        "outcome_fingerprint_version": outcome_fingerprint_version,
        "pair_fingerprint": pair_fingerprint,
        "pair_fingerprint_version": pair_fingerprint_version,
        "calculation_version": calculation_version,
        "outcome_coverage_version": outcome_coverage_version,
        "forward_frame_version": forward_frame_version,
    }
    return _sha256(canonical_json(payload))


def compute_forward_bars_hash(
    *,
    symbol: str,
    provider: Any,
    snapshot_date: Any,
    forward_bars: List[Dict[str, Any]],
    forward_frame_version: str = FORWARD_FRAME_VERSION,
) -> str:
    """SHA-256 over the deterministic canonical forward-bars frame.

    `forward_bars` is the exact ordered list of completed canonical forward
    bars used in the calculation (oldest first, strictly after
    snapshot_date). Bar list order is semantic and enters the hash as-is.
    """
    payload = {
        "forward_frame_version": forward_frame_version,
        "symbol": symbol,
        "provider": provider,
        "snapshot_date": str(snapshot_date) if snapshot_date is not None else None,
        "bars": forward_bars,
    }
    return _sha256(canonical_json(payload))
