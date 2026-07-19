"""Phase 3 (Evidence Engine): hierarchical funnel scanner.

Replaces random batch scanning with a staged funnel that:
  * Stage 0 - builds a candidate universe from the ticker cache (real values).
  * Stage 1 - applies cheap liquidity filters (market cap, volume) BEFORE any
    expensive history fetch.
  * Stage 2 - cheap daily prefilters on survivors (history length, price, shape).
  * Stage 3 - evaluates enabled strategies on survivors only.
  * Stage 4 - a documented hook for expensive data (e.g. 4H), DISABLED here.

The stage functions in `funnel.py` are pure and deterministic so they can be
unit-tested without any DB or FMP access. The orchestrator does the I/O.
"""
