"""Phase 8.1B2: paired market-path outcomes for frozen B1 shadow pairs.

Exactly ONE market-path outcome per frozen strategy_shadow_pairs row — never
one outcome per arm. Both arm evaluations share the same canonical frame and
observation close, so there is only one observed forward return per pair; the
two strategy decisions remain in strategy_shadow_evaluations and are joined
to the shared outcome when reading or aggregating.

Modules:
  * constants    - versioned identities, bounds and neutral-band constants
  * fingerprints - outcome fingerprint + forward-bars hash
  * calculator   - PURE reference/forward-alignment/return math (no I/O)
  * persistence  - typed write-once/freeze persistence over migration 011
  * service      - bounded admin-triggered orchestration (selection + fetch)
  * metrics      - PURE neutral resolution metrics (no winner labels)
"""
