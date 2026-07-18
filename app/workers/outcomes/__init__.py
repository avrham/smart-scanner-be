"""Phase 2 (Evidence Engine): signal outcome tracking + baseline comparison.

This package is split so the numeric core is pure and unit-testable without any
DB or network:

  * calculator.py - forward returns, MFE/MAE, stop/target hits, simulated R
  * baselines.py  - naive buy & hold baselines and signal-vs-baseline deltas
  * metrics.py    - aggregation of many outcomes into honest summary stats
  * persistence.py- DB CRUD for the signal_outcomes table
  * service.py    - orchestration (loads signals, fetches OHLCV, persists)
"""
