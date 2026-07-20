-- Phase 7B: scan-to-signal provenance, immutable signal identity,
-- reproducibility and outcome linkage.
--
-- Additive and idempotent (safe to run more than once). Run manually in the
-- Supabase SQL editor after 006_market_data_jobs.sql.
--
-- Five parts:
--   1. pattern_runs becomes the CANONICAL scan-run table. The UUID returned by
--      POST /api/admin/scan/start (and used by the scan WebSocket) is now the
--      pattern_runs row id. No second scan identity is created.
--   2. signals gains an IMMUTABLE identity: signal_fingerprint (SHA-256 of the
--      canonical decision inputs). The legacy UNIQUE(symbol, pattern_code,
--      snapshot_date) blocked multiple immutable variants (v2 vs v3, different
--      config hashes, different data as-of, with/without external evidence) —
--      it is replaced by:
--        * a partial unique index on signal_fingerprint (new rows), and
--        * a partial legacy dedup index on (symbol, pattern_code,
--          snapshot_date) WHERE signal_fingerprint IS NULL, preserving the old
--          dedup semantics for pre-7B rows only. Legacy rows keep fingerprint
--          NULL — fingerprints are never fabricated for them.
--   3. signal_provenance: one-to-one provenance row per signal, written
--      transactionally with the signal and NEVER overwritten by later scans
--      (it records the ORIGIN scan). Legacy signals have none and are reported
--      as provenance_status='legacy_unlinked'.
--   4. scan_run_signals: occurrence links — EVERY scan that detected a signal
--      (including re-detections of an existing immutable signal) records a
--      link. Origin (provenance.scan_run_id) and occurrences are distinct
--      concepts by design.
--   5. signal_outcomes gains frozen version columns so an outcome always knows
--      WHICH strategy/policy/config version it evaluated. Existing outcome
--      rows keep NULLs (valid).

-- ---------------------------------------------------------------------------
-- 1. Canonical scan runs (extends the existing pattern_runs table)
-- ---------------------------------------------------------------------------
-- Legacy rows: status defaults to 'completed' (they were only written at scan
-- end), other new columns stay NULL. No backfill is performed.

ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS scanner_mode TEXT;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'completed';
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS provider TEXT;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS dry_run BOOLEAN;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS requested_limit INT;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS scan_date DATE;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS telemetry JSONB;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS error_code TEXT;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE public.pattern_runs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS pattern_runs_status_idx
ON public.pattern_runs (status, run_started_at DESC);

-- ---------------------------------------------------------------------------
-- 2. Immutable signal identity
-- ---------------------------------------------------------------------------
-- signal_fingerprint = SHA-256 over the canonical decision inputs:
--   fingerprint_version, symbol, strategy_code, strategy_version,
--   decision_policy_version, config_hash, snapshot_date,
--   market_data_as_of (UTC), verdict, ORIGINAL pre-pruning evidence sha256,
--   sorted external_observation_ids.
-- Deliberately EXCLUDES scan_run_id: re-detections of the same immutable
-- decision by later scans reuse the same signal row (linked via
-- scan_run_signals) instead of duplicating or overwriting it.
--
-- signal_fingerprint_version names the fingerprint ALGORITHM
-- ('signal_fingerprint.v1'): every new fingerprinted row carries it, and a
-- future v2 algorithm can never be confused with v1 identities. Legacy rows
-- keep BOTH columns NULL (never fabricated); the CHECK constraint forbids
-- partial identity states (fingerprint without version or vice versa).

ALTER TABLE public.signals ADD COLUMN IF NOT EXISTS signal_fingerprint TEXT;
ALTER TABLE public.signals ADD COLUMN IF NOT EXISTS signal_fingerprint_version TEXT;

ALTER TABLE public.signals
DROP CONSTRAINT IF EXISTS signals_fingerprint_version_pairing_check;
ALTER TABLE public.signals
ADD CONSTRAINT signals_fingerprint_version_pairing_check
CHECK ((signal_fingerprint IS NULL) = (signal_fingerprint_version IS NULL));

-- New rows: one row per immutable (fingerprint, algorithm version) identity.
CREATE UNIQUE INDEX IF NOT EXISTS signals_fingerprint_uniq
ON public.signals (signal_fingerprint, signal_fingerprint_version)
WHERE signal_fingerprint IS NOT NULL;

-- The pre-7B constraint would block multiple immutable variants on the same
-- symbol/pattern/date (e.g. sma150.v2 AND sma150.v3), so it must go. Legacy
-- dedup semantics are preserved for legacy rows only via a partial index.
ALTER TABLE public.signals
DROP CONSTRAINT IF EXISTS signals_symbol_pattern_code_snapshot_date_key;

CREATE UNIQUE INDEX IF NOT EXISTS signals_legacy_dedup_uniq
ON public.signals (symbol, pattern_code, snapshot_date)
WHERE signal_fingerprint IS NULL;

CREATE INDEX IF NOT EXISTS signals_symbol_pattern_date_idx
ON public.signals (symbol, pattern_code, snapshot_date);

-- ---------------------------------------------------------------------------
-- 3. Signal provenance (one-to-one with signals; origin scan, never rewritten)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.signal_provenance (
  signal_id UUID PRIMARY KEY REFERENCES public.signals(id) ON DELETE CASCADE,
  scan_run_id UUID REFERENCES public.pattern_runs(id) ON DELETE SET NULL,
  source_path TEXT NOT NULL,          -- 'funnel' | 'legacy' | 'scheduled' | 'manual'
  scanner_mode TEXT,
  provider TEXT,
  strategy_code TEXT NOT NULL,
  strategy_version TEXT NOT NULL,
  decision_policy_version TEXT NOT NULL,
  provenance_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  config_snapshot JSONB NOT NULL,
  market_data_as_of TIMESTAMPTZ,      -- latest bar actually evaluated (UTC); NULL when unknown
  evidence_snapshot JSONB NOT NULL,
  evidence_original_sha256 TEXT,      -- hash of the UNPRUNED snapshot (reproducibility)
  evidence_original_size_bytes INT,
  evidence_pruned BOOLEAN NOT NULL DEFAULT FALSE,
  evidence_pruned_keys JSONB NOT NULL DEFAULT '[]',
  external_observation_ids JSONB NOT NULL DEFAULT '[]',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS signal_provenance_scan_run_idx
ON public.signal_provenance (scan_run_id);

CREATE INDEX IF NOT EXISTS signal_provenance_strategy_idx
ON public.signal_provenance (strategy_code, strategy_version);

CREATE INDEX IF NOT EXISTS signal_provenance_policy_idx
ON public.signal_provenance (decision_policy_version);

CREATE INDEX IF NOT EXISTS signal_provenance_config_hash_idx
ON public.signal_provenance (config_hash);

-- ---------------------------------------------------------------------------
-- 4. Scan-run occurrence links (every detection, not just the origin)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.scan_run_signals (
  scan_run_id UUID NOT NULL REFERENCES public.pattern_runs(id) ON DELETE CASCADE,
  signal_id UUID NOT NULL REFERENCES public.signals(id) ON DELETE CASCADE,
  source_path TEXT NOT NULL,
  created_new_signal BOOLEAN NOT NULL,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (scan_run_id, signal_id)
);

CREATE INDEX IF NOT EXISTS scan_run_signals_signal_idx
ON public.scan_run_signals (signal_id);

-- ---------------------------------------------------------------------------
-- 5. Outcome provenance columns (frozen version identity per outcome)
-- ---------------------------------------------------------------------------
-- Existing rows keep NULLs; new outcomes copy these from signal_provenance at
-- creation time so version grouping never depends on a mutable join.

ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS scan_run_id UUID;
ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS strategy_code TEXT;
ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS strategy_version TEXT;
ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS decision_policy_version TEXT;
ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS config_hash TEXT;
ALTER TABLE public.signal_outcomes ADD COLUMN IF NOT EXISTS provenance_version TEXT;

CREATE INDEX IF NOT EXISTS signal_outcomes_version_idx
ON public.signal_outcomes (strategy_code, strategy_version);
