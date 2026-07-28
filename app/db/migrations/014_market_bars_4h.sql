-- ===========================================================================
-- 014_market_bars_4h.sql — local 4H persistence + history-warmup metadata
-- ===========================================================================
-- FOUNDATION ONLY. Additive: creates two NEW tables. Touches no existing table,
-- no existing row, no campaign/evaluation/outcome/pair data, no role here (roles
-- + grants + RLS policies live in ops/sql/create_shadow_history_warmer*.sql, run
-- by the table owner, exactly like the audit-reader / outcome-maintainer roles).
-- No provider execution. No strategy change.
--
-- Canonical 4H bar model (Option A — persist provider-native adjusted 4H
-- aggregates; the existing frames_4h layer reapplies the session/completed cut):
--   * bar_start / bar_end : provider-native UTC bounds (epoch-ms starts;
--     bar_end = bar_start + 4h). bar_end > bar_start enforced.
--   * session_date        : exchange (America/New_York) date of bar_end,
--     mirroring frames_4h `_session_date(end, "America/New_York")`.
--   * exchange timezone    : America/New_York.
--   * is_regular_session   : provenance only (whether the bar sits in the
--     09:30–16:00 ET regular session). NOT used in the readiness gate, because
--     the strategy consumes all provider-native bars (no regular-session filter)
--     — recording it here avoids future drift without changing behavior now.
--   * is_completed         : the bar has fully closed and does not end in the
--     future (write-time invariant; see below). Only completed bars count toward
--     the 4H readiness gate.
--   * provider_adjustment  : the client fetches adjusted=true → split & dividend
--     adjusted. Recorded so a future unadjusted feed cannot silently collide.
--   * Early-close days      : the final 4H bar of a shortened session is simply a
--     shorter/observed bar; missing bars are NEVER synthesized (matches
--     frames_4h). Readiness counts OBSERVED completed bars.
--   * 11-bar readiness rule : candidate 4H-ready ⟺ completed 4H bar count >= 11
--     (= trigger_lookback_4h(10) + 1).
--
-- "completed must not end in the future" is a WRITE-TIME invariant enforced by
-- the history warmer (a CHECK cannot reference now() immutably). All structural
-- invariants below ARE enforced as CHECK constraints.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.market_bars_4h (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,
  bar_start TIMESTAMPTZ NOT NULL,
  bar_end TIMESTAMPTZ NOT NULL,
  session_date DATE NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume NUMERIC NOT NULL,
  is_completed BOOLEAN NOT NULL,
  is_regular_session BOOLEAN NOT NULL,
  provider TEXT NOT NULL DEFAULT 'massive',
  provider_adjustment TEXT NOT NULL DEFAULT 'split_dividend_adjusted',
  source_timestamp TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  content_fingerprint TEXT NOT NULL,       -- sha256 of canonical OHLCV → correction detection
  CONSTRAINT market_bars_4h_symbol_upper CHECK (symbol = upper(symbol)),
  CONSTRAINT market_bars_4h_time_order CHECK (bar_end > bar_start),
  CONSTRAINT market_bars_4h_volume_nonneg CHECK (volume >= 0),
  CONSTRAINT market_bars_4h_ohlc_valid CHECK (
    high >= low AND high >= open AND high >= close
    AND low <= open AND low <= close
    AND open >= 0 AND low >= 0 AND close >= 0 AND high >= 0
  ),
  CONSTRAINT market_bars_4h_provider CHECK (provider IN ('massive')),
  CONSTRAINT market_bars_4h_adjustment CHECK (
    provider_adjustment IN ('split_dividend_adjusted', 'unadjusted')
  ),
  -- Uniqueness identity: a symbol's bar at a given start, per provider AND
  -- adjustment basis. Including provider + adjustment prevents an unadjusted
  -- feed (different semantics, same start) from colliding with the canonical
  -- adjusted bar. A provider CORRECTION to the SAME (symbol,start,provider,
  -- adjustment) is an UPSERT of OHLCV + updated_at + content_fingerprint
  -- (idempotent when unchanged), never a duplicate row.
  CONSTRAINT market_bars_4h_identity UNIQUE (symbol, bar_start, provider, provider_adjustment)
);

-- latest completed bars per symbol; completed-count per symbol
CREATE INDEX IF NOT EXISTS market_bars_4h_symbol_completed_start_idx
  ON public.market_bars_4h (symbol, is_completed, bar_start DESC);
-- oldest/newest timestamp + session-date range scans + readiness manifest
CREATE INDEX IF NOT EXISTS market_bars_4h_symbol_session_idx
  ON public.market_bars_4h (symbol, session_date);
CREATE INDEX IF NOT EXISTS market_bars_4h_bar_start_idx
  ON public.market_bars_4h (bar_start);

-- ---------------------------------------------------------------------------
-- Bounded history-warmup run metadata (orchestration bookkeeping ONLY — never a
-- provider key, DSN, token or raw payload). Populated by a FUTURE warmup execute
-- (not in this task).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.history_warmup_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  mode TEXT NOT NULL DEFAULT 'history_warmup',
  status TEXT NOT NULL DEFAULT 'planned'
    CHECK (status IN ('planned', 'running', 'completed', 'failed', 'cancelled')),
  universe_hash TEXT,
  readiness_manifest_hash TEXT,
  requested_symbols JSONB,                 -- bounded, normalized symbol list
  requested_symbol_count INT,
  processed_symbol_count INT NOT NULL DEFAULT 0,
  provider_request_count INT NOT NULL DEFAULT 0,
  idempotency_key TEXT,                    -- request/batch identity for replay
  cooldown_last_finished_at TIMESTAMPTZ,
  cooldown_next_not_before TIMESTAMPTZ,
  error_code TEXT,                         -- safe identity, never a trace
  error_message TEXT,                      -- sanitized + bounded
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT history_warmup_runs_counts_nonneg CHECK (
    processed_symbol_count >= 0 AND provider_request_count >= 0
  ),
  CONSTRAINT history_warmup_runs_idempotency UNIQUE (idempotency_key)
);
CREATE INDEX IF NOT EXISTS history_warmup_runs_status_started_idx
  ON public.history_warmup_runs (status, started_at DESC);

-- Enable RLS on both new tables (fail-closed; policies for the audit reader and
-- the history-warmer role are created by the ops/sql policy script, owner-run,
-- mirroring the existing role architecture). The table owner is unaffected.
ALTER TABLE public.market_bars_4h ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.history_warmup_runs ENABLE ROW LEVEL SECURITY;
