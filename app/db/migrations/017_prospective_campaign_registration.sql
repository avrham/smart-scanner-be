-- ===========================================================================
-- 017_prospective_campaign_registration.sql
-- ===========================================================================
-- Additive: ONE new table pinning the immutable identity of a prospective
-- Wyckoff-v2-vs-baseline campaign (frozen universe + strategy config + history
-- manifests + completed-session snapshot). Touches no existing table, no
-- strategy/outcome logic, no existing row. The campaign itself still lives in
-- strategy_shadow_runs/pairs/evaluations (created by the reused shadow runner);
-- this table is the durable prospective REGISTRATION + campaign linkage +
-- idempotency identity. No role/grant here (ops/sql owns those). Apply ONLY to
-- the isolated non-production database.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.prospective_campaign_registrations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  experiment_code TEXT NOT NULL,
  experiment_contract_version TEXT NOT NULL,
  -- frozen universe identity (immutable once registered)
  universe_id UUID NOT NULL,
  universe_code TEXT NOT NULL,
  universe_version INT NOT NULL,
  universe_hash TEXT NOT NULL,
  history_config_hash TEXT NOT NULL,
  history_readiness_manifest_hash TEXT NOT NULL,
  -- pinned strategy identities
  candidate_strategy_code TEXT NOT NULL,
  candidate_strategy_version TEXT NOT NULL,
  candidate_signal_definition TEXT NOT NULL,
  candidate_allow_enter BOOLEAN NOT NULL DEFAULT FALSE,
  control_strategy_code TEXT NOT NULL,
  control_strategy_version TEXT NOT NULL,
  -- completed-session snapshot (server-selected)
  snapshot_session_date DATE NOT NULL,
  snapshot_cutoff_at TIMESTAMPTZ NOT NULL,
  market_calendar_version TEXT NOT NULL,
  -- idempotency: one registration per (experiment, universe, config, snapshot)
  registration_identity TEXT NOT NULL,
  -- campaign linkage (set at execute; NULL until then)
  campaign_id UUID,
  campaign_run_id UUID,
  campaign_execution_identity TEXT,
  -- lifecycle + crash-safe lease + bounded telemetry (no secrets)
  status TEXT NOT NULL DEFAULT 'registered'
    CHECK (status IN ('draft', 'registered', 'executing', 'completed', 'failed')),
  execution_lease_expires_at TIMESTAMPTZ,
  pair_count INT NOT NULL DEFAULT 0,
  candidate_evaluation_count INT NOT NULL DEFAULT 0,
  control_evaluation_count INT NOT NULL DEFAULT 0,
  telemetry JSONB,
  error_code TEXT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT pcr_allow_enter_false CHECK (candidate_allow_enter = FALSE),
  CONSTRAINT pcr_counts_nonneg CHECK (
    pair_count >= 0 AND candidate_evaluation_count >= 0 AND control_evaluation_count >= 0),
  CONSTRAINT pcr_identity_unique UNIQUE (registration_identity),
  CONSTRAINT pcr_execution_identity_unique UNIQUE (campaign_execution_identity)
);
CREATE INDEX IF NOT EXISTS pcr_experiment_universe_idx
  ON public.prospective_campaign_registrations (experiment_code, universe_id, snapshot_session_date);
CREATE INDEX IF NOT EXISTS pcr_status_idx
  ON public.prospective_campaign_registrations (status, created_at DESC);

ALTER TABLE public.prospective_campaign_registrations ENABLE ROW LEVEL SECURITY;
