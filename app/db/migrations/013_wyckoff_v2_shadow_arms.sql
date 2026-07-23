-- Phase 9D2/9D3: allow the wyckoff_v2_vs_baseline shadow experiment's arm
-- codes in strategy_shadow_evaluations.
--
-- Additive and idempotent (safe to run more than once). Run manually in the
-- Supabase SQL editor after 012_wyckoff_mtf_v2.sql. Migrations 010/011/012
-- are NOT modified by this file.
--
-- Why a migration is required at all: migration 010 declared an inline CHECK
-- constraint restricting arm_code to the historical sma150 experiment's arms
-- ('control_v2', 'candidate_v3'). Phase 9D generalizes the SAME shadow
-- runner/tables to a closed registry of declared experiments; the
-- wyckoff_v2_vs_baseline experiment persists honest arm codes
-- ('control_baseline' for the sma150_bounce baseline arm,
-- 'candidate_wyckoff_v2' for the wyckoff_mtf_v2 candidate arm) instead of
-- reusing misleading v2/v3 labels. Nothing else in the 010/011 schema needs
-- to change:
--   * verdicts stay ENTER/WATCH/AVOID for both arms of every declared
--     experiment (wyckoff_mtf.policy.v1 emits exactly these three);
--   * rollout-blocked and data-sufficiency states live inside the bounded
--     details_snapshot (policy.enter_eligible_without_rollout_gate,
--     policy.allow_enter, readiness.status) — no new columns are needed;
--   * pair outcomes (011) are already arm-agnostic market-path observations.
--
-- This migration changes NO data, NO defaults, NO existing rows and NO other
-- constraint. Existing sma150 shadow rows remain valid under the extended
-- CHECK. It does not enable wyckoff_mtf_v2 anywhere: patterns.is_enabled,
-- allow_enter, enable_4h_trigger and min_price are untouched (migration 012
-- rollout-safe defaults remain false / false / false / 5.0).

ALTER TABLE public.strategy_shadow_evaluations
  DROP CONSTRAINT IF EXISTS strategy_shadow_evaluations_arm_code_check;

ALTER TABLE public.strategy_shadow_evaluations
  ADD CONSTRAINT strategy_shadow_evaluations_arm_code_check
  CHECK (arm_code IN (
    'control_v2',
    'candidate_v3',
    'control_baseline',
    'candidate_wyckoff_v2'
  ));
