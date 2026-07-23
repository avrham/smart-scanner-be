-- Phase 9C2 (Evidence Engine): register wyckoff_mtf_v2 + config.
--
-- Additive and idempotent (safe to run more than once). Does NOT touch the
-- existing wyckoff_mtf (wyckoff_mtf.v1) rows or configuration in any way, and
-- performs no signal/outcome backfill and no destructive change.
--
-- wyckoff_mtf_v2 is registered DISABLED (is_enabled=false) so scheduled
-- production scans never pick it up before controlled rollout. Manual or
-- explicitly requested scans can still select it by pattern_code through the
-- existing strategy registry once the Python registry registers the class.
-- Flip is_enabled to true only when you deliberately want it scanned
-- automatically.
--
-- Rollout-safe defaults (must match app DEFAULT_CONFIG):
--   allow_enter=false
--   enable_4h_trigger=false
--   min_price=5.0
--
-- Config conflict policy: DO NOTHING (first run seeds the defaults; a rerun
-- NEVER resets operator-modified configuration). This matches migration 008.
--
-- Values mirror app/workers/strategies/wyckoff_v2/constants.py::DEFAULT_CONFIG.
-- Run manually in Supabase after 011_shadow_pair_outcomes.sql.
-- Do not create migration 013 for this phase.

INSERT INTO public.patterns (code, name, description, is_enabled) VALUES
  ('wyckoff_mtf_v2', 'Wyckoff MTF v2',
   'wyckoff_mtf.v2: deterministic multi-timeframe Wyckoff strategy with completed-bar readiness, HTF context, event/phase candidates, 4H trigger, rollout-gated policy and evidence.v1. Separate from wyckoff_mtf (wyckoff_mtf.v1). Disabled and allow_enter=false by default.',
   false)
ON CONFLICT (code) DO NOTHING;

INSERT INTO public.pattern_configs (pattern_code, key, value) VALUES
  ('wyckoff_mtf_v2', 'accumulation_close_off_low_min', '0.55'),
  ('wyckoff_mtf_v2', 'allow_enter', 'false'),
  ('wyckoff_mtf_v2', 'atr_window', '14'),
  ('wyckoff_mtf_v2', 'automatic_rally_window_bars', '10'),
  ('wyckoff_mtf_v2', 'avoid_on_htf_contradiction', 'true'),
  ('wyckoff_mtf_v2', 'bar_completion_policy', '"ny_session_close.v1"'),
  ('wyckoff_mtf_v2', 'bearish_close_location_max', '0.35'),
  ('wyckoff_mtf_v2', 'bullish_close_location_min', '0.65'),
  ('wyckoff_mtf_v2', 'climax_spread_atr_ratio', '1.5'),
  ('wyckoff_mtf_v2', 'completed_bar_exclusion_margin', '1'),
  ('wyckoff_mtf_v2', 'distribution_close_off_high_max', '0.45'),
  ('wyckoff_mtf_v2', 'effort_high_volume_ratio', '1.5'),
  ('wyckoff_mtf_v2', 'effort_low_volume_ratio', '0.8'),
  ('wyckoff_mtf_v2', 'enable_4h_trigger', 'false'),
  ('wyckoff_mtf_v2', 'enter_eligible_phases', '["C","D","E"]'),
  ('wyckoff_mtf_v2', 'event_atr_window', '14'),
  ('wyckoff_mtf_v2', 'event_breakout_buffer_atr_multiple', '0.05'),
  ('wyckoff_mtf_v2', 'event_confirmation_window_bars', '3'),
  ('wyckoff_mtf_v2', 'event_invalidation_buffer_atr_multiple', '0.1'),
  ('wyckoff_mtf_v2', 'event_min_volume_baseline_bars', '15'),
  ('wyckoff_mtf_v2', 'event_pierce_atr_multiple', '0.1'),
  ('wyckoff_mtf_v2', 'event_retest_tolerance_atr_multiple', '0.25'),
  ('wyckoff_mtf_v2', 'event_volume_baseline_window', '20'),
  ('wyckoff_mtf_v2', 'event_zone_approach_atr_multiple', '0.5'),
  ('wyckoff_mtf_v2', 'exchange_timezone', '"America/New_York"'),
  ('wyckoff_mtf_v2', 'four_hour_bar_duration_hours', '4'),
  ('wyckoff_mtf_v2', 'four_hour_timestamp_timezone', '"UTC"'),
  ('wyckoff_mtf_v2', 'history_request_margin_bars', '10'),
  ('wyckoff_mtf_v2', 'history_request_trading_days_per_month', '23'),
  ('wyckoff_mtf_v2', 'history_request_trading_days_per_week', '5'),
  ('wyckoff_mtf_v2', 'htf_structure_tolerance_pct', '0.0'),
  ('wyckoff_mtf_v2', 'lps_max_bars_after_sos', '20'),
  ('wyckoff_mtf_v2', 'lpsy_max_bars_after_sow', '20'),
  ('wyckoff_mtf_v2', 'max_4h_staleness_sessions', '1'),
  ('wyckoff_mtf_v2', 'max_breakout_contamination_fraction', '0.2'),
  ('wyckoff_mtf_v2', 'max_event_candidates_in_details', '60'),
  ('wyckoff_mtf_v2', 'max_event_candidates_in_evidence', '32'),
  ('wyckoff_mtf_v2', 'max_event_candidates_per_code', '10'),
  ('wyckoff_mtf_v2', 'max_missing_volume_fraction', '0.2'),
  ('wyckoff_mtf_v2', 'max_total_event_candidates', '120'),
  ('wyckoff_mtf_v2', 'max_width_coefficient_of_variation', '0.5'),
  ('wyckoff_mtf_v2', 'min_containment_fraction', '0.8'),
  ('wyckoff_mtf_v2', 'min_price', '5.0'),
  ('wyckoff_mtf_v2', 'min_range_volume_coverage', '0.8'),
  ('wyckoff_mtf_v2', 'min_resistance_touch_clusters', '2'),
  ('wyckoff_mtf_v2', 'min_structure_confirmed_event_types', '2'),
  ('wyckoff_mtf_v2', 'min_support_touch_clusters', '2'),
  ('wyckoff_mtf_v2', 'min_touch_separation_bars', '3'),
  ('wyckoff_mtf_v2', 'monthly_min_periods', '24'),
  ('wyckoff_mtf_v2', 'monthly_slope_lookback', '3'),
  ('wyckoff_mtf_v2', 'monthly_slope_reference_pct', '2.0'),
  ('wyckoff_mtf_v2', 'monthly_sma_window', '20'),
  ('wyckoff_mtf_v2', 'monthly_structure_window_periods', '4'),
  ('wyckoff_mtf_v2', 'narrow_spread_atr_ratio', '0.8'),
  ('wyckoff_mtf_v2', 'phase_b_min_range_bars', '30'),
  ('wyckoff_mtf_v2', 'phase_e_hold_bars', '2'),
  ('wyckoff_mtf_v2', 'quantile_interpolation', '"linear"'),
  ('wyckoff_mtf_v2', 'range_end_lookback_bars', '20'),
  ('wyckoff_mtf_v2', 'range_end_step', '1'),
  ('wyckoff_mtf_v2', 'range_length_step', '5'),
  ('wyckoff_mtf_v2', 'range_max_atr_multiple', '12.0'),
  ('wyckoff_mtf_v2', 'range_max_bars', '120'),
  ('wyckoff_mtf_v2', 'range_min_atr_multiple', '3.0'),
  ('wyckoff_mtf_v2', 'range_min_bars', '20'),
  ('wyckoff_mtf_v2', 'range_stability_step_bars', '5'),
  ('wyckoff_mtf_v2', 'range_stability_window_bars', '10'),
  ('wyckoff_mtf_v2', 'require_4h_trigger_for_enter', 'true'),
  ('wyckoff_mtf_v2', 'resistance_quantile_high', '0.95'),
  ('wyckoff_mtf_v2', 'resistance_quantile_low', '0.85'),
  ('wyckoff_mtf_v2', 'result_high_atr_ratio', '1.0'),
  ('wyckoff_mtf_v2', 'result_low_atr_ratio', '0.35'),
  ('wyckoff_mtf_v2', 'secondary_test_max_bars_after_climax', '40'),
  ('wyckoff_mtf_v2', 'secondary_test_min_separation_bars', '3'),
  ('wyckoff_mtf_v2', 'session_close_time', '"16:00"'),
  ('wyckoff_mtf_v2', 'structure_quality_full_event_types', '4'),
  ('wyckoff_mtf_v2', 'support_quantile_high', '0.15'),
  ('wyckoff_mtf_v2', 'support_quantile_low', '0.05'),
  ('wyckoff_mtf_v2', 'test_max_bars_after_spring', '20'),
  ('wyckoff_mtf_v2', 'trigger_lookback_4h', '10'),
  ('wyckoff_mtf_v2', 'volume_baseline_window', '20'),
  ('wyckoff_mtf_v2', 'weekly_min_periods', '26'),
  ('wyckoff_mtf_v2', 'weekly_slope_lookback', '4'),
  ('wyckoff_mtf_v2', 'weekly_slope_reference_pct', '1.5'),
  ('wyckoff_mtf_v2', 'weekly_sma_window', '20'),
  ('wyckoff_mtf_v2', 'weekly_structure_window_periods', '6'),
  ('wyckoff_mtf_v2', 'wide_spread_atr_ratio', '1.2')
ON CONFLICT (pattern_code, key) DO NOTHING;
