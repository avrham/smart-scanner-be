-- Phase 8 (Evidence Engine): register sma150_bounce_v3 (sma150.v3) + config.
--
-- Additive and idempotent (safe to run more than once). Does NOT touch the
-- existing sma150_bounce (sma150.v2) rows or configuration in any way, and
-- performs no signal/outcome backfill and no destructive change.
--
-- sma150_bounce_v3 is registered DISABLED (is_enabled=false) so scheduled
-- production scans never pick it up before shadow validation. Manual or
-- explicitly requested scans can still select it by pattern_code through the
-- existing strategy registry. Flip is_enabled to true only when you
-- deliberately want it scanned automatically.
--
-- Config conflict policy: DO NOTHING (first run seeds the defaults; a rerun
-- NEVER resets operator-modified v3 configuration). This differs from
-- migration 002, which deliberately used DO UPDATE because it was an
-- explicit CORRECTION of bad live values — here the migration only seeds a
-- brand-new pattern, so operator changes win. To intentionally reset a key
-- to the shipped default, delete that pattern_configs row and rerun.
--
-- Values mirror app/workers/strategies/sma150_v3.py::DEFAULT_CONFIG.
-- Run manually in Supabase after 007_scan_signal_provenance.sql.
-- Do not create migration 009 for this phase.

INSERT INTO public.patterns (code, name, description, is_enabled) VALUES
  ('sma150_bounce_v3', 'SMA-150 Bounce v3',
   'sma150.v3: layered SMA-150 bounce (data readiness / setup validity / entry confirmation / ranking) with normalized evidence.v1 output, completed-daily-bar policy ny_session_close.v1 and decision policy sma150_bounce.policy.v1. Separate from sma150_bounce (sma150.v2).',
   false)
ON CONFLICT (code) DO NOTHING;

INSERT INTO public.pattern_configs (pattern_code, key, value) VALUES
  ('sma150_bounce_v3', 'sma_window', '150'),
  ('sma150_bounce_v3', 'min_history_bars', '200'),
  ('sma150_bounce_v3', 'lookback_bars_for_history', '365'),
  ('sma150_bounce_v3', 'volume_window_bars', '20'),
  ('sma150_bounce_v3', 'slope_lookback_bars', '20'),
  ('sma150_bounce_v3', 'rebound_window_bars', '10'),
  ('sma150_bounce_v3', 'max_close_above_sma_pct', '3.0'),
  ('sma150_bounce_v3', 'max_close_below_sma_pct', '1.0'),
  ('sma150_bounce_v3', 'touch_tolerance_pct', '3.0'),
  ('sma150_bounce_v3', 'min_event_separation_bars', '15'),
  ('sma150_bounce_v3', 'min_independent_bounces', '2'),
  ('sma150_bounce_v3', 'min_median_rebound_pct', '5.0'),
  ('sma150_bounce_v3', 'min_sma_slope_pct', '0.0'),
  ('sma150_bounce_v3', 'min_close_location_value', '0.65'),
  ('sma150_bounce_v3', 'min_trigger_volume_ratio', '1.20'),
  ('sma150_bounce_v3', 'invalidation_below_sma_pct', '2.0'),
  ('sma150_bounce_v3', 'recency_half_life_bars', '126'),
  ('sma150_bounce_v3', 'trend_quality_full_scale_slope_pct', '2.0'),
  ('sma150_bounce_v3', 'bounce_quality_full_count', '4'),
  ('sma150_bounce_v3', 'rebound_quality_full_pct', '10.0'),
  ('sma150_bounce_v3', 'bar_completion_policy', '"ny_session_close.v1"'),
  ('sma150_bounce_v3', 'exchange_timezone', '"America/New_York"'),
  ('sma150_bounce_v3', 'session_close_time', '"16:00"'),
  ('sma150_bounce_v3', 'min_price', '5.0'),
  ('sma150_bounce_v3', 'min_liquidity_filters', '{"min_market_cap": 200000000, "min_daily_volume": 200000}')
ON CONFLICT (pattern_code, key) DO NOTHING;
