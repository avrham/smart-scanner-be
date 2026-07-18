-- Phase 1 (Evidence Engine): tighten sma150_bounce config and make it authoritative.
--
-- B12: the running defaults were too permissive (15% proximity, 0.1 score
-- threshold), producing noisy signals. These conservative values are the
-- authoritative Phase 1 config. We DO UPDATE (not DO NOTHING) so existing
-- deployments are corrected, and add the two new config keys the evaluator
-- now reads: score_threshold and min_price.
--
-- Run this manually in Supabase after 001_initial_schema.sql.

INSERT INTO public.pattern_configs (pattern_code, key, value) VALUES
  ('sma150_bounce', 'sma_window', '150'),
  ('sma150_bounce', 'touch_tolerance_pct', '3.0'),
  ('sma150_bounce', 'lookback_days_for_history', '365'),
  ('sma150_bounce', 'min_bounces', '2'),
  ('sma150_bounce', 'min_avg_rebound_pct', '5.0'),
  ('sma150_bounce', 'rebound_window_days', '10'),
  ('sma150_bounce', 'min_volume_sma_ratio', '1.0'),
  ('sma150_bounce', 'min_price', '5.0'),
  ('sma150_bounce', 'score_threshold', '0.5'),
  ('sma150_bounce', 'min_liquidity_filters', '{"min_market_cap": 200000000, "min_daily_volume": 200000}')
ON CONFLICT (pattern_code, key) DO UPDATE SET value = EXCLUDED.value;
