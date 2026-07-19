-- Phase 5 (Evidence Engine): register the wyckoff_mtf strategy + its config.
--
-- Additive and idempotent. Does NOT touch sma150_bounce. wyckoff_mtf is
-- registered DISABLED (is_enabled=false) so it never becomes the default/auto
-- scanner; it is opt-in via the funnel `pattern_code`. Flip is_enabled to true
-- when you deliberately want it scanned.
--
-- Values mirror app/workers/strategies/wyckoff/strategy.py::DEFAULT_CONFIG so
-- the DB config is authoritative. Run manually in Supabase after
-- 003_phase2_signal_outcomes.sql.

INSERT INTO public.patterns (code, name, description, is_enabled) VALUES
  ('wyckoff_mtf', 'Wyckoff MTF v1',
   'Deterministic multi-timeframe (monthly/weekly/daily/4H) Wyckoff-style strategy',
   false)
ON CONFLICT (code) DO NOTHING;

INSERT INTO public.pattern_configs (pattern_code, key, value) VALUES
  ('wyckoff_mtf', 'monthly_sma_window', '20'),
  ('wyckoff_mtf', 'monthly_min_bars', '24'),
  ('wyckoff_mtf', 'monthly_slope_lookback', '3'),
  ('wyckoff_mtf', 'weekly_sma_window', '20'),
  ('wyckoff_mtf', 'weekly_min_bars', '26'),
  ('wyckoff_mtf', 'weekly_slope_lookback', '4'),
  ('wyckoff_mtf', 'daily_range_lookback', '60'),
  ('wyckoff_mtf', 'atr_window', '14'),
  ('wyckoff_mtf', 'min_range_atr_multiple', '3.0'),
  ('wyckoff_mtf', 'pierce_atr_multiple', '0.10'),
  ('wyckoff_mtf', 'volume_sma_window', '20'),
  ('wyckoff_mtf', 'min_breakout_volume_ratio', '1.5'),
  ('wyckoff_mtf', 'trigger_lookback_4h', '10'),
  ('wyckoff_mtf', 'enable_4h_trigger', 'false'),
  ('wyckoff_mtf', 'require_4h_for_enter', 'true'),
  ('wyckoff_mtf', 'score_threshold', '0.55'),
  ('wyckoff_mtf', 'min_price', '5.0'),
  ('wyckoff_mtf', 'min_daily_bars', '540'),
  ('wyckoff_mtf', 'min_liquidity_filters', '{"min_market_cap": 300000000, "min_daily_volume": 300000}')
ON CONFLICT (pattern_code, key) DO UPDATE SET value = EXCLUDED.value;
