-- Massive provider support: universe metadata + local daily bars.
--
-- Additive and idempotent. Extends the existing tickers cache with the fields
-- Massive's /v3/reference/tickers returns, and adds a daily_bars table for the
-- grouped-daily snapshot + per-symbol historical aggregates.
--
-- Notes:
--  * tickers.exchange keeps the legacy short name (NASDAQ/NYSE/AMEX) so the
--    existing funnel universe query keeps working; the raw MIC goes to
--    primary_exchange (XNAS/XNYS/XASE/...).
--  * enrichment_status: 'pending' | 'enriched' | 'missing_market_cap' | 'error'
--    (missing market cap is preserved as NULL + status, NEVER coerced to 0).
--
-- Run manually in Supabase after 004_phase5_wyckoff_mtf_config.sql.

ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS market TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS locale TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS primary_exchange TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS security_type TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS cik TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS composite_figi TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS share_class_figi TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS provider_updated_at TIMESTAMPTZ;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS profile_synced_at TIMESTAMPTZ;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS enrichment_status TEXT;
ALTER TABLE public.tickers ADD COLUMN IF NOT EXISTS eligible BOOLEAN;

-- Local daily OHLCV bars (grouped snapshot + per-symbol aggregates).
CREATE TABLE IF NOT EXISTS public.daily_bars (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol TEXT NOT NULL,
  trading_date DATE NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume NUMERIC NOT NULL,
  vwap NUMERIC,
  transaction_count BIGINT,
  source TEXT NOT NULL DEFAULT 'massive',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(symbol, trading_date)
);

CREATE INDEX IF NOT EXISTS daily_bars_symbol_date_idx
ON public.daily_bars (symbol, trading_date DESC);

CREATE INDEX IF NOT EXISTS daily_bars_date_idx
ON public.daily_bars (trading_date);
