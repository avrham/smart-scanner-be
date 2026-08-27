-- ===========================================================================
-- 020 — Company news catalysts (News / Company Catalyst Context V1)
-- ===========================================================================
-- Answers ONE product question: "what material company-specific development
-- may explain why this symbol deserves attention right now?"
--
-- It is deliberately NOT a news platform, NOT a feed reader and NOT a
-- sentiment store. News is CONTEXT that sits beside the strategy result: it
-- cannot reach the candidate verdict, the Wyckoff evaluation, the attention
-- tier, the ordering or ENTER eligibility, and no scanner table references it.
--
-- WHAT IS DELIBERATELY NOT STORED
-- -------------------------------
-- The provider returns `insights[].sentiment` / `sentiment_reasoning` and an
-- AI-written `description` alongside each article. NONE of it is persisted.
-- Storing a machine's opinion next to a scanner verdict would let a reader
-- treat the two as the same kind of fact, and V1 answers "something material
-- happened" — never "this makes the stock bullish". The columns below hold
-- only checkable facts: who published what, when, where to read it, and which
-- tickers the provider attached.
--
-- TWO TABLES, ONE REASON
-- ----------------------
-- One article legitimately belongs to several symbols. Storing it once and
-- linking it keeps the dedupe key honest (an article has ONE identity) while
-- letting per-symbol relevance differ: an article can name Apple in its title
-- and merely list Bank of America among 24 other tickers.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS public.company_news_articles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Provenance. `provider_article_id` is the provider's own stable article
  -- identity and is the PRIMARY dedupe key — re-running ingestion updates a
  -- row rather than inserting a second copy of the same story.
  provider TEXT NOT NULL,
  provider_article_id TEXT NOT NULL,

  -- Publication is a PAST PUBLIC FACT. This column, not `observed_at`, is what
  -- decides whether a historical scan session was allowed to see the article.
  published_at TIMESTAMPTZ NOT NULL,

  title TEXT NOT NULL,
  -- Lowercased, punctuation-stripped, whitespace-collapsed title. Stored so
  -- the near-duplicate rule is auditable after the fact instead of being
  -- re-derived (and possibly re-derived differently) at read time.
  title_normalized TEXT NOT NULL,

  publisher TEXT NOT NULL,
  publisher_home_url TEXT,
  author TEXT,

  -- `article_url` is what a human clicks. `canonical_url` is that URL reduced
  -- to scheme+host+path with query/fragment removed — the SECOND dedupe key,
  -- which catches the same story re-issued by the provider under a new id.
  article_url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,

  -- How many tickers the provider attached to this article, and the resulting
  -- scope. A story naming 25 tickers is a market piece, not a company
  -- catalyst; the count is kept so that judgement stays inspectable.
  ticker_breadth INTEGER NOT NULL,
  scope TEXT NOT NULL,

  -- Deterministic category, plus how it was reached, so the product can say
  -- "we do not know" instead of implying provider-grade certainty.
  --   provider       the feed stated it (unused today; the entitled feed
  --                  carries no category field — reserved, never guessed)
  --   derived_title  matched one explicit high-precision title pattern
  --   default        nothing matched -> general_company_news
  category TEXT NOT NULL,
  category_source TEXT NOT NULL,

  -- When WE first stored it. Drives freshness reporting and audit only — it is
  -- NOT the point-in-time gate for news (see `published_at` above).
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT company_news_articles_scope_ck
    CHECK (scope IN ('company_specific', 'multi_company', 'market_wide')),
  CONSTRAINT company_news_articles_category_ck
    CHECK (category IN ('earnings_results', 'guidance', 'analyst_action',
                        'merger_acquisition', 'regulatory_legal', 'management',
                        'product_announcement', 'financing_capital',
                        'general_company_news')),
  CONSTRAINT company_news_articles_category_source_ck
    CHECK (category_source IN ('provider', 'derived_title', 'default')),
  CONSTRAINT company_news_articles_breadth_ck
    CHECK (ticker_breadth >= 0),

  -- Dedupe key #1: one row per provider article, forever.
  CONSTRAINT company_news_articles_provider_unique
    UNIQUE (provider, provider_article_id)
);

CREATE INDEX IF NOT EXISTS company_news_articles_published_idx
  ON public.company_news_articles (published_at DESC);

-- Dedupe key #2 is enforced by the INGESTION layer, not by a UNIQUE
-- constraint: a collision must make ingestion skip the newcomer, never make
-- the whole refresh fail. The index is what makes that lookup cheap.
CREATE INDEX IF NOT EXISTS company_news_articles_canonical_idx
  ON public.company_news_articles (provider, canonical_url);

-- ---------------------------------------------------------------------------
-- Per-symbol association.
--
-- `relevance` is a VISIBILITY fact, not an importance score:
--   primary    the article's own title names this company (its ticker or its
--              name) — the story is about it
--   mentioned  the provider attached the ticker but the title does not name
--              the company — usually a round-up or a portfolio piece
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.company_news_symbols (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  article_id UUID NOT NULL
    REFERENCES public.company_news_articles(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  relevance TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT company_news_symbols_relevance_ck
    CHECK (relevance IN ('primary', 'mentioned')),
  CONSTRAINT company_news_symbols_unique UNIQUE (symbol, article_id)
);

CREATE INDEX IF NOT EXISTS company_news_symbols_symbol_idx
  ON public.company_news_symbols (symbol);

-- ---------------------------------------------------------------------------
-- Freshness reuses `catalyst_source_state` (migration 019) with a third
-- source row, `provider_company_news`. Deliberately NOT a new table: the
-- Product API already batch-loads that one relation, and "when did this
-- source last succeed, and is it available to us at all" is the same question
-- for every catalyst source. An empty news table must never be readable as
-- "nothing happened" when the truth is "we never refreshed".
-- ---------------------------------------------------------------------------

-- RLS: enabled on every new table (policies are created by ops/sql per role).
ALTER TABLE public.company_news_articles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.company_news_symbols  ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Operating model: declare the refresh in the DISABLED daily-pipeline
-- template, immediately after the earnings/filings refresh.
--
-- `stages` on that template is a documentation list of intended daily steps,
-- not executable dispatch, and the schedule stays disabled here. Migrations
-- 018 and 019 are left untouched — this migration owns its own change.
--
-- Placement is deliberate: news sits AFTER the campaign, so it can never delay
-- or block a scan, and after `catalyst_refresh.v1`, because a news failure
-- must not cost the earnings dimension its refresh. `refresh_company_news`
-- absorbs a provider failure into `catalyst_source_state` instead of raising,
-- so the worst case is the product truthfully reporting news as unavailable.
--
-- Cadence: news moves faster than daily bars, but V1 deliberately introduces
-- NO new scheduler. One refresh per daily pipeline run is the minimum useful
-- cadence for a product whose smallest window is a whole trading session.
-- It must run on the component that already holds the provider credential
-- (the history-warmup worker). The Product API holds none.
-- ---------------------------------------------------------------------------
UPDATE public.job_schedules
SET payload_template = jsonb_set(
      payload_template, '{stages}',
      '["history_universe_refresh.v1","readiness_verification",
        "prospective_daily_campaign.v1","catalyst_refresh.v1",
        "company_news_refresh.v1",
        "outcome_maturation.v1","daily_quality_audit.v1"]'::jsonb)
WHERE schedule_code = 'SMART-SCANNER-DAILY-PIPELINE'
  AND schedule_version = 1
  AND NOT (payload_template -> 'stages' ? 'company_news_refresh.v1');
