-- ===========================================================================
-- 016_history_warmup_leases_and_universes.sql
-- ===========================================================================
-- Crash-safe warmup execution + frozen-universe identity. ADDITIVE:
--   * new columns on history_warmup_runs (provider-activity markers + execution
--     lease + reconciliation flag + universe link) — an EXISTING warmup table
--     owned by this feature (migration 014), never a strategy/campaign/
--     evaluation/outcome table;
--   * two NEW tables: history_warmup_universes + history_warmup_universe_symbols;
--   * DB-level immutability triggers for frozen universes.
-- No role/grant here (ops/sql owns those). No provider execution. No strategy
-- change. Apply ONLY to isolated Docker Postgres in this task.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Frozen universes: server-authoritative, immutable-after-freeze symbol sets.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.history_warmup_universes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  universe_code TEXT NOT NULL,
  universe_version INT NOT NULL DEFAULT 1 CHECK (universe_version >= 1),
  universe_hash TEXT,                       -- pinned at freeze (NULL while draft)
  config_hash TEXT,
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'frozen', 'superseded')),
  symbol_count INT NOT NULL DEFAULT 0 CHECK (symbol_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  frozen_at TIMESTAMPTZ,
  superseded_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT history_warmup_universes_code_upper CHECK (universe_code = upper(universe_code)),
  CONSTRAINT history_warmup_universes_identity UNIQUE (universe_code, universe_version),
  -- a frozen/superseded universe MUST carry its pinned hash + count
  CONSTRAINT history_warmup_universes_frozen_hash CHECK (
    status = 'draft' OR (universe_hash IS NOT NULL AND frozen_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS history_warmup_universes_status_idx
  ON public.history_warmup_universes (status, universe_code, universe_version DESC);

CREATE TABLE IF NOT EXISTS public.history_warmup_universe_symbols (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  universe_id UUID NOT NULL REFERENCES public.history_warmup_universes(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  ordinal INT NOT NULL CHECK (ordinal >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT history_warmup_universe_symbols_upper CHECK (symbol = upper(symbol)),
  CONSTRAINT history_warmup_universe_symbols_unique UNIQUE (universe_id, symbol),
  CONSTRAINT history_warmup_universe_symbols_ordinal UNIQUE (universe_id, ordinal)
);
CREATE INDEX IF NOT EXISTS history_warmup_universe_symbols_universe_idx
  ON public.history_warmup_universe_symbols (universe_id, ordinal);

-- ---------------------------------------------------------------------------
-- DB-level immutability: once a universe is NOT draft, its membership rows and
-- its identity/membership fields can never change (enforced regardless of role,
-- above the RLS/grant layer). Freeze/supersede status transitions are bounded.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.history_warmup_universe_symbols_guard()
RETURNS trigger AS $$
DECLARE st TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    SELECT status INTO st FROM public.history_warmup_universes WHERE id = OLD.universe_id;
    IF st IS DISTINCT FROM 'draft' THEN
      RAISE EXCEPTION 'universe % is % (not draft): membership is immutable', OLD.universe_id, st;
    END IF;
    RETURN OLD;
  END IF;
  SELECT status INTO st FROM public.history_warmup_universes WHERE id = NEW.universe_id;
  IF st IS DISTINCT FROM 'draft' THEN
    RAISE EXCEPTION 'universe % is % (not draft): membership is immutable', NEW.universe_id, st;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS history_warmup_universe_symbols_guard_trg
  ON public.history_warmup_universe_symbols;
CREATE TRIGGER history_warmup_universe_symbols_guard_trg
  BEFORE INSERT OR UPDATE OR DELETE ON public.history_warmup_universe_symbols
  FOR EACH ROW EXECUTE FUNCTION public.history_warmup_universe_symbols_guard();

CREATE OR REPLACE FUNCTION public.history_warmup_universes_guard()
RETURNS trigger AS $$
BEGIN
  -- allowed transitions: draft->draft, draft->frozen, frozen->superseded,
  -- superseded->superseded. Everything else (incl. frozen->draft, any change to
  -- identity/hash/membership count once frozen) is denied.
  IF OLD.status = 'frozen' THEN
    IF NEW.status NOT IN ('frozen', 'superseded') THEN
      RAISE EXCEPTION 'frozen universe % cannot transition to %', OLD.id, NEW.status;
    END IF;
    IF NEW.universe_code <> OLD.universe_code OR NEW.universe_version <> OLD.universe_version
       OR NEW.universe_hash IS DISTINCT FROM OLD.universe_hash
       OR NEW.symbol_count <> OLD.symbol_count OR NEW.config_hash IS DISTINCT FROM OLD.config_hash THEN
      RAISE EXCEPTION 'frozen universe % identity/membership is immutable', OLD.id;
    END IF;
  ELSIF OLD.status = 'superseded' THEN
    RAISE EXCEPTION 'superseded universe % is immutable', OLD.id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS history_warmup_universes_guard_trg ON public.history_warmup_universes;
CREATE TRIGGER history_warmup_universes_guard_trg
  BEFORE UPDATE ON public.history_warmup_universes
  FOR EACH ROW EXECUTE FUNCTION public.history_warmup_universes_guard();

-- ---------------------------------------------------------------------------
-- history_warmup_runs: provider-activity markers, execution lease, reconcile,
-- universe link. Provider cooldown is derived from provider-activity timestamps
-- (NOT run start / finished_at), so a crash AFTER provider activity keeps the
-- cooldown fail-closed while a crash BEFORE provider activity is re-drivable.
-- ---------------------------------------------------------------------------
ALTER TABLE public.history_warmup_runs
  ADD COLUMN IF NOT EXISTS provider_activity_state TEXT NOT NULL DEFAULT 'none'
    CHECK (provider_activity_state IN ('none', 'started', 'completed')),
  ADD COLUMN IF NOT EXISTS provider_activity_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_provider_activity_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS provider_request_count_attempted INT NOT NULL DEFAULT 0
    CHECK (provider_request_count_attempted >= 0),
  ADD COLUMN IF NOT EXISTS execution_lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reconciled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS universe_id UUID REFERENCES public.history_warmup_universes(id);

CREATE INDEX IF NOT EXISTS history_warmup_runs_provider_activity_idx
  ON public.history_warmup_runs (provider_activity_state, last_provider_activity_at DESC);

ALTER TABLE public.history_warmup_universes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.history_warmup_universe_symbols ENABLE ROW LEVEL SECURITY;
