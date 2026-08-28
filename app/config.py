"""
Configuration management for Smart Scanner Backend
"""

import os
from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Environment
    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    # Deployment build provenance (Deployment Readiness - Build Provenance).
    # Embedded at build/release time so a running backend can prove exactly
    # which source revision it is executing. These are OPTIONAL and default to
    # safe local values — startup and tests never require them, and they are
    # NEVER derived by running git inside the container at runtime.
    APP_GIT_SHA: str = "unknown"
    APP_BUILD_TIME: str = "unknown"   # ISO 8601 UTC build timestamp
    APP_ENVIRONMENT: str = "local"    # local | development | staging | production
    APP_RELEASE: str = "unknown"      # optional human/image release identifier

     # Database (legacy Supabase-derived identity). OPTIONAL with empty defaults so
    # an ISOLATED deployment that uses none of it can boot with ZERO Supabase
    # credentials (e.g. HISTORY_WARMUP_ONLY_MODE connects exclusively via
    # HISTORY_WARMUP_DATABASE_URL). Normal deployments still supply these via
    # secrets; the legacy connection path validates them at CONNECT time and
    # fails closed if they are missing when actually used.
    SUPABASE_URL: str = ""
    SUPABASE_SERVICE_KEY: str = ""
    SUPABASE_ANON_KEY: str = ""
    SUPABASE_DB_PASSWORD: str = ""
    SUPABASE_REGION: str = "eu-central-1"
    
    # Market data provider selection ("massive" | "fmp")
    MARKET_DATA_PROVIDER: str = "massive"

    # FMP API (fallback provider). Optional: startup must not require an FMP
    # key when MARKET_DATA_PROVIDER=massive (the factory validates at use time).
    FMP_API_KEY: str = ""
    FMP_BASE_URL: str = "https://financialmodelingprep.com/api/v3"
    FMP_MAX_CONCURRENT: int = 10
    FMP_RATE_LIMIT_PER_MIN: int = 250

    # Massive API (primary provider)
    MASSIVE_API_KEY: str = ""
    MASSIVE_BASE_URL: str = "https://api.massive.com"
    MASSIVE_REQUESTS_PER_MINUTE: int = 5   # Massive Basic plan
    MASSIVE_PROFILE_CACHE_DAYS: int = 7    # ticker-details (market cap) cache

    # Phase 7A: a 'running' market-data job whose last update is older than
    # this is considered stale (crashed process) and auto-failed so it never
    # blocks new work.
    MARKET_DATA_JOB_STALE_MINUTES: int = 30

    # Universe eligibility (Massive reference data). Classification uses the
    # provider's type/exchange fields, never ticker suffixes.
    UNIVERSE_ALLOWED_EXCHANGES: List[str] = ["XNAS", "XNYS", "XASE"]  # MIC codes
    UNIVERSE_ALLOWED_SECURITY_TYPES: List[str] = ["CS"]  # common stock
    UNIVERSE_INCLUDE_OTC: bool = False

    # Cheap local pre-screen (before any per-ticker detail calls). Dollar volume
    # is computed locally as close * volume (documented in screening.py).
    PRESCREEN_MIN_PRICE: float = 1.0
    PRESCREEN_MIN_VOLUME: float = 100_000
    PRESCREEN_MIN_DOLLAR_VOLUME: float = 1_000_000
    
    # Worker settings
    WORKER_TOKEN: str
    REQUIRE_WORKER_TOKEN: bool = False
    ENABLE_SCHEDULER: bool = True

    # Audit-only mode (Deployment Readiness). When true, the app exposes ONLY a
    # narrow read-only allowlist (revision/liveness + the shadow-cohort audit
    # routes); every other API and docs route is rejected before its handler
    # runs, and no scheduler/background work may start. Default false keeps
    # local, test and normal deployments completely unchanged.
    AUDIT_ONLY_MODE: bool = False

    # Explicit read-only audit database identity. A COMPLETE PostgreSQL DSN
    # (postgresql://<role>:<password>@<host>:<port>/<db>?sslmode=require) used
    # ONLY when AUDIT_ONLY_MODE=true. It supplies the custom least-privilege
    # role directly — the legacy Supabase-derived username cannot. SECRET:
    # never logged, never returned by any endpoint, never in .env.example with a
    # real value. Empty by default; supplied via `fly secrets set`.
    AUDIT_DATABASE_URL: str = ""
    # The PostgreSQL role the audit connection MUST authenticate as. Required
    # once AUDIT_DATABASE_URL is set in audit mode; the access-check refuses
    # readiness if current_user differs or is broader.
    AUDIT_EXPECTED_DB_ROLE: str = ""

    # Maintenance-only mode (Shadow Outcome Maintenance Environment). When true,
    # the app exposes ONLY the maintenance allowlist (revision/liveness + the
    # three shadow-maintenance routes) and connects through a dedicated
    # least-privilege WRITE-capable role — never the audit reader, never the
    # default Supabase identity. Mutually exclusive with AUDIT_ONLY_MODE and
    # incompatible with ENABLE_SCHEDULER=true. Default false leaves local, test
    # and normal deployments completely unchanged.
    MAINTENANCE_ONLY_MODE: bool = False
    # Explicit maintenance database identity. A COMPLETE PostgreSQL DSN used ONLY
    # when MAINTENANCE_ONLY_MODE=true (fail closed if absent — never falls back
    # to AUDIT_DATABASE_URL or the Supabase-derived identity). SECRET: never
    # logged, never returned by any endpoint, never committed. Supplied via
    # `fly secrets set`.
    MAINTENANCE_DATABASE_URL: str = ""
    # The PostgreSQL role the maintenance connection MUST authenticate as
    # (current_user must equal this). Required once MAINTENANCE_DATABASE_URL is
    # set in maintenance mode.
    MAINTENANCE_EXPECTED_DB_ROLE: str = ""
    # The single experiment + cohort scope maintenance execution is locked to.
    MAINTENANCE_ALLOWED_EXPERIMENT_CODE: str = "wyckoff_v2_vs_baseline"
    MAINTENANCE_ALLOWED_COHORT_SCOPE: str = "campaign"
    # Hard cap on a single bounded execution batch (never exceeded server-side).
    MAINTENANCE_MAX_BATCH_SIZE: int = 10
    # Server-enforced minimum wall-clock interval between provider-backed
    # maintenance batches. The Massive Basic plan allows ~5 requests/minute, so
    # a second batch started before this window clears is throttled (429) and
    # every pair fails retryably. Default 75s; floored to 60s whenever
    # maintenance mode is active on the Massive provider (the rolling request
    # window), clamped to a 600s maximum. Not sensitive: may be overridden via
    # Fly runtime configuration, needs no secret treatment. Ignored outside
    # maintenance mode.
    MAINTENANCE_MIN_BATCH_INTERVAL_SECONDS: int = 75

    # History-Warmup-only mode (4H/daily local-warmup foundation). When true the
    # app exposes ONLY liveness/version + the read-only history-warmup foundation
    # routes (access-check + preflight) and connects as the dedicated
    # least-privilege smart_scanner_history_warmer role. Mutually exclusive with
    # AUDIT_ONLY_MODE and MAINTENANCE_ONLY_MODE; incompatible with
    # ENABLE_SCHEDULER=true. Default false leaves all deployments unchanged.
    # NOTE: this task adds NO provider-backed execute route under this mode.
    HISTORY_WARMUP_ONLY_MODE: bool = False
    # Explicit history-warmup database identity. A COMPLETE PostgreSQL DSN used
    # ONLY when HISTORY_WARMUP_ONLY_MODE=true (fail closed if absent — never
    # falls back to AUDIT_DATABASE_URL, MAINTENANCE_DATABASE_URL or the
    # Supabase-derived default identity). It supplies the dedicated
    # least-privilege smart_scanner_history_warmer role directly. SECRET: never
    # logged, never returned by any endpoint, never committed. Supplied via
    # `fly secrets set` on the dedicated (isolated) warmup app only.
    HISTORY_WARMUP_DATABASE_URL: str = ""
    # The PostgreSQL role the history-warmup connection must authenticate as
    # (current_user must equal this). Required once HISTORY_WARMUP_DATABASE_URL
    # is set in warmup mode; the access-check refuses readiness otherwise.
    HISTORY_WARMUP_EXPECTED_DB_ROLE: str = "smart_scanner_history_warmer"
    # Hard cap on symbols per bounded warmup-execute batch. The first pilot is
    # strictly ONE symbol; the server selects it (the client can never widen a
    # batch). Never exceeded server-side.
    HISTORY_WARMUP_MAX_SYMBOLS_PER_BATCH: int = 1
    # Server-enforced minimum wall-clock interval between provider-backed warmup
    # execute batches, persisted via history_warmup_runs (survives restart /
    # auto-stop / token rotation). One symbol costs ~1 daily + 1 4H (+ up to 2
    # benchmark) provider requests plus the client's bounded retries; on the
    # Massive Basic ~5 req/min plan a second batch inside the rolling minute
    # window would be throttled (429), so the default holds a full window (75s,
    # floored to 60s in warmup mode on Massive). Not sensitive.
    HISTORY_WARMUP_MIN_BATCH_INTERVAL_SECONDS: int = 75
    # Server-controlled spacing between the individual provider requests WITHIN a
    # single symbol's warmup (daily then 4H). Belt-and-braces on top of the
    # provider client's own rolling limiter; keeps a symbol's 2-3 calls under the
    # 5/min window. Applied OUTSIDE any DB lock/transaction. 0 disables (tests).
    HISTORY_WARMUP_PROVIDER_REQUEST_SPACING_SECONDS: int = 15
    # Durable execution-lease TTL. While a run is 'running' and its lease has NOT
    # expired, an identical request is bounded-in-progress and a different one is
    # execution_locked. Once the lease expires the run is treated as abandoned and
    # is re-drivable / reconcilable per the documented rules — crash recovery
    # never leaves a permanent blocker. Sized to comfortably cover one symbol's
    # 2 provider calls + bounded client retries + spacing.
    HISTORY_WARMUP_EXECUTION_LEASE_SECONDS: int = 120
    # Hard cap on a frozen warmup universe's symbol membership.
    HISTORY_WARMUP_MAX_UNIVERSE_SYMBOLS: int = 100

    # Prospective-campaign-only mode (frozen-universe local evaluation). When true
    # the app exposes ONLY liveness/version + the prospective access-check /
    # preflight / register / execute / audit routes and connects as the dedicated
    # least-privilege smart_scanner_prospective_runner role. Mutually exclusive
    # with AUDIT/MAINTENANCE/HISTORY_WARMUP modes; incompatible with
    # ENABLE_SCHEDULER=true. Constructs NO provider (local daily_bars +
    # market_bars_4h only). Default false leaves all deployments unchanged.
    PROSPECTIVE_CAMPAIGN_ONLY_MODE: bool = False
    # Explicit prospective database identity. A COMPLETE PostgreSQL DSN used ONLY
    # when PROSPECTIVE_CAMPAIGN_ONLY_MODE=true (fail closed if absent — never
    # falls back to audit/maintenance/warmup/Supabase-default). Supplies the
    # dedicated smart_scanner_prospective_runner role. SECRET; Fly secret only.
    PROSPECTIVE_DATABASE_URL: str = ""
    # The PostgreSQL role the prospective connection must authenticate as.
    PROSPECTIVE_EXPECTED_DB_ROLE: str = "smart_scanner_prospective_runner"
    # The single experiment the prospective app is locked to.
    PROSPECTIVE_ALLOWED_EXPERIMENT_CODE: str = "wyckoff_v2_vs_baseline"
    PROSPECTIVE_EXPERIMENT_CONTRACT_VERSION: str = "wyckoff_v2_prospective_experiment.v1"
    # Durable execution-lease TTL for a prospective campaign execute.
    PROSPECTIVE_EXECUTION_LEASE_SECONDS: int = 300
    # STABLE campaign-cohort membership lock (a sha256:... value). Required for
    # ready_for_maintenance_execution; compared against the recomputed cohort
    # lock hash (NEVER the dynamic remaining hash). Empty by default; installed
    # as a Fly secret / runtime value after independent audit verification.
    MAINTENANCE_LOCKED_COHORT_HASH: str = ""

    # ---------------------------------------------------------------------
    # External-intelligence ingress (External Intelligence Hub V1,
    # migration 022). The FIRST internet-facing write path in this codebase.
    #
    # It cannot share the Product API app: that one runs AUDIT_ONLY_MODE with a
    # GET-only allowlist and connects as a role whose sessions are
    # default_transaction_read_only. A webhook is a POST that INSERTs, so it
    # gets its own bounded mode, its own Fly app and its own least-privilege
    # role — the same isolation pattern as audit / maintenance / warmup /
    # prospective.
    # ---------------------------------------------------------------------
    # When true the app exposes ONLY liveness/version + the external-signal
    # ingress routes, and never runs the scheduler. Mutually exclusive with
    # every other bounded mode. Default false leaves all deployments unchanged.
    EXTERNAL_INGEST_ONLY_MODE: bool = False
    # Explicit ingress database identity: a COMPLETE PostgreSQL DSN
    # authenticating as smart_scanner_external_ingest (fail closed if absent —
    # never falls back to the audit, maintenance, warmup, prospective or
    # default identity). SECRET; Fly secret only.
    EXTERNAL_INGEST_DATABASE_URL: str = ""
    EXTERNAL_INGEST_EXPECTED_DB_ROLE: str = "smart_scanner_external_ingest"
    # The shared ingress credential a third party must present. SECRET.
    #
    # It travels in the `X-Smart-Scanner-Token` header when the caller can set
    # headers, and in the `?token=` query parameter when it cannot — TradingView
    # webhooks send a fixed body to a fixed URL with no custom headers, so a
    # URL-borne credential is the only mechanism the platform offers. It is
    # NEVER accepted from the request BODY: an alert message is user-editable
    # text that gets pasted into support threads and screenshots.
    #
    # Empty by default, and the verifier fails CLOSED on an empty expected
    # value — a deployment that forgot this secret rejects everything rather
    # than accepting everyone.
    EXTERNAL_INGEST_TOKEN: str = ""
    # Per-process sliding window (see app/external_ingest.py on why in-process
    # is honest here). A TradingView alert fires at most once per bar close.
    EXTERNAL_INGEST_RATE_LIMIT_PER_MINUTE: int = 60
    # Hard body ceiling, enforced BEFORE the JSON parser runs.
    EXTERNAL_INGEST_MAX_PAYLOAD_BYTES: int = 8192
    # How far a source's own timestamp may sit from arrival before the delivery
    # is refused. This is the replay window; see app/external_adapters.py.
    EXTERNAL_INGEST_MAX_CLOCK_SKEW_SECONDS: int = 1800
    # OPTIONAL comma-separated source-IP allowlist, OFF by default.
    #
    # TradingView publishes the fixed addresses its webhooks come from
    # (52.89.214.238, 34.212.75.30, 54.218.53.128, 52.32.178.7 as documented at
    # the time of writing), so an operator may pin them for defence in depth.
    # It stays off by default deliberately: pinning a third party's published
    # addresses becomes an outage the day they change them without notice. The
    # token is the security boundary; this is a bonus, not a replacement.
    # Not sensitive.
    EXTERNAL_INGEST_ALLOWED_IPS: str = ""

    # ---------------------------------------------------------------------
    # Durable PostgreSQL-backed job queue + dedicated worker (migration 018).
    # PostgreSQL is the ONLY queue source of truth (no Redis/Celery/broker).
    # The queue framework is generic; only the prospective symbol-evaluation
    # task type is live-enabled in this task. These are non-secret runtime
    # knobs (safe to set via Fly env); the worker's database identity is a
    # SECRET DSN supplied separately (JOB_WORKER_DATABASE_URL).
    # ---------------------------------------------------------------------
    # The dedicated worker process (`python -m app.jobs.worker`) is enabled only
    # on the isolated worker Machine; the web app never runs it. Default false so
    # every existing deployment and test is unchanged.
    JOB_WORKER_ENABLED: bool = False
    JOB_WORKER_TYPE: str = "prospective"
    # Comma-separated queue names the worker claims from. The prospective queue
    # is the only live one now.
    JOB_WORKER_QUEUES: str = "prospective"
    # Concurrency 1 initially: one CPU-bound strategy evaluation at a time.
    JOB_WORKER_CONCURRENCY: int = 1
    # The scheduler leader loop runs INSIDE the worker parent process (never the
    # web app). Guarded by a Postgres advisory lock so exactly one leader acts.
    # No schedule is enabled in this task, so an enabled scheduler enqueues
    # nothing until an operator enables a schedule.
    JOB_SCHEDULER_ENABLED: bool = True
    # Explicit worker database identity: a COMPLETE PostgreSQL DSN authenticating
    # as the least-privilege smart_scanner_prospective_worker role. SECRET; the
    # worker fails closed if absent. Never falls back to the API/default identity.
    JOB_WORKER_DATABASE_URL: str = ""
    # The PostgreSQL role the worker connection MUST authenticate as.
    JOB_WORKER_EXPECTED_DB_ROLE: str = "smart_scanner_prospective_worker"
    # Lease / heartbeat / poll / retry defaults (seconds unless noted). The lease
    # comfortably covers one real Wyckoff-v2 symbol evaluation on shared CPU.
    JOB_TASK_LEASE_SECONDS: int = 900
    JOB_WORKER_HEARTBEAT_SECONDS: int = 15
    JOB_TASK_HEARTBEAT_SECONDS: int = 30
    JOB_QUEUE_POLL_SECONDS: int = 2
    JOB_MAX_ATTEMPTS_DEFAULT: int = 3
    # Bounded exponential backoff between attempts (seconds): attempt 1 -> 60,
    # attempt 2 -> 300, then terminal. Deterministic (jitter only under a test
    # hook) so crash-recovery tests are reproducible.
    JOB_RETRY_BACKOFF_SECONDS: List[int] = [60, 300]
    # Daily-pipeline driver: within ONE claim it advances the occurrence and, when
    # a stage is waiting on asynchronous durable work (a running campaign/outcome
    # job, a history cooldown), it sleeps DAILY_PIPELINE_DRIVER_POLL_SECONDS and
    # re-checks — up to DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS (the worker renews
    # the task lease throughout). Beyond that ceiling it defers (one retry). Tests
    # set the ceiling to 0 so a single advance either finishes or defers at once.
    DAILY_PIPELINE_DRIVER_MAX_WAIT_SECONDS: int = 10800   # 3h (a slow 25-symbol eval)
    DAILY_PIPELINE_DRIVER_POLL_SECONDS: int = 30
    # Per-handler retry backoff for the driver task (see registry HandlerSpec.
    # retry_backoff_schedule). The GLOBAL JOB_RETRY_BACKOFF_SECONDS list has only
    # two entries, which caps ANY task at 3 attempts regardless of max_attempts;
    # the driver legitimately DEFERS ("occurrence_in_progress") many times while
    # the pipeline's async children (history refresh, campaign, outcomes) run, so
    # it needs a schedule long enough to use its max_attempts=10 budget. Length 9
    # → 10 attempts total (the 10th failure is always terminal). Modest, bounded
    # waits between defers (the internal-wait loop already absorbs up to ~3h per
    # claim); this is NOT infinite retry — attempt 10 is a hard ceiling.
    DAILY_PIPELINE_DRIVER_BACKOFF_SECONDS: List[int] = [60, 120, 300, 300, 600, 600, 900, 900, 1800]
    # History-refresh task: the history-warmup service enforces a SHARED execution
    # cooldown / advisory lock, so a serially-claimed symbol often gets a KNOWN
    # transient 409 while the window from the previous symbol is still active. The
    # handler absorbs that 409 with a BOUNDED in-task wait (sleep the
    # server-indicated Retry-After + a small margin, recompute state, retry within
    # the SAME claim) instead of burning a queue-level attempt. MAX_WAIT caps the
    # total in-task wait (covers the resolved cooldown interval + lease); POLL is
    # the fallback when a lock 409 carries no numeric hint. This is bounded — it
    # never spins, and defers to a queue retry once the ceiling is reached.
    HISTORY_REFRESH_TASK_MAX_WAIT_SECONDS: int = 1800   # 30m ceiling per claim
    HISTORY_REFRESH_TASK_COOLDOWN_MARGIN_SECONDS: int = 3
    HISTORY_REFRESH_TASK_POLL_SECONDS: int = 5
    # A worker whose last heartbeat is older than this is considered stale/dead
    # (its leased/running tasks become eligible for lease-expiry reconciliation).
    JOB_WORKER_STALE_SECONDS: int = 90
    # The scheduler-leader advisory lock key (distinct from the prospective
    # execute lock 0x50524F53). "JBSC" = 0x4A425343.
    JOB_SCHEDULER_ADVISORY_LOCK_KEY: int = 0x4A425343
    # Test-only synthetic task handlers (controlled retry/crash tests) are
    # selectable ONLY when this is true. Default false → never in production /
    # never on the live worker. The live worker app never sets this.
    JOB_ALLOW_TEST_HANDLERS: bool = False

    SCAN_BATCH_SIZE: int = 150
    SCAN_TIMES: List[str] = ["10:00", "14:00", "18:00"]  # UTC times
    
    # CORS
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:3001", 
        "https://*.vercel.app"
    ]
    
    # Debug flags
    DEBUG_SAVE_AVOID: bool = False
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
