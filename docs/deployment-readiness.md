# Deployment Readiness — Build Provenance

Provider-neutral procedure for proving which source revision a running backend
is executing. No hosting provider is chosen or configured here; this only makes
any future deployment *verifiable*.

The core rule: **a generic `GET /health` HTTP 200 is NOT proof of revision.**
Revision proof comes only from `GET /version`, whose `git_sha` is embedded at
build time.

## 1. Required build metadata

Embedded at build/release time (never derived by running git inside the
container at runtime). All optional; each defaults to a safe local value.

| Variable          | Meaning                                   | Default   |
|-------------------|-------------------------------------------|-----------|
| `APP_GIT_SHA`     | full source commit SHA (7–64 hex chars)   | `unknown` |
| `APP_BUILD_TIME`  | ISO 8601 UTC build timestamp              | `unknown` |
| `APP_ENVIRONMENT` | `local` / `development` / `staging` / `production` | `local` |
| `APP_RELEASE`     | human/image release identifier            | `unknown` |

An `APP_GIT_SHA` that is not a 7–64 char hex string (e.g. `latest`, a branch
name, empty) is reported as `unknown` — a misleading value is never presented
as a trusted revision.

## 2. Local Docker build

```bash
CURRENT_SHA="$(git rev-parse HEAD)"
SHORT_SHA="$(git rev-parse --short HEAD)"
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RELEASE="smart-scanner-be-$SHORT_SHA"

docker build \
  --build-arg APP_GIT_SHA="$CURRENT_SHA" \
  --build-arg APP_BUILD_TIME="$BUILD_TIME" \
  --build-arg APP_RELEASE="$RELEASE" \
  --build-arg APP_ENVIRONMENT="staging" \
  -t "smart-scanner-be:$SHORT_SHA" .
```

Do not hardcode a SHA; always pass it from `git rev-parse HEAD` at build time.

## 3. Local Docker run (safe local config only)

Use a throwaway/local env file — never the production Supabase project, never
real Massive/FMP credentials, and keep the scheduler off for a verification
run:

```bash
docker run --rm -p 8000:8000 \
  -e ENVIRONMENT=local \
  -e ENABLE_SCHEDULER=false \
  -e REQUIRE_WORKER_TOKEN=false \
  -e WORKER_TOKEN=local-only \
  -e SUPABASE_URL=https://local.invalid \
  -e SUPABASE_SERVICE_KEY=local -e SUPABASE_ANON_KEY=local \
  -e SUPABASE_DB_PASSWORD=local \
  "smart-scanner-be:$SHORT_SHA"
```

`GET /version` works even with no database reachable (it has no DB or provider
dependency). `GET /health` will report `database: disconnected` against a fake
DB — that is expected and does not affect revision proof.

## 4. Request the revision endpoint

```bash
curl -sS http://localhost:8000/version | jq .
```

Example response (values are illustrative, never hardcoded):

```json
{
  "service": "smart-scanner-be",
  "application_version": "1.1.0",
  "git_sha": "<full sha>",
  "git_sha_short": "<7 chars>",
  "build_time": "<iso 8601 utc>",
  "environment": "staging",
  "release": "smart-scanner-be-<short>"
}
```

## 5. Expected relationship

```text
/version.git_sha == git rev-parse HEAD
```

```bash
test "$(curl -sS http://localhost:8000/version | jq -r .git_sha)" \
     = "$(git rev-parse HEAD)" && echo "revision proven" || echo "MISMATCH"
```

## 6. Inspect OCI image labels

```bash
docker image inspect "smart-scanner-be:$SHORT_SHA" \
  --format '{{ json .Config.Labels }}' | jq .
```

Expect:

```text
org.opencontainers.image.title    == smart-scanner-be
org.opencontainers.image.version  == 1.1.0
org.opencontainers.image.revision == <full sha>   (== git rev-parse HEAD)
org.opencontainers.image.created  == <build time>
```

Runtime env inside the image also carries `APP_GIT_SHA` / `APP_BUILD_TIME` /
`APP_RELEASE` / `APP_ENVIRONMENT`:

```bash
docker run --rm --entrypoint printenv "smart-scanner-be:$SHORT_SHA" APP_GIT_SHA
```

## 7. How a future hosting provider must supply metadata

Whichever provider is later chosen (Fly.io, Render, Railway, a container
registry + orchestrator, …), it MUST either:

* pass the four `--build-arg` values shown in §2 at image build time (CI knows
  the SHA it is building), **or**
* set the four `APP_*` runtime environment variables to the exact built
  revision if it builds the image separately.

The value passed MUST be the commit the image was actually built from. Do not
set it from a mutable tag such as `latest`.

## 8. Deployment verification before any production database audit

Before running any read-only production audit (e.g. the cohort closeout), prove
the deployed revision:

1. Deploy the image built from the intended commit.
2. `GET /version` on the deployed environment.
3. Confirm `git_sha` equals the intended commit (or a descendant that contains
   it — verify with `git merge-base --is-ancestor <intended> <deployed_sha>`).
4. Record: environment name, backend app name, deployed `git_sha`, `build_time`,
   `release`, and the verification method.

## 9. `/health` is not revision proof

`GET /health` reports liveness + DB connectivity and now includes a small
`revision` (short SHA) convenience field, but an HTTP 200 from `/health` on its
own does **not** prove the running revision. Always use `/version.git_sha` for
revision proof.

## 10. Cohort closeout gate

Do **not** call `GET /api/admin/shadow-cohort/closeout` (or any maturation
endpoint) until `/version.git_sha` on the target environment is proven to equal
commit `f6a6bd5652470f6e96e0a02432e454afe0ceb851`, or a descendant commit that
contains it. Until that proof exists, the runtime cohort state is unverifiable
and no production audit should be run.

## 11. Fly.io revision-verification staging (provider-specific)

A minimal, **verification-only** Fly.io app (`fly.toml` in the repo root) that
boots an exact committed revision and exposes `/version`. It runs **no
scheduler and no background processing**, uses a **non-production placeholder
database** and **no live provider credentials**, and is safe to remove.

Key `fly.toml` properties: Dockerfile build; `internal_port = 8000`;
`force_https`; a single `app` web process; `auto_stop_machines`/
`min_machines_running = 0` (scales to zero when idle); **health check on
`/version`** (not `/health`, which legitimately reports the placeholder DB
disconnected); no release command, no volume, no database/Redis resource. The
`[env]` block hard-disables background work:

```toml
[env]
  APP_ENVIRONMENT = 'staging'
  ENABLE_SCHEDULER = 'false'
  REQUIRE_WORKER_TOKEN = 'true'
```

Secrets are set once via `fly secrets set` (never committed) using
**non-production** placeholders that cannot resolve to production:

```bash
# staging-only, non-production placeholders (never real credentials)
fly secrets set -a smart-scanner-be-staging \
  SUPABASE_URL="https://staging-placeholder.invalid" \
  SUPABASE_SERVICE_KEY="staging-placeholder" \
  SUPABASE_ANON_KEY="staging-placeholder" \
  SUPABASE_DB_PASSWORD="staging-placeholder" \
  WORKER_TOKEN="$(openssl rand -hex 24)"   # generated; never printed/committed
```

Deploy the EXACT HEAD revision (SHA supplied at build time, never hardcoded):

```bash
CURRENT_SHA="$(git rev-parse HEAD)"; SHORT_SHA="$(git rev-parse --short HEAD)"
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; RELEASE="smart-scanner-be-$SHORT_SHA"
fly deploy -a smart-scanner-be-staging \
  --build-arg APP_GIT_SHA="$CURRENT_SHA" \
  --build-arg APP_BUILD_TIME="$BUILD_TIME" \
  --build-arg APP_RELEASE="$RELEASE" \
  --build-arg APP_ENVIRONMENT="staging"
```

Verify: `curl -sS https://smart-scanner-be-staging.fly.dev/version | jq -r .git_sha`
must equal `git rev-parse HEAD`. `/health` is not revision proof (§9). Remove
when done: `fly apps destroy smart-scanner-be-staging`.

## 12. Read-only cohort-audit access model (production connection)

Before connecting the audit environment to real data, the connected identity
must be a dedicated least-privilege PostgreSQL role — never the production
`postgres`/service role.

### Connection model (as built in `app/deps.py`)

* The app connects to Postgres via **asyncpg** using DSN candidates in order:
  Supabase Supavisor **pooler** `aws-0-<region>.pooler.supabase.com:6543` and
  `:5432` (username `postgres.<project_ref>`), then the **direct** host
  `db.<project_ref>.supabase.co:5432` (username `postgres`).
* The **only** DB credentials consumed by the audit path are `SUPABASE_URL`
  (parsed for the project ref + host), `SUPABASE_REGION` and
  `SUPABASE_DB_PASSWORD` (the Postgres login password).
* `SUPABASE_SERVICE_KEY` and `SUPABASE_ANON_KEY` are Supabase **HTTP API** keys
  and are **not used** by the closeout/access-check DB path (asyncpg only). The
  Supabase HTTP API is not involved.
* The audit can therefore operate with **only a dedicated PostgreSQL login**
  (username + password + host). To point the login at the dedicated role,
  the username derivation would use `<role>.<project_ref>` for the pooler or
  `<role>` for the direct host — set via the eventual real DB credential.
* **Transaction pooling caveat:** on the `:6543` transaction pooler, session
  `SET`s may not persist across statements. That is why the dedicated role sets
  `default_transaction_read_only=on` and the timeouts at the **role** level
  (`ALTER ROLE ... SET`) — those hold regardless of pooling. The app's own
  read-only transaction on `access-check` is defense-in-depth.

### Settings required only because `Settings()` validates them

`SUPABASE_SERVICE_KEY`, `SUPABASE_ANON_KEY`, `WORKER_TOKEN` (for import;
`WORKER_TOKEN` is also used to protect the audit routes), and (until real access)
`SUPABASE_URL` / `SUPABASE_DB_PASSWORD` as **non-production placeholders**.

### Settings actually consumed by the audit DB path

`SUPABASE_URL`, `SUPABASE_REGION`, `SUPABASE_DB_PASSWORD` (asyncpg login) plus
`WORKER_TOKEN` (route auth). `MASSIVE_API_KEY` / `FMP_API_KEY` are never
required (no provider is constructed in audit-only mode).

### Secrets that MUST remain placeholders (this task)

`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_ANON_KEY`,
`SUPABASE_DB_PASSWORD`, `WORKER_TOKEN` (a staging-only random value). No
Massive/FMP keys.

### The one credential that will eventually be real

A single dedicated **`smart_scanner_audit_reader`** PostgreSQL login
(host + username + password) — see `ops/sql/create_shadow_audit_reader.sql`.
It grants `CONNECT` + `USAGE` on `public` + `SELECT` on exactly the 8 closeout
relations, no write privileges, `default_transaction_read_only=on`, bounded
timeouts, `NOINHERIT`, `NOBYPASSRLS`. Verify with
`ops/sql/verify_shadow_audit_reader.sql` and, at runtime, the read-only
`GET /api/admin/shadow-cohort/access-check` endpoint (worker-token protected),
whose `ready_for_closeout_audit` must be `true` before any closeout run.

### Audit-only mode

`AUDIT_ONLY_MODE=true` (set in `fly.toml`) exposes ONLY:
`GET /`, `/version`, `/api/version`, `/health`, `/api/health`,
`/api/admin/shadow-cohort/access-check`, `/api/admin/shadow-cohort/closeout`.
Every other route (mutations, provider/universe/scan, campaign create/resume,
outcome calculation, `/docs`, `/openapi.json`, `/redoc`) returns `404` before
its handler runs, even with a valid worker token. `AUDIT_ONLY_MODE=true` with
`ENABLE_SCHEDULER=true` fails startup.

## 13. Explicit audit database connection (custom least-privilege role)

### Why the legacy connection cannot authenticate the custom role

`app/deps.build_connection_dsns` derives the PostgreSQL **username** from the
Supabase project ref (`postgres.<ref>` on the pooler, `postgres` direct).
Changing only `SUPABASE_DB_PASSWORD` keeps that derived `postgres` username, so
it can never log in as `smart_scanner_audit_reader`. Audit deployments therefore
supply a COMPLETE connection identity via `AUDIT_DATABASE_URL`.

### Configuration precedence (audit-aware selector)

`init_db_pool` chooses the DSN via `app/audit_db.select_connection_plan`:

* `AUDIT_ONLY_MODE=false` → legacy Supabase-derived candidates (unchanged).
* `AUDIT_ONLY_MODE=true` + `AUDIT_DATABASE_URL` set → **only** that DSN
  (`statement_cache_size=0` so it is pooler-safe). `database_connection_mode = audit_explicit`.
* `AUDIT_ONLY_MODE=true` + no `AUDIT_DATABASE_URL` → **fail closed** (503, no
  fallback to any default identity). `database_connection_mode = audit_unconfigured`.

`AUDIT_DATABASE_URL` is a SECRET: never logged, never returned by `/version`,
`/health`, access-check, exceptions or startup logs, and never committed.

### Expected DSN structures (placeholders only)

Supavisor **session mode** (preferred for the initial Fly audit connection —
port 5432, no prepared-statement incompatibility):

```text
postgresql://smart_scanner_audit_reader.<PROJECT_REF>:<PASSWORD>@aws-0-<REGION>.pooler.supabase.com:5432/postgres?sslmode=require
```

**Direct** connection (username has no project-ref suffix):

```text
postgresql://smart_scanner_audit_reader:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require
```

* **Do NOT use transaction mode (port 6543)** unless prepared statements are
  proven disabled. The audit pool sets `statement_cache_size=0`, which makes it
  compatible with either mode, but session mode (5432) is the safe default.
* The username is the SOURCE OF TRUTH copied from the Supabase Connect panel —
  the app never suffixes the project ref itself. On the pooler the wire username
  is `smart_scanner_audit_reader.<ref>`; PostgreSQL `current_user` is still
  `smart_scanner_audit_reader`, which is what the access-check compares.
* **Percent-encode** special characters in the password (e.g. `@` → `%40`).
* Obtain the pooler host + project ref from Supabase → Project → Connect →
  "Session pooler".

### Create + verify the role

1. Create it (manual, once) on the target database:
   `psql "<admin conn>" -v audit_password="$(openssl rand -base64 24)" -v db_name=postgres -f ops/sql/create_shadow_audit_reader.sql`
2. Verify locally / against the target as the role:
   `psql "<audit-reader conn>" -f ops/sql/verify_shadow_audit_reader.sql`
   (checks read-only defaults, SELECT on the 8 relations, absence of write
   privileges, and RLS state). A local Postgres integration test
   (`tests/test_audit_db_integration.py`) proves real enforcement end-to-end.

### Wire it to staging (later; do NOT do it in this task)

```bash
fly secrets set -a smart-scanner-be-staging \
  AUDIT_DATABASE_URL="postgresql://smart_scanner_audit_reader.<ref>:<enc-pw>@aws-0-<region>.pooler.supabase.com:5432/postgres?sslmode=require"
# AUDIT_EXPECTED_DB_ROLE=smart_scanner_audit_reader is already in fly.toml.
```

Do not print the URL. Then:

* `curl -sS $HOST/version` → confirm the deployed `git_sha`.
* `curl -sS -H "X-Worker-Token: <tok>" $HOST/api/admin/shadow-cohort/access-check`
  → require `ready_for_closeout_audit == true` (identity =
  `smart_scanner_audit_reader`, read-only defaults on, SELECT-only, no elevated
  attributes). The closeout endpoint **fails closed** (409) until this is true.

### Rollback

Remove ONLY the staging secret — never auto-drop the role:

```bash
fly secrets unset -a smart-scanner-be-staging AUDIT_DATABASE_URL
```

Audit mode then returns to the fail-closed (unavailable) state.

### Prohibition

Never use the default `postgres` (or `supabase_admin` / `service_role`) role for
the audit environment. The access-check rejects readiness for any identity that
is denylisted or holds `rolsuper` / `rolcreaterole` / `rolcreatedb` /
`rolreplication` / `rolbypassrls`.

### RLS findings (per repository migrations 001/005/010/011)

None of the 8 relations enable Row Level Security (`ENABLE ROW LEVEL SECURITY` /
`CREATE POLICY` / `FORCE ROW LEVEL SECURITY` appear in no migration), and all
are owned by the migration runner. Plain `SELECT` grants are therefore
sufficient and **no** read policy is required for `smart_scanner_audit_reader`.
`BYPASSRLS` is never granted. `verify_shadow_audit_reader.sql` re-checks RLS at
apply time and fails readiness if RLS is ever enabled without a SELECT policy.
