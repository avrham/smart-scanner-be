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
