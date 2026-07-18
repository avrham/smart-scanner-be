# New Supabase Project Setup

This guide sets up the Smart Scanner on a **fresh Supabase project** (new account /
private email). It covers env configuration, running migrations in order, and
verifying the app without triggering paid FMP scans.

> Do not reuse or restore the old paused project. Do not commit any `.env` /
> `.env.local`. Do not paste real secrets into this doc or into git.

---

## 1. Create a new Supabase project

1. Sign in to Supabase with your private email.
2. Create a new project. Choose a region close to you and record it (you'll need
   it for `SUPABASE_REGION`, e.g. `eu-central-1`).
3. Set a strong database password when prompted and store it in a password
   manager — this is `SUPABASE_DB_PASSWORD`.

## 2. Copy API URL and keys

In the new project: **Project Settings → API**.

| Value in Supabase | Env var (backend) | Env var (frontend) |
|---|---|---|
| Project URL (`https://<ref>.supabase.co`) | `SUPABASE_URL` | `NEXT_PUBLIC_SUPABASE_URL` |
| `anon` `public` key | `SUPABASE_ANON_KEY` | `NEXT_PUBLIC_SUPABASE_ANON_KEY` |
| `service_role` key (secret) | `SUPABASE_SERVICE_KEY` | — (never expose to browser) |

> Note the backend var is `SUPABASE_SERVICE_KEY` (not `SUPABASE_SERVICE_ROLE_KEY`).

## 3. Copy the database connection details

This backend does **not** use a single `DATABASE_URL`. It builds the Postgres
DSN at runtime (`app/deps.py`) from three values:

- `SUPABASE_URL` — the project ref (subdomain) is parsed from this.
- `SUPABASE_REGION` — used to form the pooler host `aws-0-<region>.pooler.supabase.com`.
- `SUPABASE_DB_PASSWORD` — the database password from step 1.

It then tries, in order: pooler `:6543`, pooler `:5432`, then the direct
`db.<ref>.supabase.co:5432` host. Confirm the region under
**Project Settings → Database → Connection pooling** matches `SUPABASE_REGION`.

## 4. Create local env files from the templates

**Backend** (`smart-scanner-be/`):

```bash
cd smart-scanner-be
cp .env.example .env
# then edit .env and fill in real values (see tables above)
```

Required backend values: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`,
`SUPABASE_ANON_KEY`, `SUPABASE_DB_PASSWORD`, `SUPABASE_REGION` (if not the
default), `FMP_API_KEY`, `WORKER_TOKEN`.

**Frontend** (`smart-scanner-ui/`):

```bash
cd smart-scanner-ui
cp .env.example .env.local
# then edit .env.local and fill in real values
```

Required frontend values: `NEXT_PUBLIC_SUPABASE_URL`,
`NEXT_PUBLIC_SUPABASE_ANON_KEY`, `NEXT_PUBLIC_API_BASE_URL`, and `WORKER_TOKEN`
(must match the backend's `WORKER_TOKEN`).

> `.env` and `.env.local` are gitignored. Only `.env.example` is committed.

## 5. Run migrations in order

Open **Supabase → SQL Editor** on the new project and run these files **in this
exact order**:

1. `app/db/migrations/001_initial_schema.sql` — creates all tables
   (`patterns`, `pattern_configs`, `signals`, `pattern_runs`, `tickers`,
   `daily_seen`, ...).
2. `app/db/migrations/002_phase1_sma150_config.sql` — tightens and makes the
   `sma150_bounce` config authoritative (Phase 1 / B12). Safe to re-run
   (`ON CONFLICT ... DO UPDATE`).

Paste the full contents of each file into the SQL editor and run it. Confirm no
errors.

### DB-only verification (no secrets, no app needed)

```sql
-- Expect 10 rows with the tightened Phase 1 values.
SELECT key, value
FROM public.pattern_configs
WHERE pattern_code = 'sma150_bounce'
ORDER BY key;

-- Expect the two new keys the evaluator reads to be present (result = 2).
SELECT count(*) AS new_keys_present
FROM public.pattern_configs
WHERE pattern_code = 'sma150_bounce'
  AND key IN ('score_threshold', 'min_price');
```

## 6. Start the backend and verify health

```bash
cd smart-scanner-be
# install deps into a venv (system site-packages lets you reuse pandas/numpy):
python3 -m venv --system-site-packages .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Then, in another terminal:

```bash
# Both must return {"status":"healthy","database":"connected","version":"1.1.0"}
curl -s http://localhost:8000/health   | jq .
curl -s http://localhost:8000/api/health | jq .
```

If `database` is `disconnected`, re-check `SUPABASE_URL`, `SUPABASE_REGION`, and
`SUPABASE_DB_PASSWORD` (the DSN is derived from these).

## 7. Verify the DB-backed pattern config is served

```bash
curl -s http://localhost:8000/api/patterns \
  | jq '.[] | select(.code=="sma150_bounce") | .config'
```

Expect the tightened values (including `score_threshold` and `min_price`),
which confirms the app reads the migrated DB rows rather than falling back to
in-code defaults. In the backend logs you should see
`Loaded DB config for pattern 'sma150_bounce' (overrides: ...)` and **not**
`using safe defaults`.

## 8. Start the frontend

```bash
cd smart-scanner-ui
npm install
npm run dev
# open http://localhost:3000
```

The Settings/health view should report healthy (it calls `/api/health`), and
the patterns page should show the `sma150_bounce` config.

## 9. Do NOT run full scans yet

- Scans call **paid** FMP endpoints. Only run a scan once `FMP_API_KEY` is set
  and you explicitly choose to.
- Keep `ENABLE_SCHEDULER=false` while validating setup if you want to avoid the
  in-process scheduler triggering scans on its own; set it back to `true` for
  normal operation.
- When ready, start with the **smallest** possible manual scan (a single known
  symbol) before any full-universe run.

---

## Env var quick reference (exact names used in code)

### Backend (`app/config.py`, `app/deps.py`, `main.py`)

| Var | Required | Default | Purpose |
|---|---|---|---|
| `SUPABASE_URL` | yes | — | Project URL; project ref parsed for DB DSN |
| `SUPABASE_SERVICE_KEY` | yes | — | service_role key (server-side) |
| `SUPABASE_ANON_KEY` | yes | — | anon/public key |
| `SUPABASE_DB_PASSWORD` | yes | — | Postgres password (DSN) |
| `SUPABASE_REGION` | no | `eu-central-1` | Pooler host region |
| `FMP_API_KEY` | yes (for scans) | — | Market data key |
| `FMP_BASE_URL` | no | FMP v3 URL | FMP base URL |
| `FMP_MAX_CONCURRENT` | no | `10` | FMP concurrency |
| `FMP_RATE_LIMIT_PER_MIN` | no | `250` | FMP rate limit |
| `WORKER_TOKEN` | yes | — | Admin/scan auth token |
| `REQUIRE_WORKER_TOKEN` | no | `false` | Enforce token |
| `ENABLE_SCHEDULER` | no | `true` | In-process scheduler |
| `SCAN_BATCH_SIZE` | no | `150` | Scan batch size |
| `SCAN_TIMES` | no | `["10:00","14:00","18:00"]` | Scheduled UTC times |
| `ENVIRONMENT` | no | `development` | Enables uvicorn reload |
| `DEBUG` | no | `true` | Debug flag |
| `LOG_LEVEL` | no | `INFO` | Logging |
| `LOG_FORMAT` | no | `json` | Logging format |
| `DEBUG_SAVE_AVOID` | no | `false` | Debug persistence |
| `ALLOWED_ORIGINS` | no | localhost + vercel | CORS |

> There is intentionally **no `DATABASE_URL`** — the DSN is derived at runtime.

### Frontend (`next.config.js`, `lib/supabase.ts`, `lib/api.ts`, `app/api/admin/scan/start/route.ts`)

| Var | Required | Default | Purpose |
|---|---|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | yes | — | Supabase project URL (browser) |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | yes | — | anon key (browser) |
| `NEXT_PUBLIC_API_BASE_URL` | no | `http://localhost:8000` | Backend base URL |
| `WORKER_TOKEN` | yes (to trigger scans) | — | Server-side scan auth; must match backend |
| `NEXT_PUBLIC_WORKER_TOKEN` | no | — | Fallback for the above (avoid; exposed to browser) |
