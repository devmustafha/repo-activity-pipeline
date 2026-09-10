# Incremental Repository Activity Ingestion Pipeline

Real-time ingestion of engineering activity (commits, issues, pull requests)
from the **GitHub REST API** straight into **Postgres**, transformed with
**dbt** and orchestrated by **Airflow**. No object-storage landing zone: the
API response is the source of record and every write is an idempotent upsert,
so a replay just re-reads and rewrites.

Framed around a fictional dev-tools company that wants visibility into
engineering activity across the repositories it depends on.

The build exists to prove four things:

- **pagination** — follow the `Link: rel="next"` header, not a page counter
- **incremental loads** — a `since`-style high-water mark per (repo, resource)
- **idempotency** — re-running changes no row counts
- **rate-limit-aware retry** — respect `X-RateLimit-*`, `Retry-After`, and
  secondary limits; use conditional requests to spend no quota on unchanged data

```
GitHub REST API          Postgres (schema: github)                     dbt
┌──────────────┐ ingest ┌─────────────────────────────┐   ┌────────────────────────────┐
│ /commits     │ ─────► │ github.commits              │──►│ repo_activity_staging.     │
│ /issues      │        │ github.issues               │   │   stg_github__*            │
│ /pulls       │        │ github.pull_requests        │   │ repo_activity_marts.       │
└──────────────┘        │ github.ingestion_state (WM) │   │   mart_repo_activity_daily │
                        │ github.rejected_records     │   └────────────────────────────┘
                        └─────────────────────────────┘
        └──────────────  Airflow DAG: repo_activity (@hourly)  ──────────────┘
```

## Repository layout

| Path | What it is |
|------|------------|
| `github_activity/client.py` | Rate-limit-aware, paginating GitHub REST client |
| `github_activity/ingest.py` | Per-(repo, resource) driver: incremental pull → validated upsert → watermark advance |
| `github_activity/tests/` | `unittest` suite for the pagination / rate-limit / retry logic |
| `sql/github_schema.sql` | Idempotent DDL for the `github` schema (3 activity tables + `ingestion_state` + `rejected_records`) |
| `dbt/` | dbt project: one staging view per resource + `mart_repo_activity_daily`, plus data tests |
| `dags/repo_activity_dag.py` | Airflow DAG wiring the whole thing together |
| `Dockerfile` / `requirements-airflow.txt` | Airflow image extended with requests, psycopg2, dbt-postgres |
| `docker-compose.yml` | Local Airflow (CeleryExecutor + Postgres + Redis) |
| `test.py` | One-shot Postgres connectivity check |

## Resources and how each stays incremental

| Resource | Endpoint | Incremental strategy |
|----------|----------|----------------------|
| `commits` | `GET /repos/{repo}/commits` | API `since=` filter (committer date) |
| `issues` | `GET /repos/{repo}/issues?state=all&sort=updated&direction=asc` | API `since=` filter (`updated_at`); also returns PRs, split out by `is_pull_request` |
| `pull_requests` | `GET /repos/{repo}/pulls?state=all&sort=updated&direction=desc` | no `since` param — page newest-first and stop at the first PR older than the watermark |

`github.ingestion_state` holds one `(repo, resource) → watermark` row. Each run:

1. reads the previous watermark (or `now − GITHUB_INITIAL_LOOKBACK_DAYS` on the
   first run);
2. pulls everything changed at/after it, tracking the max timestamp seen;
3. upserts the rows (`INSERT … ON CONFLICT (natural key) DO UPDATE`);
4. **only after** the resource finishes cleanly, advances the watermark and
   stores the first-page `ETag`.

Step 4 being last is deliberate: a crash mid-resource leaves the old watermark
in place and the next run re-fetches the overlap — harmless, given the upsert.
The `since` filter is inclusive, so each run re-fetches its boundary record;
re-running the whole ingest changes no row counts. Records failing validation
go to `github.rejected_records` rather than vanishing.

### Rate-limit handling (`github_activity/client.py`)

- reads `X-RateLimit-Remaining` / `-Reset` and sleeps until the window resets
  when the quota is spent; pauses proactively when one request from exhaustion
- honours an explicit `Retry-After` header
- exponential backoff with jitter for secondary limits and 5xx
- first-page `ETag` is sent back as `If-None-Match`; an unchanged resource
  returns `304` and costs no rate-limit quota (`last_status = no_change`)

## Setup

### 1. Prerequisites

- Python 3.12, [uv](https://docs.astral.sh/uv/)
- A Postgres database (any: local, RDS, container)
- Docker + Docker Compose (for Airflow)

### 2. Configure

```bash
cp .env.example .env
# fill in DB_* and, strongly recommended, GITHUB_TOKEN
```

A classic PAT needs `public_repo`; a fine-grained token needs read-only
**Contents**, **Issues**, **Pull requests**, **Metadata** on the target repos.
Without a token you get GitHub's 60 requests/hour unauthenticated limit
(vs 5000/hour authenticated).

### 3. Install

```bash
uv sync
cd dbt && uv run dbt deps && cd ..
```

## Running it manually (outside Airflow)

```bash
# 1. Apply the github schema (idempotent)
uv run python -c "import psycopg2,os; from dotenv import load_dotenv; load_dotenv(); \
  c=psycopg2.connect(host=os.environ['DB_HOST'],dbname=os.environ['DB_NAME'],user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],sslmode=os.environ.get('DB_SSLMODE','require')); \
  c.autocommit=True; c.cursor().execute(open('sql/github_schema.sql').read()); print('schema applied')"

# 2. Ingest (incremental after the first run)
uv run python -m github_activity.ingest

# 3. Transform + test
cd dbt && DBT_PROFILES_DIR=. uv run dbt build
```

## The dbt layer

**`repo_activity_staging`** — one view per resource: light renaming, type
casting, `date()` truncations, and cheap derived fields (PR merge lead time,
issue label counts, first-line commit subject).

**`repo_activity_marts`**

| Model | Grain | Purpose |
|-------|-------|---------|
| `mart_repo_activity_daily` | repo × day | Commit volume, issue open/close flow, PR open/merge flow, contributor counts, on one date spine |

> Completeness caveat: counts for a day are only as complete as the incremental
> pull that produced them. A day close to "now" can still gain rows on the next
> run; historical days are stable once their activity falls outside every future
> `since` window.

### Data tests

`dbt build` runs not-null / accepted-values checks on every staging view, plus
`unique_combination_of_columns` on each table's natural key and on the mart grain.

## Tests

```bash
uv run python -m unittest discover -s github_activity/tests -t .
```

Nine tests cover primary/secondary rate-limit handling, `Retry-After`, 5xx
backoff, 404 fail-fast, `Link`-header pagination, and `304` short-circuiting.
`time.sleep` is patched, so the suite runs instantly.

## Running it with Airflow

```bash
docker compose build      # builds the extended Airflow image (first run only)
docker compose up -d
# UI at http://localhost:8080  (airflow / airflow)
```

The project is mounted into the containers at `/opt/airflow/repo`; runtime
config (paths, DB creds, `GITHUB_*`) is injected from `.env` via compose.
Unpause the **`repo_activity`** DAG and trigger it:

```
apply_github_schema → ingest_github → dbt_deps → dbt_build
```

## Config reference

| Variable | Default | Purpose |
|----------|---------|---------|
| `GITHUB_TOKEN` | — | PAT / fine-grained token (strongly recommended) |
| `GITHUB_REPOS` | `dbt-labs/dbt-core,duckdb/duckdb` | comma-separated `owner/repo` list |
| `GITHUB_API_URL` | `https://api.github.com` | override for GHES |
| `GITHUB_INITIAL_LOOKBACK_DAYS` | `30` | first-run window per resource |
| `GITHUB_MAX_PAGES` | `50` | per-resource safety cap; a run that hits it does not advance the watermark |
| `DB_*` / `DB_SSLMODE` | — | Postgres connection (ingest + dbt) |
| `DBT_SCHEMA` | `repo_activity` | dbt writes to `<schema>_staging` / `<schema>_marts` |

## Notes / next steps

- `github.rejected_records` captures rows failing validation (missing natural
  key or unparseable timestamp) instead of dropping them silently.
- Natural extensions: add `/repos/{repo}/releases` and workflow-run data,
  snapshot issue/PR state transitions with a dbt snapshot, and switch the
  staging models to incremental materialization once volumes grow.
