-- github_schema.sql
--
-- Landing zone for GitHub repository activity ingested directly from the
-- GitHub REST API by extract/github/ingest.py.
--
-- Design notes (mirrors sql/staging_schema.sql):
--   * Column types are deliberately permissive. This schema's job is a
--     faithful, queryable copy of the API payload; cleaning and type
--     tightening happen in the dbt layer downstream.
--   * Every activity table carries a natural key so the ingester can upsert
--     (INSERT ... ON CONFLICT) and stay idempotent when a run is retried or
--     when overlapping "since" windows re-deliver the same records.
--   * github.ingestion_state holds one high-water mark per (repo, resource).
--     Incremental runs read it to build the "since" query and only advance it
--     after a resource finishes without error, so a mid-run failure simply
--     re-fetches from the previous watermark (safe, because upserts are
--     idempotent).
--   * github.rejected_records captures rows that fail validation instead of
--     dropping them silently.
--   * This script is idempotent (IF NOT EXISTS everywhere) so it can run as
--     the first task of the Airflow DAG on every execution.

CREATE SCHEMA IF NOT EXISTS github;

-- ---------------------------------------------------------------------------
-- commits  (one row per commit on the default branch, per repo)
-- Natural key is (repo_full_name, sha): a sha is unique within a repo, and
-- the same upstream commit can appear in more than one tracked repo (forks).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github.commits (
    repo_full_name  text NOT NULL,
    sha             text NOT NULL,
    message         text,
    author_name     text,
    author_email    text,
    author_login    text,
    authored_at     timestamptz,
    committed_at    timestamptz,
    comment_count   integer,
    html_url        text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, sha)
);

CREATE INDEX IF NOT EXISTS ix_github_commits_committed_at
    ON github.commits (repo_full_name, committed_at);

-- ---------------------------------------------------------------------------
-- issues  (one row per issue; the GitHub "issues" endpoint also returns pull
-- requests, flagged here by is_pull_request so downstream models can split them)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github.issues (
    repo_full_name  text NOT NULL,
    number          integer NOT NULL,
    id              bigint,
    title           text,
    state           text,
    is_pull_request boolean NOT NULL DEFAULT false,
    user_login      text,
    comments        integer,
    labels          jsonb,
    created_at      timestamptz,
    updated_at      timestamptz,
    closed_at       timestamptz,
    html_url        text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, number)
);

CREATE INDEX IF NOT EXISTS ix_github_issues_updated_at
    ON github.issues (repo_full_name, updated_at);

-- ---------------------------------------------------------------------------
-- pull_requests  (richer PR-specific fields the issues endpoint does not carry:
-- merged_at, draft, base/head refs)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github.pull_requests (
    repo_full_name  text NOT NULL,
    number          integer NOT NULL,
    id              bigint,
    title           text,
    state           text,
    draft           boolean,
    user_login      text,
    base_ref        text,
    head_ref        text,
    created_at      timestamptz,
    updated_at      timestamptz,
    closed_at       timestamptz,
    merged_at       timestamptz,
    html_url        text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, number)
);

CREATE INDEX IF NOT EXISTS ix_github_pull_requests_updated_at
    ON github.pull_requests (repo_full_name, updated_at);

-- ---------------------------------------------------------------------------
-- ingestion_state  (incremental high-water mark, one row per repo + resource)
--   watermark    - the value fed into the next run's "since" filter. For
--                  commits this tracks committer date; for issues/PRs it
--                  tracks updated_at.
--   etag         - the ETag returned by the first page of the last successful
--                  pull, sent back as If-None-Match so an unchanged first page
--                  costs a cheap 304 and no rate-limit quota.
--   last_status  - success | error | no_change, for operational visibility.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github.ingestion_state (
    repo_full_name    text NOT NULL,
    resource          text NOT NULL,
    watermark         timestamptz,
    etag              text,
    last_run_at       timestamptz,
    last_status       text,
    records_ingested  integer,
    _updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repo_full_name, resource)
);

-- ---------------------------------------------------------------------------
-- rejected_records
-- Rows that fail ingest-time validation land here instead of crashing the run
-- or vanishing silently. source + reason make triage possible; raw_row keeps
-- the original payload for replay.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS github.rejected_records (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source       text NOT NULL,
    repo_full_name text,
    raw_row      jsonb,
    reason       text NOT NULL,
    _rejected_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_github_rejected_records_source
    ON github.rejected_records (source);
