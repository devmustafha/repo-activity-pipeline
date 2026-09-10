"""
ingest.py

Incremental ingestion of GitHub repository activity straight into Postgres
(schema `github`). No S3 landing zone here - the API response is the source of
record and every write is an idempotent upsert, so a replay just re-reads from
the API and rewrites the same rows.

Three resources per repo:

    commits         GET /repos/{repo}/commits?since=<watermark>
    issues          GET /repos/{repo}/issues?since=<watermark>&state=all
                        &sort=updated&direction=asc      (also returns PRs)
    pull_requests   GET /repos/{repo}/pulls?state=all&sort=updated
                        &direction=desc                  (no `since` param;
                        we stop paging once updated_at <= watermark)

Incrementality
--------------
`github.ingestion_state` holds one high-water mark per (repo, resource). Each
run:
  1. reads the previous watermark (or `now - GITHUB_INITIAL_LOOKBACK_DAYS` on
     the first run);
  2. pulls everything changed at/after it, tracking the max timestamp seen;
  3. upserts the rows;
  4. only if the whole resource finished cleanly, advances the watermark to
     the max timestamp seen and stores the first-page ETag.

Because step 4 is last, a crash mid-resource leaves the old watermark in place
and the next run re-fetches the overlap - which is harmless, since the upsert
is keyed on each resource's natural key.

Config (environment)
--------------------
    GITHUB_TOKEN                  PAT or fine-grained token (strongly recommended)
    GITHUB_REPOS                  comma-separated owner/repo list
    GITHUB_API_URL               default https://api.github.com
    GITHUB_INITIAL_LOOKBACK_DAYS  first-run window, default 30
    GITHUB_MAX_PAGES              per-resource safety cap, default 50
    DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import Json, execute_values
from dotenv import load_dotenv

from github_activity.client import GitHubClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("github.ingest")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

API_URL = os.getenv("GITHUB_API_URL", "https://api.github.com")
INITIAL_LOOKBACK_DAYS = int(os.getenv("GITHUB_INITIAL_LOOKBACK_DAYS", "30"))
MAX_PAGES = int(os.getenv("GITHUB_MAX_PAGES", "50"))


def _repos() -> list[str]:
    raw = os.getenv("GITHUB_REPOS", "")
    repos = [r.strip() for r in raw.split(",") if r.strip()]
    if not repos:
        raise SystemExit("GITHUB_REPOS is empty - set it to a comma-separated owner/repo list.")
    return repos


# --------------------------------------------------------------------------
# Timestamp helpers
# --------------------------------------------------------------------------

def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    # GitHub emits RFC3339 with a trailing Z.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# Per-resource parsers: API payload -> (row tuple aligned to `columns`,
# natural-key dict, record timestamp used for the watermark)
# --------------------------------------------------------------------------

def parse_commit(repo: str, item: dict) -> dict:
    commit = item.get("commit") or {}
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    gh_author = item.get("author") or {}
    committed_at = _parse_ts(committer.get("date"))
    return {
        "key": {"repo_full_name": repo, "sha": item.get("sha")},
        "ts": committed_at,
        "row": {
            "repo_full_name": repo,
            "sha": item.get("sha"),
            "message": commit.get("message"),
            "author_name": author.get("name"),
            "author_email": author.get("email"),
            "author_login": gh_author.get("login"),
            "authored_at": _parse_ts(author.get("date")),
            "committed_at": committed_at,
            "comment_count": commit.get("comment_count"),
            "html_url": item.get("html_url"),
        },
    }


def parse_issue(repo: str, item: dict) -> dict:
    updated_at = _parse_ts(item.get("updated_at"))
    labels = [lbl.get("name") for lbl in item.get("labels", []) if isinstance(lbl, dict)]
    return {
        "key": {"repo_full_name": repo, "number": item.get("number")},
        "ts": updated_at,
        "row": {
            "repo_full_name": repo,
            "number": item.get("number"),
            "id": item.get("id"),
            "title": item.get("title"),
            "state": item.get("state"),
            "is_pull_request": "pull_request" in item,
            "user_login": (item.get("user") or {}).get("login"),
            "comments": item.get("comments"),
            "labels": Json(labels),
            "created_at": _parse_ts(item.get("created_at")),
            "updated_at": updated_at,
            "closed_at": _parse_ts(item.get("closed_at")),
            "html_url": item.get("html_url"),
        },
    }


def parse_pull_request(repo: str, item: dict) -> dict:
    updated_at = _parse_ts(item.get("updated_at"))
    return {
        "key": {"repo_full_name": repo, "number": item.get("number")},
        "ts": updated_at,
        "row": {
            "repo_full_name": repo,
            "number": item.get("number"),
            "id": item.get("id"),
            "title": item.get("title"),
            "state": item.get("state"),
            "draft": item.get("draft"),
            "user_login": (item.get("user") or {}).get("login"),
            "base_ref": (item.get("base") or {}).get("ref"),
            "head_ref": (item.get("head") or {}).get("ref"),
            "created_at": _parse_ts(item.get("created_at")),
            "updated_at": updated_at,
            "closed_at": _parse_ts(item.get("closed_at")),
            "merged_at": _parse_ts(item.get("merged_at")),
            "html_url": item.get("html_url"),
        },
    }


# --------------------------------------------------------------------------
# Resource registry
# --------------------------------------------------------------------------
# path          - endpoint under /repos/{repo}
# base_params   - static query params
# since_param   - name of the query param that takes the watermark, or None
# stop_when_old - True for endpoints without a `since` filter: stop paginating
#                 once a record older than the watermark is seen (the feed must
#                 be sorted by the watermark field, descending)
# table         - target table
# conflict      - ON CONFLICT target
# parser        - function(repo, item) -> {"key","ts","row"}

RESOURCES: dict[str, dict] = {
    "commits": {
        "path": "/commits",
        "base_params": {},
        "since_param": "since",
        "stop_when_old": False,
        "table": "github.commits",
        "conflict": "repo_full_name, sha",
        "parser": parse_commit,
    },
    "issues": {
        "path": "/issues",
        "base_params": {"state": "all", "sort": "updated", "direction": "asc"},
        "since_param": "since",
        "stop_when_old": False,
        "table": "github.issues",
        "conflict": "repo_full_name, number",
        "parser": parse_issue,
    },
    "pull_requests": {
        "path": "/pulls",
        "base_params": {"state": "all", "sort": "updated", "direction": "desc"},
        "since_param": None,
        "stop_when_old": True,
        "table": "github.pull_requests",
        "conflict": "repo_full_name, number",
        "parser": parse_pull_request,
    },
}


# --------------------------------------------------------------------------
# State (watermark) table
# --------------------------------------------------------------------------

def read_state(conn, repo: str, resource: str) -> tuple[datetime | None, str | None]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watermark, etag FROM github.ingestion_state "
            "WHERE repo_full_name = %s AND resource = %s",
            (repo, resource),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def write_state(conn, repo: str, resource: str, *, watermark, etag, status, count) -> None:
    """Persist an advanced watermark after a clean (or clean-so-far) run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO github.ingestion_state
                (repo_full_name, resource, watermark, etag, last_run_at,
                 last_status, records_ingested, _updated_at)
            VALUES (%s, %s, %s, %s, now(), %s, %s, now())
            ON CONFLICT (repo_full_name, resource) DO UPDATE SET
                watermark        = EXCLUDED.watermark,
                etag             = EXCLUDED.etag,
                last_run_at      = EXCLUDED.last_run_at,
                last_status      = EXCLUDED.last_status,
                records_ingested = EXCLUDED.records_ingested,
                _updated_at      = now()
            """,
            (repo, resource, watermark, etag, status, count),
        )
    conn.commit()


def mark_error(conn, repo: str, resource: str) -> None:
    """
    Record that a run errored WITHOUT touching the watermark or ETag - the next
    run must re-fetch from the last known-good point.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO github.ingestion_state
                (repo_full_name, resource, last_run_at, last_status, _updated_at)
            VALUES (%s, %s, now(), 'error', now())
            ON CONFLICT (repo_full_name, resource) DO UPDATE SET
                last_run_at = now(),
                last_status = 'error',
                _updated_at = now()
            """,
            (repo, resource),
        )
    conn.commit()


# --------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------

def upsert_rows(conn, table: str, conflict: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    conflict_cols = {c.strip() for c in conflict.split(",")}
    update_cols = [c for c in columns if c not in conflict_cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict})
        DO UPDATE SET {set_clause}, _loaded_at = now()
    """
    values = [tuple(r[c] for c in columns) for r in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(values)


def insert_rejects(conn, source: str, rejects: list[dict]) -> int:
    if not rejects:
        return 0
    sql = """
        INSERT INTO github.rejected_records (source, repo_full_name, raw_row, reason)
        VALUES %s
    """
    values = [
        (source, r.get("repo_full_name"), Json(r["raw_row"]), r["reason"])
        for r in rejects
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(values)


# --------------------------------------------------------------------------
# Per-resource driver
# --------------------------------------------------------------------------

def ingest_resource(conn, client: GitHubClient, repo: str, resource: str) -> dict:
    spec = RESOURCES[resource]
    prev_watermark, prev_etag = read_state(conn, repo, resource)

    if prev_watermark is None:
        prev_watermark = datetime.now(timezone.utc) - timedelta(days=INITIAL_LOOKBACK_DAYS)
        logger.info("[%s/%s] first run - lookback to %s", repo, resource, _iso(prev_watermark))
    else:
        logger.info("[%s/%s] incremental since %s", repo, resource, _iso(prev_watermark))

    params = dict(spec["base_params"])
    if spec["since_param"]:
        params[spec["since_param"]] = _iso(prev_watermark)

    path = f"/repos/{repo}{spec['path']}"
    parser = spec["parser"]

    valid_rows: list[dict] = []
    rejects: list[dict] = []
    max_ts = prev_watermark
    first_page_etag = prev_etag
    pages = 0
    stopped_early = False   # ordered feed reached a record older than the watermark
    page_cap_hit = False    # bailed out at GITHUB_MAX_PAGES with more to fetch

    for page_num, resp in enumerate(client.paginate(path, params=params, etag=prev_etag)):
        if page_num == 0:
            first_page_etag = resp.headers.get("ETag", prev_etag)

        payload = resp.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"Expected a list from {path}, got {type(payload).__name__}")

        for item in payload:
            parsed = parser(repo, item)
            key_missing = [k for k, v in parsed["key"].items() if v is None]
            if key_missing:
                rejects.append({
                    "repo_full_name": repo,
                    "raw_row": item,
                    "reason": f"missing natural key field(s): {', '.join(key_missing)}",
                })
                continue
            if parsed["ts"] is None:
                rejects.append({
                    "repo_full_name": repo,
                    "raw_row": item,
                    "reason": "missing/unparseable timestamp for watermark",
                })
                continue

            if spec["stop_when_old"] and parsed["ts"] <= prev_watermark:
                stopped_early = True
                break

            valid_rows.append(parsed["row"])
            max_ts = max(max_ts, parsed["ts"])

        pages += 1
        if stopped_early:
            break
        if pages >= MAX_PAGES and resp.links.get("next"):
            page_cap_hit = True
            logger.warning("[%s/%s] hit GITHUB_MAX_PAGES=%d with more pages available - "
                           "stopping; the watermark will not advance this run",
                           repo, resource, MAX_PAGES)
            break

    loaded = upsert_rows(conn, spec["table"], spec["conflict"], valid_rows)
    rejected = insert_rejects(conn, resource, rejects)

    # Advance the watermark only when the whole delta was consumed. If we bailed
    # at the page cap there is still unread history, so leave the watermark put
    # and let the next run resume from the same point (upserts make the overlap
    # harmless).
    if page_cap_hit:
        new_watermark = prev_watermark
        status = "partial"
    else:
        new_watermark = max_ts
        status = "success" if (loaded or rejected) else "no_change"

    write_state(conn, repo, resource,
                watermark=new_watermark, etag=first_page_etag,
                status=status, count=loaded)

    logger.info("[%s/%s] %s: %d upserted, %d rejected, watermark -> %s",
                repo, resource, status, loaded, rejected, _iso(new_watermark))
    return {"resource": resource, "status": status, "loaded": loaded, "rejected": rejected}


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def run_ingestion() -> dict:
    repos = _repos()
    client = GitHubClient(token=os.getenv("GITHUB_TOKEN"), base_url=API_URL)
    conn = psycopg2.connect(**DB_CONFIG)

    summary: dict[str, list] = {}
    try:
        for repo in repos:
            summary[repo] = []
            for resource in RESOURCES:
                try:
                    result = ingest_resource(conn, client, repo, resource)
                except Exception as exc:  # keep going with the other resources
                    conn.rollback()
                    logger.exception("[%s/%s] failed: %s", repo, resource, exc)
                    mark_error(conn, repo, resource)
                    result = {"resource": resource, "status": "error", "error": str(exc)}
                summary[repo].append(result)
    finally:
        conn.close()
    return summary


if __name__ == "__main__":
    result = run_ingestion()
    total = sum(r.get("loaded", 0) for repo in result.values() for r in repo)
    errors = [
        f"{repo}/{r['resource']}"
        for repo, rows in result.items() for r in rows
        if r["status"] == "error"
    ]
    logger.info("Ingestion complete. Total rows upserted: %d", total)
    if errors:
        logger.error("Resources that errored: %s", ", ".join(errors))
        raise SystemExit(1)
