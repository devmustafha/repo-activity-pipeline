"""Incremental Repository Activity Ingestion Pipeline.

`client.py`  - a rate-limit-aware, paginating HTTP client for the GitHub REST API.
`ingest.py`  - drives the client per (repo, resource), upserts into the `github`
               schema, and advances the incremental watermark.

Run:  python -m github_activity.ingest
"""
