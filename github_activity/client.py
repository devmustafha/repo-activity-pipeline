"""
client.py

A thin client over the GitHub REST API that handles the three things a naive
`requests.get` loop gets wrong against GitHub:

1. Pagination. GitHub paginates list endpoints and advertises the next page in
   the `Link` response header (not in the body). `paginate()` walks that chain
   and yields one response per page.

2. Rate limits. GitHub enforces a primary hourly limit (5000/hr authenticated,
   60/hr not) and an opaque *secondary* limit on burst traffic. Both surface as
   403/429. This client:
     - reads `X-RateLimit-Remaining` / `X-RateLimit-Reset` and sleeps until the
       window resets when the quota is exhausted;
     - honours an explicit `Retry-After` header when present;
     - falls back to exponential backoff with jitter for secondary limits and
       5xx responses.

3. Conditional requests. The first page of a pull can carry an `ETag`. Passing
   it back as `If-None-Match` lets GitHub answer an unchanged resource with a
   304 that does NOT count against the rate limit. `paginate()` accepts an
   `etag` and stops early (yielding nothing) on a 304.

Nothing here is GitHub-account-specific; a token is read from the caller.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.github.com"
DEFAULT_API_VERSION = "2022-11-28"

# Never sleep longer than this in one go, even if a reset header says to. A
# window that is further out than this almost always means a clock skew or a
# malformed header; better to fail loudly than to hang a task for an hour.
MAX_SLEEP_SECONDS = 900


class GitHubAPIError(RuntimeError):
    """Raised for non-retryable API failures (4xx that isn't a rate limit)."""

    def __init__(self, status_code: int, message: str):
        super().__init__(f"GitHub API {status_code}: {message}")
        self.status_code = status_code


@dataclass
class GitHubClient:
    token: str | None = None
    base_url: str = DEFAULT_BASE_URL
    per_page: int = 100
    max_retries: int = 5
    # Seconds of head-room to add after a computed rate-limit sleep, to avoid
    # waking up a hair before the window actually rolls over.
    reset_buffer_seconds: float = 2.0
    session: requests.Session = field(default_factory=requests.Session, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": DEFAULT_API_VERSION,
            "User-Agent": "multi-source-analytics-pipeline",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        else:
            logger.warning(
                "No GitHub token supplied - falling back to the 60 requests/hour "
                "unauthenticated limit. Set GITHUB_TOKEN for 5000/hour."
            )
        self.session.headers.update(headers)

    # -- internals ---------------------------------------------------------

    def _sleep(self, seconds: float, reason: str) -> None:
        seconds = max(0.0, min(seconds, MAX_SLEEP_SECONDS))
        if seconds <= 0:
            return
        logger.warning("Sleeping %.1fs (%s)", seconds, reason)
        time.sleep(seconds)

    def _backoff(self, attempt: int) -> None:
        # 1, 2, 4, 8 ... seconds with +/- 50% jitter.
        base = 2 ** attempt
        self._sleep(base + random.uniform(0, base), f"backoff, attempt {attempt + 1}")

    @staticmethod
    def _is_rate_limited(resp: requests.Response) -> bool:
        if resp.status_code not in (403, 429):
            return False
        if resp.headers.get("Retry-After"):
            return True
        if resp.headers.get("X-RateLimit-Remaining") == "0":
            return True
        return "rate limit" in resp.text.lower()

    def _wait_for_rate_limit(self, resp: requests.Response, attempt: int) -> None:
        """Sleep off a rate-limited response before the caller retries."""
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            self._sleep(float(retry_after) + self.reset_buffer_seconds, "Retry-After header")
            return

        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining == "0" and reset:
            wait = float(reset) - time.time() + self.reset_buffer_seconds
            self._sleep(wait, "primary rate limit, waiting for window reset")
            return

        # Secondary rate limit: no useful headers, GitHub just asks us to slow
        # down. Exponential backoff.
        self._backoff(attempt)

    def _proactively_pause(self, resp: requests.Response) -> None:
        """
        If we're one request away from exhausting the window, wait it out now
        rather than eat a 403 on the next call.
        """
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None and int(remaining) <= 1:
            wait = float(reset) - time.time() + self.reset_buffer_seconds
            self._sleep(wait, "rate-limit window nearly exhausted, pausing")

    def request(
        self,
        url: str,
        params: dict | None = None,
        etag: str | None = None,
    ) -> requests.Response:
        """
        GET `url` with retry/rate-limit handling. Returns the response for
        2xx and 304; raises GitHubAPIError for other 4xx; raises the last
        error after `max_retries` exhausted retryable failures.
        """
        headers = {"If-None-Match": etag} if etag else None
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            is_last = attempt == self.max_retries
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=30)
            except requests.RequestException as exc:  # connection reset, DNS, timeout
                last_exc = exc
                logger.warning("Request error (%s), attempt %d", exc, attempt + 1)
                if not is_last:
                    self._backoff(attempt)
                continue

            if resp.status_code in (200, 304):
                self._proactively_pause(resp)
                return resp

            if self._is_rate_limited(resp):
                last_exc = GitHubAPIError(resp.status_code, "rate limited")
                logger.warning(
                    "Rate limited on %s (attempt %d/%d)",
                    url, attempt + 1, self.max_retries + 1,
                )
                if not is_last:
                    self._wait_for_rate_limit(resp, attempt)
                continue

            if resp.status_code >= 500:
                last_exc = GitHubAPIError(resp.status_code, "server error")
                if not is_last:
                    self._backoff(attempt)
                continue

            # Any other 4xx is a real problem (bad repo, revoked token, ...).
            raise GitHubAPIError(resp.status_code, resp.text[:300])

        assert last_exc is not None
        raise last_exc

    def paginate(
        self,
        path: str,
        params: dict | None = None,
        etag: str | None = None,
    ) -> Iterator[requests.Response]:
        """
        Yield one response per page for a list endpoint, following the `Link`
        header. `etag` is only applied to the first page: a 304 there means the
        whole first page is unchanged, and we stop (yield nothing).

        The caller reads `resp.json()` for the page body and, on the first
        page, `resp.headers["ETag"]` to persist for next time.
        """
        url = f"{self.base_url}{path}"
        query = {**(params or {}), "per_page": self.per_page}
        first = True

        while url:
            resp = self.request(url, params=query if first else None,
                                etag=etag if first else None)
            if resp.status_code == 304:
                logger.info("304 Not Modified for %s - nothing new", path)
                return

            yield resp
            first = False

            # Subsequent page URLs from the Link header already embed their query
            # string, so params must be cleared.
            url = resp.links.get("next", {}).get("url")
