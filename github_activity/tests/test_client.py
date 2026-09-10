"""
Unit tests for the rate-limit / pagination logic in github_activity/client.py.

These are the parts of the pipeline most likely to be wrong and hardest to
exercise against the live API, so they are mocked at the `requests.Session`
boundary. `time.sleep` is patched everywhere so the suite runs instantly.

    python -m unittest discover -s github_activity/tests
"""

from __future__ import annotations

import time
import unittest
from unittest import mock

from github_activity.client import GitHubAPIError, GitHubClient


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, text="", links=None):
        self.status_code = status_code
        self._json = json_data if json_data is not None else []
        self.headers = headers or {}
        self.text = text
        self.links = links or {}

    def json(self):
        return self._json


def make_client(**kw):
    kw.setdefault("token", "x")
    return GitHubClient(session=mock.Mock(), **kw)


@mock.patch("github_activity.client.time.sleep", return_value=None)
class RequestRetryTests(unittest.TestCase):
    def test_returns_ok_response(self, _sleep):
        client = make_client()
        client.session.get.return_value = FakeResponse(200, [{"a": 1}])
        resp = client.request("https://api.github.com/x")
        self.assertEqual(resp.status_code, 200)
        client.session.get.assert_called_once()

    def test_primary_rate_limit_waits_for_reset_then_retries(self, sleep):
        client = make_client()
        reset_at = int(time.time()) + 30
        limited = FakeResponse(
            403, headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset_at)},
            text="API rate limit exceeded",
        )
        ok = FakeResponse(200, [{"ok": True}])
        client.session.get.side_effect = [limited, ok]

        resp = client.request("https://api.github.com/x")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(client.session.get.call_count, 2)
        # Slept roughly until the reset window (allow generous slack).
        slept = sleep.call_args[0][0]
        self.assertGreater(slept, 20)
        self.assertLess(slept, 40)

    def test_retry_after_header_is_honoured(self, sleep):
        client = make_client()
        limited = FakeResponse(429, headers={"Retry-After": "7"}, text="secondary rate limit")
        client.session.get.side_effect = [limited, FakeResponse(200, [])]

        client.request("https://api.github.com/x")

        self.assertGreaterEqual(sleep.call_args_list[0][0][0], 7)

    def test_secondary_rate_limit_uses_backoff(self, sleep):
        client = make_client()
        limited = FakeResponse(403, headers={}, text="You have exceeded a secondary rate limit")
        client.session.get.side_effect = [limited, limited, FakeResponse(200, [])]

        client.request("https://api.github.com/x")

        self.assertEqual(client.session.get.call_count, 3)
        self.assertGreaterEqual(sleep.call_count, 2)

    def test_5xx_is_retried_then_raises_after_max(self, _sleep):
        client = make_client(max_retries=2)
        client.session.get.return_value = FakeResponse(502, text="bad gateway")

        with self.assertRaises(GitHubAPIError):
            client.request("https://api.github.com/x")
        self.assertEqual(client.session.get.call_count, 3)  # initial + 2 retries

    def test_plain_404_raises_immediately(self, _sleep):
        client = make_client()
        client.session.get.return_value = FakeResponse(404, text="Not Found")

        with self.assertRaises(GitHubAPIError) as ctx:
            client.request("https://api.github.com/x")
        self.assertEqual(ctx.exception.status_code, 404)
        client.session.get.assert_called_once()


@mock.patch("github_activity.client.time.sleep", return_value=None)
class PaginateTests(unittest.TestCase):
    def test_follows_link_header_across_pages(self, _sleep):
        client = make_client()
        page1 = FakeResponse(
            200, [{"n": 1}], headers={"ETag": 'W/"abc"'},
            links={"next": {"url": "https://api.github.com/x?page=2"}},
        )
        page2 = FakeResponse(200, [{"n": 2}], links={})
        client.session.get.side_effect = [page1, page2]

        pages = list(client.paginate("/x"))

        self.assertEqual([p.json()[0]["n"] for p in pages], [1, 2])
        self.assertEqual(client.session.get.call_count, 2)

    def test_304_on_first_page_yields_nothing(self, _sleep):
        client = make_client()
        client.session.get.return_value = FakeResponse(304, headers={"ETag": 'W/"abc"'})

        pages = list(client.paginate("/x", etag='W/"abc"'))

        self.assertEqual(pages, [])

    def test_per_page_param_is_added_to_first_request(self, _sleep):
        client = make_client(per_page=50)
        client.session.get.return_value = FakeResponse(200, [], links={})

        list(client.paginate("/x", params={"state": "all"}))

        sent_params = client.session.get.call_args.kwargs["params"]
        self.assertEqual(sent_params["per_page"], 50)
        self.assertEqual(sent_params["state"], "all")


if __name__ == "__main__":
    unittest.main()
