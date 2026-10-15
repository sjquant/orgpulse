from __future__ import annotations

import http.client
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from orgpulse.common.errors import GitHubError
from orgpulse.common.models import PullRequest

GRAPHQL_URL = "https://api.github.com/graphql"
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0
AUTH_REQUIRED_MESSAGE = (
    "GitHub authentication is required. Set GH_TOKEN or run `gh auth login`."
)

GraphQLRequest = Callable[[str, dict[str, Any]], dict[str, Any]]

REPOSITORIES_QUERY = """
query($org: String!, $after: String) {
  organization(login: $org) {
    repositories(first: 100, after: $after, orderBy: {field: PUSHED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes { nameWithOwner pushedAt }
    }
  }
}
"""

PULL_REQUESTS_QUERY = """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        url
        createdAt
        updatedAt
        mergedAt
        additions
        deletions
        author { __typename login }
        reviews(first: 30) { nodes { submittedAt author { __typename login } } }
        timelineItems(first: 1, itemTypes: [READY_FOR_REVIEW_EVENT]) {
          nodes { ... on ReadyForReviewEvent { createdAt } }
        }
      }
    }
  }
}
"""


@dataclass(frozen=True)
class Repository:
    full_name: str
    pushed_at: datetime | None


def resolve_token() -> str:
    """Return GH_TOKEN, falling back to the GitHub CLI's active token."""

    token = os.environ.get("GH_TOKEN", "").strip()
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, check=False
        )
    except FileNotFoundError as exc:
        raise GitHubError(AUTH_REQUIRED_MESSAGE) from exc
    token = result.stdout.strip()
    if result.returncode != 0 or not token:
        raise GitHubError(f"{AUTH_REQUIRED_MESSAGE}\n{result.stderr.strip()}".strip())
    return token


def graphql_requester(token: str) -> GraphQLRequest:
    """Build a GraphQL caller that retries transient GitHub failures."""

    def request(query: str, variables: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode()
        http_request = urllib.request.Request(
            GRAPHQL_URL,
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "orgpulse",
            },
        )
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(http_request, timeout=60) as response:
                    payload = json.load(response)
                break
            except urllib.error.HTTPError as exc:
                if exc.code < 500 or attempt == MAX_ATTEMPTS:
                    detail = exc.read().decode(errors="replace")[:300]
                    raise GitHubError(
                        f"GitHub returned HTTP {exc.code}: {detail}"
                    ) from exc
            # Timeouts, dropped connections, and truncated JSON are all worth a retry.
            except (OSError, http.client.HTTPException, ValueError) as exc:
                if attempt == MAX_ATTEMPTS:
                    raise GitHubError(f"GitHub request failed: {exc}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        if payload.get("errors"):
            messages = "; ".join(
                error.get("message", "") for error in payload["errors"]
            )
            raise GitHubError(f"GitHub GraphQL error: {messages}")
        return payload["data"]

    return request


def iter_repositories(request: GraphQLRequest, org: str) -> Iterator[Repository]:
    """Yield every repository in the organization, most recently pushed first."""

    def connection(data: dict[str, Any]) -> dict[str, Any]:
        if data["organization"] is None:
            raise GitHubError(f"Organization '{org}' is not accessible.")
        return data["organization"]["repositories"]

    for node in _nodes(request, REPOSITORIES_QUERY, {"org": org}, connection):
        yield Repository(
            full_name=node["nameWithOwner"],
            pushed_at=_optional_datetime(node["pushedAt"]),
        )


def iter_pull_requests(
    request: GraphQLRequest,
    repository: str,
    *,
    updated_since: datetime,
) -> Iterator[PullRequest]:
    """Yield merged, human-authored pull requests updated at or after `updated_since`."""

    owner, name = repository.split("/", 1)
    for node in _nodes(
        request,
        PULL_REQUESTS_QUERY,
        {"owner": owner, "name": name},
        lambda data: data["repository"]["pullRequests"],
    ):
        if datetime.fromisoformat(node["updatedAt"]) < updated_since:
            return
        pull_request = _pull_request(repository, node)
        if pull_request is not None:
            yield pull_request


def _nodes(
    request: GraphQLRequest,
    query: str,
    variables: dict[str, Any],
    connection: Callable[[dict[str, Any]], dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    """Yield the nodes of a paginated connection, fetching pages as needed."""

    after = None
    while True:
        page = connection(request(query, {**variables, "after": after}))
        yield from page["nodes"]
        if not page["pageInfo"]["hasNextPage"]:
            return
        after = page["pageInfo"]["endCursor"]


def _pull_request(repository: str, node: dict[str, Any]) -> PullRequest | None:
    author = node["author"]
    if node["mergedAt"] is None or (
        author is not None and author["__typename"] == "Bot"
    ):
        return None
    login = author["login"] if author is not None else "ghost"
    created_at = datetime.fromisoformat(node["createdAt"])
    ready_events = node["timelineItems"]["nodes"]
    review_times = [
        datetime.fromisoformat(review["submittedAt"])
        for review in node["reviews"]["nodes"]
        if review["submittedAt"] is not None
        and review["author"] is not None
        and review["author"]["__typename"] != "Bot"
        and review["author"]["login"].lower() != login.lower()
    ]
    return PullRequest(
        repository=repository,
        number=node["number"],
        title=node["title"],
        url=node["url"],
        author=login,
        created_at=created_at,
        ready_at=(
            datetime.fromisoformat(ready_events[0]["createdAt"])
            if ready_events
            else created_at
        ),
        first_review_at=min(review_times, default=None),
        merged_at=datetime.fromisoformat(node["mergedAt"]),
        additions=node["additions"],
        deletions=node["deletions"],
    )


def _optional_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
