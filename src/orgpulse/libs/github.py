from __future__ import annotations

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
        closedAt
        isDraft
        additions
        deletions
        author { __typename login }
        commits(first: 1) { totalCount nodes { commit { authoredDate } } }
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
            except urllib.error.URLError as exc:
                if attempt == MAX_ATTEMPTS:
                    raise GitHubError(f"GitHub request failed: {exc.reason}") from exc
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

    after = None
    while True:
        data = request(REPOSITORIES_QUERY, {"org": org, "after": after})
        if data["organization"] is None:
            raise GitHubError(f"Organization '{org}' is not accessible.")
        connection = data["organization"]["repositories"]
        for node in connection["nodes"]:
            yield Repository(
                full_name=node["nameWithOwner"],
                pushed_at=_optional_datetime(node["pushedAt"]),
            )
        if not connection["pageInfo"]["hasNextPage"]:
            return
        after = connection["pageInfo"]["endCursor"]


def iter_pull_requests(
    request: GraphQLRequest,
    repository: str,
    *,
    updated_since: datetime,
) -> Iterator[PullRequest]:
    """Yield human-authored pull requests updated at or after `updated_since`."""

    owner, name = repository.split("/", 1)
    after = None
    while True:
        data = request(
            PULL_REQUESTS_QUERY, {"owner": owner, "name": name, "after": after}
        )
        connection = data["repository"]["pullRequests"]
        for node in connection["nodes"]:
            if _datetime(node["updatedAt"]) < updated_since:
                return
            pull_request = _pull_request(repository, node)
            if pull_request is not None:
                yield pull_request
        if not connection["pageInfo"]["hasNextPage"]:
            return
        after = connection["pageInfo"]["endCursor"]


def _pull_request(repository: str, node: dict[str, Any]) -> PullRequest | None:
    author = node["author"]
    if author is not None and author["__typename"] == "Bot":
        return None
    login = author["login"] if author is not None else "ghost"
    created_at = _datetime(node["createdAt"])
    ready_events = node["timelineItems"]["nodes"]
    ready_at = None
    if not node["isDraft"]:
        ready_at = (
            _datetime(ready_events[0]["createdAt"]) if ready_events else created_at
        )
    commits = node["commits"]
    review_times = [
        _datetime(review["submittedAt"])
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
        updated_at=_datetime(node["updatedAt"]),
        merged_at=_optional_datetime(node["mergedAt"]),
        closed_at=_optional_datetime(node["closedAt"]),
        ready_at=ready_at,
        first_commit_at=(
            _datetime(commits["nodes"][0]["commit"]["authoredDate"])
            if commits["nodes"]
            else None
        ),
        first_review_at=min(review_times, default=None),
        additions=node["additions"],
        deletions=node["deletions"],
        commits=commits["totalCount"],
    )


def _optional_datetime(value: str | None) -> datetime | None:
    return _datetime(value) if value else None


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)
