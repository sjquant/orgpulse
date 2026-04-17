from __future__ import annotations

from collections import deque
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from github import GithubException

from orgpulse.ingestion import GitHubIngestionService, RepositoryInventory, RepositoryInventoryItem
from orgpulse.models import RunConfig, RunMode


class TestGitHubIngestionService:
    def test_loads_filtered_repository_inventory_after_rate_limit_retry(self) -> None:
        """Load repository inventory after retrying a rate-limited organization listing."""
        # Given
        sleep_calls: list[float] = []
        api = FakeGithubClient(
            organizations={
                "acme": FakeOrganization(
                    login="acme",
                    repo_outcomes=[
                        self.build_github_exception(
                            status=403,
                            message="You have exceeded a secondary rate limit.",
                            headers={"retry-after": "2"},
                        ),
                        [
                            self.build_repository("acme/zeta"),
                            self.build_repository("acme/api"),
                            self.build_repository("acme/ops"),
                        ],
                    ],
                )
            },
            repositories={},
        )
        service = GitHubIngestionService(api, sleep=lambda seconds: sleep_calls.append(seconds))
        config = RunConfig(
            org="acme",
            include_repos=("api", "acme/zeta"),
            exclude_repos=("ops",),
        )

        # When
        inventory = service.load_repository_inventory(config)

        # Then
        assert sleep_calls == [2.0]
        assert inventory.organization_login == "acme"
        assert [repository.full_name for repository in inventory.repositories] == [
            "acme/api",
            "acme/zeta",
        ]

    def test_fetches_incremental_pull_requests_within_collection_window(self) -> None:
        """Fetch only pull requests updated inside the incremental collection window."""
        # Given
        repository = self.build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self.build_pull_request(number=30, updated_at="2026-04-20T09:00:00"),
                    self.build_pull_request(number=20, updated_at="2026-04-12T09:00:00"),
                    self.build_pull_request(number=10, updated_at="2026-03-31T23:59:00"),
                ]
            ],
        )
        service = GitHubIngestionService(
            FakeGithubClient(
                organizations={},
                repositories={"acme/api": [repository]},
            )
        )
        config = RunConfig(
            org="acme",
            as_of="2026-04-18",
        )

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self.build_inventory_item("acme/api"),
                ),
            ),
        )

        # Then
        assert str(result.window.start_date) == "2026-04-01"
        assert str(result.window.end_date) == "2026-04-18"
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        assert result.failures == ()

    def test_fetches_backfill_pull_requests_from_closed_period_window(self) -> None:
        """Fetch only pull requests updated inside an explicit backfill window."""
        # Given
        repository = self.build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self.build_pull_request(number=30, updated_at="2026-04-01T00:00:00"),
                    self.build_pull_request(number=20, updated_at="2026-03-20T09:00:00"),
                    self.build_pull_request(number=10, updated_at="2026-02-28T22:00:00"),
                ]
            ],
        )
        service = GitHubIngestionService(
            FakeGithubClient(
                organizations={},
                repositories={"acme/api": [repository]},
            )
        )
        config = RunConfig(
            org="acme",
            as_of="2026-04-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-03-31",
        )

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self.build_inventory_item("acme/api"),
                ),
            ),
        )

        # Then
        assert str(result.window.start_date) == "2026-03-01"
        assert str(result.window.end_date) == "2026-03-31"
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        assert result.failures == ()

    def test_records_repo_scoped_failures_without_stopping_other_repositories(self) -> None:
        """Record repo-scoped failures after retries and continue fetching other repositories."""
        # Given
        sleep_calls: list[float] = []
        failing_repository = self.build_repository(
            "acme/api",
            pull_outcomes=[
                self.build_github_exception(status=503, message="Service unavailable"),
                self.build_github_exception(status=503, message="Service unavailable"),
                self.build_github_exception(status=503, message="Service unavailable"),
            ],
        )
        succeeding_repository = self.build_repository(
            "acme/web",
            pull_outcomes=[
                [
                    self.build_pull_request(number=7, updated_at="2026-04-11T10:00:00"),
                ]
            ],
        )
        service = GitHubIngestionService(
            FakeGithubClient(
                organizations={},
                repositories={
                    "acme/api": [failing_repository],
                    "acme/web": [succeeding_repository],
                },
            ),
            max_retries=2,
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
        config = RunConfig(org="acme", as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self.build_inventory_item("acme/api"),
                    self.build_inventory_item("acme/web"),
                ),
            ),
        )

        # Then
        assert sleep_calls == [1.0, 2.0]
        assert [pull_request.repository_full_name for pull_request in result.pull_requests] == ["acme/web"]
        assert len(result.failures) == 1
        assert result.failures[0].repository_full_name == "acme/api"
        assert result.failures[0].status_code == 503
        assert result.failures[0].retriable is True
        assert result.failures[0].message == "Service unavailable"

    def build_inventory_item(self, full_name: str) -> RepositoryInventoryItem:
        """Build the minimal repository inventory item required for PR fetching."""
        return RepositoryInventoryItem(
            name=full_name.split("/", 1)[1],
            full_name=full_name,
            default_branch="main",
            private=False,
            archived=False,
            disabled=False,
        )

    def build_repository(
        self,
        full_name: str,
        *,
        pull_outcomes: list[Any] | None = None,
    ) -> FakeRepository:
        """Build a fake repository with optional pull request outcomes."""
        return FakeRepository(
            full_name=full_name,
            pull_outcomes=pull_outcomes or [],
        )

    def build_pull_request(self, *, number: int, updated_at: str) -> FakePullRequest:
        """Build a fake pull request object with deterministic metric fields."""
        timestamp = datetime.fromisoformat(updated_at)
        return FakePullRequest(
            number=number,
            title=f"PR {number}",
            state="closed",
            draft=False,
            merged=True,
            user=SimpleNamespace(login="alice"),
            created_at=timestamp,
            updated_at=timestamp,
            closed_at=timestamp,
            merged_at=timestamp,
            additions=12,
            deletions=4,
            changed_files=3,
            commits=2,
            html_url=f"https://example.test/pr/{number}",
        )

    def build_github_exception(
        self,
        *,
        status: int,
        message: str,
        headers: dict[str, str] | None = None,
    ) -> GithubException:
        """Build a GitHub exception with message and headers for retry tests."""
        return GithubException(status, {"message": message}, headers)


class FakeGithubClient:
    def __init__(
        self,
        *,
        organizations: dict[str, FakeOrganization],
        repositories: dict[str, list[Any]],
    ) -> None:
        self._organizations = organizations
        self._repositories = {
            name: deque(outcomes)
            for name, outcomes in repositories.items()
        }

    def get_organization(self, org: str) -> FakeOrganization:
        return self._organizations[org]

    def get_repo(self, full_name: str) -> FakeRepository:
        return resolve_outcome(self._repositories[full_name])


class FakeOrganization:
    def __init__(self, *, login: str, repo_outcomes: list[Any]) -> None:
        self.login = login
        self._repo_outcomes = deque(repo_outcomes)

    def get_repos(self, *, type: str, sort: str, direction: str) -> list[FakeRepository]:
        return resolve_outcome(self._repo_outcomes)


class FakeRepository:
    def __init__(self, *, full_name: str, pull_outcomes: list[Any]) -> None:
        self.name = full_name.split("/", 1)[1]
        self.full_name = full_name
        self.default_branch = "main"
        self.private = False
        self.archived = False
        self.disabled = False
        self._pull_outcomes = deque(pull_outcomes)

    def get_pulls(self, *, state: str, sort: str, direction: str) -> list[FakePullRequest]:
        return resolve_outcome(self._pull_outcomes)


class FakePullRequest:
    def __init__(self, **payload: Any) -> None:
        for key, value in payload.items():
            setattr(self, key, value)


def resolve_outcome(outcomes: deque[Any]) -> Any:
    outcome = outcomes.popleft()
    if isinstance(outcome, Exception):
        raise outcome
    return outcome
