from __future__ import annotations

import csv
import json
import tempfile
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar, cast, overload

from github import GithubException
from requests.exceptions import ChunkedEncodingError, RequestException

from orgpulse.ingestion import GitHubIngestionService, NormalizedRawSnapshotWriter
from orgpulse.models import (
    PullRequestCollection,
    PullRequestRecord,
    PullRequestReviewRecord,
    PullRequestTimelineEventRecord,
    RepositoryInventory,
    RepositoryInventoryItem,
    RunConfig,
    RunMode,
)
from orgpulse.types.github import (
    GitHubActorLike,
    GitHubIngestionClientLike,
    GitHubPullRequestLike,
    GitHubRepositoryLike,
    GitHubReviewLike,
    GitHubTeamLike,
    GitHubTimelineEventLike,
)

T = TypeVar("T")
RepositoryBatch = Sequence[GitHubRepositoryLike]
PullRequestBatch = Sequence[GitHubPullRequestLike]
ReviewBatch = Sequence[GitHubReviewLike]
TimelineEventBatch = Sequence[GitHubTimelineEventLike]
RepositoryInventoryOutcome = RepositoryBatch | GithubException | RequestException
RepositoryFetchOutcome = GitHubRepositoryLike | GithubException | RequestException
PullRequestFetchOutcome = PullRequestBatch | GithubException | RequestException
ReviewFetchOutcome = ReviewBatch | GithubException | RequestException
TimelineFetchOutcome = TimelineEventBatch | GithubException | RequestException


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
                        self._build_github_exception(
                            status=403,
                            message="You have exceeded a secondary rate limit.",
                            headers={"retry-after": "2"},
                        ),
                        [
                            self._build_repository("acme/zeta"),
                            self._build_repository("acme/api"),
                            self._build_repository("acme/ops"),
                        ],
                    ],
                )
            },
            repositories={},
        )
        service = GitHubIngestionService(
            cast(GitHubIngestionClientLike, api),
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
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

    def test_loads_repository_inventory_across_paginated_api_results(self) -> None:
        """Load repository inventory across paginated API results without dropping later pages."""
        # Given
        paginated_repositories = FakePaginatedSequence(
            (
                self._build_repository("acme/api"),
                self._build_repository("acme/docs"),
            ),
            (self._build_repository("acme/web"),),
        )
        api = FakeGithubClient(
            organizations={
                "acme": FakeOrganization(
                    login="acme",
                    repo_outcomes=[paginated_repositories],
                )
            },
            repositories={},
        )
        service = GitHubIngestionService(cast(GitHubIngestionClientLike, api))
        config = RunConfig(org="acme")

        # When
        inventory = service.load_repository_inventory(config)

        # Then
        assert [repository.full_name for repository in inventory.repositories] == [
            "acme/api",
            "acme/docs",
            "acme/web",
        ]
        assert paginated_repositories.page_accesses == [0, 1]

    def test_fetches_incremental_pull_requests_within_collection_window(self) -> None:
        """Fetch only pull requests updated inside the incremental collection window."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=30, updated_at="2026-04-20T09:00:00"
                    ),
                    self._build_pull_request(
                        number=20, updated_at="2026-04-12T09:00:00"
                    ),
                    self._build_pull_request(
                        number=10, updated_at="2026-03-31T23:59:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert str(result.window.start_date) == "2026-04-01"
        assert str(result.window.end_date) == "2026-04-18"
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        assert result.failures == ()

    def test_fetches_pull_requests_across_paginated_api_results(self) -> None:
        """Fetch pull requests across paginated API results at the repository boundary."""
        # Given
        paginated_pull_requests = FakePaginatedSequence(
            (
                self._build_pull_request(
                    number=30, updated_at="2026-04-12T09:00:00"
                ),
                self._build_pull_request(
                    number=20, updated_at="2026-04-10T09:00:00"
                ),
            ),
            (
                self._build_pull_request(
                    number=10, updated_at="2026-04-05T09:00:00"
                ),
            ),
        )
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[paginated_pull_requests],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert [pull_request.number for pull_request in result.pull_requests] == [
            10,
            20,
            30,
        ]
        assert result.failures == ()
        assert paginated_pull_requests.page_accesses == [0, 1]

    def test_fetches_pull_requests_through_graphql_batches_when_available(self) -> None:
        """Fetch pull requests through the GraphQL batch path when the client supports it."""
        # Given
        repository = self._build_repository("acme/api", pull_outcomes=[])
        graphql_requester = FakeGraphQLRequester(
            responses=[
                self._build_graphql_pull_request_response(
                    has_next_page=False,
                    nodes=[
                        self._build_graphql_pull_request_node(
                            number=20,
                            updated_at="2026-04-12T09:00:00Z",
                            reviews=(
                                self._build_graphql_review_node(
                                    review_id=101,
                                    submitted_at="2026-04-10T11:00:00Z",
                                    author_login="reviewer-a",
                                ),
                            ),
                            timeline_events=(
                                self._build_graphql_timeline_event_node(
                                    event_type="ReviewRequestedEvent",
                                    node_id="review-request-20",
                                    created_at="2026-04-10T09:30:00Z",
                                    actor_login="alice",
                                    requested_reviewer_login="reviewer-a",
                                ),
                            ),
                        ),
                        self._build_graphql_pull_request_node(
                            number=10,
                            updated_at="2026-03-31T23:59:00Z",
                        ),
                    ],
                )
            ]
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                    graphql_requester=graphql_requester,
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        pull_request = result.pull_requests[0]
        assert pull_request.reviews[0].review_id == 101
        assert (
            pull_request.timeline_events[0].requested_reviewer_login
            == "reviewer-a"
        )
        assert graphql_requester.variables == [
            {"owner": "acme", "name": "api", "after": None}
        ]
        assert result.failures == ()

    def test_falls_back_to_rest_enrichment_for_graphql_nested_page_overflow(
        self,
    ) -> None:
        """Fall back to REST pull-request enrichment when GraphQL nested pages overflow."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[],
            pull_lookup={
                20: self._build_pull_request(
                    number=20,
                    updated_at="2026-04-12T09:00:00",
                    review_outcomes=[
                        [
                            self._build_review(
                                review_id=201,
                                state="APPROVED",
                                submitted_at="2026-04-10T11:00:00",
                                author_login="reviewer-a",
                            ),
                            self._build_review(
                                review_id=202,
                                state="COMMENTED",
                                submitted_at="2026-04-10T12:00:00",
                                author_login="reviewer-b",
                            ),
                        ]
                    ],
                    timeline_outcomes=[
                        [
                            self._build_timeline_event(
                                event_id=301,
                                event="review_requested",
                                created_at="2026-04-10T09:30:00",
                                actor_login="alice",
                                requested_reviewer_login="reviewer-a",
                            ),
                            self._build_timeline_event(
                                event_id=302,
                                event="ready_for_review",
                                created_at="2026-04-10T10:00:00",
                                actor_login="alice",
                            ),
                        ]
                    ],
                )
            },
        )
        graphql_requester = FakeGraphQLRequester(
            responses=[
                self._build_graphql_pull_request_response(
                    has_next_page=False,
                    nodes=[
                        self._build_graphql_pull_request_node(
                            number=20,
                            updated_at="2026-04-12T09:00:00Z",
                            reviews=(
                                self._build_graphql_review_node(
                                    review_id=201,
                                    submitted_at="2026-04-10T11:00:00Z",
                                    author_login="reviewer-a",
                                ),
                            ),
                            review_has_next_page=True,
                        )
                    ],
                )
            ]
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                    graphql_requester=graphql_requester,
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        pull_request = result.pull_requests[0]
        assert [review.review_id for review in pull_request.reviews] == [201, 202]
        assert [event.event_id for event in pull_request.timeline_events] == [301, 302]
        assert repository.get_pull_calls == [20]
        assert result.failures == ()

    def test_returns_empty_collection_for_repository_without_pull_requests(self) -> None:
        """Return an empty collection for repositories that currently have no pull requests."""
        # Given
        repository = self._build_repository(
            "acme/empty",
            pull_outcomes=[FakePaginatedSequence[FakePullRequest]()],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/empty": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/empty"),),
            ),
        )

        # Then
        assert result.pull_requests == ()
        assert result.failures == ()

    def test_enriches_pull_requests_with_review_and_timeline_data_for_first_review_inputs(
        self,
    ) -> None:
        """Enrich pull requests with sorted review and timeline data needed for first-review timing."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=20,
                        updated_at="2026-04-12T09:00:00",
                        review_outcomes=[
                            [
                                self._build_review(
                                    review_id=102,
                                    state="APPROVED",
                                    submitted_at="2026-04-11T10:30:00",
                                    author_login="reviewer-b",
                                ),
                                self._build_review(
                                    review_id=101,
                                    state="COMMENTED",
                                    submitted_at="2026-04-10T11:00:00",
                                    author_login="reviewer-a",
                                ),
                            ]
                        ],
                        timeline_outcomes=[
                            [
                                self._build_timeline_event(
                                    event_id=302,
                                    event="ready_for_review",
                                    created_at="2026-04-10T10:00:00",
                                    actor_login="alice",
                                ),
                                self._build_timeline_event(
                                    event_id=303,
                                    event="labeled",
                                    created_at="2026-04-10T10:05:00",
                                    actor_login="alice",
                                ),
                                self._build_timeline_event(
                                    event_id=301,
                                    event="review_requested",
                                    created_at="2026-04-10T09:30:00",
                                    actor_login="alice",
                                    requested_reviewer_login="reviewer-a",
                                ),
                            ]
                        ],
                    )
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert len(result.pull_requests) == 1
        pull_request = result.pull_requests[0]
        assert [review.review_id for review in pull_request.reviews] == [101, 102]
        assert [review.author_login for review in pull_request.reviews] == [
            "reviewer-a",
            "reviewer-b",
        ]
        assert [
            timeline_event.event for timeline_event in pull_request.timeline_events
        ] == [
            "review_requested",
            "ready_for_review",
        ]
        assert pull_request.timeline_events[0].requested_reviewer_login == "reviewer-a"
        assert pull_request.timeline_events[1].actor_login == "alice"
        assert result.failures == ()

    def test_reads_requested_reviewer_and_team_from_timeline_raw_data(
        self,
    ) -> None:
        """Read review request targets from timeline raw data when the SDK omits direct attributes."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=21,
                        updated_at="2026-04-12T09:00:00",
                        timeline_outcomes=[
                            [
                                FakeTimelineEvent(
                                    id=401,
                                    event="review_requested",
                                    actor=FakeActor(login="alice"),
                                    created_at=datetime.fromisoformat(
                                        "2026-04-10T09:30:00"
                                    ),
                                    requested_reviewer=None,
                                    requested_team=None,
                                    raw_data={
                                        "requested_reviewer": {"login": "reviewer-a"},
                                        "requested_team": {"name": "platform"},
                                    },
                                )
                            ]
                        ],
                    )
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert len(result.pull_requests) == 1
        pull_request = result.pull_requests[0]
        assert len(pull_request.timeline_events) == 1
        assert pull_request.timeline_events[0].requested_reviewer_login == "reviewer-a"
        assert pull_request.timeline_events[0].requested_team_name == "platform"
        assert result.failures == ()

    def test_fetches_backfill_pull_requests_from_closed_period_window(self) -> None:
        """Fetch only pull requests updated inside an explicit backfill window."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=30, updated_at="2026-04-01T00:00:00"
                    ),
                    self._build_pull_request(
                        number=20, updated_at="2026-03-20T09:00:00"
                    ),
                    self._build_pull_request(
                        number=10, updated_at="2026-02-28T22:00:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(
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
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert str(result.window.start_date) == "2026-03-01"
        assert str(result.window.end_date) == "2026-03-31"
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        assert result.failures == ()

    def test_records_repo_scoped_failures_without_stopping_other_repositories(
        self,
    ) -> None:
        """Record repo-scoped failures after retries and continue fetching other repositories."""
        # Given
        sleep_calls: list[float] = []
        failing_repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                self._build_github_exception(status=503, message="Service unavailable"),
                self._build_github_exception(status=503, message="Service unavailable"),
                self._build_github_exception(status=503, message="Service unavailable"),
            ],
        )
        succeeding_repository = self._build_repository(
            "acme/web",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=7, updated_at="2026-04-11T10:00:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/api": [failing_repository],
                        "acme/web": [succeeding_repository],
                    },
                ),
            ),
            max_retries=2,
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self._build_inventory_item("acme/api"),
                    self._build_inventory_item("acme/web"),
                ),
            ),
        )

        # Then
        assert sleep_calls == [1.0, 2.0]
        assert [
            pull_request.repository_full_name for pull_request in result.pull_requests
        ] == ["acme/web"]
        assert len(result.failures) == 1
        assert result.failures[0].repository_full_name == "acme/api"
        assert result.failures[0].status_code == 503
        assert result.failures[0].retriable is True
        assert result.failures[0].message == "Service unavailable"

    def test_records_permission_failures_without_retrying_and_keeps_other_repositories_running(
        self,
    ) -> None:
        """Record repo-scoped permission failures as non-retriable and continue collecting other repositories."""
        # Given
        sleep_calls: list[float] = []
        succeeding_repository = self._build_repository(
            "acme/web",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=7, updated_at="2026-04-11T10:00:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/private": [
                            self._build_github_exception(
                                status=403,
                                message="Forbidden",
                            )
                        ],
                        "acme/web": [succeeding_repository],
                    },
                ),
            ),
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self._build_inventory_item("acme/private"),
                    self._build_inventory_item("acme/web"),
                ),
            ),
        )

        # Then
        assert sleep_calls == []
        assert [
            pull_request.repository_full_name for pull_request in result.pull_requests
        ] == ["acme/web"]
        assert len(result.failures) == 1
        assert result.failures[0].repository_full_name == "acme/private"
        assert result.failures[0].operation == "pull_requests"
        assert result.failures[0].status_code == 403
        assert result.failures[0].retriable is False
        assert result.failures[0].message == "Forbidden"

    def test_retries_request_transport_failures_and_records_repo_scoped_failure(
        self,
    ) -> None:
        """Retry request-layer transport failures and keep other repositories running."""
        # Given
        sleep_calls: list[float] = []
        succeeding_repository = self._build_repository(
            "acme/web",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=7, updated_at="2026-04-11T10:00:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/api": [
                            ChunkedEncodingError("Response ended prematurely"),
                            ChunkedEncodingError("Response ended prematurely"),
                            ChunkedEncodingError("Response ended prematurely"),
                        ],
                        "acme/web": [succeeding_repository],
                    },
                ),
            ),
            max_retries=2,
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self._build_inventory_item("acme/api"),
                    self._build_inventory_item("acme/web"),
                ),
            ),
        )

        # Then
        assert sleep_calls == [1.0, 2.0]
        assert [
            pull_request.repository_full_name for pull_request in result.pull_requests
        ] == ["acme/web"]
        assert len(result.failures) == 1
        assert result.failures[0].repository_full_name == "acme/api"
        assert result.failures[0].status_code == 0
        assert result.failures[0].retriable is True
        assert "Response ended prematurely" in result.failures[0].message

    def test_records_repo_scoped_failures_when_review_enrichment_exhausts_retries(
        self,
    ) -> None:
        """Record repo-scoped failures when review enrichment exhausts retries and keep other repos running."""
        # Given
        sleep_calls: list[float] = []
        failing_repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=7,
                        updated_at="2026-04-11T10:00:00",
                        review_outcomes=[
                            self._build_github_exception(
                                status=503, message="Review API unavailable"
                            ),
                            self._build_github_exception(
                                status=503, message="Review API unavailable"
                            ),
                            self._build_github_exception(
                                status=503, message="Review API unavailable"
                            ),
                        ],
                    )
                ]
            ],
        )
        succeeding_repository = self._build_repository(
            "acme/web",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=8, updated_at="2026-04-11T10:00:00"
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/api": [failing_repository],
                        "acme/web": [succeeding_repository],
                    },
                ),
            ),
            max_retries=2,
            sleep=lambda seconds: sleep_calls.append(seconds),
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(
                    self._build_inventory_item("acme/api"),
                    self._build_inventory_item("acme/web"),
                ),
            ),
        )

        # Then
        assert sleep_calls == [1.0, 2.0]
        assert [
            pull_request.repository_full_name for pull_request in result.pull_requests
        ] == ["acme/web"]
        assert len(result.failures) == 1
        assert result.failures[0].repository_full_name == "acme/api"
        assert result.failures[0].status_code == 503
        assert result.failures[0].retriable is True
        assert result.failures[0].message == "Review API unavailable"

    def test_resumes_completed_repositories_from_checkpoint_after_partial_failure(
        self,
        tmp_path,
    ) -> None:
        """Resume completed repositories from a persisted checkpoint after a partial incremental failure."""
        # Given
        api_repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=7,
                        updated_at="2026-04-11T10:00:00",
                    ),
                ]
            ],
        )
        first_service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/api": [api_repository],
                        "acme/web": [
                            self._build_github_exception(
                                status=403,
                                message="Forbidden",
                            )
                        ],
                    },
                ),
            )
        )
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
            include_repos=["api", "acme/web"],
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(
                self._build_inventory_item("acme/api"),
                self._build_inventory_item("acme/web"),
            ),
        )

        # When
        first_result = first_service.fetch_pull_requests(config, inventory)
        second_config = self._build_run_config(
            as_of="2026-04-19",
            output_dir=tmp_path,
            include_repos=["acme/api", "web"],
        )
        second_service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={
                        "acme/web": [
                            self._build_repository(
                                "acme/web",
                                pull_outcomes=[
                                    [
                                        self._build_pull_request(
                                            number=8,
                                            updated_at="2026-04-12T10:00:00",
                                        ),
                                    ]
                                ],
                            )
                        ]
                    },
                ),
            )
        )
        second_result = second_service.fetch_pull_requests(second_config, inventory)

        # Then
        assert [pull_request.repository_full_name for pull_request in first_result.pull_requests] == [
            "acme/api"
        ]
        assert [failure.repository_full_name for failure in first_result.failures] == [
            "acme/web"
        ]
        assert [
            pull_request.repository_full_name for pull_request in second_result.pull_requests
        ] == ["acme/api", "acme/web"]
        assert [pull_request.number for pull_request in second_result.pull_requests] == [
            7,
            8,
        ]
        assert second_result.failures == ()
        checkpoint_manifest_path = (
            tmp_path
            / "checkpoints"
            / "month"
            / "created_at"
            / "incremental"
            / "acme"
            / "manifest.json"
        )
        assert json.loads(checkpoint_manifest_path.read_text(encoding="utf-8")) == {
            "completed_repositories": ["acme/api", "acme/web"],
            "contract": {
                "collection_window": {
                    "scope": "open_period",
                    "start_date": "2026-04-01",
                },
                "exclude_repos": [],
                "include_repos": ["acme/api", "acme/web"],
                "mode": "incremental",
                "period_grain": "month",
                "target_org": "acme",
                "time_anchor": "created_at",
            },
        }

    def test_writes_normalized_raw_snapshots_partitioned_by_period(
        self,
        tmp_path,
    ) -> None:
        """Write period-partitioned normalized raw snapshots from enriched pull request records."""
        # Given
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[
                [
                    self._build_pull_request(
                        number=20,
                        updated_at="2026-04-12T09:00:00",
                        review_outcomes=[
                            [
                                self._build_review(
                                    review_id=101,
                                    state="APPROVED",
                                    submitted_at="2026-04-12T10:00:00",
                                    author_login="reviewer-a",
                                ),
                            ]
                        ],
                        timeline_outcomes=[
                            [
                                self._build_timeline_event(
                                    event_id=201,
                                    event="review_requested",
                                    created_at="2026-04-10T09:30:00",
                                    actor_login="alice",
                                    requested_reviewer_login="reviewer-a",
                                ),
                            ]
                        ],
                    ),
                    self._build_pull_request(
                        number=10,
                        updated_at="2026-03-20T09:00:00",
                    ),
                ]
            ],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-04-18",
            mode=RunMode.FULL,
            output_dir=tmp_path,
        )
        inventory = RepositoryInventory(
            organization_login="acme",
            repositories=(self._build_inventory_item("acme/api"),),
        )

        # When
        collection = service.fetch_pull_requests(config, inventory)
        result = writer.write(config, collection)

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-03" / "pull_requests.csv"
        ) == [
            {
                "period_key": "2026-03",
                "repository_full_name": "acme/api",
                "pull_request_number": "10",
                "title": "PR 10",
                "state": "closed",
                "draft": "False",
                "merged": "True",
                "author_login": "alice",
                "created_at": "2026-03-20T09:00:00",
                "updated_at": "2026-03-20T09:00:00",
                "closed_at": "2026-03-20T09:00:00",
                "merged_at": "2026-03-20T09:00:00",
                "additions": "12",
                "deletions": "4",
                "changed_files": "3",
                "commits": "2",
                "html_url": "https://example.test/pr/10",
            }
        ]
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_request_reviews.csv"
        ) == [
            {
                "period_key": "2026-04",
                "repository_full_name": "acme/api",
                "pull_request_number": "20",
                "review_id": "101",
                "state": "APPROVED",
                "author_login": "reviewer-a",
                "submitted_at": "2026-04-12T10:00:00",
                "commit_id": "commit-101",
            }
        ]
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_request_timeline_events.csv"
        ) == [
            {
                "period_key": "2026-04",
                "repository_full_name": "acme/api",
                "pull_request_number": "20",
                "event_id": "201",
                "event": "review_requested",
                "actor_login": "alice",
                "created_at": "2026-04-10T09:30:00",
                "requested_reviewer_login": "reviewer-a",
                "requested_team_name": "",
            }
        ]

    def test_refreshes_prior_anchor_periods_during_incremental_created_at_runs(
        self,
        tmp_path,
    ) -> None:
        """Refresh anchor periods touched by incremental updates while still keeping the active period output."""
        # Given
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(
                self._build_pull_request_record(
                    number=20,
                    updated_at="2026-04-12T09:00:00",
                    created_at="2026-03-31T23:00:00",
                ),
            ),
            failures=(),
        )

        # When
        result = writer.write(config, collection)

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-03" / "pull_requests.csv"
        ) == [
            {
                "period_key": "2026-03",
                "repository_full_name": "acme/api",
                "pull_request_number": "20",
                "title": "PR 20",
                "state": "closed",
                "draft": "False",
                "merged": "True",
                "author_login": "alice",
                "created_at": "2026-03-31T23:00:00",
                "updated_at": "2026-04-12T09:00:00",
                "closed_at": "2026-04-12T09:00:00",
                "merged_at": "2026-04-12T09:00:00",
                "additions": "12",
                "deletions": "4",
                "changed_files": "3",
                "commits": "2",
                "html_url": "https://example.test/pr/20",
            }
        ]
        assert (
            self._read_csv_rows(
                tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
            )
            == []
        )

    def test_writes_empty_backfill_snapshots_for_requested_periods_without_rows(
        self,
        tmp_path,
    ) -> None:
        """Write header-only snapshots for each requested backfill period even when no pull requests are returned."""
        # Given
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-05-18",
            mode=RunMode.BACKFILL,
            backfill_start="2026-03-01",
            backfill_end="2026-04-30",
            output_dir=tmp_path,
        )
        collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(),
            failures=(),
        )

        # When
        result = writer.write(config, collection)

        # Then
        assert [period.key for period in result.periods] == ["2026-03", "2026-04"]
        assert (
            self._read_csv_rows(
                tmp_path / "raw" / "month" / "created_at" / "2026-03" / "pull_requests.csv"
            )
            == []
        )
        assert (
            self._read_csv_rows(
                tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_request_reviews.csv"
            )
            == []
        )

    def test_prunes_stale_period_snapshots_during_full_rebuild(
        self,
        tmp_path,
    ) -> None:
        """Remove obsolete period directories during a full rebuild before rewriting current snapshots."""
        # Given
        stale_period_dir = tmp_path / "raw" / "month" / "created_at" / "2026-03"
        stale_period_dir.mkdir(parents=True)
        (stale_period_dir / "pull_requests.csv").write_text(
            "stale snapshot\n",
            encoding="utf-8",
        )
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-04-18",
            mode=RunMode.FULL,
            output_dir=tmp_path,
        )
        timestamp = datetime.fromisoformat("2026-04-12T09:00:00")
        collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(
                PullRequestRecord(
                    repository_full_name="acme/api",
                    number=30,
                    title="PR 30",
                    state="closed",
                    draft=False,
                    merged=True,
                    author_login="alice",
                    created_at=timestamp,
                    updated_at=timestamp,
                    closed_at=timestamp,
                    merged_at=timestamp,
                    additions=12,
                    deletions=4,
                    changed_files=3,
                    commits=2,
                    html_url="https://example.test/pr/30",
                ),
            ),
            failures=(),
        )

        # When
        result = writer.write(config, collection)

        # Then
        assert [period.key for period in result.periods] == ["2026-04"]
        assert stale_period_dir.exists() is False
        assert (tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv").exists()

    def test_rewrites_incremental_snapshots_idempotently_on_rerun(
        self,
        tmp_path,
    ) -> None:
        """Rewrite the same incremental snapshot deterministically across repeated runs."""
        # Given
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(
                self._build_pull_request_record(
                    number=20,
                    updated_at="2026-04-12T09:00:00",
                    reviews=(
                        self._build_pull_request_review_record(
                            review_id=101,
                            submitted_at="2026-04-12T10:00:00",
                            author_login="reviewer-a",
                        ),
                    ),
                    timeline_events=(
                        self._build_pull_request_timeline_event_record(
                            event_id=201,
                            event="review_requested",
                            created_at="2026-04-10T09:30:00",
                            actor_login="alice",
                            requested_reviewer_login="reviewer-a",
                        ),
                    ),
                ),
            ),
            failures=(),
        )

        # When
        first_result = writer.write(config, collection)
        first_file_contents = self._read_snapshot_texts(first_result)
        second_result = writer.write(config, collection)
        second_file_contents = self._read_snapshot_texts(second_result)

        # Then
        assert [period.key for period in first_result.periods] == ["2026-04"]
        assert [period.key for period in second_result.periods] == ["2026-04"]
        assert first_file_contents == second_file_contents
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        ) == [
            {
                "period_key": "2026-04",
                "repository_full_name": "acme/api",
                "pull_request_number": "20",
                "title": "PR 20",
                "state": "closed",
                "draft": "False",
                "merged": "True",
                "author_login": "alice",
                "created_at": "2026-04-12T09:00:00",
                "updated_at": "2026-04-12T09:00:00",
                "closed_at": "2026-04-12T09:00:00",
                "merged_at": "2026-04-12T09:00:00",
                "additions": "12",
                "deletions": "4",
                "changed_files": "3",
                "commits": "2",
                "html_url": "https://example.test/pr/20",
            }
        ]

    def test_overwrites_stale_active_period_rows_on_rerun(
        self,
        tmp_path,
    ) -> None:
        """Overwrite active-period files on rerun so stale rows from prior snapshots are removed."""
        # Given
        writer = NormalizedRawSnapshotWriter()
        config = self._build_run_config(
            as_of="2026-04-18",
            output_dir=tmp_path,
        )
        first_collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(
                self._build_pull_request_record(
                    number=20,
                    updated_at="2026-04-12T09:00:00",
                    reviews=(
                        self._build_pull_request_review_record(
                            review_id=101,
                            submitted_at="2026-04-12T10:00:00",
                            author_login="reviewer-a",
                        ),
                    ),
                    timeline_events=(
                        self._build_pull_request_timeline_event_record(
                            event_id=201,
                            event="review_requested",
                            created_at="2026-04-10T09:30:00",
                            actor_login="alice",
                            requested_reviewer_login="reviewer-a",
                        ),
                    ),
                ),
                self._build_pull_request_record(
                    number=21,
                    updated_at="2026-04-14T09:00:00",
                ),
            ),
            failures=(),
        )
        second_collection = PullRequestCollection(
            window=config.collection_window,
            pull_requests=(
                self._build_pull_request_record(
                    number=20,
                    updated_at="2026-04-12T09:00:00",
                    title="PR 20 rerun",
                ),
            ),
            failures=(),
        )

        # When
        writer.write(config, first_collection)
        writer.write(config, second_collection)

        # Then
        assert self._read_csv_rows(
            tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_requests.csv"
        ) == [
            {
                "period_key": "2026-04",
                "repository_full_name": "acme/api",
                "pull_request_number": "20",
                "title": "PR 20 rerun",
                "state": "closed",
                "draft": "False",
                "merged": "True",
                "author_login": "alice",
                "created_at": "2026-04-12T09:00:00",
                "updated_at": "2026-04-12T09:00:00",
                "closed_at": "2026-04-12T09:00:00",
                "merged_at": "2026-04-12T09:00:00",
                "additions": "12",
                "deletions": "4",
                "changed_files": "3",
                "commits": "2",
                "html_url": "https://example.test/pr/20",
            }
        ]
        assert (
            self._read_csv_rows(
                tmp_path / "raw" / "month" / "created_at" / "2026-04" / "pull_request_reviews.csv"
            )
            == []
        )
        assert (
            self._read_csv_rows(
                tmp_path
                / "raw"
                / "month"
                / "created_at"
                / "2026-04"
                / "pull_request_timeline_events.csv"
            )
            == []
        )

    def test_stops_iterating_paginated_pull_requests_once_the_window_is_exhausted(
        self,
    ) -> None:
        """Stop iterating paginated pull requests once descending results fall behind the collection window."""
        # Given
        paginated_pull_requests = FakePaginatedSequence(
            (
                self._build_pull_request(
                    number=30, updated_at="2026-04-20T09:00:00"
                ),
                self._build_pull_request(
                    number=20, updated_at="2026-04-12T09:00:00"
                ),
            ),
            (
                self._build_pull_request(
                    number=10, updated_at="2026-03-31T23:59:00"
                ),
                self._build_pull_request(
                    number=5, updated_at="2026-03-15T09:00:00"
                ),
            ),
            (
                self._build_pull_request(
                    number=1, updated_at="2026-03-01T09:00:00"
                ),
            ),
        )
        repository = self._build_repository(
            "acme/api",
            pull_outcomes=[paginated_pull_requests],
        )
        service = GitHubIngestionService(
            cast(
                GitHubIngestionClientLike,
                FakeGithubClient(
                    organizations={},
                    repositories={"acme/api": [repository]},
                ),
            )
        )
        config = self._build_run_config(as_of="2026-04-18")

        # When
        result = service.fetch_pull_requests(
            config,
            RepositoryInventory(
                organization_login="acme",
                repositories=(self._build_inventory_item("acme/api"),),
            ),
        )

        # Then
        assert [pull_request.number for pull_request in result.pull_requests] == [20]
        assert paginated_pull_requests.page_accesses == [0, 1]
        assert [
            pull_request.number
            for pull_request in paginated_pull_requests.iterated_items
        ] == [30, 20, 10]

    def _build_pull_request_record(
        self,
        *,
        number: int,
        updated_at: str,
        created_at: str | None = None,
        title: str | None = None,
        reviews: tuple[PullRequestReviewRecord, ...] = (),
        timeline_events: tuple[PullRequestTimelineEventRecord, ...] = (),
    ) -> PullRequestRecord:
        """Build a deterministic pull request record for snapshot rerun tests."""
        updated_timestamp = datetime.fromisoformat(updated_at)
        created_timestamp = (
            updated_timestamp
            if created_at is None
            else datetime.fromisoformat(created_at)
        )
        return PullRequestRecord(
            repository_full_name="acme/api",
            number=number,
            title=f"PR {number}" if title is None else title,
            state="closed",
            draft=False,
            merged=True,
            author_login="alice",
            created_at=created_timestamp,
            updated_at=updated_timestamp,
            closed_at=updated_timestamp,
            merged_at=updated_timestamp,
            additions=12,
            deletions=4,
            changed_files=3,
            commits=2,
            html_url=f"https://example.test/pr/{number}",
            reviews=reviews,
            timeline_events=timeline_events,
        )

    def _build_pull_request_review_record(
        self,
        *,
        review_id: int,
        submitted_at: str,
        author_login: str | None,
    ) -> PullRequestReviewRecord:
        """Build a deterministic pull request review record for snapshot rerun tests."""
        return PullRequestReviewRecord(
            review_id=review_id,
            state="APPROVED",
            author_login=author_login,
            submitted_at=datetime.fromisoformat(submitted_at),
            commit_id=f"commit-{review_id}",
        )

    def _build_pull_request_timeline_event_record(
        self,
        *,
        event_id: int,
        event: str,
        created_at: str,
        actor_login: str | None,
        requested_reviewer_login: str | None = None,
        requested_team_name: str | None = None,
    ) -> PullRequestTimelineEventRecord:
        """Build a deterministic pull request timeline event record for snapshot rerun tests."""
        return PullRequestTimelineEventRecord(
            event_id=event_id,
            event=event,
            actor_login=actor_login,
            created_at=datetime.fromisoformat(created_at),
            requested_reviewer_login=requested_reviewer_login,
            requested_team_name=requested_team_name,
        )

    def _read_snapshot_texts(
        self,
        result,
    ) -> dict[str, str]:
        """Read snapshot file contents back for deterministic rerun assertions."""
        period = result.periods[0]
        return {
            "pull_requests": period.pull_requests_path.read_text(encoding="utf-8"),
            "reviews": period.reviews_path.read_text(encoding="utf-8"),
            "timeline_events": period.timeline_events_path.read_text(
                encoding="utf-8"
            ),
        }

    def _build_inventory_item(self, full_name: str) -> RepositoryInventoryItem:
        """Build the minimal repository inventory item required for PR fetching."""
        return RepositoryInventoryItem(
            name=full_name.split("/", 1)[1],
            full_name=full_name,
            default_branch="main",
            private=False,
            archived=False,
            disabled=False,
        )

    def _build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for ingestion tests."""
        return RunConfig.model_validate(
            {
                "org": "acme",
                "output_dir": tempfile.mkdtemp(prefix="orgpulse-test-"),
                **overrides,
            }
        )

    def _build_repository(
        self,
        full_name: str,
        *,
        pull_outcomes: list[PullRequestFetchOutcome] | None = None,
        pull_lookup: dict[int, GitHubPullRequestLike] | None = None,
    ) -> FakeRepository:
        """Build a fake repository with optional pull request outcomes."""
        return FakeRepository(
            full_name=full_name,
            pull_outcomes=[] if pull_outcomes is None else pull_outcomes,
            pull_lookup=pull_lookup,
        )

    def _build_pull_request(
        self,
        *,
        number: int,
        updated_at: str,
        created_at: str | None = None,
        review_outcomes: list[ReviewFetchOutcome] | None = None,
        timeline_outcomes: list[TimelineFetchOutcome] | None = None,
    ) -> FakePullRequest:
        """Build a fake pull request object with deterministic metric fields."""
        updated_timestamp = datetime.fromisoformat(updated_at)
        created_timestamp = (
            updated_timestamp
            if created_at is None
            else datetime.fromisoformat(created_at)
        )
        return FakePullRequest(
            number=number,
            title=f"PR {number}",
            state="closed",
            draft=False,
            merged=True,
            user=FakeActor(login="alice"),
            created_at=created_timestamp,
            updated_at=updated_timestamp,
            closed_at=updated_timestamp,
            merged_at=updated_timestamp,
            additions=12,
            deletions=4,
            changed_files=3,
            commits=2,
            html_url=f"https://example.test/pr/{number}",
            review_outcomes=[()] if review_outcomes is None else review_outcomes,
            timeline_outcomes=[()] if timeline_outcomes is None else timeline_outcomes,
        )

    def _build_review(
        self,
        *,
        review_id: int,
        state: str,
        submitted_at: str,
        author_login: str | None,
    ) -> FakeReview:
        """Build a fake pull request review record for enrichment tests."""
        return FakeReview(
            id=review_id,
            state=state,
            submitted_at=datetime.fromisoformat(submitted_at),
            user=None if author_login is None else FakeActor(login=author_login),
            commit_id=f"commit-{review_id}",
        )

    def _build_timeline_event(
        self,
        *,
        event_id: int,
        event: str,
        created_at: str,
        actor_login: str | None,
        requested_reviewer_login: str | None = None,
        requested_team_name: str | None = None,
    ) -> FakeTimelineEvent:
        """Build a fake timeline event record for review timing enrichment tests."""
        return FakeTimelineEvent(
            id=event_id,
            event=event,
            created_at=datetime.fromisoformat(created_at),
            actor=None if actor_login is None else FakeActor(login=actor_login),
            requested_reviewer=None
            if requested_reviewer_login is None
            else FakeActor(login=requested_reviewer_login),
            requested_team=None
            if requested_team_name is None
            else FakeTeam(name=requested_team_name),
            raw_data={
                "requested_reviewer": (
                    None
                    if requested_reviewer_login is None
                    else {"login": requested_reviewer_login}
                ),
                "requested_team": (
                    None
                    if requested_team_name is None
                    else {"name": requested_team_name}
                ),
            },
        )

    def _build_github_exception(
        self,
        *,
        status: int,
        message: str,
        headers: dict[str, str] | None = None,
    ) -> GithubException:
        """Build a GitHub exception with message and headers for retry tests."""
        return GithubException(status, {"message": message}, headers)

    def _build_graphql_pull_request_response(
        self,
        *,
        has_next_page: bool,
        nodes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a fake GraphQL pull request page response."""
        return {
            "data": {
                "repository": {
                    "pullRequests": {
                        "pageInfo": {
                            "hasNextPage": has_next_page,
                            "endCursor": "cursor-1" if has_next_page else None,
                        },
                        "nodes": nodes,
                    }
                }
            }
        }

    def _build_graphql_pull_request_node(
        self,
        *,
        number: int,
        updated_at: str,
        created_at: str | None = None,
        reviews: tuple[dict[str, Any], ...] = (),
        timeline_events: tuple[dict[str, Any], ...] = (),
        review_has_next_page: bool = False,
        timeline_has_next_page: bool = False,
    ) -> dict[str, Any]:
        """Build a fake GraphQL pull request node for ingestion tests."""
        return {
            "number": number,
            "title": f"Pull request {number}",
            "updatedAt": updated_at,
            "createdAt": updated_at if created_at is None else created_at,
            "closedAt": "2026-04-12T09:00:00Z",
            "mergedAt": "2026-04-12T09:00:00Z",
            "state": "MERGED",
            "isDraft": False,
            "additions": 8,
            "deletions": 2,
            "changedFiles": 2,
            "commits": {"totalCount": 1},
            "url": f"https://example.test/pr/{number}",
            "author": {"login": "alice"},
            "reviews": {
                "pageInfo": {"hasNextPage": review_has_next_page},
                "nodes": list(reviews),
            },
            "timelineItems": {
                "pageInfo": {"hasNextPage": timeline_has_next_page},
                "nodes": list(timeline_events),
            },
        }

    def _build_graphql_review_node(
        self,
        *,
        review_id: int,
        submitted_at: str,
        author_login: str,
    ) -> dict[str, Any]:
        """Build a fake GraphQL review node."""
        return {
            "databaseId": review_id,
            "state": "APPROVED",
            "submittedAt": submitted_at,
            "author": {"login": author_login},
            "commit": {"oid": f"commit-{review_id}"},
        }

    def _build_graphql_timeline_event_node(
        self,
        *,
        event_type: str,
        node_id: str,
        created_at: str,
        actor_login: str,
        requested_reviewer_login: str | None = None,
        requested_team_name: str | None = None,
    ) -> dict[str, Any]:
        """Build a fake GraphQL timeline event node."""
        requested_reviewer: dict[str, Any] | None = None
        if requested_reviewer_login is not None:
            requested_reviewer = {
                "__typename": "User",
                "login": requested_reviewer_login,
            }
        elif requested_team_name is not None:
            requested_reviewer = {
                "__typename": "Team",
                "name": requested_team_name,
            }

        return {
            "__typename": event_type,
            "id": node_id,
            "createdAt": created_at,
            "actor": {"login": actor_login},
            "requestedReviewer": requested_reviewer,
        }

    def _read_csv_rows(self, path) -> list[dict[str, str]]:
        """Read snapshot rows back as dictionaries for integration-style assertions."""
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))


class FakeGithubClient:
    def __init__(
        self,
        *,
        organizations: dict[str, FakeOrganization],
        repositories: dict[str, list[RepositoryFetchOutcome]],
        graphql_requester: "FakeGraphQLRequester | None" = None,
    ) -> None:
        self._organizations = organizations
        self._repositories = {
            name: deque(outcomes) for name, outcomes in repositories.items()
        }
        self.requester = graphql_requester

    def get_organization(self, org: str) -> FakeOrganization:
        return self._organizations[org]

    def get_repo(self, full_name: str) -> GitHubRepositoryLike:
        return resolve_outcome(self._repositories[full_name])


class FakeOrganization:
    def __init__(
        self, *, login: str, repo_outcomes: list[RepositoryInventoryOutcome]
    ) -> None:
        self.login = login
        self._repo_outcomes = deque(repo_outcomes)

    def get_repos(self, *, type: str, sort: str, direction: str) -> RepositoryBatch:
        return resolve_outcome(self._repo_outcomes)


class FakeRepository:
    def __init__(
        self,
        *,
        full_name: str,
        pull_outcomes: list[PullRequestFetchOutcome],
        pull_lookup: dict[int, GitHubPullRequestLike] | None = None,
    ) -> None:
        self.name = full_name.split("/", 1)[1]
        self.full_name = full_name
        self.default_branch = "main"
        self.private = False
        self.archived = False
        self.disabled = False
        self._pull_outcomes = deque(pull_outcomes)
        self._pull_lookup = {} if pull_lookup is None else dict(pull_lookup)
        self.get_pull_calls: list[int] = []

    def get_pulls(self, *, state: str, sort: str, direction: str) -> PullRequestBatch:
        return resolve_outcome(self._pull_outcomes)

    def get_pull(self, number: int) -> GitHubPullRequestLike:
        self.get_pull_calls.append(number)
        return self._pull_lookup[number]


class FakePullRequest:
    def __init__(
        self,
        *,
        number: int,
        title: str,
        state: str,
        draft: bool,
        merged: bool,
        user: GitHubActorLike | None,
        created_at: datetime,
        updated_at: datetime,
        closed_at: datetime | None,
        merged_at: datetime | None,
        additions: int,
        deletions: int,
        changed_files: int,
        commits: int,
        html_url: str,
        review_outcomes: list[ReviewFetchOutcome],
        timeline_outcomes: list[TimelineFetchOutcome],
    ) -> None:
        self.number = number
        self.title = title
        self.state = state
        self.draft = draft
        self.merged = merged
        self.user = user
        self.created_at = created_at
        self.updated_at = updated_at
        self.closed_at = closed_at
        self.merged_at = merged_at
        self.additions = additions
        self.deletions = deletions
        self.changed_files = changed_files
        self.commits = commits
        self.html_url = html_url
        self._review_outcomes = deque(review_outcomes)
        self._timeline_outcomes = deque(timeline_outcomes)

    def get_reviews(self) -> ReviewBatch:
        return resolve_outcome(self._review_outcomes)

    def as_issue(self) -> FakeIssue:
        return FakeIssue(self._timeline_outcomes)


class FakeIssue:
    def __init__(self, timeline_outcomes: deque[TimelineFetchOutcome]) -> None:
        self._timeline_outcomes = timeline_outcomes

    def get_timeline(self) -> TimelineEventBatch:
        return resolve_outcome(self._timeline_outcomes)


class FakePaginatedSequence(Sequence[T]):
    def __init__(self, *pages: Sequence[T]) -> None:
        self._pages = tuple(tuple(page) for page in pages)
        self._flattened = tuple(item for page in self._pages for item in page)
        self.page_accesses: list[int] = []
        self.iterated_items: list[T] = []

    def __iter__(self):
        for page_index, page in enumerate(self._pages):
            self.page_accesses.append(page_index)
            for item in page:
                self.iterated_items.append(item)
                yield item

    def __len__(self) -> int:
        return len(self._flattened)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]: ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        return self._flattened[index]


@dataclass(frozen=True)
class FakeActor:
    login: str


@dataclass(frozen=True)
class FakeTeam:
    name: str


@dataclass(frozen=True)
class FakeReview:
    id: int
    state: str
    user: GitHubActorLike | None
    submitted_at: datetime | None
    commit_id: str | None


@dataclass(frozen=True)
class FakeTimelineEvent:
    id: int
    event: str
    actor: GitHubActorLike | None
    created_at: datetime | None
    requested_reviewer: GitHubActorLike | None
    requested_team: GitHubTeamLike | None
    raw_data: dict[str, object]


class FakeGraphQLRequester:
    def __init__(
        self,
        *,
        responses: list[dict[str, Any] | GithubException | RequestException],
    ) -> None:
        self._responses = deque(responses)
        self.variables: list[dict[str, Any]] = []

    def graphql_query(
        self,
        query: str,
        variables: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        self.variables.append(dict(variables))
        return {}, resolve_outcome(self._responses)


def resolve_outcome(outcomes: deque[T | GithubException | RequestException]) -> T:
    outcome = outcomes.popleft()
    if isinstance(outcome, (GithubException, RequestException)):
        raise outcome
    return outcome
