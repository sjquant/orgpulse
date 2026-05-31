from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from github import Auth, Github

from orgpulse.common.models import (
    GitHubTargetContext,
    PullRequestCollection,
    RepositoryInventory,
    RunConfig,
)
from orgpulse.libs.github.auth import GitHubAuthService, resolve_auth_token
from orgpulse.libs.github.ingestion import (
    GitHubIngestionService,
    PullRequestFetchProgress,
)
from orgpulse.libs.output_store.pipeline import (
    OutputPipelineResult,
    execute_output_pipeline,
)


@dataclass(frozen=True)
class SourceRefreshResult:
    config: RunConfig
    github_context: GitHubTargetContext
    inventory: RepositoryInventory
    collection: PullRequestCollection
    outputs: OutputPipelineResult

    def to_payload(self) -> dict[str, object]:
        return {
            "config": self.config.model_dump(mode="json"),
            "github": self.github_context.model_dump(mode="json"),
            "inventory": {
                "organization_login": self.inventory.organization_login,
                "repository_count": len(self.inventory.repositories),
            },
            "collection": {
                "window": self.collection.window.model_dump(mode="json"),
                "pull_request_count": len(self.collection.pull_requests),
                "failure_count": len(self.collection.failures),
                "failures": [
                    failure.model_dump(mode="json")
                    for failure in self.collection.failures
                ],
            },
            **self.outputs.to_payload_fields(),
        }


def execute_source_refresh(
    config: RunConfig,
    *,
    progress_callback: Callable[[PullRequestFetchProgress], None] | None = None,
) -> SourceRefreshResult:
    resolved_token = resolve_auth_token(config)
    github_client = Github(auth=Auth.Token(resolved_token.token))
    github_context = GitHubAuthService(
        github_client, resolved_token.source
    ).validate_access(config)
    ingestion_service = GitHubIngestionService(github_client)
    inventory = ingestion_service.load_repository_inventory(config)
    collection = ingestion_service.fetch_pull_requests(
        config,
        inventory,
        progress_callback=progress_callback,
    )
    outputs = execute_output_pipeline(
        config,
        repository_count=len(inventory.repositories),
        collection=collection,
    )
    if not collection.failures:
        ingestion_service.clear_checkpoint(config)
    return SourceRefreshResult(
        config=config,
        github_context=github_context,
        inventory=inventory,
        collection=collection,
        outputs=outputs,
    )
