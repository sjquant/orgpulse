from __future__ import annotations

from dataclasses import dataclass

from orgpulse.common.models import (
    PullRequestCollection,
    PullRequestRecord,
    RunConfig,
)
from orgpulse.libs.github.ingestion import CanonicalRawInventoryStore
from orgpulse.libs.output_store.pipeline import (
    OutputPipelineResult,
    execute_output_pipeline,
)


@dataclass(frozen=True)
class ReaggregationResult:
    config: RunConfig
    repository_count: int
    filtered_pull_requests: tuple[PullRequestRecord, ...]
    collection: PullRequestCollection
    outputs: OutputPipelineResult

    def to_payload(self) -> dict[str, object]:
        return {
            "config": self.config.model_dump(mode="json"),
            "source": {
                "kind": "canonical_raw_inventory",
                "repository_count": self.repository_count,
                "pull_request_count": len(self.filtered_pull_requests),
            },
            "collection": {
                "window": self.collection.window.model_dump(mode="json"),
                "pull_request_count": len(self.collection.pull_requests),
                "failure_count": len(self.collection.failures),
                "failures": [],
            },
            **self.outputs.to_payload_fields(),
        }


def execute_reaggregation(config: RunConfig) -> ReaggregationResult | None:
    canonical_pull_requests = CanonicalRawInventoryStore().load(config)
    if canonical_pull_requests is None:
        return None

    filtered_pull_requests = reaggregate_pull_requests(
        config,
        canonical_pull_requests=canonical_pull_requests,
    )
    collection = PullRequestCollection(
        window=config.collection_window,
        pull_requests=filtered_pull_requests,
        failures=(),
    )
    repository_count = len(
        {
            pull_request.repository_full_name
            for pull_request in filtered_pull_requests
        }
    )
    outputs = execute_output_pipeline(
        config,
        repository_count=repository_count,
        collection=collection,
    )
    return ReaggregationResult(
        config=config,
        repository_count=repository_count,
        filtered_pull_requests=filtered_pull_requests,
        collection=collection,
        outputs=outputs,
    )


def reaggregate_pull_requests(
    config: RunConfig,
    *,
    canonical_pull_requests: tuple[PullRequestRecord, ...],
) -> tuple[PullRequestRecord, ...]:
    return tuple(
        pull_request
        for pull_request in canonical_pull_requests
        if (
            anchor_at := config.time_anchor.pull_request_datetime(pull_request)
        )
        is not None
        and anchor_at.date() <= config.collection_window.end_date
    )


