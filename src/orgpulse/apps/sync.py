from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time, timedelta, timezone

from orgpulse.common.config import Config
from orgpulse.common.models import PullRequest
from orgpulse.libs.github import (
    GraphQLRequest,
    iter_pull_requests,
    iter_repositories,
    pull_request_commit_counts,
)
from orgpulse.libs.store import Store

# Re-read a little before the previous sync so updates racing it are not missed.
SYNC_OVERLAP = timedelta(minutes=10)
# GitHub answers a page of pull requests slowly, so repositories are fetched in parallel.
FETCH_WORKERS = 4


def sync(
    config: Config,
    request: GraphQLRequest,
    *,
    on_repository: Callable[[str, int], None] = lambda full_name, count: None,
) -> int:
    """Fetch merged pull requests changed since the last sync; return how many were saved."""

    started_at = datetime.now(timezone.utc)
    floor = datetime.combine(config.since, time.min, config.timezone)
    saved = 0
    with Store(config.database) as store:
        updated_since: dict[str, datetime] = {}
        for repository in iter_repositories(request, config.org):
            if not config.includes_repository(repository.full_name):
                continue
            # Repositories arrive most recently pushed first.
            if repository.pushed_at is None or repository.pushed_at < floor:
                break
            synced_at = store.synced_at(repository.full_name, since=config.since)
            updated_since[repository.full_name] = (
                synced_at - SYNC_OVERLAP if synced_at else floor
            )

        missing_counts = store.missing_commit_counts()

        def fetch(full_name: str) -> tuple[list[PullRequest], dict[int, int]]:
            pull_requests = [
                pull_request
                for pull_request in iter_pull_requests(
                    request, full_name, updated_since=updated_since[full_name]
                )
                if pull_request.created_at >= floor
            ]
            fetched = {pr.number for pr in pull_requests}
            missing = [
                number
                for number in missing_counts.get(full_name, [])
                if number not in fetched
            ]
            counts = pull_request_commit_counts(request, full_name, missing)
            return pull_requests, counts

        executor = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
        try:
            futures = {executor.submit(fetch, name): name for name in updated_since}
            for future in as_completed(futures):
                full_name = futures[future]
                pull_requests, counts = future.result()
                store.save(
                    full_name, pull_requests, since=config.since, synced_at=started_at
                )
                store.save_commit_counts(full_name, counts)
                saved += len(pull_requests)
                on_repository(full_name, len(pull_requests))
        finally:
            # On failure or Ctrl+C, drop queued repositories instead of fetching them.
            executor.shutdown(cancel_futures=True)
    return saved
