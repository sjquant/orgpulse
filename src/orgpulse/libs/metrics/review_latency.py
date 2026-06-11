from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class ReviewLike(Protocol):
    """Describe the review fields needed for review-latency calculations."""

    @property
    def review_id(self) -> int: ...

    @property
    def author_login(self) -> str | None: ...

    @property
    def state(self) -> str: ...

    @property
    def submitted_at(self) -> datetime | None: ...


class TimelineEventLike(Protocol):
    """Describe the timeline fields needed for review-latency calculations."""

    @property
    def event_id(self) -> object: ...

    @property
    def event(self) -> str: ...

    @property
    def created_at(self) -> datetime | None: ...

    @property
    def requested_reviewer_login(self) -> str | None: ...

    @property
    def requested_team_name(self) -> str | None: ...


@dataclass(frozen=True)
class ReviewCycleMarkers:
    """Store the externally visible review-cycle timing markers."""

    review_ready_at: datetime | None
    review_requested_at: datetime | None
    review_started_at: datetime | None
    first_review_at: datetime | None


def review_cycle_markers(
    *,
    author_login: str | None,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
    reviews: Sequence[ReviewLike],
) -> ReviewCycleMarkers:
    """Calculate first external review timing from normalized PR facts."""

    first_review_at = _first_external_review_at(
        author_login=author_login,
        reviews=reviews,
    )
    reference_at = first_review_at or _last_review_cycle_reference(
        created_at=created_at,
        timeline_events=timeline_events,
    )
    review_ready_at = _review_ready_at(
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
        reference_at=reference_at,
    )
    review_requested_at = _review_requested_at(
        timeline_events=timeline_events,
        reference_at=reference_at,
    )
    return ReviewCycleMarkers(
        review_ready_at=review_ready_at,
        review_requested_at=review_requested_at,
        review_started_at=_review_started_at(
            review_ready_at=review_ready_at,
            review_requested_at=review_requested_at,
        ),
        first_review_at=first_review_at,
    )


def first_review_hours(
    *,
    author_login: str | None,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
    reviews: Sequence[ReviewLike],
) -> float | None:
    """Calculate hours from review readiness/request to the first external review."""

    markers = review_cycle_markers(
        author_login=author_login,
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
        reviews=reviews,
    )
    return hours_between(markers.review_started_at, markers.first_review_at)


def approval_hours(
    *,
    author_login: str | None,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
    reviews: Sequence[ReviewLike],
) -> float | None:
    """Calculate hours until the final external approval decision."""

    final_decision_review = _final_external_decision_review(
        author_login=author_login,
        reviews=reviews,
    )
    if final_decision_review is None or final_decision_review.state != "APPROVED":
        return None
    review_started_at = _approval_started_at(
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
        approval_submitted_at=final_decision_review.submitted_at,
    )
    return hours_between(review_started_at, final_decision_review.submitted_at)


def hours_between(start_at: datetime | None, end_at: datetime | None) -> float | None:
    """Calculate rounded hours between two datetimes."""

    if start_at is None or end_at is None:
        return None
    start_at, end_at = _matching_datetime_awareness(start_at, end_at)
    if end_at < start_at:
        return None
    return round((end_at - start_at).total_seconds() / 3600, 2)


def _final_external_decision_review(
    *,
    author_login: str | None,
    reviews: Sequence[ReviewLike],
) -> ReviewLike | None:
    final_decision_review = None
    final_decision_key: tuple[datetime, int] | None = None
    for review in reviews:
        if review.submitted_at is None:
            continue
        if review.state not in {"APPROVED", "CHANGES_REQUESTED"}:
            continue
        if _same_login(review.author_login, author_login):
            continue
        decision_key = (review.submitted_at, review.review_id)
        if final_decision_key is None or decision_key > final_decision_key:
            final_decision_key = decision_key
            final_decision_review = review
    return final_decision_review


def _approval_started_at(
    *,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
    approval_submitted_at: datetime | None,
) -> datetime | None:
    if approval_submitted_at is None:
        return None
    review_ready_at = _review_ready_at(
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
        reference_at=approval_submitted_at,
    )
    review_requested_at = _review_requested_at(
        timeline_events=timeline_events,
        reference_at=approval_submitted_at,
    )
    return _review_started_at(
        review_ready_at=review_ready_at,
        review_requested_at=review_requested_at,
    )


def _first_external_review_at(
    *,
    author_login: str | None,
    reviews: Sequence[ReviewLike],
) -> datetime | None:
    for review in _reviews_by_submission_time(reviews):
        if review.submitted_at is None:
            continue
        if _same_login(review.author_login, author_login):
            continue
        return review.submitted_at
    return None


def _reviews_by_submission_time(
    reviews: Sequence[ReviewLike],
) -> tuple[ReviewLike, ...]:
    return tuple(
        sorted(
            reviews,
            key=lambda review: (
                review.submitted_at.isoformat() if review.submitted_at else "",
                review.review_id,
            ),
        )
    )


def _last_review_cycle_reference(
    *,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
) -> datetime:
    event_times = [
        event.created_at for event in timeline_events if event.created_at is not None
    ]
    return max(event_times, default=created_at)


def _review_ready_at(
    *,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
    reference_at: datetime,
) -> datetime | None:
    review_ready_at = _initial_review_ready_at(
        draft=draft,
        created_at=created_at,
        timeline_events=timeline_events,
    )
    for event in _timeline_events_by_created_time(timeline_events):
        if event.created_at is None:
            continue
        if event.created_at > reference_at:
            break
        if event.event == "converted_to_draft":
            review_ready_at = None
        elif event.event == "ready_for_review":
            review_ready_at = event.created_at
    return review_ready_at


def _initial_review_ready_at(
    *,
    draft: bool,
    created_at: datetime,
    timeline_events: Sequence[TimelineEventLike],
) -> datetime | None:
    first_transition_event = _first_draft_transition_event(timeline_events)
    if first_transition_event == "ready_for_review":
        return None
    if first_transition_event == "converted_to_draft":
        return created_at
    if draft:
        return None
    return created_at


def _first_draft_transition_event(
    timeline_events: Sequence[TimelineEventLike],
) -> str | None:
    for event in _timeline_events_by_created_time(timeline_events):
        if event.created_at is None:
            continue
        if event.event in {"converted_to_draft", "ready_for_review"}:
            return event.event
    return None


def _review_requested_at(
    *,
    timeline_events: Sequence[TimelineEventLike],
    reference_at: datetime,
) -> datetime | None:
    active_requests: set[str] = set()
    review_requested_at: datetime | None = None
    for event in _timeline_events_by_created_time(timeline_events):
        if event.created_at is None:
            continue
        if event.created_at > reference_at:
            break
        if event.event == "converted_to_draft":
            active_requests.clear()
            review_requested_at = None
            continue
        if event.event == "review_requested":
            request_key = _request_key(event)
            if request_key not in active_requests and not active_requests:
                review_requested_at = event.created_at
            active_requests.add(request_key)
            continue
        if event.event == "review_request_removed":
            active_requests.discard(_request_key(event))
            if not active_requests:
                review_requested_at = None
    return review_requested_at


def _timeline_events_by_created_time(
    timeline_events: Sequence[TimelineEventLike],
) -> tuple[TimelineEventLike, ...]:
    return tuple(
        sorted(
            timeline_events,
            key=lambda event: (
                event.created_at.isoformat() if event.created_at else "",
                str(event.event_id),
            ),
        )
    )


def _request_key(event: TimelineEventLike) -> str:
    if event.requested_reviewer_login is not None:
        return f"user:{event.requested_reviewer_login.lower()}"
    if event.requested_team_name is not None:
        return f"team:{event.requested_team_name.lower()}"
    return f"event:{event.event_id}"


def _review_started_at(
    *,
    review_ready_at: datetime | None,
    review_requested_at: datetime | None,
) -> datetime | None:
    if review_ready_at is None:
        return None
    if review_requested_at is None:
        return review_ready_at
    return max(review_ready_at, review_requested_at)


def _same_login(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    return left.lower() == right.lower()


def _matching_datetime_awareness(
    start_at: datetime,
    end_at: datetime,
) -> tuple[datetime, datetime]:
    if start_at.tzinfo is None and end_at.tzinfo is not None:
        return start_at, end_at.replace(tzinfo=None)
    if start_at.tzinfo is not None and end_at.tzinfo is None:
        return start_at.replace(tzinfo=None), end_at
    return start_at, end_at
