from __future__ import annotations

from datetime import date

from orgpulse.models import PeriodStatePayload, TimeAnchorContextPayload


def build_time_anchor_context(
    time_anchor: str,
) -> TimeAnchorContextPayload:
    """Build the shared time-anchor description used across report payloads."""

    scope = time_anchor_scope(time_anchor)
    return TimeAnchorContextPayload(
        field=time_anchor,
        scope=scope,
        description=(
            "All counts and summaries in this file are grouped by "
            f"{scope}."
        ),
    )


def time_anchor_scope(
    time_anchor: str,
) -> str:
    """Return the canonical scope string for a time anchor."""

    return f"pull_request.{time_anchor}"


def build_anchored_metric_label(
    label: str,
    time_anchor: str,
) -> str:
    """Append the canonical time-anchor scope to a metric label."""

    return f"{label} ({time_anchor_scope(time_anchor)})"


def build_period_state_payload(
    *,
    period_grain: str,
    start_date: date,
    end_date: date,
    closed: bool,
    as_of: date,
    since: date | None = None,
    until: date | None = None,
) -> PeriodStatePayload:
    """Build the shared period-state payload used across reporting layers."""

    observed_through_date = _observed_through_date(
        start_date=start_date,
        end_date=end_date,
        closed=closed,
        as_of=as_of,
        until=until,
    )
    status = _period_status(closed)
    is_partial = (
        not closed
        or (since is not None and since > start_date)
        or observed_through_date < end_date
    )
    return PeriodStatePayload(
        status=status,
        label=_period_state_label(
            period_grain=period_grain,
            closed=closed,
            is_partial=is_partial,
        ),
        is_open=not closed,
        is_closed=closed,
        is_partial=is_partial,
        observed_through_date=observed_through_date.isoformat(),
        open_week=not closed and period_grain == "week",
        open_month=not closed and period_grain == "month",
    )


def _observed_through_date(
    *,
    start_date: date,
    end_date: date,
    closed: bool,
    as_of: date,
    until: date | None,
) -> date:
    if until is not None and until < end_date:
        return until
    if closed:
        return end_date
    if as_of < start_date:
        return start_date
    if as_of > end_date:
        return end_date
    return as_of


def _period_status(
    closed: bool,
) -> str:
    return "closed" if closed else "open"


def _period_state_label(
    *,
    period_grain: str,
    closed: bool,
    is_partial: bool,
) -> str:
    if not closed:
        return f"open {period_grain}"
    if is_partial:
        return f"partial {period_grain}"
    return f"closed {period_grain}"
