from __future__ import annotations

from collections.abc import Sequence
from math import ceil, floor

SUPPORTED_DISTRIBUTION_PERCENTILES = frozenset({95, 99, 100})


def validate_distribution_percentile(
    percentile: int,
) -> int:
    """Validate the supported upper-tail percentile cutoff for analysis summaries."""
    if percentile not in SUPPORTED_DISTRIBUTION_PERCENTILES:
        supported_values = ", ".join(
            str(value) for value in sorted(SUPPORTED_DISTRIBUTION_PERCENTILES)
        )
        raise ValueError(
            "distribution percentile must be one of "
            f"{supported_values}"
        )
    return percentile


def trim_upper_tail(
    values: Sequence[int],
    *,
    percentile: int,
) -> tuple[int, ...]:
    """Return values at or below the configured upper-tail percentile threshold."""
    validate_distribution_percentile(percentile)
    if not values:
        return ()
    if percentile == 100:
        return tuple(values)
    threshold = upper_percentile_threshold(values, percentile=percentile)
    if threshold is None:
        return ()
    return tuple(value for value in values if value <= threshold)


def upper_percentile_threshold(
    values: Sequence[int],
    *,
    percentile: int,
) -> float | None:
    """Compute a linearly interpolated percentile threshold for upper-tail trimming."""
    validate_distribution_percentile(percentile)
    if not values:
        return None
    ordered_values = sorted(float(value) for value in values)
    if percentile == 100 or len(ordered_values) == 1:
        return ordered_values[-1]
    position = (len(ordered_values) - 1) * (percentile / 100)
    lower_index = floor(position)
    upper_index = ceil(position)
    lower_value = ordered_values[lower_index]
    upper_value = ordered_values[upper_index]
    interpolation = position - lower_index
    return lower_value + ((upper_value - lower_value) * interpolation)


__all__ = [
    "SUPPORTED_DISTRIBUTION_PERCENTILES",
    "trim_upper_tail",
    "upper_percentile_threshold",
    "validate_distribution_percentile",
]
