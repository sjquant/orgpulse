from __future__ import annotations

import csv
from collections.abc import Mapping
from pathlib import Path
from typing import Literal


def read_snapshot_csv_rows(
    path: Path,
    *,
    missing: Literal["error", "empty"] = "error",
) -> tuple[dict[str, str], ...]:
    """Read normalized raw snapshot rows from disk.

    Args:
        path: CSV file to read.
        missing: Missing-file policy for the requested CSV.

    Returns:
        Parsed CSV rows in file order.
    """

    if not path.exists():
        if missing == "empty":
            return ()
        raise RuntimeError(f"local source snapshot file is missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return tuple(csv.DictReader(handle))


def pull_request_row_key(row: Mapping[str, str]) -> tuple[str, str]:
    """Build a stable pull request identity tuple from a raw row.

    Args:
        row: Raw snapshot row containing repository and pull request identifiers.

    Returns:
        A repository-and-number tuple suitable for grouping related rows.
    """

    return row["repository_full_name"], row["pull_request_number"]
