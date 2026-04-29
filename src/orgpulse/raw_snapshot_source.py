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
    if not path.exists():
        if missing == "empty":
            return ()
        raise RuntimeError(f"local source snapshot file is missing: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return tuple(csv.DictReader(handle))


def pull_request_row_key(row: Mapping[str, str]) -> tuple[str, str]:
    return row["repository_full_name"], row["pull_request_number"]
