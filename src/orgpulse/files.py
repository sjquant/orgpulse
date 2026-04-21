from __future__ import annotations

import csv
import json
import os
import tempfile
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import IO, Any


def atomic_write_csv(
    *,
    path: Path,
    fieldnames: tuple[str, ...],
    rows: Iterable[Mapping[str, object]],
) -> None:
    def write(handle) -> None:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    _atomic_write(path, mode="w", newline="", writer=write)


def atomic_write_json(
    path: Path,
    payload: dict[str, object],
) -> None:
    def write(handle) -> None:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    _atomic_write(path, mode="w", newline="\n", writer=write)


def atomic_write_text(
    path: Path,
    document: str,
) -> None:
    def write(handle) -> None:
        handle.write(document)

    _atomic_write(path, mode="w", newline="\n", writer=write)


def _atomic_write(
    path: Path,
    *,
    mode: str,
    newline: str | None,
    writer: Callable[[IO[Any]], None],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path_text = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text="b" not in mode,
    )
    temp_path = Path(temp_path_text)
    try:
        with os.fdopen(fd, mode, encoding="utf-8", newline=newline) as handle:
            writer(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise
