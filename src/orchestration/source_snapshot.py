"""Pin local source files for Luigi run recovery."""

import hashlib
import json
from pathlib import Path

from src.orchestration.config import (
    PROJECT_ROOT,
    STATE_ROOT,
)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def local_source_snapshot():
    source_dir = PROJECT_ROOT / "data" / "raw"

    files = sorted(source_dir.glob("*.csv"))

    if not files:
        raise RuntimeError("No local source CSV files found.")

    return {
        "source": "local",
        "files": {
            path.name: sha256_file(path)
            for path in files
        },
    }


def verify_or_pin_snapshot(run_id, source):
    """
    Pin the source manifest for a new run.

    Reject retries if local source files have changed.
    """

    if source != "local":
        raise NotImplementedError(
            "MinIO source snapshot verification "
            "has not been implemented yet."
        )

    current = local_source_snapshot()

    snapshot_path = (
        STATE_ROOT / f"source_{run_id}_{source}.json"
    )

    if snapshot_path.exists():
        previous = json.loads(
            snapshot_path.read_text(encoding="utf-8")
        )

        if previous != current:
            raise RuntimeError(
                "Source files changed since this run started. "
                "Start a NEW pipeline run instead."
            )

        print("PASS: Original source snapshot unchanged")
        return

    # Exclusive creation prevents silently overwriting
    # a previously pinned source snapshot.
    with snapshot_path.open(
        "x",
        encoding="utf-8",
    ) as stream:
        json.dump(current, stream, indent=2)

    print("PASS: Source snapshot pinned")