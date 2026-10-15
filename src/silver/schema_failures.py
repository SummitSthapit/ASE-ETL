
"""Persist diagnostics for incompatible Silver source schemas."""

import json
import os
import tempfile

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.silver.config import PROJECT_ROOT
from src.silver.schema_contract import SCHEMA_VERSION


FAILURE_ROOT = (
    PROJECT_ROOT
    / "data"
    / "diagnostics"
    / "silver"
    / "schema_failures"
)


def record_schema_failure(
    dataset: str,
    source_name: str,
    error: Exception,
    output_root: Path | None = None,
) -> Path:
    """
    Persist a schema failure without changing Silver outputs.

    Records only the dataset, source path, schema version,
    error description, and UTC timestamp.

    The temporary file is replaced atomically after writing.
    """
    root = (
        Path(output_root)
        if output_root is not None
        else FAILURE_ROOT
    )

    root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc)

    report = {
        "dataset": dataset,
        "source_name": source_name,
        "schema_version": SCHEMA_VERSION,
        "status": "REJECTED_SCHEMA",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "detected_at_utc": timestamp.isoformat(),
    }

    filename = (
        f"schema_failure_"
        f"{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}_"
        f"{uuid4().hex}.json"
    )

    destination = root / filename

    fd, temporary_name = tempfile.mkstemp(
        prefix=".schema_failure_",
        suffix=".tmp",
        dir=root,
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)

        os.replace(temporary_name, destination)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)

    return destination
