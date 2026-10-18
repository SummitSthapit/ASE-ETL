"""Launch the ASE-ETL Luigi workflow."""

import argparse
import subprocess
import sys
import uuid
from datetime import datetime, timezone

from src.orchestration.config import PROJECT_ROOT

from src.orchestration.source_snapshot import (
    verify_or_pin_snapshot,
)

def main():
    parser = argparse.ArgumentParser(
        description="Execute the ASE-ETL workflow."
    )

    parser.add_argument(
        "--source",
        choices=["local", "minio"],
        default="local",
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help="Reuse an existing run ID when recovering a run.",
    )

    args = parser.parse_args()

    run_id = args.run_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    verify_or_pin_snapshot(
        run_id=run_id,
        source=args.source,
    )

    print("=" * 65, flush=True)
    print("ASE-ETL ORCHESTRATED PIPELINE", flush=True)
    print("Run ID:", run_id, flush=True)
    print("Source:", args.source, flush=True)
    print("=" * 65, flush=True)

    command = [
        sys.executable,
        "-m",
        "luigi",
        "--module",
        "src.orchestration.tasks",
        "RunETLPipeline",
        "--run-id",
        run_id,
        "--source",
        args.source,
        "--scheduler-host",
        "127.0.0.1",
        "--scheduler-port",
        "8082",
    ]

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )

    if result.returncode != 0:
        raise SystemExit(result.returncode)

    # Confirm that the original source snapshot
    # remained unchanged throughout this execution.
    verify_or_pin_snapshot(
        run_id=run_id,
        source=args.source,
    )

    print(
        "PASS: Source snapshot verified after execution",
        flush=True,
    )

    # Confirm that Luigi's final reporting artifact
    # was produced for this exact run.
    from src.orchestration.config import STATE_ROOT

    state_path = (
        STATE_ROOT
        / f"reports_{run_id}_{args.source}.json"
    )

    if not state_path.is_file():
        print(
            "ERROR: Workflow did not produce "
            "a successful reporting completion marker.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(
        "\nASE-ETL Luigi workflow completed successfully.",
        flush=True,
    )


if __name__ == "__main__":
    main()