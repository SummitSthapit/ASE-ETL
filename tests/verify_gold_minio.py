"""Independently verify the published Gold MinIO snapshot."""

import hashlib
import json

from sqlalchemy import text

from src.silver.publish import ENGINE, MINIO_CLIENT
from src.gold.config import GOLD_BUCKET, GOLD_DATASETS


def sha256_object(bucket, object_name):
    response = MINIO_CLIENT.get_object(bucket, object_name)
    digest = hashlib.sha256()

    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()

    return digest.hexdigest()


def main():
    with ENGINE.connect() as connection:
        publication = connection.execute(
            text("""
                SELECT *
                FROM gold._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().one()

    if publication["publication_status"] != "PUBLISHED":
        raise ValueError("Gold is not marked PUBLISHED")

    prefix = publication["minio_prefix"]

    response = MINIO_CLIENT.get_object(
        GOLD_BUCKET,
        f"{prefix}/manifest.json",
    )

    try:
        manifest = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()

    if manifest["gold_snapshot_fingerprint"] != (
        publication["gold_snapshot_fingerprint"]
    ):
        raise ValueError("Gold fingerprint mismatch")

    if manifest["silver_snapshot_fingerprint"] != (
        publication["silver_snapshot_fingerprint"]
    ):
        raise ValueError("Silver fingerprint mismatch")

    if manifest["gold_model_version"] != (
        publication["gold_model_version"]
    ):
        raise ValueError("Gold model version mismatch")

    if manifest["prefix"] != prefix:
        raise ValueError("MinIO prefix mismatch")

    expected_counts = {
        "dim_employee": publication["employee_count"],
        "dim_date": publication["date_count"],
        "fact_timesheet": publication["timesheet_count"],
    }

    if manifest["counts"] != expected_counts:
        raise ValueError("Manifest record counts mismatch")

    if set(manifest["files"]) != set(GOLD_DATASETS):
        raise ValueError("Unexpected manifest datasets")

    for dataset in GOLD_DATASETS:
        files = manifest["files"][dataset]

        if not files:
            raise ValueError(f"No files for {dataset}")

        total_rows = 0

        for entry in files:
            object_name = entry["object"]

            if not object_name.startswith(
                f"{prefix}/{dataset}/"
            ):
                raise ValueError(
                    f"Unexpected object path: {object_name}"
                )

            actual_hash = sha256_object(
                GOLD_BUCKET,
                object_name,
            )

            if actual_hash != entry["sha256"]:
                raise ValueError(
                    f"Checksum mismatch: {object_name}"
                )

            total_rows += entry["rows"]

        if total_rows != expected_counts[dataset]:
            raise ValueError(
                f"{dataset}: row-count mismatch"
            )

        print(
            f"PASS: {dataset} — "
            f"{total_rows:,} rows, "
            f"{len(files)} verified objects"
        )

    print("\nPASS: Gold MinIO publication integrity verified")


if __name__ == "__main__":
    main()