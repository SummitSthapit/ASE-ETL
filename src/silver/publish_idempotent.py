
import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import text

from src.silver.publish import (
    ENGINE,
    MINIO_CLIENT,
    SILVER_BUCKET,
    DATASETS,
    dataset_files,
    verify_local,
    load_dataframe,
    sql_types,
)


def calculate_snapshot_fingerprint():
    """
    Produce a deterministic fingerprint from the
    contents of all Silver Parquet datasets.
    """
    digest = hashlib.sha256()

    for dataset in sorted(DATASETS):
        for path in dataset_files(dataset):
            file_hash = hashlib.sha256()

            with path.open("rb") as stream:
                for chunk in iter(
                    lambda: stream.read(1024 * 1024),
                    b"",
                ):
                    file_hash.update(chunk)

            digest.update(
                f"{dataset}/{path.name}:"
                f"{file_hash.hexdigest()}\n".encode()
            )

    return digest.hexdigest()


def ensure_registry(connection):
    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS
                silver._publication_registry (
                publication_id INTEGER PRIMARY KEY,
                snapshot_fingerprint CHAR(64) NOT NULL,
                minio_prefix TEXT NOT NULL,
                employee_count BIGINT NOT NULL,
                timesheet_count BIGINT NOT NULL,
                published_at TIMESTAMPTZ NOT NULL
            )
        """)
    )


def verify_database_counts(connection, counts):
    for table in ("employees", "timesheets"):
        actual = connection.execute(
            text(
                f"SELECT COUNT(*) FROM silver.{table}"
            )
        ).scalar_one()

        if actual != counts[table]:
            raise ValueError(
                f"Database count mismatch: {table}, "
                f"expected {counts[table]}, got {actual}"
            )


def upload_snapshot(prefix, fingerprint, counts):
    """
    Store an immutable content-addressed snapshot.

    Existing objects are compared by SHA-256 rather
    than silently overwritten.
    """
    if not MINIO_CLIENT.bucket_exists(SILVER_BUCKET):
        MINIO_CLIENT.make_bucket(SILVER_BUCKET)

    for dataset in DATASETS:
        for path in dataset_files(dataset):
            object_name = f"{prefix}/{dataset}/{path.name}"

            expected_hash = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()

            exists = False

            try:
                MINIO_CLIENT.stat_object(
                    SILVER_BUCKET, object_name
                )
                exists = True
            except Exception as exc:
                if getattr(exc, "code", None) not in {
                    "NoSuchKey",
                    "NoSuchObject",
                }:
                    raise

            if not exists:
                MINIO_CLIENT.fput_object(
                    SILVER_BUCKET,
                    object_name,
                    str(path),
                )

            response = MINIO_CLIENT.get_object(
                SILVER_BUCKET,
                object_name,
            )

            digest = hashlib.sha256()

            try:
                for chunk in response.stream(1024 * 1024):
                    digest.update(chunk)
            finally:
                response.close()
                response.release_conn()

            if digest.hexdigest() != expected_hash:
                raise ValueError(
                    f"MinIO SHA-256 mismatch: {object_name}"
                )

        print(f"Verified MinIO: {dataset}")

    manifest = {
        "snapshot_fingerprint": fingerprint,
        "prefix": prefix,
        "counts": counts,
        "datasets": DATASETS,
        "status": "prepared",
    }

    # This describes the uploaded snapshot.
    # PostgreSQL's registry determines which snapshot
    # is actually committed as published.
    import io

    payload = json.dumps(
        manifest, indent=2, sort_keys=True
    ).encode("utf-8")

    MINIO_CLIENT.put_object(
        SILVER_BUCKET,
        f"{prefix}/manifest.json",
        io.BytesIO(payload),
        len(payload),
        content_type="application/json",
    )


def publish_database(fingerprint, prefix, counts):
    with ENGINE.begin() as connection:
        # Prevent concurrent publisher transactions from
        # modifying the current publication simultaneously.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(7182401)")
        )

        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS silver")
        )

        ensure_registry(connection)

        previous = connection.execute(
            text("""
                SELECT snapshot_fingerprint
                FROM silver._publication_registry
                WHERE publication_id = 1
            """)
        ).scalar_one_or_none()

        if previous == fingerprint:
            verify_database_counts(connection, counts)

            print("Snapshot already published.")
            print("Skipping PostgreSQL reload.")
            return False

        # Preserve existing tables and their constraints.
        # All deletes and inserts occur in one transaction.
        for dataset in ("timesheets", "employees"):
            connection.execute(
                text(f"DELETE FROM silver.{dataset}")
            )

        for dataset in ("employees", "timesheets"):
            frame = load_dataframe(dataset)

            frame["ingested_at_utc"] = (
                __import__("pandas").to_datetime(
                    frame["ingested_at_utc"],
                    utc=True,
                )
            )

            frame.to_sql(
                name=dataset,
                con=connection,
                schema="silver",
                if_exists="append",
                index=False,
                dtype=sql_types(frame, dataset),
                chunksize=5000,
            )

        verify_database_counts(connection, counts)

        connection.execute(
            text("""
                INSERT INTO silver._publication_registry (
                    publication_id,
                    snapshot_fingerprint,
                    minio_prefix,
                    employee_count,
                    timesheet_count,
                    published_at
                )
                VALUES (
                    1, :fingerprint, :prefix,
                    :employees, :timesheets, :published_at
                )
                ON CONFLICT (publication_id)
                DO UPDATE SET
                    snapshot_fingerprint =
                        EXCLUDED.snapshot_fingerprint,
                    minio_prefix =
                        EXCLUDED.minio_prefix,
                    employee_count =
                        EXCLUDED.employee_count,
                    timesheet_count =
                        EXCLUDED.timesheet_count,
                    published_at =
                        EXCLUDED.published_at
            """),
            {
                "fingerprint": fingerprint,
                "prefix": prefix,
                "employees": counts["employees"],
                "timesheets": counts["timesheets"],
                "published_at": datetime.now(
                    timezone.utc
                ),
            },
        )

    return True


def main():
    print("=" * 65)
    print("ASE-ETL IDEMPOTENT SILVER PUBLISHER")
    print("=" * 65)

    counts = verify_local()

    fingerprint = calculate_snapshot_fingerprint()
    prefix = f"runs/{fingerprint}"

    print("\nSnapshot fingerprint:", fingerprint)
    print("MinIO prefix:", prefix)

    upload_snapshot(prefix, fingerprint, counts)

    changed = publish_database(
        fingerprint, prefix, counts
    )

    print("\nPostgreSQL changed:", changed)
    print("Current snapshot:", prefix)
    print("SILVER PUBLICATION VERIFIED")


if __name__ == "__main__":
    main()
