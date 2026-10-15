
import hashlib
import json
from datetime import datetime, timezone
import pandas as pd

from sqlalchemy import inspect

from src.silver.schema_contract import (
    SCHEMA_VERSION,
    SCHEMA_COLUMNS,
)

from src.silver.migrations import migrate_employee_schema

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
    """
    Create or upgrade the publication registry.

    The caller controls the transaction.
    """
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

    connection.execute(
        text("""
            ALTER TABLE silver._publication_registry
            ADD COLUMN IF NOT EXISTS
                schema_version VARCHAR(20)
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
            "schema_version": SCHEMA_VERSION,
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
    """
    Publish Silver v2 inside one PostgreSQL transaction.

    Includes schema migration, data replacement,
    verification, and publication registry update.
    """
    with ENGINE.begin() as connection:

        # Prevent concurrent publishers from changing
        # the active publication simultaneously.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(7182401)")
        )

        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS silver")
        )

        ensure_registry(connection)

        previous = connection.execute(
            text("""
                SELECT
                    snapshot_fingerprint,
                    schema_version
                FROM silver._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().one_or_none()

        # Ensure the database structure is compatible
        # even when the fingerprint matches.
        existing_columns = {
            column["name"]
            for column in inspect(connection).get_columns(
                "employees",
                schema="silver",
            )
        }

        expected_columns = set(
            SCHEMA_COLUMNS["employees"]
        )

        same_publication = (
            previous is not None
            and previous["snapshot_fingerprint"]
                == fingerprint
            and previous["schema_version"]
                == SCHEMA_VERSION
            and existing_columns == expected_columns
        )

        if same_publication:
            # Also validate restored column types.
            migrate_employee_schema(connection)

            verify_database_counts(
                connection, counts
            )

            print(
                "Snapshot already published. "
                "Skipping PostgreSQL reload."
            )

            return False

        print("\nMIGRATING EMPLOYEE SCHEMA")

        added = migrate_employee_schema(
            connection
        )

        print("Employee columns added:", len(added))

        # Validate the complete employee column set.
        migrated_columns = {
            column["name"]
            for column in inspect(connection).get_columns(
                "employees",
                schema="silver",
            )
        }

        if migrated_columns != expected_columns:
            raise ValueError(
                "PostgreSQL employee columns do not "
                "match the Silver v2 contract"
            )

        print("\nLOADING SILVER V2 DATA")

        # Remove existing rows within this transaction.
        # If any later operation fails, PostgreSQL
        # restores the previous committed records.
        for dataset in ("timesheets", "employees"):
            connection.execute(
                text(
                    f"DELETE FROM silver.{dataset}"
                )
            )

        for dataset in ("employees", "timesheets"):
            frame = load_dataframe(dataset)

            frame["ingested_at_utc"] = pd.to_datetime(
                frame["ingested_at_utc"],
                utc=True,
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

            print(
                f"Loaded {dataset}: "
                f"{len(frame):,} records"
            )

        # Confirm the inserted row counts.
        verify_database_counts(
            connection, counts
        )

        # Verify that PostgreSQL primary-key constraints
        # still exist after the migration and reload.
        inspector = inspect(connection)

        employee_pk = inspector.get_pk_constraint(
            "employees", schema="silver"
        )["constrained_columns"]

        timesheet_pk = inspector.get_pk_constraint(
            "timesheets", schema="silver"
        )["constrained_columns"]

        if employee_pk != ["client_employee_id"]:
            raise ValueError(
                "Employee primary key was not preserved"
            )

        if set(timesheet_pk) != {
            "client_employee_id",
            "punch_in_datetime",
            "punch_out_datetime",
        }:
            raise ValueError(
                "Timesheet primary key was not preserved"
            )

        # Update the registry only after successfully
        # loading and verifying the database tables.
        connection.execute(
            text("""
                INSERT INTO silver._publication_registry (
                    publication_id,
                    snapshot_fingerprint,
                    minio_prefix,
                    schema_version,
                    employee_count,
                    timesheet_count,
                    published_at
                )
                VALUES (
                    1,
                    :fingerprint,
                    :prefix,
                    :schema_version,
                    :employees,
                    :timesheets,
                    :published_at
                )
                ON CONFLICT (publication_id)
                DO UPDATE SET
                    snapshot_fingerprint =
                        EXCLUDED.snapshot_fingerprint,
                    minio_prefix =
                        EXCLUDED.minio_prefix,
                    schema_version =
                        EXCLUDED.schema_version,
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
                "schema_version": SCHEMA_VERSION,
                "employees": counts["employees"],
                "timesheets": counts["timesheets"],
                "published_at": datetime.now(
                    timezone.utc
                ),
            },
        )

        # The context manager commits only when
        # all operations have completed successfully.

    return True


def verify_silver_v2():
    """
    Verify that local Silver v2 outputs exist
    before publishing anything to MinIO or PostgreSQL.
    """
    counts = verify_local()

    for dataset in ("employees", "timesheets"):
        frame = load_dataframe(dataset)

        expected_columns = SCHEMA_COLUMNS[dataset]

        if frame.columns.tolist() != expected_columns:
            raise ValueError(
                f"{dataset}: local Silver schema does not "
                f"match the approved v2 contract. "
                f"Run the Silver v2 pipeline first."
            )

    schema_audit = load_dataframe(
        "audit/schema_evolution"
    )

    if schema_audit.empty:
        raise ValueError("Schema-evolution audit is empty")

    if not schema_audit["schema_version"].eq(
        SCHEMA_VERSION
    ).all():
        raise ValueError(
            "Schema-evolution audit has an incompatible version"
        )

    if int(schema_audit["source_rows"].sum()) != (
        50 + 412571
    ):
        raise ValueError(
            "Schema audit does not reconcile with "
            "the current Bronze snapshot"
        )

    # Published audits must not contain
    # machine-specific absolute source paths.
    from pathlib import PurePosixPath

    for source in schema_audit["source_name"]:
        if (
            not isinstance(source, str)
            or "\\" in source
            or PurePosixPath(source).is_absolute()
            or ".." in PurePosixPath(source).parts
            or ":" in source
        ):
            raise ValueError(
                "Schema audit contains a nonportable source path"
            )

    print(
        f"PASS: Local Silver {SCHEMA_VERSION} "
        f"publication checks"
    )

    return counts


def main():
    print("=" * 65)
    print("ASE-ETL IDEMPOTENT SILVER PUBLISHER")
    print("=" * 65)

    counts = verify_silver_v2()

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
