
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text


# Configuration

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
PARQUET_DIR = ROOT / "data" / "bronze" / "parquet"

CHUNK_SIZE = 50_000
SCHEMA = "bronze"

SOURCE_FILES = [
    "employee_202510161125.csv",
    "timesheet_202509151540.csv",
    "timesheet_202510161121.csv",
    "timesheet_202510161122.csv",
    "timesheet_202510161124.csv",
]

METADATA_COLUMNS = {
    "source_file",
    "source_row_number",
    "ingested_at_utc",
    "ingestion_batch_id",
}

load_dotenv(ROOT / ".env", override=True)

DATABASE_URL = os.environ["DATABASE_URL"]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "ase-bronze")

# Supported values: local, minio
INGEST_SOURCE = os.getenv("INGEST_SOURCE", "local").strip().lower()

# Used only when INGEST_SOURCE=minio
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "ase-source")
SOURCE_PREFIX = os.getenv("SOURCE_PREFIX", "raw/").strip("/")

if INGEST_SOURCE not in {"local", "minio"}:
    raise ValueError(
        "INGEST_SOURCE must be either 'local' or 'minio'."
    )

# Keep source objects separate from the outputs that this job deletes.
if INGEST_SOURCE == "minio" and SOURCE_BUCKET == BRONZE_BUCKET:
    raise ValueError(
        "SOURCE_BUCKET and BRONZE_BUCKET must be different "
        "when INGEST_SOURCE=minio."
    )

MINIO_CLIENT = Minio(
    MINIO_ENDPOINT,
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=MINIO_SECURE,
)

ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)


# Source validation

def validate_sources():
    """Verify every input exists before clearing previous outputs."""

    if INGEST_SOURCE == "local":
        missing = [
            filename
            for filename in SOURCE_FILES
            if not (RAW_DIR / filename).is_file()
        ]

        if missing:
            raise FileNotFoundError(
                f"Missing local source files in {RAW_DIR}: {missing}"
            )

        print(f"Validated {len(SOURCE_FILES)} local source files.")
        return

    if not MINIO_CLIENT.bucket_exists(SOURCE_BUCKET):
        raise FileNotFoundError(
            f"MinIO source bucket does not exist: {SOURCE_BUCKET}"
        )

    missing = []

    for filename in SOURCE_FILES:
        object_name = f"{SOURCE_PREFIX}/{filename}".lstrip("/")

        try:
            MINIO_CLIENT.stat_object(SOURCE_BUCKET, object_name)
        except Exception as exc:
            # MinIO reports missing objects through an S3 error response.
            if getattr(exc, "code", None) in {
                "NoSuchKey",
                "NoSuchObject",
                "NoSuchBucket",
            }:
                missing.append(object_name)
            else:
                raise

    if missing:
        raise FileNotFoundError(
            f"Missing source objects in bucket '{SOURCE_BUCKET}': "
            f"{missing}"
        )

    print(
        f"Validated {len(SOURCE_FILES)} MinIO source objects "
        f"in bucket '{SOURCE_BUCKET}'."
    )

# MinIO output setup and Bronze cleanup

def ensure_bronze_bucket():
    if not MINIO_CLIENT.bucket_exists(BRONZE_BUCKET):
        MINIO_CLIENT.make_bucket(BRONZE_BUCKET)
        print(f"Created Bronze output bucket: {BRONZE_BUCKET}")
    else:
        print(f"Bronze output bucket exists: {BRONZE_BUCKET}")


def clear_previous_bronze():
    """Reset only the five Bronze tables and their output objects."""

    with ENGINE.begin() as connection:
        connection.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        )

        for filename in SOURCE_FILES:
            table_name = f"{Path(filename).stem}_raw"

            connection.execute(
                text(
                    f'DROP TABLE IF EXISTS "{SCHEMA}"."{table_name}"'
                )
            )

    for filename in SOURCE_FILES:
        stem = Path(filename).stem
        output_prefix = f"raw/{stem}/"

        for obj in MINIO_CLIENT.list_objects(
            BRONZE_BUCKET,
            prefix=output_prefix,
            recursive=True,
        ):
            MINIO_CLIENT.remove_object(
                BRONZE_BUCKET,
                obj.object_name,
            )

        shutil.rmtree(
            PARQUET_DIR / stem,
            ignore_errors=True,
        )

    print("Previous Bronze outputs cleared.")


# Read the selected input source

def open_source(filename):
    """
    Return a pandas chunk reader and an optional MinIO response.

    Local mode reads the original pipe-delimited file.
    MinIO mode streams the corresponding source object.
    """

    options = {
        "sep": "|",
        "dtype": str,
        "keep_default_na": False,
        "na_filter": False,
        "encoding": "utf-8-sig",
        "chunksize": CHUNK_SIZE,
    }

    if INGEST_SOURCE == "local":
        return pd.read_csv(RAW_DIR / filename, **options), None

    object_name = f"{SOURCE_PREFIX}/{filename}".lstrip("/")
    response = MINIO_CLIENT.get_object(
        SOURCE_BUCKET,
        object_name,
    )

    try:
        reader = pd.read_csv(response, **options)
    except Exception:
        response.close()
        response.release_conn()
        raise

    return reader, response

# Bronze ingestion

def ingest_file(filename, batch_id):
    stem = Path(filename).stem
    table_name = f"{stem}_raw"

    local_output_dir = PARQUET_DIR / stem
    local_output_dir.mkdir(parents=True, exist_ok=True)

    next_row_number = 1
    total_rows = 0
    part_number = 0

    print(f"\nIngesting: {filename} [source={INGEST_SOURCE}]")

    reader, response = open_source(filename)

    try:
        for chunk in reader:
            collisions = METADATA_COLUMNS.intersection(chunk.columns)

            if collisions:
                raise ValueError(
                    f"{filename} contains reserved metadata columns: "
                    f"{sorted(collisions)}"
                )

            row_count = len(chunk)

            # Add ingestion lineage without changing source files.
            chunk["source_file"] = filename
            chunk["source_row_number"] = range(
                next_row_number,
                next_row_number + row_count,
            )
            chunk["ingested_at_utc"] = (
                datetime.now(timezone.utc).isoformat()
            )
            chunk["ingestion_batch_id"] = batch_id

            parquet_path = (
                local_output_dir / f"part-{part_number:05d}.parquet"
            )

            # Write a local Parquet part.
            chunk.to_parquet(parquet_path, index=False)

            # Store the same part in the Bronze output bucket.
            object_name = f"raw/{stem}/{parquet_path.name}"

            MINIO_CLIENT.fput_object(
                BRONZE_BUCKET,
                object_name,
                str(parquet_path),
            )

            # Preserve source rows, including duplicates and blanks.
            chunk.to_sql(
                name=table_name,
                con=ENGINE,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            next_row_number += row_count
            total_rows += row_count
            part_number += 1

            print(
                f"  Part {part_number}: {row_count:,} rows "
                f"(total {total_rows:,})"
            )
    finally:
        if response is not None:
            response.close()
            response.release_conn()

    if total_rows == 0:
        raise ValueError(f"No data rows found in {filename}")

    print(
        f"Completed {filename}: {total_rows:,} rows, "
        f"{part_number} Parquet part(s)"
    )

    return {
        "source_file": filename,
        "table_name": f"{SCHEMA}.{table_name}",
        "rows_loaded": total_rows,
        "parquet_parts": part_number,
    }

# Main

def main():
    print("Starting Bronze ingestion")
    print(f"Input mode: {INGEST_SOURCE}")
    print(f"Bronze output bucket: {BRONZE_BUCKET}")

    # Validate before deleting any previous Bronze output.
    validate_sources()
    ensure_bronze_bucket()

    batch_id = str(uuid.uuid4())

    print(f"Batch ID: {batch_id}")

    if INGEST_SOURCE == "local":
        print(f"Source directory: {RAW_DIR}")
    else:
        print(f"Source bucket: {SOURCE_BUCKET}")
        print(f"Source prefix: {SOURCE_PREFIX}/")

    clear_previous_bronze()

    results = [
        ingest_file(filename, batch_id)
        for filename in SOURCE_FILES
    ]

    print("\n" + "=" * 65)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 65)

    total_rows = 0

    for result in results:
        print(
            f"{result['table_name']}: "
            f"{result['rows_loaded']:,} rows; "
            f"{result['parquet_parts']} Parquet part(s)"
        )
        total_rows += result["rows_loaded"]

    print(f"\nTotal rows ingested: {total_rows:,}")
    print(f"PostgreSQL schema: {SCHEMA}")
    print(f"MinIO output bucket: {BRONZE_BUCKET}")
    print(f"Batch ID: {batch_id}")
    print("Bronze ingestion completed.")


if __name__ == "__main__":
    main()
