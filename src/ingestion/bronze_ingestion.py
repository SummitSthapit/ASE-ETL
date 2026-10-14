
import argparse
import hashlib
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
PARQUET_DIR = ROOT / "data" / "bronze" / "parquet"

CHUNK_SIZE = 50_000
SCHEMA = "bronze"
REGISTRY_TABLE = "_ingestion_registry"

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
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "ase-source")
SOURCE_PREFIX = os.getenv("SOURCE_PREFIX", "raw/").strip("/")

parser = argparse.ArgumentParser(
    description="Incrementally ingest CSV files into Bronze."
)
parser.add_argument(
    "--source",
    choices=["local", "minio"],
    default=None,
    help="Input source. Defaults to INGEST_SOURCE in .env, then local.",
)
args = parser.parse_args()

INGEST_SOURCE = (
    args.source or os.getenv("INGEST_SOURCE", "local")
).strip().lower()

if INGEST_SOURCE not in {"local", "minio"}:
    raise ValueError("INGEST_SOURCE must be 'local' or 'minio'.")

if INGEST_SOURCE == "minio" and SOURCE_BUCKET == BRONZE_BUCKET:
    raise ValueError(
        "SOURCE_BUCKET and BRONZE_BUCKET must be different."
    )

MINIO_CLIENT = Minio(
    MINIO_ENDPOINT,
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=MINIO_SECURE,
)

ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def table_name_for(filename):
    """Convert a validated CSV filename into a stable table name."""

    return Path(filename).stem.lower().replace("-", "_") + "_raw"


def source_object_name(filename):
    """Build the MinIO source object key."""

    return f"{SOURCE_PREFIX}/{filename}".lstrip("/")


def validate_filename(filename):
    """Reject unsafe or unsupported source filenames."""

    if not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]*\.csv",
        filename,
        flags=re.IGNORECASE,
    ):
        raise ValueError(
            f"Unsupported CSV filename: {filename!r}. "
            "Use letters, numbers, underscores, and hyphens."
        )


# ---------------------------------------------------------
# Discover and validate source files
# ---------------------------------------------------------

def discover_source_files():
    """Discover CSV files directly inside the selected source location."""

    if INGEST_SOURCE == "local":
        if not RAW_DIR.is_dir():
            raise FileNotFoundError(
                f"Local source directory not found: {RAW_DIR}"
            )

        filenames = sorted(
            path.name
            for path in RAW_DIR.iterdir()
            if path.is_file() and path.suffix.lower() == ".csv"
        )

    else:
        if not MINIO_CLIENT.bucket_exists(SOURCE_BUCKET):
            raise FileNotFoundError(
                f"MinIO source bucket does not exist: {SOURCE_BUCKET}"
            )

        prefix = f"{SOURCE_PREFIX}/" if SOURCE_PREFIX else ""

        filenames = sorted(
            obj.object_name[len(prefix):]
            for obj in MINIO_CLIENT.list_objects(
                SOURCE_BUCKET,
                prefix=prefix,
                recursive=True,
            )
            if obj.object_name.startswith(prefix)
            and "/" not in obj.object_name[len(prefix):]
            and obj.object_name.lower().endswith(".csv")
        )

    if not filenames:
        raise ValueError(
            f"No CSV files found for input mode '{INGEST_SOURCE}'."
        )

    for filename in filenames:
        validate_filename(filename)

    table_names = [table_name_for(name) for name in filenames]

    if len(table_names) != len(set(table_names)):
        raise ValueError(
            "Multiple source files map to the same PostgreSQL table."
        )

    print(f"Discovered {len(filenames)} CSV file(s):")
    for filename in filenames:
        print(f"  - {filename}")

    return filenames


def validate_sources(source_files):
    """Confirm discovered inputs exist before modifying Bronze outputs."""

    if INGEST_SOURCE == "local":
        missing = [
            name for name in source_files
            if not (RAW_DIR / name).is_file()
        ]

        if missing:
            raise FileNotFoundError(
                f"Missing local source files: {missing}"
            )

    else:
        if not MINIO_CLIENT.bucket_exists(SOURCE_BUCKET):
            raise FileNotFoundError(
                f"MinIO source bucket does not exist: {SOURCE_BUCKET}"
            )

        missing = []

        for filename in source_files:
            object_name = source_object_name(filename)

            try:
                MINIO_CLIENT.stat_object(SOURCE_BUCKET, object_name)
            except Exception as exc:
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
                f"Missing MinIO source objects: {missing}"
            )

    print(f"Validated {len(source_files)} source file(s).")


# ---------------------------------------------------------
# File fingerprints
# ---------------------------------------------------------

def calculate_file_hash(filename):
    """
    Calculate SHA-256 of the source file's exact bytes.

    The hash allows the pipeline to detect new, unchanged,
    and modified files. It does not rely on file names alone.
    """

    digest = hashlib.sha256()

    if INGEST_SOURCE == "local":
        with (RAW_DIR / filename).open("rb") as file:
            while True:
                data = file.read(1024 * 1024)
                if not data:
                    break
                digest.update(data)

    else:
        response = MINIO_CLIENT.get_object(
            SOURCE_BUCKET,
            source_object_name(filename),
        )

        try:
            while True:
                data = response.read(1024 * 1024)
                if not data:
                    break
                digest.update(data)
        finally:
            response.close()
            response.release_conn()

    return digest.hexdigest()


# ---------------------------------------------------------
# Registry
# ---------------------------------------------------------

def ensure_registry():
    """Create the persistent file-ingestion registry if necessary."""

    with ENGINE.begin() as connection:
        connection.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        )

        connection.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{REGISTRY_TABLE}" (
                    source_file TEXT PRIMARY KEY,
                    content_sha256 CHAR(64) NOT NULL,
                    table_name TEXT NOT NULL,
                    rows_loaded BIGINT NOT NULL,
                    parquet_parts INTEGER NOT NULL,
                    ingestion_batch_id TEXT NOT NULL,
                    ingested_at_utc TIMESTAMPTZ NOT NULL
                )
            """)
        )


def get_registered_file(filename):
    """Return the existing registry entry, if any."""

    with ENGINE.connect() as connection:
        result = connection.execute(
            text(f"""
                SELECT content_sha256, table_name, rows_loaded,
                       parquet_parts, ingestion_batch_id
                FROM "{SCHEMA}"."{REGISTRY_TABLE}"
                WHERE source_file = :filename
            """),
            {"filename": filename},
        ).mappings().first()

    return result


def record_success(filename, content_hash, batch_id, result):
    """Register a file only after its ingestion completes successfully."""

    with ENGINE.begin() as connection:
        connection.execute(
            text(f"""
                INSERT INTO "{SCHEMA}"."{REGISTRY_TABLE}" (
                    source_file,
                    content_sha256,
                    table_name,
                    rows_loaded,
                    parquet_parts,
                    ingestion_batch_id,
                    ingested_at_utc
                )
                VALUES (
                    :filename,
                    :content_hash,
                    :table_name,
                    :rows_loaded,
                    :parquet_parts,
                    :batch_id,
                    :ingested_at
                )
                ON CONFLICT (source_file) DO UPDATE SET
                    content_sha256 = EXCLUDED.content_sha256,
                    table_name = EXCLUDED.table_name,
                    rows_loaded = EXCLUDED.rows_loaded,
                    parquet_parts = EXCLUDED.parquet_parts,
                    ingestion_batch_id = EXCLUDED.ingestion_batch_id,
                    ingested_at_utc = EXCLUDED.ingested_at_utc
            """),
            {
                "filename": filename,
                "content_hash": content_hash,
                "table_name": result["table_name"],
                "rows_loaded": result["rows_loaded"],
                "parquet_parts": result["parquet_parts"],
                "batch_id": batch_id,
                "ingested_at": datetime.now(timezone.utc),
            },
        )


# ---------------------------------------------------------
# Output setup and per-file cleanup
# ---------------------------------------------------------

def ensure_bronze_bucket():
    if not MINIO_CLIENT.bucket_exists(BRONZE_BUCKET):
        MINIO_CLIENT.make_bucket(BRONZE_BUCKET)
        print(f"Created Bronze bucket: {BRONZE_BUCKET}")


def clear_file_outputs(filename):
    """
    Clear only the outputs for the file being ingested or replaced.

    This also removes incomplete outputs left by an earlier failed run.
    """

    stem = Path(filename).stem
    table_name = table_name_for(filename)
    preparer = ENGINE.dialect.identifier_preparer

    quoted_schema = preparer.quote_schema(SCHEMA)
    quoted_table = preparer.quote(table_name)

    with ENGINE.begin() as connection:
        connection.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        )

        connection.execute(
            text(
                f"DROP TABLE IF EXISTS "
                f"{quoted_schema}.{quoted_table}"
            )
        )

        # If a changed file is being replaced, remove its old registry entry.
        connection.execute(
            text(f"""
                DELETE FROM "{SCHEMA}"."{REGISTRY_TABLE}"
                WHERE source_file = :filename
            """),
            {"filename": filename},
        )

    output_prefix = f"raw/{stem}/"

    if MINIO_CLIENT.bucket_exists(BRONZE_BUCKET):
        for obj in MINIO_CLIENT.list_objects(
            BRONZE_BUCKET,
            prefix=output_prefix,
            recursive=True,
        ):
            MINIO_CLIENT.remove_object(
                BRONZE_BUCKET,
                obj.object_name,
            )

    shutil.rmtree(PARQUET_DIR / stem, ignore_errors=True)
    (PARQUET_DIR / stem).mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Read selected input source
# ---------------------------------------------------------

def open_source(filename):
    """Return a chunked pandas reader and optional MinIO response."""

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

    response = MINIO_CLIENT.get_object(
        SOURCE_BUCKET,
        source_object_name(filename),
    )

    try:
        reader = pd.read_csv(response, **options)
    except Exception:
        response.close()
        response.release_conn()
        raise

    return reader, response


# ---------------------------------------------------------
# Ingest one file
# ---------------------------------------------------------

def ingest_file(filename, batch_id):
    stem = Path(filename).stem
    table_name = table_name_for(filename)

    local_output_dir = PARQUET_DIR / stem
    local_output_dir.mkdir(parents=True, exist_ok=True)

    next_row_number = 1
    total_rows = 0
    part_number = 0

    print(f"\nIngesting: {filename}")

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

            # Write the local Parquet part.
            chunk.to_parquet(parquet_path, index=False)

            # Store the same part in MinIO.
            MINIO_CLIENT.fput_object(
                BRONZE_BUCKET,
                f"raw/{stem}/{parquet_path.name}",
                str(parquet_path),
            )

            # Append this chunk to the file's Bronze table.
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


# ---------------------------------------------------------
# Incremental orchestration
# ---------------------------------------------------------

def main():
    print("Starting incremental Bronze ingestion")
    print(f"Input mode: {INGEST_SOURCE}")
    print(f"PostgreSQL schema: {SCHEMA}")
    print(f"MinIO output bucket: {BRONZE_BUCKET}")

    # Discover and validate before modifying any outputs.
    source_files = discover_source_files()
    validate_sources(source_files)

    ensure_bronze_bucket()
    ensure_registry()

    batch_id = str(uuid.uuid4())
    print(f"Batch ID: {batch_id}")

    ingested = []
    skipped = []
    replaced = []

    for filename in source_files:
        print(f"\nChecking: {filename}")

        # Hashing reads the source bytes but does not alter the source.
        content_hash = calculate_file_hash(filename)
        existing = get_registered_file(filename)

        if (
            existing is not None
            and existing["content_sha256"].strip() == content_hash
        ):
            print("  Unchanged file; skipping ingestion.")
            skipped.append(filename)
            continue

        if existing is None:
            print("  New or unregistered file; ingesting.")
        else:
            print("  File content changed; replacing this file only.")
            replaced.append(filename)

        # Clear only this file's outputs, not other Bronze tables.
        clear_file_outputs(filename)

        result = ingest_file(filename, batch_id)

        # Register only after the complete file ingestion succeeds.
        record_success(
            filename,
            content_hash,
            batch_id,
            result,
        )

        ingested.append(result)

    print("\n" + "=" * 65)
    print("INCREMENTAL BRONZE INGESTION SUMMARY")
    print("=" * 65)

    if ingested:
        for result in ingested:
            print(
                f"{result['table_name']}: "
                f"{result['rows_loaded']:,} rows; "
                f"{result['parquet_parts']} Parquet part(s)"
            )
    else:
        print("No new or modified files required ingestion.")

    print(f"\nFiles ingested/replaced: {len(ingested)}")
    print(f"Files skipped unchanged: {len(skipped)}")
    print(f"Modified files replaced: {len(replaced)}")
    print(f"Rows loaded this run: {sum(r['rows_loaded'] for r in ingested):,}")
    print(f"Batch ID: {batch_id}")
    print("Incremental Bronze ingestion completed.")


if __name__ == "__main__":
    main()
