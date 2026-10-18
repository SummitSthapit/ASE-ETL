
"""Remove the controlled Phase 8 incremental-ingestion test."""

import hashlib
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import text

from src.silver.publish import ENGINE

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

FILENAME = "timesheet_incremental_test.csv"
STEM = Path(FILENAME).stem
SOURCE = ROOT / "data" / "raw" / FILENAME
PARQUET = ROOT / "data" / "bronze" / "parquet" / STEM
TABLE = "bronze.timesheet_incremental_test_raw"

client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)
bucket = os.getenv("BRONZE_BUCKET", "ase-bronze")
prefix = f"raw/{STEM}/"

# Validate the exact test artifacts before any deletion.
if not SOURCE.is_file():
    raise RuntimeError("Test CSV missing; stopping cleanup")

with SOURCE.open("rb") as stream:
    source_hash = hashlib.file_digest(stream, "sha256").hexdigest()

with ENGINE.connect() as connection:
    registry = connection.execute(
        text("""
            SELECT content_sha256, table_name, rows_loaded
            FROM bronze._ingestion_registry
            WHERE source_file = :filename
        """),
        {"filename": FILENAME},
    ).mappings().one_or_none()

    if registry is None:
        raise RuntimeError("Test ingestion registry entry missing")

    count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM bronze.timesheet_incremental_test_raw
        """)
    ).scalar_one()

if (
    registry["table_name"] != TABLE
    or registry["rows_loaded"] != 10
    or count != 10
    or registry["content_sha256"].strip() != source_hash
):
    raise RuntimeError("Test artifacts differ from expectations; stopping")

objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))

if not objects or not all(
    obj.object_name.startswith(prefix)
    and obj.object_name.endswith(".parquet")
    for obj in objects
):
    raise RuntimeError("Unexpected Bronze MinIO objects; stopping")

if not PARQUET.is_dir():
    raise RuntimeError("Local Bronze test Parquet missing; stopping")

print("PASS: Verified exact 10-row test ingestion")
print("Cleaning up:", FILENAME)

# Remove DB artifacts transactionally.
with ENGINE.begin() as connection:
    connection.execute(
        text("DROP TABLE bronze.timesheet_incremental_test_raw")
    )
    connection.execute(
        text("""
            DELETE FROM bronze._ingestion_registry
            WHERE source_file = :filename
        """),
        {"filename": FILENAME},
    )

# Remove exact MinIO test objects and local files.
for obj in objects:
    client.remove_object(bucket, obj.object_name)

shutil.rmtree(PARQUET)
SOURCE.unlink()

print("PASS: Incremental test Bronze artifacts removed")
