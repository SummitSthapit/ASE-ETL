"""Verify Bronze PostgreSQL, local Parquet, and MinIO consistency."""

import hashlib
import os
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

PARQUET_ROOT = ROOT / "data" / "bronze" / "parquet"

ENGINE = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)

BUCKET = os.getenv("BRONZE_BUCKET", "ase-bronze")

CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def minio_sha256(object_name):
    response = CLIENT.get_object(BUCKET, object_name)
    digest = hashlib.sha256()

    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()

    return digest.hexdigest()


def main():
    print("=" * 65)
    print("ASE-ETL BRONZE PUBLICATION VERIFICATION")
    print("=" * 65)

    if not CLIENT.bucket_exists(BUCKET):
        raise RuntimeError(f"Missing MinIO bucket: {BUCKET}")

    with ENGINE.connect() as connection:
        registry = connection.execute(
            text("""
                SELECT source_file, table_name,
                       rows_loaded, parquet_parts
                FROM bronze._ingestion_registry
                ORDER BY source_file
            """)
        ).mappings().all()

        if not registry:
            raise RuntimeError("Bronze ingestion registry is empty")

        total_rows = 0

        for record in registry:
            filename = record["source_file"]
            stem = Path(filename).stem
            table_name = record["table_name"]

            # Verify registered table names.
            expected_table = (
                "bronze."
                + stem.lower().replace("-", "_")
                + "_raw"
            )

            if table_name != expected_table:
                raise ValueError(
                    f"Unexpected Bronze table: {table_name}"
                )

            folder = PARQUET_ROOT / stem
            parts = sorted(folder.glob("*.parquet"))

            if len(parts) != record["parquet_parts"]:
                raise ValueError(
                    f"{filename}: Parquet part count mismatch"
                )

            parquet_rows = 0

            for part in parts:
                metadata = pq.read_metadata(part)
                parquet_rows += metadata.num_rows

                object_name = f"raw/{stem}/{part.name}"

                remote = CLIENT.stat_object(
                    BUCKET, object_name
                )

                if remote.size != part.stat().st_size:
                    raise ValueError(
                        f"MinIO object size mismatch: {object_name}"
                    )

                if file_sha256(part) != minio_sha256(object_name):
                    raise ValueError(
                        f"MinIO checksum mismatch: {object_name}"
                    )

            if parquet_rows != record["rows_loaded"]:
                raise ValueError(
                    f"{filename}: registry/Parquet count mismatch"
                )

            # The table identifier was checked against
            # the ingestion script's naming convention.
            # The registry stores schema.table_name.
            # Extract the validated, unqualified table name.

            unqualified_table = table_name.split(".", 1)[1]

            db_rows = connection.execute(
                text(
                    f'SELECT COUNT(*) '
                    f'FROM bronze."{unqualified_table}"'
                )
            ).scalar_one()

            if db_rows != parquet_rows:
                raise ValueError(
                    f"{filename}: PostgreSQL/Parquet count mismatch"
                )

            total_rows += parquet_rows

            print(
                f"PASS: {filename} — "
                f"{parquet_rows:,} rows, "
                f"{len(parts)} verified parts"
            )

    print("-" * 65)
    print(f"Verified source files: {len(registry)}")
    print(f"Verified rows: {total_rows:,}")
    print("PASS: Bronze publication verification completed")


if __name__ == "__main__":
    main()