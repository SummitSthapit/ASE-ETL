
import hashlib
import os
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text

from src.silver.config import SILVER_ROOT


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

RUN_ID = "7936e99d6fbf41c2a8510fc1f00241b3"
BUCKET = os.getenv("SILVER_BUCKET", "ase-silver")

DATASETS = {
    "employees": 50,
    "timesheets": 381732,
    "audit/duplicate_records": 30839,
    "audit/quality_issues": 0,
    "audit/rejected_employees": 0,
    "audit/rejected_timesheets": 0,
}

client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.getenv(
        "MINIO_SECURE", "false"
    ).lower() == "true",
)

engine = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)


def local_hash(path):
    digest = hashlib.sha256()

    with open(path, "rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def minio_hash(bucket, object_name):
    digest = hashlib.sha256()

    response = client.get_object(
        bucket, object_name
    )

    try:
        for chunk in response.stream(
            1024 * 1024
        ):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()

    return digest.hexdigest()


def verify_minio():
    print("\nMINIO VERIFICATION")

    assert client.bucket_exists(BUCKET)

    prefix = f"runs/{RUN_ID}"

    for dataset, expected in DATASETS.items():
        folder = SILVER_ROOT / dataset

        files = sorted(folder.glob("*.parquet"))
        assert files, f"Missing local files: {dataset}"

        total = 0

        for path in files:
            key = (
                f"{prefix}/{dataset}/{path.name}"
            )

            client.stat_object(BUCKET, key)

            assert local_hash(path) == minio_hash(
                BUCKET, key
            ), f"SHA-256 mismatch: {key}"

            total += pq.read_metadata(
                path
            ).num_rows

        assert total == expected, (
            f"{dataset}: expected {expected}, got {total}"
        )

        print(
            f"PASS: {dataset} "
            f"({total:,} rows, SHA-256 matched)"
        )


def verify_postgres():
    print("\nPOSTGRESQL VERIFICATION")

    with engine.connect() as connection:
        for table, expected in [
            ("employees", 50),
            ("timesheets", 381732),
        ]:
            count = connection.execute(
                text(
                    f"SELECT COUNT(*) "
                    f"FROM silver.{table}"
                )
            ).scalar_one()

            assert count == expected

            print(
                f"PASS: silver.{table} "
                f"({count:,} rows)"
            )

        employee_duplicates = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM (
                    SELECT client_employee_id
                    FROM silver.employees
                    GROUP BY client_employee_id
                    HAVING COUNT(*) > 1
                ) duplicate_keys
            """)
        ).scalar_one()

        assert employee_duplicates == 0

        timesheet_duplicates = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        client_employee_id,
                        punch_in_datetime,
                        punch_out_datetime
                    FROM silver.timesheets
                    GROUP BY
                        client_employee_id,
                        punch_in_datetime,
                        punch_out_datetime
                    HAVING COUNT(*) > 1
                ) duplicate_keys
            """)
        ).scalar_one()

        assert timesheet_duplicates == 0

        print("PASS: No duplicate primary keys")

        for table in ["employees", "timesheets"]:
            constraints = connection.execute(
                text("""
                    SELECT COUNT(*)
                    FROM pg_constraint c
                    JOIN pg_class t
                        ON t.oid = c.conrelid
                    JOIN pg_namespace n
                        ON n.oid = t.relnamespace
                    WHERE n.nspname = 'silver'
                      AND t.relname = :table
                      AND c.contype = 'p'
                """),
                {"table": table},
            ).scalar_one()

            assert constraints == 1, (
                f"Missing PostgreSQL primary key: {table}"
            )

        print("PASS: PostgreSQL primary-key constraints")


def main():
    print("=" * 65)
    print("ASE-ETL SILVER STORAGE VERIFICATION")
    print("=" * 65)

    verify_minio()
    verify_postgres()

    print("\n" + "=" * 65)
    print("ALL SILVER STORAGE CHECKS PASSED")
    print("=" * 65)


if __name__ == "__main__":
    main()
