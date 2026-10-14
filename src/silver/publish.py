
import os
import uuid
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import (
    create_engine,
    text,
    String,
    Date,
    DateTime,
    Numeric,
    BigInteger,
)

from src.silver.config import SILVER_ROOT


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]

SILVER_BUCKET = os.getenv(
    "SILVER_BUCKET", "ase-silver"
)

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=(
        os.getenv("MINIO_SECURE", "false").lower()
        == "true"
    ),
)

ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

DATASETS = [
    "employees",
    "timesheets",
    "audit/duplicate_records",
    "audit/quality_issues",
    "audit/rejected_employees",
    "audit/rejected_timesheets",
]

EXPECTED_COUNTS = {
    "employees": 50,
    "timesheets": 381732,
    "audit/duplicate_records": 30839,
    "audit/quality_issues": 0,
    "audit/rejected_employees": 0,
    "audit/rejected_timesheets": 0,
}


def dataset_files(name):
    files = sorted(
        (SILVER_ROOT / name).glob("*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            f"Missing Silver dataset: {name}"
        )

    return files


def verify_local():
    counts = {}

    for name in DATASETS:
        files = dataset_files(name)

        counts[name] = sum(
            pq.read_metadata(file).num_rows
            for file in files
        )

        expected = EXPECTED_COUNTS[name]

        if counts[name] != expected:
            raise ValueError(
                f"{name}: expected {expected}, "
                f"found {counts[name]}"
            )

    return counts


def upload_and_verify(run_id, counts):
    if not MINIO_CLIENT.bucket_exists(SILVER_BUCKET):
        MINIO_CLIENT.make_bucket(SILVER_BUCKET)

    prefix = f"runs/{run_id}"

    for name in DATASETS:
        uploaded_rows = 0

        for file in dataset_files(name):
            object_name = (
                f"{prefix}/{name}/{file.name}"
            )

            MINIO_CLIENT.fput_object(
                SILVER_BUCKET,
                object_name,
                str(file),
            )

            # Read the uploaded Parquet metadata
            # through the MinIO API.
            response = MINIO_CLIENT.get_object(
                SILVER_BUCKET,
                object_name,
            )

            try:
                data = response.read()
            finally:
                response.close()
                response.release_conn()

            import io

            metadata = pq.read_metadata(
                io.BytesIO(data)
            )

            uploaded_rows += metadata.num_rows

        if uploaded_rows != counts[name]:
            raise ValueError(
                f"MinIO count mismatch: {name}"
            )

        print(
            f"MinIO verified: {name} "
            f"({uploaded_rows:,} rows)"
        )

    return prefix


def sql_types(df, dataset):
    # IDs and other source text remain strings.
    mapping = {
        col: String()
        for col in df.columns
        if pd.api.types.is_string_dtype(
            df[col]
        )
    }

    mapping["source_row_number"] = BigInteger()
    mapping["ingested_at_utc"] = DateTime(timezone=True)

    if dataset == "employees":
        for col in [
            "job_start_date",
            "dob",
            "hire_date",
            "recent_hire_date",
            "anniversary_date",
            "term_date",
        ]:
            mapping[col] = Date()

        mapping["scheduled_weekly_hour"] = Numeric(
            12, 2
        )

    else:
        mapping["punch_apply_date"] = Date()

        for col in [
            "punch_in_datetime",
            "punch_out_datetime",
            "scheduled_start_datetime",
            "scheduled_end_datetime",
        ]:
            mapping[col] = DateTime(timezone=False)

        mapping["hours_worked"] = Numeric(12, 2)

    return mapping


def load_dataframe(name):
    return pd.concat(
        [
            pd.read_parquet(file)
            for file in dataset_files(name)
        ],
        ignore_index=True,
    )


def publish_postgres(counts, run_id):
    # Short, strictly generated staging identifiers.
    suffix = run_id[:12]

    with ENGINE.begin() as connection:
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS silver")
        )

        for dataset in ["employees", "timesheets"]:
            frame = load_dataframe(dataset)

            # Explicitly parse timezone-aware lineage.
            frame["ingested_at_utc"] = pd.to_datetime(
                frame["ingested_at_utc"],
                utc=True,
            )

            staging = f"_stage_{dataset}_{suffix}"

            # Clean up this run's staging table
            # if a previous attempt left it behind.
            connection.execute(
                text(
                    f'DROP TABLE IF EXISTS '
                    f'silver."{staging}"'
                )
            )

            frame.to_sql(
                name=staging,
                con=connection,
                schema="silver",
                if_exists="fail",
                index=False,
                dtype=sql_types(frame, dataset),
                chunksize=5000,
            )

            if dataset == "employees":
                constraint = (
                    "PRIMARY KEY (client_employee_id)"
                )
            else:
                constraint = """
                    PRIMARY KEY (
                        client_employee_id,
                        punch_in_datetime,
                        punch_out_datetime
                    )
                """

            connection.execute(
                text(
                    f'ALTER TABLE silver."{staging}" '
                    f"ADD CONSTRAINT "
                    f'"{staging}_pk" {constraint}'
                )
            )

            staged_count = connection.execute(
                text(
                    f'SELECT COUNT(*) FROM '
                    f'silver."{staging}"'
                )
            ).scalar_one()

            if staged_count != counts[dataset]:
                raise ValueError(
                    f"PostgreSQL staging count mismatch "
                    f"for {dataset}"
                )

        # Publish both live tables inside the
        # same PostgreSQL transaction.
        for dataset in ["employees", "timesheets"]:
            staging = f"_stage_{dataset}_{suffix}"

            connection.execute(
                text(
                    f"DROP TABLE IF EXISTS "
                    f"silver.{dataset}"
                )
            )

            connection.execute(
                text(
                    f'ALTER TABLE silver."{staging}" '
                    f"RENAME TO {dataset}"
                )
            )

        for dataset in ["employees", "timesheets"]:
            actual = connection.execute(
                text(
                    f"SELECT COUNT(*) "
                    f"FROM silver.{dataset}"
                )
            ).scalar_one()

            if actual != counts[dataset]:
                raise ValueError(
                    f"PostgreSQL verification failed: "
                    f"{dataset}"
                )

            print(
                f"PostgreSQL verified: {dataset} "
                f"({actual:,} rows)"
            )


def main():
    print("=" * 65)
    print("ASE-ETL SILVER PUBLISHING")
    print("=" * 65)

    counts = verify_local()

    run_id = uuid.uuid4().hex

    print("\nPublishing run:", run_id)

    prefix = upload_and_verify(
        run_id,
        counts,
    )

    publish_postgres(
        counts,
        run_id,
    )

    print("\nMinIO bucket:", SILVER_BUCKET)
    print("MinIO run prefix:", prefix)
    print("PostgreSQL schema: silver")

    print("\nSILVER PUBLISHING COMPLETED")


if __name__ == "__main__":
    main()
