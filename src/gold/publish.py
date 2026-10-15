"""Gold layer Parquet export and MinIO publication utilities."""

import hashlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json

from datetime import datetime, timezone

from src.gold.config import GOLD_MODEL_VERSION

from sqlalchemy import text

from src.silver.publish import ENGINE, MINIO_CLIENT
from src.gold.config import GOLD_BUCKET, GOLD_DATASETS

from sqlalchemy import inspect


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_EXPORT_ROOT = PROJECT_ROOT / "data" / "gold" / "exports"

EXPORT_CHUNK_SIZE = 25000

DECIMAL_EXPORT_TYPES = {
    "dim_employee": {
        "scheduled_weekly_hour": pa.decimal128(18, 4),
        "years_of_experience": pa.decimal128(18, 4),
    },
    "dim_date": {},
    "fact_timesheet": {
        "hours_worked": pa.decimal128(18, 4),
    },
}

def build_arrow_schema(connection, dataset):
    """
    Build a stable Arrow schema from PostgreSQL metadata.

    Override NUMERIC columns with approved decimal types.
    """

    inspector = inspect(connection)

    columns = inspector.get_columns(
        dataset,
        schema="gold",
    )

    if not columns:
        raise ValueError(
            f"No PostgreSQL columns found for gold.{dataset}"
        )

    fields = []

    for column in columns:
        name = column["name"]
        sql_type = column["type"]
        type_name = str(sql_type).upper()

        if name in DECIMAL_EXPORT_TYPES[dataset]:
            arrow_type = DECIMAL_EXPORT_TYPES[dataset][name]

        elif type_name == "BIGINT":
            arrow_type = pa.int64()

        elif type_name in {"INTEGER", "INT"}:
            arrow_type = pa.int32()

        elif type_name == "SMALLINT":
            arrow_type = pa.int16()

        elif type_name == "DATE":
            arrow_type = pa.date32()

        elif type_name.startswith("TIMESTAMP"):
            arrow_type = (
                pa.timestamp("us", tz="UTC")
                if getattr(sql_type, "timezone", False)
                else pa.timestamp("us")
            )

        elif type_name == "BOOLEAN":
            arrow_type = pa.bool_()

        elif (
            type_name.startswith("VARCHAR")
            or type_name == "TEXT"
        ):
            arrow_type = pa.string()

        else:
            raise TypeError(
                f"Unsupported Gold PostgreSQL type: "
                f"{dataset}.{name} = {sql_type}"
            )

        fields.append(
            pa.field(
                name,
                arrow_type,
                nullable=column["nullable"],
            )
        )

    return pa.schema(fields)

def export_gold_parquet(connection, output_root):
    """
    Export Gold tables to Parquet in bounded chunks.

    The caller must provide a transactionally consistent
    PostgreSQL connection.

    Existing output directories must not be overwritten.
    """

    output_root = Path(output_root)

    if output_root.exists():
        raise FileExistsError(
            f"Gold export directory already exists: {output_root}"
        )

    output_root.mkdir(parents=True)

    counts = {}

    order_columns = {
        "dim_employee": "employee_key",
        "dim_date": "date_key",
        "fact_timesheet": "timesheet_key",
    }

    for dataset in GOLD_DATASETS:

        dataset_dir = output_root / dataset
        dataset_dir.mkdir(parents=True)

        # Build a fixed Arrow schema for this dataset.
        arrow_schema = build_arrow_schema(
            connection,
            dataset,
        )

        # Read records in deterministic order.
        query = text(
            f"SELECT * FROM gold.{dataset} "
            f"ORDER BY {order_columns[dataset]}"
        )

        total_rows = 0

        # Read PostgreSQL data in manageable chunks.
        for index, chunk in enumerate(
            pd.read_sql_query(
                query,
                connection,
                chunksize=EXPORT_CHUNK_SIZE,
                coerce_float=False,
            )
        ):

            path = (
                dataset_dir
                / f"part-{index:05d}.parquet"
            )

            # Apply the complete, explicit Arrow schema.
            # This also preserves all-NULL decimal columns.
            table = pa.Table.from_pandas(
                chunk,
                schema=arrow_schema,
                preserve_index=False,
                safe=True,
            )

            pq.write_table(
                table,
                path,
                compression="snappy",
            )

            total_rows += len(chunk)

        counts[dataset] = total_rows

        print(
            f"Exported {dataset}: {total_rows:,} rows"
        )

    return counts


def calculate_export_fingerprint(output_root):
    """
    Calculate a deterministic SHA-256 fingerprint
    from the exported Parquet files.
    """

    output_root = Path(output_root)
    digest = hashlib.sha256()

    for path in sorted(output_root.rglob("*.parquet")):
        relative_path = path.relative_to(
            output_root
        ).as_posix()

        file_hash = hashlib.sha256()

        with path.open("rb") as stream:
            for chunk in iter(
                lambda: stream.read(1024 * 1024),
                b"",
            ):
                file_hash.update(chunk)

        digest.update(
            f"{relative_path}:{file_hash.hexdigest()}\n".encode()
        )

    return digest.hexdigest()

def verify_gold_export(connection, output_root):
    """
    Verify local Gold Parquet files against PostgreSQL.

    Checks row counts, schemas, and dataset parts.
    """

    output_root = Path(output_root)

    if not output_root.is_dir():
        raise FileNotFoundError(
            f"Gold export directory missing: {output_root}"
        )

    counts = {}

    for dataset in GOLD_DATASETS:
        dataset_dir = output_root / dataset

        if not dataset_dir.is_dir():
            raise FileNotFoundError(
                f"Missing Gold dataset directory: {dataset}"
            )

        files = sorted(dataset_dir.glob("*.parquet"))

        if not files:
            raise FileNotFoundError(
                f"No Parquet files found for {dataset}"
            )

        expected_schema = build_arrow_schema(
            connection,
            dataset,
        )

        total_rows = 0

        for path in files:
            metadata = pq.read_metadata(path)
            parquet_schema = pq.read_schema(path)

            if not parquet_schema.equals(
                expected_schema,
                check_metadata=False,
            ):
                raise ValueError(
                    f"Schema mismatch: {path}"
                )

            # Read the part to detect data-page corruption.
            table = pq.read_table(path)

            if table.num_rows != metadata.num_rows:
                raise ValueError(
                    f"Parquet row-count mismatch: {path}"
                )

            total_rows += metadata.num_rows

        expected_count = connection.execute(
            text(f"SELECT COUNT(*) FROM gold.{dataset}")
        ).scalar_one()

        if total_rows != expected_count:
            raise ValueError(
                f"{dataset}: expected {expected_count:,} "
                f"rows, found {total_rows:,}"
            )

        counts[dataset] = total_rows

        print(
            f"PASS: {dataset} — "
            f"{total_rows:,} rows, "
            f"{len(files)} Parquet parts"
        )

    return counts

def sha256_file(path):
    """Calculate the SHA-256 hash of a local file."""
    digest = hashlib.sha256()

    with Path(path).open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def verify_minio_object(bucket, object_name, expected_hash):
    """Verify an uploaded MinIO object's SHA-256."""

    response = MINIO_CLIENT.get_object(
        bucket,
        object_name,
    )

    digest = hashlib.sha256()

    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
    finally:
        response.close()
        response.release_conn()

    actual_hash = digest.hexdigest()

    if actual_hash != expected_hash:
        raise ValueError(
            f"MinIO checksum mismatch: {object_name}"
        )


def upload_gold_snapshot(
    output_root,
    fingerprint,
    silver_fingerprint,
    counts,
):
    """
    Upload and verify Gold Parquet files in MinIO.

    Does not update PostgreSQL publication status.
    """

    output_root = Path(output_root)

    if not MINIO_CLIENT.bucket_exists(GOLD_BUCKET):
        MINIO_CLIENT.make_bucket(GOLD_BUCKET)

    prefix = f"runs/{fingerprint}"

    manifest_files = {}

    for dataset in GOLD_DATASETS:

        dataset_files = sorted(
            (output_root / dataset).glob("*.parquet")
        )

        if not dataset_files:
            raise FileNotFoundError(
                f"Missing Gold Parquet dataset: {dataset}"
            )

        manifest_files[dataset] = []

        for path in dataset_files:

            object_name = (
                f"{prefix}/{dataset}/{path.name}"
            )

            expected_hash = sha256_file(path)

            exists = False

            try:
                MINIO_CLIENT.stat_object(
                    GOLD_BUCKET,
                    object_name,
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
                    GOLD_BUCKET,
                    object_name,
                    str(path),
                )

            verify_minio_object(
                GOLD_BUCKET,
                object_name,
                expected_hash,
            )

            manifest_files[dataset].append({
                "object": object_name,
                "sha256": expected_hash,
                "rows": pq.read_metadata(path).num_rows,
            })

        print(f"PASS: MinIO verified {dataset}")

    manifest = {
        "gold_snapshot_fingerprint": fingerprint,
        "silver_snapshot_fingerprint": silver_fingerprint,
        "gold_model_version": GOLD_MODEL_VERSION,
        "prefix": prefix,
        "counts": counts,
        "files": manifest_files,
        "status": "prepared",
    }

    payload = json.dumps(
        manifest,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")

    manifest_object = f"{prefix}/manifest.json"

    expected_hash = hashlib.sha256(payload).hexdigest()

    manifest_exists = False

    try:
        MINIO_CLIENT.stat_object(
            GOLD_BUCKET,
            manifest_object,
        )
        manifest_exists = True

    except Exception as exc:
        if getattr(exc, "code", None) not in {
            "NoSuchKey",
            "NoSuchObject",
        }:
            raise

    if not manifest_exists:
        MINIO_CLIENT.put_object(
            GOLD_BUCKET,
            manifest_object,
            io.BytesIO(payload),
            len(payload),
            content_type="application/json",
        )

    verify_minio_object(
        GOLD_BUCKET,
        manifest_object,
        expected_hash,
    )

    print("PASS: Gold MinIO snapshot uploaded")

    return prefix