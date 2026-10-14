
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from src.silver.config import SILVER_ROOT


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

ENGINE = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)

KEYS = {
    "employees": ["client_employee_id"],
    "timesheets": [
        "client_employee_id",
        "punch_in_datetime",
        "punch_out_datetime",
    ],
}

DATE_COLUMNS = {
    "employees": {
        "job_start_date",
        "dob",
        "hire_date",
        "recent_hire_date",
        "anniversary_date",
        "term_date",
    },
    "timesheets": {"punch_apply_date"},
}

DATETIME_COLUMNS = {
    "employees": set(),
    "timesheets": {
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
    },
}

NUMERIC_COLUMNS = {
    "employees": {"scheduled_weekly_hour"},
    "timesheets": {"hours_worked"},
}

CHUNK_SIZE = 10000


def canonical(value, column, dataset):
    """Convert a value into a comparable representation."""

    if pd.isna(value):
        return None

    if column in NUMERIC_COLUMNS[dataset]:
        # Do not round: detect actual numeric differences.
        return str(Decimal(str(value)).normalize())

    if column in DATE_COLUMNS[dataset]:
        return pd.Timestamp(value).strftime("%Y-%m-%d")

    if column in DATETIME_COLUMNS[dataset]:
        return pd.Timestamp(value).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )

    if column == "ingested_at_utc":
        return pd.Timestamp(value).tz_convert(
            "UTC"
        ).isoformat()

    if column == "source_row_number":
        return str(int(value))

    return str(value)


def normalize_frame(frame, dataset):
    result = pd.DataFrame(index=frame.index)

    for column in frame.columns:
        result[column] = frame[column].map(
            lambda value: canonical(
                value, column, dataset
            )
        )

    return result


def verify_dataset(dataset):
    print(f"\nVERIFYING: {dataset}")

    files = sorted(
        (SILVER_ROOT / dataset).glob("*.parquet")
    )

    assert files, f"No Silver files for {dataset}"

    parquet = pd.concat(
        [pd.read_parquet(file) for file in files],
        ignore_index=True,
    )

    key = KEYS[dataset]
    columns = parquet.columns.tolist()

    assert not parquet.duplicated(
        subset=key
    ).any()

    assert not parquet[key].isna().any().any()

    # Normalize values and sort by the validated key.
    expected = normalize_frame(
        parquet, dataset
    ).sort_values(
        key
    ).reset_index(drop=True)

    quoted_columns = ", ".join(
        f'"{column}"' for column in columns
    )

    order_columns = ", ".join(
        f'"{column}"' for column in key
    )

    query = text(
        f'SELECT {quoted_columns} '
        f'FROM silver."{dataset}" '
        f'ORDER BY {order_columns}'
    )

    compared = 0

    with ENGINE.connect() as connection:
        chunks = pd.read_sql_query(
            query,
            connection,
            chunksize=CHUNK_SIZE,
        )

        for chunk in chunks:
            actual = normalize_frame(
                chunk, dataset
            ).reset_index(drop=True)

            end = compared + len(actual)

            expected_chunk = expected.iloc[
                compared:end
            ].reset_index(drop=True)

            pd.testing.assert_frame_equal(
                expected_chunk,
                actual,
                check_dtype=False,
                check_exact=True,
            )

            compared = end

            if compared % 50000 < CHUNK_SIZE:
                print(
                    f"Compared {compared:,} records"
                )

    assert compared == len(expected), (
        f"Row-count mismatch for {dataset}: "
        f"PostgreSQL={compared}, Parquet={len(expected)}"
    )

    print(
        f"PASS: All {compared:,} {dataset} records "
        f"match across every column."
    )


def main():
    print("=" * 65)
    print("SILVER CROSS-STORAGE CONTENT VERIFICATION")
    print("=" * 65)

    verify_dataset("employees")
    verify_dataset("timesheets")

    print("\n" + "=" * 65)
    print("ALL CROSS-STORAGE CHECKS PASSED")
    print("=" * 65)


if __name__ == "__main__":
    main()
