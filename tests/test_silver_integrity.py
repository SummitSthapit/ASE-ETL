
from pathlib import Path

import pandas as pd

from src.silver.config import (
    BRONZE_ROOT,
    SILVER_ROOT,
    EMPLOYEE_DROP_COLUMNS,
    TIMESHEET_BUSINESS_COLUMNS,
)

EMPLOYEE_KEY = ["client_employee_id"]

TIMESHEET_KEY = [
    "client_employee_id",
    "punch_in_datetime",
    "punch_out_datetime",
]

LINEAGE_KEY = [
    "source_file",
    "source_row_number",
    "ingestion_batch_id",
]

EXPECTED = {
    "bronze_employees": 50,
    "bronze_timesheets": 412571,
    "silver_employees": 50,
    "silver_timesheets": 381732,
    "duplicate_occurrences": 30839,
}


def load_parquet(directory: Path) -> pd.DataFrame:
    files = sorted(directory.glob("*.parquet"))

    assert files, f"No Parquet files in {directory}"

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )


def load_bronze(pattern: str) -> pd.DataFrame:
    files = sorted(BRONZE_ROOT.glob(pattern))

    assert files, f"No Bronze files matching {pattern}"

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )


def check_primary_key(
    df: pd.DataFrame,
    key: list[str],
    name: str,
):
    assert not df[key].isna().any().any(), (
        f"{name}: NULL primary-key component"
    )

    assert not df.duplicated(subset=key).any(), (
        f"{name}: Duplicate primary key"
    )

    print(f"PASS: {name} primary key")


def main():
    print("=" * 65)
    print("ASE-ETL SILVER INTEGRITY TEST")
    print("=" * 65)

    bronze_employees = load_bronze(
        "employee_*/*.parquet"
    )

    bronze_timesheets = load_bronze(
        "timesheet_*/*.parquet"
    )

    employees = load_parquet(
        SILVER_ROOT / "employees"
    )

    timesheets = load_parquet(
        SILVER_ROOT / "timesheets"
    )

    duplicates = load_parquet(
        SILVER_ROOT / "audit" / "duplicate_records"
    )

    quality_issues = load_parquet(
        SILVER_ROOT / "audit" / "quality_issues"
    )

    rejected_employees = load_parquet(
        SILVER_ROOT / "audit" / "rejected_employees"
    )

    rejected_timesheets = load_parquet(
        SILVER_ROOT / "audit" / "rejected_timesheets"
    )

    # -----------------------------------------------
    # 1. Expected snapshot counts
    # -----------------------------------------------

    assert len(bronze_employees) == EXPECTED[
        "bronze_employees"
    ]

    assert len(bronze_timesheets) == EXPECTED[
        "bronze_timesheets"
    ]

    assert len(employees) == EXPECTED[
        "silver_employees"
    ]

    assert len(timesheets) == EXPECTED[
        "silver_timesheets"
    ]

    assert len(duplicates) == EXPECTED[
        "duplicate_occurrences"
    ]

    print("PASS: Expected snapshot row counts")

    # -----------------------------------------------
    # 2. Row-count reconciliation
    # -----------------------------------------------

    assert len(bronze_employees) == (
        len(employees) + len(rejected_employees)
    )

    assert len(bronze_timesheets) == (
        len(timesheets)
        + len(duplicates)
        + len(rejected_timesheets)
    )

    print("PASS: Bronze-to-Silver reconciliation")

    # -----------------------------------------------
    # 3. Primary keys
    # -----------------------------------------------

    check_primary_key(
        employees,
        EMPLOYEE_KEY,
        "Employee",
    )

    check_primary_key(
        timesheets,
        TIMESHEET_KEY,
        "Timesheet",
    )

    # -----------------------------------------------
    # 4. Duplicate integrity
    # -----------------------------------------------

    assert not timesheets.duplicated(
        subset=TIMESHEET_BUSINESS_COLUMNS
    ).any()

    assert duplicates["audit_reason"].eq(
        "DUPLICATE_BUSINESS_RECORD"
    ).all()

    assert duplicates[
        "kept_source_file"
    ].notna().all()

    assert duplicates[
        "kept_source_row_number"
    ].notna().all()

    assert duplicates[
        "kept_ingestion_batch_id"
    ].notna().all()

    # Every discarded occurrence must point
    # to a real retained Silver record.
    removed_refs = duplicates[
        LINEAGE_KEY
    ].drop_duplicates()

    kept_refs = duplicates[
        [
            "kept_source_file",
            "kept_source_row_number",
            "kept_ingestion_batch_id",
        ]
    ].copy()

    kept_refs.columns = LINEAGE_KEY
    kept_refs = kept_refs.drop_duplicates()

    silver_refs = timesheets[LINEAGE_KEY]

    assert len(
        removed_refs.merge(
            bronze_timesheets[LINEAGE_KEY],
            on=LINEAGE_KEY,
            how="left",
            indicator=True,
        ).query("_merge != 'both'")
    ) == 0

    assert len(
        kept_refs.merge(
            silver_refs,
            on=LINEAGE_KEY,
            how="left",
            indicator=True,
        ).query("_merge != 'both'")
    ) == 0

    print("PASS: Duplicate audit lineage")

    # -----------------------------------------------
    # 5. Employee schema
    # -----------------------------------------------

    assert not (
        set(EMPLOYEE_DROP_COLUMNS)
        & set(employees.columns)
    )

    assert len(employees.columns) == 26

    assert len(timesheets.columns) == 18

    print("PASS: Silver schema")

    # -----------------------------------------------
    # 6. Data type checks
    # -----------------------------------------------

    assert pd.api.types.is_numeric_dtype(
        employees["scheduled_weekly_hour"]
    )

    assert pd.api.types.is_numeric_dtype(
        timesheets["hours_worked"]
    )

    for column in [
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
    ]:
        assert pd.api.types.is_datetime64_any_dtype(
            timesheets[column]
        )

    print("PASS: Silver data types")

    # -----------------------------------------------
    # 7. Quality and reject reconciliation
    # -----------------------------------------------

    critical = quality_issues.loc[
        quality_issues["severity"] == "CRITICAL"
    ]

    rejected_count = (
        len(rejected_employees)
        + len(rejected_timesheets)
    )

    if rejected_count:
        assert not critical.empty

    print("PASS: Quality-audit consistency")

    # -----------------------------------------------
    # 8. Check for unexpected masked/missing IDs
    # -----------------------------------------------

    assert employees[
        "client_employee_id"
    ].notna().all()

    assert timesheets[
        "client_employee_id"
    ].notna().all()

    print("PASS: Required identifiers")

    print("\n" + "=" * 65)
    print("ALL SILVER INTEGRITY TESTS PASSED")
    print("=" * 65)


if __name__ == "__main__":
    main()
