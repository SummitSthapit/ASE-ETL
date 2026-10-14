
from pathlib import Path
import shutil
import tempfile

import pandas as pd

from src.silver.config import (
    BRONZE_ROOT,
    SILVER_ROOT,
    TIMESHEET_BUSINESS_COLUMNS,
)
from src.silver.transformations import (
    transform_employees,
    transform_timesheets,
)
from src.silver.validation import (
    validate_employees,
    validate_timesheets,
)
from src.silver.deduplication import (
    deduplicate_timesheets,
)


LINEAGE_KEY = [
    "source_file",
    "source_row_number",
    "ingestion_batch_id",
]

TIMESHEET_PRIMARY_KEY = [
    "client_employee_id",
    "punch_in_datetime",
    "punch_out_datetime",
]


def load_bronze(pattern: str) -> pd.DataFrame:
    files = sorted(BRONZE_ROOT.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No Bronze files found for: {pattern}"
        )

    frames = [
        pd.read_parquet(path)
        for path in files
    ]

    result = pd.concat(
        frames,
        ignore_index=True,
    )

    if result.duplicated(subset=LINEAGE_KEY).any():
        raise ValueError(
            "Duplicate source lineage detected in Bronze."
        )

    return result


def split_critical_records(
    df: pd.DataFrame,
    issues: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separate records with critical validation findings.

    Warning-only records remain eligible for Silver.
    """

    critical = issues.loc[
        issues["severity"] == "CRITICAL",
        LINEAGE_KEY,
    ].drop_duplicates()

    if critical.empty:
        return (
            df.reset_index(drop=True),
            df.iloc[0:0].copy(),
        )

    marked = df.merge(
        critical.assign(_critical=True),
        on=LINEAGE_KEY,
        how="left",
        validate="one_to_one",
    )

    reject_mask = (
        marked["_critical"]
        .fillna(False)
        .astype(bool)
    )

    accepted = marked.loc[
        ~reject_mask
    ].drop(columns="_critical")

    rejected = marked.loc[
        reject_mask
    ].drop(columns="_critical")

    return (
        accepted.reset_index(drop=True),
        rejected.reset_index(drop=True),
    )


def validate_primary_key(
    df: pd.DataFrame,
    columns: list[str],
    dataset: str,
) -> None:
    missing = df[columns].isna().any(axis=1)

    if missing.any():
        raise ValueError(
            f"{dataset}: NULL primary-key component."
        )

    duplicates = df.duplicated(
        subset=columns
    )

    if duplicates.any():
        raise ValueError(
            f"{dataset}: Primary-key collision detected."
        )


def write_dataset(
    root: Path,
    relative_path: str,
    df: pd.DataFrame,
) -> None:
    output_dir = root / relative_path
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        output_dir / "part-00000.parquet",
        index=False,
        engine="pyarrow",
    )


def main():
    print("=" * 65)
    print("ASE-ETL SILVER PIPELINE")
    print("=" * 65)

    # ----------------------------------------------
    # 1. Load Bronze
    # ----------------------------------------------

    employees_raw = load_bronze(
        "employee_*/*.parquet"
    )

    timesheets_raw = load_bronze(
        "timesheet_*/*.parquet"
    )

    print("\nBRONZE INPUT")
    print("Employees:", len(employees_raw))
    print("Timesheets:", len(timesheets_raw))

    # ----------------------------------------------
    # 2. Transform
    # ----------------------------------------------

    employees = transform_employees(
        employees_raw
    )

    timesheets = transform_timesheets(
        timesheets_raw
    )

    # ----------------------------------------------
    # 3. Validate
    # ----------------------------------------------

    employee_issues = validate_employees(
        employees_raw,
        employees,
    )

    timesheet_issues = validate_timesheets(
        timesheets_raw,
        timesheets,
    )

    employee_issues.insert(
        0, "dataset", "employees"
    )

    timesheet_issues.insert(
        0, "dataset", "timesheets"
    )

    quality_issues = pd.concat(
        [employee_issues, timesheet_issues],
        ignore_index=True,
    )

    # ----------------------------------------------
    # 4. Quarantine critical records
    # ----------------------------------------------

    employees_clean, employees_rejected = (
        split_critical_records(
            employees,
            employee_issues,
        )
    )

    timesheets_valid, timesheets_rejected = (
        split_critical_records(
            timesheets,
            timesheet_issues,
        )
    )

    # ----------------------------------------------
    # 5. Deduplicate valid timesheets
    # ----------------------------------------------

    timesheets_clean, duplicate_audit = (
        deduplicate_timesheets(
            timesheets_valid
        )
    )

    # ----------------------------------------------
    # 6. Verify primary keys
    # ----------------------------------------------

    validate_primary_key(
        employees_clean,
        ["client_employee_id"],
        "employees",
    )

    validate_primary_key(
        timesheets_clean,
        TIMESHEET_PRIMARY_KEY,
        "timesheets",
    )

    if timesheets_clean.duplicated(
        subset=TIMESHEET_BUSINESS_COLUMNS
    ).any():
        raise ValueError(
            "Duplicate business records remain."
        )

    # ----------------------------------------------
    # 7. Reconcile counts
    # ----------------------------------------------

    assert len(employees_raw) == (
        len(employees_clean)
        + len(employees_rejected)
    )

    assert len(timesheets_raw) == (
        len(timesheets_clean)
        + len(timesheets_rejected)
        + len(duplicate_audit)
    )

    # ----------------------------------------------
    # 8. Write staged outputs
    # ----------------------------------------------

    SILVER_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    staging = Path(
        tempfile.mkdtemp(
            prefix=".silver_staging_",
            dir=SILVER_ROOT,
        )
    )

    try:
        outputs = {
            "employees": employees_clean,
            "timesheets": timesheets_clean,
            "audit/duplicate_records": duplicate_audit,
            "audit/quality_issues": quality_issues,
            "audit/rejected_employees": employees_rejected,
            "audit/rejected_timesheets": timesheets_rejected,
        }

        for name, frame in outputs.items():
            write_dataset(
                staging,
                name,
                frame,
            )

        # Replace outputs rather than append to them.
        # This makes reruns non-accumulating.
        for name in outputs:
            destination = SILVER_ROOT / name
            staged = staging / name

            if destination.exists():
                shutil.rmtree(destination)

            destination.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            shutil.move(
                str(staged),
                str(destination),
            )

    finally:
        shutil.rmtree(
            staging,
            ignore_errors=True,
        )

    # ----------------------------------------------
    # 9. Execution report
    # ----------------------------------------------

    print("\nSILVER OUTPUT")
    print("Employees:", len(employees_clean))
    print("Timesheets:", len(timesheets_clean))

    print("\nAUDIT")
    print("Duplicate occurrences:", len(duplicate_audit))
    print("Employee rejects:", len(employees_rejected))
    print("Timesheet rejects:", len(timesheets_rejected))
    print("Quality issues:", len(quality_issues))

    print("\nRECONCILIATION PASSED")
    print("SILVER PIPELINE COMPLETED")


if __name__ == "__main__":
    main()
