
"""Approved source schemas and schema-evolution checks for Silver."""

from dataclasses import dataclass

import pandas as pd

from src.silver.config import (
    METADATA_COLUMNS,
    TIMESHEET_BUSINESS_COLUMNS,
)


SCHEMA_VERSION = "2.0"


# All 34 employee business attributes from the source.
EMPLOYEE_BUSINESS_COLUMNS = [
    "client_employee_id",
    "first_name",
    "middle_name",
    "last_name",
    "preferred_name",
    "job_code",
    "job_title",
    "job_start_date",
    "organization_id",
    "organization_name",
    "department_id",
    "department_name",
    "dob",
    "hire_date",
    "recent_hire_date",
    "anniversary_date",
    "term_date",
    "years_of_experience",
    "work_email",
    "address",
    "city",
    "state",
    "zip",
    "country",
    "manager_employee_id",
    "manager_employee_name",
    "fte_status",
    "is_per_deim",
    "cell_phone",
    "work_phone",
    "scheduled_weekly_hour",
    "active_status",
    "termination_reason",
    "clinical_level",
]


SCHEMA_COLUMNS = {
    "employees": EMPLOYEE_BUSINESS_COLUMNS + METADATA_COLUMNS,
    "timesheets": TIMESHEET_BUSINESS_COLUMNS + METADATA_COLUMNS,
}


# These fields must exist in every source dataset.
# Column presence is different from row-level validity.
REQUIRED_COLUMNS = {
    "employees": {
        "client_employee_id",
        *METADATA_COLUMNS,
    },
    "timesheets": {
        "client_employee_id",
        "pay_code",
        "hours_worked",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        *METADATA_COLUMNS,
    },
}


class SchemaDriftError(ValueError):
    """A source schema is incompatible with the approved contract."""


@dataclass(frozen=True)
class SchemaReport:
    dataset: str
    source_name: str
    schema_version: str
    missing_optional_columns: tuple[str, ...]


def align_source_schema(
    df: pd.DataFrame,
    dataset: str,
    source_name: str,
) -> tuple[pd.DataFrame, SchemaReport]:
    """
    Check one source file against its approved schema.

    - Reject unknown columns.
    - Reject missing required columns.
    - Reject duplicate column names.
    - Fill missing optional columns with NULL.
    - Reorder columns to the approved schema.
    - Preserve row counts and source values.

    Call separately for each Bronze source export,
    before concatenating files.
    """
    if dataset not in SCHEMA_COLUMNS:
        raise ValueError(f"Unknown dataset: {dataset}")

    expected = SCHEMA_COLUMNS[dataset]
    required = REQUIRED_COLUMNS[dataset]
    actual = df.columns.tolist()

    if len(actual) != len(set(actual)):
        raise SchemaDriftError(
            f"{source_name}: duplicate column names detected"
        )

    unknown = sorted(set(actual) - set(expected))
    missing_required = sorted(required - set(actual))

    if unknown or missing_required:
        raise SchemaDriftError(
            f"{source_name}: incompatible schema. "
            f"Unknown columns={unknown}; "
            f"Missing required columns={missing_required}"
        )

    missing_optional = tuple(
        column
        for column in expected
        if column not in actual
    )

    # Reindex adds absent optional columns as NULL
    # and establishes deterministic column ordering.
    aligned = df.reindex(columns=expected).copy()

    if len(aligned) != len(df):
        raise AssertionError(
            "Schema alignment unexpectedly changed row count"
        )

    report = SchemaReport(
        dataset=dataset,
        source_name=source_name,
        schema_version=SCHEMA_VERSION,
        missing_optional_columns=missing_optional,
    )

    return aligned, report
