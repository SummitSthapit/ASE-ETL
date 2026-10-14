
import pandas as pd

from src.silver.config import MISSING_MARKERS


def _normalize(series: pd.Series) -> pd.Series:
    """Normalize raw values for validation comparisons."""
    values = series.astype("string").str.strip()

    return values.mask(
        values.str.upper().isin(MISSING_MARKERS)
    )


def _add_issue(
    issues: list,
    df: pd.DataFrame,
    mask: pd.Series,
    issue_code: str,
    severity: str,
) -> None:
    """Record source references for affected rows."""

    affected = df.loc[mask.fillna(False)]

    for _, row in affected.iterrows():
        issues.append({
            "source_file": row["source_file"],
            "source_row_number": row["source_row_number"],
            "ingestion_batch_id": row["ingestion_batch_id"],
            "issue_code": issue_code,
            "severity": severity,
        })


def validate_employees(
    raw: pd.DataFrame,
    silver: pd.DataFrame,
) -> pd.DataFrame:
    """Validate transformed employee records."""

    issues = []

    missing_id = silver["client_employee_id"].isna()

    _add_issue(
        issues,
        silver,
        missing_id,
        "MISSING_EMPLOYEE_ID",
        "CRITICAL",
    )

    for column in [
        "job_start_date",
        "dob",
        "hire_date",
        "recent_hire_date",
        "anniversary_date",
        "term_date",
    ]:
        original = _normalize(raw[column])

        invalid = (
            original.notna()
            & silver[column].isna()
        )

        _add_issue(
            issues,
            silver,
            invalid,
            f"INVALID_{column.upper()}",
            "CRITICAL",
        )

    weekly_hours = silver["scheduled_weekly_hour"]

    invalid_hours = (
        _normalize(raw["scheduled_weekly_hour"]).notna()
        & weekly_hours.isna()
    )

    _add_issue(
        issues,
        silver,
        invalid_hours,
        "INVALID_SCHEDULED_WEEKLY_HOURS",
        "CRITICAL",
    )

    negative_hours = weekly_hours < 0

    _add_issue(
        issues,
        silver,
        negative_hours,
        "NEGATIVE_SCHEDULED_WEEKLY_HOURS",
        "CRITICAL",
    )

    return pd.DataFrame(
        issues,
        columns=[
            "source_file",
            "source_row_number",
            "ingestion_batch_id",
            "issue_code",
            "severity",
        ],
    )


def validate_timesheets(
    raw: pd.DataFrame,
    silver: pd.DataFrame,
) -> pd.DataFrame:
    """Validate transformed timesheet records."""

    issues = []

    for column in [
        "client_employee_id",
        "pay_code",
        "punch_apply_date",
    ]:
        _add_issue(
            issues,
            silver,
            silver[column].isna(),
            f"MISSING_{column.upper()}",
            "CRITICAL",
        )

    numeric_raw = _normalize(raw["hours_worked"])

    invalid_hours = (
        numeric_raw.notna()
        & silver["hours_worked"].isna()
    )

    _add_issue(
        issues,
        silver,
        invalid_hours,
        "INVALID_HOURS_WORKED",
        "CRITICAL",
    )

    _add_issue(
        issues,
        silver,
        silver["hours_worked"].isna(),
        "MISSING_HOURS_WORKED",
        "CRITICAL",
    )

    _add_issue(
        issues,
        silver,
        silver["hours_worked"] < 0,
        "NEGATIVE_HOURS_WORKED",
        "CRITICAL",
    )

    date_columns = [
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
    ]

    for column in date_columns:
        original = _normalize(raw[column])

        invalid = (
            original.notna()
            & silver[column].isna()
        )

        _add_issue(
            issues,
            silver,
            invalid,
            f"INVALID_{column.upper()}",
            "CRITICAL",
        )

    punch_in = silver["punch_in_datetime"]
    punch_out = silver["punch_out_datetime"]

    # Missing punch timestamps are flagged, not
    # automatically rejected: some pay codes may
    # describe non-worked or administrative hours.
    for column in [
        "punch_in_datetime",
        "punch_out_datetime",
    ]:
        _add_issue(
            issues,
            silver,
            silver[column].isna(),
            f"MISSING_{column.upper()}",
            "WARNING",
        )

    # Overnight shifts are valid because the full
    # dates are compared, not just the clock times.
    reversed_punch = (
        punch_in.notna()
        & punch_out.notna()
        & (punch_out < punch_in)
    )

    _add_issue(
        issues,
        silver,
        reversed_punch,
        "PUNCH_OUT_BEFORE_PUNCH_IN",
        "CRITICAL",
    )

    return pd.DataFrame(
        issues,
        columns=[
            "source_file",
            "source_row_number",
            "ingestion_batch_id",
            "issue_code",
            "severity",
        ],
    )
