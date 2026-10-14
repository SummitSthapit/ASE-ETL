
import pandas as pd

from src.silver.config import (
    MISSING_MARKERS,
    EMPLOYEE_DROP_COLUMNS,
    EMPLOYEE_DATE_COLUMNS,
    EMPLOYEE_NUMERIC_COLUMNS,
    TIMESHEET_DATE_COLUMNS,
    TIMESHEET_DATETIME_COLUMNS,
    TIMESHEET_NUMERIC_COLUMNS,
    METADATA_COLUMNS,
)


def normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize recognized textual missing markers to pd.NA.

    Preserve source identifiers as strings, including leading zeros.
    Does not modify the original DataFrame.
    """
    result = df.copy()

    for column in result.columns:
        if column in METADATA_COLUMNS:
            continue

        values = result[column].astype("string").str.strip()

        result[column] = values.mask(
            values.str.upper().isin(MISSING_MARKERS)
        )

    return result


def convert_dates(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Convert YYYY-MM-DD values to date objects.

    Invalid non-null values become NaT.
    Validation must separately detect conversion failures.
    """
    result = df.copy()

    for column in columns:
        converted = pd.to_datetime(
            result[column],
            format="%Y-%m-%d",
            errors="coerce",
        )

        result[column] = converted.dt.date

    return result


def convert_datetimes(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Convert source datetime strings to timestamp values.

    Source punch timestamps are treated as local/naive
    datetimes because the source does not specify a timezone.
    """
    result = df.copy()

    for column in columns:
        result[column] = pd.to_datetime(
            result[column],
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce",
        )

    return result


def convert_numeric(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Convert numeric strings to nullable numeric values.

    Invalid values become missing and will be handled
    by the validation stage.
    """
    result = df.copy()

    for column in columns:
        result[column] = pd.to_numeric(
            result[column],
            errors="coerce",
        ).astype("Float64")

    return result


def transform_employees(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean employee records and apply the Silver schema.
    """
    result = normalize_missing_values(df)

    # Exclude documented, unusable source columns.
    result = result.drop(
        columns=EMPLOYEE_DROP_COLUMNS
    )

    result = convert_dates(
        result,
        EMPLOYEE_DATE_COLUMNS,
    )

    result = convert_numeric(
        result,
        EMPLOYEE_NUMERIC_COLUMNS,
    )

    # Employee IDs remain strings.
    # Never convert them to numeric values.

    return result


def transform_timesheets(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Standardize timesheet records without deduplicating.

    Deduplication is a separate pipeline stage so
    discarded occurrences can be audited.
    """
    result = normalize_missing_values(df)

    result = convert_dates(
        result,
        TIMESHEET_DATE_COLUMNS,
    )

    result = convert_datetimes(
        result,
        TIMESHEET_DATETIME_COLUMNS,
    )

    result = convert_numeric(
        result,
        TIMESHEET_NUMERIC_COLUMNS,
    )

    # Do not overwrite hours_worked with calculated hours.
    # Preserve overnight punch timestamps as-is.

    return result
