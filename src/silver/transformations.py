
import pandas as pd

from src.silver.config import (
    MISSING_MARKERS,
    EMPLOYEE_DATE_COLUMNS,
    EMPLOYEE_NUMERIC_COLUMNS,
    EMPLOYEE_FULL_MASK_COLUMNS,
    EMPLOYEE_PHONE_COLUMNS,
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

def normalize_employee_masks(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert recognized source masking patterns to NULL.

    Do not modify original Bronze values.
    Do not treat ordinary strings containing 'x' as masked.
    """
    result = df.copy()

    # Entirely masked geographical/address fields.
    for column in EMPLOYEE_FULL_MASK_COLUMNS:
        if column not in result.columns:
            continue

        values = result[column].astype("string")

        masked = values.str.fullmatch(
            r"[Xx]+",
            na=False,
        )

        result[column] = values.mask(masked)

    # Phone numbers containing X placeholders are incomplete.
    # Keep all other values as strings.
    for column in EMPLOYEE_PHONE_COLUMNS:
        if column not in result.columns:
            continue

        values = result[column].astype("string")

        masked = values.str.contains(
            r"[Xx]",
            regex=True,
            na=False,
        )

        result[column] = values.mask(masked)

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
    Clean employee records while preserving all
    approved business attributes and source lineage.
    """
    result = normalize_missing_values(df)

    # Normalize recognized masking patterns.
    result = normalize_employee_masks(result)

    # Apply employee date types.
    result = convert_dates(
        result,
        EMPLOYEE_DATE_COLUMNS,
    )

    # Apply nullable numeric types.
    result = convert_numeric(
        result,
        EMPLOYEE_NUMERIC_COLUMNS,
    )

    # Employee IDs, ZIP codes, phone numbers, and
    # other identifiers remain strings.

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
