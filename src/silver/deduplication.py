
import pandas as pd

from src.silver.config import (
    METADATA_COLUMNS,
    TIMESHEET_BUSINESS_COLUMNS,
)


def deduplicate_timesheets(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove identical business-content occurrences across
    timesheet exports.

    Survivor policy:
    1. Prefer the most recent source export filename.
    2. Break ties using the lowest source row number.

    Returns:
        clean: Unique timesheet business records.
        audit: Removed occurrences with survivor lineage.

    Does not modify the input DataFrame.
    """

    required_columns = (
        TIMESHEET_BUSINESS_COLUMNS + METADATA_COLUMNS
    )

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Normalize the DataFrame index to prevent alignment
    # problems after concatenating several Parquet parts.
    working = df.reset_index(drop=True).copy()

    # Sort deterministically by export and source position.
    # Current source filenames contain sortable timestamps.
    working = working.sort_values(
        by=[
            "source_file",
            "source_row_number",
            "ingestion_batch_id",
        ],
        ascending=[False, True, True],
        kind="stable",
    ).reset_index(drop=True)

    # Every business column participates in the match.
    # dropna=False treats matching missing fields as equal.
    group_ids = working.groupby(
        TIMESHEET_BUSINESS_COLUMNS,
        dropna=False,
        sort=False,
    ).ngroup()

    working["_duplicate_group_id"] = group_ids

    # The first sorted occurrence is our survivor.
    duplicate_mask = working.duplicated(
        subset=TIMESHEET_BUSINESS_COLUMNS,
        keep="first",
    )

    clean = working.loc[~duplicate_mask].copy()
    removed = working.loc[duplicate_mask].copy()

    # Locate the surviving record for each duplicate group.
    survivors = clean[
        [
            "_duplicate_group_id",
            "source_file",
            "source_row_number",
            "ingestion_batch_id",
        ]
    ].rename(
        columns={
            "source_file": "kept_source_file",
            "source_row_number": "kept_source_row_number",
            "ingestion_batch_id": "kept_ingestion_batch_id",
        }
    )

    audit = removed[
        [
            "_duplicate_group_id",
            "source_file",
            "source_row_number",
            "ingestion_batch_id",
        ]
    ].merge(
        survivors,
        on="_duplicate_group_id",
        how="left",
        validate="many_to_one",
    )

    # Group numbers are run-local diagnostics, not stable
    # business identifiers, so exclude them from outputs.
    clean = clean.drop(
        columns=["_duplicate_group_id"]
    )

    audit = audit.drop(
        columns=["_duplicate_group_id"]
    )

    audit.insert(
        0,
        "audit_reason",
        "DUPLICATE_BUSINESS_RECORD",
    )

    # Return consistent sequential indices.
    return (
        clean.reset_index(drop=True),
        audit.reset_index(drop=True),
    )
