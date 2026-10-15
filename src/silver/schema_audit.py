
"""Structured audit records for Silver source-schema checks."""

import json

import pandas as pd

from src.silver.schema_contract import SchemaReport


AUDIT_COLUMNS = [
    "dataset",
    "source_name",
    "schema_version",
    "source_rows",
    "schema_status",
    "missing_optional_columns",
]


def make_schema_audit_record(
    report: SchemaReport,
    source_rows: int,
) -> dict:
    """
    Produce one auditable record per Bronze Parquet part.

    Column lists are JSON strings for stable Parquet
    serialization and future PostgreSQL compatibility.
    """
    missing = list(report.missing_optional_columns)

    return {
        "dataset": report.dataset,
        "source_name": report.source_name,
        "schema_version": report.schema_version,
        "source_rows": int(source_rows),
        "schema_status": (
            "ALIGNED_OPTIONAL"
            if missing
            else "PASS"
        ),
        "missing_optional_columns": json.dumps(
            missing
        ),
    }


def build_schema_audit(
    records: list[dict],
) -> pd.DataFrame:
    """Build a consistently ordered audit DataFrame."""
    return pd.DataFrame(
        records,
        columns=AUDIT_COLUMNS,
    )
