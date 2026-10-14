from pathlib import Path
import pandas as pd

BRONZE_ROOT = Path("data/bronze/parquet")

MISSING_MARKERS = {
    "",
    "[NULL]",
    "NULL",
    "N/A",
    "NA",
    "NAN",
    "NONE",
    "UNKNOWN",
    "NOT AVAILABLE",
}

MASKED_COLUMNS = [
    "address",
    "city",
    "state",
    "zip",
    "country",
    "cell_phone",
    "work_phone",
]

METADATA_COLUMNS = {
    "source_file",
    "source_row_number",
    "ingested_at_utc",
    "ingestion_batch_id",
}


def load_dataset(pattern):
    files = sorted(BRONZE_ROOT.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No files found matching {pattern}"
        )

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )


def normalize(series):
    result = series.astype("string").str.strip()

    return result.mask(
        result.str.upper().isin(MISSING_MARKERS)
    )


# =====================================================
# 1. EMPLOYEE MASKED FIELD ANALYSIS
# =====================================================

employees = load_dataset("employee_*/*.parquet")

print("\n" + "=" * 70)
print("EMPLOYEE MASKED FIELD ANALYSIS")
print("=" * 70)

for column in MASKED_COLUMNS:
    values = normalize(employees[column])

    # Detect values composed entirely of X or x.
    fully_masked = values.str.fullmatch(
        r"[Xx]+",
        na=False,
    )

    # Detect partially masked values, such as
    # 978xxxxxxx or 986xxxxxxx.
    partially_masked = (
        values.str.contains(r"[Xx]", na=False)
        & ~fully_masked
    )

    print(f"\nCOLUMN: {column}")
    print("Total rows:", len(values))
    print("Missing:", int(values.isna().sum()))
    print("Fully masked:", int(fully_masked.sum()))
    print(
        "Partially masked:",
        int(partially_masked.sum()),
    )
    print(
        "Usable/unmasked:",
        int(
            (
                values.notna()
                & ~fully_masked
                & ~partially_masked
            ).sum()
        ),
    )


# =====================================================
# 2. TIMESHEET DUPLICATE MULTIPLICITY
# =====================================================

timesheets = load_dataset("timesheet_*/*.parquet")

business_columns = [
    col for col in timesheets.columns
    if col not in METADATA_COLUMNS
]

normalized = timesheets[business_columns].copy()

for column in business_columns:
    normalized[column] = normalize(
        normalized[column]
    )

# Group by every normalized business column.
# dropna=False ensures NULL-containing rows
# participate in duplicate detection.

group_sizes = (
    normalized.groupby(
        business_columns,
        dropna=False,
    )
    .size()
)

duplicate_groups = group_sizes[group_sizes > 1]

print("\n" + "=" * 70)
print("TIMESHEET DUPLICATE GROUP ANALYSIS")
print("=" * 70)

print("Total timesheet rows:", len(timesheets))

print(
    "Unique business records:",
    len(group_sizes),
)

print(
    "Duplicate groups:",
    len(duplicate_groups),
)

print(
    "Additional duplicate occurrences:",
    int((duplicate_groups - 1).sum()),
)

print("\nGROUP SIZE DISTRIBUTION:")

print(
    duplicate_groups.value_counts()
    .sort_index()
    .to_string()
)

print("\nMaximum duplicate group size:")

print(
    int(group_sizes.max())
)

print("\nSOURCE FILE RECORD COUNTS:")

print(
    timesheets["source_file"]
    .value_counts()
    .sort_index()
    .to_string()
)