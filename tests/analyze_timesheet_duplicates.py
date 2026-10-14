from pathlib import Path
import pandas as pd

ROOT = Path("data/bronze/parquet")

files = sorted(ROOT.glob("timesheet_*/*.parquet"))

df = pd.concat(
    [pd.read_parquet(path) for path in files],
    ignore_index=True
)

metadata_columns = [
    "source_file",
    "source_row_number",
    "ingested_at_utc",
    "ingestion_batch_id",
]

business_columns = [
    col for col in df.columns
    if col not in metadata_columns
]

missing_markers = {
    "", "[NULL]", "NULL", "N/A",
    "NA", "NAN", "NONE",
    "UNKNOWN", "NOT AVAILABLE",
}

normalized = df[business_columns].copy()

for col in business_columns:
    normalized[col] = (
        normalized[col]
        .astype("string")
        .str.strip()
    )

    normalized[col] = normalized[col].mask(
        normalized[col].str.upper().isin(
            missing_markers
        )
    )

# Duplicate groups across all source files
group_ids = pd.util.hash_pandas_object(
    normalized,
    index=False
)

# Use exact normalized row comparison for the
# authoritative count; hashes aid grouping below.
duplicate_mask = normalized.duplicated(
    keep=False
)

duplicate_rows = df.loc[duplicate_mask].copy()

duplicate_rows["_group_hash"] = (
    group_ids.loc[duplicate_mask].to_numpy()
)

print("\nDUPLICATE DISTRIBUTION BY SOURCE FILE")
print(
    duplicate_rows.groupby("source_file")
    .size()
    .to_string()
)

print("\nDUPLICATES WITHIN EACH SOURCE FILE")

for source_file, indices in df.groupby(
    "source_file"
).groups.items():

    within = normalized.loc[indices]

    count = within.duplicated().sum()

    print(f"{source_file}: {count}")

print("\nCROSS-FILE DUPLICATE GROUPS")

cross_file = (
    duplicate_rows.groupby("_group_hash")[
        "source_file"
    ]
    .nunique()
)

print(
    "Potential groups spanning multiple files:",
    int((cross_file > 1).sum())
)

print("\nEXAMPLE DUPLICATE GROUPS")

example_hashes = cross_file[
    cross_file > 1
].index[:5]

for group_hash in example_hashes:
    sample = duplicate_rows[
        duplicate_rows["_group_hash"] == group_hash
    ]

    print("\n", sample[
        [
            "client_employee_id",
            "punch_apply_date",
            "punch_in_datetime",
            "punch_out_datetime",
            "hours_worked",
            "pay_code",
            "source_file",
            "source_row_number",
        ]
    ].to_string(index=False))