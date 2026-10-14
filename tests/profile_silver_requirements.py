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

DATASETS = {
    "employee": sorted(
        BRONZE_ROOT.glob("employee_*/*.parquet")
    ),
    "timesheet": sorted(
        BRONZE_ROOT.glob("timesheet_*/*.parquet")
    ),
}

DATE_COLUMNS = {
    "employee": [
        "job_start_date",
        "dob",
        "hire_date",
        "recent_hire_date",
        "anniversary_date",
        "term_date",
    ],
    "timesheet": [
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
    ],
}

NUMERIC_COLUMNS = {
    "employee": [
        "years_of_experience",
        "scheduled_weekly_hour",
    ],
    "timesheet": ["hours_worked"],
}


def normalize_missing(series):
    text = series.astype("string").str.strip()

    return text.mask(
        text.str.upper().isin(MISSING_MARKERS)
    )


for dataset, files in DATASETS.items():
    if not files:
        raise FileNotFoundError(
            f"No Bronze files found for {dataset}"
        )

    df = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    print("\n" + "=" * 80)
    print("DATASET:", dataset.upper())
    print("ROWS:", len(df))

    print("\nMISSING VALUES:")
    for column in df.columns:
        if column in {
            "source_file",
            "source_row_number",
            "ingested_at_utc",
            "ingestion_batch_id",
        }:
            continue

        cleaned = normalize_missing(df[column])
        count = int(cleaned.isna().sum())

        if count:
            print(f"{column}: {count}")

    print("\nINVALID DATE/TIMESTAMP VALUES:")
    for column in DATE_COLUMNS[dataset]:
        cleaned = normalize_missing(df[column])

        converted = pd.to_datetime(
            cleaned,
            errors="coerce",
            format=(
                "%Y-%m-%d"
                if dataset == "employee"
                or column == "punch_apply_date"
                else "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

        invalid = (
            cleaned.notna() & converted.isna()
        ).sum()

        print(f"{column}: {invalid}")

    print("\nINVALID NUMERIC VALUES:")
    for column in NUMERIC_COLUMNS[dataset]:
        cleaned = normalize_missing(df[column])

        converted = pd.to_numeric(
            cleaned,
            errors="coerce",
        )

        invalid = (
            cleaned.notna() & converted.isna()
        ).sum()

        print(f"{column}: {invalid}")

    print("\nDUPLICATE ANALYSIS:")

    if dataset == "timesheet":
        business_columns = [
            col for col in df.columns
            if col not in {
                "source_file",
                "source_row_number",
                "ingested_at_utc",
                "ingestion_batch_id",
            }
        ]

        normalized_business = df[
            business_columns
        ].apply(normalize_missing)

        duplicates = normalized_business.duplicated(
            keep=False
        )

        print(
            "Rows belonging to duplicate groups:",
            int(duplicates.sum()),
        )

        print(
            "Additional duplicate occurrences:",
            int(
                normalized_business.duplicated().sum()
            ),
        )

    else:
        employee_ids = normalize_missing(
            df["client_employee_id"]
        )

        print(
            "Duplicate employee IDs:",
            int(
                employee_ids.dropna().duplicated().sum()
            ),
        )