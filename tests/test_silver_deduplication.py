
from pathlib import Path

import pandas as pd

from src.silver.config import (
    BRONZE_ROOT,
    TIMESHEET_BUSINESS_COLUMNS,
)
from src.silver.transformations import transform_timesheets
from src.silver.deduplication import deduplicate_timesheets


def main():
    files = sorted(
        BRONZE_ROOT.glob("timesheet_*/*.parquet")
    )

    if not files:
        raise FileNotFoundError(
            "No Bronze timesheet Parquet files found."
        )

    raw = pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )

    transformed = transform_timesheets(raw)

    clean, audit = deduplicate_timesheets(transformed)

    print("=" * 65)
    print("SILVER TIMESHEET DEDUPLICATION TEST")
    print("=" * 65)

    print("Bronze rows:", len(raw))
    print("Silver unique rows:", len(clean))
    print("Removed occurrences:", len(audit))

    print("\nREMOVED OCCURRENCES BY SOURCE")
    print(
        audit["source_file"]
        .value_counts()
        .sort_index()
        .to_string()
    )

    print("\nRECONCILIATION")
    print(
        "Clean + removed:",
        len(clean) + len(audit)
    )

    remaining_duplicates = clean.duplicated(
        subset=TIMESHEET_BUSINESS_COLUMNS
    ).sum()

    print(
        "Remaining duplicates:",
        remaining_duplicates
    )

    assert len(raw) == 412571
    assert len(clean) == 381732
    assert len(audit) == 30839
    assert len(clean) + len(audit) == len(raw)
    assert remaining_duplicates == 0
    assert audit["kept_source_file"].notna().all()

    # Running deduplication again must not remove records.
    clean_again, audit_again = deduplicate_timesheets(
        clean
    )

    
    # Validate proposed Phase 2 composite primary key

    TIMESHEET_COMPOSITE_KEY = [
        "client_employee_id",
        "punch_in_datetime",
        "punch_out_datetime",
    ]

    key_duplicates = clean.duplicated(
        subset=TIMESHEET_COMPOSITE_KEY,
        keep=False,
    )

    duplicate_key_rows = clean.loc[key_duplicates]

    missing_key_rows = clean[
        TIMESHEET_COMPOSITE_KEY
    ].isna().any(axis=1)

    print("\n" + "=" * 65)
    print("TIMESHEET COMPOSITE PRIMARY KEY VALIDATION")
    print("=" * 65)

    print(
        "Total Silver candidate rows:",
        len(clean),
    )

    print(
        "Rows with duplicate composite keys:",
        int(key_duplicates.sum()),
    )

    print(
        "Additional duplicate composite key occurrences:",
        int(
            clean.duplicated(
                subset=TIMESHEET_COMPOSITE_KEY
            ).sum()
        ),
    )

    print(
        "Rows with NULL composite key components:",
        int(missing_key_rows.sum()),
    )

    if not duplicate_key_rows.empty:
        print("\nEXAMPLE COMPOSITE KEY COLLISIONS:")

        print(
            duplicate_key_rows.sort_values(
                TIMESHEET_COMPOSITE_KEY
            )[
                TIMESHEET_COMPOSITE_KEY
                + [
                    "pay_code",
                    "hours_worked",
                    "source_file",
                ]
            ]
            .head(20)
            .to_string(index=False)
        )

        assert len(clean_again) == len(clean)
        assert audit_again.empty

        print("\nALL DEDUPLICATION CHECKS PASSED")


if __name__ == "__main__":
    main()
