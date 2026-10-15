
"""End-to-end Silver v2 dry run without modifying Silver v1."""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.silver import pipeline
from src.silver.config import (
    PROJECT_ROOT,
    SILVER_ROOT,
)
from src.silver.schema_contract import (
    SCHEMA_COLUMNS,
    SCHEMA_VERSION,
)


EXPECTED_COUNTS = {
    "employees": 50,
    "timesheets": 381732,
    "audit/duplicate_records": 30839,
    "audit/quality_issues": 0,
    "audit/rejected_employees": 0,
    "audit/rejected_timesheets": 0,
    "audit/schema_evolution": 11,
}


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def snapshot_hashes(root: Path) -> dict:
    """Record exact bytes of existing Silver files."""
    return {
        str(path.relative_to(root)): file_hash(path)
        for path in sorted(root.rglob("*.parquet"))
    }


def read_dataset(root: Path, name: str) -> pd.DataFrame:
    files = sorted((root / name).glob("*.parquet"))

    assert files, f"Missing dataset: {name}"

    return pd.concat(
        [pd.read_parquet(path) for path in files],
        ignore_index=True,
    )


def main():
    print("=" * 65)
    print("SILVER V2 ISOLATED END-TO-END TEST")
    print("=" * 65)

    original_hashes = snapshot_hashes(SILVER_ROOT)

    assert original_hashes, (
        "Existing Silver v1 Parquet files not found"
    )

    data_root = PROJECT_ROOT / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="silver_v2_dryrun_",
        dir=data_root,
    ) as temporary:
        candidate_root = Path(temporary)

        print("\nTemporary output:", candidate_root)

        # Redirect only the pipeline output root.
        # Existing data/silver remains untouched.
        with patch.object(
            pipeline,
            "SILVER_ROOT",
            candidate_root,
        ):
            pipeline.main()

        print("\nVERIFYING SILVER V2 OUTPUT")

        for name, expected in EXPECTED_COUNTS.items():
            df = read_dataset(candidate_root, name)

            actual = len(df)

            assert actual == expected, (
                f"{name}: expected {expected}, "
                f"found {actual}"
            )

            print(f"PASS: {name}: {actual:,} rows")

        employees = read_dataset(
            candidate_root, "employees"
        )

        timesheets = read_dataset(
            candidate_root, "timesheets"
        )

        schema_audit = read_dataset(
            candidate_root,
            "audit/schema_evolution",
        )

        assert employees.columns.tolist() == (
            SCHEMA_COLUMNS["employees"]
        )

        assert timesheets.columns.tolist() == (
            SCHEMA_COLUMNS["timesheets"]
        )

        assert len(employees.columns) == 38
        assert len(timesheets.columns) == 18

        print("PASS: Employee schema has 38 columns")
        print("PASS: Timesheet schema has 18 columns")

        assert schema_audit["schema_version"].eq(
            SCHEMA_VERSION
        ).all()

        assert schema_audit["schema_status"].eq(
            "PASS"
        ).all()

        assert int(schema_audit["source_rows"].sum()) == (
            50 + 412571
        )

        print("PASS: Schema audit and row reconciliation")

        # Confirm previously excluded fields now exist.
        restored_columns = [
            "middle_name",
            "years_of_experience",
            "zip",
            "manager_employee_name",
            "is_per_deim",
            "work_phone",
            "clinical_level",
            "address",
            "city",
            "state",
            "country",
            "cell_phone",
        ]

        for column in restored_columns:
            assert column in employees.columns, (
                f"Missing restored employee field: {column}"
            )

        print("PASS: All 12 employee fields restored")

    # Temporary output is automatically removed.
    # Verify the existing Silver files are unchanged.
    after_hashes = snapshot_hashes(SILVER_ROOT)

    assert after_hashes == original_hashes, (
        "Existing Silver v1 files unexpectedly changed!"
    )

    print("\nPASS: Existing Silver v1 files unchanged")
    print("PASS: Temporary Silver v2 files cleaned up")

    print("\n" + "=" * 65)
    print("ALL SILVER V2 ISOLATED CHECKS PASSED")
    print("=" * 65)


if __name__ == "__main__":
    main()
