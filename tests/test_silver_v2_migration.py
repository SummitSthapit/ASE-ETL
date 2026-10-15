
"""Transactional rollback test for Silver v2 migration."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

from src.silver.migrations import (
    EMPLOYEE_V2_COLUMNS,
    migrate_employee_schema,
)


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

ENGINE = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)


def read_state(connection):
    inspector = inspect(connection)

    employee_columns = {
        column["name"]
        for column in inspector.get_columns(
            "employees", schema="silver"
        )
    }

    employee_count = connection.execute(
        text("SELECT COUNT(*) FROM silver.employees")
    ).scalar_one()

    timesheet_count = connection.execute(
        text("SELECT COUNT(*) FROM silver.timesheets")
    ).scalar_one()

    fingerprint = connection.execute(
        text("""
            SELECT snapshot_fingerprint
            FROM silver._publication_registry
            WHERE publication_id = 1
        """)
    ).scalar_one()

    return {
        "columns": employee_columns,
        "employees": employee_count,
        "timesheets": timesheet_count,
        "fingerprint": fingerprint,
    }


def main():
    print("=" * 65)
    print("SILVER V2 TRANSACTIONAL MIGRATION TEST")
    print("=" * 65)

    with ENGINE.connect() as connection:
        identity = connection.execute(
            text("""
                SELECT current_database(), current_user
            """)
        ).one()

        assert identity[0] == "ase_etl"
        assert identity[1] == "ase_user"

        before = read_state(connection)

    print("\nBEFORE MIGRATION")
    print("Employee columns:", len(before["columns"]))
    print("Employees:", before["employees"])
    print("Timesheets:", before["timesheets"])

    assert before["employees"] == 50
    assert before["timesheets"] == 381732

    # Run the migration but intentionally
    # never commit the transaction.
    with ENGINE.connect() as connection:
        transaction = connection.begin()

        try:
            added = migrate_employee_schema(connection)

            print("\nINSIDE TRANSACTION")
            print("Columns added:", len(added))

            for column in added:
                print("ADDED:", column)

            during = read_state(connection)

            assert set(EMPLOYEE_V2_COLUMNS).issubset(
                during["columns"]
            )

            assert len(during["columns"]) == 38
            assert during["employees"] == 50
            assert during["timesheets"] == 381732

            print("PASS: All 38 employee columns present")
            print("PASS: Existing row counts preserved")

        finally:
            # Always roll back, including if
            # any assertion or migration fails.
            transaction.rollback()
            print("\nTransaction rolled back")

    # Use a new connection to inspect the
    # committed database after rollback.
    with ENGINE.connect() as connection:
        after = read_state(connection)

    assert after == before, (
        "Database state changed despite rollback"
    )

    print("\nAFTER ROLLBACK")
    print("Employee columns:", len(after["columns"]))
    print("Employees:", after["employees"])
    print("Timesheets:", after["timesheets"])

    print("PASS: Original employee schema preserved")
    print("PASS: Row counts unchanged")
    print("PASS: Publication fingerprint unchanged")

    print("\n" + "=" * 65)
    print("ALL SILVER V2 MIGRATION CHECKS PASSED")
    print("No migration was committed.")
    print("=" * 65)


if __name__ == "__main__":
    main()
