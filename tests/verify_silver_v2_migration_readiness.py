
"""Read-only PostgreSQL preflight before Silver v2 migration."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

ENGINE = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)

RESTORED_COLUMNS = [
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


def main():
    print("=" * 65)
    print("SILVER V2 POSTGRESQL MIGRATION PREFLIGHT")
    print("=" * 65)

    with ENGINE.connect() as connection:
        identity = connection.execute(
            text("""
                SELECT
                    current_database() AS database_name,
                    current_user AS database_user
            """)
        ).mappings().one()

        print("\nDATABASE CONNECTION")
        print("Database:", identity["database_name"])
        print("User:", identity["database_user"])

        # Protect against accidentally checking
        # the wrong PostgreSQL installation.
        assert identity["database_name"] == "ase_etl"
        assert identity["database_user"] == "ase_user"

        inspector = inspect(connection)

        assert inspector.has_table(
            "employees", schema="silver"
        )
        assert inspector.has_table(
            "timesheets", schema="silver"
        )
        assert inspector.has_table(
            "_publication_registry", schema="silver"
        )

        print("\nCURRENT EMPLOYEE SCHEMA")

        columns = inspector.get_columns(
            "employees",
            schema="silver",
        )

        existing_columns = {
            column["name"]: column
            for column in columns
        }

        print("Column count:", len(columns))

        for column in columns:
            print(
                f"{column['name']}: "
                f"{column['type']}"
            )

        print("\nRESTORED COLUMN STATUS")

        already_present = []
        missing = []

        for column in RESTORED_COLUMNS:
            if column in existing_columns:
                already_present.append(column)
                print(f"PRESENT: {column}")
            else:
                missing.append(column)
                print(f"MISSING: {column}")

        print("\nSUMMARY")
        print("Already present:", len(already_present))
        print("Need migration:", len(missing))

        employee_pk = inspector.get_pk_constraint(
            "employees",
            schema="silver",
        )

        timesheet_pk = inspector.get_pk_constraint(
            "timesheets",
            schema="silver",
        )

        print("\nPRIMARY KEYS")
        print(
            "Employees:",
            employee_pk["constrained_columns"],
        )
        print(
            "Timesheets:",
            timesheet_pk["constrained_columns"],
        )

        assert set(employee_pk["constrained_columns"]) == {
            "client_employee_id",
        }

        assert set(timesheet_pk["constrained_columns"]) == {
            "client_employee_id",
            "punch_in_datetime",
            "punch_out_datetime",
        }

        employees = connection.execute(
            text("SELECT COUNT(*) FROM silver.employees")
        ).scalar_one()

        timesheets = connection.execute(
            text("SELECT COUNT(*) FROM silver.timesheets")
        ).scalar_one()

        print("\nCURRENT ROW COUNTS")
        print("Employees:", employees)
        print("Timesheets:", timesheets)

        assert employees == 50
        assert timesheets == 381732

        publication = connection.execute(
            text("""
                SELECT
                    snapshot_fingerprint,
                    minio_prefix,
                    employee_count,
                    timesheet_count
                FROM silver._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().one()

        print("\nCURRENT PUBLICATION")
        print(
            "Fingerprint:",
            publication["snapshot_fingerprint"],
        )
        print(
            "MinIO prefix:",
            publication["minio_prefix"],
        )

        assert publication["employee_count"] == employees
        assert publication["timesheet_count"] == timesheets

    print("\n" + "=" * 65)
    print("SILVER V2 MIGRATION PREFLIGHT PASSED")
    print("No database changes were made.")
    print("=" * 65)


if __name__ == "__main__":
    main()
