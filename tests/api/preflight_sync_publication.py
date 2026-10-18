
"""Read-only preflight for live employee synchronization.

Does not modify PostgreSQL or MinIO.
"""

from sqlalchemy import text

from src.api.database import engine


def main():
    with engine.connect() as connection:
        db = connection.execute(
            text("SELECT current_database()")
        ).scalar_one()

        print("=" * 60)
        print("PHASE 7 FINAL SYNCHRONIZATION PREFLIGHT")
        print("=" * 60)
        print("Connected database:", db)

        if db != "ase_etl":
            raise RuntimeError(
                "Expected development database ase_etl"
            )

        silver = connection.execute(
            text("""
                SELECT
                    snapshot_fingerprint,
                    employee_count,
                    timesheet_count,
                    published_at
                FROM silver._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().one_or_none()

        gold = connection.execute(
            text("""
                SELECT
                    silver_snapshot_fingerprint,
                    publication_status,
                    employee_count,
                    date_count,
                    timesheet_count,
                    published_at
                FROM gold._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().one_or_none()

        if silver is None or gold is None:
            raise RuntimeError(
                "Missing Silver or Gold publication registry"
            )

        counts = {}

        for name, query in {
            "silver_employees":
                "SELECT COUNT(*) FROM silver.employees",
            "silver_timesheets":
                "SELECT COUNT(*) FROM silver.timesheets",
            "gold_employees":
                "SELECT COUNT(*) FROM gold.dim_employee",
            "gold_dates":
                "SELECT COUNT(*) FROM gold.dim_date",
            "gold_timesheets":
                "SELECT COUNT(*) FROM gold.fact_timesheet",
        }.items():
            counts[name] = connection.execute(
                text(query)
            ).scalar_one()

        statuses = connection.execute(
            text("""
                SELECT sync_status, COUNT(*) AS total
                FROM api.employees
                GROUP BY sync_status
                ORDER BY sync_status
            """)
        ).all()

        changes = connection.execute(
            text("""
                SELECT
                    COUNT(*) AS event_count,
                    MAX(change_id) AS latest_change_id
                FROM api.employee_change_log
            """)
        ).mappings().one()

        print("\nSILVER PUBLICATION")
        print("Fingerprint:", silver["snapshot_fingerprint"])
        print("Employees:", silver["employee_count"])
        print("Timesheets:", silver["timesheet_count"])

        print("\nGOLD PUBLICATION")
        print("Status:", gold["publication_status"])
        print(
            "Source fingerprint:",
            gold["silver_snapshot_fingerprint"],
        )

        print("\nDATABASE COUNTS")
        for name, count in counts.items():
            print(f"{name}: {count:,}")

        print("\nAPI SYNCHRONIZATION STATUS")
        for status, count in statuses:
            print(f"{status}: {count}")

        print("\nCHANGE LEDGER")
        print("Total events:", changes["event_count"])
        print("Latest event:", changes["latest_change_id"])

        assert (
            silver["snapshot_fingerprint"]
            == gold["silver_snapshot_fingerprint"]
        ), "Silver and Gold publications do not match"

        assert (
            gold["publication_status"] == "PUBLISHED"
        ), "Gold publication is not finalized"

        assert (
            silver["employee_count"]
            == counts["silver_employees"]
        )

        assert (
            silver["timesheet_count"]
            == counts["silver_timesheets"]
        )

        assert (
            gold["employee_count"]
            == counts["gold_employees"]
        )

        assert (
            gold["date_count"]
            == counts["gold_dates"]
        )

        assert (
            gold["timesheet_count"]
            == counts["gold_timesheets"]
        )

        print("\nPASS: Registry and database counts agree")
        print("PASS: Silver and Gold fingerprints match")
        print("PASS: Gold publication is finalized")
        print("\nREAD-ONLY PREFLIGHT COMPLETED")


if __name__ == "__main__":
    main()
