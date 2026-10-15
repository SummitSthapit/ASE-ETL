"""Gold pipeline orchestration and source verification."""

from sqlalchemy import text

from src.silver.publish import ENGINE
from src.gold.config import GOLD_MODEL_VERSION

from datetime import datetime, timezone

from src.gold.migrations import (
    ensure_dim_employee,
    ensure_dim_date,
    ensure_fact_timesheet,
    ensure_gold_registry,
)

from src.gold.transformations import (
    load_dim_employee,
    load_dim_date,
    load_fact_timesheet,
    remove_stale_facts,
    remove_stale_employees,
)

from src.gold.validation import (
    validate_gold_constraints,
    validate_gold_data,
    validate_employee_profiles,
    validate_dim_date,
)


SUPPORTED_SILVER_SCHEMA_VERSION = "2.0"

GOLD_ADVISORY_LOCK_ID = 7182402


def verify_silver_publication(connection):
    """
    Verify that the active Silver PostgreSQL data matches
    the registered Silver v2 publication.

    This does not modify Silver or Gold.
    """

    publication = connection.execute(
        text("""
            SELECT
                snapshot_fingerprint,
                minio_prefix,
                schema_version,
                employee_count,
                timesheet_count,
                published_at
            FROM silver._publication_registry
            WHERE publication_id = 1
        """)
    ).mappings().one_or_none()

    if publication is None:
        raise RuntimeError(
            "No active Silver publication found"
        )

    if publication["schema_version"] != (
        SUPPORTED_SILVER_SCHEMA_VERSION
    ):
        raise ValueError(
            "Incompatible Silver schema version: "
            f"{publication['schema_version']}"
        )

    for table, expected_column in (
        ("employees", "employee_count"),
        ("timesheets", "timesheet_count"),
    ):
        actual_count = connection.execute(
            text(f"SELECT COUNT(*) FROM silver.{table}")
        ).scalar_one()

        expected_count = publication[expected_column]

        if actual_count != expected_count:
            raise ValueError(
                f"Silver {table} count mismatch: "
                f"registry={expected_count}, "
                f"database={actual_count}"
            )

    print(
        "PASS: Active Silver publication verified"
    )
    print(
        "Silver fingerprint:",
        publication["snapshot_fingerprint"],
    )
    print(
        "Gold model version:",
        GOLD_MODEL_VERSION,
    )

    return publication

def run_gold_pipeline():
    """
    Synchronize PostgreSQL Gold with the active Silver
    publication in a single database transaction.

    MinIO publication is handled separately.
    """

    print("=" * 65)
    print("ASE-ETL GOLD PIPELINE")
    print("=" * 65)

    with ENGINE.begin() as connection:

        # 1. Coordinate with the Silver publisher.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(7182401)")
        )

        # 2. Prevent concurrent Gold executions.
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                ":lock_id)"
            ),
            {"lock_id": GOLD_ADVISORY_LOCK_ID},
        )

        # 3. Verify active Silver publication.
        publication = verify_silver_publication(
            connection
        )

        # 4. Ensure Gold database structures exist.
        print("\nVERIFYING GOLD SCHEMA")

        ensure_dim_employee(connection)
        ensure_dim_date(connection)
        ensure_fact_timesheet(connection)
        ensure_gold_registry(connection)

        # 5. Load dimensions.
        print("\nLOADING DIMENSIONS")

        load_dim_employee(connection)
        load_dim_date(connection)

        # 6. Load facts.
        print("\nLOADING FACTS")

        load_fact_timesheet(connection)

        # 7. Synchronize removed records.
        print("\nSYNCHRONIZING GOLD")

        remove_stale_facts(connection)
        remove_stale_employees(connection)

        # 8. Validate all Gold tables.
        print("\nVALIDATING GOLD")

        validate_gold_constraints(connection)
        validate_gold_data(connection)
        validate_employee_profiles(connection)
        validate_dim_date(connection)

        # 9. Calculate committed record counts.
        counts = {}

        for table in (
            "dim_employee",
            "dim_date",
            "fact_timesheet",
        ):
            counts[table] = connection.execute(
                text(f"SELECT COUNT(*) FROM gold.{table}")
            ).scalar_one()

        # 10. Record database readiness.
        # This is NOT a completed MinIO publication.
        connection.execute(
            text("""
                INSERT INTO gold._publication_registry (
                    publication_id,
                    silver_snapshot_fingerprint,
                    gold_model_version,
                    employee_count,
                    date_count,
                    timesheet_count,
                    publication_status,
                    database_committed_at,
                    gold_snapshot_fingerprint,
                    minio_prefix,
                    published_at
                )
                VALUES (
                    1,
                    :silver_fingerprint,
                    :model_version,
                    :employee_count,
                    :date_count,
                    :timesheet_count,
                    'DB_READY',
                    :committed_at,
                    NULL,
                    NULL,
                    NULL
                )
                ON CONFLICT (publication_id)
                DO UPDATE SET
                    silver_snapshot_fingerprint =
                        EXCLUDED.silver_snapshot_fingerprint,
                    gold_model_version =
                        EXCLUDED.gold_model_version,
                    employee_count =
                        EXCLUDED.employee_count,
                    date_count =
                        EXCLUDED.date_count,
                    timesheet_count =
                        EXCLUDED.timesheet_count,
                    publication_status = 'DB_READY',
                    database_committed_at =
                        EXCLUDED.database_committed_at,
                    gold_snapshot_fingerprint = NULL,
                    minio_prefix = NULL,
                    published_at = NULL
            """),
            {
                "silver_fingerprint":
                    publication["snapshot_fingerprint"],
                "model_version": GOLD_MODEL_VERSION,
                "employee_count": counts["dim_employee"],
                "date_count": counts["dim_date"],
                "timesheet_count": counts["fact_timesheet"],
                "committed_at": datetime.now(timezone.utc),
            },
        )

        print("\nGOLD DATABASE VALIDATION PASSED")
        print("Employees:", counts["dim_employee"])
        print("Dates:", counts["dim_date"])
        print("Timesheets:", counts["fact_timesheet"])

    # Reaching here means PostgreSQL committed successfully.
    print("\nGOLD DATABASE TRANSACTION COMMITTED")
    print("Publication status: DB_READY")
    print("MinIO publication: PENDING")

if __name__ == "__main__":
    run_gold_pipeline()