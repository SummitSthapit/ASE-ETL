
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text

from src.silver.schema_contract import SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

ENGINE = create_engine(
    os.environ["DATABASE_URL"],
    pool_pre_ping=True,
)

BUCKET = os.getenv("SILVER_BUCKET", "ase-silver")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=(
        os.getenv("MINIO_SECURE", "false").lower()
        == "true"
    ),
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


def get_publication():
    with ENGINE.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT
                    snapshot_fingerprint,
                    minio_prefix,
                    schema_version,
                    employee_count,
                    timesheet_count
                FROM silver._publication_registry
                WHERE publication_id = 1
            """)
        ).mappings().all()

    assert len(rows) == 1, (
        "Expected exactly one current publication"
    )

    return dict(rows[0])


def get_manifest(prefix):
    object_name = f"{prefix}/manifest.json"

    response = MINIO_CLIENT.get_object(
        BUCKET, object_name
    )

    try:
        manifest = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()

    return manifest


def test_publication_consistency():
    print("\nPUBLICATION CONSISTENCY")

    publication = get_publication()
    assert publication["schema_version"] == SCHEMA_VERSION

    prefix = publication["minio_prefix"]
    fingerprint = publication["snapshot_fingerprint"]

    manifest = get_manifest(prefix)
    assert manifest["schema_version"] == SCHEMA_VERSION

    assert prefix == f"runs/{fingerprint}"

    assert manifest["snapshot_fingerprint"] == fingerprint
    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["prefix"] == prefix
    assert manifest["counts"] == EXPECTED_COUNTS

    assert set(manifest["datasets"]) == set(
        EXPECTED_COUNTS
    )

    assert manifest["status"] == "prepared"

    assert publication["employee_count"] == 50
    assert publication["timesheet_count"] == 381732

    with ENGINE.connect() as connection:
        employees = connection.execute(
            text("SELECT COUNT(*) FROM silver.employees")
        ).scalar_one()

        timesheets = connection.execute(
            text("SELECT COUNT(*) FROM silver.timesheets")
        ).scalar_one()

    assert employees == publication["employee_count"]
    assert timesheets == publication["timesheet_count"]

    print("PASS: Manifest matches publication registry")
    print("PASS: PostgreSQL counts match registry")
    print("PASS: Current snapshot prefix is consistent")

    # The manifest describes a prepared snapshot.
    # The PostgreSQL registry is our source of truth
    # for identifying the committed publication.


def test_transaction_rollback():
    print("\nTRANSACTION ROLLBACK TEST")

    with ENGINE.connect() as connection:
        before = connection.execute(
            text("""
                SELECT
                    client_employee_id,
                    first_name
                FROM silver.employees
                ORDER BY client_employee_id
                LIMIT 1
            """)
        ).mappings().one()

    employee_id = before["client_employee_id"]
    original_name = before["first_name"]

    class DeliberateRollback(Exception):
        pass

    try:
        # SQLAlchemy automatically rolls back an
        # uncommitted transaction when this exception
        # exits the ENGINE.begin() context.
        with ENGINE.begin() as connection:

            connection.execute(
                text("""
                    UPDATE silver.employees
                    SET first_name = :temporary
                    WHERE client_employee_id = :id
                """),
                {
                    "temporary": "__ROLLBACK_TEST__",
                    "id": employee_id,
                },
            )

            temporary_value = connection.execute(
                text("""
                    SELECT first_name
                    FROM silver.employees
                    WHERE client_employee_id = :id
                """),
                {"id": employee_id},
            ).scalar_one()

            assert temporary_value == "__ROLLBACK_TEST__"

            raise DeliberateRollback(
                "Simulated transaction failure"
            )

    except DeliberateRollback:
        print("Simulated failure triggered")

    with ENGINE.connect() as connection:
        after = connection.execute(
            text("""
                SELECT first_name
                FROM silver.employees
                WHERE client_employee_id = :id
            """),
            {"id": employee_id},
        ).scalar_one()

    assert after == original_name, (
        "Rollback failed: committed data changed"
    )

    print("PASS: Transaction rolled back")
    print("PASS: Original employee value preserved")


def main():
    print("=" * 65)
    print("ASE-ETL SILVER PUBLICATION VERIFICATION")
    print("=" * 65)

    test_publication_consistency()
    test_transaction_rollback()

    print("\n" + "=" * 65)
    print("ALL PUBLICATION CHECKS PASSED")
    print("=" * 65)


if __name__ == "__main__":
    main()
