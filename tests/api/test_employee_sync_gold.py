
"""Verify actual API changes through the Gold employee dimension.

This test ONLY runs on ase_etl_test.
It does not publish MinIO snapshots or modify development data.
"""

import pandas as pd
import pytest

from sqlalchemy import text

from src.api.database import engine
from src.api.employee_sync_stage import stage_employee_overlay
from src.gold.transformations import load_dim_employee
from src.silver.publish import sql_types

# Reuse fixtures and helpers from the previous passing tests.
from test_employee_sync_api_to_silver import (
    prerequisites,
    api_context,
    source_frame,
    events_for,
)


GOLD_COLUMNS = {
    "first_name": "VARCHAR",
    "middle_name": "VARCHAR",
    "last_name": "VARCHAR",
    "preferred_name": "VARCHAR",
    "job_code": "VARCHAR",
    "job_title": "VARCHAR",
    "job_start_date": "DATE",
    "organization_id": "VARCHAR",
    "organization_name": "VARCHAR",
    "department_id": "VARCHAR",
    "department_name": "VARCHAR",
    "hire_date": "DATE",
    "recent_hire_date": "DATE",
    "term_date": "DATE",
    "manager_employee_id": "VARCHAR",
    "fte_status": "VARCHAR",
    "scheduled_weekly_hour": "NUMERIC",
    "active_status": "VARCHAR",
    "years_of_experience": "NUMERIC",
    "clinical_level": "VARCHAR",
    "is_per_deim": "VARCHAR",
    "has_employee_profile": "BOOLEAN NOT NULL DEFAULT FALSE",
}


def verify_test_database(connection):
    database = connection.execute(
        text("SELECT current_database()")
    ).scalar_one()

    if database != "ase_etl_test":
        raise RuntimeError(
            f"REFUSING TEST WRITES TO {database}"
        )


def prepare_warehouse():
    with engine.begin() as connection:
        verify_test_database(connection)

        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS silver")
        )
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS gold")
        )

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.dim_employee (
                employee_key BIGINT GENERATED ALWAYS
                    AS IDENTITY PRIMARY KEY,
                client_employee_id VARCHAR NOT NULL UNIQUE
            )
        """))

        # The previous API tests may have created a minimal
        # Gold dimension. Add the attributes required by the
        # actual production Gold transformation.
        for column, sql_type in GOLD_COLUMNS.items():
            connection.execute(text(
                f"ALTER TABLE gold.dim_employee "
                f"ADD COLUMN IF NOT EXISTS "
                f"{column} {sql_type}"
            ))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS silver.timesheets (
                client_employee_id VARCHAR
            )
        """))


def materialize_silver(source, employee_id):
    """Build a disposable test-DB Silver employee table."""

    staged, audit = stage_employee_overlay(
        source,
        events=events_for(employee_id),
        expected_source_count=len(source),
    )

    with engine.begin() as connection:
        verify_test_database(connection)

        staged.to_sql(
            "employees",
            con=connection,
            schema="silver",
            if_exists="replace",
            index=False,
            dtype=sql_types(staged, "employees"),
        )

    return staged, audit


def gold_employee(employee_id):
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT
                employee_key,
                first_name,
                department_name,
                has_employee_profile
            FROM gold.dim_employee
            WHERE client_employee_id = :employee_id
        """), {
            "employee_id": employee_id,
        }).mappings().one_or_none()


def build_gold():
    with engine.begin() as connection:
        verify_test_database(connection)
        load_dim_employee(connection)


def test_api_employee_create_update_delete_in_gold(
    api_context,
):
    client, headers, employee_id = api_context

    prepare_warehouse()
    source = source_frame()

    try:
        # Give this employee historical timesheet identity
        # coverage, as in the real warehouse.
        with engine.begin() as connection:
            verify_test_database(connection)
            connection.execute(text("""
                INSERT INTO silver.timesheets (
                    client_employee_id
                ) VALUES (:employee_id)
            """), {"employee_id": employee_id})

        # ----------------------------------------
        # CREATE
        # ----------------------------------------
        response = client.post(
            "/api/v1/employees",
            headers=headers,
            json={
                "client_employee_id": employee_id,
                "first_name": "Integration",
                "last_name": "Test",
                "department_name": "Engineering",
            },
        )

        assert response.status_code == 201, response.text

        staged, audit = materialize_silver(
            source, employee_id
        )

        assert len(staged) == 2
        assert int(audit.iloc[0]["employee_revision"]) == 1

        build_gold()

        created = gold_employee(employee_id)

        assert created is not None
        assert created["first_name"] == "Integration"
        assert created["department_name"] == "Engineering"
        assert created["has_employee_profile"] is True

        original_key = created["employee_key"]

        # ----------------------------------------
        # UPDATE
        # ----------------------------------------
        response = client.patch(
            f"/api/v1/employees/{employee_id}",
            headers=headers,
            json={"department_name": "Data Platform"},
        )

        assert response.status_code == 200, response.text

        _, audit = materialize_silver(source, employee_id)

        assert int(audit.iloc[0]["employee_revision"]) == 2

        build_gold()

        updated = gold_employee(employee_id)

        assert updated["department_name"] == "Data Platform"
        assert updated["employee_key"] == original_key
        assert updated["has_employee_profile"] is True

        # Rebuilding the same model is idempotent.
        build_gold()

        assert (
            gold_employee(employee_id)["employee_key"]
            == original_key
        )

        # ----------------------------------------
        # SOFT DELETE
        # ----------------------------------------
        response = client.delete(
            f"/api/v1/employees/{employee_id}",
            headers=headers,
        )

        assert response.status_code == 204, response.text

        staged, audit = materialize_silver(
            source, employee_id
        )

        assert employee_id not in set(
            staged["client_employee_id"]
        )

        assert int(audit.iloc[0]["employee_revision"]) == 3
        assert audit.iloc[0]["last_operation"] == "delete"

        build_gold()

        deleted = gold_employee(employee_id)

        # Timesheet-covered identities remain in Gold,
        # but no longer have an active Silver profile.
        assert deleted is not None
        assert deleted["employee_key"] == original_key
        assert deleted["has_employee_profile"] is False
        assert deleted["first_name"] is None
        assert deleted["department_name"] is None

        with engine.connect() as connection:
            history_count = connection.execute(text("""
                SELECT COUNT(*)
                FROM silver.timesheets
                WHERE client_employee_id = :employee_id
            """), {"employee_id": employee_id}).scalar_one()

        assert history_count == 1

    finally:
        # Restrict cleanup to our disposable test identity.
        with engine.begin() as connection:
            verify_test_database(connection)
            connection.execute(text("""
                DELETE FROM silver.timesheets
                WHERE client_employee_id = :employee_id
            """), {"employee_id": employee_id})

            connection.execute(text("""
                DELETE FROM gold.dim_employee
                WHERE client_employee_id = :employee_id
            """), {"employee_id": employee_id})
