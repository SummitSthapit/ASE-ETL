
"""Verify historical Gold facts survive employee profile changes.

Only uses ase_etl_test. No MinIO publication is performed.
"""

from datetime import date, datetime

from sqlalchemy import inspect, text

from src.api.database import engine
from src.gold.transformations import (
    load_dim_employee,
    remove_stale_employees,
    remove_stale_facts,
)

from test_employee_sync_api_to_silver import (
    prerequisites,
    api_context,
    assert_test_db,
    source_frame,
)

from test_employee_sync_gold import (
    prepare_warehouse,
    materialize_silver,
)


TEST_DATE = date(2099, 12, 30)
DATE_KEY = 20991230
START = datetime(2099, 12, 30, 9)
END = datetime(2099, 12, 30, 17)


def ensure_test_fact_schema():
    assert_test_db()
    prepare_warehouse()

    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE
            )
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.fact_timesheet (
                timesheet_key BIGINT GENERATED ALWAYS
                    AS IDENTITY PRIMARY KEY,
                employee_key BIGINT
                    REFERENCES gold.dim_employee(employee_key),
                client_employee_id VARCHAR NOT NULL,
                punch_apply_date_key INTEGER NOT NULL,
                punch_in_datetime TIMESTAMP NOT NULL,
                punch_out_datetime TIMESTAMP NOT NULL,
                scheduled_start_datetime TIMESTAMP,
                scheduled_end_datetime TIMESTAMP,
                hours_worked NUMERIC NOT NULL,
                department_name VARCHAR,
                pay_code VARCHAR
            )
        """))

        # Upgrade lightweight test fixtures only.
        connection.execute(text("""
            ALTER TABLE silver.timesheets
            ADD COLUMN IF NOT EXISTS
                punch_in_datetime TIMESTAMP
        """))

        connection.execute(text("""
            ALTER TABLE silver.timesheets
            ADD COLUMN IF NOT EXISTS
                punch_out_datetime TIMESTAMP
        """))

        connection.execute(text("""
            ALTER TABLE gold.fact_timesheet
            ADD COLUMN IF NOT EXISTS employee_key BIGINT
        """))

        connection.execute(text("""
            DO $block$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid =
                        'gold.fact_timesheet'::regclass
                      AND confrelid =
                        'gold.dim_employee'::regclass
                      AND contype = 'f'
                ) THEN
                    ALTER TABLE gold.fact_timesheet
                    ADD CONSTRAINT fk_test_timesheet_employee
                    FOREIGN KEY (employee_key)
                    REFERENCES gold.dim_employee(employee_key);
                END IF;
            END
            $block$;
        """))


def insert_test_date(connection):
    d = TEST_DATE

    existing = connection.execute(
        text("""
            SELECT 1
            FROM gold.dim_date
            WHERE date_key = :key
        """),
        {"key": DATE_KEY},
    ).scalar_one_or_none()

    if existing is not None:
        raise RuntimeError(
            "Disposable Gold date fixture already exists"
        )

    values = {
        "date_key": DATE_KEY,
        "full_date": d,
        "day_of_month": d.day,
        "day_of_week": d.isoweekday(),
        "day_name": d.strftime("%A"),
        "week_of_year": d.isocalendar().week,
        "month_number": d.month,
        "month_name": d.strftime("%B"),
        "quarter_number": (d.month - 1) // 3 + 1,
        "year_number": d.year,
        "iso_year": d.isocalendar().year,
        "is_weekend": d.isoweekday() >= 6,
    }

    present = {
        column["name"]
        for column in inspect(connection).get_columns(
            "dim_date", schema="gold"
        )
    }

    used = {
        key: value
        for key, value in values.items()
        if key in present
    }

    columns = ", ".join(used)
    parameters = ", ".join(f":{key}" for key in used)

    connection.execute(
        text(
            f"INSERT INTO gold.dim_date ({columns}) "
            f"VALUES ({parameters}) "
            "ON CONFLICT (date_key) DO NOTHING"
        ),
        used,
    )


def insert_test_fact(connection, employee_id, employee_key):
    values = {
        "employee_key": employee_key,
        "client_employee_id": employee_id,
        "punch_apply_date_key": DATE_KEY,
        "punch_in_date_key": DATE_KEY,
        "punch_out_date_key": DATE_KEY,
        "punch_in_datetime": START,
        "punch_out_datetime": END,
        "scheduled_start_datetime": START,
        "scheduled_end_datetime": END,
        "hours_worked": 8,
        "department_name": "Engineering",
        "pay_code": "REG",
    }

    present = {
        column["name"]
        for column in inspect(connection).get_columns(
            "fact_timesheet", schema="gold"
        )
    }

    used = {
        key: value
        for key, value in values.items()
        if key in present
    }

    columns = ", ".join(used)
    parameters = ", ".join(f":{key}" for key in used)

    return connection.execute(
        text(
            f"INSERT INTO gold.fact_timesheet ({columns}) "
            f"VALUES ({parameters}) "
            "RETURNING timesheet_key"
        ),
        used,
    ).scalar_one()


def read_gold(connection, employee_id):
    return connection.execute(
        text("""
            SELECT
                employee_key,
                first_name,
                department_name,
                has_employee_profile
            FROM gold.dim_employee
            WHERE client_employee_id = :employee_id
        """),
        {"employee_id": employee_id},
    ).mappings().one()


def test_employee_update_delete_preserves_fact_and_timesheet_api(
    api_context,
):
    client, headers, employee_id = api_context

    ensure_test_fact_schema()
    source = source_frame()

    try:
        # CREATE
        response = client.post(
            "/api/v1/employees",
            headers=headers,
            json={
                "client_employee_id": employee_id,
                "first_name": "History",
                "last_name": "Tester",
                "department_name": "Engineering",
            },
        )

        assert response.status_code == 201, response.text

        materialize_silver(source, employee_id)

        with engine.begin() as connection:
            load_dim_employee(connection)

            created = read_gold(connection, employee_id)

            employee_key = created["employee_key"]
            assert created["has_employee_profile"] is True

            insert_test_date(connection)

            connection.execute(
                text("""
                    INSERT INTO silver.timesheets (
                        client_employee_id,
                        punch_in_datetime,
                        punch_out_datetime
                    )
                    VALUES (:employee_id, :start, :end)
                """),
                {
                    "employee_id": employee_id,
                    "start": START,
                    "end": END,
                },
            )

            fact_key = insert_test_fact(
                connection,
                employee_id,
                employee_key,
            )

        # UPDATE
        response = client.patch(
            f"/api/v1/employees/{employee_id}",
            headers=headers,
            json={"department_name": "Analytics"},
        )

        assert response.status_code == 200, response.text

        materialize_silver(source, employee_id)

        with engine.begin() as connection:
            load_dim_employee(connection)

            updated = read_gold(connection, employee_id)

            assert updated["employee_key"] == employee_key
            assert updated["department_name"] == "Analytics"

        # SOFT DELETE
        response = client.delete(
            f"/api/v1/employees/{employee_id}",
            headers=headers,
        )

        assert response.status_code == 204, response.text

        staged, audit = materialize_silver(
            source,
            employee_id,
        )

        assert employee_id not in set(
            staged.client_employee_id
        )
        assert audit.iloc[0]["last_operation"] == "delete"

        with engine.begin() as connection:
            # Production cleanup functions affect the entire
            # test warehouse. Refuse to touch unrelated fixtures.
            other_facts = connection.execute(
                text("""
                    SELECT COUNT(*)
                    FROM gold.fact_timesheet
                    WHERE client_employee_id <> :employee_id
                """),
                {"employee_id": employee_id},
            ).scalar_one()

            assert other_facts == 0, (
                "Unexpected unrelated Gold facts in test DB"
            )

            other_stale = connection.execute(
                text("""
                    SELECT COUNT(*)
                    FROM gold.dim_employee e
                    WHERE e.client_employee_id <> :employee_id
                      AND NOT EXISTS (
                          SELECT 1
                          FROM silver.employees s
                          WHERE s.client_employee_id =
                              e.client_employee_id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM silver.timesheets t
                          WHERE t.client_employee_id =
                              e.client_employee_id
                      )
                """),
                {"employee_id": employee_id},
            ).scalar_one()

            assert other_stale == 0, (
                "Unrelated stale Gold employees in test DB"
            )

            load_dim_employee(connection)
            remove_stale_facts(connection)
            remove_stale_employees(connection)

            deleted = read_gold(
                connection,
                employee_id,
            )

            fact = connection.execute(
                text("""
                    SELECT
                        timesheet_key,
                        employee_key,
                        hours_worked
                    FROM gold.fact_timesheet
                    WHERE client_employee_id = :employee_id
                """),
                {"employee_id": employee_id},
            ).mappings().one()

            assert deleted["employee_key"] == employee_key
            assert deleted["has_employee_profile"] is False
            assert deleted["first_name"] is None

            assert fact["timesheet_key"] == fact_key
            assert fact["employee_key"] == employee_key
            assert float(fact["hours_worked"]) == 8.0

        # Verify the historical fact is still accessible
        # through the authenticated read-only API.
        response = client.get(
            f"/api/v1/employees/{employee_id}/timesheets",
            headers=headers,
        )

        assert response.status_code == 200, response.text
        assert response.json()["total"] == 1

        item = response.json()["items"][0]

        assert item["client_employee_id"] == employee_id

    finally:
        assert_test_db()

        with engine.begin() as connection:
            connection.execute(
                text("""
                    DELETE FROM gold.fact_timesheet
                    WHERE client_employee_id = :employee_id
                """),
                {"employee_id": employee_id},
            )

            connection.execute(
                text("""
                    DELETE FROM silver.timesheets
                    WHERE client_employee_id = :employee_id
                """),
                {"employee_id": employee_id},
            )

            connection.execute(
                text("""
                    DELETE FROM gold.dim_employee
                    WHERE client_employee_id = :employee_id
                """),
                {"employee_id": employee_id},
            )

            connection.execute(
                text("""
                    DELETE FROM gold.dim_date
                    WHERE date_key = :date_key
                """),
                {"date_key": DATE_KEY},
            )
