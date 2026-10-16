
"""Database setup for the ASE-ETL operational employee API."""

from sqlalchemy import text

from src.silver.publish import ENGINE


def initialize_api_database():
    """Create API employee storage and bootstrap source profiles."""

    with ENGINE.begin() as connection:

        # Keep operational data separate from the warehouse.
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS api")
        )

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS api.employees (
                    client_employee_id VARCHAR PRIMARY KEY,

                    first_name VARCHAR,
                    middle_name VARCHAR,
                    last_name VARCHAR,
                    preferred_name VARCHAR,

                    job_code VARCHAR,
                    job_title VARCHAR,

                    department_id VARCHAR,
                    department_name VARCHAR,

                    organization_id VARCHAR,
                    organization_name VARCHAR,

                    hire_date DATE,
                    term_date DATE,
                    active_status VARCHAR,

                    created_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW(),

                    is_deleted BOOLEAN NOT NULL
                        DEFAULT FALSE,

                    sync_status VARCHAR NOT NULL
                        DEFAULT 'pending'
                        CHECK (
                            sync_status IN (
                                'pending',
                                'synced',
                                'failed'
                            )
                        )
                )
            """)
        )

        # Populate existing employee profiles from Silver.
        # ON CONFLICT prevents overwriting API-managed changes.
        connection.execute(
            text("""
                INSERT INTO api.employees (
                    client_employee_id,
                    first_name,
                    middle_name,
                    last_name,
                    preferred_name,
                    job_code,
                    job_title,
                    department_id,
                    department_name,
                    organization_id,
                    organization_name,
                    hire_date,
                    term_date,
                    active_status,
                    sync_status
                )
                SELECT
                    client_employee_id,
                    first_name,
                    middle_name,
                    last_name,
                    preferred_name,
                    job_code,
                    job_title,
                    department_id,
                    department_name,
                    organization_id,
                    organization_name,
                    hire_date,
                    term_date,
                    active_status,
                    'synced'
                FROM silver.employees
                WHERE client_employee_id IS NOT NULL

                ON CONFLICT (client_employee_id)
                DO NOTHING
            """)
        )

        connection.execute(
            text("""
                CREATE INDEX IF NOT EXISTS
                    idx_api_employees_department
                ON api.employees (department_id)
            """)
        )

        count = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM api.employees
            """)
        ).scalar_one()

    print("PASS: API schema initialized")
    print("PASS: API employee table initialized")
    print(f"Employee profiles: {count:,}")


if __name__ == "__main__":
    initialize_api_database()
