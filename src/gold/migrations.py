"""Controlled PostgreSQL schema management for Gold."""

from sqlalchemy import inspect, text

from src.gold.config import GOLD_SCHEMA


EMPLOYEE_COLUMNS = {
    "employee_key": "BIGINT",
    "client_employee_id": "VARCHAR",
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
    "has_employee_profile": "BOOLEAN",
}


def ensure_gold_schema(connection):
    """Create the Gold schema if it does not exist."""
    connection.execute(
        text(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    )


def ensure_dim_employee(connection):
    """
    Create the employee dimension if missing.

    Preserve existing rows and surrogate keys.
    Reject incompatible existing schemas.
    """

    ensure_gold_schema(connection)

    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS gold.dim_employee (
                employee_key BIGINT
                    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

                client_employee_id VARCHAR NOT NULL UNIQUE,

                first_name VARCHAR,
                middle_name VARCHAR,
                last_name VARCHAR,
                preferred_name VARCHAR,

                job_code VARCHAR,
                job_title VARCHAR,
                job_start_date DATE,

                organization_id VARCHAR,
                organization_name VARCHAR,

                department_id VARCHAR,
                department_name VARCHAR,

                hire_date DATE,
                recent_hire_date DATE,
                term_date DATE,

                manager_employee_id VARCHAR,

                fte_status VARCHAR,
                scheduled_weekly_hour NUMERIC,
                active_status VARCHAR,

                years_of_experience NUMERIC,
                clinical_level VARCHAR,
                is_per_deim VARCHAR,

                has_employee_profile BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
    )

    inspector = inspect(connection)

    columns = {
        column["name"]: column
        for column in inspector.get_columns(
            "dim_employee",
            schema=GOLD_SCHEMA,
        )
    }

    if set(columns) != set(EMPLOYEE_COLUMNS):
        raise ValueError(
            "gold.dim_employee columns do not match "
            "the approved Gold schema contract"
        )

    for name, expected_type in EMPLOYEE_COLUMNS.items():
        actual_type = str(columns[name]["type"]).upper()

        if actual_type != expected_type:
            raise TypeError(
                f"gold.dim_employee.{name}: "
                f"expected {expected_type}, "
                f"found {actual_type}"
            )

    primary_key = inspector.get_pk_constraint(
        "dim_employee",
        schema=GOLD_SCHEMA,
    )["constrained_columns"]

    if primary_key != ["employee_key"]:
        raise ValueError(
            "Invalid gold.dim_employee primary key"
        )

    unique_constraints = inspector.get_unique_constraints(
        "dim_employee",
        schema=GOLD_SCHEMA,
    )

    if not any(
        constraint["column_names"] == ["client_employee_id"]
        for constraint in unique_constraints
    ):
        raise ValueError(
            "client_employee_id must be unique"
        )

    if columns["employee_key"]["nullable"]:
        raise ValueError("employee_key must be NOT NULL")

    if columns["client_employee_id"]["nullable"]:
        raise ValueError(
            "client_employee_id must be NOT NULL"
        )

    if columns["has_employee_profile"]["nullable"]:
        raise ValueError(
            "has_employee_profile must be NOT NULL"
        )

    print("PASS: gold.dim_employee schema verified")

def ensure_dim_date(connection):
    """
    Create the Gold date dimension if missing.

    Date keys use YYYYMMDD integer format.
    Existing records are preserved.
    """

    ensure_gold_schema(connection)

    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS gold.dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,

                day_of_month SMALLINT NOT NULL,
                day_of_week SMALLINT NOT NULL,
                day_name VARCHAR(10) NOT NULL,

                week_of_year SMALLINT NOT NULL,
                month_number SMALLINT NOT NULL,
                month_name VARCHAR(10) NOT NULL,

                quarter_number SMALLINT NOT NULL,
                year_number INTEGER NOT NULL,

                is_weekend BOOLEAN NOT NULL
            )
        """)
    )

    connection.execute(
        text("""
            ALTER TABLE gold.dim_date
            ADD COLUMN IF NOT EXISTS iso_year INTEGER
        """)
    )

    inspector = inspect(connection)

    expected_columns = {
        "date_key",
        "full_date",
        "day_of_month",
        "day_of_week",
        "day_name",
        "week_of_year",
        "month_number",
        "month_name",
        "quarter_number",
        "year_number",
        "iso_year",
        "is_weekend",
    }

    actual_columns = {
        column["name"]
        for column in inspector.get_columns(
            "dim_date",
            schema=GOLD_SCHEMA,
        )
    }

    if actual_columns != expected_columns:
        raise ValueError(
            "gold.dim_date does not match "
            "the approved schema"
        )

    primary_key = inspector.get_pk_constraint(
        "dim_date",
        schema=GOLD_SCHEMA,
    )["constrained_columns"]

    if primary_key != ["date_key"]:
        raise ValueError(
            "Invalid gold.dim_date primary key"
        )

    print("PASS: gold.dim_date schema verified")

def ensure_fact_timesheet(connection):
    """
    Create and verify the Gold timesheet fact table.

    Preserve existing rows and surrogate keys.
    """

    ensure_gold_schema(connection)

    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS gold.fact_timesheet (
                timesheet_key BIGINT
                    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

                employee_key BIGINT NOT NULL
                    REFERENCES gold.dim_employee(employee_key),

                punch_apply_date_key INTEGER NOT NULL
                    REFERENCES gold.dim_date(date_key),

                punch_in_date_key INTEGER NOT NULL
                    REFERENCES gold.dim_date(date_key),

                punch_out_date_key INTEGER NOT NULL
                    REFERENCES gold.dim_date(date_key),

                client_employee_id VARCHAR NOT NULL,

                punch_in_datetime TIMESTAMP NOT NULL,
                punch_out_datetime TIMESTAMP NOT NULL,

                scheduled_start_datetime TIMESTAMP,
                scheduled_end_datetime TIMESTAMP,

                hours_worked NUMERIC NOT NULL,

                department_id VARCHAR,
                department_name VARCHAR,

                home_department_id VARCHAR,
                home_department_name VARCHAR,

                pay_code VARCHAR,

                punch_in_comment VARCHAR,
                punch_out_comment VARCHAR,

                source_file VARCHAR,
                source_row_number BIGINT,
                ingested_at_utc TIMESTAMPTZ,
                ingestion_batch_id VARCHAR,

                CONSTRAINT uq_gold_timesheet_event UNIQUE (
                    client_employee_id,
                    punch_in_datetime,
                    punch_out_datetime
                )
            )
        """)
    )

    inspector = inspect(connection)

    if not inspector.has_table(
        "fact_timesheet",
        schema=GOLD_SCHEMA,
    ):
        raise RuntimeError(
            "gold.fact_timesheet was not created"
        )

    columns = {
        column["name"]
        for column in inspector.get_columns(
            "fact_timesheet",
            schema=GOLD_SCHEMA,
        )
    }

    expected = {
        "timesheet_key",
        "employee_key",
        "punch_apply_date_key",
        "punch_in_date_key",
        "punch_out_date_key",
        "client_employee_id",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
        "hours_worked",
        "department_id",
        "department_name",
        "home_department_id",
        "home_department_name",
        "pay_code",
        "punch_in_comment",
        "punch_out_comment",
        "source_file",
        "source_row_number",
        "ingested_at_utc",
        "ingestion_batch_id",
    }

    if columns != expected:
        raise ValueError(
            "gold.fact_timesheet columns do not "
            "match the approved schema"
        )

    primary_key = inspector.get_pk_constraint(
        "fact_timesheet",
        schema=GOLD_SCHEMA,
    )["constrained_columns"]

    if primary_key != ["timesheet_key"]:
        raise ValueError(
            "Invalid gold.fact_timesheet primary key"
        )

    print("PASS: gold.fact_timesheet schema verified")

def ensure_gold_registry(connection):
    """
    Create and verify the Gold publication registry.

    The caller manages the transaction.
    """

    ensure_gold_schema(connection)

    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS
                gold._publication_registry (
                    publication_id INTEGER PRIMARY KEY,

                    silver_snapshot_fingerprint CHAR(64) NOT NULL,
                    gold_model_version VARCHAR(20) NOT NULL,

                    employee_count BIGINT NOT NULL,
                    date_count BIGINT NOT NULL,
                    timesheet_count BIGINT NOT NULL,

                    gold_snapshot_fingerprint CHAR(64),
                    minio_prefix TEXT,

                    publication_status VARCHAR(20) NOT NULL
                        CHECK (
                            publication_status IN (
                                'DB_READY',
                                'PUBLISHED'
                            )
                        ),

                    database_committed_at TIMESTAMPTZ NOT NULL,
                    published_at TIMESTAMPTZ,

                    CONSTRAINT chk_gold_publication_complete
                    CHECK (
                        publication_status <> 'PUBLISHED'
                        OR (
                            gold_snapshot_fingerprint IS NOT NULL
                            AND minio_prefix IS NOT NULL
                            AND published_at IS NOT NULL
                        )
                    )
                )
        """)
    )

    inspector = inspect(connection)

    if not inspector.has_table(
        "_publication_registry",
        schema=GOLD_SCHEMA,
    ):
        raise RuntimeError(
            "Gold publication registry was not created"
        )

    print("PASS: Gold publication registry verified")