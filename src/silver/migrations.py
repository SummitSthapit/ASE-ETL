
"""Controlled PostgreSQL schema migrations for Silver."""

from sqlalchemy import inspect, text


# Approved Silver v2 employee columns.
# These names and SQL types are explicitly controlled.
EMPLOYEE_V2_COLUMNS = {
    "middle_name": "VARCHAR",
    "years_of_experience": "NUMERIC(12, 2)",
    "zip": "VARCHAR",
    "manager_employee_name": "VARCHAR",
    "is_per_deim": "VARCHAR",
    "work_phone": "VARCHAR",
    "clinical_level": "VARCHAR",
    "address": "VARCHAR",
    "city": "VARCHAR",
    "state": "VARCHAR",
    "country": "VARCHAR",
    "cell_phone": "VARCHAR",
}


def migrate_employee_schema(connection):
    """
    Add missing approved Silver v2 employee columns.

    The caller owns the database transaction.

    Existing columns are checked for type compatibility.
    Existing data and primary keys are preserved.
    """

    inspector = inspect(connection)

    if not inspector.has_table(
        "employees", schema="silver"
    ):
        raise RuntimeError(
            "silver.employees does not exist"
        )

    existing = {
        column["name"]: column
        for column in inspector.get_columns(
            "employees", schema="silver"
        )
    }

    added = []

    for column_name, sql_type in EMPLOYEE_V2_COLUMNS.items():

        if column_name in existing:
            actual_type = str(
                existing[column_name]["type"]
            ).upper()

            compatible = (
                actual_type in {"VARCHAR", "TEXT"}
                if sql_type == "VARCHAR"
                else actual_type == sql_type
            )

            if not compatible:
                raise TypeError(
                    f"Incompatible type for {column_name}: "
                    f"expected {sql_type}, "
                    f"found {actual_type}"
                )

            if not existing[column_name]["nullable"]:
                raise ValueError(
                    f"{column_name} must be nullable"
                )

            continue

        # Column names and types come exclusively
        # from the approved constant above.
        connection.execute(
            text(
                f'ALTER TABLE silver.employees '
                f'ADD COLUMN "{column_name}" {sql_type}'
            )
        )

        added.append(column_name)

    # Verify the resulting schema in this transaction.
    result = connection.execute(
        text("""
            SELECT
                column_name,
                data_type,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name = 'employees'
        """)
    ).mappings().all()

    columns = {
        row["column_name"]: row
        for row in result
    }

    for column_name, sql_type in EMPLOYEE_V2_COLUMNS.items():
        assert column_name in columns

        column = columns[column_name]

        assert column["is_nullable"] == "YES"

        if sql_type == "NUMERIC(12, 2)":
            assert column["data_type"] == "numeric"
            assert column["numeric_precision"] == 12
            assert column["numeric_scale"] == 2
        else:
            assert column["data_type"] in {
                "character varying",
                "text",
            }

    return added
