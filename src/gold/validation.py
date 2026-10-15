"""Automated validation checks for the Gold layer."""

from sqlalchemy import inspect
from sqlalchemy import text


EXPECTED_PRIMARY_KEYS = {
    "dim_employee": ["employee_key"],
    "dim_date": ["date_key"],
    "fact_timesheet": ["timesheet_key"],
}

EXPECTED_UNIQUE_CONSTRAINTS = {
    "dim_employee": [
        ["client_employee_id"],
    ],
    "dim_date": [
        ["full_date"],
    ],
    "fact_timesheet": [
        [
            "client_employee_id",
            "punch_in_datetime",
            "punch_out_datetime",
        ],
    ],
}

EXPECTED_FOREIGN_KEYS = {
    "fact_timesheet": {
        "employee_key": ("dim_employee", "employee_key"),
        "punch_apply_date_key": ("dim_date", "date_key"),
        "punch_in_date_key": ("dim_date", "date_key"),
        "punch_out_date_key": ("dim_date", "date_key"),
    },
}


def validate_gold_constraints(connection):
    """
    Validate Gold primary keys, unique constraints,
    and foreign-key relationships.

    Raises an exception if the database structure
    differs from the approved Gold model.
    """

    inspector = inspect(connection)

    for table, expected_pk in EXPECTED_PRIMARY_KEYS.items():
        actual_pk = inspector.get_pk_constraint(
            table, schema="gold"
        )["constrained_columns"]

        if actual_pk != expected_pk:
            raise ValueError(
                f"{table}: invalid primary key. "
                f"Expected {expected_pk}, got {actual_pk}"
            )

        actual_unique = [
            constraint["column_names"]
            for constraint in inspector.get_unique_constraints(
                table, schema="gold"
            )
        ]

        for expected_columns in EXPECTED_UNIQUE_CONSTRAINTS[table]:
            if expected_columns not in actual_unique:
                raise ValueError(
                    f"{table}: missing unique constraint "
                    f"on {expected_columns}"
                )

    for table, expected_relationships in (
        EXPECTED_FOREIGN_KEYS.items()
    ):
        foreign_keys = inspector.get_foreign_keys(
            table, schema="gold"
        )

        actual_relationships = {
            tuple(fk["constrained_columns"]): (
                fk["referred_schema"],
                fk["referred_table"],
                tuple(fk["referred_columns"]),
            )
            for fk in foreign_keys
        }

        for column, (target_table, target_column) in (
            expected_relationships.items()
        ):
            actual = actual_relationships.get((column,))

            expected = (
                "gold",
                target_table,
                (target_column,),
            )

            if actual != expected:
                raise ValueError(
                    f"{table}.{column}: expected FK "
                    f"{expected}, got {actual}"
                )

    print("PASS: Gold primary keys verified")
    print("PASS: Gold unique constraints verified")
    print("PASS: Gold foreign-key relationships verified")

def validate_gold_data(connection):
    """
    Reconcile Gold dimensions and facts with Silver.

    Validate current source coverage, fact grain,
    dimension relationships, and business measures.
    """

    result = connection.execute(
        text("""
            WITH source_employee_ids AS (
                SELECT client_employee_id
                FROM silver.employees

                UNION

                SELECT client_employee_id
                FROM silver.timesheets
            ),

            missing_employees AS (
                SELECT client_employee_id
                FROM source_employee_ids

                EXCEPT

                SELECT client_employee_id
                FROM gold.dim_employee
            ),

            missing_facts AS (
                SELECT
                    client_employee_id,
                    punch_in_datetime,
                    punch_out_datetime
                FROM silver.timesheets

                EXCEPT

                SELECT
                    client_employee_id,
                    punch_in_datetime,
                    punch_out_datetime
                FROM gold.fact_timesheet
            ),

            extra_facts AS (
                SELECT
                    client_employee_id,
                    punch_in_datetime,
                    punch_out_datetime
                FROM gold.fact_timesheet

                EXCEPT

                SELECT
                    client_employee_id,
                    punch_in_datetime,
                    punch_out_datetime
                FROM silver.timesheets
            )

            SELECT
                (SELECT COUNT(*) FROM source_employee_ids)
                    AS expected_employees,

                (SELECT COUNT(*) FROM gold.dim_employee)
                    AS actual_employees,

                (SELECT COUNT(*) FROM silver.timesheets)
                    AS expected_facts,

                (SELECT COUNT(*) FROM gold.fact_timesheet)
                    AS actual_facts,

                (SELECT COUNT(*) FROM missing_employees)
                    AS missing_employees,

                (SELECT COUNT(*) FROM missing_facts)
                    AS missing_facts,

                (SELECT COUNT(*) FROM extra_facts)
                    AS extra_facts
        """)
    ).mappings().one()

    if result["actual_employees"] != result["expected_employees"]:
        raise ValueError(
            "Gold employee count does not match "
            "the current Silver identity population"
        )

    if result["actual_facts"] != result["expected_facts"]:
        raise ValueError(
            "Gold timesheet count does not match Silver"
        )

    for check in (
        "missing_employees",
        "missing_facts",
        "extra_facts",
    ):
        if result[check] != 0:
            raise ValueError(
                f"Gold reconciliation failed: "
                f"{check}={result[check]}"
            )

    mismatches = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM silver.timesheets s

            JOIN gold.fact_timesheet f
              ON s.client_employee_id =
                 f.client_employee_id
             AND s.punch_in_datetime =
                 f.punch_in_datetime
             AND s.punch_out_datetime =
                 f.punch_out_datetime

            JOIN gold.dim_employee e
              ON f.employee_key = e.employee_key

            JOIN gold.dim_date da
              ON f.punch_apply_date_key = da.date_key

            JOIN gold.dim_date di
              ON f.punch_in_date_key = di.date_key

            JOIN gold.dim_date do_date
              ON f.punch_out_date_key = do_date.date_key

            WHERE
                e.client_employee_id IS DISTINCT FROM
                    s.client_employee_id

                OR da.full_date IS DISTINCT FROM
                    s.punch_apply_date

                OR di.full_date IS DISTINCT FROM
                    s.punch_in_datetime::date

                OR do_date.full_date IS DISTINCT FROM
                    s.punch_out_datetime::date

                OR f.hours_worked IS DISTINCT FROM
                    s.hours_worked

                OR f.pay_code IS DISTINCT FROM
                    s.pay_code

                OR f.department_id IS DISTINCT FROM
                    s.department_id

                OR f.department_name IS DISTINCT FROM
                    s.department_name

                OR f.home_department_id IS DISTINCT FROM
                    s.home_department_id

                OR f.home_department_name IS DISTINCT FROM
                    s.home_department_name

                OR f.scheduled_start_datetime
                   IS DISTINCT FROM
                    s.scheduled_start_datetime

                OR f.scheduled_end_datetime
                   IS DISTINCT FROM
                    s.scheduled_end_datetime

                OR f.punch_in_comment IS DISTINCT FROM
                    s.punch_in_comment

                OR f.punch_out_comment IS DISTINCT FROM
                    s.punch_out_comment

                OR f.source_file IS DISTINCT FROM
                    s.source_file

                OR f.source_row_number IS DISTINCT FROM
                    s.source_row_number

                OR f.ingested_at_utc IS DISTINCT FROM
                    s.ingested_at_utc

                OR f.ingestion_batch_id IS DISTINCT FROM
                    s.ingestion_batch_id
        """)
    ).scalar_one()

    if mismatches != 0:
        raise ValueError(
            f"Gold fact business-value mismatches: "
            f"{mismatches}"
        )

    print(
        "PASS: Gold employee population reconciled "
        f"({result['actual_employees']:,})"
    )

    print(
        "PASS: Gold fact population reconciled "
        f"({result['actual_facts']:,})"
    )

    print("PASS: Gold fact business values reconciled")

def validate_employee_profiles(connection):
    """
    Verify employee profile availability and attribute
    consistency between Silver and Gold.
    """

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) FILTER (
                    WHERE e.client_employee_id IS NOT NULL
                      AND NOT g.has_employee_profile
                ) AS missing_profile_flags,

                COUNT(*) FILTER (
                    WHERE e.client_employee_id IS NULL
                      AND g.has_employee_profile
                ) AS incorrect_profile_flags,

                COUNT(*) FILTER (
                    WHERE e.client_employee_id IS NOT NULL
                      AND (
                          g.first_name IS DISTINCT FROM e.first_name
                          OR g.middle_name IS DISTINCT FROM e.middle_name
                          OR g.last_name IS DISTINCT FROM e.last_name
                          OR g.preferred_name IS DISTINCT FROM e.preferred_name
                          OR g.job_code IS DISTINCT FROM e.job_code
                          OR g.job_title IS DISTINCT FROM e.job_title
                          OR g.job_start_date IS DISTINCT FROM e.job_start_date
                          OR g.organization_id IS DISTINCT FROM e.organization_id
                          OR g.organization_name IS DISTINCT FROM e.organization_name
                          OR g.department_id IS DISTINCT FROM e.department_id
                          OR g.department_name IS DISTINCT FROM e.department_name
                          OR g.hire_date IS DISTINCT FROM e.hire_date
                          OR g.recent_hire_date IS DISTINCT FROM e.recent_hire_date
                          OR g.term_date IS DISTINCT FROM e.term_date
                          OR g.manager_employee_id IS DISTINCT FROM e.manager_employee_id
                          OR g.fte_status IS DISTINCT FROM e.fte_status
                          OR g.scheduled_weekly_hour IS DISTINCT FROM e.scheduled_weekly_hour
                          OR g.active_status IS DISTINCT FROM e.active_status
                          OR g.years_of_experience IS DISTINCT FROM e.years_of_experience
                          OR g.clinical_level IS DISTINCT FROM e.clinical_level
                          OR g.is_per_deim IS DISTINCT FROM e.is_per_deim
                      )
                ) AS profile_attribute_mismatches,

                COUNT(*) FILTER (
                    WHERE e.client_employee_id IS NULL
                      AND (
                          g.first_name IS NOT NULL
                          OR g.last_name IS NOT NULL
                          OR g.job_title IS NOT NULL
                          OR g.department_id IS NOT NULL
                      )
                ) AS unexpected_unprofiled_attributes

            FROM gold.dim_employee g

            LEFT JOIN silver.employees e
              ON g.client_employee_id = e.client_employee_id
        """)
    ).mappings().one()

    failures = {
        name: value
        for name, value in result.items()
        if value != 0
    }

    if failures:
        raise ValueError(
            f"Gold employee profile reconciliation failed: {failures}"
        )

    print("PASS: Gold employee profile flags verified")
    print("PASS: Gold employee profile attributes reconciled")

def validate_dim_date(connection):
    """
    Validate Gold calendar attributes and source-date coverage.

    Checks:
    - Correct YYYYMMDD keys
    - Correct calendar and ISO attributes
    - Continuous date coverage
    - Every Silver timesheet date is represented
    """

    result = connection.execute(
        text("""
            WITH calendar_checks AS (
                SELECT
                    COUNT(*) AS total_dates,

                    COUNT(*) FILTER (
                        WHERE date_key <>
                            TO_CHAR(full_date, 'YYYYMMDD')::integer
                    ) AS incorrect_date_keys,

                    COUNT(*) FILTER (
                        WHERE day_of_month <>
                            EXTRACT(DAY FROM full_date)
                        OR day_of_week <>
                            EXTRACT(ISODOW FROM full_date)
                        OR week_of_year <>
                            EXTRACT(WEEK FROM full_date)
                        OR month_number <>
                            EXTRACT(MONTH FROM full_date)
                        OR quarter_number <>
                            EXTRACT(QUARTER FROM full_date)
                        OR year_number <>
                            EXTRACT(YEAR FROM full_date)
                        OR iso_year <>
                            EXTRACT(ISOYEAR FROM full_date)
                        OR is_weekend IS DISTINCT FROM
                            (EXTRACT(ISODOW FROM full_date)
                             IN (6, 7))
                        OR day_name IS DISTINCT FROM
                            TRIM(TO_CHAR(full_date, 'Day'))
                        OR month_name IS DISTINCT FROM
                            TRIM(TO_CHAR(full_date, 'Month'))
                    ) AS incorrect_attributes,

                    MIN(full_date) AS min_date,
                    MAX(full_date) AS max_date
                FROM gold.dim_date
            ),

            silver_dates AS (
                SELECT punch_apply_date AS dt
                FROM silver.timesheets
                WHERE punch_apply_date IS NOT NULL

                UNION

                SELECT punch_in_datetime::date
                FROM silver.timesheets

                UNION

                SELECT punch_out_datetime::date
                FROM silver.timesheets
            ),

            missing_dates AS (
                SELECT dt
                FROM silver_dates

                EXCEPT

                SELECT full_date
                FROM gold.dim_date
            )

            SELECT
                c.*,

                (c.max_date - c.min_date + 1)
                    AS expected_calendar_days,

                (SELECT COUNT(*) FROM missing_dates)
                    AS missing_source_dates

            FROM calendar_checks c
        """)
    ).mappings().one()

    if result["total_dates"] == 0:
        raise ValueError("Gold date dimension is empty")

    if result["incorrect_date_keys"] != 0:
        raise ValueError(
            "Gold date dimension contains incorrect date keys"
        )

    if result["incorrect_attributes"] != 0:
        raise ValueError(
            "Gold date dimension contains incorrect calendar attributes"
        )

    if (
        result["total_dates"]
        != result["expected_calendar_days"]
    ):
        raise ValueError(
            "Gold date dimension contains gaps"
        )

    if result["missing_source_dates"] != 0:
        raise ValueError(
            "Gold date dimension is missing Silver dates"
        )

    print(
        "PASS: Gold date attributes verified "
        f"({result['total_dates']:,} dates)"
    )

    print("PASS: Gold calendar continuity verified")
    print("PASS: Silver-to-Gold date coverage verified")