
"""Read-only performance and data-quality review."""

from sqlalchemy import text
from src.silver.publish import ENGINE


QUERIES = {
    "monthly_workload": """
        SELECT * FROM mart.kpi_monthly_workload
    """,
    "department_workload": """
        SELECT * FROM mart.kpi_department_workload
    """,
    "attendance_compliance": """
        SELECT * FROM mart.kpi_attendance_compliance
    """,
    "department_overtime": """
        SELECT * FROM mart.kpi_department_overtime
    """,
    "workforce_monthly": """
        SELECT * FROM mart.workforce_monthly
    """,
    "employee_working_hours": """
        SELECT * FROM mart.kpi_employee_working_hours
    """,
}


def main():
    with ENGINE.connect() as connection:
        connection.execute(text(
            "SET TRANSACTION READ ONLY"
        ))

        print("ANALYTICS QUERY PERFORMANCE")
        print("=" * 65)

        for name, query in QUERIES.items():
            plan = connection.execute(
                text(
                    "EXPLAIN (ANALYZE, BUFFERS, "
                    "TIMING OFF) " + query
                )
            ).scalars().all()

            execution_time = next(
                (
                    line.strip()
                    for line in plan
                    if "Execution Time:" in line
                ),
                "Execution time unavailable",
            )

            print(f"{name}: {execution_time}")

        print("\nSOURCE AND CLASSIFICATION COVERAGE")
        print("=" * 65)

        coverage = connection.execute(text("""
            SELECT
                COUNT(*) AS total_records,
                COUNT(*) FILTER (
                    WHERE work_status = 'eligible'
                ) AS eligible_records,
                COUNT(*) FILTER (
                    WHERE work_status = 'excluded'
                ) AS excluded_records,
                COUNT(*) FILTER (
                    WHERE work_status = 'unresolved'
                ) AS unresolved_records,
                COUNT(*) FILTER (
                    WHERE eligible_for_attendance
                ) AS attendance_eligible_records
            FROM mart.v_timesheet_classification
        """)).mappings().one()

        for name, value in coverage.items():
            print(f"{name}: {value}")

        dates = connection.execute(text("""
            SELECT
                MIN(full_date) AS first_date,
                MAX(full_date) AS last_date
            FROM gold.dim_date
        """)).mappings().one()

        print("\nREPORTING DATE COVERAGE")
        print(dict(dates))

        # End the read-only transaction.
        connection.rollback()


if __name__ == "__main__":
    main()
