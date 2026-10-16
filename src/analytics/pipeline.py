
"""ASE-ETL Phase 6: Analytics pipeline."""

from multiprocessing.dummy import connection
from pathlib import Path

from sqlalchemy import text

from src.silver.publish import ENGINE


SQL_ROOT = Path(__file__).resolve().parent / "sql"

ANALYTICS_LOCK_ID = 7182403


def verify_gold_publication(connection):
    """Verify the active published Gold snapshot."""

    publication = connection.execute(
        text("""
            SELECT
                gold_snapshot_fingerprint,
                gold_model_version,
                employee_count,
                date_count,
                timesheet_count,
                publication_status
            FROM gold._publication_registry
            WHERE publication_id = 1
        """)
    ).mappings().one_or_none()

    if publication is None:
        raise RuntimeError(
            "No active Gold publication found."
        )

    if publication["publication_status"] != "PUBLISHED":
        raise RuntimeError(
            "Gold publication is not complete."
        )

    if not publication["gold_snapshot_fingerprint"]:
        raise RuntimeError(
            "Gold publication fingerprint is missing."
        )

    expected_counts = {
        "dim_employee": publication["employee_count"],
        "dim_date": publication["date_count"],
        "fact_timesheet": publication["timesheet_count"],
    }

    for table_name, expected in expected_counts.items():
        actual = connection.execute(
            text(
                f"SELECT COUNT(*) "
                f"FROM gold.{table_name}"
            )
        ).scalar_one()

        if actual != expected:
            raise ValueError(
                f"Gold {table_name} count mismatch: "
                f"expected {expected}, found {actual}"
            )

    print("PASS: Active Gold publication verified")
    print(
        "Gold fingerprint:",
        publication["gold_snapshot_fingerprint"],
    )

    return publication


def execute_sql_file(connection, filename):
    """Execute a trusted, version-controlled SQL file."""

    path = SQL_ROOT / filename

    if not path.is_file():
        raise FileNotFoundError(
            f"Missing SQL file: {path}"
        )

    sql = path.read_text(encoding="utf-8-sig")

    # The psycopg driver accepts this trusted
    # multi-statement migration script.
    connection.exec_driver_sql(sql)

    print(f"PASS: Executed {filename}")


def verify_classification_inventory(connection):
    """Verify that source pay codes are registered."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(DISTINCT pay_code)
            FROM gold.fact_timesheet
            WHERE pay_code IS NOT NULL
        """)
    ).scalar_one()

    registered = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.pay_code_classification
        """)
    ).scalar_one()

    missing = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT f.pay_code
                FROM gold.fact_timesheet f
                WHERE f.pay_code IS NOT NULL

                EXCEPT

                SELECT c.pay_code
                FROM mart.pay_code_classification c
            ) AS missing_codes
        """)
    ).scalar_one()

    if missing != 0:
        raise ValueError(
            f"{missing} pay codes are not classified "
            "or registered."
        )

    print(
        f"PASS: {result} source pay codes registered"
    )
    print(
        f"Classification inventory: "
        f"{registered} codes"
    )


def verify_component_inventory(connection):
    """Verify pay-code components were registered."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) AS total_components,

                COUNT(*) FILTER (
                    WHERE pay_category = 'unclassified'
                ) AS unclassified_components

            FROM mart.pay_code_component
        """)
    ).mappings().one()

    if result["total_components"] == 0:
        raise ValueError(
            "No pay-code components registered."
        )

    missing = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT TRIM(component) AS code
                FROM mart.pay_code_classification c
                CROSS JOIN LATERAL
                    unnest(
                        string_to_array(c.pay_code, '|')
                    ) AS component
                WHERE TRIM(component) <> ''

                EXCEPT

                SELECT component_code
                FROM mart.pay_code_component
            ) missing_components
        """)
    ).scalar_one()

    if missing != 0:
        raise ValueError(
            f"{missing} pay-code components missing."
        )

    print(
        "PASS: Pay-code components registered:",
        result["total_components"],
    )

    print(
        "Unclassified components:",
        result["unclassified_components"],
    )


def verify_classification_rules(connection):
    """Summarize classified and unclassified components."""

    rows = connection.execute(
        text("""
            SELECT
                pay_category,
                COUNT(*) AS component_count
            FROM mart.pay_code_component
            GROUP BY pay_category
            ORDER BY pay_category
        """)
    ).mappings().all()

    total = sum(row["component_count"] for row in rows)

    if total == 0:
        raise ValueError(
            "No component classifications found."
        )

    print("\nPAY-CODE CLASSIFICATION SUMMARY")

    for row in rows:
        print(
            f"{row['pay_category']}: "
            f"{row['component_count']}"
        )


def verify_timesheet_classification(connection):
    """Verify one classified row per Gold timesheet."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) AS total_records,

                COUNT(DISTINCT timesheet_key)
                    AS distinct_records,

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
        """)
    ).mappings().one()

    gold_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_timesheet
        """)
    ).scalar_one()

    if result["total_records"] != gold_count:
        raise ValueError(
            "Timesheet classification row count mismatch."
        )

    if result["distinct_records"] != gold_count:
        raise ValueError(
            "Duplicate timesheet classification detected."
        )

    print("\nTIMESHEET CLASSIFICATION SUMMARY")

    for key, value in result.items():
        print(f"{key}: {value}")

    print(
        "PASS: Timesheet classification integrity verified"
    )


def verify_attendance_mart(connection):
    """Validate attendance eligibility and reporting counts."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) AS total_events,

                COUNT(*) FILTER (
                    WHERE eligible_for_attendance
                ) AS eligible_events,

                COUNT(*) FILTER (
                    WHERE is_late_arrival
                ) AS late_arrivals,

                COUNT(*) FILTER (
                    WHERE is_early_departure
                ) AS early_departures,

                COUNT(*) FILTER (
                    WHERE NOT eligible_for_attendance
                      AND (
                          is_late_arrival
                          OR is_early_departure
                      )
                ) AS invalid_flags

            FROM mart.v_attendance_events
        """)
    ).mappings().one()

    gold_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_timesheet
        """)
    ).scalar_one()

    if result["total_events"] != gold_count:
        raise ValueError(
            "Attendance event count mismatch."
        )

    if result["invalid_flags"] != 0:
        raise ValueError(
            "Ineligible events contain attendance flags."
        )

    daily = connection.execute(
        text("""
            SELECT
                COALESCE(
                    SUM(total_timesheet_records), 0
                ) AS total_records,

                COALESCE(
                    SUM(eligible_shift_records), 0
                ) AS eligible_records,

                COALESCE(
                    SUM(late_arrival_count), 0
                ) AS late_arrivals,

                COALESCE(
                    SUM(early_departure_count), 0
                ) AS early_departures

            FROM mart.employee_attendance_daily
        """)
    ).mappings().one()

    checks = {
        "total_records": "total_events",
        "eligible_records": "eligible_events",
        "late_arrivals": "late_arrivals",
        "early_departures": "early_departures",
    }

    for daily_key, event_key in checks.items():
        if daily[daily_key] != result[event_key]:
            raise ValueError(
                f"Attendance reconciliation failed: "
                f"{daily_key}"
            )

    print("\nATTENDANCE MART SUMMARY")

    for key, value in result.items():
        print(f"{key}: {value}")

    print("PASS: Attendance mart integrity verified")

def verify_working_hours_mart(connection):
    """Reconcile daily and weekly working-hours views."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) AS daily_rows,
                COALESCE(SUM(timesheet_records), 0)
                    AS timesheet_records,
                COALESCE(SUM(recorded_hours), 0)
                    AS recorded_hours,
                COALESCE(SUM(worked_hours), 0)
                    AS worked_hours,
                COALESCE(SUM(unresolved_records), 0)
                    AS unresolved_records
            FROM mart.employee_work_hours_daily
        """)
    ).mappings().one()

    weekly = connection.execute(
        text("""
            SELECT
                COALESCE(SUM(timesheet_records), 0)
                    AS timesheet_records,
                COALESCE(SUM(recorded_hours), 0)
                    AS recorded_hours,
                COALESCE(SUM(worked_hours), 0)
                    AS worked_hours
            FROM mart.employee_work_hours_weekly
        """)
    ).mappings().one()

    gold = connection.execute(
        text("""
            SELECT
                COUNT(*) AS total_records,
                ROUND(SUM(hours_worked), 2)
                    AS recorded_hours
            FROM gold.fact_timesheet
        """)
    ).mappings().one()

    if result["timesheet_records"] != gold["total_records"]:
        raise ValueError(
            "Daily timesheet count does not match Gold."
        )

    if result["recorded_hours"] != gold["recorded_hours"]:
        raise ValueError(
            "Recorded hours do not reconcile with Gold."
        )

    for metric in (
        "timesheet_records",
        "recorded_hours",
        "worked_hours",
    ):
        if result[metric] != weekly[metric]:
            raise ValueError(
                f"Weekly reconciliation failed: {metric}"
            )

    rolling_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.employee_work_hours_rolling
        """)
    ).scalar_one()

    if rolling_count != result["daily_rows"]:
        raise ValueError(
            "Rolling view row count mismatch."
        )

    print("\nWORKING HOURS MART SUMMARY")

    for key, value in result.items():
        print(f"{key}: {value}")

    print(f"rolling_rows: {rolling_count}")
    print("PASS: Working hours mart integrity verified")


def verify_working_hours_kpis(connection):
    """Verify working-hours KPI reporting views."""

    result = connection.execute(
        text("""
            SELECT
                COUNT(*) AS employee_rows,

                COUNT(*) FILTER (
                    WHERE eligible_workdays > 0
                ) AS employees_with_worked_hours,

                COUNT(*) FILTER (
                    WHERE eligible_workdays = 0
                ) AS employees_without_eligible_work,

                COUNT(*) FILTER (
                    WHERE eligible_workdays = 0
                      AND avg_daily_worked_hours IS NOT NULL
                ) AS invalid_daily_averages,

                COALESCE(
                    SUM(total_worked_hours), 0
                ) AS total_worked_hours

            FROM mart.kpi_employee_working_hours
        """)
    ).mappings().one()

    daily_total = connection.execute(
        text("""
            SELECT
                COALESCE(SUM(worked_hours), 0)
            FROM mart.employee_work_hours_daily
        """)
    ).scalar_one()

    if result["total_worked_hours"] != daily_total:
        raise ValueError(
            "KPI worked-hours total mismatch."
        )

    if result["invalid_daily_averages"] != 0:
        raise ValueError(
            "Invalid average for employee "
            "without eligible workdays."
        )

    rolling_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.kpi_rolling_working_hours
        """)
    ).scalar_one()

    daily_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.employee_work_hours_daily
        """)
    ).scalar_one()

    if rolling_count != daily_count:
        raise ValueError(
            "Rolling KPI row count mismatch."
        )

    print("\nWORKING HOURS KPI SUMMARY")

    for key, value in result.items():
        print(f"{key}: {value}")

    print(f"rolling_kpi_rows: {rolling_count}")

    print("PASS: Working hours KPI verification")


def verify_overtime_mart(connection):
    """Validate overtime events and daily aggregation."""

    events = connection.execute(
        text("""
            SELECT
                COUNT(*) AS total_events,

                COUNT(*) FILTER (
                    WHERE eligible_for_attendance
                ) AS eligible_events,

                COUNT(*) FILTER (
                    WHERE is_overtime_event
                ) AS overtime_events,

                COALESCE(
                    SUM(overtime_proxy_hours), 0
                ) AS overtime_hours,

                COUNT(*) FILTER (
                    WHERE is_overtime_event
                      AND (
                          NOT eligible_for_attendance
                          OR duration_difference_minutes <= 5
                      )
                ) AS invalid_flags,

                COUNT(*) FILTER (
                    WHERE overtime_proxy_hours < 0
                ) AS negative_overtime

            FROM mart.v_overtime_events
        """)
    ).mappings().one()

    daily = connection.execute(
        text("""
            SELECT
                COALESCE(
                    SUM(total_timesheet_records), 0
                ) AS total_events,

                COALESCE(
                    SUM(eligible_shift_records), 0
                ) AS eligible_events,

                COALESCE(
                    SUM(overtime_event_count), 0
                ) AS overtime_events,

                COALESCE(
                    SUM(total_overtime_proxy_hours), 0
                ) AS overtime_hours

            FROM mart.employee_overtime_daily
        """)
    ).mappings().one()

    gold_count = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM gold.fact_timesheet
        """)
    ).scalar_one()

    if events["total_events"] != gold_count:
        raise ValueError(
            "Overtime event count mismatch."
        )

    if events["invalid_flags"] != 0:
        raise ValueError(
            "Invalid overtime flags detected."
        )

    if events["negative_overtime"] != 0:
        raise ValueError(
            "Negative overtime hours detected."
        )

    for metric in (
        "total_events",
        "eligible_events",
        "overtime_events",
        "overtime_hours",
    ):
        if events[metric] != daily[metric]:
            raise ValueError(
                f"Overtime reconciliation failed: {metric}"
            )

    print("\nOVERTIME MART SUMMARY")

    for key, value in events.items():
        print(f"{key}: {value}")

    print("PASS: Overtime mart integrity verified")


def verify_workforce_mart(connection):
    """Validate workforce reporting and headcount trends."""

    profiles = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.v_workforce_profiles
        """)
    ).scalar_one()

    invalid_dates = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.v_workforce_profiles
            WHERE term_date < hire_date
        """)
    ).scalar_one()

    if invalid_dates:
        raise ValueError(
            f"{invalid_dates} invalid employment dates"
        )

    rows = connection.execute(
        text("""
            SELECT
                month_start,
                reporting_date,
                is_complete_calendar_month,
                opening_headcount,
                closing_headcount,
                new_hires,
                terminations,
                turnover_rate_pct
            FROM mart.workforce_monthly
            ORDER BY month_start
        """)
    ).mappings().all()

    if not rows:
        raise ValueError(
            "No workforce reporting periods found."
        )

    for row in rows:
        if row["closing_headcount"] > profiles:
            raise ValueError(
                "Headcount exceeds profiled population."
            )

        if row["terminations"] > profiles:
            raise ValueError(
                "Terminations exceed profiled population."
            )

    print("\nWORKFORCE MART SUMMARY")
    print(f"profiled_employees: {profiles}")
    print(f"reporting_months: {len(rows)}")

    for row in rows:
        print(
            f"{row['month_start']}: "
            f"headcount={row['closing_headcount']}, "
            f"hires={row['new_hires']}, "
            f"terminations={row['terminations']}, "
            f"turnover={row['turnover_rate_pct']}%, "
            f"complete={row['is_complete_calendar_month']}"
        )

    print("PASS: Workforce mart validation")

def verify_tenure_attrition_mart(connection):
    """Validate department tenure and early attrition."""

    tenure = connection.execute(
        text("""
            SELECT
                COUNT(*) AS employees,
                COUNT(DISTINCT employee_key) AS distinct_employees,

                COUNT(*) FILTER (
                    WHERE tenure_days < 0
                ) AS negative_tenure,

                COUNT(*) FILTER (
                    WHERE is_early_attrition
                      AND NOT completed_90_day_observation
                ) AS immature_early_attrition
            FROM mart.v_employee_tenure
        """)
    ).mappings().one()

    departments = connection.execute(
        text("""
            SELECT
                COUNT(*) AS departments,
                COALESCE(SUM(employee_count), 0)
                    AS employees
            FROM mart.department_tenure
        """)
    ).mappings().one()

    attrition = connection.execute(
        text("""
            SELECT *
            FROM mart.kpi_early_attrition
        """)
    ).mappings().one()

    if tenure["employees"] != tenure["distinct_employees"]:
        raise ValueError("Duplicate employee tenure records.")

    if tenure["negative_tenure"] != 0:
        raise ValueError("Negative employee tenure detected.")

    if tenure["immature_early_attrition"] != 0:
        raise ValueError(
            "Early attrition counted for immature cohort."
        )

    if departments["employees"] != tenure["employees"]:
        raise ValueError("Department tenure count mismatch.")

    if attrition["early_terminations"] > attrition["eligible_hires"]:
        raise ValueError("Early attrition exceeds eligible hires.")

    print("\nTENURE AND ATTRITION SUMMARY")

    print("employees:", tenure["employees"])
    print("departments:", departments["departments"])
    print("eligible_hires:", attrition["eligible_hires"])
    print("early_terminations:", attrition["early_terminations"])
    print(
        "early_attrition_rate_pct:",
        attrition["early_attrition_rate_pct"],
    )

    print("PASS: Tenure and attrition mart verified")


def verify_supplementary_analytics(connection):
    """Validate six supplementary analytical KPIs."""

    print("\nSUPPLEMENTARY ANALYTICS VALIDATION")

    # Monthly and departmental reporting must reconcile
    # with the underlying Gold fact table.
    gold_count = connection.execute(
        text("""
            SELECT COUNT(*) FROM gold.fact_timesheet
        """)
    ).scalar_one()

    for view in (
        "mart.kpi_monthly_workload",
        "mart.kpi_department_workload",
        "mart.kpi_weekday_workload",
    ):
        actual = connection.execute(
            text(
                f"SELECT COALESCE("
                f"SUM(timesheet_records), 0) FROM {view}"
            )
        ).scalar_one()

        if actual != gold_count:
            raise ValueError(
                f"{view}: record count mismatch"
            )

        print(f"PASS: {view} record reconciliation")

    # Department overtime must reconcile with event data.
    overtime = connection.execute(
        text("""
            SELECT
                COALESCE(SUM(overtime_events), 0)
            FROM mart.kpi_department_overtime
        """)
    ).scalar_one()

    expected_overtime = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.v_overtime_events
            WHERE is_overtime_event
        """)
    ).scalar_one()

    if overtime != expected_overtime:
        raise ValueError(
            "Department overtime reconciliation failed."
        )

    print("PASS: Department overtime reconciliation")

    # Attendance compliance must have valid bounds.
    invalid_compliance = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.kpi_attendance_compliance
            WHERE compliant_events > eligible_events
               OR (
                   compliance_rate_pct IS NOT NULL
                   AND (
                       compliance_rate_pct < 0
                       OR compliance_rate_pct > 100
                   )
               )
        """)
    ).scalar_one()

    if invalid_compliance:
        raise ValueError(
            "Invalid attendance compliance result."
        )

    print("PASS: Attendance compliance bounds")

    # Workforce changes must preserve headcount identity.
    invalid_workforce = connection.execute(
        text("""
            SELECT COUNT(*)
            FROM mart.kpi_workforce_changes
            WHERE closing_headcount <>
                opening_headcount
                + new_hires
                - terminations
        """)
    ).scalar_one()

    if invalid_workforce:
        raise ValueError(
            "Workforce change reconciliation failed."
        )

    print("PASS: Workforce change reconciliation")

    print("PASS: Supplementary analytics verified")

def main():
    print("=" * 65)
    print("ASE-ETL ANALYTICS PIPELINE")
    print("=" * 65)

    with ENGINE.begin() as connection:

        # Coordinate with upstream publishers.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(7182401)")
        )

        connection.execute(
            text("SELECT pg_advisory_xact_lock(7182402)")
        )

        # Prevent concurrent analytics executions.
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock(:lock_id)"
            ),
            {"lock_id": ANALYTICS_LOCK_ID},
        )

        verify_gold_publication(connection)

        print("\nINITIALIZING ANALYTICS SCHEMA")

        execute_sql_file(
            connection,
            "01_schema.sql",
        )

        execute_sql_file(
            connection,
            "02_pay_code_classification.sql",
        )

        execute_sql_file(
            connection,
            "03_classification_rules.sql",
        )
        
        execute_sql_file(
            connection,
            "04_timesheet_classification.sql",
        )

        execute_sql_file(
            connection,
            "05_attendance_mart.sql",
        )

        execute_sql_file(
            connection,
            "06_working_hours_mart.sql",
        )

        execute_sql_file(
            connection,
            "07_working_hours_kpis.sql",
        )

        execute_sql_file(
            connection,
            "08_overtime_mart.sql",
        )

        execute_sql_file(
            connection,
            "09_workforce_mart.sql",
        )
        
        execute_sql_file(
            connection,
            "10_tenure_attrition_mart.sql",
        )

        execute_sql_file(
            connection,
            "11_workload_analytics.sql",
        )

        execute_sql_file(
            connection,
            "12_attendance_analytics.sql",
        )

        execute_sql_file(
            connection,
            "13_workforce_analytics.sql",
        )

        print("\nVERIFYING CLASSIFICATION INVENTORY")

        verify_classification_inventory(connection)
        verify_component_inventory(connection)
        verify_classification_rules(connection)
        verify_timesheet_classification(connection)
        verify_attendance_mart(connection)
        verify_working_hours_mart(connection)
        verify_working_hours_kpis(connection)
        verify_overtime_mart(connection)
        verify_workforce_mart(connection)
        verify_tenure_attrition_mart(connection)
        verify_supplementary_analytics(connection)


    print("\nANALYTICS FOUNDATION INITIALIZED")


if __name__ == "__main__":
    main()
