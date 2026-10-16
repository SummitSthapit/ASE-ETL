
"""Diagnostic audit for the overtime KPI."""

from sqlalchemy import text

from src.silver.publish import ENGINE


AUDIT_SQL = """
WITH metrics AS (
    SELECT
        timesheet_key,
        hours_worked,
        actual_duration_minutes / 60.0
            AS punch_duration_hours,
        duration_difference_minutes,
        overtime_proxy_hours,
        is_overtime_event
    FROM mart.v_overtime_events
    WHERE eligible_for_attendance
)
SELECT
    COUNT(*) AS eligible_events,

    COUNT(*) FILTER (
        WHERE ABS(
            punch_duration_hours - hours_worked
        ) > 0.25
    ) AS duration_mismatch_over_15min,

    COUNT(*) FILTER (
        WHERE duration_difference_minutes > 60
    ) AS overrun_over_1hour,

    COUNT(*) FILTER (
        WHERE duration_difference_minutes > 240
    ) AS overrun_over_4hours,

    COUNT(*) FILTER (
        WHERE duration_difference_minutes > 720
    ) AS overrun_over_12hours,

    ROUND(
        MAX(duration_difference_minutes),
        2
    ) AS max_overrun_minutes,

    ROUND(
        AVG(duration_difference_minutes) FILTER (
            WHERE is_overtime_event
        ),
        2
    ) AS avg_qualifying_overrun_minutes,

    ROUND(
        SUM(overtime_proxy_hours), 4
    ) AS total_overtime_proxy_hours

FROM metrics;
"""


def main():
    print("=" * 65)
    print("ASE-ETL OVERTIME DATA QUALITY AUDIT")
    print("=" * 65)

    with ENGINE.connect() as connection:
        result = connection.execute(
            text(AUDIT_SQL)
        ).mappings().one()

    for metric, value in result.items():
        print(f"{metric}: {value}")

    print("\nOVERTIME AUDIT COMPLETED")


if __name__ == "__main__":
    main()
