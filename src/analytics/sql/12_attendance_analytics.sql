
-- KPI 12: Attendance Compliance Rate
-- KPI 13: Department Shift-Overrun Frequency

CREATE OR REPLACE VIEW mart.kpi_attendance_compliance AS

SELECT
    d.full_date AS work_date,

    COUNT(*) FILTER (
        WHERE a.eligible_for_attendance
    ) AS eligible_events,

    COUNT(*) FILTER (
        WHERE a.eligible_for_attendance
          AND NOT a.is_late_arrival
          AND NOT a.is_early_departure
    ) AS compliant_events,

    COUNT(*) FILTER (
        WHERE a.is_late_arrival
    ) AS late_arrivals,

    COUNT(*) FILTER (
        WHERE a.is_early_departure
    ) AS early_departures,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE a.eligible_for_attendance
              AND NOT a.is_late_arrival
              AND NOT a.is_early_departure
        )
        / NULLIF(
            COUNT(*) FILTER (
                WHERE a.eligible_for_attendance
            ), 0
        ),
        2
    ) AS compliance_rate_pct

FROM mart.v_attendance_events a

JOIN gold.dim_date d
    ON a.punch_apply_date_key = d.date_key

GROUP BY d.full_date;


CREATE OR REPLACE VIEW mart.kpi_department_overtime AS

SELECT
    COALESCE(
        NULLIF(TRIM(department_name), ''),
        'Unknown'
    ) AS department_name,

    COUNT(*) FILTER (
        WHERE eligible_for_attendance
    ) AS eligible_events,

    COUNT(*) FILTER (
        WHERE is_overtime_event
    ) AS overtime_events,

    ROUND(
        SUM(overtime_proxy_hours), 4
    ) AS overtime_proxy_hours,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_overtime_event
        )
        / NULLIF(
            COUNT(*) FILTER (
                WHERE eligible_for_attendance
            ), 0
        ),
        2
    ) AS overtime_frequency_pct

FROM mart.v_overtime_events

GROUP BY
    COALESCE(
        NULLIF(TRIM(department_name), ''),
        'Unknown'
    );
