
-- Phase 6: Attendance analytics
-- Grace period: 5 minutes
-- Source: mart.v_timesheet_classification

CREATE OR REPLACE VIEW mart.v_attendance_events AS

WITH attendance AS (
    SELECT
        timesheet_key,
        employee_key,
        client_employee_id,
        punch_apply_date_key,

        punch_in_datetime,
        punch_out_datetime,

        scheduled_start_datetime,
        scheduled_end_datetime,

        hours_worked,
        pay_code,
        department_id,
        department_name,

        work_status,
        eligible_for_attendance,

        CASE
            WHEN eligible_for_attendance
            THEN EXTRACT(
                EPOCH FROM (
                    punch_in_datetime
                    - scheduled_start_datetime
                )
            ) / 60.0
            ELSE NULL
        END AS arrival_difference_minutes,

        CASE
            WHEN eligible_for_attendance
            THEN EXTRACT(
                EPOCH FROM (
                    scheduled_end_datetime
                    - punch_out_datetime
                )
            ) / 60.0
            ELSE NULL
        END AS departure_difference_minutes

    FROM mart.v_timesheet_classification
)

SELECT
    *,

    (
        eligible_for_attendance
        AND arrival_difference_minutes > 5
    ) AS is_late_arrival,

    (
        eligible_for_attendance
        AND departure_difference_minutes > 5
    ) AS is_early_departure

FROM attendance;


-- Daily reporting data mart

CREATE OR REPLACE VIEW mart.employee_attendance_daily AS

SELECT
    employee_key,
    client_employee_id,
    punch_apply_date_key,

    COUNT(*) AS total_timesheet_records,

    COUNT(*) FILTER (
        WHERE eligible_for_attendance
    ) AS eligible_shift_records,

    COUNT(*) FILTER (
        WHERE is_late_arrival
    ) AS late_arrival_count,

    COUNT(*) FILTER (
        WHERE is_early_departure
    ) AS early_departure_count,

    ROUND(
        AVG(arrival_difference_minutes) FILTER (
            WHERE eligible_for_attendance
        ),
        2
    ) AS avg_arrival_difference_minutes,

    ROUND(
        AVG(departure_difference_minutes) FILTER (
            WHERE eligible_for_attendance
        ),
        2
    ) AS avg_departure_difference_minutes

FROM mart.v_attendance_events

GROUP BY
    employee_key,
    client_employee_id,
    punch_apply_date_key;
