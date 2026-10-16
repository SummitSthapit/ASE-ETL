
-- KPI 7: Scheduled shift-duration overrun
--
-- Metric is an overtime proxy, NOT confirmed
-- paid or contractual overtime.
--
-- Eligible population:
--   Valid, attendance-eligible scheduled events.
--
-- Grace:
--   Overrun qualifies only if > 5 minutes.
--   Once qualified, report the full overrun.
--
-- Actual duration is punch-out minus punch-in.
-- This does not account for unpaid breaks.

CREATE OR REPLACE VIEW mart.v_overtime_events AS

WITH durations AS (
    SELECT
        timesheet_key,
        employee_key,
        client_employee_id,
        punch_apply_date_key,
        pay_code,
        department_id,
        department_name,
        hours_worked,
        eligible_for_attendance,

        CASE
            WHEN eligible_for_attendance
            THEN EXTRACT(
                EPOCH FROM (
                    punch_out_datetime
                    - punch_in_datetime
                )
            ) / 60.0
        END AS actual_duration_minutes,

        CASE
            WHEN eligible_for_attendance
            THEN EXTRACT(
                EPOCH FROM (
                    scheduled_end_datetime
                    - scheduled_start_datetime
                )
            ) / 60.0
        END AS scheduled_duration_minutes

    FROM mart.v_timesheet_classification
),

differences AS (
    SELECT
        *,

        CASE
            WHEN eligible_for_attendance
            THEN actual_duration_minutes
                 - scheduled_duration_minutes
        END AS duration_difference_minutes

    FROM durations
)

SELECT
    *,

    (
        eligible_for_attendance
        AND duration_difference_minutes > 5
    ) AS is_overtime_event,

    CASE
        WHEN eligible_for_attendance
         AND duration_difference_minutes > 5
        THEN ROUND(
            duration_difference_minutes / 60.0,
            4
        )
        ELSE 0::NUMERIC
    END AS overtime_proxy_hours

FROM differences;


-- Daily employee overtime reporting mart

CREATE OR REPLACE VIEW mart.employee_overtime_daily AS

SELECT
    employee_key,
    client_employee_id,
    punch_apply_date_key,

    COUNT(*) AS total_timesheet_records,

    COUNT(*) FILTER (
        WHERE eligible_for_attendance
    ) AS eligible_shift_records,

    COUNT(*) FILTER (
        WHERE is_overtime_event
    ) AS overtime_event_count,

    ROUND(
        SUM(overtime_proxy_hours), 4
    ) AS total_overtime_proxy_hours

FROM mart.v_overtime_events

GROUP BY
    employee_key,
    client_employee_id,
    punch_apply_date_key;
