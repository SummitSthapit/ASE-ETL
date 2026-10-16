
-- Phase 6: Working-hours reporting marts
-- Date basis: punch_apply_date_key
-- Worked hours use the provisional pay-code policy.

CREATE OR REPLACE VIEW mart.employee_work_hours_daily AS

SELECT
    f.employee_key,
    f.client_employee_id,
    d.full_date AS work_date,

    COUNT(*) AS timesheet_records,

    ROUND(SUM(f.hours_worked), 2)
        AS recorded_hours,

    ROUND(
        COALESCE(
            SUM(f.hours_worked) FILTER (
                WHERE c.work_status = 'eligible'
            ), 0
        ), 2
    ) AS worked_hours,

    COUNT(*) FILTER (
        WHERE c.work_status = 'eligible'
    ) AS eligible_records,

    COUNT(*) FILTER (
        WHERE c.work_status = 'excluded'
    ) AS excluded_records,

    COUNT(*) FILTER (
        WHERE c.work_status = 'unresolved'
    ) AS unresolved_records,

    ROUND(
        COALESCE(
            SUM(f.hours_worked) FILTER (
                WHERE c.work_status = 'unresolved'
            ), 0
        ), 2
    ) AS unresolved_hours

FROM gold.fact_timesheet f

JOIN gold.dim_date d
    ON f.punch_apply_date_key = d.date_key

JOIN mart.v_timesheet_classification c
    ON f.timesheet_key = c.timesheet_key

GROUP BY
    f.employee_key,
    f.client_employee_id,
    d.full_date;


-- Weekly employee reporting view

CREATE OR REPLACE VIEW mart.employee_work_hours_weekly AS

SELECT
    employee_key,
    client_employee_id,

    DATE_TRUNC('week', work_date)::DATE
        AS week_start_date,

    COUNT(*) AS recorded_days,

    SUM(timesheet_records)
        AS timesheet_records,

    ROUND(SUM(recorded_hours), 2)
        AS recorded_hours,

    ROUND(SUM(worked_hours), 2)
        AS worked_hours,

    SUM(eligible_records)
        AS eligible_records,

    SUM(excluded_records)
        AS excluded_records,

    SUM(unresolved_records)
        AS unresolved_records,

    ROUND(SUM(unresolved_hours), 2)
        AS unresolved_hours,

    ROUND(AVG(worked_hours), 2)
        AS avg_worked_hours_per_recorded_day

FROM mart.employee_work_hours_daily

GROUP BY
    employee_key,
    client_employee_id,
    DATE_TRUNC('week', work_date)::DATE;


-- Rolling 7-calendar-day working-hours average
-- Includes days with recorded timesheets only.
-- Missing days are not assumed to be zero-hour days.

CREATE OR REPLACE VIEW mart.employee_work_hours_rolling AS

SELECT
    employee_key,
    client_employee_id,
    work_date,
    worked_hours,
    recorded_hours,
    unresolved_hours,

    ROUND(
        AVG(worked_hours) OVER (
            PARTITION BY employee_key
            ORDER BY work_date::TIMESTAMP
            RANGE BETWEEN INTERVAL '6 days' PRECEDING
                      AND CURRENT ROW
        ), 2
    ) AS rolling_7_day_avg_worked_hours,

    COUNT(*) OVER (
        PARTITION BY employee_key
        ORDER BY work_date::TIMESTAMP
        RANGE BETWEEN INTERVAL '6 days' PRECEDING
                  AND CURRENT ROW
    ) AS rolling_7_day_recorded_days

FROM mart.employee_work_hours_daily;
