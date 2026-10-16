
-- Phase 6: Working-hours KPI reporting
-- KPI 4: Average Working Hours per Employee
-- KPI 8: Rolling Average Working Hours

CREATE OR REPLACE VIEW mart.kpi_employee_working_hours AS

WITH daily AS (
    SELECT
        employee_key,
        client_employee_id,

        COUNT(*) FILTER (
            WHERE eligible_records > 0
        ) AS eligible_workdays,

        SUM(worked_hours) AS total_worked_hours,

        SUM(recorded_hours) AS total_recorded_hours,

        SUM(unresolved_hours) AS total_unresolved_hours,

        SUM(eligible_records) AS eligible_records,

        SUM(unresolved_records) AS unresolved_records

    FROM mart.employee_work_hours_daily

    GROUP BY
        employee_key,
        client_employee_id
),

weekly AS (
    SELECT
        employee_key,

        COUNT(*) FILTER (
            WHERE eligible_records > 0
        ) AS eligible_weeks,

        ROUND(
            AVG(worked_hours) FILTER (
                WHERE eligible_records > 0
            ), 2
        ) AS avg_weekly_worked_hours

    FROM mart.employee_work_hours_weekly

    GROUP BY employee_key
)

SELECT
    d.employee_key,
    d.client_employee_id,

    d.eligible_workdays,
    w.eligible_weeks,

    d.total_worked_hours,
    d.total_recorded_hours,
    d.total_unresolved_hours,

    d.eligible_records,
    d.unresolved_records,

    ROUND(
        d.total_worked_hours
        / NULLIF(d.eligible_workdays, 0),
        2
    ) AS avg_daily_worked_hours,

    w.avg_weekly_worked_hours

FROM daily d

JOIN weekly w
    ON d.employee_key = w.employee_key;


-- KPI 8: Rolling working-hours reporting

CREATE OR REPLACE VIEW mart.kpi_rolling_working_hours AS

SELECT
    employee_key,
    client_employee_id,
    work_date,

    worked_hours,

    rolling_7_day_avg_worked_hours,

    rolling_7_day_recorded_days

FROM mart.employee_work_hours_rolling;
