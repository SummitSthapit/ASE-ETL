
-- KPI 10: Monthly Worked Hours Trend
-- KPI 11: Department Workload Distribution
-- KPI 14: Weekend vs Weekday Working Hours

CREATE OR REPLACE VIEW mart.kpi_monthly_workload AS
SELECT
    DATE_TRUNC('month', work_date)::DATE AS month_start,
    COUNT(DISTINCT employee_key) AS employees_with_records,
    SUM(timesheet_records) AS timesheet_records,
    ROUND(SUM(recorded_hours), 2) AS recorded_hours,
    ROUND(SUM(worked_hours), 2) AS worked_hours,
    ROUND(SUM(unresolved_hours), 2) AS unresolved_hours,
    SUM(eligible_records) AS eligible_records,
    SUM(unresolved_records) AS unresolved_records
FROM mart.employee_work_hours_daily
GROUP BY DATE_TRUNC('month', work_date)::DATE;


CREATE OR REPLACE VIEW mart.kpi_department_workload AS
SELECT
    COALESCE(
        NULLIF(TRIM(f.department_name), ''),
        'Unknown'
    ) AS department_name,

    COUNT(*) AS timesheet_records,
    COUNT(DISTINCT f.employee_key) AS employees_with_records,

    ROUND(SUM(f.hours_worked), 2) AS recorded_hours,

    ROUND(
        COALESCE(
            SUM(f.hours_worked) FILTER (
                WHERE c.work_status = 'eligible'
            ), 0
        ), 2
    ) AS worked_hours,

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
JOIN mart.v_timesheet_classification c
    ON f.timesheet_key = c.timesheet_key

GROUP BY
    COALESCE(
        NULLIF(TRIM(f.department_name), ''),
        'Unknown'
    );


CREATE OR REPLACE VIEW mart.kpi_weekday_workload AS
SELECT
    d.day_of_week,
    d.day_name,
    d.is_weekend,

    COUNT(*) AS timesheet_records,
    COUNT(DISTINCT f.employee_key) AS employees_with_records,

    ROUND(SUM(f.hours_worked), 2) AS recorded_hours,

    ROUND(
        COALESCE(
            SUM(f.hours_worked) FILTER (
                WHERE c.work_status = 'eligible'
            ), 0
        ), 2
    ) AS worked_hours,

    COUNT(*) FILTER (
        WHERE c.work_status = 'unresolved'
    ) AS unresolved_records

FROM gold.fact_timesheet f

JOIN gold.dim_date d
    ON f.punch_apply_date_key = d.date_key

JOIN mart.v_timesheet_classification c
    ON f.timesheet_key = c.timesheet_key

GROUP BY
    d.day_of_week,
    d.day_name,
    d.is_weekend;
