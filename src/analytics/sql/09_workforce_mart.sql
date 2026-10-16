
-- Phase 6: Workforce analytics
-- Source: Gold employee profiles
--
-- Scope: Employees with verified source profiles
-- and non-null hire dates.
--
-- Headcount counts employees active at the
-- end of each available reporting period.
--
-- Turnover = terminations / average headcount.
-- Incomplete final months are explicitly marked.

CREATE OR REPLACE VIEW mart.v_workforce_profiles AS

SELECT
    employee_key,
    client_employee_id,
    first_name,
    last_name,
    department_id,
    department_name,
    hire_date,
    term_date,
    active_status
FROM gold.dim_employee
WHERE has_employee_profile = TRUE
  AND hire_date IS NOT NULL;


CREATE OR REPLACE VIEW mart.workforce_monthly AS

WITH bounds AS (
    SELECT
        MIN(full_date) AS first_date,
        MAX(full_date) AS last_date
    FROM gold.dim_date
),

months AS (
    SELECT
        gs.month_start::DATE AS month_start,

        (
            gs.month_start
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE AS calendar_month_end,

        LEAST(
            (
                gs.month_start
                + INTERVAL '1 month'
                - INTERVAL '1 day'
            )::DATE,
            b.last_date
        ) AS reporting_date

    FROM bounds b

    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('month', b.first_date)::TIMESTAMP,
        DATE_TRUNC('month', b.last_date)::TIMESTAMP,
        INTERVAL '1 month'
    ) AS gs(month_start)
),

metrics AS (
    SELECT
        m.month_start,
        m.calendar_month_end,
        m.reporting_date,

        (
            m.reporting_date = m.calendar_month_end
        ) AS is_complete_calendar_month,

        COUNT(*) FILTER (
            WHERE e.hire_date < m.month_start
              AND (
                  e.term_date IS NULL
                  OR e.term_date >= m.month_start
              )
        ) AS opening_headcount,

        COUNT(*) FILTER (
            WHERE e.hire_date <= m.reporting_date
              AND (
                  e.term_date IS NULL
                  OR e.term_date > m.reporting_date
              )
        ) AS closing_headcount,

        COUNT(*) FILTER (
            WHERE e.hire_date BETWEEN
                m.month_start AND m.reporting_date
        ) AS new_hires,

        COUNT(*) FILTER (
            WHERE e.term_date BETWEEN
                m.month_start AND m.reporting_date
        ) AS terminations

    FROM months m
    CROSS JOIN mart.v_workforce_profiles e

    GROUP BY
        m.month_start,
        m.calendar_month_end,
        m.reporting_date
)

SELECT
    *,

    ROUND(
        (
            opening_headcount + closing_headcount
        )::NUMERIC / 2,
        2
    ) AS average_headcount,

    ROUND(
        100.0 * terminations
        / NULLIF(
            (
                opening_headcount + closing_headcount
            )::NUMERIC / 2,
            0
        ),
        2
    ) AS turnover_rate_pct

FROM metrics;
