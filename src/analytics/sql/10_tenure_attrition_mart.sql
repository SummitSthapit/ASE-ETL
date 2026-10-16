
-- KPI 3: Average Tenure by Department
-- KPI 9: Early Attrition Rate
--
-- Population: Source-profiled employees.
-- Early attrition threshold: 90 days.
-- Reporting cutoff: Last Gold calendar date.

CREATE OR REPLACE VIEW mart.v_employee_tenure AS

WITH reporting_cutoff AS (
    SELECT MAX(full_date) AS as_of_date
    FROM gold.dim_date
),

employees AS (
    SELECT
        e.employee_key,
        e.client_employee_id,

        e.department_id,

        COALESCE(
            NULLIF(TRIM(e.department_name), ''),
            'Unknown'
        ) AS department_name,

        e.hire_date,
        e.term_date,
        c.as_of_date,

        LEAST(
            COALESCE(e.term_date, c.as_of_date),
            c.as_of_date
        ) AS tenure_end_date

    FROM mart.v_workforce_profiles e
    CROSS JOIN reporting_cutoff c

    WHERE e.hire_date <= c.as_of_date
      AND (
          e.term_date IS NULL
          OR e.term_date >= e.hire_date
      )
)

SELECT
    *,

    (
        tenure_end_date - hire_date
    ) AS tenure_days,

    ROUND(
        (tenure_end_date - hire_date)::NUMERIC
        / 365.25,
        2
    ) AS tenure_years,

    (
        term_date IS NOT NULL
        AND term_date <= as_of_date
    ) AS is_terminated,

    (
        hire_date <= as_of_date - 90
    ) AS completed_90_day_observation,

    (
        term_date IS NOT NULL
        AND term_date <= as_of_date
        AND term_date >= hire_date
        AND term_date < hire_date + 90
    ) AS is_early_attrition

FROM employees;


-- Average tenure by department

CREATE OR REPLACE VIEW mart.department_tenure AS

SELECT
    department_id,
    department_name,

    COUNT(*) AS employee_count,

    COUNT(*) FILTER (
        WHERE is_terminated
    ) AS terminated_employees,

    ROUND(
        AVG(tenure_days), 2
    ) AS average_tenure_days,

    ROUND(
        AVG(tenure_years), 2
    ) AS average_tenure_years

FROM mart.v_employee_tenure

GROUP BY
    department_id,
    department_name;


-- Overall early attrition KPI

CREATE OR REPLACE VIEW mart.kpi_early_attrition AS

SELECT
    MAX(as_of_date) AS reporting_date,

    COUNT(*) FILTER (
        WHERE completed_90_day_observation
    ) AS eligible_hires,

    COUNT(*) FILTER (
        WHERE completed_90_day_observation
          AND is_early_attrition
    ) AS early_terminations,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE completed_90_day_observation
              AND is_early_attrition
        )
        / NULLIF(
            COUNT(*) FILTER (
                WHERE completed_90_day_observation
            ),
            0
        ),
        2
    ) AS early_attrition_rate_pct

FROM mart.v_employee_tenure;
