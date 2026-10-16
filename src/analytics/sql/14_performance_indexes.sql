
-- ASE-ETL Gold analytical access-path indexes.
--
-- These indexes support joins, date filtering,
-- employee filtering and profiled workforce queries.
--
-- Do not modify warehouse data or KPI definitions.

-- Employee-based fact queries and joins.
CREATE INDEX IF NOT EXISTS
    idx_gold_timesheet_employee_date
ON gold.fact_timesheet (
    employee_key,
    punch_apply_date_key
);

-- Date-based reporting and filtering.
CREATE INDEX IF NOT EXISTS
    idx_gold_timesheet_apply_date_employee
ON gold.fact_timesheet (
    punch_apply_date_key,
    employee_key
);

-- Fast access to employees with actual HR profiles.
CREATE INDEX IF NOT EXISTS
    idx_gold_employee_profile_hire_term
ON gold.dim_employee (
    hire_date,
    term_date
)
WHERE has_employee_profile = TRUE
  AND hire_date IS NOT NULL;
