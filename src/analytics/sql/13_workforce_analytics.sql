
-- KPI 15: Workforce Hiring and Termination Summary
-- Population: 50 source-profiled employees

CREATE OR REPLACE VIEW mart.kpi_workforce_changes AS

SELECT
    month_start,
    reporting_date,
    is_complete_calendar_month,

    opening_headcount,
    closing_headcount,

    new_hires,
    terminations,

    new_hires - terminations AS net_workforce_change,

    ROUND(
        100.0 * (new_hires - terminations)
        / NULLIF(opening_headcount, 0),
        2
    ) AS net_change_rate_pct,

    turnover_rate_pct

FROM mart.workforce_monthly;
