
-- Phase 6: Record-level timesheet classification.
-- One output row per Gold timesheet record.
-- Composite hours are never divided across components.

CREATE OR REPLACE VIEW mart.v_timesheet_classification AS

WITH component_status AS (
    SELECT
        f.timesheet_key,

        COUNT(*) AS component_count,

        COUNT(*) FILTER (
            WHERE c.pay_category = 'unclassified'
               OR c.component_code IS NULL
        ) AS unknown_component_count,

        COUNT(*) FILTER (
            WHERE c.counts_as_worked_time
        ) AS worked_component_count,

        COUNT(*) FILTER (
            WHERE NOT c.counts_as_worked_time
              AND c.pay_category <> 'unclassified'
        ) AS non_work_component_count,

        BOOL_AND(
            COALESCE(c.eligible_for_attendance, FALSE)
        ) AS all_attendance_eligible

    FROM gold.fact_timesheet f

    CROSS JOIN LATERAL
        unnest(string_to_array(f.pay_code, '|'))
        AS parts(component)

    LEFT JOIN mart.pay_code_component c
        ON c.component_code = TRIM(parts.component)

    GROUP BY f.timesheet_key
),

classification AS (
    SELECT
        timesheet_key,
        component_count,
        unknown_component_count,

        CASE
            WHEN unknown_component_count > 0
                THEN 'unresolved'

            WHEN worked_component_count = component_count
                THEN 'eligible'

            WHEN non_work_component_count = component_count
                THEN 'excluded'

            ELSE 'unresolved'
        END AS work_status,

        CASE
            WHEN unknown_component_count = 0
             AND worked_component_count = component_count
             AND all_attendance_eligible
                THEN TRUE
            ELSE FALSE
        END AS attendance_code_eligible

    FROM component_status
)

SELECT
    f.timesheet_key,
    f.employee_key,
    f.client_employee_id,
    f.punch_apply_date_key,
    f.punch_in_datetime,
    f.punch_out_datetime,
    f.scheduled_start_datetime,
    f.scheduled_end_datetime,
    f.hours_worked,
    f.pay_code,
    f.department_id,
    f.department_name,

    COALESCE(
        c.work_status,
        'unresolved'
    ) AS work_status,

    COALESCE(
        c.attendance_code_eligible,
        FALSE
    ) AS attendance_code_eligible,

    COALESCE(
        c.unknown_component_count,
        0
    ) AS unknown_component_count,

    (
        COALESCE(c.attendance_code_eligible, FALSE)
        AND f.scheduled_start_datetime IS NOT NULL
        AND f.scheduled_end_datetime IS NOT NULL
        AND f.scheduled_end_datetime >
            f.scheduled_start_datetime
        AND f.punch_out_datetime >
            f.punch_in_datetime
    ) AS eligible_for_attendance

FROM gold.fact_timesheet f

LEFT JOIN classification c
    ON f.timesheet_key = c.timesheet_key;
