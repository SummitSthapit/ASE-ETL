"""Gold layer SQL transformations."""

from sqlalchemy import text


def load_dim_employee(connection):
    """
    Populate the Gold employee dimension from Silver.

    - Preserve all distinct employee identities.
    - Attach available employee profiles.
    - Preserve existing employee surrogate keys.
    - Support safe reruns using UPSERT.
    """

    connection.execute(
        text("""
            WITH employee_ids AS (
                SELECT client_employee_id
                FROM silver.employees

                UNION

                SELECT DISTINCT client_employee_id
                FROM silver.timesheets
            )

            INSERT INTO gold.dim_employee (
                client_employee_id,
                first_name,
                middle_name,
                last_name,
                preferred_name,
                job_code,
                job_title,
                job_start_date,
                organization_id,
                organization_name,
                department_id,
                department_name,
                hire_date,
                recent_hire_date,
                term_date,
                manager_employee_id,
                fte_status,
                scheduled_weekly_hour,
                active_status,
                years_of_experience,
                clinical_level,
                is_per_deim,
                has_employee_profile
            )

            SELECT
                ids.client_employee_id,
                e.first_name,
                e.middle_name,
                e.last_name,
                e.preferred_name,
                e.job_code,
                e.job_title,
                e.job_start_date,
                e.organization_id,
                e.organization_name,
                e.department_id,
                e.department_name,
                e.hire_date,
                e.recent_hire_date,
                e.term_date,
                e.manager_employee_id,
                e.fte_status,
                e.scheduled_weekly_hour,
                e.active_status,
                e.years_of_experience,
                e.clinical_level,
                e.is_per_deim,

                (e.client_employee_id IS NOT NULL)

            FROM employee_ids ids

            LEFT JOIN silver.employees e
                ON ids.client_employee_id =
                   e.client_employee_id

            ON CONFLICT (client_employee_id)
            DO UPDATE SET
                first_name = EXCLUDED.first_name,
                middle_name = EXCLUDED.middle_name,
                last_name = EXCLUDED.last_name,
                preferred_name = EXCLUDED.preferred_name,
                job_code = EXCLUDED.job_code,
                job_title = EXCLUDED.job_title,
                job_start_date = EXCLUDED.job_start_date,
                organization_id = EXCLUDED.organization_id,
                organization_name = EXCLUDED.organization_name,
                department_id = EXCLUDED.department_id,
                department_name = EXCLUDED.department_name,
                hire_date = EXCLUDED.hire_date,
                recent_hire_date = EXCLUDED.recent_hire_date,
                term_date = EXCLUDED.term_date,
                manager_employee_id =
                    EXCLUDED.manager_employee_id,
                fte_status = EXCLUDED.fte_status,
                scheduled_weekly_hour =
                    EXCLUDED.scheduled_weekly_hour,
                active_status = EXCLUDED.active_status,
                years_of_experience =
                    EXCLUDED.years_of_experience,
                clinical_level = EXCLUDED.clinical_level,
                is_per_deim = EXCLUDED.is_per_deim,
                has_employee_profile =
                    EXCLUDED.has_employee_profile
        """)
    )

    print("PASS: Gold employee dimension loaded")

def load_dim_date(connection):
    """
    Populate the date dimension from Silver date ranges.

    Includes punch-in, punch-out, and punch-apply dates.
    """

    connection.execute(
        text("""
            WITH source_dates AS (
                SELECT punch_apply_date AS dt
                FROM silver.timesheets
                WHERE punch_apply_date IS NOT NULL

                UNION ALL

                SELECT punch_in_datetime::date
                FROM silver.timesheets

                UNION ALL

                SELECT punch_out_datetime::date
                FROM silver.timesheets
            ),

            date_bounds AS (
                SELECT
                    MIN(dt) AS min_date,
                    MAX(dt) AS max_date
                FROM source_dates
            ),

            calendar AS (
                SELECT
                    generate_series(
                        min_date,
                        max_date,
                        INTERVAL '1 day'
                    )::date AS full_date
                FROM date_bounds
            )

            INSERT INTO gold.dim_date (
                date_key,
                full_date,
                day_of_month,
                day_of_week,
                day_name,
                week_of_year,
                month_number,
                month_name,
                quarter_number,
                year_number,
                iso_year,
                is_weekend
            )

            SELECT
                TO_CHAR(full_date, 'YYYYMMDD')::integer,
                full_date,

                EXTRACT(DAY FROM full_date)::smallint,
                EXTRACT(ISODOW FROM full_date)::smallint,
                TRIM(TO_CHAR(full_date, 'Day')),

                EXTRACT(WEEK FROM full_date)::smallint,
                EXTRACT(MONTH FROM full_date)::smallint,
                TRIM(TO_CHAR(full_date, 'Month')),

                EXTRACT(QUARTER FROM full_date)::smallint,
                EXTRACT(YEAR FROM full_date)::integer,
                EXTRACT(ISOYEAR FROM full_date)::integer,

                EXTRACT(ISODOW FROM full_date) IN (6, 7)

            FROM calendar

            ON CONFLICT (date_key)
            DO UPDATE SET
                full_date = EXCLUDED.full_date,
                day_of_month = EXCLUDED.day_of_month,
                day_of_week = EXCLUDED.day_of_week,
                day_name = EXCLUDED.day_name,
                week_of_year = EXCLUDED.week_of_year,
                month_number = EXCLUDED.month_number,
                month_name = EXCLUDED.month_name,
                quarter_number = EXCLUDED.quarter_number,
                year_number = EXCLUDED.year_number,
                iso_year = EXCLUDED.iso_year,
                is_weekend = EXCLUDED.is_weekend
        """)
    )

    print("PASS: Gold date dimension loaded")

def load_fact_timesheet(connection):
    """
    Load Silver timesheets into the Gold fact table.

    Preserve existing timesheet surrogate keys.
    Avoid duplicates using Silver's event business key.
    """

    result = connection.execute(
        text("""
            INSERT INTO gold.fact_timesheet (
                employee_key,
                punch_apply_date_key,
                punch_in_date_key,
                punch_out_date_key,

                client_employee_id,
                punch_in_datetime,
                punch_out_datetime,
                scheduled_start_datetime,
                scheduled_end_datetime,

                hours_worked,

                department_id,
                department_name,
                home_department_id,
                home_department_name,
                pay_code,

                punch_in_comment,
                punch_out_comment,

                source_file,
                source_row_number,
                ingested_at_utc,
                ingestion_batch_id
            )

            SELECT
                e.employee_key,

                da.date_key,
                di.date_key,
                do_date.date_key,

                t.client_employee_id,
                t.punch_in_datetime,
                t.punch_out_datetime,
                t.scheduled_start_datetime,
                t.scheduled_end_datetime,

                t.hours_worked,

                t.department_id,
                t.department_name,
                t.home_department_id,
                t.home_department_name,
                t.pay_code,

                t.punch_in_comment,
                t.punch_out_comment,

                t.source_file,
                t.source_row_number,
                t.ingested_at_utc,
                t.ingestion_batch_id

            FROM silver.timesheets t

            JOIN gold.dim_employee e
                ON t.client_employee_id = e.client_employee_id

            JOIN gold.dim_date da
                ON t.punch_apply_date = da.full_date

            JOIN gold.dim_date di
                ON t.punch_in_datetime::date = di.full_date

            JOIN gold.dim_date do_date
                ON t.punch_out_datetime::date =
                   do_date.full_date

            ON CONFLICT (
                client_employee_id,
                punch_in_datetime,
                punch_out_datetime
            )

            DO UPDATE SET
                employee_key = EXCLUDED.employee_key,
                punch_apply_date_key =
                    EXCLUDED.punch_apply_date_key,
                punch_in_date_key =
                    EXCLUDED.punch_in_date_key,
                punch_out_date_key =
                    EXCLUDED.punch_out_date_key,

                scheduled_start_datetime =
                    EXCLUDED.scheduled_start_datetime,
                scheduled_end_datetime =
                    EXCLUDED.scheduled_end_datetime,

                hours_worked = EXCLUDED.hours_worked,

                department_id = EXCLUDED.department_id,
                department_name = EXCLUDED.department_name,

                home_department_id =
                    EXCLUDED.home_department_id,
                home_department_name =
                    EXCLUDED.home_department_name,

                pay_code = EXCLUDED.pay_code,

                punch_in_comment =
                    EXCLUDED.punch_in_comment,
                punch_out_comment =
                    EXCLUDED.punch_out_comment,

                source_file = EXCLUDED.source_file,
                source_row_number =
                    EXCLUDED.source_row_number,
                ingested_at_utc =
                    EXCLUDED.ingested_at_utc,
                ingestion_batch_id =
                    EXCLUDED.ingestion_batch_id
        """)
    )

    print(
        "PASS: Gold timesheet fact load executed "
        f"({result.rowcount:,} inserted/updated)"
    )