
from pathlib import Path

# Project paths

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "parquet"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"

EMPLOYEE_OUTPUT = SILVER_ROOT / "employees"
TIMESHEET_OUTPUT = SILVER_ROOT / "timesheets"
AUDIT_OUTPUT = SILVER_ROOT / "audit"

# Source missing-value markers

MISSING_MARKERS = {
    "",
    "[NULL]",
    "NULL",
    "N/A",
    "NA",
    "NAN",
    "NONE",
    "UNKNOWN",
    "NOT AVAILABLE",
}


# Employee schema

EMPLOYEE_DROP_COLUMNS = [
    "middle_name",
    "years_of_experience",
    "zip",
    "manager_employee_name",
    "is_per_deim",
    "work_phone",
    "clinical_level",
    "address",
    "city",
    "state",
    "country",
    "cell_phone",
]

EMPLOYEE_DATE_COLUMNS = [
    "job_start_date",
    "dob",
    "hire_date",
    "recent_hire_date",
    "anniversary_date",
    "term_date",
]

EMPLOYEE_NUMERIC_COLUMNS = [
    "scheduled_weekly_hour",
]


# Timesheet schema

TIMESHEET_DATE_COLUMNS = [
    "punch_apply_date",
]

TIMESHEET_DATETIME_COLUMNS = [
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime",
]

TIMESHEET_NUMERIC_COLUMNS = [
    "hours_worked",
]


# Ingestion metadata


METADATA_COLUMNS = [
    "source_file",
    "source_row_number",
    "ingested_at_utc",
    "ingestion_batch_id",
]

# Explicit timesheet business-field contract.
# Do not use employee_id + date as a unique key.

TIMESHEET_BUSINESS_COLUMNS = [
    "client_employee_id",
    "department_id",
    "department_name",
    "home_department_id",
    "home_department_name",
    "pay_code",
    "punch_in_comment",
    "punch_out_comment",
    "hours_worked",
    "punch_apply_date",
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime",
]
