"""Configuration and contracts for the Gold layer."""

GOLD_SCHEMA = "gold"

GOLD_MODEL_VERSION = "1.0"

EXPECTED_EMPLOYEE_COUNT = 10794
EXPECTED_TIMESHEET_COUNT = 381732

EMPLOYEE_BUSINESS_KEY = "client_employee_id"

TIMESHEET_BUSINESS_KEY = [
    "client_employee_id",
    "punch_in_datetime",
    "punch_out_datetime",
]

GOLD_TABLES = [
    "dim_employee",
    "dim_date",
    "fact_timesheet",
]