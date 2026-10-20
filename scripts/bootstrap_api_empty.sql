-- Bootstrap bridge for a fresh EMPTY local ase_etl database.
-- Creates the API operational employee table without SELECTing Silver first.
-- Necessary because the current Silver pipeline reads api.employee_change_log,
-- while src.api.migrations seeds api.employees from silver.employees.
-- Derived from src/api/migrations.py. NOT a substitute for its final seed step.
-- Review against the current repository revision before applying.

CREATE SCHEMA IF NOT EXISTS api;
CREATE TABLE IF NOT EXISTS api.employees (
    client_employee_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    middle_name VARCHAR,
    last_name VARCHAR,
    preferred_name VARCHAR,
    job_code VARCHAR,
    job_title VARCHAR,
    department_id VARCHAR,
    department_name VARCHAR,
    organization_id VARCHAR,
    organization_name VARCHAR,
    hire_date DATE,
    term_date DATE,
    active_status VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    sync_status VARCHAR NOT NULL DEFAULT 'pending'
      CHECK (sync_status IN ('pending', 'synced', 'failed'))
);
