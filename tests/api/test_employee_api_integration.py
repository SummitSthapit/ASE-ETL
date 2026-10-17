"""Isolated ASE-ETL employee/timesheet API tests.

Run ONLY with DATABASE_URL pointing to the dedicated ase_etl_test database.
Creates lightweight operational/Gold fixtures in that test database only.
"""
from datetime import date, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.database import engine
from src.api.main import app
from src.api.security import create_access_token, hash_password


def require_test_database():
    with engine.connect() as connection:
        name = connection.execute(text('SELECT current_database()')).scalar_one()
    if name != 'ase_etl_test':
        raise RuntimeError(f'Refusing database-changing tests on {name!r}; expected ase_etl_test')


@pytest.fixture(scope='module', autouse=True)
def test_schema():
    require_test_database()
    with engine.begin() as c:
        c.execute(text('CREATE SCHEMA IF NOT EXISTS api'))
        c.execute(text('CREATE SCHEMA IF NOT EXISTS gold'))
        c.execute(text('''CREATE TABLE IF NOT EXISTS api.employees (
            client_employee_id VARCHAR PRIMARY KEY,
            first_name VARCHAR, middle_name VARCHAR, last_name VARCHAR,
            preferred_name VARCHAR, job_code VARCHAR, job_title VARCHAR,
            department_id VARCHAR, department_name VARCHAR,
            organization_id VARCHAR, organization_name VARCHAR,
            hire_date DATE, term_date DATE, active_status VARCHAR,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
            sync_status VARCHAR NOT NULL DEFAULT 'pending'
                CHECK(sync_status IN ('pending','synced','failed'))
        )'''))
        # Minimal Gold contract sufficient for verifying API read behavior.
        c.execute(text('''CREATE TABLE IF NOT EXISTS gold.dim_employee (
            employee_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            client_employee_id VARCHAR NOT NULL UNIQUE
        )'''))
        c.execute(text('''CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_key INTEGER PRIMARY KEY, full_date DATE NOT NULL UNIQUE
        )'''))
        c.execute(text('''CREATE TABLE IF NOT EXISTS gold.fact_timesheet (
            timesheet_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            client_employee_id VARCHAR NOT NULL,
            punch_apply_date_key INTEGER NOT NULL,
            punch_in_datetime TIMESTAMP NOT NULL,
            punch_out_datetime TIMESTAMP NOT NULL,
            scheduled_start_datetime TIMESTAMP,
            scheduled_end_datetime TIMESTAMP,
            hours_worked NUMERIC NOT NULL,
            department_name VARCHAR, pay_code VARCHAR
        )'''))
    yield


@pytest.fixture
def client_context():
    require_test_database()
    suffix = uuid4().hex[:12]
    admin_name = f'api_test_admin_{suffix}'
    viewer_name = f'api_test_viewer_{suffix}'
    emp_id = f'API-TEST-{suffix}'
    gold_emp_id = f'GOLD-TEST-{suffix}'
    date_key = 20991231
    # Insert a user fixture, and a Gold employee/timesheet for the read-only test.
    with engine.begin() as c:
        admin_id = c.execute(text('''INSERT INTO api.users
            (username,hashed_password,role,is_active,token_version)
            VALUES (:username,:hash,'admin',TRUE,0) RETURNING user_id'''),
            {'username': admin_name, 'hash': hash_password('test-only-passphrase-admin')}).scalar_one()
        viewer_id = c.execute(text('''INSERT INTO api.users
            (username,hashed_password,role,is_active,token_version)
            VALUES (:username,:hash,'viewer',TRUE,0) RETURNING user_id'''),
            {'username': viewer_name, 'hash': hash_password('test-only-passphrase-viewer')}).scalar_one()
        c.execute(text('''INSERT INTO gold.dim_employee (client_employee_id)
            VALUES (:eid)'''), {'eid': gold_emp_id})
        c.execute(text('''INSERT INTO gold.dim_date (date_key, full_date)
            VALUES (:k, :d) ON CONFLICT (date_key) DO NOTHING'''),
            {'k': date_key, 'd': date(2099,12,31)})
        c.execute(text('''INSERT INTO gold.fact_timesheet
            (client_employee_id,punch_apply_date_key,punch_in_datetime,
            punch_out_datetime,hours_worked,department_name,pay_code)
            VALUES (:eid, :k, :start, :end, 8, 'Engineering','REG')'''),
            {'eid': gold_emp_id, 'k': date_key,
             'start': datetime(2099,12,31,9), 'end': datetime(2099,12,31,17)})
    admin_headers = {'Authorization': f'Bearer {create_access_token(admin_id, 0)}'}
    viewer_headers = {'Authorization': f'Bearer {create_access_token(viewer_id, 0)}'}
    try:
        with TestClient(app) as client:
            yield client, admin_headers, viewer_headers, emp_id, gold_emp_id
    finally:
        require_test_database()
        with engine.begin() as c:
            c.execute(text('DELETE FROM api.employees WHERE client_employee_id=:eid'), {'eid': emp_id})
            c.execute(text('DELETE FROM gold.fact_timesheet WHERE client_employee_id=:eid'), {'eid': gold_emp_id})
            c.execute(text('DELETE FROM gold.dim_employee WHERE client_employee_id=:eid'), {'eid': gold_emp_id})
            c.execute(text('DELETE FROM api.auth_events WHERE user_id IN (:a,:v)'), {'a': admin_id,'v':viewer_id})
            c.execute(text('DELETE FROM api.users WHERE user_id IN (:a,:v)'), {'a': admin_id,'v':viewer_id})


def employee_payload(emp_id):
    return {'client_employee_id': emp_id, 'first_name': 'Api', 'last_name': 'Tester',
            'department_name': 'Engineering', 'hire_date': '2026-01-01'}


def test_employee_requires_auth(client_context):
    client, _, _, _, _ = client_context
    assert client.get('/api/v1/employees').status_code == 401
    assert client.post('/api/v1/employees', json=employee_payload('unauth')).status_code == 401


def test_viewer_cannot_write(client_context):
    client, _, viewer, emp_id, _ = client_context
    assert client.get('/api/v1/employees', headers=viewer).status_code == 200
    assert client.post('/api/v1/employees', json=employee_payload(emp_id),headers=viewer).status_code == 403
    assert client.patch(f'/api/v1/employees/{emp_id}', json={'job_title':'Lead'},headers=viewer).status_code == 403
    assert client.delete(f'/api/v1/employees/{emp_id}', headers=viewer).status_code == 403


def test_admin_crud_and_soft_deletion(client_context):
    client, admin, _, emp_id, _ = client_context
    created = client.post('/api/v1/employees', json=employee_payload(emp_id), headers=admin)
    assert created.status_code == 201, created.text
    assert created.json()['sync_status'] == 'pending'
    read = client.get(f'/api/v1/employees/{emp_id}', headers=admin)
    assert read.status_code == 200 and read.json()['first_name'] == 'Api'
    updated = client.patch(f'/api/v1/employees/{emp_id}',json={'job_title':'Data Engineer'}, headers=admin)
    assert updated.status_code == 200 and updated.json()['job_title'] == 'Data Engineer'
    deleted = client.delete(f'/api/v1/employees/{emp_id}', headers=admin)
    assert deleted.status_code == 204, deleted.text
    assert client.get(f'/api/v1/employees/{emp_id}', headers=admin).status_code == 404
    with engine.connect() as c:
        row = c.execute(text('SELECT is_deleted,sync_status FROM api.employees WHERE client_employee_id=:eid'),{'eid':emp_id}).one()
    assert row == (True, 'pending')


def test_duplicate_employee_rejected(client_context):
    client, admin, _, emp_id, _ = client_context
    assert client.post('/api/v1/employees', json=employee_payload(emp_id),headers=admin).status_code == 201
    assert client.post('/api/v1/employees', json=employee_payload(emp_id),headers=admin).status_code == 409


def test_gold_identifier_collision_rejected(client_context):
    client, admin, _, _, gold_emp_id = client_context
    assert client.post('/api/v1/employees', json=employee_payload(gold_emp_id),headers=admin).status_code == 409


def test_invalid_employee_data_rejected(client_context):
    client, admin, _, emp_id, _ = client_context
    bad = employee_payload(emp_id)
    bad['term_date'] = '2025-01-01'
    assert client.post('/api/v1/employees',json=bad,headers=admin).status_code == 422
    bad = employee_payload(emp_id)
    bad['not_a_field'] = 'oops'
    assert client.post('/api/v1/employees',json=bad,headers=admin).status_code == 422


def test_timesheet_read_and_date_filters(client_context):
    client, admin, viewer, _, gold_emp_id = client_context
    route = '/api/v1/timesheets'
    assert client.get(route).status_code == 401
    response = client.get(route, params={'employee_id':gold_emp_id},headers=viewer)
    assert response.status_code == 200, response.text
    assert response.json()['total'] == 1
    assert len(response.json()['items']) == 1
    assert client.get(route,params={'employee_id':gold_emp_id,'start_date':'2099-12-30','end_date':'2099-12-31'},headers=admin).json()['total'] == 1
    assert client.get(route,params={'employee_id':gold_emp_id,'start_date':'2099-01-01','end_date':'2099-01-02'},headers=viewer).json()['total'] == 0
    assert client.get(route,params={'start_date':'2099-12-31','end_date':'2099-01-01'},headers=viewer).status_code == 422
    assert client.post(route,json={'employee_id':gold_emp_id},headers=admin).status_code == 405
    nested = client.get(f'/api/v1/employees/{gold_emp_id}/timesheets',headers=viewer)
    assert nested.status_code == 200 and nested.json()['total'] == 1
