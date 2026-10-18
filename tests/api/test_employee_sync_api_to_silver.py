"""End-to-end API writes -> durable ledger -> Silver overlay, isolated to ase_etl_test.

Does not publish PostgreSQL Silver/Gold or upload MinIO objects.
"""
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.database import engine
from src.api.employee_change_migrations import migrate
from src.api.employee_overlay import load_employee_change_events
from src.api.employee_sync_stage import stage_employee_overlay, validate_employee_override_snapshot
from src.api.main import app
from src.api.security import create_access_token, hash_password
from src.silver.schema_contract import SCHEMA_COLUMNS


def assert_test_db():
    with engine.connect() as c:
        db = c.execute(text('SELECT current_database()')).scalar_one()
    if db != 'ase_etl_test':
        raise RuntimeError(f'REFUSING TEST WRITES: connected to {db}, expected ase_etl_test')


@pytest.fixture(scope='module', autouse=True)
def prerequisites():
    assert_test_db()
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
        c.execute(text('''CREATE TABLE IF NOT EXISTS gold.dim_employee (
            employee_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            client_employee_id VARCHAR NOT NULL UNIQUE
        )'''))
    migrate()
    yield


@pytest.fixture
def api_context():
    assert_test_db()
    tag = uuid4().hex[:14]
    employee_id = f'SYNC-E2E-{tag}'
    username = f'sync_e2e_admin_{tag}'
    with engine.begin() as c:
        user_id = c.execute(text('''INSERT INTO api.users
            (username, hashed_password, role, is_active, token_version)
            VALUES (:name,:pw,'admin',TRUE,0) RETURNING user_id'''),
            {'name': username, 'pw': hash_password('isolated-test-user-only')}).scalar_one()
    token = create_access_token(user_id, 0)
    headers = {'Authorization': f'Bearer {token}'}
    try:
        with TestClient(app) as client:
            yield client, headers, employee_id
    finally:
        assert_test_db()
        with engine.begin() as c:
            c.execute(text('DELETE FROM api.employee_change_log WHERE client_employee_id=:id'), {'id': employee_id})
            c.execute(text('DELETE FROM api.employees WHERE client_employee_id=:id'), {'id': employee_id})
            c.execute(text('DELETE FROM api.auth_events WHERE user_id=:uid'), {'uid': user_id})
            c.execute(text('DELETE FROM api.users WHERE user_id=:uid'), {'uid': user_id})


def events_for(employee_id):
    with engine.connect() as c:
        return [r for r in load_employee_change_events(c)
                if r['client_employee_id'] == employee_id]


def source_frame():
    # One unrelated Bronze employee, so API creation must increase count by one.
    row = {column: None for column in SCHEMA_COLUMNS['employees']}
    row.update(client_employee_id='BRONZE-UNCHANGED', first_name='Original',
               last_name='Source', work_email='protected@example.invalid',
               source_file='employee_batch/source.csv', source_row_number=1,
               ingestion_batch_id='bronze-source-1',
               ingested_at_utc=datetime(2025, 10, 16, tzinfo=timezone.utc))
    return pd.DataFrame([row], columns=SCHEMA_COLUMNS['employees'])


def stage(source, employee_id):
    return stage_employee_overlay(source, events=events_for(employee_id),
                                  expected_source_count=len(source))


def test_actual_api_create_update_delete_replays_to_silver(api_context):
    client, headers, employee_id = api_context
    source = source_frame()

    response = client.post('/api/v1/employees', headers=headers, json={
        'client_employee_id': employee_id, 'first_name': 'Api',
        'last_name': 'Test', 'department_name': 'Operations',
        'hire_date': '2026-01-02'
    })
    assert response.status_code == 201, response.text
    assert response.json()['sync_status'] == 'pending'
    added, audit = stage(source, employee_id)
    assert len(added) == 2
    assert audit.iloc[0]['employee_revision'] == 1
    assert added.set_index('client_employee_id').loc[employee_id, 'first_name'] == 'Api'
    assert validate_employee_override_snapshot(added, audit, 1) == 2

    response = client.patch(f'/api/v1/employees/{employee_id}', headers=headers,
                            json={'department_name': 'Data Engineering'})
    assert response.status_code == 200, response.text
    updated, updated_audit = stage(source, employee_id)
    assert updated_audit.iloc[0]['employee_revision'] == 2
    assert updated.set_index('client_employee_id').loc[employee_id, 'department_name'] == 'Data Engineering'
    assert updated.set_index('client_employee_id').loc['BRONZE-UNCHANGED', 'work_email'] == 'protected@example.invalid'
    assert len(updated) == 2

    response = client.delete(f'/api/v1/employees/{employee_id}', headers=headers)
    assert response.status_code == 204, response.text
    assert client.get(f'/api/v1/employees/{employee_id}', headers=headers).status_code == 404
    removed, removed_audit = stage(source, employee_id)
    assert removed_audit.iloc[0]['employee_revision'] == 3
    assert removed_audit.iloc[0]['last_operation'] == 'delete'
    assert removed.client_employee_id.tolist() == ['BRONZE-UNCHANGED']
    assert validate_employee_override_snapshot(removed, removed_audit, 1) == 1
    assert [event['operation'] for event in events_for(employee_id)] == ['create','update','delete']
    with engine.connect() as c:
        row = c.execute(text('''SELECT revision, is_deleted, sync_status FROM api.employees
                                WHERE client_employee_id=:id'''), {'id': employee_id}).one()
    assert row == (3, True, 'pending')


def test_revision_is_frozen_when_new_api_write_arrives(api_context):
    client, headers, employee_id = api_context
    source = source_frame()
    created = client.post('/api/v1/employees', headers=headers, json={
        'client_employee_id': employee_id, 'first_name': 'First', 'last_name': 'Version'
    })
    assert created.status_code == 201, created.text
    staged, frozen_audit = stage(source, employee_id)
    assert int(frozen_audit.iloc[0]['employee_revision']) == 1
    response = client.patch(f'/api/v1/employees/{employee_id}', headers=headers,
                            json={'first_name': 'Second'})
    assert response.status_code == 200, response.text
    # A staged snapshot remains stable: a newer revision belongs to the next run.
    assert staged.set_index('client_employee_id').loc[employee_id, 'first_name'] == 'First'
    assert frozen_audit.iloc[0]['employee_revision'] == 1
    new_staged, new_audit = stage(source, employee_id)
    assert new_audit.iloc[0]['employee_revision'] == 2
    assert new_staged.set_index('client_employee_id').loc[employee_id, 'first_name'] == 'Second'
