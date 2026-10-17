"""Isolated tests for operational change capture and Silver overlay.

Only executes database-changing tests in ase_etl_test.
"""
from datetime import date, datetime, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import text

from src.api.database import engine
from src.api.employee_change_migrations import migrate
from src.api.employee_overlay import apply_employee_change_events
from src.silver.schema_contract import SCHEMA_COLUMNS


def ensure_test_db():
    with engine.connect() as c:
        name = c.execute(text('SELECT current_database()')).scalar_one()
    if name != 'ase_etl_test':
        raise RuntimeError(f'REFUSING WRITES TO {name!r}; expected ase_etl_test')


@pytest.fixture(scope='module', autouse=True)
def migrated_test_schema():
    ensure_test_db()
    with engine.begin() as c:
        c.execute(text('CREATE SCHEMA IF NOT EXISTS api'))
        # Reuse the same operational employee column subset as Phase 7.5.
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
    migrate()
    yield


@pytest.fixture
def unique_id():
    ensure_test_db()
    employee_id = 'LEDGER-TEST-' + uuid4().hex[:14]
    yield employee_id
    ensure_test_db()
    with engine.begin() as c:
        c.execute(text('DELETE FROM api.employee_change_log WHERE client_employee_id=:id'), {'id': employee_id})
        c.execute(text('DELETE FROM api.employees WHERE client_employee_id=:id'), {'id': employee_id})


def events_for(employee_id):
    with engine.connect() as c:
        return [dict(r) for r in c.execute(text('''
            SELECT employee_revision, operation, changed_fields, actor_user_id
            FROM api.employee_change_log WHERE client_employee_id=:id
            ORDER BY employee_revision
        '''), {'id': employee_id}).mappings()]


def test_create_update_delete_log_is_versioned(unique_id):
    with engine.begin() as c:
        c.execute(text('''INSERT INTO api.employees (
            client_employee_id, first_name, last_name, department_name, sync_status
        ) VALUES (:id,'Demo','Employee','Sales','pending')'''), {'id': unique_id})
        c.execute(text('''UPDATE api.employees SET department_name='Engineering',
            sync_status='pending' WHERE client_employee_id=:id'''), {'id': unique_id})
        c.execute(text('''UPDATE api.employees SET is_deleted=TRUE,
            sync_status='pending' WHERE client_employee_id=:id'''), {'id': unique_id})
    events = events_for(unique_id)
    assert [e['employee_revision'] for e in events] == [1, 2, 3]
    assert [e['operation'] for e in events] == ['create', 'update', 'delete']
    assert events[0]['changed_fields']['department_name'] == 'Sales'
    assert events[1]['changed_fields'] == {'department_name': 'Engineering'}
    assert events[2]['changed_fields'] == {'is_deleted': True}
    with engine.connect() as c:
        row = c.execute(text('''SELECT revision, sync_status, is_deleted
            FROM api.employees WHERE client_employee_id=:id'''), {'id': unique_id}).one()
    assert row == (3, 'pending', True)


def test_bootstrap_and_status_changes_have_no_business_events(unique_id):
    with engine.begin() as c:
        c.execute(text('''INSERT INTO api.employees (
            client_employee_id, first_name, last_name, sync_status
        ) VALUES (:id,'Demo','Employee','synced')'''), {'id': unique_id})
        c.execute(text('''UPDATE api.employees SET first_name='Demo'
            WHERE client_employee_id=:id'''), {'id': unique_id})
        c.execute(text('''UPDATE api.employees SET sync_status='pending'
            WHERE client_employee_id=:id'''), {'id': unique_id})
    assert events_for(unique_id) == []
    with engine.connect() as c:
        row = c.execute(text('''SELECT revision, sync_status FROM api.employees
            WHERE client_employee_id=:id'''), {'id': unique_id}).one()
    assert row == (0, 'synced')
    with engine.begin() as c:
        c.execute(text('''UPDATE api.employees SET last_name='Changed'
            WHERE client_employee_id=:id'''), {'id': unique_id})
        c.execute(text('''UPDATE api.employees SET sync_status='synced'
            WHERE client_employee_id=:id'''), {'id': unique_id})
    assert [(e['employee_revision'], e['operation']) for e in events_for(unique_id)] == [(1, 'update')]


def test_overlay_preserves_untouched_source_fields():
    columns = SCHEMA_COLUMNS['employees']
    base = []
    for employee_id in ['OLD', 'REMOVE']:
        row = {column: None for column in columns}
        row.update({
            'client_employee_id': employee_id,
            'first_name': 'Source',
            'last_name': 'Record',
            'department_name': 'Original',
            'work_email': 'historical@example.invalid',
            'dob': date(1990, 5, 20),
            'source_file': 'bronze/employee.csv',
            'source_row_number': 1 if employee_id == 'OLD' else 2,
            'ingested_at_utc': datetime(2026, 1, 1, tzinfo=timezone.utc),
            'ingestion_batch_id': 'bronze-1',
        })
        base.append(row)
    source = pd.DataFrame(base, columns=columns)
    now = datetime(2026, 10, 17, tzinfo=timezone.utc)
    changes = [
        dict(change_id=100, client_employee_id='OLD', employee_revision=1,
             operation='update', changed_fields={'department_name':'New Department'}, changed_at=now),
        dict(change_id=101, client_employee_id='REMOVE', employee_revision=1,
             operation='delete', changed_fields={'is_deleted':True}, changed_at=now),
        dict(change_id=102, client_employee_id='CREATED', employee_revision=1,
             operation='create', changed_fields={'first_name':'Api', 'last_name':'Created',
                                                 'hire_date':'2026-10-17'}, changed_at=now),
    ]
    result = apply_employee_change_events(source, changes).set_index('client_employee_id')
    assert set(result.index) == {'OLD', 'CREATED'}
    assert result.loc['OLD', 'department_name'] == 'New Department'
    assert result.loc['OLD', 'work_email'] == 'historical@example.invalid'
    assert result.loc['OLD', 'dob'] == date(1990, 5, 20)
    assert result.loc['OLD', 'source_file'] == 'bronze/employee.csv'
    assert result.loc['CREATED', 'hire_date'] == date(2026, 10, 17)
    assert result.loc['CREATED', 'source_file'] == 'api/employee_change_log'
    assert list(result.columns) == [x for x in columns if x != 'client_employee_id']
    # No event means the original source is unchanged.
    assert apply_employee_change_events(source, []).equals(source)
