"""Pure tests: no database, MinIO, or local Silver output modifications."""
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from src.api.employee_sync_stage import (
    AUDIT_COLUMNS,
    stage_employee_overlay,
    validate_employee_override_snapshot,
)
from src.silver.schema_contract import SCHEMA_COLUMNS


def source_frame():
    rows = []
    for idx, employee_id in enumerate(('SOURCE_A', 'SOURCE_B'), start=1):
        row = {name: None for name in SCHEMA_COLUMNS['employees']}
        row.update({
            'client_employee_id': employee_id,
            'first_name': 'Source',
            'last_name': 'Profile',
            'work_email': 'preserve@example.invalid',
            'dob': date(1995, 1, 1),
            'source_file': 'bronze/employee.csv',
            'source_row_number': idx,
            'ingested_at_utc': datetime(2026, 1, 1, tzinfo=timezone.utc),
            'ingestion_batch_id': 'original',
        })
        rows.append(row)
    return pd.DataFrame(rows, columns=SCHEMA_COLUMNS['employees'])


def event(change_id, employee_id, revision, operation, fields):
    return {
        'change_id': change_id,
        'client_employee_id': employee_id,
        'employee_revision': revision,
        'operation': operation,
        'changed_fields': fields,
        'changed_at': datetime(2026, 10, 17, tzinfo=timezone.utc),
    }


def test_stage_create_update_delete_and_reconcile():
    base = source_frame()
    events = [
        event(1, 'SOURCE_A', 1, 'update', {'department_name': 'Engineering'}),
        event(2, 'SOURCE_B', 1, 'delete', {'is_deleted': True}),
        event(3, 'NEW_API', 1, 'create', {'first_name': 'New', 'last_name': 'Profile'}),
    ]
    output, audit = stage_employee_overlay(base, events, expected_source_count=2)
    assert set(output['client_employee_id']) == {'SOURCE_A', 'NEW_API'}
    assert audit.columns.tolist() == AUDIT_COLUMNS
    assert len(audit) == 3
    assert validate_employee_override_snapshot(output, audit, 2) == 2
    old = output.set_index('client_employee_id').loc['SOURCE_A']
    assert old['work_email'] == 'preserve@example.invalid'
    assert old['dob'] == date(1995, 1, 1)
    assert old['department_name'] == 'Engineering'
    assert len(audit.loc[audit['client_employee_id'] == 'SOURCE_B']) == 1


def test_no_changes_returns_original_source_and_empty_audit():
    base = source_frame()
    output, audit = stage_employee_overlay(base, [], expected_source_count=2)
    assert output.equals(base)
    assert audit.empty
    assert audit.columns.tolist() == AUDIT_COLUMNS
    assert validate_employee_override_snapshot(output, audit, 2) == 2


def test_invalid_audit_detected_before_publication():
    base = source_frame()
    output, audit = stage_employee_overlay(
        base,
        [event(1, 'SOURCE_A', 1, 'delete', {'is_deleted': True})],
        expected_source_count=2,
    )
    corrupt = audit.copy()
    corrupt.loc[0, 'published_profile_present'] = True
    with pytest.raises(ValueError, match='audit does not match'):
        validate_employee_override_snapshot(output, corrupt, 2)


def test_bronze_source_count_guard():
    with pytest.raises(ValueError, match='Bronze employee baseline changed'):
        stage_employee_overlay(source_frame(), [], expected_source_count=50)


def test_replay_is_deterministic():
    base = source_frame()
    events = [
        event(10, 'SOURCE_A', 1, 'update', {'department_name': 'Team A'}),
        event(12, 'SOURCE_A', 2, 'update', {'department_name': 'Team B'}),
    ]
    first, first_audit = stage_employee_overlay(base, events, expected_source_count=2)
    second, second_audit = stage_employee_overlay(base, events, expected_source_count=2)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(first_audit, second_audit)
    assert first_audit.iloc[0]['employee_revision'] == 2
