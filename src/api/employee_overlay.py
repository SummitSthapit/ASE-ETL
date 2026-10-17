"""Pure, deterministic overlay of API employee changes onto cleaned Silver.

This is a staging component, not an independent Silver publication path.
Silver's existing validator, Parquet fingerprint, MinIO/PG publication and
Gold derivation must still run before a change can be called published.
"""

from datetime import date, datetime
import pandas as pd
from sqlalchemy import text

from src.silver.schema_contract import SCHEMA_COLUMNS

EMPLOYEE_COLUMNS = SCHEMA_COLUMNS['employees']
BUSINESS_COLUMNS = set(EMPLOYEE_COLUMNS) - {
    'client_employee_id', 'source_file', 'source_row_number',
    'ingested_at_utc', 'ingestion_batch_id',
}
DATE_COLUMNS = {
    'job_start_date', 'dob', 'hire_date', 'recent_hire_date',
    'anniversary_date', 'term_date',
}


def load_employee_change_events(connection):
    """Read all committed events; NEVER filter solely by a global ID watermark.

    Per-employee revisions make replay deterministic; replaying all events
    prevents missing out-of-order commits from concurrent workers.
    """
    return [dict(r) for r in connection.execute(text("""
        SELECT change_id, client_employee_id, employee_revision,
               operation, changed_fields, changed_at
        FROM api.employee_change_log
        ORDER BY client_employee_id, employee_revision
    """)).mappings()]


def _date_value(key, value):
    if key in DATE_COLUMNS and isinstance(value, str):
        return date.fromisoformat(value)
    return value


def apply_employee_change_events(source: pd.DataFrame, events: list[dict]):
    """Overlay API create/update/delete events while preserving Silver's schema.

    A change applies only fields explicitly changed by the API.
    Source-only attributes, e.g. dob, work_email, and original lineage, remain.
    Deletion removes the employee *profile* from Silver, never timesheet facts.
    """
    if list(source.columns) != EMPLOYEE_COLUMNS:
        raise ValueError('Employee source does not match Silver schema contract')
    if not events:
        return source.copy()
    result = source.copy()
    if result['client_employee_id'].duplicated().any():
        raise ValueError('Silver input contains duplicate employee identities')
    if result['client_employee_id'].isna().any():
        raise ValueError('Silver input contains NULL employee identities')

    # Keep a row dictionary so replay is explicit and never mutates source.
    by_id = {
        row['client_employee_id']: row
        for row in result.to_dict(orient='records')
    }
    revisions = {}
    deleted = set()

    for event in events:
        employee_id = event['client_employee_id']
        revision = int(event['employee_revision'])
        if revision <= revisions.get(employee_id, 0):
            raise ValueError(f'Non-increasing employee revision: {employee_id}')
        if revisions.get(employee_id, 0) and revision != revisions[employee_id] + 1:
            raise ValueError(f'Employee revision gap: {employee_id}')
        revisions[employee_id] = revision
        operation = event['operation']
        patch = event['changed_fields']
        if not isinstance(patch, dict):
            raise ValueError('Ledger patch must be a JSON object')
        unexpected = set(patch) - BUSINESS_COLUMNS - {'is_deleted'}
        if unexpected:
            raise ValueError(f'Unsupported API fields: {sorted(unexpected)}')

        if operation == 'delete':
            deleted.add(employee_id)
            by_id.pop(employee_id, None)
            continue

        if operation not in ('create', 'update'):
            raise ValueError(f'Unsupported event operation: {operation}')
        if employee_id in deleted:
            raise ValueError(f'API edits a deleted employee: {employee_id}')
        if operation == 'update' and employee_id not in by_id:
            raise ValueError(f'API update has no employee profile: {employee_id}')

        if employee_id not in by_id:
            if operation != 'create':
                raise ValueError(f'New employee requires create event: {employee_id}')
            # A new API-created employee must include all Silver columns, with
            # missing optional values represented as NULL, never dropped columns.
            by_id[employee_id] = {column: None for column in EMPLOYEE_COLUMNS}
            by_id[employee_id]['client_employee_id'] = employee_id
            by_id[employee_id]['source_file'] = 'api/employee_change_log'
            by_id[employee_id]['source_row_number'] = int(event['change_id'])
            by_id[employee_id]['ingestion_batch_id'] = f"api-{event['change_id']}"
            by_id[employee_id]['ingested_at_utc'] = event['changed_at']

        for key, value in patch.items():
            if key != 'is_deleted':
                by_id[employee_id][key] = _date_value(key, value)

    # A source employee with no API changes remains bytewise-value-equivalent.
    # The column contract and primary key are preserved.
    if not by_id:
        return source.iloc[0:0].copy()
    frame = pd.DataFrame(list(by_id.values()), columns=EMPLOYEE_COLUMNS)
    if frame['client_employee_id'].duplicated().any():
        raise ValueError('Overlay created a duplicate employee identity')
    for col in DATE_COLUMNS:
        frame[col] = pd.to_datetime(frame[col], errors='raise').dt.date
    frame['ingested_at_utc'] = pd.to_datetime(frame['ingested_at_utc'], utc=True)
    frame['source_row_number'] = pd.to_numeric(frame['source_row_number'], errors='raise').astype('Int64')
    return frame
