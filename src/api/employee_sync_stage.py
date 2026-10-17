"""Stage a consistent operational employee-change snapshot in Silver.

All durable outputs belong to the existing Silver publication. This module
never writes to Silver/Gold PostgreSQL tables or to MinIO directly.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict

import pandas as pd
import numpy as np

from src.api.employee_overlay import (
    apply_employee_change_events,
    load_employee_change_events,
)
from src.silver.schema_contract import SCHEMA_COLUMNS

AUDIT_DATASET = 'audit/api_employee_overrides'
AUDIT_COLUMNS = [
    'client_employee_id',
    'change_id',
    'employee_revision',
    'last_operation',
    'source_profile_present',
    'published_profile_present',
    'events_sha256',
]


def _canonical_digest(events: list[dict]) -> str:
    """Bind the final per-employee revision to every event it incorporates."""
    normalized = [
        {
            'change_id': int(event['change_id']),
            'employee_revision': int(event['employee_revision']),
            'operation': event['operation'],
            'changed_fields': event['changed_fields'],
            'changed_at': event['changed_at'].isoformat(),
        }
        for event in events
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def validate_employee_override_snapshot(
    employees: pd.DataFrame,
    audit: pd.DataFrame,
    source_employee_count: int = 50,
) -> int:
    """Reconcile API profile additions/deletions with the frozen Silver output.

    The original Bronze/source count is checked independently by the Silver
    pipeline; this derived count is for the overlaid output alone.
    """
    if employees.columns.tolist() != SCHEMA_COLUMNS['employees']:
        raise ValueError('Published Silver employee schema differs from contract')
    if audit.columns.tolist() != AUDIT_COLUMNS:
        raise ValueError('API employee override audit schema differs from contract')
    if employees['client_employee_id'].isna().any():
        raise ValueError('NULL Silver employee identity')
    if employees['client_employee_id'].duplicated().any():
        raise ValueError('Duplicate Silver employee identity')
    if audit['client_employee_id'].isna().any() or audit['client_employee_id'].duplicated().any():
        raise ValueError('NULL/duplicate identity in API override audit')
    if source_employee_count < 0:
        raise ValueError('Negative source employee count')

    employee_ids = set(employees['client_employee_id'].tolist())
    expected = source_employee_count
    for record in audit.to_dict(orient='records'):
        if record['last_operation'] not in ('create', 'update', 'delete'):
            raise ValueError('Unsupported API audit operation')
        if int(record['employee_revision']) < 1 or int(record['change_id']) < 1:
            raise ValueError('Invalid API audit revision or change id')
        if not re.fullmatch(r'[0-9a-f]{64}', str(record['events_sha256'])):
            raise ValueError('Invalid API audit event digest')
        source_present = record['source_profile_present']
        published_present = record['published_profile_present']
        if not isinstance(source_present, (bool, np.bool_)) or not isinstance(published_present, (bool, np.bool_)):
            raise ValueError('Invalid API audit presence indicators')
        if pd.isna(source_present) or pd.isna(published_present):
            raise ValueError('NULL API audit presence indicator')
        source_present = bool(source_present)
        published_present = bool(published_present)
        if (record['client_employee_id'] in employee_ids) != published_present:
            raise ValueError('API audit does not match staged employee identities')
        if record['last_operation'] == 'delete' and published_present:
            raise ValueError('Deleted profile remains in staged Silver employees')
        expected += int(published_present) - int(source_present)

    if len(employees) != expected:
        raise ValueError(
            f'API overlay employee reconciliation failed: expected={expected}, actual={len(employees)}'
        )
    return expected


def stage_employee_overlay(
    source: pd.DataFrame,
    events: list[dict] | None = None,
    expected_source_count: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Capture committed ledger events once and build two frozen outputs.

    API writes committed *after* event capture are not part of this snapshot;
    they remain pending for the following run. Replay always starts from the
    entire committed event history, not a global change_id cutoff.
    """
    if source.columns.tolist() != SCHEMA_COLUMNS['employees']:
        raise ValueError('Source employees violate approved Silver schema')
    if len(source) != expected_source_count:
        raise ValueError(
            f'Bronze employee baseline changed: expected={expected_source_count}, got={len(source)}'
        )

    if events is None:
        # No reusable connection held during parquet export; this SELECT sees
        # one PostgreSQL statement snapshot of committed events.
        from src.api.database import engine
        with engine.connect() as connection:
            events = load_employee_change_events(connection)

    output = apply_employee_change_events(source, events)
    by_employee: dict[str, list[dict]] = defaultdict(list)
    source_ids = set(source['client_employee_id'].tolist())
    for event in events:
        by_employee[event['client_employee_id']].append(event)

    rows = []
    output_ids = set(output['client_employee_id'].tolist())
    for employee_id in sorted(by_employee):
        history = by_employee[employee_id]
        if [int(item['employee_revision']) for item in history] != list(range(1, len(history) + 1)):
            raise ValueError(f'Employee revision gap in audit snapshot: {employee_id}')
        if history[0]['operation'] == 'create' and employee_id in source_ids:
            raise ValueError('API create conflicts with a Bronze employee identity')
        last = history[-1]
        rows.append({
            'client_employee_id': employee_id,
            'change_id': int(last['change_id']),
            'employee_revision': int(last['employee_revision']),
            'last_operation': last['operation'],
            'source_profile_present': employee_id in source_ids,
            'published_profile_present': employee_id in output_ids,
            'events_sha256': _canonical_digest(history),
        })

    audit = pd.DataFrame(rows, columns=AUDIT_COLUMNS)
    validate_employee_override_snapshot(output, audit, expected_source_count)
    return output, audit
