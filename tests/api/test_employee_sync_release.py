from datetime import datetime, timezone
import hashlib
import pytest
import pandas as pd

from src.api.employee_sync_stage import AUDIT_COLUMNS, _canonical_digest
from src.api.employee_sync_release import compare_audit_with_ledger


def records():
    at = datetime(2026, 10, 17, tzinfo=timezone.utc)
    return [
        dict(change_id=1, client_employee_id="EMP-X", employee_revision=1,
             operation="create", changed_fields={"first_name":"A"}, changed_at=at),
        dict(change_id=3, client_employee_id="EMP-X", employee_revision=2,
             operation="update", changed_fields={"first_name":"B"}, changed_at=at),
    ]


def audit_row(events):
    return pd.DataFrame([dict(client_employee_id="EMP-X", change_id=1,
        employee_revision=1, last_operation="create",
        source_profile_present=False, published_profile_present=True,
        events_sha256=_canonical_digest(events[:1]))], columns=AUDIT_COLUMNS)


def test_accepts_exact_staged_prefix_with_newer_revision_pending():
    events = records()
    assert compare_audit_with_ledger(audit_row(events), events) == {"EMP-X": 1}


def test_rejects_corrupted_audit_digest():
    events = records()
    audit = audit_row(events)
    audit.loc[0,"events_sha256"] = "0"*64
    with pytest.raises(RuntimeError, match="stale or corrupted"):
        compare_audit_with_ledger(audit, events)


def test_rejects_missing_history():
    events = records()
    audit = audit_row(events)
    audit.loc[0,"employee_revision"] = 3
    with pytest.raises(RuntimeError, match="Missing ledger revisions"):
        compare_audit_with_ledger(audit, events)


def test_new_identity_after_staging_remains_unacknowledged():
    events = records() + [
        dict(change_id=4, client_employee_id="EMP-Y", employee_revision=1,
             operation="create", changed_fields={"first_name":"Y"},
             changed_at=events_date())
    ]
    assert compare_audit_with_ledger(audit_row(events), events) == {"EMP-X": 1}


def events_date():
    return datetime(2026,10,17,tzinfo=timezone.utc)
