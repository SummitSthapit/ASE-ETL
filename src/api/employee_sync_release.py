"""Controlled Silver -> Gold employee synchronization.

Run ONLY with production development DATABASE_URL (ase_etl), not the test DB.
One orchestrator invocation should be run at a time; see README.
"""
from collections import defaultdict
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.api.employee_overlay import load_employee_change_events
from src.api.employee_sync_stage import (
    AUDIT_DATASET, AUDIT_COLUMNS, _canonical_digest,
)
from src.api.database import engine
from src.silver.publish import dataset_files
from src.silver.publish_idempotent import calculate_snapshot_fingerprint


def read_staged_audit():
    files = dataset_files(AUDIT_DATASET)
    audit = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    if audit.columns.tolist() != AUDIT_COLUMNS:
        raise RuntimeError("Unexpected employee override audit schema")
    if audit.client_employee_id.duplicated().any():
        raise RuntimeError("Duplicate employee override audit identities")
    return audit


def compare_audit_with_ledger(audit, events):
    """Audit MUST describe an authentic prefix of every employee's history.

    Changes committed since staging may exist: they remain pending.
    A global change_id cutoff is intentionally not used (commit ordering risk).
    """
    grouped = defaultdict(list)
    for event in events:
        grouped[event["client_employee_id"]].append(event)
    pinned = {}
    for item in audit.to_dict("records"):
        key = item["client_employee_id"]
        revision = int(item["employee_revision"])
        history = [e for e in grouped[key] if int(e["employee_revision"]) <= revision]
        if len(history) != revision or [int(e["employee_revision"]) for e in history] != list(range(1, revision + 1)):
            raise RuntimeError(f"Missing ledger revisions for {key}")
        last = history[-1]
        if (int(last["change_id"]) != int(item["change_id"])
                or last["operation"] != item["last_operation"]
                or _canonical_digest(history) != item["events_sha256"]):
            raise RuntimeError(f"Staged employee audit is stale or corrupted for {key}")
        pinned[key] = revision
    # Events for identities created AFTER staging are not included here;
    # they remain pending and will be included in the next publication.
    return pinned


def check_staged_ledger(audit):
    with engine.connect() as connection:
        return compare_audit_with_ledger(audit, load_employee_change_events(connection))


def acknowledge_published_changes(expected_fingerprint, audit):
    """Acknowledge only current revisions, AFTER matching Gold MinIO publication."""
    with engine.begin() as connection:
        # Match lock order of the warehouse, preventing a newer Silver/Gold
        # publication while checking the publication registries.
        connection.execute(text("SELECT pg_advisory_xact_lock(7182401)"))
        connection.execute(text("SELECT pg_advisory_xact_lock(7182402)"))
        silver = connection.execute(text("""
            SELECT snapshot_fingerprint FROM silver._publication_registry
            WHERE publication_id = 1
        """)).scalar_one_or_none()
        gold = connection.execute(text("""
            SELECT silver_snapshot_fingerprint, publication_status,
                   gold_snapshot_fingerprint, minio_prefix
            FROM gold._publication_registry WHERE publication_id = 1
        """)).mappings().one_or_none()
        if silver != expected_fingerprint or not gold or (
            gold["silver_snapshot_fingerprint"] != expected_fingerprint
            or gold["publication_status"] != "PUBLISHED"
            or not gold["gold_snapshot_fingerprint"]
            or not gold["minio_prefix"]
        ):
            raise RuntimeError("Cannot acknowledge: matching published Gold release not verified")

        pinned = compare_audit_with_ledger(audit, load_employee_change_events(connection))
        synced = 0
        pending_newer = 0
        for key, revision in pinned.items():
            result = connection.execute(text("""
                UPDATE api.employees
                SET sync_status = 'synced'
                WHERE client_employee_id = :id
                  AND revision = :revision
                  AND sync_status <> 'synced'
            """), {"id": key, "revision": revision})
            synced += result.rowcount
            if result.rowcount == 0:
                newest = connection.execute(text("""
                    SELECT revision, sync_status FROM api.employees
                    WHERE client_employee_id = :id
                """), {"id": key}).mappings().one_or_none()
                if not newest:
                    raise RuntimeError(f"Operational employee disappeared: {key}")
                if newest["revision"] != revision:
                    pending_newer += 1
        return synced, pending_newer


def main():
    from src.silver.pipeline import main as stage_silver
    from src.silver.publish_idempotent import main as publish_silver
    from src.gold.pipeline import run_gold_pipeline
    from src.gold.release import publish_gold

    print("ASE-ETL EMPLOYEE SYNC: SILVER -> GOLD")
    # No publisher should run independently at the same time as this workflow.
    # A single process re-stages immediately before publication.
    stage_silver()
    audit = read_staged_audit()
    check_staged_ledger(audit)
    expected_fingerprint = calculate_snapshot_fingerprint()

    publish_silver()
    # Abort if another process modified the local Silver snapshot.
    if calculate_snapshot_fingerprint() != expected_fingerprint:
        raise RuntimeError("Local Silver snapshot mutated during publication")

    run_gold_pipeline()
    publish_gold()
    synced, pending_newer = acknowledge_published_changes(expected_fingerprint, audit)
    print(f"PASS: Matching Silver/Gold publication. Synced changes: {synced}; newer revisions left pending: {pending_newer}")


if __name__ == "__main__":
    main()
