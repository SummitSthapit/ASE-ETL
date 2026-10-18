"""Acknowledge API changes after verified Gold publication."""

from src.api.employee_sync_release import (
    read_staged_audit,
    check_staged_ledger,
    acknowledge_published_changes,
)

from src.silver.publish_idempotent import (
    calculate_snapshot_fingerprint,
)


def main():
    print("=" * 65)
    print("ASE-ETL API EMPLOYEE SYNC ACKNOWLEDGEMENT")
    print("=" * 65)

    # Read the employee revisions used during Silver staging.
    audit = read_staged_audit()

    # Confirm the staged revisions belong to the API ledger.
    check_staged_ledger(audit)

    # Identify the Silver snapshot already published.
    expected_fingerprint = calculate_snapshot_fingerprint()

    # This function verifies that Gold was published from
    # the matching Silver snapshot before acknowledging changes.
    synced, pending_newer = acknowledge_published_changes(
        expected_fingerprint,
        audit,
    )

    print(f"Employee revisions acknowledged: {synced}")
    print(f"Newer revisions left pending: {pending_newer}")
    print("PASS: API employee synchronization acknowledgement")


if __name__ == "__main__":
    main()