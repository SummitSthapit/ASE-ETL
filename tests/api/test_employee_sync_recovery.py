
"""Test synchronization acknowledgment and publication failure recovery.

All database writes are restricted to ase_etl_test.
Publication registry entries are simulated; no MinIO uploads occur.
"""

import importlib
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from src.api.database import engine
from src.api.employee_sync_release import acknowledge_published_changes
from src.api.employee_sync_stage import stage_employee_overlay
from src.gold.migrations import ensure_gold_registry
from src.silver.publish_idempotent import ensure_registry

from test_employee_sync_api_to_silver import (
    prerequisites,
    api_context,
    assert_test_db,
    events_for,
    source_frame,
)


SILVER_A = "a" * 64
SILVER_B = "b" * 64
GOLD_HASH = "c" * 64


@pytest.fixture
def fake_registries():
    """Own disposable publication registry rows in the test DB."""

    assert_test_db()

    with engine.begin() as connection:
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS silver")
        )
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS gold")
        )

        ensure_registry(connection)
        ensure_gold_registry(connection)

        for table in (
            "silver._publication_registry",
            "gold._publication_registry",
        ):
            existing = connection.execute(
                text(
                    f"SELECT 1 FROM {table} "
                    "WHERE publication_id = 1"
                )
            ).scalar_one_or_none()

            if existing is not None:
                raise RuntimeError(
                    f"Test registry is not empty: {table}"
                )

    def configure(
        status="DB_READY",
        fingerprint=SILVER_A,
        gold_source=None,
    ):
        assert_test_db()

        source = gold_source or fingerprint
        published = status == "PUBLISHED"

        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO silver._publication_registry (
                        publication_id,
                        snapshot_fingerprint,
                        minio_prefix,
                        schema_version,
                        employee_count,
                        timesheet_count,
                        published_at
                    )
                    VALUES (
                        1, :fingerprint, :prefix,
                        '2.0', 0, 0, NOW()
                    )
                    ON CONFLICT (publication_id)
                    DO UPDATE SET
                        snapshot_fingerprint =
                            EXCLUDED.snapshot_fingerprint,
                        minio_prefix = EXCLUDED.minio_prefix
                """),
                {
                    "fingerprint": fingerprint,
                    "prefix": f"test-only/{fingerprint}",
                },
            )

            connection.execute(
                text("""
                    INSERT INTO gold._publication_registry (
                        publication_id,
                        silver_snapshot_fingerprint,
                        gold_model_version,
                        employee_count,
                        date_count,
                        timesheet_count,
                        gold_snapshot_fingerprint,
                        minio_prefix,
                        publication_status,
                        database_committed_at,
                        published_at
                    )
                    VALUES (
                        1, :source, '1.0',
                        0, 0, 0,
                        :gold_fp, :gold_prefix,
                        :status, NOW(), :published_at
                    )
                    ON CONFLICT (publication_id)
                    DO UPDATE SET
                        silver_snapshot_fingerprint =
                            EXCLUDED.silver_snapshot_fingerprint,
                        gold_snapshot_fingerprint =
                            EXCLUDED.gold_snapshot_fingerprint,
                        minio_prefix = EXCLUDED.minio_prefix,
                        publication_status =
                            EXCLUDED.publication_status,
                        published_at = EXCLUDED.published_at
                """),
                {
                    "source": source,
                    "gold_fp": GOLD_HASH if published else None,
                    "gold_prefix": (
                        "test-only/gold" if published else None
                    ),
                    "status": status,
                    "published_at": (
                        datetime.now(timezone.utc)
                        if published else None
                    ),
                },
            )

    try:
        yield configure

    finally:
        assert_test_db()

        with engine.begin() as connection:
            connection.execute(
                text("""
                    DELETE FROM gold._publication_registry
                    WHERE publication_id = 1
                """)
            )

            connection.execute(
                text("""
                    DELETE FROM silver._publication_registry
                    WHERE publication_id = 1
                """)
            )


def get_status(employee_id):
    with engine.connect() as connection:
        return connection.execute(
            text("""
                SELECT revision, sync_status
                FROM api.employees
                WHERE client_employee_id = :employee_id
            """),
            {"employee_id": employee_id},
        ).one()


def snapshot(employee_id):
    _, audit = stage_employee_overlay(
        source_frame(),
        events=events_for(employee_id),
        expected_source_count=1,
    )
    return audit


def test_failed_publication_retry_and_newer_revision(
    api_context,
    fake_registries,
):
    client, headers, employee_id = api_context

    response = client.post(
        "/api/v1/employees",
        headers=headers,
        json={
            "client_employee_id": employee_id,
            "first_name": "Initial",
            "last_name": "Test",
        },
    )

    assert response.status_code == 201, response.text

    audit_v1 = snapshot(employee_id)
    assert get_status(employee_id) == (1, "pending")

    # 1. Gold database exists but publication has not finished.
    fake_registries(status="DB_READY")

    with pytest.raises(RuntimeError, match="Cannot acknowledge"):
        acknowledge_published_changes(SILVER_A, audit_v1)

    assert get_status(employee_id) == (1, "pending")

    # 2. A published Gold registry points to the wrong Silver snapshot.
    fake_registries(
        status="PUBLISHED",
        gold_source=SILVER_B,
    )

    with pytest.raises(RuntimeError, match="Cannot acknowledge"):
        acknowledge_published_changes(SILVER_A, audit_v1)

    assert get_status(employee_id) == (1, "pending")

    # 3. Simulate a matching published registry after successful retry.
    fake_registries(status="PUBLISHED")

    assert acknowledge_published_changes(
        SILVER_A, audit_v1
    ) == (1, 0)

    assert get_status(employee_id) == (1, "synced")

    # 4. Repeated acknowledgment is idempotent.
    assert acknowledge_published_changes(
        SILVER_A, audit_v1
    ) == (0, 0)

    # 5. A new API revision must not be acknowledged by the old audit.
    response = client.patch(
        f"/api/v1/employees/{employee_id}",
        headers=headers,
        json={"first_name": "Updated"},
    )

    assert response.status_code == 200, response.text
    assert get_status(employee_id) == (2, "pending")

    assert acknowledge_published_changes(
        SILVER_A, audit_v1
    ) == (0, 1)

    assert get_status(employee_id) == (2, "pending")

    # 6. A newer staged snapshot can acknowledge revision 2.
    audit_v2 = snapshot(employee_id)

    assert int(audit_v2.iloc[0]["employee_revision"]) == 2

    fake_registries(
        status="PUBLISHED",
        fingerprint=SILVER_B,
    )

    assert acknowledge_published_changes(
        SILVER_B, audit_v2
    ) == (1, 0)

    assert get_status(employee_id) == (2, "synced")


def test_orchestrator_never_acknowledges_when_gold_upload_fails(
    monkeypatch,
):
    """Inject a Gold publication failure and check execution order."""

    release = importlib.import_module(
        "src.api.employee_sync_release"
    )
    silver_pipeline = importlib.import_module(
        "src.silver.pipeline"
    )
    silver_publisher = importlib.import_module(
        "src.silver.publish_idempotent"
    )
    gold_pipeline = importlib.import_module(
        "src.gold.pipeline"
    )
    gold_release = importlib.import_module(
        "src.gold.release"
    )

    calls = []

    monkeypatch.setattr(
        silver_pipeline,
        "main",
        lambda: calls.append("stage"),
    )

    monkeypatch.setattr(
        release,
        "read_staged_audit",
        lambda: object(),
    )

    monkeypatch.setattr(
        release,
        "check_staged_ledger",
        lambda _: calls.append("check"),
    )

    monkeypatch.setattr(
        release,
        "calculate_snapshot_fingerprint",
        lambda: SILVER_A,
    )

    monkeypatch.setattr(
        silver_publisher,
        "main",
        lambda: calls.append("silver"),
    )

    monkeypatch.setattr(
        gold_pipeline,
        "run_gold_pipeline",
        lambda: calls.append("gold-db"),
    )

    def failed_gold_upload():
        calls.append("gold-upload-failed")
        raise RuntimeError("injected upload failure")

    monkeypatch.setattr(
        gold_release,
        "publish_gold",
        failed_gold_upload,
    )

    monkeypatch.setattr(
        release,
        "acknowledge_published_changes",
        lambda *_: pytest.fail(
            "Acknowledgment ran after failed Gold upload"
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="injected upload failure",
    ):
        release.main()

    assert calls == [
        "stage",
        "check",
        "silver",
        "gold-db",
        "gold-upload-failed",
    ]
