"""
Publish a verified PostgreSQL Gold snapshot to MinIO.

Coordinates:
- Silver and Gold advisory locks
- Consistent PostgreSQL snapshot
- Local Parquet export and validation
- Content-addressed MinIO publication
- Transactional publication registry update
"""

import hashlib
import uuid

from sqlalchemy import text

from src.silver.publish import ENGINE

from src.gold.config import GOLD_MODEL_VERSION

from src.gold.pipeline import (
    verify_silver_publication,
    GOLD_ADVISORY_LOCK_ID,
)

from src.gold.publish import (
    GOLD_EXPORT_ROOT,
    export_gold_parquet,
    verify_gold_export,
    calculate_export_fingerprint,
    upload_gold_snapshot,
)

from src.gold.validation import (
    validate_gold_constraints,
    validate_gold_data,
    validate_employee_profiles,
    validate_dim_date,
)


SILVER_LOCK_ID = 7182401


def calculate_release_fingerprint(
    silver_fingerprint,
    model_version,
    export_fingerprint,
):
    """Bind exported data to its source and model version."""

    payload = (
        f"{silver_fingerprint}\n"
        f"{model_version}\n"
        f"{export_fingerprint}\n"
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def get_gold_registry(connection):
    """Retrieve the current Gold publication record."""

    return connection.execute(
        text("""
            SELECT *
            FROM gold._publication_registry
            WHERE publication_id = 1
        """)
    ).mappings().one_or_none()


def publish_gold():
    """
    Export and publish the current verified Gold database.

    Keep the database snapshot and advisory locks active
    until MinIO verification and registry finalization.

    A failure rolls back the registry update.
    """

    print("=" * 65)
    print("ASE-ETL GOLD PUBLICATION")
    print("=" * 65)

    GOLD_EXPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    # A fresh directory prevents accidental overwrites.
    output_root = (
        GOLD_EXPORT_ROOT
        / f"release_{uuid.uuid4().hex}"
    )

    # All reads and the final registry write belong
    # to one PostgreSQL transaction.
    with ENGINE.begin() as connection:

        connection.execute(
            text(
                "SET TRANSACTION ISOLATION LEVEL "
                "REPEATABLE READ"
            )
        )

        # Acquire locks in the same order as Gold loading.
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock(:lock_id)"
            ),
            {"lock_id": SILVER_LOCK_ID},
        )

        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock(:lock_id)"
            ),
            {"lock_id": GOLD_ADVISORY_LOCK_ID},
        )

        # Verify the active source publication.
        silver = verify_silver_publication(
            connection
        )

        registry = get_gold_registry(connection)

        if registry is None:
            raise RuntimeError(
                "Gold publication registry is empty. "
                "Run the Gold database pipeline first."
            )

        if (
            registry["silver_snapshot_fingerprint"]
            != silver["snapshot_fingerprint"]
        ):
            raise ValueError(
                "Gold was built from a different "
                "Silver publication"
            )

        if (
            registry["gold_model_version"]
            != GOLD_MODEL_VERSION
        ):
            raise ValueError(
                "Gold model version mismatch"
            )

        # Avoid attempting publication against
        # an inconsistent Gold database.
        print("\nVALIDATING GOLD")

        validate_gold_constraints(connection)
        validate_gold_data(connection)
        validate_employee_profiles(connection)
        validate_dim_date(connection)

        counts = {
            "dim_employee": registry["employee_count"],
            "dim_date": registry["date_count"],
            "fact_timesheet": registry["timesheet_count"],
        }

        for dataset, expected in counts.items():
            actual = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM gold.{dataset}"
                )
            ).scalar_one()

            if actual != expected:
                raise ValueError(
                    f"{dataset}: registry count mismatch"
                )

        # A published release can be reused.
        if registry["publication_status"] == "PUBLISHED":
            if not (
                registry["gold_snapshot_fingerprint"]
                and registry["minio_prefix"]
            ):
                raise ValueError(
                    "Published registry is incomplete"
                )

            print("\nGold already published.")
            print(
                "MinIO prefix:",
                registry["minio_prefix"],
            )
            print("No new export required.")
            return

        if registry["publication_status"] != "DB_READY":
            raise ValueError(
                "Gold database is not ready for publication"
            )

        # Export a consistent database snapshot.
        print("\nEXPORTING GOLD")

        exported_counts = export_gold_parquet(
            connection,
            output_root,
        )

        if exported_counts != counts:
            raise ValueError(
                "Gold export counts do not match registry"
            )

        print("\nVERIFYING LOCAL PARQUET")

        verified_counts = verify_gold_export(
            connection,
            output_root,
        )

        if verified_counts != counts:
            raise ValueError(
                "Local Parquet verification failed"
            )

        export_fingerprint = (
            calculate_export_fingerprint(output_root)
        )

        release_fingerprint = calculate_release_fingerprint(
            silver["snapshot_fingerprint"],
            GOLD_MODEL_VERSION,
            export_fingerprint,
        )

        print(
            "\nGold release fingerprint:",
            release_fingerprint,
        )

        # Upload and verify immutable objects.
        print("\nUPLOADING TO MINIO")

        prefix = upload_gold_snapshot(
            output_root=output_root,
            fingerprint=release_fingerprint,
            silver_fingerprint=silver[
                "snapshot_fingerprint"
            ],
            counts=counts,
        )

        # Mark published only after uploads succeed.
        result = connection.execute(
            text("""
                UPDATE gold._publication_registry
                SET
                    publication_status = 'PUBLISHED',
                    gold_snapshot_fingerprint = :fingerprint,
                    minio_prefix = :prefix,
                    published_at = CURRENT_TIMESTAMP
                WHERE publication_id = 1
                  AND publication_status = 'DB_READY'
                  AND silver_snapshot_fingerprint =
                      :silver_fingerprint
                  AND gold_model_version = :model_version
            """),
            {
                "fingerprint": release_fingerprint,
                "prefix": prefix,
                "silver_fingerprint":
                    silver["snapshot_fingerprint"],
                "model_version": GOLD_MODEL_VERSION,
            },
        )

        if result.rowcount != 1:
            raise RuntimeError(
                "Gold registry finalization failed"
            )

    # PostgreSQL commits when the context exits.
    print("\nGOLD PUBLICATION COMPLETED")
    print("Status: PUBLISHED")
    print("MinIO prefix:", prefix)
    print("Employee records:", counts["dim_employee"])
    print("Calendar records:", counts["dim_date"])
    print("Timesheet records:", counts["fact_timesheet"])


if __name__ == "__main__":
    publish_gold()