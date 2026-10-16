
"""Export validated ASE-ETL analytics views to CSV.

Run:
    python -m src.analytics.reporting
"""

import csv
import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from src.silver.publish import ENGINE

from src.analytics.pipeline import verify_gold_publication

REPORTS = {
    "01_monthly_headcount_turnover": "mart.workforce_monthly",
    "02_department_tenure": "mart.department_tenure",
    "03_employee_working_hours": "mart.kpi_employee_working_hours",
    "04_daily_attendance": "mart.employee_attendance_daily",
    "05_daily_overtime": "mart.employee_overtime_daily",
    "06_rolling_working_hours": "mart.kpi_rolling_working_hours",
    "07_early_attrition": "mart.kpi_early_attrition",
    "08_monthly_workload": "mart.kpi_monthly_workload",
    "09_department_workload": "mart.kpi_department_workload",
    "10_attendance_compliance": "mart.kpi_attendance_compliance",
    "11_department_overtime": "mart.kpi_department_overtime",
    "12_weekday_workload": "mart.kpi_weekday_workload",
    "13_workforce_changes": "mart.kpi_workforce_changes",
}

OUTPUT_ROOT = Path("reports") / "analytics"
BATCH_SIZE = 5000

def validate_publication(connection):
    """Reuse the verified Gold publication checks."""

    publication = verify_gold_publication(connection)

    return dict(publication)

def export_view(connection, view_name, destination):
    """Stream a SQL view to CSV without loading all rows."""

    result = connection.execution_options(
        stream_results=True
    ).execute(text(f"SELECT * FROM {view_name}"))

    columns = list(result.keys())
    row_count = 0

    try:
        with destination.open(
            "w", newline="", encoding="utf-8-sig"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(columns)

            while True:
                batch = result.fetchmany(BATCH_SIZE)

                if not batch:
                    break

                writer.writerows(batch)
                row_count += len(batch)
    finally:
        result.close()

    digest = hashlib.sha256()

    with destination.open("rb") as file:
        for block in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            digest.update(block)

    return {
        "source_view": view_name,
        "file": destination.name,
        "rows": row_count,
        "columns": columns,
        "sha256": digest.hexdigest(),
        "bytes": destination.stat().st_size,
    }


def main():
    print("=" * 65)
    print("ASE-ETL AUTOMATED ANALYTICS REPORTING")
    print("=" * 65)

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid.uuid4().hex[:8]
    )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    staging = OUTPUT_ROOT / f".staging_{run_id}"
    destination = OUTPUT_ROOT / run_id

    staging.mkdir()

    try:
        with ENGINE.connect() as connection:
            # All reports read from the same database snapshot.
            connection.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL "
                "REPEATABLE READ, READ ONLY"
            )

            publication = validate_publication(connection)

            manifest = {
                "run_id": run_id,
                "generated_at_utc": datetime.now(
                    timezone.utc
                ).isoformat(),
                "source": "PostgreSQL Gold / Analytics Mart",
                "publication": publication,
                "reports": {},
            }

            for report_name, view_name in REPORTS.items():
                filename = f"{report_name}.csv"

                metadata = export_view(
                    connection,
                    view_name,
                    staging / filename,
                )

                manifest["reports"][report_name] = metadata

                print(
                    f"PASS: {report_name} "
                    f"— {metadata['rows']:,} rows"
                )

        manifest_path = staging / "manifest.json"

        manifest_path.write_text(
            json.dumps(manifest, indent=2, default=str),
            encoding="utf-8",
        )

        # Publish the complete export directory only after
        # every report has been exported successfully.
        staging.rename(destination)

    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    print("-" * 65)
    print(f"Reports generated: {len(REPORTS)}")
    print(f"Output: {destination}")
    print("PASS: Analytics report export completed")


if __name__ == "__main__":
    main()
