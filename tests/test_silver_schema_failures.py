
import json
import tempfile
import unittest

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.silver.pipeline import load_bronze
from src.silver.schema_contract import SchemaDriftError
from src.silver.schema_failures import record_schema_failure


class TestSilverSchemaFailures(unittest.TestCase):

    def test_failure_diagnostic_written(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            report_path = record_schema_failure(
                dataset="employees",
                source_name="synthetic_employee.parquet",
                error=SchemaDriftError(
                    "Unknown columns=['new_column']"
                ),
                output_root=root,
            )

            self.assertTrue(report_path.exists())

            report = json.loads(
                report_path.read_text(encoding="utf-8")
            )

            self.assertEqual(
                report["dataset"],
                "employees",
            )

            self.assertEqual(
                report["schema_version"],
                "2.0",
            )

            self.assertEqual(
                report["status"],
                "REJECTED_SCHEMA",
            )

            self.assertEqual(
                report["error_type"],
                "SchemaDriftError",
            )

            self.assertIn(
                "new_column",
                report["error_message"],
            )

    def test_pipeline_records_schema_failure(self):
        original = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
        )

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            bronze_root = root / "bronze"
            source_dir = bronze_root / "employee_future"
            source_dir.mkdir(parents=True)

            changed = original.copy()
            changed["unapproved_column"] = "example"

            changed.to_parquet(
                source_dir / "part-00000.parquet",
                index=False,
            )

            diagnostics = root / "diagnostics"

            # Redirect both source discovery and diagnostic
            # files into disposable test directories.
            with (
                patch(
                    "src.silver.pipeline.BRONZE_ROOT",
                    bronze_root,
                ),
                patch(
                    "src.silver.schema_failures.FAILURE_ROOT",
                    diagnostics,
                ),
                patch(
                    "src.silver.pipeline.record_schema_failure",
                    wraps=lambda **kwargs: record_schema_failure(
                        **kwargs,
                        output_root=diagnostics,
                    ),
                ),
            ):
                with self.assertRaises(SchemaDriftError):
                    load_bronze(
                        "employee_*/*.parquet",
                        dataset="employees",
                    )

            reports = list(
                diagnostics.glob("schema_failure_*.json")
            )

            self.assertEqual(len(reports), 1)

            report = json.loads(
                reports[0].read_text(encoding="utf-8")
            )

            self.assertEqual(
                report["status"],
                "REJECTED_SCHEMA",
            )

            self.assertIn(
                "unapproved_column",
                report["error_message"],
            )

    def test_multiple_failures_create_separate_reports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            first = record_schema_failure(
                "employees",
                "first.parquet",
                SchemaDriftError("Missing required column"),
                output_root=root,
            )

            second = record_schema_failure(
                "employees",
                "second.parquet",
                SchemaDriftError("Unexpected column"),
                output_root=root,
            )

            self.assertNotEqual(first, second)
            self.assertEqual(
                len(list(root.glob("*.json"))),
                2,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
