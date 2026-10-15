
import unittest
from unittest.mock import patch

from pathlib import Path
import tempfile

import pandas as pd

from src.silver.pipeline import load_bronze
from src.silver.schema_contract import (
    SCHEMA_COLUMNS,
    SchemaDriftError,
)


class TestSilverSchemaIntegration(unittest.TestCase):

    def test_current_employee_loading(self):
        result = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
        )

        self.assertEqual(len(result), 50)
        self.assertEqual(len(result.columns), 38)
        self.assertEqual(
            result.columns.tolist(),
            SCHEMA_COLUMNS["employees"],
        )

    def test_current_timesheet_loading(self):
        result = load_bronze(
            "timesheet_*/*.parquet",
            dataset="timesheets",
        )

        self.assertEqual(len(result), 412571)
        self.assertEqual(len(result.columns), 18)
        self.assertEqual(
            result.columns.tolist(),
            SCHEMA_COLUMNS["timesheets"],
        )

    def test_new_column_rejected_before_concat(self):
        original = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
        )

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "employee_future"
            source.mkdir()

            changed = original.copy()
            changed["new_business_field"] = "example"

            changed.to_parquet(
                source / "part-00000.parquet",
                index=False,
            )

            with patch(
                "src.silver.pipeline.BRONZE_ROOT",
                root,
            ):
                with self.assertRaises(SchemaDriftError):
                    load_bronze(
                        "employee_*/*.parquet",
                        dataset="employees",
                    )

    def test_missing_optional_column_aligned(self):
        original = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
        )

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "employee_future"
            source.mkdir()

            changed = original.drop(
                columns=["work_phone"]
            )

            changed.to_parquet(
                source / "part-00000.parquet",
                index=False,
            )

            with patch(
                "src.silver.pipeline.BRONZE_ROOT",
                root,
            ):
                loaded = load_bronze(
                    "employee_*/*.parquet",
                    dataset="employees",
                )

            self.assertEqual(len(loaded.columns), 38)
            self.assertTrue(
                loaded["work_phone"].isna().all()
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
