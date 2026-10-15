
"""Regression tests for the Silver schema contract."""

import unittest
from pathlib import Path

import pandas as pd

from src.silver.schema_contract import (
    SCHEMA_VERSION,
    SCHEMA_COLUMNS,
    SchemaDriftError,
    align_source_schema,
)


class TestSilverSchemaContract(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        bronze_root = Path("data/bronze/parquet")

        employee_files = sorted(
            bronze_root.glob("employee_*/*.parquet")
        )

        if not employee_files:
            raise FileNotFoundError(
                "Bronze employee Parquet not found"
            )

        cls.raw_employee = pd.read_parquet(
            employee_files[0]
        )

    def test_current_employee_schema(self):
        aligned, report = align_source_schema(
            self.raw_employee,
            "employees",
            "current_employee_export",
        )

        self.assertEqual(len(aligned), 50)
        self.assertEqual(len(aligned.columns), 38)
        self.assertEqual(
            aligned.columns.tolist(),
            SCHEMA_COLUMNS["employees"],
        )
        self.assertEqual(report.schema_version, SCHEMA_VERSION)
        self.assertFalse(report.missing_optional_columns)

    def test_future_city_and_experience_values(self):
        future = self.raw_employee.copy()

        future.loc[future.index[0], "city"] = "Chicago"
        future.loc[
            future.index[0], "years_of_experience"
        ] = "8"

        aligned, _ = align_source_schema(
            future,
            "employees",
            "future_employee_export",
        )

        self.assertEqual(
            aligned.iloc[0]["city"], "Chicago"
        )
        self.assertEqual(
            aligned.iloc[0]["years_of_experience"], "8"
        )

    def test_missing_optional_column(self):
        future = self.raw_employee.drop(
            columns=["work_phone"]
        )

        aligned, report = align_source_schema(
            future,
            "employees",
            "missing_optional_export",
        )

        self.assertIn(
            "work_phone",
            report.missing_optional_columns,
        )
        self.assertTrue(aligned["work_phone"].isna().all())

    def test_unexpected_column_rejected(self):
        future = self.raw_employee.copy()
        future["employment_category"] = "Full Time"

        with self.assertRaises(SchemaDriftError):
            align_source_schema(
                future,
                "employees",
                "unexpected_column_export",
            )

    def test_missing_required_column_rejected(self):
        future = self.raw_employee.drop(
            columns=["client_employee_id"]
        )

        with self.assertRaises(SchemaDriftError):
            align_source_schema(
                future,
                "employees",
                "missing_employee_id_export",
            )

    def test_column_reordering(self):
        future = self.raw_employee[
            list(reversed(self.raw_employee.columns))
        ]

        aligned, _ = align_source_schema(
            future,
            "employees",
            "reordered_export",
        )

        self.assertEqual(
            aligned.columns.tolist(),
            SCHEMA_COLUMNS["employees"],
        )

    def test_duplicate_columns_rejected(self):
        future = self.raw_employee.copy()
        future.columns = (
            list(future.columns[:-1])
            + [future.columns[0]]
        )

        with self.assertRaises(SchemaDriftError):
            align_source_schema(
                future,
                "employees",
                "duplicate_columns_export",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
