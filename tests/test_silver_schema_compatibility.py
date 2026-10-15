
"""Silver v2 schema compatibility regression tests."""

import unittest
from pathlib import Path

import pandas as pd

from src.silver.schema_contract import (
    SCHEMA_COLUMNS,
    align_source_schema,
)
from src.silver.transformations import transform_employees
from src.silver.validation import validate_employees
from src.silver.pipeline import split_critical_records


class TestSilverSchemaCompatibility(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        files = sorted(
            Path("data/bronze/parquet").glob(
                "employee_*/*.parquet"
            )
        )

        if not files:
            raise FileNotFoundError(
                "Bronze employee Parquet not found"
            )

        cls.raw = pd.read_parquet(files[0]).head(3)

    def process(self, source):
        aligned, report = align_source_schema(
            source,
            dataset="employees",
            source_name="synthetic_future_export",
        )

        silver = transform_employees(aligned)
        issues = validate_employees(aligned, silver)

        return aligned, silver, issues, report

    def test_missing_optional_numeric_column(self):
        source = self.raw.drop(
            columns=["years_of_experience"]
        )

        _, silver, issues, report = self.process(source)

        self.assertIn(
            "years_of_experience",
            report.missing_optional_columns,
        )

        self.assertIn(
            "years_of_experience",
            silver.columns,
        )

        self.assertTrue(
            silver["years_of_experience"].isna().all()
        )

        self.assertTrue(
            pd.api.types.is_numeric_dtype(
                silver["years_of_experience"]
            )
        )

        self.assertTrue(issues.empty)

    def test_future_populated_fields(self):
        source = self.raw.copy()
        index = source.index[0]

        updates = {
            "middle_name": "James",
            "years_of_experience": "12",
            "city": "Chicago",
            "state": "Illinois",
            "country": "USA",
            "zip": "00123",
            "clinical_level": "Level 3",
            "is_per_deim": "Yes",
            "work_phone": "5551234567",
        }

        for column, value in updates.items():
            source.loc[index, column] = value

        _, silver, issues, _ = self.process(source)

        for column, expected in updates.items():
            actual = silver.loc[index, column]

            if column == "years_of_experience":
                self.assertEqual(actual, 12.0)
            else:
                self.assertEqual(actual, expected)

        self.assertTrue(issues.empty)

    def test_invalid_future_numeric_value(self):
        source = self.raw.copy()

        source.loc[
            source.index[0],
            "years_of_experience",
        ] = "not-a-number"

        _, silver, issues, _ = self.process(source)

        self.assertIn(
            "INVALID_YEARS_OF_EXPERIENCE",
            issues["issue_code"].tolist(),
        )

        accepted, rejected = split_critical_records(
            silver, issues
        )

        self.assertEqual(len(rejected), 1)
        self.assertEqual(len(accepted), 2)

    def test_column_order_does_not_change_silver(self):
        source = self.raw[
            list(reversed(self.raw.columns))
        ]

        _, silver, issues, _ = self.process(source)

        self.assertEqual(
            silver.columns.tolist(),
            SCHEMA_COLUMNS["employees"],
        )

        self.assertTrue(issues.empty)

    def test_current_source_values_unchanged(self):
        original = self.raw.copy(deep=True)

        self.process(self.raw)

        pd.testing.assert_frame_equal(
            self.raw,
            original,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
