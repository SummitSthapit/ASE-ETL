
import unittest
from pathlib import Path

import pandas as pd

from src.silver.schema_contract import align_source_schema
from src.silver.transformations import transform_employees
from src.silver.validation import validate_employees
from src.silver.pipeline import split_critical_records


class TestSilverEmployeeValidationV2(unittest.TestCase):

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

        cls.employee = pd.read_parquet(files[0]).head(3)

    def validate(self, raw):
        aligned, _ = align_source_schema(
            raw,
            dataset="employees",
            source_name="synthetic_employee_test",
        )

        silver = transform_employees(aligned)

        issues = validate_employees(
            aligned,
            silver,
        )

        return silver, issues

    def test_valid_experience(self):
        raw = self.employee.copy()

        raw.loc[
            raw.index[0],
            "years_of_experience",
        ] = "8"

        silver, issues = self.validate(raw)

        self.assertEqual(
            silver.iloc[0]["years_of_experience"],
            8.0,
        )

        self.assertNotIn(
            "INVALID_YEARS_OF_EXPERIENCE",
            issues["issue_code"].tolist(),
        )

    def test_invalid_experience(self):
        raw = self.employee.copy()

        raw.loc[
            raw.index[0],
            "years_of_experience",
        ] = "eight years"

        silver, issues = self.validate(raw)

        self.assertIn(
            "INVALID_YEARS_OF_EXPERIENCE",
            issues["issue_code"].tolist(),
        )

        accepted, rejected = split_critical_records(
            silver,
            issues,
        )

        self.assertEqual(len(rejected), 1)
        self.assertEqual(len(accepted), 2)

    def test_negative_experience(self):
        raw = self.employee.copy()

        raw.loc[
            raw.index[0],
            "years_of_experience",
        ] = "-3"

        silver, issues = self.validate(raw)

        self.assertIn(
            "NEGATIVE_YEARS_OF_EXPERIENCE",
            issues["issue_code"].tolist(),
        )

        accepted, rejected = split_critical_records(
            silver,
            issues,
        )

        self.assertEqual(len(rejected), 1)

    def test_null_experience_accepted(self):
        silver, issues = self.validate(
            self.employee.copy()
        )

        self.assertTrue(
            silver["years_of_experience"].isna().all()
        )

        self.assertNotIn(
            "INVALID_YEARS_OF_EXPERIENCE",
            issues["issue_code"].tolist(),
        )

    def test_future_text_attributes_preserved(self):
        raw = self.employee.copy()
        index = raw.index[0]

        raw.loc[index, "clinical_level"] = "Level 2"
        raw.loc[index, "is_per_deim"] = "Yes"
        raw.loc[index, "city"] = "Chicago"

        silver, issues = self.validate(raw)

        self.assertEqual(
            silver.loc[index, "clinical_level"],
            "Level 2",
        )

        self.assertEqual(
            silver.loc[index, "is_per_deim"],
            "Yes",
        )

        self.assertEqual(
            silver.loc[index, "city"],
            "Chicago",
        )

        self.assertTrue(issues.empty)


if __name__ == "__main__":
    unittest.main(verbosity=2)
