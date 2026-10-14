
import unittest

import pandas as pd

from src.silver.transformations import (
    transform_employees,
    transform_timesheets,
)

from src.silver.validation import (
    validate_employees,
    validate_timesheets,
)

from src.silver.deduplication import (
    deduplicate_timesheets,
)

from src.silver.pipeline import (
    split_critical_records,
    validate_primary_key,
)


class TestSilverNegativeCases(unittest.TestCase):

    def setUp(self):
        from pathlib import Path

        root = Path("data/bronze/parquet")

        employee_file = next(
            root.glob("employee_*/*.parquet")
        )

        timesheet_file = next(
            root.glob("timesheet_*/*.parquet")
        )

        self.employee = pd.read_parquet(
            employee_file
        ).head(3).copy()

        self.timesheet = pd.read_parquet(
            timesheet_file
        ).head(3).copy()

    def test_missing_employee_id(self):
        raw = self.employee.copy()
        raw.loc[raw.index[0], "client_employee_id"] = ""

        silver = transform_employees(raw)
        issues = validate_employees(raw, silver)

        self.assertIn(
            "MISSING_EMPLOYEE_ID",
            issues["issue_code"].tolist(),
        )

        accepted, rejected = split_critical_records(
            silver, issues
        )

        self.assertEqual(len(rejected), 1)
        self.assertEqual(len(accepted), 2)

    def test_invalid_employee_date(self):
        raw = self.employee.copy()
        raw.loc[raw.index[0], "hire_date"] = (
            "not-a-date"
        )

        silver = transform_employees(raw)
        issues = validate_employees(raw, silver)

        self.assertIn(
            "INVALID_HIRE_DATE",
            issues["issue_code"].tolist(),
        )

    def test_invalid_timesheet_hours(self):
        raw = self.timesheet.copy()
        raw.loc[raw.index[0], "hours_worked"] = (
            "invalid"
        )

        silver = transform_timesheets(raw)
        issues = validate_timesheets(raw, silver)

        self.assertIn(
            "INVALID_HOURS_WORKED",
            issues["issue_code"].tolist(),
        )

    def test_negative_timesheet_hours(self):
        raw = self.timesheet.copy()
        raw.loc[raw.index[0], "hours_worked"] = "-8"

        silver = transform_timesheets(raw)
        issues = validate_timesheets(raw, silver)

        self.assertIn(
            "NEGATIVE_HOURS_WORKED",
            issues["issue_code"].tolist(),
        )

    def test_reversed_punch_timestamps(self):
        raw = self.timesheet.copy()

        index = raw.index[0]

        raw.loc[index, "punch_in_datetime"] = (
            "2025-09-22 19:00:00.000"
        )

        raw.loc[index, "punch_out_datetime"] = (
            "2025-09-22 07:00:00.000"
        )

        silver = transform_timesheets(raw)
        issues = validate_timesheets(raw, silver)

        self.assertIn(
            "PUNCH_OUT_BEFORE_PUNCH_IN",
            issues["issue_code"].tolist(),
        )

    def test_duplicate_primary_key_rejected(self):
        silver = transform_timesheets(
            self.timesheet
        )

        duplicated = pd.concat(
            [silver.iloc[[0]], silver.iloc[[0]]],
            ignore_index=True,
        )

        with self.assertRaises(ValueError):
            validate_primary_key(
                duplicated,
                [
                    "client_employee_id",
                    "punch_in_datetime",
                    "punch_out_datetime",
                ],
                "timesheets",
            )

    def test_deduplication_is_idempotent(self):
        silver = transform_timesheets(
            self.timesheet
        )

        duplicated = pd.concat(
            [silver, silver.iloc[[0]]],
            ignore_index=True,
        )

        clean, audit = deduplicate_timesheets(
            duplicated
        )

        self.assertEqual(len(clean), len(silver))
        self.assertEqual(len(audit), 1)

        clean_again, audit_again = (
            deduplicate_timesheets(clean)
        )

        self.assertEqual(
            len(clean_again), len(clean)
        )

        self.assertTrue(audit_again.empty)


if __name__ == "__main__":
    unittest.main(verbosity=2)
