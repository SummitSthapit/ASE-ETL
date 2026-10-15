
import json
from sys import audit
import unittest
from pathlib import Path

from src.silver.pipeline import load_bronze
from src.silver.schema_audit import AUDIT_COLUMNS


class TestSilverSchemaAudit(unittest.TestCase):

    def test_employee_schema_audit(self):
        employees, audit = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
            return_audit=True,
        )

        self.assertEqual(len(employees), 50)
        self.assertEqual(len(audit), 1)

        self.assertEqual(
            audit.columns.tolist(),
            AUDIT_COLUMNS,
        )

        row = audit.iloc[0]

        self.assertEqual(row["dataset"], "employees")
        self.assertEqual(row["schema_version"], "2.0")
        self.assertEqual(row["source_rows"], 50)
        self.assertEqual(row["schema_status"], "PASS")

        self.assertEqual(
            json.loads(row["missing_optional_columns"]),
            [],
        )

    def test_timesheet_schema_audit(self):
        timesheets, audit = load_bronze(
            "timesheet_*/*.parquet",
            dataset="timesheets",
            return_audit=True,
        )

        self.assertEqual(len(timesheets), 412571)

        # Six September parts, plus four October parts.
        self.assertEqual(len(audit), 10)

        self.assertEqual(
            int(audit["source_rows"].sum()),
            412571,
        )

        self.assertTrue(
            audit["schema_status"].eq("PASS").all()
        )

        self.assertTrue(
            audit["schema_version"].eq("2.0").all()
        )

    def test_default_loader_remains_compatible(self):
        employees = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
        )

        self.assertEqual(len(employees), 50)

    def test_source_paths_are_relative(self):
        _, audit = load_bronze(
            "employee_*/*.parquet",
            dataset="employees",
            return_audit=True,
        )

        for source_name in audit["source_name"]:
            self.assertFalse(
                Path(source_name).is_absolute()
            )

            self.assertNotIn(
                "\\",
                source_name,
            )

            self.assertTrue(
                source_name.startswith("employee_")
            )

            self.assertTrue(
                source_name.endswith(".parquet")
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
