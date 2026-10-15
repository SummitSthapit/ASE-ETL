
"""Read-only checks for Silver v2 publisher configuration."""

import unittest

import pandas as pd

from sqlalchemy.sql.sqltypes import Numeric

from src.silver.publish import (
    DATASETS,
    EXPECTED_COUNTS,
    sql_types,
)


class TestSilverV2PublisherConfig(unittest.TestCase):

    def test_schema_audit_registered(self):
        self.assertIn(
            "audit/schema_evolution",
            DATASETS,
        )

        self.assertEqual(len(DATASETS), 7)

    def test_expected_counts_cover_all_datasets(self):
        self.assertEqual(
            set(DATASETS),
            set(EXPECTED_COUNTS),
        )

        self.assertEqual(
            EXPECTED_COUNTS["audit/schema_evolution"],
            11,
        )

    def test_employee_numeric_types(self):
        frame = pd.DataFrame({
            "client_employee_id": pd.Series(
                ["00037"], dtype="string"
            ),
            "scheduled_weekly_hour": pd.Series(
                [40.0], dtype="Float64"
            ),
            "years_of_experience": pd.Series(
                [8.0], dtype="Float64"
            ),
            "source_row_number": [1],
            "ingested_at_utc": pd.to_datetime(
                ["2025-10-16T11:25:00Z"],
                utc=True,
            ),
        })

        mapping = sql_types(frame, "employees")

        for column in [
            "scheduled_weekly_hour",
            "years_of_experience",
        ]:
            self.assertIsInstance(
                mapping[column],
                Numeric,
            )

            self.assertEqual(
                mapping[column].precision,
                12,
            )

            self.assertEqual(
                mapping[column].scale,
                2,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
