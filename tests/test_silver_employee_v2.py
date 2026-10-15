
import unittest
from pathlib import Path

import pandas as pd

from src.silver.transformations import transform_employees
from src.silver.schema_contract import (
    SCHEMA_COLUMNS,
    align_source_schema,
)


class TestSilverEmployeeV2(unittest.TestCase):

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

        cls.raw = pd.read_parquet(files[0])

    def transform(self, frame):
        aligned, _ = align_source_schema(
            frame,
            "employees",
            "test_employee_source",
        )
        return transform_employees(aligned)

    def test_all_columns_preserved(self):
        silver = self.transform(self.raw)

        self.assertEqual(len(silver), 50)
        self.assertEqual(len(silver.columns), 38)
        self.assertEqual(
            silver.columns.tolist(),
            SCHEMA_COLUMNS["employees"],
        )

    def test_missing_columns_preserved_as_null(self):
        silver = self.transform(self.raw)

        for column in [
            "middle_name",
            "years_of_experience",
            "zip",
            "work_phone",
            "clinical_level",
        ]:
            self.assertIn(column, silver.columns)
            self.assertTrue(silver[column].isna().all())

    def test_current_masked_geography(self):
        silver = self.transform(self.raw)

        for column in [
            "address",
            "city",
            "state",
            "country",
        ]:
            self.assertTrue(
                silver[column].isna().all(),
                f"Unrecognized masks in {column}",
            )

    def test_future_values_preserved(self):
        future = self.raw.copy()
        index = future.index[0]

        future.loc[index, "city"] = "Oxford"
        future.loc[index, "state"] = "Illinois"
        future.loc[index, "country"] = "USA"
        future.loc[index, "zip"] = "00123"
        future.loc[index, "cell_phone"] = "9876543210"
        future.loc[index, "years_of_experience"] = "8"

        silver = self.transform(future)
        row = silver.loc[index]

        self.assertEqual(row["city"], "Oxford")
        self.assertEqual(row["state"], "Illinois")
        self.assertEqual(row["country"], "USA")
        self.assertEqual(row["zip"], "00123")
        self.assertEqual(row["cell_phone"], "9876543210")
        self.assertEqual(
            row["years_of_experience"], 8.0
        )

    def test_partially_masked_phone_becomes_null(self):
        future = self.raw.copy()
        index = future.index[0]

        future.loc[index, "cell_phone"] = "978xxxxxxx"

        silver = self.transform(future)

        self.assertTrue(
            pd.isna(silver.loc[index, "cell_phone"])
        )

    def test_optional_column_can_be_absent(self):
        future = self.raw.drop(
            columns=["work_phone"]
        )

        silver = self.transform(future)

        self.assertIn("work_phone", silver.columns)
        self.assertTrue(silver["work_phone"].isna().all())

    def test_bronze_not_modified(self):
        original = self.raw.copy(deep=True)

        self.transform(self.raw)

        pd.testing.assert_frame_equal(
            self.raw,
            original,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
