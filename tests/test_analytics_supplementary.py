
"""Integration tests for six supplementary analytics KPIs."""

import unittest

from sqlalchemy import text
from src.silver.publish import ENGINE


class TestSupplementaryAnalytics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.connection = ENGINE.connect()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def scalar(self, sql):
        return self.connection.execute(
            text(sql)
        ).scalar_one()

    def test_monthly_workload(self):
        """Monthly totals reconcile with Gold."""

        result = self.scalar("""
            SELECT COALESCE(SUM(recorded_hours), 0)
            FROM mart.kpi_monthly_workload
        """)

        expected = self.scalar("""
            SELECT ROUND(SUM(hours_worked), 2)
            FROM gold.fact_timesheet
        """)

        self.assertEqual(result, expected)

    def test_department_workload(self):
        """Department totals preserve all source hours."""

        result = self.scalar("""
            SELECT COALESCE(SUM(recorded_hours), 0)
            FROM mart.kpi_department_workload
        """)

        expected = self.scalar("""
            SELECT ROUND(SUM(hours_worked), 2)
            FROM gold.fact_timesheet
        """)

        self.assertEqual(result, expected)

    def test_attendance_compliance(self):
        """Compliance events reconcile with attendance data."""

        actual = self.scalar("""
            SELECT COALESCE(SUM(compliant_events), 0)
            FROM mart.kpi_attendance_compliance
        """)

        expected = self.scalar("""
            SELECT COUNT(*)
            FROM mart.v_attendance_events
            WHERE eligible_for_attendance
              AND NOT is_late_arrival
              AND NOT is_early_departure
        """)

        self.assertEqual(actual, expected)

    def test_department_overtime(self):
        """Department totals preserve overtime-proxy hours."""

        actual = self.scalar("""
            SELECT COALESCE(SUM(overtime_proxy_hours), 0)
            FROM mart.kpi_department_overtime
        """)

        expected = self.scalar("""
            SELECT COALESCE(SUM(overtime_proxy_hours), 0)
            FROM mart.v_overtime_events
        """)

        self.assertEqual(actual, expected)

    def test_weekday_workload(self):
        """Weekend and weekday totals preserve source hours."""

        actual = self.scalar("""
            SELECT COALESCE(SUM(recorded_hours), 0)
            FROM mart.kpi_weekday_workload
        """)

        expected = self.scalar("""
            SELECT ROUND(SUM(hours_worked), 2)
            FROM gold.fact_timesheet
        """)

        self.assertEqual(actual, expected)

    def test_workforce_changes(self):
        """Monthly workforce movements match source mart."""

        result = self.scalar("""
            SELECT COUNT(*)
            FROM mart.kpi_workforce_changes k
            JOIN mart.workforce_monthly m
              ON k.month_start = m.month_start
            WHERE k.new_hires <> m.new_hires
               OR k.terminations <> m.terminations
               OR k.net_workforce_change <>
                  m.new_hires - m.terminations
        """)

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
