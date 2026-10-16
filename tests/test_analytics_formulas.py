
"""Independent SQL formula tests for ASE-ETL Phase 6.

Uses controlled fixture values, not production data.
Does not modify Bronze, Silver, Gold, or marts.
"""

import unittest
from decimal import Decimal

from sqlalchemy import text

from src.silver.publish import ENGINE


class TestAnalyticsFormulas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.connection = ENGINE.connect()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def query(self, sql):
        return self.connection.execute(
            text(sql)
        ).mappings().all()

    def test_attendance_grace_period(self):
        """Exactly five minutes is within grace."""

        rows = self.query("""
            WITH cases AS (
                SELECT *
                FROM (
                    VALUES
                        (1, 5, 5, FALSE, FALSE),
                        (2, 6, 6, TRUE, TRUE),
                        (3, 0, 0, FALSE, FALSE),
                        (4, -10, -10, FALSE, FALSE)
                ) AS v(
                    case_id,
                    arrival_difference,
                    departure_difference,
                    expected_late,
                    expected_early
                )
            )
            SELECT
                case_id,
                arrival_difference > 5 AS actual_late,
                departure_difference > 5 AS actual_early,
                expected_late,
                expected_early
            FROM cases
        """)

        for row in rows:
            self.assertEqual(
                row["actual_late"],
                row["expected_late"],
            )
            self.assertEqual(
                row["actual_early"],
                row["expected_early"],
            )

    def test_overtime_grace_period(self):
        """Overruns must exceed five minutes."""

        rows = self.query("""
            WITH cases AS (
                SELECT *
                FROM (
                    VALUES
                        (480, 480, FALSE, 0),
                        (485, 480, FALSE, 0),
                        (486, 480, TRUE, 6),
                        (500, 480, TRUE, 20),
                        (450, 480, FALSE, 0)
                ) AS v(
                    actual_minutes,
                    scheduled_minutes,
                    expected_flag,
                    expected_minutes
                )
            )
            SELECT
                (
                    actual_minutes - scheduled_minutes > 5
                ) AS actual_flag,

                CASE
                    WHEN actual_minutes
                         - scheduled_minutes > 5
                    THEN actual_minutes - scheduled_minutes
                    ELSE 0
                END AS actual_minutes,

                expected_flag,
                expected_minutes
            FROM cases
        """)

        for row in rows:
            self.assertEqual(
                row["actual_flag"],
                row["expected_flag"],
            )
            self.assertEqual(
                row["actual_minutes"],
                row["expected_minutes"],
            )

    def test_headcount_date_boundaries(self):
        """Employees terminated on cutoff are excluded."""

        rows = self.query("""
            WITH employees AS (
                SELECT *
                FROM (
                    VALUES
                        (1, DATE '2025-01-01', NULL::DATE),
                        (2, DATE '2025-05-31', NULL::DATE),
                        (3, DATE '2025-01-01',
                            DATE '2025-05-31'),
                        (4, DATE '2025-06-01', NULL::DATE)
                ) AS v(employee_id, hire_date, term_date)
            )
            SELECT
                COUNT(*) FILTER (
                    WHERE hire_date <= DATE '2025-05-31'
                      AND (
                          term_date IS NULL
                          OR term_date > DATE '2025-05-31'
                      )
                ) AS closing_headcount
            FROM employees
        """)

        self.assertEqual(
            rows[0]["closing_headcount"],
            2,
        )

    def test_turnover_calculation(self):
        """Turnover uses average opening/closing headcount."""

        rows = self.query("""
            SELECT ROUND(
                100.0 * 2
                / NULLIF((50 + 46)::NUMERIC / 2, 0),
                2
            ) AS turnover_pct
        """)

        self.assertEqual(
            rows[0]["turnover_pct"],
            Decimal("4.17"),
        )

    def test_tenure_and_early_attrition(self):
        """89 days qualifies; 90 days does not."""

        rows = self.query("""
            WITH cases AS (
                SELECT *
                FROM (
                    VALUES
                        (DATE '2025-01-01',
                         DATE '2025-03-31', TRUE),
                        (DATE '2025-01-01',
                         DATE '2025-04-01', FALSE)
                ) AS v(
                    hire_date,
                    term_date,
                    expected_early
                )
            )
            SELECT
                term_date - hire_date AS tenure_days,
                term_date < hire_date + 90
                    AS actual_early,
                expected_early
            FROM cases
        """)

        self.assertEqual(rows[0]["tenure_days"], 89)
        self.assertEqual(rows[1]["tenure_days"], 90)

        for row in rows:
            self.assertEqual(
                row["actual_early"],
                row["expected_early"],
            )

    def test_average_worked_hours(self):
        """Average uses dates with eligible worked hours."""

        rows = self.query("""
            WITH daily AS (
                SELECT *
                FROM (
                    VALUES
                        (8.0::NUMERIC, 1),
                        (10.0::NUMERIC, 1),
                        (0.0::NUMERIC, 0)
                ) AS v(worked_hours, eligible_records)
            )
            SELECT ROUND(
                SUM(worked_hours)
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_records > 0
                    ),
                    0
                ),
                2
            ) AS average_hours
            FROM daily
        """)

        self.assertEqual(
            rows[0]["average_hours"],
            9.00,
        )

    def test_rolling_calendar_window(self):
        """Rolling window excludes records older than 6 days."""

        rows = self.query("""
            WITH daily AS (
                SELECT *
                FROM (
                    VALUES
                        (DATE '2025-05-01', 6::NUMERIC),
                        (DATE '2025-05-02', 8::NUMERIC),
                        (DATE '2025-05-09', 10::NUMERIC)
                ) AS v(work_date, worked_hours)
            )
            SELECT
                work_date,

                ROUND(
                    AVG(worked_hours) OVER (
                        ORDER BY work_date::TIMESTAMP
                        RANGE BETWEEN
                            INTERVAL '6 days' PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                ) AS rolling_average

            FROM daily
            ORDER BY work_date
        """)

        self.assertEqual(
            rows[-1]["rolling_average"],
            10.00,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
