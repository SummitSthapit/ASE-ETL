
"""Production KPI integration tests.

Read-only tests against PostgreSQL Gold and mart views.
"""

import unittest

from sqlalchemy import text

from src.silver.publish import ENGINE


class TestAnalyticsIntegration(unittest.TestCase):

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

    def test_attendance_kpis(self):
        """Recalculate attendance flags from timestamps."""

        expected = self.connection.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE c.eligible_for_attendance
                          AND f.punch_in_datetime >
                              f.scheduled_start_datetime
                              + INTERVAL '5 minutes'
                    ) AS late_arrivals,

                    COUNT(*) FILTER (
                        WHERE c.eligible_for_attendance
                          AND f.punch_out_datetime <
                              f.scheduled_end_datetime
                              - INTERVAL '5 minutes'
                    ) AS early_departures

                FROM gold.fact_timesheet f

                JOIN mart.v_timesheet_classification c
                    ON f.timesheet_key = c.timesheet_key
            """)
        ).mappings().one()

        actual = self.connection.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE is_late_arrival
                    ) AS late_arrivals,

                    COUNT(*) FILTER (
                        WHERE is_early_departure
                    ) AS early_departures

                FROM mart.v_attendance_events
            """)
        ).mappings().one()

        self.assertEqual(
            dict(actual),
            dict(expected),
        )

    def test_overtime_kpi(self):
        """Independently calculate qualifying shift overruns."""

        expected = self.scalar("""
            SELECT COUNT(*)
            FROM gold.fact_timesheet f
            JOIN mart.v_timesheet_classification c
                ON f.timesheet_key = c.timesheet_key
            WHERE c.eligible_for_attendance
              AND (
                  (f.punch_out_datetime - f.punch_in_datetime)
                  -
                  (
                      f.scheduled_end_datetime
                      - f.scheduled_start_datetime
                  )
              ) > INTERVAL '5 minutes'
        """)

        actual = self.scalar("""
            SELECT COUNT(*)
            FROM mart.v_overtime_events
            WHERE is_overtime_event
        """)

        self.assertEqual(actual, expected)

    def test_working_hours_kpi(self):
        """Reconcile classified work hours to Gold."""

        expected = self.scalar("""
            SELECT COALESCE(
                ROUND(SUM(f.hours_worked), 2),
                0
            )
            FROM gold.fact_timesheet f
            JOIN mart.v_timesheet_classification c
                ON f.timesheet_key = c.timesheet_key
            WHERE c.work_status = 'eligible'
        """)

        actual = self.scalar("""
            SELECT COALESCE(
                SUM(total_worked_hours), 0
            )
            FROM mart.kpi_employee_working_hours
        """)

        self.assertEqual(actual, expected)

    def test_monthly_headcount(self):
        """Recalculate closing headcount from Gold profiles."""

        rows = self.connection.execute(
            text("""
                SELECT
                    m.month_start,
                    m.reporting_date,
                    m.closing_headcount
                FROM mart.workforce_monthly m
            """)
        ).mappings().all()

        for row in rows:
            expected = self.connection.execute(
                text("""
                    SELECT COUNT(*)
                    FROM gold.dim_employee e
                    WHERE e.has_employee_profile = TRUE
                      AND e.hire_date IS NOT NULL
                      AND e.hire_date <= :cutoff
                      AND (
                          e.term_date IS NULL
                          OR e.term_date > :cutoff
                      )
                """),
                {"cutoff": row["reporting_date"]},
            ).scalar_one()

            self.assertEqual(
                row["closing_headcount"],
                expected,
            )

    def test_early_attrition(self):
        """Independently verify 90-day attrition counts."""

        expected = self.connection.execute(
            text("""
                WITH cutoff AS (
                    SELECT MAX(full_date) AS as_of_date
                    FROM gold.dim_date
                )
                SELECT
                    COUNT(*) FILTER (
                        WHERE e.hire_date <=
                              c.as_of_date - 90
                    ) AS eligible_hires,

                    COUNT(*) FILTER (
                        WHERE e.hire_date <=
                              c.as_of_date - 90
                          AND e.term_date IS NOT NULL
                          AND e.term_date >= e.hire_date
                          AND e.term_date <
                              e.hire_date + 90
                          AND e.term_date <= c.as_of_date
                    ) AS early_terminations

                FROM gold.dim_employee e
                CROSS JOIN cutoff c

                WHERE e.has_employee_profile = TRUE
                  AND e.hire_date IS NOT NULL
                  AND e.hire_date <= c.as_of_date
                  AND (
                      e.term_date IS NULL
                      OR e.term_date >= e.hire_date
                  )
            """)
        ).mappings().one()

        actual = self.connection.execute(
            text("""
                SELECT
                    eligible_hires,
                    early_terminations
                FROM mart.kpi_early_attrition
            """)
        ).mappings().one()

        self.assertEqual(
            dict(actual),
            dict(expected),
        )


    def test_monthly_turnover(self):
        """Verify turnover rates from monthly headcounts."""

        rows = self.connection.execute(
            text("""
                SELECT
                    month_start,
                    opening_headcount,
                    closing_headcount,
                    terminations,
                    turnover_rate_pct
                FROM mart.workforce_monthly
            """)
        ).mappings().all()

        self.assertGreater(len(rows), 0)

        from decimal import Decimal

        for row in rows:
            average_headcount = (
                Decimal(row["opening_headcount"])
                + Decimal(row["closing_headcount"])
            ) / Decimal(2)

            expected = (
                (
                    Decimal(row["terminations"])
                    * Decimal(100)
                    / average_headcount
                ).quantize(Decimal("0.01"))
                if average_headcount > 0
                else None
            )

            self.assertEqual(
                row["turnover_rate_pct"],
                expected,
                f"Turnover mismatch: {row['month_start']}",
            )


    def test_department_tenure(self):
        """Recalculate department tenure from employee dates."""

        rows = self.connection.execute(
            text("""
                WITH cutoff AS (
                    SELECT MAX(full_date) AS as_of_date
                    FROM gold.dim_date
                ),
                employee_tenure AS (
                    SELECT
                        e.department_id,

                        COALESCE(
                            NULLIF(TRIM(e.department_name), ''),
                            'Unknown'
                        ) AS department_name,

                        LEAST(
                            COALESCE(
                                e.term_date,
                                c.as_of_date
                            ),
                            c.as_of_date
                        ) - e.hire_date AS tenure_days

                    FROM gold.dim_employee e
                    CROSS JOIN cutoff c

                    WHERE e.has_employee_profile
                      AND e.hire_date IS NOT NULL
                      AND e.hire_date <= c.as_of_date
                      AND (
                          e.term_date IS NULL
                          OR e.term_date >= e.hire_date
                      )
                )
                SELECT
                    d.department_name,
                    d.employee_count,
                    d.average_tenure_days,
                    x.employee_count AS expected_count,
                    x.average_tenure_days AS expected_average

                FROM mart.department_tenure d

                JOIN (
                    SELECT
                        department_id,
                        department_name,
                        COUNT(*) AS employee_count,
                        ROUND(
                            AVG(tenure_days), 2
                        ) AS average_tenure_days
                    FROM employee_tenure
                    GROUP BY department_id, department_name
                ) x
                  ON d.department_id
                        IS NOT DISTINCT FROM x.department_id
                 AND d.department_name = x.department_name
            """)
        ).mappings().all()

        department_count = self.scalar("""
            SELECT COUNT(*)
            FROM mart.department_tenure
        """)

        self.assertEqual(len(rows), department_count)

        for row in rows:
            self.assertEqual(
                row["employee_count"],
                row["expected_count"],
            )
            self.assertEqual(
                row["average_tenure_days"],
                row["expected_average"],
            )


    def test_rolling_working_hours(self):
        """Recalculate selected rolling averages independently."""

        rows = self.connection.execute(
            text("""
                SELECT
                    r.employee_key,
                    r.work_date,
                    r.rolling_7_day_avg_worked_hours
                FROM mart.kpi_rolling_working_hours r
                ORDER BY r.employee_key, r.work_date
                LIMIT 30
            """)
        ).mappings().all()

        self.assertGreater(len(rows), 0)

        for row in rows:
            expected = self.connection.execute(
                text("""
                    SELECT ROUND(AVG(worked_hours), 2)
                    FROM mart.employee_work_hours_daily
                    WHERE employee_key = :employee_key
                      AND work_date BETWEEN
                          :work_date - 6
                          AND :work_date
                """),
                {
                    "employee_key": row["employee_key"],
                    "work_date": row["work_date"],
                },
            ).scalar_one()

            self.assertEqual(
                row["rolling_7_day_avg_worked_hours"],
                expected,
            )


    def test_composite_pay_code_integrity(self):
        """Composite pay codes must not duplicate fact records."""

        result = self.connection.execute(
            text("""
                SELECT
                    COUNT(*) AS total_records,

                    COUNT(DISTINCT timesheet_key)
                        AS distinct_records,

                    COUNT(*) FILTER (
                        WHERE pay_code LIKE '%|%'
                    ) AS composite_records,

                    COUNT(*) FILTER (
                        WHERE work_status NOT IN (
                            'eligible',
                            'excluded',
                            'unresolved'
                        )
                    ) AS invalid_status

                FROM mart.v_timesheet_classification
            """)
        ).mappings().one()

        gold_count = self.scalar("""
            SELECT COUNT(*)
            FROM gold.fact_timesheet
        """)

        self.assertEqual(
            result["total_records"],
            gold_count,
        )

        self.assertEqual(
            result["distinct_records"],
            gold_count,
        )

        self.assertEqual(
            result["composite_records"],
            self.scalar("""
                SELECT COUNT(*)
                FROM gold.fact_timesheet
                WHERE pay_code LIKE '%|%'
            """),
        )

        self.assertEqual(result["invalid_status"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
