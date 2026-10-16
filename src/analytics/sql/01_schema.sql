
-- Reporting schema and pay-code classification.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.pay_code_classification (
    pay_code TEXT PRIMARY KEY,

    pay_category VARCHAR(50) NOT NULL
        DEFAULT 'unclassified',

    counts_as_worked_time BOOLEAN NOT NULL
        DEFAULT FALSE,

    eligible_for_attendance BOOLEAN NOT NULL
        DEFAULT FALSE,

    classification_notes TEXT,

    CONSTRAINT chk_pay_category
        CHECK (
            pay_category IN (
                'regular_work',
                'overtime_related',
                'orientation_training',
                'administrative',
                'paid_leave',
                'unpaid_leave',
                'on_call',
                'other',
                'unclassified'
            )
        )
);

-- Register source pay codes without making
-- unsupported assumptions about their meaning.

INSERT INTO mart.pay_code_classification (
    pay_code
)
SELECT DISTINCT pay_code
FROM gold.fact_timesheet
WHERE pay_code IS NOT NULL
ON CONFLICT (pay_code) DO NOTHING;
