
-- Phase 6: Pay-code component inventory
-- Preserve original pay-code strings in Gold.
-- Do not allocate hours across composite components.

CREATE TABLE IF NOT EXISTS mart.pay_code_component (
    component_code TEXT PRIMARY KEY,

    pay_category VARCHAR(50) NOT NULL
        DEFAULT 'unclassified',

    counts_as_worked_time BOOLEAN NOT NULL
        DEFAULT FALSE,

    eligible_for_attendance BOOLEAN NOT NULL
        DEFAULT FALSE,

    classification_notes TEXT,

    CONSTRAINT chk_component_category
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

INSERT INTO mart.pay_code_component (
    component_code
)
SELECT DISTINCT TRIM(component)
FROM mart.pay_code_classification c
CROSS JOIN LATERAL
    unnest(string_to_array(c.pay_code, '|'))
    AS component
WHERE TRIM(component) <> ''
ON CONFLICT (component_code) DO NOTHING;
