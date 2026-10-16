
-- Phase 6: Conservative pay-code classification.
-- These are provisional, label-based classifications.
-- Unlisted codes remain unclassified.

UPDATE mart.pay_code_component
SET
    pay_category = CASE component_code
        WHEN 'normal_worked' THEN 'regular_work'
        WHEN 'Hourly' THEN 'regular_work'
        WHEN 'Regular TM' THEN 'regular_work'

        WHEN 'Vacation TM' THEN 'paid_leave'
        WHEN 'PTO Scheduled TM' THEN 'paid_leave'

        WHEN 'Unpaid Time TM' THEN 'unpaid_leave'
        WHEN 'LOA - Un Paid TM' THEN 'unpaid_leave'

        WHEN 'TM-12 OT 50HP' THEN 'overtime_related'

        WHEN 'HH-Meeting' THEN 'administrative'

        WHEN 'TM-Orient12 indirect'
            THEN 'orientation_training'
    END,
    counts_as_worked_time = CASE
        WHEN component_code IN (
            'normal_worked',
            'Hourly',
            'Regular TM',
            'TM-12 OT 50HP'
        ) THEN TRUE
        ELSE FALSE
    END,
    eligible_for_attendance = CASE
        WHEN component_code IN (
            'normal_worked',
            'Hourly',
            'Regular TM',
            'TM-12 OT 50HP'
        ) THEN TRUE
        ELSE FALSE
    END,
    classification_notes =
        'Provisional classification based on pay-code label'
WHERE component_code IN (
    'normal_worked',
    'Hourly',
    'Regular TM',
    'Vacation TM',
    'PTO Scheduled TM',
    'Unpaid Time TM',
    'LOA - Un Paid TM',
    'TM-12 OT 50HP',
    'HH-Meeting',
    'TM-Orient12 indirect'
);
