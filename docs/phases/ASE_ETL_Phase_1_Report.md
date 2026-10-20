# ASE ETL Assignment — Phase 1 Report
*Data Understanding, Profiling, and Project Planning*

Prepared: 12 October 2026

**Architecture:** Medallion (Bronze → Silver → Gold)

## 1. Executive summary

Phase 1 data understanding, profiling, and project planning is complete. The project will use Medallion Architecture (Bronze → Silver → Gold), replacing the earlier normal-ETL approach. The current implementation approach is Python/pandas; Spark is not assumed.

The dataset has one 50-row employee roster and four timesheet exports totaling 412,571 rows. Profiling found very low roster-to-timesheet ID overlap, identical records repeated across two October exports, source-specific missing-value markers, and some material differences between recorded hours and elapsed punch duration. These require documented, conservative Silver-layer rules.

Important status: profiling findings are available, but the Bronze ingestion cell proposed at the end of the previous chat has not yet been run/verified. Do not describe Bronze/Silver/Gold as implemented. The findings and plan below are the handoff context for the next chat.

## 2. Project goal and working rules

Goal: satisfy Data Engineering Assignment 1 with a reproducible, well-documented pipeline that preserves lineage, validates quality, and produces analytics-ready data.

Project root: D:\ASE-ETL (Windows). Raw files: data\raw. Profiling outputs: data\profiling. The notebook has been run from D:\ASE-ETL\notebook, where Path.cwd().parent resolves to the project root.

Work incrementally, one phase at a time. Inspect real values before choosing rules; verify each phase before moving forward; never silently discard ambiguous records; distinguish confirmed facts from provisional assumptions; and keep original files under data/raw unchanged.

## 3. Source inventory

employee_202510161125.csv — 50 rows, 34 columns.

timesheet_202509151540.csv — 269,921 rows, 14 columns.

timesheet_202510161121.csv — 48,258 rows, 14 columns.

timesheet_202510161122.csv — 55,435 rows, 14 columns.

timesheet_202510161124.csv — 38,957 rows, 14 columns.

Four timesheet files total 412,571 rows. Keep file-level lineage; these are separate source exports.

## 4. Source schemas

Employee columns: client_employee_id, first_name, middle_name, last_name, preferred_name, job_code, job_title, job_start_date, organization_id, organization_name, department_id, department_name, dob, hire_date, recent_hire_date, anniversary_date, term_date, years_of_experience, work_email, address, city, state, zip, country, manager_employee_id, manager_employee_name, fte_status, is_per_deim, cell_phone, work_phone, scheduled_weekly_hour, active_status, termination_reason, clinical_level.

Timesheet columns: client_employee_id, department_id, department_name, home_department_id, home_department_name, pay_code, punch_in_comment, punch_out_comment, hours_worked, punch_apply_date, punch_in_datetime, punch_out_datetime, scheduled_start_datetime, scheduled_end_datetime.

Keep identifiers such as employee IDs and ZIP codes as strings. Numeric casting can destroy leading zeros.

## 5. Employee roster findings

The roster has 50 rows and 50 unique, non-missing client_employee_id values; no duplicate employee IDs were found.

IDs such as 00037, 00080, and 00131 have leading zeros and must remain strings.

term_date is missing for 44 employees and populated for 6. A null term_date alone does not prove the employee is active; check active_status and termination_reason together.

Do not infer keys from names or DOB based on apparent uniqueness in this small roster.

## 6. Employee ID relationship findings

Distinct timesheet IDs compared with the 50-row roster: September 9,792 total / 25 matched / 9,767 unmatched (~0.26% matched); October 11:21 8,303 / 27 / 8,276 (~0.33%); October 11:22 8,270 / 27 / 8,243 (~0.33%); October 11:24 6,411 / 17 / 6,394 (~0.27%).

These low match rates do not prove the unmatched IDs are invalid. The roster may be a subset or a different population. Preserve unmatched records and classify/report relationship status rather than dropping them.

September ID patterns: 269,420 rows digits-only (9,747 distinct); 493 rows numeric-hyphen-numeric (43 distinct); four rows CMCRN-3; four rows TEST2. The three October files have digits-only IDs.

101656-418941 and 1837-102143 appear on plausible populated records. CMCRN-3 appears on vacation records with blank department fields. TEST2 appears on four records with Operating Room department and multiple pay codes. Preserve and classify as unresolved unless authoritative rules clarify them.

## 7. Missing-value findings

Pandas isna() does not treat literal [NULL] as missing. In timesheet_202510161124.csv, home_department_id, home_department_name, scheduled_start_datetime, and scheduled_end_datetime are 100% missing. punch_in_comment is [NULL] on every row; punch_out_comment is [NULL] on 38,956 rows and VERY_LATE_OUT on one row.

Bronze should preserve source values. Silver should normalize only documented markers, potentially including blank strings, [NULL], NULL, N/A, NA, NAN, NONE, UNKNOWN, and NOT AVAILABLE, after checking the impact and documenting the convention.

Missingness differs by file and column. Do not discard records merely because schedule or department fields are absent.

## 8. Duplicates, candidate key, and cross-file overlap

No exact duplicate rows were found within any individual timesheet file.

client_employee_id + punch_in_datetime + punch_out_datetime is unique within each file and all key values are present. Adding pay_code is also unique but unnecessary for observed uniqueness. This is a provisional empirical candidate key, not a guaranteed universal business key.

client_employee_id + punch_apply_date + pay_code is not unique in all files. Department-based composite keys are unreliable because department_id is often blank; it is blank throughout the October 11:24 file.

Punch-in ranges: September 2025-04-18 to 2025-09-15 23:59; October 11:21 2025-09-16 to 2025-09-26 23:30; October 11:22 2025-09-22 to 2025-10-06 23:30; October 11:24 2025-10-04 to 2025-10-16 23:59. Punch-out ranges end at 2025-09-16 17:25, 2025-09-28 05:00, 2025-10-07 17:00, and 2025-10-17 19:30 respectively.

Shared candidate keys: September vs each October file = 0; October 11:21 vs 11:22 = 23,892; October 11:21 vs 11:24 = 0; October 11:22 vs 11:24 = 6,947.

For both overlapping pairs, all non-key columns matched for shared keys. Thus 30,839 repeated records would result from blindly concatenating all four exports, based on the tested key. Bronze retains every source record. Silver should reconcile identical cross-file records under a documented rule and preserve lineage. No three-way overlap was found because 11:21 and 11:24 share no keys.

## 9. Hours and timestamp validation

All files had no invalid punch timestamp parses, no missing/non-numeric hours_worked, no zero/negative recorded hours, and no zero/negative elapsed intervals. Every row was comparable.

Absolute difference <= 0.01 hours (36 seconds): September 267,948; October 11:21 47,845; October 11:22 55,266; October 11:24 38,922. This tolerance is a profiling threshold, not an approved business rule.

Absolute difference > 0.25 hours: September 615; October 11:21 210; October 11:22 109; October 11:24 33. Absolute difference > 1 hour: 133, 21, 9, and 0 respectively.

Median absolute difference: September 0.003333 hours; October 11:21 0; October 11:22 0; October 11:24 0. Maximum absolute difference: 2.213333, 1.25, 1.236667, and 1.0 hours respectively.

Many outliers have recorded hours lower than elapsed duration, especially September normal_worked records. Example: employee 421346 on 2025-06-17 08:31–17:00, normal_worked, 6.27 recorded vs 8.483333 elapsed (−2.213333 hours). On 2025-06-12, 08:30–17:03, the same employee has 6.37 recorded vs 8.55 elapsed (−2.18 hours).

Unpaid breaks or pay rules may explain differences, but this is unconfirmed. Keep hours_worked as the authoritative source measure. Use elapsed duration as a validation signal and flag discrepancies; do not auto-correct hours_worked.

## 10. Medallion architecture

Bronze: preserve source data and add technical lineage such as _source_file and _source_row_number. Record ingestion metadata. Do not clean, deduplicate, reinterpret, or overwrite original files. Keep data/raw unchanged.

Silver: normalize documented missing markers; cast validated types; keep IDs as strings; parse dates/timestamps/numeric measures; add quality flags; classify unresolved employee IDs; reconcile identical cross-file duplicates under the documented candidate key; retain lineage and audit counts. Flag or quarantine ambiguous/invalid rows under explicit rules, not silent deletion.

Gold: analytics-ready model. A provisional design is dim_employee (surrogate employee_key and source/business client_employee_id), optional dim_date, fact_timesheet, and potentially dim_pay_code. Finalize fact grain, keys, joins, and KPIs only after Silver behavior and assignment requirements are checked.

## 11. Documentation and profiling artifacts

Create/maintain README.md, docs/data_dictionary.md, docs/business_rules.md, a reproducible Phase 1 profiling notebook, data/profiling outputs, src/ for pipeline code, tests/ for validations, requirements.txt, and .gitignore as appropriate. Do not claim these all exist until checked.

Data dictionary fields: source file, source column, meaning (known/unknown), raw representation, Silver target type, nullable expectation, validation rule, transformation, key role, and status (confirmed/provisional/unresolved).

Business rules should document missing markers, ID handling/classification, candidate-key status, cross-file duplicate policy, hours/timestamp validation, referential integrity, quality reporting, lineage, and decisions.

Profiling outputs identified: source_file_inventory.csv; initial_column_profile.csv; per-file JSON reports; key_candidate_profile.csv; employee_id_pattern_profile.csv; timesheet_exact_duplicate_profile.csv; timesheet_composite_key_profile.csv; timesheet_date_ranges.csv; timesheet_cross_file_overlap.csv; timesheet_overlap_comparison.csv; timesheet_overlap_column_differences.csv; timesheet_hours_validation_profile.csv; timesheet_hours_validation_examples.csv; timesheet_hours_outlier_summary.csv; timesheet_hours_outlier_records.csv. Confirm actual existence before claiming all are present.

## 12. Decisions and unresolved questions

Decisions: (1) Medallion architecture; (2) Python/pandas, not assumed Spark; (3) raw files remain unchanged; (4) IDs remain strings; (5) unmatched timesheet IDs are not automatically invalid; (6) Bronze preserves source records, Silver reconciles identical cross-file overlaps with lineage; (7) tested three-column timesheet key is provisional; (8) hours_worked remains authoritative; (9) assumptions must be explicitly documented.

Open questions: why does the roster match less than 0.4% of distinct timesheet IDs? What do CMCRN-3 and TEST2 mean? Which missing markers are authoritative? Do pay-code/break rules explain hours discrepancies? Is the candidate key guaranteed by the source system? What exact Gold grain/KPIs are required? Which profiling outputs and docs currently exist in Git?

## 13. Handoff to the next chat: next phase

Phase 1 profiling is complete. Do not restart profiling. First put this report plus the data dictionary and business rules into the repository and verify documentation state. Then begin Phase 2 Bronze ingestion incrementally.

The proposed but unverified Bronze cell sets PROJECT_DIR = Path.cwd().parent, RAW_DIR = PROJECT_DIR / 'data' / 'raw', BRONZE_DIR = PROJECT_DIR / 'data' / 'bronze'; reads CSVs with dtype='string', keep_default_na=False, encoding='utf-8-sig'; adds _source_file and _source_row_number; writes copies under data/bronze; and creates ingestion_manifest.csv. The user has not yet run/shared output from this cell.

Bronze validation gate: confirm all five source files are found; compare source/Bronze row counts and expected source-column counts; check leading-zero IDs, empty strings, and literal [NULL] are preserved; check lineage fields and manifest; and confirm data/raw is unchanged. Troubleshoot any mismatch before Silver.

After Bronze passes: implement Silver in small verified steps—explicit missing/type rules, quality flags and exception outputs, unresolved roster matches, 30,839 cross-file overlap reconciliation, and before/after audit counts. Then finalize Gold model and analytics requirements.

NEW-CHAT PROMPT: Continue ASE ETL Assignment 1 from this report. Project root D:\ASE-ETL on Windows. We chose Medallion Architecture (Bronze → Silver → Gold), replacing normal ETL. Phase 1 data understanding/profiling is complete. Do not restart profiling or jump ahead. First help place/verify Phase 1 docs in the repo; then proceed incrementally to Bronze ingestion using the proposed cell above, whose output is not yet verified. Preserve raw files, IDs as strings, lineage, and unresolved records. Do not assume unmatched IDs are invalid or recalculate hours_worked from punch duration. Use the assignment PDF and existing project files if available, and verify each stage before moving on.
