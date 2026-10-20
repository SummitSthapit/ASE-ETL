# ASE-ETL — Phase 4: Silver Layer Implementation Report

**Project:** ASE-ETL Data Engineering Assignment 1  
**Architecture:** Bronze–Silver–Gold Medallion  
**Phase:** 4 — Silver (cleaning, validation, deduplication, and publication)  
**Status:** Implemented and verified against the tests described below  
**Report date:** 14 October 2026  
**Next phase:** Gold relational modeling and SQL analytics

> **Evidence basis:** This report records the implementation decisions, executed scripts, and observed outputs shared during the ASE-ETL project. It is not an independent source-code audit or a claim that every possible failure mode has been tested. Where a design is planned rather than implemented, it is explicitly labeled.

## 1. Executive summary

Phase 4 converts raw Bronze extracts into reliable, typed, deduplicated Silver datasets. The processing path is **Bronze Parquet → transformations → validation/quarantine → timesheet deduplication → reconciliation → local Silver Parquet → MinIO snapshot + PostgreSQL Silver tables**. Source-file lineage is retained and discarded duplicate occurrences are audited.

The published Silver snapshot contains **50 employees** and **381,732 timesheets**. From the original **412,571 timesheet rows**, **30,839 repeated occurrences** were removed; **zero records** were rejected by the currently implemented critical validation rules. Six Parquet datasets are published to MinIO, with business tables in PostgreSQL. Byte-level local-versus-MinIO checks, exhaustive canonicalized local-Parquet-versus-PostgreSQL comparisons, idempotent rerun checks, publication consistency checks, and a PostgreSQL transaction rollback test all passed.

The major reference-data limitation is quantified: of **381,732 accepted timesheets**, only **509** (0.13%) match one of the 50 supplied employee profiles. The other **381,223** timesheets (99.87%) lack matching employee *profiles*, but are valid business records for many types of timesheet analysis. They are deliberately retained. The Gold design will separate observed employee identifiers from the optional profiles (subject to confirming consistent ID semantics).

## 2. Context, purpose, and boundaries

### 2.1 Position in the Medallion architecture

| Layer | Role | ASE-ETL implementation |
|---|---|---|
| Bronze | Preserve source data and ingestion lineage | CSV extracts ingested as Parquet, MinIO objects, and PostgreSQL Bronze records |
| **Silver** | Standardize, validate, reconcile, and deduplicate | This phase: cleaned Parquet; PostgreSQL tables; MinIO snapshots and audits |
| Gold | Curate normalized business model and analytics | **Not yet implemented**; follows this report |

Bronze preserves source representations, including text-based dates and values. Silver introduces explicit data types and quality rules without substituting guessed business facts. Gold will introduce business entities, relationships, appropriate analytic classifications, and SQL queries for assignment requirements.

### 2.2 Scope delivered

- Inspect Bronze schemas and determine Silver column/type rules.
- Normalize recognized textual missing-value markers, whitespace, dates, timestamps, and numeric fields.
- Retain employee and timesheet identifiers as strings.
- Exclude 12 fully missing/unusable/masked employee attributes from Silver (originals remain in Bronze).
- Enforce critical validation checks, collect quality issues, and write quarantine datasets.
- Deduplicate repeated timesheet records across overlapping source exports, retaining an audit.
- Validate uniqueness of the chosen business keys and reconcile every input record.
- Publish Silver Parquet and audits locally and to MinIO; publish clean business tables to PostgreSQL.
- Establish repeatable, content-addressed publication with a database registry.
- Test correctness, negative cases, consistency, rollback, and unchanged-input idempotency.
- Quantify the missing employee-profile reference coverage and define the Gold handoff.

**Not in scope:** Gold normalized dimensions, finished KPI definitions, pay-code classification, complete payroll-rule reconstruction, enterprise workflow scheduling, distributed two-phase commit, or a comprehensive failure-injection suite.

## 3. Source data and Bronze baseline

### 3.1 Input inventory

| Source extract | Bronze rows |
|---|---:|
| `employee_202510161125.csv` | 50 |
| `timesheet_202509151540.csv` | 269,921 |
| `timesheet_202510161121.csv` | 48,258 |
| `timesheet_202510161122.csv` | 55,435 |
| `timesheet_202510161124.csv` | 38,957 |
| **Timesheet subtotal** | **412,571** |
| **All source rows** | **412,621** |

Earlier Bronze verification counted **412,621 rows in 11 Parquet parts** across the five source datasets. This is consistent with 50 employee records plus 412,571 timesheet records.

### 3.2 Lineage fields

Each processed record carries: `source_file`, `source_row_number`, `ingested_at_utc`, and `ingestion_batch_id`. These fields connect Silver records and duplicate audits back to their source extract and Bronze ingestion event. They are **not** used as business attributes to decide whether two timesheets represent the same normalized content.

### 3.3 Reference-data limitation established in profiling

The employee extract has 50 unique `client_employee_id` values; the timesheet extracts include thousands of IDs missing from that small employee file. Missing descriptive profiles do **not** imply timesheet invalidity. Silver therefore does **not** impose a foreign key from all timesheets directly to the 50 employee profiles and does **not** reject timesheets merely because their IDs lack a profile.

## 4. Silver architecture and project organization

### 4.1 Logical data flow

```text
Bronze Parquet (five source datasets)
       |
       v
Read and concatenate by entity
       |
       v
Normalize values and assign types
       |
       v
Validate business/data-quality rules
       |----------------------> Rejected and quality-issue audit outputs
       v
Deduplicate timesheets --------> Duplicate-record audit output
       |
       v
Primary-key checks + row-count reconciliation
       |
       v
Local Silver Parquet (six datasets)
       |
       v
Content-addressed Silver publisher
       |----------------------> MinIO ase-silver/runs/<fingerprint>/...
       v
PostgreSQL silver.employees / silver.timesheets
       |
       v
PostgreSQL silver._publication_registry (active snapshot)
```

### 4.2 Implemented Python modules

| Module | Responsibility |
|---|---|
| `src/silver/config.py` | Input/output paths, missing markers, selected columns, date/numeric lists, metadata |
| `src/silver/transformations.py` | `transform_employees`, `transform_timesheets`, value/type normalization |
| `src/silver/validation.py` | `validate_employees`, `validate_timesheets`, critical errors and warnings |
| `src/silver/deduplication.py` | `deduplicate_timesheets`, deterministic survivors and duplicate audit |
| `src/silver/pipeline.py` | Execute processing, reconciliation, and write local Silver outputs |
| `src/silver/publish.py` | Initial MinIO/PostgreSQL integration and shared publishing helpers |
| `src/silver/publish_idempotent.py` | Fingerprinted MinIO snapshots and registry-based repeatable publication |

Silver code belongs under **`src/silver/`**. Bronze integration code is under `src/ingestion/bronze_ingestion.py`; the project-level `tests/` folder is **outside** `src/`.

### 4.3 Local outputs

```text
data/silver/
  employees/part-00000.parquet
  timesheets/part-00000.parquet
  audit/duplicate_records/part-00000.parquet
  audit/quality_issues/part-00000.parquet
  audit/rejected_employees/part-00000.parquet
  audit/rejected_timesheets/part-00000.parquet
```

Zero-row audit datasets are still written as Parquet artifacts, maintaining predictable paths for checks and publication.

## 5. Transformation specification and rationale

### 5.1 Missing-value and text standardization

Recognized textual placeholders and blank/whitespace-only values are converted to genuine missing values; surrounding whitespace is stripped. This avoids treating strings such as `"[NULL]"` as meaningful values. Missing numeric data is **not** silently replaced with zero. The configuration centralizes the missing-value policy for deterministic processing.

### 5.2 Employee projection: 38 to 26 columns

The employee Bronze schema has **38 columns**; Silver retains **26** (22 business columns plus four lineage fields). The following **12** attributes are excluded based on the source profiling evidence:

| Reason | Employee fields excluded from Silver |
|---|---|
| Entirely missing/unusable in supplied extract | `middle_name`, `years_of_experience`, `zip`, `manager_employee_name`, `is_per_deim`, `work_phone`, `clinical_level` |
| Entirely masked or not analytically meaningful in extract | `address`, `city`, `state`, `country`, `cell_phone` |

The original data remains available in Bronze. The excluded-column set is fixed in configuration rather than inferred afresh from each new input batch. Partially available attributes such as termination-related data are retained where useful.

The employee transformation converts appropriate employment-related dates and `scheduled_weekly_hour` into typed values. `client_employee_id` is a **string** so leading zeros are preserved. The source data alone is insufficient to recover anonymized personal details; the pipeline does not attempt to do so.

### 5.3 Timesheet projection: 18 columns

All **14 timesheet business columns** are retained along with the four lineage fields, giving **18 Silver columns**. Transformations include:

- `hours_worked` → numeric type;
- `punch_apply_date` → date;
- punch-in/punch-out and scheduled start/end values → timestamps;
- ID fields and pay codes → text identifiers/codes, avoiding destructive integer conversion;
- normalized missing-value treatment.

**Reported hours are preserved.** The source contains cases where punch-duration calculations differ from reported hours. Without validated payroll rules (e.g., break deduction, rounding, pay-code treatment, manual adjustment), replacing recorded hours with a calculated difference would fabricate a potentially incorrect business value. Overnight shifts are not automatically labeled invalid simply because they cross midnight.

## 6. Validation and quarantine

Validation is run **after** type normalization and **before** the deduplication/accepted-output stage. Rules are implemented in `src/silver/validation.py` and divide findings into:

- **Critical:** invalid required identifiers, essential dates/numerics, negative hours, impossible punch ordering, or other enforced rules; affected rows are quarantined from the accepted dataset.
- **Warning:** observations that may be legitimate in the payroll context but deserve review; retain the record and report the issue (for example certain missing punch information).

Examples tested synthetically include missing employee IDs, invalid employee dates, invalid/non-numeric timesheet hours, negative hours, and reversed punch timestamps. A warning does not automatically mean that a payroll event is wrong.

### 6.1 Observed results

| Outcome | Employees | Timesheets |
|---|---:|---:|
| Input rows | 50 | 412,571 |
| Critical validation rejects | 0 | 0 |
| Logged quality issues in generated audit | 0 | 0 |

These zero results mean the data satisfied the **implemented** checks; they are not a guarantee that every potential business anomaly has been detected. Duplicate removals are counted **separately** from validation rejections.

## 7. Cross-file timesheet deduplication

### 7.1 Why deduplication was required

The four timesheet exports overlap. Repeated business records appeared across separate files; none were identified as within-file duplicates in the profiling performed. Without deduplication, summed hours and record-count metrics would be overstated.

### 7.2 Match rule and survivor selection

Duplicates are determined using **all 14 normalized timesheet business columns**, omitting the four ingestion metadata fields. Distinct source locations are therefore not sufficient to make otherwise identical business rows different events. The survivor selection is deterministic, preferring the latest source-export filename with documented tie-breakers (source row and batch information). The dropped copies appear in `audit/duplicate_records`, with retained-record identification for traceability.

This approach is more conservative than deduplicating on `(client_employee_id, punch_apply_date)` alone: multiple legitimate events can share the same employee and day. It does depend on an assumption that identical 14-column business rows represent repeated exported occurrences, because the source lacks a globally unique transaction ID.

### 7.3 Observed duplicate analysis

| Source file associated with excluded occurrences | Excluded rows |
|---|---:|
| `timesheet_202510161121.csv` | 23,892 |
| `timesheet_202510161122.csv` | 6,947 |
| **Total excluded** | **30,839** |

The 30,839 duplicate groups each had **exactly two copies**. Deduplication retained one occurrence per group.

### 7.4 Reconciliation

| Timesheet reconciliation measure | Count |
|---|---:|
| Bronze timesheet records | 412,571 |
| Critical validation rejects | 0 |
| Extra duplicate occurrences excluded | 30,839 |
| **Accepted unique Silver timesheets** | **381,732** |

**Check:** `412,571 = 0 + 30,839 + 381,732`. Employee reconciliation is `50 = 0 + 50`.

## 8. Keys and relational integrity

### 8.1 Silver employees

`client_employee_id` is the primary key for `silver.employees`; observed row count is 50.

### 8.2 Silver timesheets

The empirically verified composite key is:

```sql
PRIMARY KEY (
    client_employee_id,
    punch_in_datetime,
    punch_out_datetime
)
```

After deduplication, tests found **zero duplicate composite keys** and **zero NULL key components** in the accepted timesheets. The PostgreSQL tables enforce these key constraints. The full-row deduplication rule and the primary-key rule serve different purposes: the former eliminates overlapping source export occurrences; the latter establishes valid row identity.

### 8.3 Why there is no direct Silver foreign key to employees

The 50-row employee file is a limited profile extract. Most valid timesheet IDs are absent from it. A foreign key from every timesheet directly to `silver.employees` would either reject most records or require invented employee profiles. Neither is acceptable. Proper referential integrity without data loss is a **Gold design task**, addressed in Section 12.

## 9. Local and dual-storage publication

### 9.1 Storage roles

| Storage | Role |
|---|---|
| Local `data/silver/` | Deterministic staged Parquet outputs and local validation |
| MinIO bucket `ase-silver` | Immutable-by-convention fingerprinted snapshots, six datasets, manifest, auditable lineage |
| PostgreSQL schema `silver` | SQL-queryable `employees`, `timesheets`, and active publication registry |

The PostgreSQL environment configured for the project uses database `ase_etl`, user `ase_user`, and Docker host port **5433** (container port **5432**). Runtime secrets are read from `.env` and are **not** reproduced in this report. MinIO uses host API port 9000, with console at 9001 in the project's Compose setup.

### 9.2 Initial publication and refinement

The initial `src/silver/publish.py` established working MinIO upload, PostgreSQL loading, row-count checks, and primary-key enforcement. Its first run used a UUID-prefixed MinIO snapshot (`7936e99d6fbf41c2a8510fc1f00241b3`). Subsequently, `src/silver/publish_idempotent.py` improved repeatability and publication consistency.

### 9.3 Content-addressed snapshot identity

The verified content fingerprint of the current local Silver Parquet artifacts is:

```text
36d5173eb09290a43d424ed9aabd2ebd94a6a241e1a9d967c6d9928dfc0923cb
```

The current MinIO snapshot is stored beneath `ase-silver/runs/<fingerprint>/...`. An identical set of Parquet **bytes** yields the same fingerprint and prefix; a different serialization of semantically identical data can yield a different fingerprint.

### 9.4 Manifest, registry, and publication semantics

The MinIO `manifest.json` records snapshot fingerprint, object prefix, datasets, row counts, and status `prepared`. PostgreSQL's singleton `silver._publication_registry` identifies the **committed active** fingerprint, MinIO prefix, counts, and publication time. The registry is treated as the authority for the published snapshot; a prepared MinIO prefix alone is **not** proof of an active database publication.

The improved publisher uses a transaction-scoped PostgreSQL advisory lock. If the active fingerprint and the expected row counts already match, it skips the PostgreSQL reload; otherwise it updates the Silver tables and registry within a PostgreSQL transaction. It **does not** provide an atomic two-system commit between MinIO and PostgreSQL. A prepared snapshot may remain in MinIO if database publication fails; downstream clients should use the committed registry, not infer completion from object presence.

### 9.5 Idempotent-rerun evidence

| Execution | MinIO behavior | PostgreSQL behavior |
|---|---|---|
| First content-addressed publication | Upload or validate snapshot objects | Published tables; `PostgreSQL changed: True` |
| Repeat with unchanged files | Reuse/verify snapshot | Skip reload; `PostgreSQL changed: False` |

The unchanged-run fast path checks the fingerprint and expected table counts. It **does not** automatically re-hash all PostgreSQL rows on every publish. Separate cross-storage tests covered row-level equivalence.

## 10. Verification and test evidence

| Test/module | What was checked | Observed result |
|---|---|---|
| `tests/test_silver_deduplication.py` | Expected deduplication counts, uniqueness, repeat deduplication | Passed |
| `tests/test_silver_integrity.py` | Nine integrity checks incl. schema, keys, lineage, counts, types | All 9 passed |
| `tests/test_silver_negative_cases.py` | Seven constructed invalid/repeat cases | All 7 passed |
| `tests/test_silver_storage.py` | MinIO-vs-local SHA-256 on all six Parquet datasets; DB counts and PK constraints | Passed |
| `tests/test_silver_cross_storage.py` | Every employee and timesheet business/lineage value in local Parquet vs PostgreSQL, with canonical type comparison | 50 + 381,732 rows matched |
| `tests/test_silver_publication.py` | MinIO manifest vs PostgreSQL registry, prefix/fingerprint, counts, rollback | All publication checks passed |

### 10.1 Publication verification output

Final executed command:

```powershell
python -m tests.test_silver_publication
```

Observed output included:

```text
PASS: Manifest matches publication registry
PASS: PostgreSQL counts match registry
PASS: Current snapshot prefix is consistent
Simulated failure triggered
PASS: Transaction rolled back
PASS: Original employee value preserved
ALL PUBLICATION CHECKS PASSED
```

The transaction test made a temporary employee-name change within an uncommitted database transaction, raised a simulated exception, and verified rollback left the committed employee value unchanged. This is evidence for the tested PostgreSQL transaction behavior, **not** a full cross-system outage/recovery guarantee.

### 10.2 Consistency chain and what it proves

1. Local Parquet ↔ MinIO: six published files had matching SHA-256 hashes.
2. Local Parquet ↔ PostgreSQL: all 50 employee rows and all 381,732 timesheets matched after canonicalization of strings, numeric values, dates, and timestamps.
3. PostgreSQL registry ↔ MinIO manifest: active fingerprint, prefix, dataset descriptions, and expected counts were consistent.
4. Repeat publication: unchanged input did not reload PostgreSQL tables.

Together these establish strong evidence for **this tested snapshot** while leaving infrastructure and future-data edge cases outside the tested scope.

## 11. Measured employee-profile coverage limitation

### 11.1 SQL coverage analysis performed against Silver

The match analysis used a left join between `silver.timesheets` and `silver.employees` by `client_employee_id`, grouping accepted timesheets into matched and unmatched groups. A second query counted supplied profiles with or without any timesheet.

| Match status | Timesheet rows | Distinct timesheet employee IDs | Sum of reported hours |
|---|---:|---:|---:|
| MATCHED | 509 | 29 | 4,293.08 |
| UNMATCHED | 381,223 | 10,744 | 3,571,997.91 |
| **Total** | **381,732** | **10,773** | **3,576,290.99** |

| Employee-profile measure | Count |
|---|---:|
| Profiles in employee extract | 50 |
| Employee profiles with at least one timesheet | 29 |
| Employee profiles without timesheets in these extracts | 21 |
| Timesheet-only distinct employee IDs | 10,744 |
| **Distinct IDs across both inputs (set union)** | **10,794** |

**Interpretation:** only about **0.13% of timesheet rows** (509/381,732) and **0.12% of summed reported hours** have a matching supplied employee profile. Approximately **99.87% of rows** and **99.88% of reported hours** do not. The 50-row employee file is not a complete reference set for all timesheet IDs.

### 11.2 Why unmatched records were retained

An unmatched timesheet may still contain reported hours, dates, pay codes, and potentially department information and is therefore valid input for analyses based on those fields. Excluding unmatched timesheets would remove almost all the supplied timesheet population and dramatically understate whole-timesheet totals. Lack of an employee profile is a **reference-data coverage problem**, not sufficient evidence of an invalid timesheet.

Profile-dependent analytics (e.g., hours by job title, hire cohort, or employee employment status) must state that only matched profiles are represented, or explicitly classify unmatched IDs as `Unknown` without inventing their attributes. **Reported hours** should not automatically be relabeled **physically worked hours**, nor should these extracts be assumed to describe the full organization without a source-scope definition.

### 11.3 Example SQL to reproduce the match audit

```sql
WITH coverage AS (
    SELECT t.client_employee_id, t.hours_worked,
           CASE WHEN e.client_employee_id IS NULL
                THEN 'UNMATCHED' ELSE 'MATCHED' END AS match_status
    FROM silver.timesheets AS t
    LEFT JOIN silver.employees AS e
      ON e.client_employee_id = t.client_employee_id
)
SELECT match_status,
       COUNT(*) AS timesheet_records,
       COUNT(DISTINCT client_employee_id) AS unique_employee_ids,
       ROUND(SUM(hours_worked), 2) AS total_reported_hours
FROM coverage
GROUP BY match_status
ORDER BY match_status;
```

```sql
SELECT COUNT(*) AS total_employee_profiles,
       COUNT(*) FILTER (
           WHERE EXISTS (
               SELECT 1 FROM silver.timesheets t
               WHERE t.client_employee_id = e.client_employee_id
           )
       ) AS employees_with_timesheets,
       COUNT(*) FILTER (
           WHERE NOT EXISTS (
               SELECT 1 FROM silver.timesheets t
               WHERE t.client_employee_id = e.client_employee_id
           )
       ) AS employees_without_timesheets
FROM silver.employees e;
```

## 12. Explicit handoff to Gold (design recommendation, not implemented)

The following is the **agreed direction for Gold design**, subject to confirming that employee-ID namespaces and semantics are consistent across sources:

1. **`gold.employee_identity`** — one row per distinct `client_employee_id` observed in either source, estimated **10,794** rows for this snapshot; do not invent personal attributes.
2. **`gold.employee_profiles`** — the **50** supplied descriptive employee rows, linked optionally/one-to-one to observed identities.
3. **`gold.timesheets`** — retain **all 381,732** accepted timesheets; foreign key to `gold.employee_identity`, not to the incomplete profiles table.
4. **Coverage view/report** — a SQL view or audit report to identify IDs and records without available employee profiles; no need to physically split primary matched/unmatched timesheets.
5. **Additional normalized entities** — investigate `department`, `pay_code`, and other source-supported entities, attribute consistency, and functional dependencies before final DDL.
6. **Analytics** — build whole-timesheet metrics separately from profile-dependent metrics, explicitly labeling coverage and pay-code assumptions.

The Silver composite key is verified for this snapshot. Gold should revalidate it when implementing any transformations that could change record grain. Do not infer department–employee relationships or job titles not present in the source.

## 13. Reproducibility and operations

### 13.1 Environment

From Windows PowerShell, with Docker Desktop running and the project virtual environment active, work in `D:\ASE-ETL`. Docker Compose brings up PostgreSQL and MinIO. The project `.env` contains connection settings, including `DATABASE_URL`, MinIO endpoint/credentials, and `SILVER_BUCKET=ase-silver`. **Never commit secret values to Git.**

### 13.2 Commands

```powershell
docker compose up -d
python -m src.silver.pipeline
python -m src.silver.publish_idempotent
python -m tests.test_silver_deduplication
python -m tests.test_silver_integrity
python -m tests.test_silver_negative_cases
python -m tests.test_silver_storage
python -m tests.test_silver_cross_storage
python -m tests.test_silver_publication
```

Use these commands from the project root with the expected dependencies installed. Transformation and publishing are **separate invocations**, not an automatically scheduled end-to-end job. `docker compose down -v` should be avoided when the intent is to preserve PostgreSQL/MinIO named-volume data.

### 13.3 PostgreSQL verification

```sql
SELECT COUNT(*) AS employees FROM silver.employees;
SELECT COUNT(*) AS timesheets FROM silver.timesheets;
SELECT * FROM silver._publication_registry;
```

Expected accepted counts: **50**, **381,732**. pgAdmin must connect to the Docker-host-mapped PostgreSQL instance at port **5433**, database **`ase_etl`**, not an unrelated local default `postgres` database. Inside the container, PostgreSQL reports server port **5432**.

## 14. Known limitations, assumptions, and future enhancements

| Topic | Current state / risk | Possible future action |
|---|---|---|
| Employee-reference completeness | 99.87% of timesheets have no supplied employee profile | Gold identity/profile model, visible coverage reporting |
| Source transaction identity | No globally unique timesheet event ID supplied | Document full-business-row dedup assumption; seek source ID if available |
| Hours discrepancies | Reported hours can differ from punch duration | Define business rules and create non-destructive discrepancy metrics |
| Pay-code semantics | All reported hours retained; pay codes not fully classified | Obtain definitions before worked-hours/overtime KPIs |
| Cross-storage transactions | MinIO and PostgreSQL lack shared atomic commit | Registry-based publication contract, eventual retry/repair plan |
| Publication shortcut | Fingerprint + counts do not detect in-place DB changes with unchanged count | Optional row-hash/periodic reconciliation, stricter immutability controls |
| Failures and recovery | Tested transaction rollback; not full infrastructure fault injection | Test MinIO interruption, partial publish, crash/restart, changed snapshots |
| Pipeline automation | Commands executed manually | Future orchestration/scheduling if assignment scope calls for it |
| ID meaning | Match by `client_employee_id`; consistency across sources not formally proven | Confirm identifier namespaces before Gold foreign keys |
| Input change management | Findings verified for current exports | Re-profile/verify with new source snapshots |

## 15. Completion criteria and phase closure

Phase 4 meets its agreed implementation and verification acceptance criteria for the supplied dataset:

- [x] Read Bronze data and preserve lineage.
- [x] Normalize values, project appropriate fields, and assign explicit data types.
- [x] Implement critical/warning validation and quarantine output paths.
- [x] Resolve overlapping cross-file timesheet occurrences with auditability.
- [x] Enforce and verify employee and timesheet primary keys.
- [x] Reconcile the full input population with accepted, duplicated, and rejected counts.
- [x] Store six Silver Parquet datasets locally and in MinIO.
- [x] Publish SQL-queryable PostgreSQL Silver business tables and an active registry.
- [x] Verify file hashes, row values, counts, registry consistency, unchanged-run idempotency, and tested rollback.
- [x] Quantify employee-profile coverage and document the no-data-loss decision.
- [ ] Implement the normalized Gold schema, analytics, and final assignment deliverables (**next phase**).

**Conclusion:** The tested Silver snapshot is ready to serve as Gold's input. Gold must explicitly preserve all valid timesheets, model the incomplete employee reference data without fabrication, and document the coverage of any profile-dependent analysis.

---

**Report note:** Exact employee field spelling, pipeline semantics, and test outcomes are recorded from the project's prior implementation discussions. For a final code-to-document compliance audit, compare this report with the checked-in source files and the original assignment rubric; no claim of rubric-by-rubric independent verification is made here.
