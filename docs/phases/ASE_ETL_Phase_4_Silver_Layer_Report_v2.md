# ASE-ETL — Phase 4: Silver Layer Implementation Report (v2)

**Project:** ASE-ETL — Data Engineering Assignment 1  
**Architecture:** Bronze → Silver → Gold  
**Phase:** 4 — Silver data cleaning, quality validation, schema evolution and publication  
**Report date:** 15 October 2026  
**Status:** Silver v2 implemented, published, and verified for the supplied source snapshot  
**Next phase:** Gold relational design and assignment SQL analytics

> **Evidence and scope.** This report consolidates the implementation design established during the ASE-ETL project and the terminal outputs supplied for testing and publication. It is not an independent audit of every file in the user's current local working tree or exhaustive verification of future datasets. Gold recommendations are proposals, not implemented tables.

## 1. Executive summary

Silver converts Bronze source extracts into clean, consistently typed, deduplicated, traceable records. **Silver v2 supersedes the earlier 26-column employee projection**: it preserves **all 34 original employee business columns and four ingestion metadata columns** (38 total), even where the current source extract contains only NULL or masked values. Known missing markers and masking placeholders are normalized; usable future values in those same columns can be retained without redesigning the schema.

The complete flow is: **per-file Bronze schema checks → schema alignment and audit → transformations → value validation → critical-record quarantine → timesheet duplicate reconciliation → key and row-count validation → local Parquet → content-addressed MinIO snapshot → transactional PostgreSQL migration/publication**.

The published v2 snapshot contains **50 employees**, **381,732 accepted timesheets**, **30,839 duplicate occurrences audited**, **zero critical rejects**, **zero recorded quality issues** under the implemented validation rules, and **11 schema-evolution audit rows**. All **seven** Silver datasets were uploaded and verified in MinIO. Local Parquet and PostgreSQL matched for **every column of all 50 employee and 381,732 timesheet rows**. The publication manifest matched the PostgreSQL registry, the tested PostgreSQL rollback succeeded, and a repeat publish skipped the reload (`PostgreSQL changed: False`).

**Active Silver v2 fingerprint:** `a016e5a75dfe0ad263a7ed5f229368624932da29be8c7278c8a5f7f8921675b7`.

The employee roster has very limited coverage of timesheet IDs: **381,223 of 381,732 accepted timesheets (99.87%)** do not have a matching profile among the 50 employee records. These timesheets are retained, not rejected; the Gold design must make this coverage gap explicit.

## 2. Input data and established Bronze baseline

| Source extract | Records | Business columns |
|---|---:|---:|
| `employee_202510161125.csv` | 50 | 34 |
| `timesheet_202509151540.csv` | 269,921 | 14 |
| `timesheet_202510161121.csv` | 48,258 | 14 |
| `timesheet_202510161122.csv` | 55,435 | 14 |
| `timesheet_202510161124.csv` | 38,957 | 14 |
| **Timesheet total** | **412,571** | — |
| **Overall Bronze total** | **412,621** | — |

Bronze Parquet consists of **11 parts**: one employee part plus ten timesheet parts. The September export occupies six parts; October exports occupy four. Bronze preserves raw representations and four technical lineage columns: `source_file`, `source_row_number`, `ingested_at_utc`, and `ingestion_batch_id`.

Bronze remains the authoritative record of source-supplied values. Silver is an approved, cleaned interpretation rather than a replacement for the raw record.

## 3. Why we revised Silver v1

Silver v1 excluded 12 employee attributes because they were empty or masked in the current extract: `middle_name`, `years_of_experience`, `zip`, `manager_employee_name`, `is_per_deim`, `work_phone`, `clinical_level`, `address`, `city`, `state`, `country`, `cell_phone`. Although retained in Bronze, those attributes would continue to be dropped from Silver even if future extracts supplied meaningful values.

**Silver v2 decision:** preserve all approved source business fields in the Silver schema, regardless of whether the current snapshot contains usable values; normalize missing or masked *values* without removing the *columns*. Gold will choose attributes necessary for each relational model, metric, and business view.

| Entity | Bronze columns including lineage | Silver v1 | **Silver v2** |
|---|---:|---:|---:|
| Employees | 38 | 26 | **38** |
| Timesheets | 18 | 18 | **18** |

This is a controlled schema revision, not a new ingestion architecture. No Bronze reingestion was required. Historical v1 MinIO snapshots were not deleted.

## 4. Silver v2 workflow and code structure

```text
Bronze Parquet: 1 employee part + 10 timesheet parts
    │
    ├── Per-part approved schema contract and failure diagnostics
    │        └── schema-evolution audit: 11 inspected parts
    ▼
Align nullable optional columns; standardize column order
    ▼
Normalize missing/masked values; convert approved data types
    ▼
Validate values; quarantine critical failures; log warnings
    ▼
Deduplicate timesheets using all 14 business columns
    │        └── duplicate audit: 30,839 excluded occurrences
    ▼
Primary-key validation and count reconciliation
    ▼
Local Silver v2: seven Parquet datasets
    ▼
MinIO: ase-silver/runs/<content-fingerprint>/ with manifest
    ▼
PostgreSQL: transactional 12-column migration + data reload
    ▼
Registry identifies active published v2 snapshot
```

| Module | Role |
|---|---|
| `src/silver/config.py` | Paths, recognized missing markers, typed-field configuration, timesheet business columns, lineage |
| `src/silver/schema_contract.py` | Approved ordered columns, required/optional fields, schema version `2.0`, schema drift detection/alignment |
| `src/silver/schema_audit.py` | Structured per-Parquet-part schema inspection records |
| `src/silver/schema_failures.py` | Persistent JSON diagnostics for rejected schema changes |
| `src/silver/transformations.py` | Missing/mask normalization, type conversion, employee/timesheet transforms |
| `src/silver/validation.py` | Critical/warning value checks and employee experience validation |
| `src/silver/deduplication.py` | Deterministic cross-export business-row deduplication and lineage audit |
| `src/silver/pipeline.py` | Per-part load, validation, audit accumulation, reconciliation, staged Parquet outputs |
| `src/silver/migrations.py` | Controlled additive PostgreSQL migration for 12 restored employee columns |
| `src/silver/publish.py` | Shared connection, dataset inventory, file and SQL type helpers; original publisher implementation |
| `src/silver/publish_idempotent.py` | Silver v2 preflight, content-addressed MinIO publishing, transactional database and registry update |

The pipeline (`python -m src.silver.pipeline`) and publisher (`python -m src.silver.publish_idempotent`) remain separate, manually invoked operations.

## 5. Schema contract and evolution policy

**Contract version:** `2.0`. The approved employee schema contains the 34 named source business attributes plus four lineage columns. The timesheet schema retains its 14 business fields plus four lineage columns. Each Bronze Parquet **part is inspected before concatenation** to avoid hiding per-file differences.

| Input condition | Implemented behavior |
|---|---|
| Exact approved columns | Accept; reorder to contract order if necessary |
| Approved optional column absent | Insert nullable column, continue, record `ALIGNED_OPTIONAL` in audit |
| Required column absent | Raise `SchemaDriftError`; stop rather than silently continue |
| Unexpected/unapproved column | Raise `SchemaDriftError` pending contract review |
| Duplicate column names | Reject schema |
| Incompatible numeric/date values | Convert under explicit rules and detect invalid nonmissing source values through validation |
| Completely novel data types or changed field semantics | **Not automatically accepted**; require review/contract update |

For this supplied snapshot, all **11** parts matched the v2 contract exactly; **zero** optional-column alignments were needed. The emitted successful audit includes `dataset`, portable relative `source_name` (such as `employee_202510161125/part-00000.parquet`), `schema_version`, `source_rows`, `schema_status`, and JSON-encoded `missing_optional_columns`. The audit reconciles to all **412,621** Bronze records.

Schema rejection writes a JSON diagnostic to `data/diagnostics/silver/schema_failures/`, outside the active Silver snapshot, including source identifier, schema version, error type and UTC detection timestamp. Synthetic tests confirmed separate diagnostic files for separate failures. Diagnostics record structural errors, not source row contents. These failure records are **local diagnostics**, not a fully centralized operational event log.

**Limit:** this is controlled schema evolution, not an automatic general-purpose schema inference engine. Column presence, known type conversions, and their validation are covered; unfamiliar source formats and newly introduced attributes require explicit review.

## 6. Transformation and data-quality rules

### 6.1 Employee records

Silver v2 retains all 38 columns. Recognized blank and textual missing markers (e.g., `[NULL]`, `NULL`, `N/A`) become actual missing values; surrounding whitespace is normalized. Employee identifiers, manager/organization/department codes, ZIP codes, and phone numbers remain **strings** to avoid dropping leading zeros.

Recognized entirely masked values made of `X`/`x` in `address`, `city`, `state`, and `country` are converted to NULL. In the configured phone fields, a value containing an `X`/`x` placeholder is treated as incomplete and converted to NULL. These rules are **column-specific**: a legitimate city like `Oxford` is retained. Original masked and unmasked raw values remain in Bronze. Contact fields should be handled with access controls appropriate to potentially personal data when future inputs are populated.

Employment and related dates are converted to date values. `scheduled_weekly_hour` and the restored `years_of_experience` are nullable numeric values, with PostgreSQL mapping `NUMERIC(12,2)`. Nonmissing nonnumeric experience values and negative experience values produce **critical validation findings**. Missing experience is valid. `is_per_deim` and `clinical_level` remain strings until source-defined permissible domains are established; no speculative Boolean/code mapping is applied.

### 6.2 Timesheet records

All 18 fields are retained. `hours_worked` is converted to numeric; `punch_apply_date` to date; punch-in, punch-out, scheduled start and scheduled end to timestamps; identifiers/pay codes remain strings. Reported hours are **not overwritten** using elapsed punch time, because unconfirmed break, rounding and payroll-code rules could make that derived value wrong. Valid overnight shifts are preserved.

### 6.3 Validation, quarantine and audit results

The value validator detects missing/invalid critical identifiers and dates, invalid/negative hours, reversed punch ordering, and the new experience-specific conditions. Missing punches may be warning-level depending on the situation; warning-only rows remain eligible. Critical rows are routed to rejected-record Parquet datasets and accounted for by reconciliation. The supplied snapshot produced **zero rejects and zero logged quality issues** under the implemented rules. This is not a certification of business-level payroll correctness.

## 7. Deduplication, keys and reconciliation

Timesheet source exports overlap. Duplicate occurrences are identified by equality of **all 14 normalized timesheet business columns**, intentionally excluding the four lineage fields. The deterministic survivor is selected by the source-export ordering, with source row/batch tie-breakers; removed occurrences retain survivor lineage in `audit/duplicate_records`. Empirical profiling found **30,839 groups of two**, with duplicates across exports rather than within each export.

| Timesheet reconciliation | Count |
|---|---:|
| Bronze timesheet occurrences | 412,571 |
| Critical rejects | 0 |
| Discarded duplicate occurrences (audited) | 30,839 |
| **Accepted Silver timesheets** | **381,732** |

Employee reconciliation: `50 Bronze = 50 Silver + 0 rejected`. Timesheet reconciliation: `412,571 = 381,732 + 30,839 + 0`.

The verified PostgreSQL primary keys remain `silver.employees(client_employee_id)` and `silver.timesheets(client_employee_id, punch_in_datetime, punch_out_datetime)`. Full-business-row deduplication removes repeated export occurrences; primary keys enforce uniqueness of accepted relational rows. Because no global timesheet transaction ID was supplied, treating fully identical business rows as repeated events is a documented assumption.

## 8. Seven Silver v2 output datasets

```text
data/silver/
  employees/part-00000.parquet
  timesheets/part-00000.parquet
  audit/duplicate_records/part-00000.parquet
  audit/quality_issues/part-00000.parquet
  audit/rejected_employees/part-00000.parquet
  audit/rejected_timesheets/part-00000.parquet
  audit/schema_evolution/part-00000.parquet
```

| Dataset | Verified rows |
|---|---:|
| `employees` | 50 |
| `timesheets` | 381,732 |
| `audit/duplicate_records` | 30,839 |
| `audit/quality_issues` | 0 |
| `audit/rejected_employees` | 0 |
| `audit/rejected_timesheets` | 0 |
| **`audit/schema_evolution`** | **11** |

Zero-row audits are deliberately retained as Parquet datasets. The schema-evolution audit records 11 parts, **not** 11 source CSV files (there are five source extracts). Local output replacement is staged, but swapping each output directory is not a single atomic multi-directory operation.

## 9. MinIO and PostgreSQL publication

### 9.1 Storage and active snapshot

- **MinIO bucket:** `ase-silver`; immutable-by-convention, content-addressed path `runs/<snapshot_fingerprint>/...`, including all seven Parquet datasets and `manifest.json`.
- **PostgreSQL:** schema `silver`, tables `employees` (50), `timesheets` (381,732), and singleton `_publication_registry`.
- **Active v2 fingerprint:** `a016e5a75dfe0ad263a7ed5f229368624932da29be8c7278c8a5f7f8921675b7`.
- **Historical v1 fingerprint:** `36d5173eb09290a43d424ed9aabd2ebd94a6a241e1a9d967c6d9928dfc0923cb`, retained in its previous MinIO prefix.

A fingerprint is computed from serialized Parquet **bytes**. The same business values can result in different fingerprints when their Parquet serialization differs; it is not itself a semantic business-record hash.

### 9.2 Controlled PostgreSQL v2 migration

Before publication, the Silver v1 table had **26 columns**. A read-only preflight verified database `ase_etl`, user `ase_user`, both table counts and primary keys, and the active v1 registry fingerprint. A real PostgreSQL migration was tested inside a transaction, adding all 12 missing nullable columns, checking count retention, then rolling back; the post-rollback database returned to **26 columns** and retained its v1 fingerprint.

The actual v2 publication successfully added the **12 columns** (including numeric `years_of_experience`) and reloaded the business tables in the PostgreSQL transaction. Table counts, primary keys, and the publication registry were verified before commit. The previous raw Bronze data and historical MinIO snapshot were not destroyed.

### 9.3 Manifest, registry and idempotency

The MinIO manifest contains `snapshot_fingerprint`, `prefix`, **`schema_version: "2.0"`**, counts, dataset names and `status: "prepared"`. PostgreSQL `_publication_registry` records the committed active fingerprint, prefix, counts, publication time and **`schema_version`**. **The registry, not the mere presence of a MinIO object, identifies the active published database snapshot.**

The publisher verifies the local v2 contract and all seven datasets before upload; MinIO objects are re-read and checked by SHA-256. PostgreSQL publication uses an advisory transaction lock, applies additive migration where required, loads the two business tables, verifies counts/keys, then updates the registry. When the same fingerprint, schema version and approved employee column set are present, it verifies counts and skips the PostgreSQL reload. An observed unchanged rerun reported **`PostgreSQL changed: False`** with the same v2 fingerprint.

**Transactional boundary:** PostgreSQL schema/data/registry changes are grouped in one database transaction. MinIO and PostgreSQL do **not** share a distributed atomic transaction. MinIO can retain a `prepared` snapshot if the subsequent database transaction fails. The current fast path does not compare all PostgreSQL field values on each run; an independent cross-storage verification was executed for this snapshot.

## 10. Verification evidence

The tests below were executed or reported during the v2 revision. Passing one scenario does not imply all possible new-schema or infrastructure failure scenarios have been tested.

| Verification | Observed evidence |
|---|---|
| Schema-contract tests | 7 passed: ordering, optional/required changes, unknown and duplicate columns, future values |
| Employee-v2 transformation tests | 7 passed: 38 columns, masks, valid future attributes, Bronze immutability |
| Schema integration tests | 4 passed; 50 employee and 412,571 timesheet input records |
| Schema-audit tests | 4 passed; portable relative source paths and 11 parts |
| Employee-v2 validation tests | 5 passed; valid, invalid, negative and NULL experience |
| Schema failure diagnostics | 3 passed; persisted distinct JSON failures and rejection behavior |
| Future-schema compatibility | 5 passed; optional numeric field absence, populated fields, type errors, order |
| Publisher configuration tests | 3 passed; seven datasets and employee numeric SQL mappings |
| Isolated full pipeline | Passed: all seven outputs, restored 38-column schema, current v1 outputs byte-identical afterward |
| Transactional schema migration | Passed: 12 columns added within transaction and successfully rolled back |
| Final local pipeline + integrity | Passed: reconciled counts, keys, lineage, types, seven outputs |
| Final PostgreSQL vs Parquet comparison | Passed **every column of all 50 employees and 381,732 timesheets** |
| Final manifest/registry publication test | Passed: fingerprint/prefix/counts/version consistency and PostgreSQL rollback |
| Unchanged-input second publication | Passed: identical fingerprint and **`PostgreSQL changed: False`** |

**Observed final publication verification:**

```text
PUBLICATION CONSISTENCY
PASS: Manifest matches publication registry
PASS: PostgreSQL counts match registry
PASS: Current snapshot prefix is consistent

TRANSACTION ROLLBACK TEST
Simulated failure triggered
PASS: Transaction rolled back
PASS: Original employee value preserved

ALL PUBLICATION CHECKS PASSED
```

The final cross-storage test checked the two **business** datasets. MinIO upload verification covered **all seven** Parquet datasets. The registry and manifest tests checked the publication metadata; this is not equivalent to exhaustive row-by-row SQL comparison of audit datasets, which remain in object storage.

## 11. Employee-profile coverage: why unmatched timesheets stay

SQL analysis of the cleaned Silver tables established:

| Coverage status | Timesheet rows | Distinct timesheet IDs | Sum of reported hours |
|---|---:|---:|---:|
| Profile matched | 509 | 29 | 4,293.08 |
| No matching profile | 381,223 | 10,744 | 3,571,997.91 |
| **Total** | **381,732** | **10,773** | **3,576,290.99** |

The source employee file has **50 profiles**: 29 have at least one matched timesheet, and 21 do not. There are **10,794 distinct employee IDs** across the union of both sources: 10,773 timesheet IDs plus 21 employee-only IDs.

Missing profiles are **not grounds for dropping valid timesheets**. Without the unmatched rows, we would exclude **99.87%** of records and almost all reported hours. Timesheet-only metrics (e.g., reported hours by pay code/month) can still use those records, while profile-dependent reports (e.g., by job title) must disclose the severely limited profile coverage. Do not fabricate names or roles, infer organization-wide completeness, or mislabel *reported payroll hours* as *physically worked hours* without pay-code definitions.

### Reproduce the reference-coverage check

```sql
SELECT CASE WHEN e.client_employee_id IS NULL
            THEN 'UNMATCHED' ELSE 'MATCHED' END AS match_status,
       COUNT(*) AS timesheet_records,
       COUNT(DISTINCT t.client_employee_id) AS unique_employee_ids,
       ROUND(SUM(t.hours_worked), 2) AS total_reported_hours
FROM silver.timesheets AS t
LEFT JOIN silver.employees AS e
  ON e.client_employee_id = t.client_employee_id
GROUP BY 1
ORDER BY 1;
```

## 12. Gold handoff — agreed direction, not yet implemented

Gold should preserve all **381,732** accepted timesheets and model the incomplete employee-reference population without fabricating descriptions. Recommended starting design, subject to checking employee-ID semantics and functional dependencies:

1. `gold.employee_identity`: one row for each of **10,794 observed employee IDs** from the two source populations, subject to verifying namespace compatibility.
2. `gold.employee_profiles`: **50** optionally linked descriptive employee profiles; no invented data for timesheet-only IDs.
3. `gold.timesheets`: all validated timesheets, joined by employee identity rather than by an incomplete employee profile foreign key.
4. Profile-coverage SQL view/audit and appropriate normalized department, pay-code and date structures based on actual source relationships.
5. Clearly distinguished whole-timesheet and profile-dependent analytics, with documented pay-code interpretation.

Additional Gold modeling may use relevant restored employee attributes (`city`, `country`, `years_of_experience`, etc.) when populated in future sources. This is the reason for preserving the complete approved employee schema in Silver v2.

## 13. Operation and reproducibility

The project runs from `D:\ASE-ETL` on Windows with Python virtual environment, Docker Compose, MinIO, PostgreSQL 16 and pandas/PyArrow. PostgreSQL is available through the Windows host's mapped port **5433** (internal 5432) using database `ase_etl` and user `ase_user`; credentials reside in `.env`, not in documentation or commits.

```powershell
# Services
 docker compose up -d

# Local Silver v2 generation and integrity
 python -m src.silver.pipeline
 python -m tests.test_silver_integrity

# Publication (writes MinIO and PostgreSQL)
 python -m src.silver.publish_idempotent

# Postpublication verification
 python -m tests.test_silver_cross_storage
 python -m tests.test_silver_publication

# Re-run publisher with unchanged data to check idempotency
 python -m src.silver.publish_idempotent
```

Do not use `docker compose down -v` unless deliberately deleting named-volume data. Do not commit `.env`, credentials, or generated data artifacts. Git branch used for the revision: `silver-v2-schema-evolution`.

## 14. Known boundaries and future improvements

| Area | Remaining qualification |
|---|---|
| General source evolution | Unexpected columns intentionally fail pending explicit approval; arbitrary new types/domains are not inferred automatically |
| Business meanings | `is_per_deim`, `clinical_level`, payroll rules and pay codes need authoritative definitions before interpretation |
| Masked PII | Current field-specific patterns are tested for known examples; future masking formats may require new rules and privacy review |
| Duplicate interpretation | Full-business-row equality is a pragmatic assumption in the absence of a source transaction ID |
| Employee-reference gaps | Most timesheets lack descriptive employee profiles; this must be visible in Gold analytics |
| Cross-system atomicity | MinIO preparation and PostgreSQL commit are not a distributed transaction |
| Incrementality | The implemented flow processes the provided snapshot; source-change scheduling and efficient incremental updates are not complete |
| Local publication | Local output folder replacement is staged per dataset, not a single cross-directory atomic swap |
| Publication fast path | Fingerprint, schema and counts are checked, but DB rows are not rehashed on every unchanged rerun |
| Testing extent | Verified supplied snapshot and synthetic failure cases; no exhaustive infrastructure fault-injection suite |

## 15. Final acceptance status

- [x] Preserve all approved employee business attributes (38 total including lineage).
- [x] Preserve all 18 timesheet columns and four lineage fields.
- [x] Normalize defined missing/masked values and apply defensible typed conversions.
- [x] Detect schema drift per Bronze Parquet part and persist successful alignment audits.
- [x] Persist JSON diagnostics for rejected schema changes outside published data.
- [x] Validate critical values, quarantine invalid rows and reconcile accepted/rejected counts.
- [x] Deduplicate overlapping exports with a traceable 30,839-occurrence audit.
- [x] Enforce employee and timesheet primary keys.
- [x] Produce seven Silver v2 Parquet datasets.
- [x] Validate transactional PostgreSQL migration rollback before applying the 12 columns.
- [x] Publish the seven datasets to MinIO and business tables to PostgreSQL.
- [x] Record schema version `2.0`, fingerprint and active snapshot in manifest/registry.
- [x] Verify all employee and timesheet column values across Parquet and PostgreSQL.
- [x] Pass publication consistency, rollback and unchanged-rerun idempotency checks.
- [x] Document missing employee-profile coverage and the no-data-loss decision.
- [ ] Implement Gold normalization, relationships and assignment SQL analysis (**next phase**).

**Conclusion:** Silver v2 is complete and verified for the supplied ASE-ETL snapshot. It is ready to provide the cleaned input for Gold design. Gold must preserve unmatched but valid timesheets, define analytical measures transparently, and confirm source ID and pay-code semantics before making broader business claims.
