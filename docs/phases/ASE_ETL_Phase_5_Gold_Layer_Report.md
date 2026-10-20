# ASE-ETL — Phase 5: Gold Layer Technical Report

**Implementation report · Gold model v1.0 · 16 October 2026**  
**Status:** Implemented, validated, and published for the verified Silver v2 snapshot  
**Scope:** PostgreSQL dimensional warehouse, controlled transformations, automated validation, Parquet export, MinIO publication, and rerun safety

---

## 1. Executive summary

Phase 5 transformed the cleaned and deduplicated Silver v2 data into an analytics-ready Gold dimensional warehouse. The result is a PostgreSQL star-schema-style model built around one timesheet fact table and two reusable dimensions. A Python-driven pipeline synchronizes Gold to the active Silver publication, validates data and database constraints in a transaction, and coordinates an immutable, checksummed Parquet publication in MinIO.

The **current Gold release is marked `PUBLISHED`**, and an independent verification script successfully checked the MinIO manifest and SHA-256 hashes of all **18 referenced Parquet objects**. The unchanged-source rerun test also passed: the pipeline skipped the redundant load and preserved the existing release fingerprint, prefix, and timestamp.

| Verified output | Current value |
|---|---:|
| `gold.dim_employee` rows | 10,794 |
| `gold.dim_date` rows | 183 |
| `gold.fact_timesheet` rows | 381,732 |
| Gold Parquet objects | 18 (1 employee, 1 date, 16 fact) |
| Active Silver schema version | `2.0` |
| Gold model version | `1.0` |
| Gold registry status | `PUBLISHED` |

**Completion boundary:** This phase is complete for the published source snapshot. Simulated future source changes, some stronger release-immutability tests, and downstream consumer cutover are documented as limitations and follow-up work, not described as tests already performed. Analytical aggregation and data marts will be implemented in Phase 6.

## 2. Purpose and architectural scope

The Silver layer is the conformed, cleaned, validated, deduplicated source. Gold reorganizes that data for business-facing analytics without repeating Bronze ingestion or Silver cleaning. It assigns stable warehouse keys, defines fact grain, relates facts to employee/date dimensions, and makes a verified release available to downstream SQL, APIs, and BI tools.

```text
Raw employee/timesheet files
            |
            v
BRONZE  — source preservation and ingestion lineage
            |
            v
SILVER v2 — typed, validated, deduplicated records
  PostgreSQL: silver.employees / silver.timesheets
  Registry:   silver._publication_registry
            |
            v
GOLD v1.0 — PostgreSQL dimensional warehouse
  gold.dim_employee   gold.dim_date   gold.fact_timesheet
  Registry: gold._publication_registry
            |
            +----> Consistent Parquet export ----> MinIO: ase-gold
            |                                    runs/<release-fingerprint>/
            v
NEXT PHASE — SQL analytics and aggregated data marts
            |
            v
REST API, Power BI, orchestration and end-to-end reporting
```

The data marts are planned as SQL-based reporting structures built *from* the Gold dimensional model. A data mart is a logical serving/design responsibility; it need not be a completely separate physical tier. Gold may legitimately contain aggregations in other architectures, but this project deliberately keeps event-level facts in the core Gold model and creates business-specific summaries in the next phase.

## 3. Source contract and verified baseline

### 3.1 Source tables and publication control

Gold reads the **published PostgreSQL Silver tables**, not the original Bronze files:

| Source object | Observed structure/use |
|---|---|
| `silver.employees` | 38 columns; one record per `client_employee_id` in the current snapshot; 50 profiles |
| `silver.timesheets` | 18 columns; 381,732 deduplicated event records |
| `silver._publication_registry` | Active source fingerprint, schema version, row counts, MinIO prefix, and publication timestamp |

The source registry reported publication ID `1`, schema version `2.0`, **50 employee rows**, **381,732 timesheet rows**, and a source fingerprint. Gold verifies that this publication exists, has the supported schema version, and agrees with current Silver table counts before loading. Count agreement does not by itself rehash PostgreSQL row content; source correctness also depends on the controlled Silver publication process.

### 3.2 Employee identity coverage analysis

The employee profile file is *not* a complete roster of every employee present in timesheets. The team established the following counts before designing `dim_employee`:

| Measurement | Result |
|---|---:|
| Silver employee profiles / unique profile IDs | 50 / 50 |
| Unique employee IDs in Silver timesheets | 10,773 |
| Profiled employees with timesheets | 29 |
| Profiled employees without timesheets | 21 |
| Timesheet employee IDs without a profile | 10,744 |
| **Union of distinct employee identities** | **10,794** |
| Timesheet rows without a matching profile | 381,223 |

All 10,794 original identifiers remained distinct after a diagnostic uppercase-and-trim normalization check; no surrounding-space or lowercase cases were found. Accordingly, Gold keeps Silver's existing `client_employee_id` values and does not invent a new business-identity normalization rule.

### 3.3 Fact grain and source measure checks

Silver's business-event primary key is **`(client_employee_id, punch_in_datetime, punch_out_datetime)`**. The Gold fact has the same unique business-event constraint. Employee ID plus calendar date was explicitly rejected as a timesheet key because multiple legitimate events can occur on one date.

The initial profiling checks found zero NULL, zero-hour, or negative `hours_worked` values; values ranged from **0.02 to 32.50 hours**, and there were zero NULL `punch_apply_date` values in the current Silver snapshot. The maximum value was retained pending business interpretation; elapsed punch duration was not substituted for payroll `hours_worked`.

## 4. Dimensional data model

### 4.1 Logical relationships

```text
 gold.dim_employee                     gold.dim_date
 PK employee_key                       PK date_key
 UK client_employee_id                 UK full_date
          |                                  |
          | 1-to-many                        | three roles
          v                                  v
                   gold.fact_timesheet
                   PK timesheet_key
                   FK employee_key
                   FK punch_apply_date_key
                   FK punch_in_date_key
                   FK punch_out_date_key
                   UK (client_employee_id,
                       punch_in_datetime,
                       punch_out_datetime)
```

The dimensions are modeled once and referenced from the fact. The three date foreign keys support distinct time-related analyses without conflating the source's punch-apply date with the actual punch timestamps.

### 4.2 `gold.dim_employee` — 24 columns

**Grain:** One row per distinct `client_employee_id` in the current Silver identity population. `employee_key` is an identity-generated `BIGINT` warehouse surrogate primary key; `client_employee_id` remains the non-null unique business identifier. `has_employee_profile` is non-null and indicates whether `silver.employees` supplied a profile.

The dimension includes selected names and profile fields, job code/title and start date, organization/department attributes, employment dates/status, manager ID, FTE/scheduled hours, experience, clinical level, and the source-named `is_per_deim` field. Source metadata and sensitive/unnecessary attributes such as DOB, personal addresses, and phone numbers were **not copied into this analytics-facing dimension**; the full records remain available in Silver under the agreed retention model.

**Identity design:** `UNION` distinct IDs from `silver.employees` and `silver.timesheets`, then `LEFT JOIN` employee profiles. This guarantees a dimension row for timesheet employees without profiles. Unavailable descriptive fields remain NULL instead of receiving invented data. The published result contains **50 rows with `has_employee_profile = TRUE`** and **10,744 with `FALSE`**.

**History strategy:** Initial **Slowly Changing Dimension Type 1 (SCD1)**. The employee UPSERT updates current descriptive values rather than storing historical employee-profile versions. Surrogate keys are preserved for retained business IDs. Historical profile reconstruction is not a guaranteed feature of this Gold model; upstream snapshot retention is distinct from a Type 2 historical dimension.

### 4.3 `gold.dim_date` — 12 columns

**Grain:** One row per calendar date, with integer `YYYYMMDD` primary key `date_key` and unique `full_date`.

Attributes: `date_key`, `full_date`, `day_of_month`, `day_of_week`, `day_name`, `week_of_year`, `month_number`, `month_name`, `quarter_number`, `year_number`, `iso_year`, `is_weekend`.

The date loader determines the minimum/maximum of three relevant Silver dates: `punch_apply_date`, `punch_in_datetime::date`, and `punch_out_datetime::date`; it generates each intervening day using PostgreSQL `generate_series`. It produced **183 consecutive calendar dates, 18 April 2025 through 17 October 2025**. The source referenced **169 distinct dates**, all of which matched a dimension row. The controlled `iso_year` migration ensures ISO week-number reporting can be grouped by the correct ISO week-year rather than calendar year near New Year.

### 4.4 `gold.fact_timesheet` — 22 columns

**Grain:** One row per unique, accepted Silver timesheet event (not per employee per day). The fact has an identity-generated `BIGINT` primary key `timesheet_key` and the three-column event uniqueness constraint.

| Column group | Included attributes |
|---|---|
| Relationships | `employee_key`, `punch_apply_date_key`, `punch_in_date_key`, `punch_out_date_key` |
| Source business identity | `client_employee_id`, `punch_in_datetime`, `punch_out_datetime` |
| Schedules and measure | `scheduled_start_datetime`, `scheduled_end_datetime`, `hours_worked` |
| Workplace/payroll context | `department_id`, `department_name`, `home_department_id`, `home_department_name`, `pay_code` |
| Event details | `punch_in_comment`, `punch_out_comment` |
| Traceability | `source_file`, `source_row_number`, `ingested_at_utc`, `ingestion_batch_id` |

Four foreign keys reference the employee and date dimensions. `hours_worked` is a source-provided numeric measure. The event's original business fields and ingestion lineage are preserved for traceability and future data marts.

## 5. Code structure and responsibilities

```text
src/gold/
  __init__.py
  config.py           Gold constants, bucket/dataset/model contracts
  migrations.py       Create/verify dimensions, fact, registry; ISO-year migration
  transformations.py  Dimension/fact upserts and stale-record synchronization
  validation.py       Constraints and source-to-target validation
  pipeline.py         Transactional PostgreSQL loading and skip-on-rerun logic
  publish.py          Arrow schemas, chunked exports, SHA-256, MinIO operations
  release.py          Consistent publication coordinator / registry finalization

tests/
  verify_gold_minio.py  Independent published-object integrity verification
```

Gold reuses the established SQLAlchemy `ENGINE` and MinIO client from `src.silver.publish`, which loads connection secrets from the project's `.env`. Gold-specific data is stored in a separate PostgreSQL schema and MinIO bucket. Secrets are not embedded in the Gold report or new Gold source files.

### 5.1 Controlled database migrations

`ensure_dim_employee`, `ensure_dim_date`, `ensure_fact_timesheet`, and `ensure_gold_registry` create missing objects without dropping existing data. Initial checks verified columns and primary keys, then `validate_gold_constraints()` independently checked primary, unique, and foreign-key relationships, including the referenced tables/columns. The `iso_year` extension used `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` followed by a date reload.

The existing migrations are not a complete generic migration framework: some column type/nullability checks (especially for date and fact) can be strengthened for future schema evolution. This is recorded as a technical limitation rather than a completed test.

### 5.2 Gold transformations

1. **Employees:** Union all distinct source identities; attach profile attributes; UPSERT on `client_employee_id` to retain `employee_key` for existing employees.
2. **Dates:** Generate a continuous calendar from Silver date boundaries; UPSERT on `date_key` and populate calendar/ISO attributes.
3. **Facts:** Join Silver timesheets to employee and date dimensions to resolve foreign keys; UPSERT on the composite Silver business-event key, retaining `timesheet_key` for matching events.
4. **Stale records:** Delete facts absent from the current Silver snapshot, then remove employee identities absent from *both* Silver tables. Date rows are retained as reusable reference dates.

The stale-record procedures are only invoked as part of the controlled transactional pipeline, not as standalone manual deletion steps. They removed **0 facts** and **0 employees** in the verified current-snapshot run. Retained event keys were unchanged in repeated-load fingerprint tests. For genuinely changed source snapshots, add/change/remove scenarios have not yet been separately fixture-tested.

## 6. Transactional loading and publication controls

### 6.1 PostgreSQL Gold pipeline

Command: `python -m src.gold.pipeline`

Inside `ENGINE.begin()`, the pipeline obtains the Silver publication advisory lock `7182401` and then Gold's lock `7182402`, verifies the active Silver publication, ensures Gold structures, and either skips an unchanged verified published release or performs dimensions → facts → stale-record reconciliation → validations → registry update to `DB_READY`. If work fails before commit, PostgreSQL rolls back the transaction. Matching counts, event keys, profiles, calendar attributes, and constraints are checked before commit.

The unchanged-snapshot skip is conditioned on the Silver source fingerprint, Gold model version, `PUBLISHED` status, required metadata, validation success, and registry/table count agreement. The observed rerun **skipped the load and preserved** the existing Gold fingerprint, MinIO prefix and publication timestamp. This tests unchanged-source behavior, not all conceivable failure/recovery cases.

### 6.2 Local Parquet export

Command used for the local export was a PowerShell Python here-string invoking `export_gold_parquet()` on a PostgreSQL **REPEATABLE READ** snapshot. The export orders rows by each table's stable surrogate/date primary key, reads chunks of **25,000**, and writes Snappy-compressed Parquet via PyArrow. PostgreSQL NUMERIC values are read with `coerce_float=False`, and explicit Arrow schemas are built from PostgreSQL metadata so nullable/all-NULL columns keep stable types.

The approved Parquet numeric columns use `decimal128(18, 4)`: employee scheduled hours, employee years of experience, and fact hours worked. This is a documented export precision/scale contract, not proof that arbitrary future higher-scale values will fit. The exported `years_of_experience` column currently had no non-NULL samples; an all-NULL decimal type test passed.

The local export succeeded for all datasets and was verified by `verify_gold_export()` for schema, readability, and row counts. A file-content SHA-256 fingerprint was calculated. Deterministic row ordering improves repeatability, but byte-identical Parquet across library/serialization changes is not guaranteed by ordering alone.

### 6.3 MinIO publication and registry states

MinIO target bucket: **`ase-gold`** (separate from Silver). The `release.py` coordinator acquires source and Gold advisory locks, validates the published Silver source and Gold, exports all tables using a consistent snapshot, checks local Parquet, generates a release fingerprint combining the Silver fingerprint, Gold model version, and Parquet export hash, uploads each object under `runs/<release-fingerprint>/`, and reads it back to verify its SHA-256 digest.

The manifest records model/source fingerprints, relative publication prefix, dataset counts, each referenced object's SHA-256, and recorded rows. An existing object is checked instead of silently overwritten. The final registry update to **`PUBLISHED`** is made only after all upload/verification operations return successfully.

| Registry state | Meaning |
|---|---|
| `DB_READY` | Gold PostgreSQL data committed/validated; no completed MinIO release yet |
| `PUBLISHED` | Manifest/object upload verified and publication registry finalized |

Because PostgreSQL and MinIO do not share a distributed transaction, uploads performed before a failed registry commit can leave verified but unreferenced MinIO objects. Retry and content-addressed verification handle this case without treating `DB_READY` as a published release. The final release coordinator holds a database transaction/locks while uploading; suitable for this project size, but a staging/promotion approach would better serve long-running production exports.

## 7. Verification evidence and results

All figures below come from actual SQL, Python, and publication outputs captured during Phase 5; they are not hypothetical expectations.

| Test / gate | Recorded evidence | Outcome |
|---|---|---|
| Employee identity normalization | Original=10,794; normalized=10,794; spaces=0; lowercase=0 | PASS |
| Employee union and flags | 10,794 total; 50 profiled; 10,744 unprofiled | PASS |
| Employee dimension timesheet coverage | 381,732 matched; 0 unmatched | PASS |
| Employee surrogate-key rerun stability | MD5 `630c8a12c553829afc87fc0c3c5b888c` unchanged | PASS |
| Date range and distinct keys | 183 days; 2025-04-18 to 2025-10-17; 183 keys | PASS |
| Date source coverage | 169 source dates matched; 0 missing | PASS |
| ISO-year attribute | 183 rows; 0 NULL; min/max=2025 | PASS |
| Fact business-event uniqueness | 381,732 rows; 381,732 unique event keys | PASS |
| Dimension key completeness in fact | 0 NULL employee keys; 0 NULL date keys | PASS |
| Fact surrogate-key rerun stability | MD5 `ac1404ee333c4229bd87ca281d6c9ea7` unchanged | PASS |
| Selected field reconciliation | 0 missing facts; 0 mismatched hours/pay-code/department/apply-date | PASS |
| Automated constraints | PK, UNIQUE and all four FK targets verified | PASS |
| Automated source-to-Gold checks | Fact population/attributes, employee profiles, calendar validated | PASS |
| Transactional Gold database run | Employee=10,794; date=183; fact=381,732; stale deletes=0 | PASS |
| Local Parquet metadata and readability | 18 parts; three tables' counts/schemas verified | PASS |
| First MinIO publication | Object checksums verified; registry advanced to `PUBLISHED` | PASS |
| Unchanged Silver rerun | Reload skipped; registry fields remained unchanged | PASS |
| Independent MinIO verification | 1+1+16 referenced objects with matching SHA-256; manifest checked | PASS |

**Important interpretation:** The independent MinIO script verifies manifest contents, the checksums of objects named in the manifest, and the sum of manifest-recorded row counts. It does not parse every remote Parquet row or search for unreferenced extra objects in the prefix. Likewise, a successful unchanged-publication shortcut does not itself rehash all remote objects; that is why the separate independent verification script was run.

### 7.1 Final publication identifiers

```text
Active Silver version: 2.0
Silver publication ID: 1
Silver snapshot fingerprint:
a016e5a75dfe0ad263a7ed5f229368624932da29be8c7278c8a5f7f8921675b7

Gold model version: 1.0
Gold publication ID: 1
Gold release fingerprint:
29f4572bd7b57d3d4a86ed885eee3d92c31ef0801361990bc52f69971aeea4d0

Gold MinIO bucket: ase-gold
Gold MinIO prefix:
runs/29f4572bd7b57d3d4a86ed885eee3d92c31ef0801361990bc52f69971aeea4d0
Gold published_at (UTC): 2026-10-15 17:14:12.944858+00
```

### 7.2 Published layout

```text
ase-gold/
└── runs/<gold-release-fingerprint>/
    ├── dim_employee/
    │   └── part-00000.parquet           10,794 rows
    ├── dim_date/
    │   └── part-00000.parquet              183 rows
    ├── fact_timesheet/
    │   ├── part-00000.parquet
    │   ├── ...
    │   └── part-00015.parquet          381,732 rows total
    └── manifest.json
```

## 8. Operational guide and reproducibility

**Prerequisites:** Docker services for PostgreSQL and MinIO running; Python virtual environment activated; installed dependencies (including SQLAlchemy, psycopg, pandas, PyArrow, python-dotenv, and MinIO client); project-root `.env` containing `DATABASE_URL`, `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, and `MINIO_ROOT_PASSWORD`. The optional `MINIO_SECURE` and `GOLD_BUCKET` settings are supported. Do not commit real credentials to Git.

Run commands from the repository root (`D:\ASE-ETL` in the development environment):

```powershell
# Verify existing PostgreSQL Gold against the current published Silver.
python -m src.gold.pipeline

# Publish DB_READY Gold to MinIO, or recognize an existing release.
python -m src.gold.release

# Independently verify current Gold publication manifest and object hashes.
python -m tests.verify_gold_minio
```

A successful unchanged-source `pipeline` rerun validates then skips reloading. For a genuinely new Silver publication, the pipeline is intended to synchronize the current Gold state and mark it `DB_READY`; the release coordinator then publishes the new snapshot. The project should not automatically expose a `DB_READY` state as a fully published release in future API/BI services.

**Read-only registry check:**

```sql
SELECT publication_id, silver_snapshot_fingerprint,
       gold_model_version, employee_count, date_count,
       timesheet_count, publication_status,
       gold_snapshot_fingerprint, minio_prefix, published_at
FROM gold._publication_registry
WHERE publication_id = 1;
```

**Read-only row-count check:**

```sql
SELECT 'dim_employee' AS dataset, COUNT(*) FROM gold.dim_employee
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'fact_timesheet', COUNT(*) FROM gold.fact_timesheet;
```

## 9. Design decisions and rationale

| Decision | Reason / consequence |
|---|---|
| Gold consumes published Silver PostgreSQL | Avoids recleaning raw files and tracks a registered source snapshot |
| Gold fact remains event-grained | Preserves legitimate multi-event days for flexible later aggregations |
| Employee identity union across both Silver datasets | Prevents loss of facts for employees lacking source profiles |
| Nullable unprofiled attributes plus a profile flag | Distinguishes missing profile from a known-but-incomplete profile |
| SCD Type 1 for employee descriptions | Keeps one current record per employee; does not promise Type 2 history |
| Surrogate keys plus unique business keys | Stable relationships for retained identities/events and controlled upserts |
| Shared role-playing date dimension | Uniform date slicing for apply, punch-in, and punch-out dates |
| Separate ISO year | Makes year-week grouping correct at year boundaries |
| Dynamic validations derived from Silver | Avoids treating snapshot-specific record counts as permanent invariants |
| Transactional pipeline and advisory locks | Controls consistency and concurrent publication activity |
| Stable Arrow schemas and Decimal export | Preserves typed NULLs and avoids silent numeric float coercion |
| Separate content-addressed Gold bucket and registry | Traceable Parquet release with explicit database-ready versus published states |

## 10. Known limitations, risks, and future test coverage

1. **Future Silver snapshots not fixture-tested end to end.** Current-snapshot loads and reruns passed. Controlled tests for new, removed, and changed employees/facts, including failure injection, remain to be performed; the synchronization functions are implemented but not proven against every future case.
2. **SCD1 does not keep profile history.** Gold stores current descriptive values, while history of source publications is an upstream concern. Any historical dimension requirement needs an explicit SCD2 design.
3. **Fixed Arrow decimal scale.** Parquet exports use `decimal128(18, 4)`; future data with unsupported precision/scale must be validated and handled deliberately rather than silently rounded.
4. **Schema migration coverage is not exhaustive.** The independent constraint checker verifies PK/unique/FK relationships, but migrations do not uniformly assert every expected column type and nullability for every table.
5. **Remote verifier scope.** It checks manifest-referenced object hashes and recorded counts; it does not independently scan all objects or re-compare remote business rows against PostgreSQL.
6. **Already-published shortcut.** `release.py` trusts an existing `PUBLISHED` registry record after validating current Gold; it does not recheck remote object hashes on that branch. The separate verification command supplies that assurance for the current release.
7. **Release coordination and consumers.** PostgreSQL and MinIO are distinct systems. The registry must govern when future API/Power BI clients treat a release as published. Holding locks during upload trades simplicity for longer transactions.
8. **Gold is not an exhaustive employee-profile archive.** Personal/sensitive and currently unnecessary fields remain in Silver rather than in `dim_employee`. Existing Gold profile-column selection should be revisited if a confirmed business requirement needs additional fields.
9. **Snapshot byte determinism is conditional.** Fixed row ordering and schemas make repeat exports more consistent, but library-version/Parquet writer changes may alter file bytes and thus the release fingerprint without changing logical values.
10. **Assignment traceability requires the actual rubric.** This report describes implemented and observed work. It does not claim an independent line-by-line audit of the original assignment PDF, which was not re-examined while producing this document.

## 11. Phase handoff and conclusion

The **Phase 5 implementation and publication are complete for the verified Silver v2 snapshot**. The dimensional warehouse supports analytics at timesheet-event grain and preserves employee identity coverage despite incomplete profiles. Structural constraints, record matching, selected full-field reconciliation, repeatability, Parquet readability, manifest consistency, and all referenced MinIO checksums were verified.

**Next: Phase 6 — SQL Analytics and Data Marts.** Use `gold.fact_timesheet`, `gold.dim_employee`, and `gold.dim_date` to define reusable SQL measures and reporting datasets, for example employee daily worked hours, weekly/monthly trends, and department/pay-code breakdowns. Confirm business definitions for hours and departmental allocation before publishing KPI aggregates. Subsequent project phases can expose these marts through the REST API and Power BI and add scheduler-driven end-to-end orchestration.

---

**Evidence basis:** Recorded SQL results, PowerShell/Python execution logs, source snippets, and the successful publication/verifier outputs from the project's Phase 5 implementation conversation. No unpublished SQL results or unexecuted future-snapshot tests have been presented as completed.
