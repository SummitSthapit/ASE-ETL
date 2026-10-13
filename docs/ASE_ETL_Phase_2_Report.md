# Phase 2 — Database and Medallion Architecture Design

**Project:** ASE ETL — ETL to Insights  
**Phase:** Phase 2 — Database and Medallion Design  
**Status:** Architecture and technology choices finalized; physical schema and key definitions pending source verification  
**Architecture:** Medallion Architecture (Bronze, Silver, Gold)

---

## 1. Introduction

The objective of Phase 2 is to establish the database architecture, data storage strategy, transformation layers, data quality policies, and orchestration design for the ASE ETL project.

The project processes employee and timesheet data from five source CSV files. The resulting pipeline must support reliable ingestion, data cleaning, validation, business metric calculation, analytical queries, reporting, and API access.

The system is designed to preserve the original source data while progressively improving data quality and preparing the data for analytics.

The project will use the **Medallion Architecture**, consisting of three layers:

- **Bronze:** Preserves source data in its ingested form with lineage metadata.
- **Silver:** Standardizes, cleans, validates, and classifies records.
- **Gold:** Produces business-ready datasets and analytical metrics.

The pipeline will be implemented incrementally in Python, with each layer validated before proceeding to the next.

## 2. Project Objectives

The database and architecture design must support the following objectives:

1. Ingest data from local files and MinIO object storage.
2. Preserve original source files without modifying them.
3. Store Bronze, Silver, and Gold datasets in Parquet format.
4. Use PostgreSQL for relational data management, metadata, constraints, and analytical queries.
5. Clean and standardize data while retaining information needed for auditing.
6. Preserve unresolved records instead of silently discarding or arbitrarily correcting them.
7. Detect and classify duplicate records.
8. Calculate workforce and attendance metrics.
9. Orchestrate dependent pipeline tasks using Luigi.
10. Expose employee and timesheet data through a FastAPI service.
11. Support validation, repeatable execution, failure recovery, and automated reporting.

## 3. Technology Stack

The following technologies have been selected for this implementation.

| Technology | Responsibility |
|---|---|
| Python | Main implementation language |
| pandas | Data ingestion, profiling, transformation, and validation |
| PostgreSQL | Relational storage, pipeline metadata, constraints, and SQL analytics |
| MinIO | S3-compatible object storage for Parquet datasets |
| Apache Parquet | Columnar storage format for processed datasets |
| Luigi | Workflow orchestration and task dependency management |
| FastAPI | HTTP API for employee and timesheet access |
| Jupyter Notebook | Incremental development, investigation, and testing |
| Git and GitHub | Version control and project collaboration |

These technologies constitute the selected project implementation. They are not all mandatory technology choices specified by the assignment itself.

### 3.1 Why PostgreSQL?

PostgreSQL provides relational tables, constraints, indexes, transactions, and SQL capabilities needed for data validation and workforce analytics.

It will be used for:

- Employee and timesheet serving tables, where appropriate.
- Ingestion batch and pipeline execution metadata.
- Data integrity constraints.
- Analytical queries and reporting tables.
- Data access for the FastAPI service.

PostgreSQL will not replace the Parquet-based Medallion storage. Each serves a different purpose.

### 3.2 Why MinIO and Parquet?

MinIO provides S3-compatible object storage for the data lake layers.

Parquet is suitable for analytical workloads because it stores data in a columnar format and supports efficient reading of selected columns.

Together, they provide persistent storage for Bronze, Silver, and Gold datasets without requiring every intermediate dataset to be stored exclusively as a PostgreSQL table.

### 3.3 Why Luigi?

Luigi will manage the pipeline's task dependencies and execution order. It will help ensure that downstream tasks run only after their prerequisites have completed successfully.

### 3.4 Why FastAPI?

FastAPI will provide the application interface for employee CRUD operations and read-only timesheet access, including filtering and authentication/authorization.

The API will be developed after the underlying data pipeline and serving schema are sufficiently stable.

## 4. High-Level Architecture

The planned data flow is:

```text
                    SOURCE DATA
                         |
             +-----------+-----------+
             |                       |
        Local CSV Files          MinIO Input
             |                       |
             +-----------+-----------+
                         |
                         v
              INGESTION AND VALIDATION
                         |
                         v
                BRONZE DATA LAYER
             Source-faithful Parquet
             Lineage and batch metadata
                         |
                         v
                SILVER DATA LAYER
             Standardized and cleaned data
             Duplicate classification
             Relationship validation
             Unresolved-record retention
                         |
                         v
                  GOLD DATA LAYER
             Business-ready datasets
             Workforce and attendance KPIs
                         |
                +--------+--------+
                |                 |
                v                 v
         PostgreSQL/SQL      Reporting and
          Analytics          Visualizations
                |
                v
             FastAPI
       Employee and Timesheet API

       Luigi orchestrates the workflow
       and its dependencies throughout.
```

### 4.1 Separation of responsibilities

- **MinIO and Parquet:** Persistent storage for the Medallion datasets.
- **PostgreSQL:** Relational data management, pipeline metadata, serving tables, and SQL analytics.
- **pandas:** Data processing and validation.
- **Luigi:** Workflow dependencies and execution.
- **FastAPI:** Controlled access to application data.
- **Reporting tools:** Visualization and presentation of business metrics.

This separation avoids treating object storage, a relational database, and an orchestrator as interchangeable components.

## 5. Source Data Management

The project contains five source CSV files identified during Phase 1:

1. `employee_202510161125.csv`
2. `timesheet_202509151540.csv`
3. `timesheet_202510161121.csv`
4. `timesheet_202510161122.csv`
5. `timesheet_202510161124.csv`

These filenames represent the source inventory identified during project investigation. The actual column names, delimiter configuration, encodings, and parsing details must be confirmed against the source files before the ingestion implementation is finalized.

### 5.1 Immutable raw data

The `data/raw/` directory will contain the original source files.

The pipeline must not overwrite, rename, edit, or clean these files as part of normal processing.

All transformations must operate on data loaded into memory or on separate derived datasets.

### 5.2 Input support

The ingestion design must support:

- Reading source files from the local filesystem.
- Reading supported source objects from MinIO.

Both input methods should produce consistent ingestion results for equivalent source data.

### 5.3 Source parsing

Before ingestion, the pipeline must use verified source-specific parsing settings, including:

- Delimiter.
- Encoding.
- Header configuration.
- Quoting and escaping rules.
- Expected column names.
- Expected source schema.

The ingestion implementation must not assume that every CSV uses the same parsing configuration without verification.

## 6. Medallion Architecture

### 6.1 Bronze Layer — Raw Ingestion

**Purpose:** Preserve the ingested source data and its provenance.

Bronze is the first persistent layer of the pipeline. It is designed to preserve the original field values rather than immediately applying business transformations.

#### Responsibilities

1. Read the source file using its verified parsing configuration.
2. Preserve source column names and source values.
3. Keep identifier columns as strings.
4. Attach ingestion and lineage metadata.
5. Store the resulting dataset as Parquet in MinIO.
6. Record ingestion status and relevant metadata in PostgreSQL.
7. Validate the output before declaring ingestion successful.

#### Initial data-type policy

Source values should initially be ingested as strings wherever practical. This prevents identifiers, leading zeros, and source representations from being unintentionally changed during initial ingestion.

Date, timestamp, numeric, and other business types will be interpreted and validated in Silver.

Null handling must be explicit. Empty fields and source-specific missing-value markers must not be converted in a way that loses the distinction between original source representations and normalized values.

#### Lineage metadata

Each ingested record should carry the following metadata fields:

| Field | Purpose |
|---|---|
| `source_file` | Original source filename |
| `source_row_number` | Logical record position within the parsed source file |
| `source_file_hash` | SHA-256 hash of the source file |
| `ingestion_batch_id` | Identifier for the ingestion batch |
| `ingested_at` | Timestamp at which the record was ingested |

The exact data types and implementation details will be confirmed during Bronze development.

`source_row_number` represents a parsed record position, not necessarily a physical text-file line number. This distinction matters when CSV records contain quoted multiline fields.

#### Bronze storage layout

The following is the planned object-key layout:

```text
ase-etl/
└── bronze/
    ├── employee_source/
    │   └── file_hash=<sha256>/
    │       └── part-000.parquet
    └── timesheet_source/
        └── file_hash=<sha256>/
            └── part-000.parquet
```

The layout is illustrative. Exact bucket names, object naming rules, partitioning, and file sizes will be finalized during implementation.

The source file hash helps distinguish different source versions and supports reproducibility.

### 6.2 Silver Layer — Standardized and Validated Data

**Purpose:** Convert source-faithful Bronze records into consistent, validated datasets while preserving auditability.

Silver transformations will be implemented incrementally after Bronze ingestion has passed its acceptance checks.

#### Responsibilities

1. Normalize whitespace where appropriate.
2. Identify and normalize missing-value markers.
3. Convert fields to appropriate data types.
4. Validate dates, timestamps, and numeric values.
5. Investigate and classify duplicates.
6. Validate employee and timesheet relationships.
7. Apply documented business rules.
8. Preserve unresolved or invalid records for investigation.
9. Maintain lineage back to Bronze records.
10. Produce quality metrics and validation results.

#### Data cleaning policy

Cleaning must be controlled and documented rather than applied indiscriminately.

For example:

- Whitespace can be trimmed from text fields where appropriate.
- Missing-value markers can be normalized according to an explicit policy.
- Invalid dates should be classified rather than silently replaced.
- Numeric conversion failures should be recorded.
- Identifier values must remain strings.
- Business rules must be supported by evidence or assignment requirements.

Bronze must remain available as the reference for the original ingested values.

#### Duplicate handling

Duplicate classification must distinguish between:

- Exact duplicate source rows.
- Records that share an identifier but differ in other fields.
- Multiple legitimate timesheet records belonging to the same employee or date.
- Potential business duplicates that require further investigation.

A repeated employee identifier or date does not automatically establish that a timesheet record is a duplicate.

Where a deduplication rule is applied, the rule and resulting disposition must be documented. Removed-from-current-output records must remain traceable through Bronze data and appropriate quality or audit outputs.

#### Relationship validation

Timesheet employee identifiers must be checked against the employee roster using the verified source keys.

Records with unmatched employee identifiers must be retained in a suitable unresolved-record dataset. They must not be discarded or mapped to another employee without evidence.

#### Silver storage layout

```text
ase-etl/
└── silver/
    ├── employees/
    ├── timesheets/
    └── unresolved_records/
```

This layout is a logical starting point. Partitioning and versioning will be determined based on the source schema and observed query patterns.

### 6.3 Gold Layer — Business-Ready Analytics

**Purpose:** Produce consistent datasets and metrics for reporting, visualization, and API consumption.

Gold transformations will be developed after Silver datasets and their validation rules are established.

#### Responsibilities

1. Calculate assignment-defined workforce metrics.
2. Aggregate attendance and working-hours data.
3. Produce datasets suitable for visualization.
4. Support analytical SQL queries.
5. Validate metric outputs against documented definitions.
6. Make approved datasets available to downstream consumers.

#### Planned analytics

The project intends to support the following metrics specified by the assignment:

- Active headcount over time.
- Employee turnover trends.
- Average tenure by department.
- Average working hours per employee.
- Late-arrival frequency using the specified five-minute grace period.
- Early-departure counts.
- Overtime counts.
- Rolling average working hours.
- Early attrition rate.

The exact formulas, required fields, treatment of missing data, date boundaries, and aggregation levels must be documented before each metric is implemented.

Metrics must not be calculated using unsupported assumptions about the meaning of source fields.

#### Gold storage layout

```text
ase-etl/
└── gold/
    ├── workforce_metrics/
    ├── attendance_metrics/
    └── working_hours_metrics/
```

Gold datasets may be persisted in MinIO as Parquet and exposed through PostgreSQL analytical or serving tables where appropriate.

## 7. PostgreSQL Database Design

PostgreSQL will support pipeline control, relational data access, and analytical serving.

The logical database organization is divided into three areas.

### 7.1 Pipeline control and metadata

This area will contain tables such as:

**`ingestion_batches`**

Tracks source ingestion batches.

Potential fields include:

- Batch identifier.
- Source filename.
- Source file hash.
- Source location or storage reference.
- Start and completion timestamps.
- Ingestion status.
- Input and output record counts.
- Error details, when applicable.

**`pipeline_runs`**

Tracks orchestration executions.

Potential fields include:

- Pipeline run identifier.
- Task or pipeline name.
- Associated batch identifier, where applicable.
- Start and completion timestamps.
- Execution status.
- Retry count.
- Error details, when applicable.

These are logical designs. The final columns, types, constraints, and relationships will be defined during implementation.

### 7.2 Relational data tables

The relational data area will support employee and timesheet access.

Potential entities include:

- Employees.
- Timesheet records.
- Supporting reference tables, if justified by the source data and business rules.

Physical table definitions will be based on the verified source schemas and the final Silver transformation rules.

### 7.3 Analytics and serving tables

This area will contain selected Gold outputs needed for SQL analytics, reporting, and API access.

Possible outputs include workforce metrics, attendance metrics, working-hours aggregates, and other assignment-defined KPIs.

The design will avoid unnecessarily duplicating all Parquet datasets in PostgreSQL. Only the relational and analytical structures required for querying and serving will be maintained there.

## 8. Identifier and Key Design

Primary keys and business keys must be selected based on evidence from source profiling and validation.

### 8.1 Employee identifiers

The source employee identifier will be retained as a string.

Before it is designated as a natural key, we must verify:

- Whether it is populated for all relevant records.
- Whether it is unique within the employee roster.
- Whether the identifier is stable and meaningful across the source files.
- Whether duplicate or conflicting employee records exist.

### 8.2 Timesheet identifiers

A timesheet primary key has not yet been finalized.

In particular, neither `client_employee_id` alone nor the combination of `client_employee_id` and `punch_apply_date` will be assumed to uniquely identify a timesheet record without verification.

Before selecting a business or composite key, investigate:

- Uniqueness.
- Nullability.
- Duplicate patterns.
- Multiple records per employee and date.
- The meaning and granularity of each candidate field.
- Whether the key remains stable across source files and source versions.

If the source does not provide a reliable business key, a generated technical identifier may be used to identify individual ingested records. This technical identifier must not be presented as proof of business-level uniqueness.

### 8.3 Foreign keys and unresolved records

Relational constraints must not cause unmatched source records to disappear.

Foreign keys may be applied to curated relational tables only when their implications are understood and unresolved records are handled separately.

The source identifiers and lineage information must remain available even when a record cannot be linked to a valid employee entity.

## 9. Data Quality and Validation Policy

Validation will be applied at multiple stages.

### 9.1 Bronze validation

Bronze acceptance checks include:

- The expected source file is present and readable.
- Parsing succeeds using the verified configuration.
- Expected columns are present.
- Record counts are recorded.
- Source values are preserved according to the ingestion contract.
- Identifier values remain strings.
- Lineage metadata is present.
- The Parquet output can be read back successfully.
- The output is registered against the correct ingestion batch.

### 9.2 Silver validation

Silver validation will include:

- Data-type conversion checks.
- Missing-value profiling.
- Date and numeric validity checks.
- Duplicate classification.
- Key and relationship checks.
- Unresolved-record accounting.
- Row-count reconciliation between input and output categories.
- Lineage retention.

The accounting must explain why records were retained, classified, rejected from a curated output, or placed in an unresolved dataset.

### 9.3 Gold validation

Gold validation will include:

- Required columns and data types.
- Aggregation consistency.
- Metric-definition checks.
- Appropriate null handling.
- Reconciliation with validated Silver data.
- Checks for duplicate aggregate rows at the intended grain.
- Validation of outputs against independently calculated examples where practical.

Validation results should be recorded so failures can be investigated and reproduced.

## 10. Idempotency, Versioning, and Recovery

The pipeline must support repeatable execution.

### 10.1 Source hashing

A SHA-256 hash will be calculated for each source file.

The hash identifies the exact file contents and helps distinguish unchanged files from changed source versions.

### 10.2 Idempotent ingestion

When an identical source file has already been ingested successfully, a normal rerun should not create unintended duplicate Bronze data.

The ingestion process should check the file hash and batch status before deciding whether to skip, reuse, or intentionally reprocess a source file.

Intentional reprocessing must be distinguishable from an accidental duplicate run.

### 10.3 Source version changes

When the file contents change, its hash changes. The new version should be traceable as a distinct ingestion event.

Previous Bronze data must not be silently overwritten by the new version.

### 10.4 Failure recovery

A failed task must be recorded with sufficient information for investigation and retry.

A pipeline must not mark a batch as successful until its required outputs have been written and validated.

Repeated execution must not leave inconsistent metadata or produce unintended duplicate downstream outputs.

## 11. Luigi Orchestration Design

Luigi will coordinate the dependencies between pipeline tasks.

The planned workflow is:

```text
Validate Configuration and Inputs
                |
                v
       Calculate File Hashes
                |
                v
     Register Ingestion Batches
                |
                v
        Bronze Ingestion
                |
                v
       Validate Bronze Data
                |
                v
       Silver Transformations
                |
                v
       Validate Silver Data
                |
                v
        Gold Transformations
                |
                v
        Validate Gold Data
                |
                v
     Refresh/Validate SQL Outputs
                |
                v
      Generate Quality Report
```

The final task structure may be split into smaller tasks based on the verified source inventory and implementation requirements.

### 11.1 Dependency rules

- Silver tasks must depend on successful Bronze outputs and validations.
- Gold tasks must depend on successful Silver outputs and validations.
- Analytical serving tasks must depend on the Gold datasets they consume.
- Reporting tasks must depend on the relevant completed validation and metric tasks.

### 11.2 Orchestration requirements

The workflow should provide:

- Explicit task dependencies.
- Repeatable task execution.
- Failure visibility.
- Retry behavior where appropriate.
- Clear logging.
- Batch and run tracking.
- Validation before downstream processing.
- Protection against unintended duplicate output generation.

## 12. FastAPI Integration Design

FastAPI will expose controlled access to the curated relational data.

### 12.1 Employee API

The API must support the assignment's employee CRUD requirement:

- Create an employee.
- Retrieve employee records.
- Update an employee.
- Delete an employee.

The final endpoint paths and payload schemas will be designed after the employee serving schema is established.

### 12.2 Timesheet API

Timesheet access will be read-only.

It should support retrieving timesheet records and applying appropriate filters based on verified fields.

### 12.3 Security

The API must include authentication and authorization.

Access rules must distinguish permitted employee modifications from read-only timesheet access. Credentials and secrets must not be hard-coded in source files.

API implementation and endpoint testing will be completed after the underlying database structures are ready.

## 13. Security and Configuration

Environment-specific configuration will be kept separate from source code.

A `.env.example` file should document required configuration variables without containing real credentials.

The project must avoid committing actual passwords, access keys, or other secrets to Git.

Database and object-storage access should use the minimum permissions needed for each component.

## 14. Planned Repository Organization

The following is the intended logical organization. It should be reconciled with the existing repository before creating or moving files.

```text
ASE-ETL/
├── data/
│   ├── raw/
│   ├── profiling/
│   └── processed/
├── docs/
│   ├── phase1/
│   └── phase2/
├── notebooks/
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── database/
│   ├── orchestration/
│   └── api/
├── tests/
├── configs/
├── requirements.txt
├── .env.example
└── README.md
```

Responsibilities:

- `data/raw/`: Immutable original source files.
- `data/profiling/`: Profiling outputs and source investigation results.
- `data/processed/`: Optional local development outputs, not a replacement for persistent MinIO datasets.
- `docs/phase1/`: Phase 1 profiling and planning documentation.
- `docs/phase2/`: Database and architecture design documentation.
- `notebooks/`: Incremental investigation, development, and verification.
- `src/`: Reusable implementation modules.
- `tests/`: Automated validation and regression tests.
- `configs/`: Non-secret configuration templates and source-specific settings.

The directory layout is a plan, not a claim that all directories or modules already exist.

## 15. Design Decisions and Open Items

The following decisions are established:

| Decision | Status |
|---|---|
| Use Medallion Architecture | Finalized |
| Preserve original raw files | Finalized |
| Use PostgreSQL for relational and analytical serving | Finalized |
| Use MinIO for object storage | Finalized |
| Use Parquet for Medallion datasets | Finalized |
| Use pandas for transformations | Finalized |
| Use Luigi for orchestration | Finalized |
| Use FastAPI for application access | Finalized |
| Preserve identifiers as strings during initial ingestion | Finalized |
| Retain lineage and unresolved records | Finalized |
| Use source hashing and repeatable ingestion controls | Design finalized; implementation pending |
| Exact source delimiters and parsing configuration | Must be verified |
| Exact source column names and data types | Must be verified |
| Employee natural key | Pending source verification |
| Timesheet business or composite key | Pending source verification |
| Exact PostgreSQL table definitions | Pending source and query requirements |
| Final constraints and indexes | Pending key and workload verification |
| Exact Silver cleaning and deduplication rules | Pending implementation and validation |
| Exact KPI formulas and edge-case handling | Must be documented before metric implementation |
| MinIO bucket, partitioning, and file-size strategy | To be finalized during implementation |

These open items are deliberate. Committing to unverified schemas, primary keys, or business rules would risk corrupting data or producing misleading results.

## 16. Phase 2 Acceptance Criteria

Phase 2 design will be considered complete when the following items are documented and verified:

- [x] Medallion architecture selected.
- [x] Responsibilities of Bronze, Silver, and Gold defined.
- [x] Core technology stack selected.
- [x] PostgreSQL and MinIO responsibilities distinguished.
- [x] Raw-data immutability policy established.
- [x] Initial identifier and lineage policies established.
- [x] Unresolved-record preservation policy established.
- [x] High-level Luigi workflow defined.
- [x] API responsibilities and security requirements defined.
- [ ] Exact source parsing configuration verified against the files.
- [ ] Source schemas and key candidates verified against profiling evidence.
- [ ] Physical PostgreSQL schemas and constraints finalized.
- [ ] Final storage naming, versioning, and partitioning details recorded.

The unchecked items require source evidence or implementation-level verification. The design document does not claim that those checks have already passed.

## 17. Next Phase — Bronze Ingestion

After this document is saved and the existing project structure is checked, implementation will proceed to Bronze ingestion.

The Bronze phase will be performed incrementally:

1. Verify the current repository and source file paths.
2. Confirm the five files' delimiters, headers, encodings, and source schemas.
3. Review the Phase 1 profiling evidence relevant to source parsing and record counts.
4. Define the Bronze ingestion configuration.
5. Implement file hashing and ingestion batch tracking.
6. Read source values without unintended type inference or identifier conversion.
7. Add the required lineage metadata.
8. Write the Bronze Parquet outputs to MinIO.
9. Read the outputs back and verify schemas, counts, and lineage.
10. Verify idempotent rerun behavior.
11. Record and resolve any discrepancies before starting Silver transformations.

**Silver transformations will not begin until Bronze ingestion has passed its validation checks.**

## 18. Conclusion

The selected design separates immutable source data, standardized datasets, and business-ready analytics into clear layers. MinIO and Parquet provide persistent storage for the Medallion datasets, while PostgreSQL provides relational and analytical access. pandas performs the transformations, Luigi coordinates task execution, and FastAPI exposes controlled access to curated data.

The architecture and major technology choices are established. Source-specific parsing, key selection, and physical schema details remain subject to verification against the actual files and profiling evidence.

The next implementation milestone is to verify the source configuration and build a reproducible, validated Bronze ingestion process.
