# ASE-ETL — Bronze Layer Implementation Documentation

**Architecture:** Medallion (Bronze → Silver → Gold)  
**Project root:** `D:\ASE-ETL`  
**Status:** Bronze implementation and listed consistency checks completed; recovery hardening remains a follow-up.

## 1. Purpose and scope

This document records the Bronze layer implementation for the ASE-ETL assignment: what was built, how it works, why the design choices were made, what has been verified, and what remains a limitation. It describes the current implementation and observed verification results rather than claiming unfinished work is complete.

Project root: D:\ASE-ETL. Repository: https://github.com/SummitSthapit/ASE-ETL.git.


## 2. Why Medallion Architecture

We selected Medallion Architecture instead of a single undifferentiated ETL flow because it separates raw ingestion, cleaned/validated data, and business-ready analytics.

- Raw: original supplied files, preserved unchanged.
- Bronze: source-faithful ingestion with lineage, preserving duplicates and unresolved values for later analysis.
- Silver: planned standardization, validation, and controlled exception handling.
- Gold: planned business metrics and analytics outputs.
This separation makes transformations auditable: Silver can apply cleaning rules without destroying the ingested evidence held in Bronze, and Gold can focus on business questions rather than raw-data repair.


## 3. Technology and infrastructure

| Component | Selected technology / configuration | Reason |
|---|---|---|
| Language and processing | Python, pandas | Readable, inspectable ETL logic and chunked CSV processing. |
| Relational storage | PostgreSQL | Structured querying, SQL validation, and downstream API/analytics support. |
| Object storage | MinIO | Local S3-compatible object storage for Parquet datasets. |
| Columnar format | Parquet | Compact columnar storage suited to analytical reads and partitioned/chunk outputs. |
| Orchestration | Luigi (planned for pipeline orchestration) | Explicit task dependencies and reruns; not claimed as completed in Bronze ingestion. |
| API | FastAPI (planned) | Assignment requirement for employee CRUD and read-only timesheet access. |

Docker runs the local PostgreSQL and MinIO services. PostgreSQL container name: ase_postgres; database: ase_etl; user: ase_user; host port: 5433 mapped to container port 5432. MinIO container name: ase_minio; endpoint: localhost:9000; console: http://localhost:9001.

The host port 5433 is intentional so the Docker PostgreSQL service does not conflict with a native PostgreSQL service that may use port 5432.


## 4. Source data and baseline

The supplied CSV files are pipe-delimited. The original source files are kept under data/raw and are not modified by the Bronze ingestion process.

| Source file | Verified initial row count |
|---|---|
| employee_202510161125.csv | 50 |
| timesheet_202509151540.csv | 269,921 |
| timesheet_202510161121.csv | 48,258 |
| timesheet_202510161122.csv | 55,435 |
| timesheet_202510161124.csv | 38,957 |
| Total | 412,621 |

These are the verified baseline counts for the initial set of files. If a source file is intentionally changed and re-ingested, that file's row count may change.


## 5. Bronze outputs and locations

- PostgreSQL schema: bronze.
- Raw-ingestion tables: bronze.employee_202510161125_raw, bronze.timesheet_202509151540_raw, bronze.timesheet_202510161121_raw, bronze.timesheet_202510161122_raw, and bronze.timesheet_202510161124_raw.
- Local Parquet: data/bronze/parquet/<source-stem>/part-....parquet.
- MinIO output bucket: ase-bronze; objects are organized beneath raw/<source-stem>/ (for example, raw/timesheet_202509151540/part-....parquet).
- MinIO source bucket: ase-source, with source objects under raw/. The source and output buckets are separate to prevent cleanup of Bronze outputs from deleting source objects.
A PostgreSQL registry table, bronze._ingestion_registry, records per-file ingestion state. It contains source_file, content_sha256, table_name, rows_loaded, parquet_parts, ingestion_batch_id, and ingested_at_utc.


## 6. How ingestion works

1. Load configuration from the project .env file and establish PostgreSQL and MinIO clients.
1. Choose local-file or MinIO input mode using --source local / --source minio, or the INGEST_SOURCE environment setting.
1. Discover source CSVs from data/raw or from SOURCE_BUCKET and SOURCE_PREFIX in MinIO.
1. Validate candidate filenames and input availability before making changes.
1. Calculate a SHA-256 hash for each source file and compare it with the ingestion registry.
1. Skip a file when its content hash is unchanged; ingest a new file or reprocess a changed file.
1. Read CSV data in pandas chunks of 50,000 rows, using pipe delimiter and string-preserving input settings.
1. Add lineage columns, write each chunk to local Parquet, upload the same Parquet part to MinIO, and append rows to the appropriate PostgreSQL table.
1. Record a successful file-level result in the registry after that file finishes successfully.
1. Print a summary including loaded rows and Parquet part counts.
The current implementation uses pandas settings equivalent to sep='|', dtype=str, keep_default_na=False, na_filter=False, and encoding='utf-8-sig'. This preserves identifiers as text and avoids automatically converting source tokens into nulls without an explicit rule.


## 7. Why these implementation choices were made

### Preserve identifiers as strings

Employee and timesheet identifiers are identifiers, not quantities. Reading them as strings avoids accidental numeric conversion, loss of leading zeros, and inconsistent joins.

### Do not rewrite raw source files

Keeping data/raw immutable gives the project a reproducible source of truth. Corrections and standardization belong in later layers, not in the supplied inputs.

### Retain duplicates in Bronze

Phase 1 profiling identified 30,839 exact duplicate timesheet rows in the original data. Bronze retains these rows intentionally so later rules can distinguish true duplicates from repeated legitimate entries or revised snapshots.

### Retain unresolved records

Unmatched references and other questionable values are not silently discarded during ingestion. Silver can validate and route them to exception outputs with lineage.

### Use chunked processing

The largest timesheet file is approximately 50.9 MB and contains 269,921 rows. Processing in 50,000-row chunks limits peak memory use and produces multiple manageable Parquet parts.

### Write both relational and object/columnar outputs

PostgreSQL supports SQL queries and relational validation; Parquet in MinIO provides object storage and an analytical file format. Local Parquet also supports straightforward inspection and verification during development.

### Use SHA-256 content hashes

The registry hash allows the job to distinguish unchanged files from changed files based on content rather than filename alone. It supports idempotency and incremental reprocessing.

### Keep per-file registry state

A registry record makes ingestion outcomes inspectable: which file was processed, which table received it, the row and part counts, the batch ID, timestamp, and content hash.


## 8. Lineage metadata

| Column | Meaning |
|---|---|
| source_file | Original source filename for the row. |
| source_row_number | Row position in the source file, used to trace a record back to its source. |
| ingested_at_utc | UTC timestamp associated with ingestion. |
| ingestion_batch_id | Identifier tying the row to an ingestion run/batch. |

These columns are added to Bronze records to make troubleshooting and auditing possible. They should be preserved or mapped to an equivalent lineage representation in Silver.


## 9. Idempotency and incremental behaviour

The implemented file-level behaviour is content-hash driven.

- Unchanged file: skipped; existing output is not needlessly reloaded.
- New file: its table, Parquet parts, MinIO objects, and registry state are created.
- Changed file: only that file's prior outputs are targeted for replacement and the changed content is reprocessed.
- Unchanged sibling files: their outputs are intended to remain untouched when another file changes.
The user has tested and confirmed idempotency and incremental behaviour. The registry is written only after a file finishes successfully, which avoids recording a successful status for a file that failed during processing.


## 10. Configuration and secrets

The project uses a root .env file, which must remain local and must not be committed. Configuration includes PostgreSQL connection details, MinIO endpoint and credentials, source/output bucket names, input prefix, and input mode.

```text
POSTGRES_PASSWORD=... 
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...
DATABASE_URL=postgresql+psycopg://ase_user:<password>@localhost:5433/ase_etl
MINIO_ENDPOINT=localhost:9000
BRONZE_BUCKET=ase-bronze
MINIO_SECURE=false
INGEST_SOURCE=local
SOURCE_BUCKET=ase-source
SOURCE_PREFIX=raw/
```

Use the real values in the local .env file. Do not paste secrets into source code, documentation committed publicly, or GitHub. The examples above intentionally use placeholders for credentials.


## 11. Verification completed

| Verification | Result |
|---|---|
| Idempotency | Passed; unchanged inputs are skipped. |
| Incremental behaviour | Passed; changed/new files are processed without needlessly reprocessing unchanged siblings. |
| Local Parquet row counts | Passed; all five source folders total 412,621 rows. |
| PostgreSQL row counts | User reports checked and cleared against expected counts. |
| MinIO consistency | User reports checked and cleared against expected Parquet outputs. |
| Cross-storage baseline | 412,621 rows across the five original source files. |

Local Parquet verification output observed: employee_202510161125: 50 rows / 1 part; timesheet_202509151540: 269,921 / 6 parts; timesheet_202510161121: 48,258 / 1 part; timesheet_202510161122: 55,435 / 2 parts; timesheet_202510161124: 38,957 / 1 part.

The row-count and consistency checks establish that the initial outputs agree at the row-count level. They do not prove every value is identical byte-for-byte across all stores unless a separate content-level comparison is performed.


## 12. Known limitation: recovery is not yet proven

The current replacement process is not fully atomic at the per-file level. A changed file's previous outputs can be cleared before the new version finishes. If the new ingestion then fails, that file may be left incomplete until a successful retry.

- The registry is recorded only after success, so a failed run should not falsely register the new content as completed.
- Other files are designed to be handled independently, but the changed file's previous good output is not guaranteed to remain available during a failed replacement.
- A disposable test CSV should be used for recovery testing; never intentionally break an original source file.
- A future hardening step is staged output: write a new run to temporary table/prefix, validate it, then publish/swap it so the last known good output survives a failed attempt.
Recovery testing has not been reported as completed. It remains an explicit follow-up rather than a passed test.


## 13. Commands and operational notes

```text
cd D:\ASE-ETL
# Run ingestion from local data/raw (assuming the project venv is active)
python src\ingestion\bronze_ingestion.py --source local

# Run ingestion from MinIO source objects
python src\ingestion\bronze_ingestion.py --source minio

# Verify local Parquet row counts
python tests\verify_bronze_parquet.py
```

Use the actual script path in the repository if it differs; the existing ingestion module discussed during implementation is src/ingestion/bronze_ingestion.py. Run commands from the project root so relative paths resolve correctly.

PostgreSQL can be inspected through pgAdmin using host localhost, port 5433, database ase_etl, and username ase_user. MinIO console is available at http://localhost:9001. Use the password values from .env.


## 14. Project status and next steps

- Completed: Phase 1 profiling and planning; Bronze ingestion implementation; local and MinIO source modes; per-file hash registry; lineage metadata; idempotency and incremental behaviour verification; local Parquet row counts; PostgreSQL and MinIO consistency checks as reported.
- Not yet claimed complete: automated recovery testing and atomic/staged replacement.
- Next: proceed to Silver schema inspection and transformation design, using actual Bronze columns and Phase 1 findings. Do not modify Bronze to implement Silver.
Bronze's goal is source-faithful, traceable ingestion. It is not responsible for all business cleaning, final duplicate resolution, or analytics calculations; those belong in Silver and Gold.

