# ASE-ETL — Phase 8: Luigi Orchestration and End-to-End Reliability

**Project:** ASE-ETL Data Engineering Assignment 1  
**Phase:** 8 — Pipeline Orchestration and Integration  
**Status:** Functionally complete for the tested assignment workflows  
**Evidence date:** 18 October 2026  
**Supported entry point:** `python -m src.orchestration.pipeline --source {local|minio}`  
**Next phase:** Power BI visualization and presentation

---

## 1. Purpose and scope

Phase 8 integrates the separately developed Bronze ingestion, Silver cleaning/publication, Gold dimensional modeling/publication, API employee synchronization, SQL analytics, and CSV reporting into a single dependency-controlled workflow. Luigi is the orchestrator: it determines task order, checks completion outputs, blocks downstream execution when dependencies fail, and supports targeted retries. The existing layer implementations remain responsible for their transformations, data checks, and publication transactions.

The operational launcher generates a unique run ID by default, pins the source input snapshot, acquires a PostgreSQL advisory lock, launches Luigi against the central scheduler, verifies the source snapshot again, and checks that the reporting completion marker exists before declaring success.

**Boundary of completion:** The measured local- and MinIO-source workflows, reporting integrity checks, employee API create/delete propagation, failure recovery, concurrency rejection, and controlled duplicate-file ingestion all passed. The limitations in Section 11 are not represented as solved.

## 2. Architecture and data flow

```text
Local raw CSVs (data/raw/) OR MinIO raw CSVs (ase-source/raw/)
                          |
            [Source inventory + SHA-256 pin]
                          |
                 BronzeIngestion
            +-----------------------------+
            | PostgreSQL bronze.*_raw     |
            | Local Bronze Parquet        |
            | MinIO ase-bronze/raw/...    |
            | Ingestion registry          |
            +-----------------------------+
                          |
          SilverTransformation (reads LOCAL Bronze Parquet)
               |          + API employee-change ledger snapshot
               |          + 14-field timesheet deduplication
               |          + quality + schema + lineage audits
                          |
                  SilverPublication
           (PostgreSQL + local/MinIO Silver Parquet,
                 fingerprint + registry)
                          |
                    GoldModeling
              dim_employee + dim_date + fact_timesheet
                          |
                   GoldPublication
           (PostgreSQL publication + local/MinIO
                   fingerprinted artifacts)
                          |
              EmployeeSyncAcknowledgement
                 (matched Silver/Gold only)
                          |
                      SQLAnalytics
                  mart.* views and KPI SQL
                          |
                    ReportGeneration
           13 CSVs + manifest + SHA-256 checks
                          |
             RunETLPipeline (Luigi wrapper)
```

**Important:** `--source minio` selects the **raw CSV input for Bronze**. It does not make the Silver pipeline read directly from MinIO. Bronze produces local Parquet copies, which are the current Silver input.

## 3. Luigi task dependency graph

| Order | Luigi task | Existing executable or action | Dependency |
|---|---|---|---|
| 1 | `BronzeIngestion` | `src.ingestion.bronze_ingestion --source ...`; Bronze publication verifier | None |
| 2 | `SilverTransformation` | `src.silver.pipeline` (includes API change overlay staging) | Bronze ingestion |
| 3 | `SilverPublication` | `src.silver.publish_idempotent` | Silver transformation |
| 4 | `GoldModeling` | `src.gold.pipeline` | Silver publication |
| 5 | `GoldPublication` | `src.gold.release` | Gold modeling |
| 6 | `EmployeeSyncAcknowledgement` | `src.orchestration.acknowledge`, reusing Phase 7 revision checks | Gold publication |
| 7 | `SQLAnalytics` | `src.analytics.pipeline` | API sync acknowledgement |
| 8 | `ReportGeneration` | `src.analytics.reporting` and `tests.verify_analytics_reports` | SQL analytics |
| 9 | `RunETLPipeline` | Root Luigi `WrapperTask` | Report generation |

Eight tasks perform or verify processing stages; the ninth is the root wrapper that represents the complete workflow. All task identities include a `run_id` and a source mode (`local` or `minio`). Luigi uses `requires()`, `output()`, and task completion checks to enforce dependencies.

## 4. Detailed stage behavior

### 4.1 Source selection and Bronze ingestion

The launcher selects one raw CSV source: local `data/raw/` or a configured MinIO source bucket/prefix (tested as `ase-source/raw/`). The ingestion component discovers valid CSV filenames, computes source SHA-256 hashes, and uses `bronze._ingestion_registry` to skip unchanged files or ingest changed/new files. The raw fields are loaded as strings to avoid premature loss of formatting, and lineage is appended: `source_file`, `source_row_number`, `ingested_at_utc`, and `ingestion_batch_id`.

Bronze publishes full raw records **plus lineage metadata** to per-file PostgreSQL `bronze.*_raw` tables, local `data/bronze/parquet/` files, and the configured MinIO Bronze bucket. Bronze Parquet is not merely a separate metadata-only export.

### 4.2 Silver transformation, quality control, and publication

Silver reads the **local Bronze Parquet** parts, not the MinIO objects directly. Each part is aligned with an approved schema contract. Missing optional columns are represented as NULL; missing required columns, unknown columns, and duplicate column names are rejected. Textual missing markers and appropriately configured masked placeholders are normalized; numeric, date, and timestamp fields receive their intended types. Columns that happen to contain only missing values are retained when part of the contract.

Timesheets from all source exports are combined. Cross-file duplicate occurrences are identified using **14 timesheet business columns**, distinct from lineage. A deterministic survivor is selected (latest sortable source filename, with source-row tie-break), and removed occurrences are recorded in an audit. The Silver PostgreSQL timesheet primary key is the composite `(client_employee_id, punch_in_datetime, punch_out_datetime)`; it is **not** the same thing as the 14-column duplicate comparison. If two nonidentical records collide on the composite key, primary-key validation rejects the collision rather than silently merging events.

The Silver pipeline validates and quarantines critical rows and creates employee, timesheet, and audit Parquet datasets. In the API overlay step, it captures committed employee ledger events, replays per-employee revisions, and derives the staged employee profile output and `audit/api_employee_overrides` snapshot. The Silver publisher validates reconciliation, publishes PostgreSQL Silver tables and fingerprinted Parquet in local storage/MinIO, and registers the active publication in `silver._publication_registry`. The verified baseline is 50 employee profiles and 381,732 unique timesheet records.

Schema evolution is **controlled**, not unrestricted: missing optional fields are accommodated automatically, but newly introduced unknown columns require an approved schema-contract/code change. Multiple employee export files with overlapping `client_employee_id` values are not automatically reconciled as employee updates; duplicate identities may trigger validation failures.

### 4.3 Gold dimensional warehouse and publication

Gold reads the **published Silver PostgreSQL tables** and constructs a star schema:

| Table | Grain / key | Role |
|---|---|---|
| `gold.dim_employee` | One distinct employee identity; `employee_key` identity surrogate PK; unique `client_employee_id` | Includes both profile-bearing identities and timesheet-only identities, flagged using `has_employee_profile` |
| `gold.dim_date` | One calendar date; `date_key` in YYYYMMDD integer form | Calendar attributes and date foreign-key target |
| `gold.fact_timesheet` | One deduplicated timesheet event; generated `timesheet_key` PK; `employee_key` and multiple date-key FKs | Event-level working-time fact, with original business identity and lineage where specified |

Employee IDs are drawn from the **union** of Silver employee profiles and distinct timesheet employee IDs. Thus an employee without timesheets can exist in `dim_employee`, and a timesheet employee without an employee profile can also exist in `dim_employee`. However, **the fact table contains only actual timesheet events**: an employee with no timesheets has no corresponding fact row. `date_key` is deterministic YYYYMMDD, not a PostgreSQL-generated identity sequence.

Gold publishes its modeled tables to PostgreSQL, exports local/MinIO Parquet artifacts, and records the active Silver fingerprint, Gold model version, Gold snapshot fingerprint, and publication status in `gold._publication_registry`. The baseline Gold checks previously verified 10,794 employee identities, 183 dates, and 381,732 timesheet facts.

### 4.4 SQL analytics, data marts, and reports

The analytics module uses published Gold to build and validate `mart.*` views/KPI outputs. These are exported to 13 locally stored CSV reports, under a unique `reports/analytics/<report_batch>/` directory. Each report batch includes `manifest.json` with the source view, columns, row count, SHA-256, and referenced Gold publication fingerprint. Reporting reads a consistent database snapshot.

The Luigi `ReportGeneration` task identifies the batch produced by its own exporter invocation, verifies that specific directory, and validates the report inventory and file hashes. Its `complete()` method also compares the report manifest's Gold fingerprint with the active published Gold fingerprint. **This does not replace detection of new un-ingested Bronze inputs or pending API changes**; newly started runs are required to process those.

The 13 report names are: monthly headcount/turnover; department tenure; employee working hours; daily attendance; daily overtime; rolling working hours; early attrition; monthly workload; department workload; attendance compliance; department overtime; weekday workload; workforce changes.

### 4.5 API employee synchronization

FastAPI exposes protected employee CRUD endpoints and read-only timesheet endpoints. Administrators may create/update/soft-delete employee profiles; authorized viewers may read employee and timesheet data but not mutate employee data. The `/docs` endpoint provides the FastAPI/Swagger interface; this phase does not claim a separately built custom frontend.

An API mutation updates the **operational** `api.employees` model and appends to `api.employee_change_log`; it **does not directly write to Silver or Gold**. This operational schema is not a byte-for-byte clone of all Silver employee columns. The change is marked pending. On the **next new Luigi pipeline run**, Silver transformation captures a committed ledger snapshot and overlays it onto cleaned Bronze employee profiles. After Silver publication, Gold modeling, and Gold publication succeed, the acknowledgement task checks the staged audit and matching Silver/Gold fingerprints, then marks only the matching published employee revision as synced. Changes arriving after snapshot capture remain pending for a later run.

API-created IDs that subsequently appear as employee IDs in newly ingested source profiles are a **source-of-truth collision**, not an automatic last-write-wins merge. The existing overlay validates such conflicts; a deliberate reconciliation policy would be needed for that case.

## 5. Orchestration controls and operational artifacts

| Mechanism | Implementation | Intent / boundary |
|---|---|---|
| Unique run IDs | UTC timestamp + short random identifier | Separate new workflow executions from retries |
| Reusable run IDs | Explicit `--run-id ...` | Retry unfinished tasks using their saved outputs |
| Source pinning | SHA-256 per discovered local/MinIO raw CSV | Reject retries with changed source bytes; verify before and after Luigi |
| Incremental Bronze registry | Source filename + content hash + ingestion metadata | Skip unchanged files and register newly ingested/replaced sources |
| Publication registries | `silver._publication_registry` and `gold._publication_registry` | Verify active snapshots and prevent acknowledgment before matching publication |
| Concurrency control | PostgreSQL session advisory lock `7182499` held by launcher | Reject overlapping invocations through supported launcher |
| Task state | JSON targets under `data/orchestration/state/` | Luigi completion state for a specific task/run/source |
| Task logs | Files under `data/orchestration/logs/` | Per-stage diagnostics and failure investigation |
| Report integrity | Per-batch manifest and CSV SHA-256; active Gold check | Detect damaged or mismatched report outputs |

The launcher relies on a running Luigi central scheduler on `127.0.0.1:8082`, and on PostgreSQL/MinIO services and project environment configuration. Luigi's central scheduler coordinates dependency scheduling; **this project has not been shown to have a recurring time-of-day trigger**. A Windows Task Scheduler job, cron, or another scheduler could be added later if periodic execution becomes necessary.

## 6. Execution and recovery runbook

Run from `D:\ASE-ETL` with `.venv` active, Docker-backed dependencies available, and the central Luigi scheduler running. Where necessary, start it in another terminal:

```powershell
luigid --address 127.0.0.1 --port 8082
```

**New local-source run:**

```powershell
python -m src.orchestration.pipeline --source local
```

**New MinIO-source run:**

```powershell
python -m src.orchestration.pipeline --source minio
```

**Recover an existing run (provided its sources have not changed):**

```powershell
python -m src.orchestration.pipeline --source local --run-id <original-run-id>
```

**Read-only operational checks:**

```powershell
python -m src.orchestration.source_snapshot --source local
python -m src.orchestration.source_snapshot --source minio
python -m src.orchestration.api_state
python -m tests.verify_analytics_reports
python -c "from src.silver.publish import verify_local; print(verify_local())"
```

If a task fails, review the named task log, repair the cause, and retry **the same run ID**. Luigi will skip already completed dependencies when their completion targets remain valid. Do not delete the state directory wholesale or rerun direct publisher modules while another workflow is active. If underlying source bytes changed since a run began, start a new run instead of retrying the old one. A source file removed from the input location does not automatically remove its old Bronze registry/table/objects; source retirement requires an explicit, verified cleanup/reconciliation process.

## 7. Evidence and acceptance tests

| Test / scenario | Observed result | Conclusion |
|---|---|---|
| Initial complete workflow (`full-test-002`) | 9/9 Luigi tasks; 13/13 verified reports | Dependencies and report integrity passed |
| Repeated completed run | 1 root task already complete; 0 tasks rerun | Basic completion-marker behavior passed |
| Reporting failure injection (`recovery-test-001`) | Reporting failed; root blocked; retry ran only report + wrapper | Dependency failure propagation and retry passed |
| Windows log-decoding failure | Fixed `UnicodeDecodeError`, recovered existing run without rerunning upstream | Practical recovery passed |
| Local source snapshot | Original SHA-256 inventory pinned and checked at both boundaries | Source changes detected at run boundaries |
| Local versus MinIO raw source | Five filenames and five SHA-256 hashes identical | Raw source inputs equivalent |
| MinIO-mode complete workflow | 9/9 Luigi tasks; 13/13 reports | Alternative source mode integrated; unchanged files may be skipped by Bronze registry |
| API test create | `ASE_LUIGI_E2E_002` pending, then synced; 1 row in API, Silver, and Gold | API create propagated |
| API test soft delete | API revision 2 synced, retained soft-deleted record; zero corresponding Silver/Gold profiles | API deletion propagated |
| Lock contention | Second launcher raised `An ASE-ETL workflow is already running`; lock available after release | Launcher concurrency protection passed |
| Incremental duplicate-only CSV | Ten additional Bronze source rows; Silver timesheets unchanged at 381,732; duplicate audit 30,839 → 30,849 | New-file ingestion and cross-file deduplication passed |
| Controlled test-source cleanup | Test Bronze files/registry/table removed; clean full run 9/9 | Baseline restored |
| Final state (`20261018T113817Z_fdc9474a`) | 50 Silver employee profiles; 381,732 Silver timesheets; 30,839 duplicate audit; 11 schema audit parts; 2 API override audit identities; 13/13 reports; 0 pending API changes | Verified final working state |

The final API operational table held **52 total records, all marked synced**, including two deliberately soft-deleted test identities. API record counts are therefore not expected to equal active Silver employee profile counts.

## 8. Data quality and reconciliation summary

Baseline raw Bronze ingestion comprises one employee source file with 50 rows and four timesheet source files with 412,571 rows, for **412,621 raw data rows**. Silver combines timesheet source files, audits **30,839** duplicate occurrences, and publishes **381,732** unique timesheet events, with zero critical employee/timesheet rejects in the verified baseline. The expected reconciliation is `412,571 = 381,732 + 30,839 + 0`.

Schema-audit records describe the 11 original Bronze Parquet parts. In the controlled new-source test, an additional 10-row duplicate-only CSV increased the audit part count to 12 and duplicate audit to 30,849 while leaving Silver timesheet events unchanged. After verified cleanup and rerun, all counts returned to baseline.

## 9. Files and modules relevant to Phase 8

- `src/orchestration/config.py`: project, log, and state paths.
- `src/orchestration/pipeline.py`: supported end-to-end launcher; run IDs, source checks, session advisory lock, subprocess execution, success checks.
- `src/orchestration/tasks.py`: Luigi task classes and dependency chain.
- `src/orchestration/source_snapshot.py`: local/MinIO SHA-256 inventories and pinned-run comparison.
- `src/orchestration/api_state.py`: read-only pending/synced operational API status.
- `src/orchestration/acknowledge.py`: publication-qualified API revision acknowledgement.
- `src/ingestion/bronze_ingestion.py`: incremental local/MinIO Bronze source ingestion.
- `src/silver/pipeline.py`, `src/silver/publish_idempotent.py`, `src/silver/publish.py`: Silver staging, reconciliation, fingerprinted publication and schema checks.
- `src/api/employee_sync_stage.py`, `src/api/employee_sync_release.py`: API overlay and staged-revision verification.
- `src/gold/pipeline.py`, `src/gold/release.py`: dimensional warehouse building and publication.
- `src/analytics/pipeline.py`, `src/analytics/reporting.py`: SQL marts/KPIs and 13 CSV exports.
- `tests/verify_bronze_publication.py`, `tests/verify_analytics_reports.py`: data and report verification.

## 10. Operational decisions

1. **Both employee files and timesheet files remain supported inputs.** API CRUD is an additional authenticated way to maintain employee profiles, not an architectural replacement for employee source ingestion.
2. **Employee source-profile conflicts are deliberately validated.** A new source file with new employee IDs can be processed if the schema and identity checks pass. A repeated export of existing employee IDs is not an automatic incremental employee merge, and an ID previously created through API may conflict when it later appears in Bronze.
3. **The local Bronze Parquet directory is the current Silver intake.** MinIO support concerns Bronze raw input and publication artifacts at each layer.
4. **CLI execution is sufficient for the assignment.** A one-click launcher or scheduled production trigger can be deferred until after Power BI and documentation.

## 11. Known limitations and follow-on improvements

- Source SHA-256 pinning checks bytes at invocation boundaries; it is not full mid-run transactional snapshot isolation. A source mutated and reverted while the run is active could evade the boundary checks.
- The PostgreSQL advisory lock is enforced by the supported orchestration launcher, not by every possible direct invocation of Silver/Gold scripts.
- Existing run IDs may retain stale upstream task markers; the report completion check validates active Gold but does not independently discover brand-new source files or pending API revisions before those inputs are processed. Start a **new run** for new work.
- Additional unknown columns require updating the approved Silver schema contract. Missing optional columns are supported as NULL; required columns remain mandatory.
- Conflicting source employee exports and API-created identities require an explicit source-of-truth/merge policy; avoid representing these as automatically resolved.
- The new-file integration test used only duplicate existing timesheet rows. It demonstrated ingestion, reconciliation, and deduplication, **not** propagation of genuinely new timesheet business events.
- MinIO input testing used files already present in the Bronze registry; successful MinIO-mode orchestration does not independently demonstrate a first-ever raw-object upload from MinIO.
- Raw files removed from a source are not automatically retired from Bronze; controlled cleanup is required.
- An API revision committed during a run is intended to remain pending for a later run, but rigorous concurrent-mutation / publication-race testing beyond the demonstrated scenarios is still future hardening.
- Regular time-based scheduling and a separate one-click GUI have not been implemented or required for the completed assignment workflow.

## 12. Conclusion and next phase

Phase 8 successfully integrated the existing ASE-ETL components into a recoverable Luigi dependency graph with distinct local and MinIO source modes. The final test returned the system to its original baseline and verified all 13 analytics CSVs and the absence of pending API changes. The supported command-line launcher is sufficient for the current assignment. The logical next step is **Power BI**, using the validated `mart.*` SQL outputs and/or the latest verified analytics report batch, followed by final project-wide documentation and GitHub commit verification.
