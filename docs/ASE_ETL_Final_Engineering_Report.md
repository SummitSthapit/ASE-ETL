# ASE-ETL | Final Engineering Report

**Assignment:** Data Engineering Assignment 1 — *ETL to Insights*  
**Project:** ASE-ETL  
**Repository:** https://github.com/SummitSthapit/ASE-ETL  
**Review date:** 19 October 2026  
**Scope:** Implemented pipeline, supported engineering decisions, test evidence, and known limitations

> **Evidence standard.** “Implemented” refers to code and phase reports available on the project’s GitHub `main` branch, supplemented by the final command outputs shared during this project. Counts describe the validated supplied snapshot, not universal invariants. A prior demonstration is identified as prior evidence where it was not rerun in the final verification session. This report does not claim independent execution of the Windows environment by its author.

## Executive summary

ASE-ETL delivers a modular, source-to-insight data engineering pipeline for an employee roster and four overlapping timesheet extracts. Python, pandas, PostgreSQL 16, MinIO, Parquet, SQL, Luigi, FastAPI and Power BI form a medallion-style system: **local files or MinIO CSV → Bronze → Silver → Gold → SQL marts and 13 report CSVs → Power BI**. A separate authenticated employee-management API records operational changes that are subsequently applied through controlled Silver and Gold publications.

The source comprises **50 employee records** and **412,571 timesheet occurrences**. Cross-file overlap accounts for **30,839 duplicate occurrences**; **381,732 unique accepted timesheet events** remain. Gold contains **10,794 employee identities**, despite only **50 available employee profiles**, because missing roster coverage must not invalidate otherwise legitimate timesheets. The analytics layer implements **all nine assignment KPIs** and six additional views while explicitly disclosing unknown pay-code semantics, incomplete roster coverage and provisional overtime estimates.

During the final October 19 session, the user reported **22/22 analytics tests passing**, both Luigi source-mode executions finishing with **9/9 successful tasks**, Bronze/Gold integrity checks passing, and **13/13 generated CSV reports validating**. Swagger/Uvicorn logs additionally demonstrated administrator and viewer access boundaries, including the expected **403 Forbidden** responses for viewer mutations. Earlier Phase 7 evidence records **44 API regression tests passing** and successful employee-create/update/delete synchronization exercises. The Power BI file contains three report pages; its import is tied to an older, validated CSV snapshot rather than automatically following each new run.

## 1. Requirements and solution boundary

The assignment asks for configurable local/MinIO extraction, cleaning and derived business metrics, post-processing quality checks, dependency-driven orchestration, nine SQL insights, authenticated employee CRUD and read-only timesheets, at least three effective visualizations, a normalized/relational design with constraints/indexes, and documentation. The repository contains the respective modules and tests. Database migrations and containerization are considered additions; the current Docker Compose file runs **PostgreSQL and MinIO**, not every Python application in containers.

| Assignment objective | Implementation and evidence |
|---|---|
| Extract and load from two source modes | `src/ingestion/bronze_ingestion.py`; final local and MinIO Luigi runs passed |
| Transform and validate | `src/silver/`; explicit missing-marker, type, deduplication, schema and quality rules |
| Serve relational analytics | `gold.dim_employee`, `gold.dim_date`, `gold.fact_timesheet`; `mart` SQL views |
| Automated processing and reruns | Nine Luigi tasks, run IDs, per-stage artifacts, locks, source snapshots and recovery exercises |
| API access control | FastAPI JWT with admin/viewer roles; manual status-code verification and earlier tests |
| Insight generation | Nine required and six supplementary SQL KPIs; 13 hash-verified CSV exports |
| Visualization | Three Power BI report pages with scoped slicers and documented interpretation rules |
| Documentation | This report and accompanying setup, usage, schema and verification guides |

## 2. Source understanding and decisions made before implementation (Phase 1)

The five source CSVs are one **34-column employee** export (50 rows) and four **14-column timesheet** exports (269,921; 48,258; 55,435; 38,957 rows). We profiled actual columns, CSV parsing, ID patterns, null-like representations, candidate keys, date coverage, pay codes and recorded-versus-elapsed hours **before finalizing a physical model**. This prevented schema decisions based on assumptions.

Several findings changed the design:

- **Identifier preservation:** Employee identifiers such as `00037` contain leading zeros. Ingestion therefore reads business identifiers as strings, not integers. Names and dates are not substituted as primary keys merely because they look unique in 50 records.
- **Sparse roster relationship:** Among 10,773 timesheet employee IDs, only 29 have an associated employee profile; 10,744 lack one. A mandatory roster-match filter would destroy nearly all observed timesheet facts. Unmatched IDs are preserved and flagged.
- **Cross-file repetition:** Individual timesheet files showed no exact intra-file duplicates, but the two overlapping October-file pairs contributed **23,892 + 6,947 = 30,839** duplicate occurrences. Appending all exports without a cross-file comparison was rejected.
- **Business event grain:** `employee_id + punch_apply_date + pay_code` was not unique. Even an employee/date cannot uniquely identify a shift or pay event. The tested event key `client_employee_id + punch_in_datetime + punch_out_datetime` was selected for relational uniqueness, while **all 14 normalized business columns** determine identical export occurrences in Silver. Both are documented assumptions pending a source-system transaction ID.
- **Missing and masked values:** Literal `[NULL]`, blanks and `X`-masked fields are not equivalent to trustworthy data. Empty schedule fields must not cause entire valid files to be dropped; original representations are retained in Bronze.
- **Hours validation:** Recorded `hours_worked` can differ from punch-to-punch elapsed time. Without confirmed break/pay rules, overwriting recorded hours with elapsed hours would silently change payroll meaning. Recorded hours remain the source measure; discrepancy is audited.

The initial architecture alternatives were conventional one-pass ETL versus Bronze/Silver/Gold and pandas versus Spark. The team chose **medallion separation with pandas**, because roughly 413,000 source records are practical for controlled chunked processing on the supplied machine and because traceable raw retention, schema evolution and published analytics were more valuable than adding distributed-processing infrastructure.

## 3. Architecture, infrastructure and data ownership (Phase 2)

**PostgreSQL** supplies relational integrity, transactions, constraints, SQL and indexing. **MinIO** supplies S3-compatible raw input and published Parquet artifacts. **Parquet/PyArrow** supports typed, columnar stage snapshots; **pandas** handles explicit transformations and auditing. **Luigi** supplies transparent task dependencies and recoverable run targets rather than hiding ETL behavior behind a managed service. **FastAPI** provides a typed REST interface and generated Swagger/OpenAPI pages. **Power BI Desktop** is a visualization client of SQL-derived reporting datasets.

The responsibilities are intentionally separated:

```text
CSV input (data/raw/ OR MinIO ase-source/raw/)
  -> Bronze: immutable-ish raw representation + PostgreSQL source tables + provenance
  -> Silver: explicit schema/type/missing-value rules, rejected-record audits, deduplication
  -> Gold: employee/date dimensions and event-grained fact
  -> mart.*: SQL classification, workforce, hours, attendance and overtime definitions
  -> reports/analytics/<run-specific folder>/: 13 CSVs and manifest
  -> powerbi/ASE_Workforce_Intelligence.pbix: three analytical pages

FastAPI admin edits -> api.employees + api.employee_change_log
  -> Silver employee overlay -> Gold matching release -> revision acknowledgment
```

**Important boundary:** MinIO mode selects the **raw Bronze input**. Silver currently reads **local Bronze Parquet** produced by ingestion, not directly from MinIO. Docker Compose provides the database and object store, while Python/Luigi/FastAPI/Power BI run in the host environment. `.env` holds environment-dependent settings and must not be committed.

The physical design evolved beyond Phase 2's provisional plans; authoritative later schema contracts and migrations take precedence over early planning diagrams.

## 4. Raw ingestion and traceability (Phase 3 — Bronze)

Bronze ingests local or MinIO CSV without recasting source business values, uses **50,000-row chunks**, and populates source-named PostgreSQL raw tables plus local and MinIO Parquet. Four columns provide traceability: `source_file`, `source_row_number`, `ingested_at_utc` and `ingestion_batch_id`. `bronze._ingestion_registry` records source filename, SHA-256 hash, loaded rows, Parquet parts and ingestion batch.

**Why preserve duplicates and masked fields?** Bronze is the evidence layer: changing source rows or removing uncertain records here would make later business-rule corrections irreproducible. A hash-backed per-file registry allows unchanged inputs to be skipped and newly discovered/replaced files to be identified. Source hashes also help protect reruns against accidental input changes. Bronze originally established **412,621** rows across **11 Parquet parts** (one employee, ten timesheet).

A caveat remains: removing a file from the upstream source does not automatically remove its prior Bronze state; that requires controlled retirement. Per-file hash checks establish source change detection, not an end-to-end distributed transaction.

## 5. Transformations, quality policy and corrective redesign (Phase 4 — Silver)

The most consequential implementation revision occurred here. **Silver v1 excluded 12 empty or masked employee attributes** (`middle_name`, `years_of_experience`, `zip`, `manager_employee_name`, `is_per_deim`, `work_phone`, `clinical_level`, `address`, `city`, `state`, `country`, `cell_phone`). This risked permanently losing new values appearing in future extracts. After reviewing the role of Silver as a reusable cleaned source, the implementation was **revised to Silver v2**: all 34 employee source fields and 14 timesheet source fields are retained, plus four lineage fields per record. Gold, not Silver, selects what belongs in analytical dimensions.

Silver v2 enforces an **explicit version 2.0 schema contract**. Missing approved optional columns can be added as nullable and reported; absent required fields, duplicate names or unexpected new columns are rejected for review. Schema failures leave local diagnostics. Future unknown fields are **not** automatically inferred into the production schema; evolution is controlled. Eleven Bronze parts passed the approved schema without optional alignment in the supplied snapshot.

Transformations trim whitespace; convert agreed missing markers such as `[NULL]` and `N/A` into nulls; parse dates, timestamps and numeric hours; and preserve identifiers and uncertain categories as text. Fully masked address/place strings and partially masked phone values receive column-specific null handling (avoiding false positives for legitimate words containing `x`). Zero or negative hours, invalid required IDs/dates or reversed punches can raise critical findings; warnings remain distinguishable from quarantine. Source values are never retroactively rewritten in Bronze.

**Duplicate correction:** Identical rows are determined using all **14 normalized timesheet business fields**, ignoring lineage so two export copies compare equal. One deterministic survivor is kept; discarded occurrences are retained in `audit/duplicate_records` with traceability. Reconciliation is:

| Silver v2 reconciliation | Rows |
|---|---:|
| Bronze timesheet occurrences | 412,571 |
| Deduplicated accepted timesheet events | 381,732 |
| Audited duplicate occurrences | 30,839 |
| Critical timesheet rejects | 0 |
| Bronze employee rows → accepted employee profiles | 50 → 50 |
| Schema-part audit records | 11 |

Silver writes seven primary datasets: `employees`, `timesheets`, `audit/duplicate_records`, `audit/quality_issues`, `audit/rejected_employees`, `audit/rejected_timesheets`, and `audit/schema_evolution`. API integration later adds an employee-override audit. Zero-row audit datasets are intentionally meaningful evidence. PostgreSQL Silver snapshots, local Parquet and MinIO publication are validated through fingerprints and registry state.

**Trade-off:** This approach favors auditable completeness over discarding difficult data; it also requires clear classification of unknown values, schema contracts and publication coordination. Staged local directory swaps are not a single atomic multi-directory transaction, and the MinIO and PostgreSQL commits are not one distributed transaction.

## 6. Dimensional warehouse and relationship strategy (Phase 5 — Gold)

The Gold model is an **event-grained star schema**, not a preaggregated daily table. `gold.dim_employee` stores one identity per distinct employee ID occurring in either the source roster or timesheets. A nullable descriptive profile with `has_employee_profile` distinguishes the **50 real profiles** from **10,744 timesheet-only IDs**. `gold.dim_date` covers relevant apply, punch-in and punch-out dates (183 consecutive dates, 18 April–17 October 2025). `gold.fact_timesheet` stores one accepted event with three foreign keys to the same date dimension and one employee foreign key.

Relational keys use surrogate `employee_key` and `timesheet_key` for stable joins, combined with unique business identifiers/event constraints. The fact remains event-level so multi-event days are not lost. `dim_employee` uses **SCD Type 1** for the currently known employee attributes: later updates replace descriptions while retaining keys; historical profile versions are not modeled as Type 2. Personally sensitive columns not required for the warehouse are left in Silver rather than copied to the analytics-facing dimension.

Gold reads the registered **published PostgreSQL Silver state**, verifies its source fingerprint and expected structure, publishes controlled database state, then exports local/MinIO Parquet with a manifest and release registry. Content-addressed releases and `DB_READY`/`PUBLISHED` states separate database readiness from complete publication. Independent integrity checks validate row counts and referenced remote object hashes. A known boundary is that previously published fast paths do not always rehash every remote object; use the independent verifier when release integrity matters.

## 7. SQL marts, measures and performance (Phase 6)

The analytical layer uses `src/analytics/sql/01_schema.sql` through `14_performance_indexes.sql`, materializing definitions principally as **SQL views** over Gold. Views avoid duplicating large warehouse datasets and keep the source of truth centralized, at the expense of query-time computation. Data-mart semantics are deliberately distinct from Gold's reusable event model.

**All nine mandatory KPIs are represented:** monthly active headcount, monthly turnover, department tenure, average eligible hours per employee, late arrival count, early departure count, shift-overrun/overtime count, seven-calendar-day rolling average hours and early attrition rate. Six additional outputs cover monthly workload, department workload, attendance compliance, department overtime frequency, weekend/weekday hours and workforce changes. The exact SQL view names and caveats are cataloged in `DATABASE_SCHEMA.md`.

The business rules are explicit. Late arrival means actual punch-in **more than five minutes** after schedule; early departure is more than five minutes before schedule; overtime is an elapsed-duration overrun by **more than five minutes** (a *proxy*, not approved payroll overtime). Rolling hours average eligible observed workdays in a seven-day calendar window; absent days are not automatically zero-filled. Early attrition is departure before 90 days, measured over cohorts with the full 90-day observation window. April and October are partial reporting months, not complete monthly coverage.

**The main analytical uncertainty is pay-code interpretation.** Source codes can be composite; the pipeline decomposes them and uses a conservative rule set. Of 246 observed pay-code components, **236 remained unclassified**; at the validated baseline, 78,417 of 381,732 timesheet events had unresolved eligibility. Unknown or mixed codes are not silently counted as ordinary work. Observed classified working hours, attendance compliance and overtime therefore describe **defined eligible subsets**, not automatically complete payroll or organization-wide truth.

A measured indexing exercise added three targeted indexes on `(employee_key, punch_apply_date_key)`, `(punch_apply_date_key, employee_key)` and profiled employee `(hire_date, term_date)`. Tests showed strong improvement on a selective workforce query but **not uniform acceleration** of expensive full-table aggregations. The documentation preserves the measured trade-off rather than claiming that every query was faster.

Thirteen SQL-based CSV exports are produced by the reporting task with immutable timestamped folders, `manifest.json`, row counts and SHA-256 hashes. Their independent verifier checks that a report batch is structurally intact.

## 8. Operational API, permissions and synchronized releases (Phase 7)

The assignment requires employee CRUD but a direct API update to published Silver/Gold would invalidate snapshots and bypass quality controls. The chosen design introduces **`api.employees`**, an operational writable projection, plus an append-only **`api.employee_change_log`** and per-employee revisions. Admin CREATE/PATCH/soft DELETE affect this operational model immediately; Silver overlays the acknowledged change history on source profiles in a later pipeline run, Gold republishes, and the synchronization step marks a revision `synced` **only when its matching Silver/Gold release is published**. Newer changes remain pending rather than being incorrectly acknowledged.

FastAPI exposes employee list/read/write routes and timesheet read-only routes with employee/date filters and pagination. Authentication uses database users and **Argon2id-hashed passwords**, **HS256 JWT access tokens** with a configured 64-hex-character signing secret and 15-minute expiry, active-account/token-version checks, login throttling and event auditing. Admin can mutate employee records; viewer can read employees and timesheets but not mutate. The `/docs` Swagger interface is developer-facing API documentation; a separate frontend is **not required** by the assignment.

Earlier isolated Phase 7 verification recorded **44 passing API tests**, including roles, token validation, CRUD, ledger behavior and API-to-Silver/Gold publication recovery. In the final manual demo, admin auth/read/DB health calls returned HTTP 200; viewer reads returned 200; attempts to call admin-check, POST employee, or DELETE employee as viewer returned expected HTTP **403**. Those rejected writes demonstrate working authorization, not failed CRUD implementation.

**Trade-off:** The API is eventually consistent with the analytics warehouse; data is intentionally not instantly modified in Gold. Its local privileged user-provisioning CLI is appropriate for this development assignment, but not a production self-service account-management interface. HTTPS, centralized deployment, broader concurrency controls and production monitoring are outside the proven scope.

## 9. Dependency-driven orchestration, recovery and practical fixes (Phase 8)

Luigi expresses a nine-task chain: `BronzeIngestion → SilverTransformation → SilverPublication → GoldModeling → GoldPublication → EmployeeSyncAcknowledgement → SQLAnalytics → ReportGeneration → RunETLPipeline` (root wrapper). The entry point `python -m src.orchestration.pipeline --source local|minio` creates a run ID, pins source files by SHA-256, acquires a PostgreSQL advisory lock to reject overlapping supported launches, delegates execution to the central scheduler on port 8082, and verifies report completion and source immutability at run end.

Prior integration exercises verified full local and MinIO runs, API create/delete propagation, source-mode equivalence, duplicate-only incremental ingestion, failure-induced report-task retry, completed-run skip behavior and concurrent-launch rejection. A **Windows log-decoding `UnicodeDecodeError`** during a recovery test led to a defensive decoding change; the interrupted run was resumed without rerunning successful upstream tasks. Earlier hard-coded expected row counts were also made more dynamic in orchestration/reliability refinements so valid API overlay changes and additional inputs could reconcile correctly rather than spuriously fail.

**Run IDs are meaningful:** a new run is required to capture genuinely new source files or pending API changes; replaying a completed run ID may simply reuse Luigi success targets. Luigi is a dependency scheduler here, **not a recurring clock-time trigger**. Source hashes are checked at boundaries, not continuously while files are being processed. A launcher-level lock does not prevent unsafe manually initiated writes from every possible direct module invocation.

## 10. Power BI data storytelling and corrections (Phase 9)

`powerbi/ASE_Workforce_Intelligence.pbix` contains three pages: **Workforce Overview**, **Retention & Workforce Stability**, and **Attendance & Workload**. The report consumes the approved SQL CSV extracts via Power BI Import, with month, department and selected-employee interactions scoped to the appropriate datasets. DAX largely combines prepared SQL outputs instead of duplicating the complex eligibility or attendance logic.

Development review corrected **Power Query type-conversion errors** in the daily overtime and rolling-working-hours imports. A separate interpretation issue was identified: days or departments with very few eligible attendance events can display visually dramatic **100% compliance/overtime**. The report's documented response includes discontinuity-aware daily charting, sample-count interpretation, and recommendation of department minimum-sample thresholds. A recommended 30-event threshold was **not confirmed as applied** in the final screenshots (the last noted label showed a minimum of five). Another open presentation check is whether the average-tenure card visibly includes “years”; a directly visible employee average-hours ranking is an optional clarity improvement.

The published PBIX uses a **fixed October 18 reporting folder** (`20261018T114042Z_4ed531be`), not the later October 19 CSV output. Its existing visualizations were validated against the former source, but refreshing the PBIX without updating Power Query source paths **will not switch to the new run**. We preserve this honest provenance rather than claiming real-time or automatically current BI. The assignment asks for usable visualizations, not Power BI Service deployment.

## 11. Validation, correctness and accepted limitations

### Evidence recorded at final review

| Verification | Result |
|---|---|
| Container services | PostgreSQL healthy and MinIO running |
| Bronze stored occurrences | 412,621 rows; five source extracts; publication verifier passed |
| Silver deduplication baseline | 30,839 duplicate occurrences; 381,732 unique timesheets |
| Gold publication integrity | 10,794 employee identities; 183 dates; 381,732 facts; MinIO verifier passed |
| SQL formula/integration/supplementary tests | **22 passed** |
| Luigi local source, run `20261019T173314Z_f6269e64` | **9/9 tasks successful** |
| Luigi MinIO source, run `20261019T173515Z_475f7000` | **9/9 tasks successful** |
| Post-run analytics reports | **13/13** exports verified |
| FastAPI manual role checks | Authorized calls HTTP 200, viewer mutations HTTP 403 |
| Earlier Phase 7 API regression | **44 passed** in isolated test database (historical evidence) |

The complete command/output provenance and 13 report row counts are in `FINAL_VERIFICATION.md`.

### Limitations affecting interpretation or reproducibility

1. **Roster coverage:** only 50 employee profiles describe a 10,794-identity timesheet population. Headcount, attrition and tenure are profile-cohort measures, not global personnel totals.
2. **Pay-code classification:** 236/246 components remain unresolved; neither eligible hours nor shift-overrun overtime should be called complete payroll truth.
3. **Source semantics:** no authoritative global timesheet transaction ID, break rules, pay-code dictionary or explanation of anomalous test IDs has been supplied.
4. **Snapshot architecture:** the MinIO source mode stages locally before Silver; PostgreSQL and MinIO are not governed by a distributed transaction.
5. **Time and history:** fact coverage is 18 April to 17 October 2025, with incomplete end months; employee attributes in Gold use SCD1, not full historical profile tracking.
6. **Operations:** no proven always-on schedule, full containerization, disaster-recovery drill or multi-instance distributed staging guarantees; the supported host launcher is the tested path.
7. **Dashboard provenance:** Power BI is a fixed validated October 18 import, not an automatically refreshed October 19 release. Some visual refinement suggestions remain unconfirmed.
8. **Clean-install sequence:** The demonstrated runs used an initialized database. The fresh-volume setup guide documents a code-inspected API-ledger bootstrap bridge, but a cold-start execution of that bridge is not independently verified.

These are documented boundaries, **not** omissions from the core assignment requirements.

## 12. Final engineering assessment and handoff

The project satisfies the **implemented scope** of the assignment with substantial verification: all major pipeline stages are present, both raw input modes ran through the orchestration, required SQL KPI calculations exist and passed targeted regression tests, role-based API behavior was demonstrated, and three visualization pages were produced. What this evidence does **not** establish is full production readiness, certified payroll interpretation or a byte-level reconciliation of the PBIX against the last run.

For a reproducible handoff, use the accompanying documents in this order: `SETUP_GUIDE.md` for environment preparation; `USAGE_GUIDE.md` for running and operating the software; `DATABASE_SCHEMA.md` for entities, keys and data contracts; and `FINAL_VERIFICATION.md` for commands, observed results and evidence scope. The original nine phase reports remain the detailed engineering history and should be kept alongside this consolidated overview.

## Sources and evidence trail

- **Assignment:** `Data_Engineering_Assignment_1.pdf`, pages 1–5 (user-supplied).
- **Primary code and committed phase reports:** https://github.com/SummitSthapit/ASE-ETL (`docs/ASE_ETL_Phase_1_Report.md` through `docs/ASE_ETL_Phase_9_PowerBI_Report.md`, including Silver Phase 4 v2).
- **Current implementations:** `src/ingestion`, `src/silver`, `src/gold`, `src/analytics`, `src/api`, `src/orchestration`, `tests/`, `compose.yaml`, `powerbi/`.
- **Final user-provided CLI results:** 19 October 2026 Bronze/Silver/Gold verification, 22 analytics tests, local and MinIO Luigi task summaries, 13 CSV verification outputs, and FastAPI Uvicorn/Swagger status-code evidence.

*End of report.*
