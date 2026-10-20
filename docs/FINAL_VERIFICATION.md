# ASE-ETL | Final Verification and Evidence Register

**Final check date:** 19 October 2026 (user-executed Windows/PowerShell session).  
**Evidence scope:** Terminal outputs and Swagger/Uvicorn logs provided in the project conversation, plus earlier phase documentation. These are recorded observations; this document does not claim a second independently rerun environment.

## 1. Environment and data publication

| Check | Observed result | Evidence |
|---|---|---|
| `docker compose ps` | `ase_postgres` up and healthy; `ase_minio` running | Windows Docker Compose output |
| Bronze local Parquet | 5 source extracts, 11 parts, **412,621 rows** | `python -m tests.verify_bronze_parquet` |
| Bronze publication | 5 files, **412,621 verified rows**, PASS | `python -m tests.verify_bronze_publication` |
| Duplicate baseline | **412,571** timesheet occurrences; **30,839** duplicated records; **381,732** unique | `python -m tests.verify_silver_design` |
| Gold MinIO publication | `dim_employee`: 10,794; `dim_date`: 183; `fact_timesheet`: 381,732; PASS | `python -m tests.verify_gold_minio` |
| Analytics unit/integration/supplementary | **22 passed** | `python -m pytest tests/test_analytics_formulas.py tests/test_analytics_integration.py tests/test_analytics_supplementary.py -v` |

The Silver design verifier was run twice with the same baseline classification/masking results. These were primarily design/profiling observations; they are not a substitute for running every Silver fault-injection test.

## 2. Luigi integrated reruns

| Input mode | Run ID | Outcome |
|---|---|---|
| Local raw CSV | `20261019T173314Z_f6269e64` | 9 Luigi tasks successful; no failed tasks/dependencies; original source snapshot unchanged |
| MinIO raw CSV | `20261019T173515Z_475f7000` | 9 Luigi tasks successful; no failed tasks/dependencies; original source snapshot unchanged |

Both logs ended with `ASE-ETL Luigi workflow completed successfully.` and indicated the source snapshot was pinned and verified after execution. The Luigi stages were Bronze ingestion, Silver transformation, Silver publication, Gold modeling, Gold publication, employee-sync acknowledgment, SQL analytics, report generation and final wrapper.

**Interpretation:** These results demonstrate both supported source modes against the existing populated environment and that the complete dependency chain ran. They do **not** independently constitute a fresh-install MinIO upload test or an exhaustive concurrent outage recovery drill; the Phase 8 report describes separate earlier reliability experiments.

## 3. Generated report integrity

The user executed `python -m tests.verify_analytics_reports` after the two pipeline reruns and all **13** exports passed count/hash/header verification. Observed CSV data rows:

| CSV prefix (filename `.csv`) | Rows |
|---|---:|
| `01_monthly_headcount_turnover` | 7 |
| `02_department_tenure` | 38 |
| `03_employee_working_hours` | 10,773 |
| `04_daily_attendance` | 379,480 |
| `05_daily_overtime` | 379,480 |
| `06_rolling_working_hours` | 379,480 |
| `07_early_attrition` | 1 |
| `08_monthly_workload` | 7 |
| `09_department_workload` | 512 |
| `10_attendance_compliance` | 168 |
| `11_department_overtime` | 512 |
| `12_weekday_workload` | 7 |
| `13_workforce_changes` | 7 |

A completed report folder also contains a manifest with SHA-256 and row counts. **No claim is made here that Power BI imported the October 19 folder.** The confirmed PBIX import snapshot is from October 18 (`20261018T114042Z_4ed531be`).

## 4. API and authorization checks

**Observed final manual Uvicorn status codes:**

| Scenario | HTTP status | Interpretation |
|---|---:|---|
| Start API and browse `/docs`, `/openapi.json` | 200 | Documentation served |
| Admin login `POST /api/v1/auth/token` | 200 | Credential authentication successful |
| Admin `/api/v1/auth/me`, `/admin-check`, `/health/db` | 200 each | Active identity, admin role and DB reachable |
| Admin employee and timesheet GET | 200 | Read endpoints work |
| Viewer login and `/me` | 200 | Viewer identity authenticated |
| Viewer employee and timesheet GET | 200 | Permitted reads work |
| Viewer `/api/v1/auth/admin-check` | **403** | Admin-only route denied |
| Viewer `POST /api/v1/employees` | **403** | Employee creation denied |
| Viewer `DELETE /api/v1/employees/123` | **403** | Employee deletion denied |

The Swagger response for rejected viewer creation was `{"detail":"Insufficient permissions"}`, an **expected security pass**. The user said CRUD had previously been exercised repeatedly and elected not to perform another destructive-write demonstration during the final session. The Phase 7 report independently records **44 passed API tests** in `ase_etl_test`, including actual CRUD, sync, token and authorization integration tests, plus live development CREATE/UPDATE/DELETE publication exercises. This is **earlier test evidence**, not an October 19 repeat of those 44 tests.

## 5. Power BI verification boundary

Phase 9 documentation records three saved Power BI pages, working month/department/employee interactions, resolution of two Power Query conversion issues, and visually/externally checked key KPI cards (16,386 late arrivals, 36,692 early departures, 41,668 overtime events, 9.29 eligible hours per eligible workday). The final `.pbix` exists at `powerbi/ASE_Workforce_Intelligence.pbix` on the reviewed GitHub branch.

**No new October 19 PBIX refresh or automatic table-by-table reconciliation was carried out.** This is acceptable for a fixed validated assignment snapshot if stated transparently. Visual-design open items noted in Phase 9 remain presentation checks rather than silently claimed corrections: department minimum-sample threshold, tenure card's unit label and optional directly visible average-hours-per-employee summary.

## 6. Requirements traceability and sign-off

| Assignment requirement | Evidence position | Assessment |
|---|---|---|
| Source ingest from local and MinIO | October 19 integrated runs, Bronze checks | Verified on populated environment |
| Transform, clean and post-process | Phase 4 Silver contract, quality/dedup audit, Gold checks | Implemented, core observed outputs verified |
| Orchestration and failure/rerun model | Nine-stage Luigi runs and Phase 8 recovery tests | Verified within documented test boundaries |
| SQL insight coverage | Phase 6 SQL inventory, 22 final passing tests | Verified |
| Employee CRUD and read-only timesheets | Phase 7 regression + final endpoint reads/permissions | Verified; CRUD final manual rerun omitted |
| Authentication and authorization | Final admin/viewer status codes, Phase 7 security tests | Verified |
| Visualization | Saved PBIX, Phase 9 reviewed screenshots | Implemented, fixed-snapshot provenance disclosed |
| Engineering/setup/usage/schema docs | Documentation package | Included |

### Remaining submission actions (not code failures)

1. Copy the documentation files to the repository, confirm Word opens and the Markdown Mermaid ER diagram previews as expected.
2. Add/update root `README.md` and safe `.env.example`, reviewing for any repository-specific environment changes before committing.
3. Review Git changes for `.env`, personal/source data and generated outputs; commit and push the intentional deliverables.
4. If presenting the Power BI dashboard, show the fixed validated snapshot date and the limitations of 50 roster profiles and unresolved pay codes.

**Sign-off:** The requirements are functionally covered to the level supported by the available code, previous phase tests and final-session output. This is a development/assignment validation, **not a production compliance or security certification**.
