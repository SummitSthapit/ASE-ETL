# ASE-ETL | Operations and Usage Guide

This runbook describes the **supported normal workflow** on a configured environment. For first-time dependencies and credentials, read `SETUP_GUIDE.md` before running any commands.

## 1. Service startup and normal run

```powershell
cd D:\ASE-ETL
.\.venv\Scripts\Activate.ps1
docker compose up -d
docker compose ps
```

Keep a scheduler terminal open:

```powershell
luigid --address 127.0.0.1 --port 8082
```

Launch from a separate terminal. Choose exactly one input source per run:

```powershell
# Raw CSV from data/raw/
python -m src.orchestration.pipeline --source local

# Raw CSV from MinIO SOURCE_BUCKET/SOURCE_PREFIX
python -m src.orchestration.pipeline --source minio
```

The command prints a generated `run_id` (e.g. `20261019T173314Z_f6269e64`). Record it in operational notes. The chain completes only after all nine Luigi tasks finish successfully, the source is rechecked, and the final report-completion marker exists.

**Task sequence:** Bronze ingestion → Silver transformation → Silver publication → Gold modeling → Gold publication → API sync acknowledgment → SQL analytics → report generation → Luigi root wrapper. The source mode selects **Bronze raw input**; Silver still consumes staged **local Bronze Parquet**.

## 2. Understand the data lifecycle

| Layer or component | Primary source | Key outputs |
|---|---|---|
| Bronze | Local or MinIO raw CSV | `bronze.*_raw`, Bronze registry, local and MinIO Parquet |
| Silver | Local Bronze Parquet + API revision overlay | `silver.employees`, `silver.timesheets`, quality/audit Parquet, active publication |
| Gold | Registered Silver PostgreSQL | `gold.dim_employee`, `gold.dim_date`, `gold.fact_timesheet`; Gold release and Parquet |
| SQL analytics | Gold warehouse | `mart.*` classification, KPI, attendance/workforce/workload views |
| Reporting | SQL views | `reports/analytics/<timestamp_id>/` with 13 CSV files and `manifest.json` |
| Power BI | A selected valid reporting folder | Three report pages in `powerbi/ASE_Workforce_Intelligence.pbix` |

Input files and employee API edits do not immediately or directly mutate analytical fact tables. **A new controlled run** advances the published warehouse state.

## 3. Routine integrity checks

```powershell
python -m tests.verify_bronze_parquet
python -m tests.verify_bronze_publication
python -m tests.verify_silver_design
python -m tests.verify_gold_minio
python -m tests.verify_analytics_reports
```

The first two confirm Bronze part counts/publication integrity; `verify_silver_design` primarily reproduces source profiling and duplicate/masking analysis; the Gold and report verifiers check publication/export integrity. The historical Phase 4 reports also identify dedicated Silver unit and integration tests if deeper transformation regression is required.

Run the SQL formula/integration tests:

```powershell
python -m pytest tests/test_analytics_formulas.py tests/test_analytics_integration.py tests/test_analytics_supplementary.py -v
```

The October 19 verification had **22 passes**; do not assert that a future run passed until you inspect its output. For other tests and isolated mutation-based scenarios see `tests/` and the corresponding phase reports.

## 4. API use, administration and permissions

Start API in a terminal while database services remain up:

```powershell
uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

Browse `http://127.0.0.1:8000/docs`. Swagger UI is the intended interactive API testing/documentation interface, **not a missing custom frontend**. Sign in with the **Authorize** control. `POST /api/v1/auth/token` accepts form username/password and issues a bearer JWT. Never paste access tokens into screenshots or repository files.

| Operation | Admin | Viewer | Unauthenticated |
|---|---|---|---|
| `GET /api/v1/employees` and `/{employee_id}` | Allow | Allow | Deny |
| `POST /api/v1/employees` | Allow | 403 | Deny |
| `PATCH /api/v1/employees/{employee_id}` | Allow | 403 | Deny |
| `DELETE /api/v1/employees/{employee_id}` | Allow (soft delete) | 403 | Deny |
| `GET /api/v1/timesheets` | Allow | Allow | Deny |
| `GET /api/v1/employees/{employee_id}/timesheets` | Allow | Allow | Deny |
| `GET /api/v1/auth/admin-check`, `/health/db` | Allow | 403 | Deny |

Use `?employee_id=...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&limit=25&offset=0` with the general timesheet endpoint, or its employee-specific route. Date ranges are inclusive at the calendar-date level. Limits must be 1–100. Typical statuses: `200` successful read/update, `201` created, `204` deleted, `401` missing/invalid token, `403` forbidden role, `404` absent employee, `409` duplicate/conflict and `422` input validation error. A viewer's `403 Insufficient permissions` is **correct** behavior.

**Provisioning:** `python -m src.api.bootstrap_admin` creates the first administrator only when no users exist; `python -m src.api.create_user --username viewer1 --role viewer` creates additional accounts. Both prompt for passwords. Reuse existing accounts instead of recreating them. Passwords must be 14–128 characters long and tokens expire after approximately 15 minutes.

## 5. Operational employee writes and synchronization

Use a **unique synthetic ID**, never an existing business employee, for a demonstration. The Swagger POST example contains placeholder strings for every optional field; review its schema and enter meaningful test values with correctly ordered dates.

1. Authenticate as `admin`; create a synthetic employee through `POST /api/v1/employees`. Record its `client_employee_id`, status and `sync_status` (usually `pending` initially).
2. GET the record, PATCH a permitted field, then GET it again to confirm the operational API state.
3. Launch a **new** Luigi run; it applies the committed employee revision through a Silver overlay, publishes Gold, then conditionally acknowledges only the matching revision. Check for a synced state and the expected Gold profile presence.
4. If demonstrating deletion, use `DELETE` on the synthetic employee, then launch another **new** run and verify that the profile is absent from the current published Silver/Gold profile representation. The operational deletion is **soft**: the API ledger/tombstone remain. A Gold identity may still be retained if referenced by historical timesheets.

Running create/update/delete against actual source employees is **not necessary** for demonstration. Do not run mutation-oriented API regression tests with `DATABASE_URL` pointing to the populated development warehouse; the documented API suite requires its **isolated `ase_etl_test`** database.

## 6. Report exports and Power BI

A successful orchestrated run automatically performs SQL preparation followed by report generation. You may also invoke the report generator as a separate operation after confirming a current Gold publication:

```powershell
python -m src.analytics.pipeline
python -m src.analytics.reporting
python -m tests.verify_analytics_reports
```

Each successful batch lives under a timestamped directory containing 13 CSV files plus `manifest.json`, with SHA-256 digests and row counts. **Do not mix reports from different batch folders** in a single Power BI model. The 13 file names and reference row counts are in `FINAL_VERIFICATION.md`.

Open `powerbi/ASE_Workforce_Intelligence.pbix`. The existing report imports a validated **October 18** folder and does **not** automatically switch to the most recent run. For a deliberate refresh to a later folder, open **Transform Data / Power Query**, update every CSV source to **one newly verified folder**, recheck types (especially IDs/text, dates, decimal/null fields), **Close & Apply**, validate slicers/cards, and save. The assignment accepts the retained validated snapshot; updating Power BI after every identical raw-source rerun is optional, but its source date must be disclosed.

## 7. Reruns, failures and safe recovery

A normal **new** run ID represents new work. For an **interrupted** run, reuse its original ID to allow Luigi to reuse completed targets:

```powershell
python -m src.orchestration.pipeline --source local --run-id <original-run-id>
```

This is safe only when the pinned input bytes and relevant intermediate state remain compatible. The launcher checks source fingerprints and refuses concurrent supported launches. If a run has already been fully completed, reusing that ID may skip all processing even if new inputs or employee revisions later appeared. **Use a new run ID for new changes.**

Troubleshooting sequence: inspect PowerShell failure output → Luigi UI at port 8082 → `data/orchestration/logs/` for the stage → `data/orchestration/state/` for task markers → Silver/Gold publication registries → report batch manifest and verifier. Do not delete completion markers, publication rows or MinIO objects casually: that can make state inconsistent. A manually invoked Silver/Gold publisher outside the supported launcher may bypass workflow-level serialization.

## 8. Security and clean handoff

Keep `.env`, passwords, JWTs, raw personal data, generated reports, Parquet snapshots and development caches out of public commits unless you have explicit data-sharing approval. `data/raw/`, generated `data/` outputs and `/reports/analytics/` are ignored in `.gitignore`. Review `git status`, then stage **selected source/docs files**, not blindly generated data. Preserve `powerbi/ASE_Workforce_Intelligence.pbix` as the deliverable BI model; the installed Power BI Desktop environment is external to the repository.
