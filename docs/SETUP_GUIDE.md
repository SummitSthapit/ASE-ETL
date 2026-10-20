# ASE-ETL | Setup and Configuration Guide

**Target environment:** Windows / PowerShell, Python 3.12-compatible virtual environment, Docker Desktop (Linux containers), PostgreSQL 16, MinIO and Power BI Desktop. Run commands from the repository root; existing project evidence used `D:\ASE-ETL`.

## 1. Prerequisites

- Git, Python 3.12 (the final test logs used Python 3.12.4), Docker Desktop with its engine started, and Power BI Desktop for the `.pbix` report.
- A copy of the five supplied CSV extracts, or equivalent files with the approved schema. **The repository ignores `data/raw/`**: source files must be provided separately.
- Credentials for a local **PostgreSQL development database** and a locally exposed MinIO instance. Do not use production credentials or publish the source employee/timesheet data unintentionally.
- Enough local storage for raw CSV, staging Parquet, PostgreSQL named volume, MinIO named volume and reports.

## 2. Clone and install

```powershell
# Example location; choose your own folder if needed.
git clone https://github.com/SummitSthapit/workforce-analytics-etl
cd ASE-ETL
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Use `python -m` from the **project root**. Run commands in a terminal where the virtual environment is active. Exact compatibility is governed by the checked-in `requirements.txt`, not by an assumed global Python installation.

## 3. Create the private `.env`

Copy the included `.env.example` to `.env`; replace all placeholder secrets. The `.env` file is ignored by Git. Avoid spaces around the `=` assignment and never paste real secrets into documentation or screenshots.

```powershell
Copy-Item .env.example .env
```

| Variable | Purpose / typical local value |
|---|---|
| `POSTGRES_PASSWORD` | PostgreSQL container password, used by `compose.yaml` |
| `DATABASE_URL` | SQLAlchemy URL, e.g. `postgresql+psycopg://ase_user:<URL-encoded-password>@localhost:5433/ase_etl` |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO credentials, same ones Compose uses |
| `MINIO_ENDPOINT` | `localhost:9000` (MinIO API, **not** web console port) |
| `MINIO_SECURE` | `false` on local HTTP MinIO; use TLS appropriately outside localhost |
| `SOURCE_BUCKET`, `SOURCE_PREFIX` | Raw input bucket/prefix, default `ase-source` / `raw/` |
| `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET` | Published stage buckets (defaults `ase-bronze`, `ase-silver`, `ase-gold`) |
| `INGEST_SOURCE` | Optional default source mode (`local` or `minio`); CLI `--source` overrides it |
| `API_JWT_SECRET` | **64 cryptographically random hexadecimal characters**, required when starting FastAPI |

Generate a random JWT secret locally (copy the output into `.env`, not into Git):

```powershell
python -c "import secrets; print(secrets.token_hex(32))"
```

Additional optional bucket/path overrides may exist in individual modules; the code is the authority for defaults. A URL-encode-sensitive password in `DATABASE_URL` is safer than relying on raw special characters.

## 4. Start infrastructure

```powershell
docker compose up -d
docker compose ps
```

Expected services in this repository: `ase_postgres` on host **5433 → 5432** and `ase_minio` on **9000 (S3 API)** and **9001 (web console)**. Verify that PostgreSQL is healthy. The MinIO console is `http://localhost:9001` and requires the configured credentials.

**Avoid `docker compose down -v`** unless you intend to delete the named PostgreSQL/MinIO data volumes. An ordinary `docker compose down` stops services without removing these volumes.

## 5. Provide the source CSVs

For the local source mode, put the five original files directly in `data/raw/`:

```text
employee_202510161125.csv
timesheet_202509151540.csv
timesheet_202510161121.csv
timesheet_202510161122.csv
timesheet_202510161124.csv
```

For the MinIO source mode, create/confirm the `ase-source` bucket in the MinIO console and upload the **same byte-identical files** beneath the `raw/` object prefix (e.g. `raw/employee_202510161125.csv`). Do not upload Parquet in place of raw CSVs. In the project verification, file hashes matched between local and MinIO inputs. Uploading these files is a prerequisite for a **fresh** MinIO-only environment; a previously tested dataset may already exist in the named MinIO volume.

## 6. First-time database initialization

The tested October 19 runs used a **previously prepared database**. A truly empty database has a bootstrap ordering constraint: current Silver transformation reads `api.employee_change_log`, whereas `src.api.migrations` normally seeds operational employees from **already published Silver**. Therefore, simply running the full Luigi launcher on untouched volumes is **not** a verified zero-configuration path.

For a fresh local development instance, this package provides **`docs/bootstrap_api_empty.sql`**, a small one-time table bridge derived from the checked-in `src/api/migrations.py`. It creates an **empty** API employee table without requiring Silver to exist, allowing subsequent API ledger initialization *before* the Silver pipeline. This fresh-volume procedure is **code-inspected but not independently end-to-end tested**; review it against your checkout and test it on an isolated empty volume before relying on it for a formal cold-start demonstration.

```powershell
# 1. Ingest source CSVs into Bronze.
python -m src.ingestion.bronze_ingestion --source local

# 2. Empty-db bridge. Only for a truly fresh DB; run it from repo root.
Get-Content .\docs\bootstrap_api_empty.sql | docker compose exec -T postgres psql -U ase_user -d ase_etl -v ON_ERROR_STOP=1

# 3. Initialize user identity, login audit, change ledger/revision triggers.
python -m src.api.auth_migrations
python -m src.api.security_migrations
python -m src.api.employee_change_migrations

# 4. Produce the first clean Silver publication.
python -m src.silver.pipeline
python -m src.silver.publish_idempotent

# 5. Seed API operational employee profiles from published Silver.
python -m src.api.migrations

# 6. Publish the initial Gold model and verify remote integrity.
python -m src.gold.pipeline
python -m src.gold.release
python -m tests.verify_gold_minio
```

**For an existing populated `ase_etl` database**, skip this empty-database bootstrap sequence; do not attempt to re-seed or reset existing snapshots. The repository includes additional API migration/compatibility utilities and tests under `src/api/`; confirm your installed schema against the current code before changing a populated instance. Scripts with names such as `apply_reliability_migrations.py` are development source patches, **not** general database initialization commands.

Initialize the first admin **only if `api.users` is empty**:

```powershell
python -m src.api.bootstrap_admin
```

When prompted, enter a unique username and a password 14–128 characters long. An existing populated `api.users` table intentionally blocks repeated first-admin bootstrap. To add a viewer or additional admin later:

```powershell
python -m src.api.create_user --username viewer1 --role viewer
python -m src.api.create_user --username admin2 --role admin
```

Account provisioning is a **trusted local CLI**, not a public web registration endpoint. The bootstrap SQL is intentionally small and should not be mistaken for an implementation of database-wide migration automation.

## 7. Launch the integrated pipeline

Start Luigi's scheduler in a dedicated terminal:

```powershell
luigid --address 127.0.0.1 --port 8082
```

Open `http://127.0.0.1:8082` to inspect task execution. In another activated terminal, start **one** new complete run:

```powershell
python -m src.orchestration.pipeline --source local
# Or, after confirming the MinIO raw objects exist:
python -m src.orchestration.pipeline --source minio
```

The orchestration will validate the current source snapshot, execute the Bronze → Silver → Gold → API acknowledgment → SQL → reports chain, and check completion. It uses an advisory lock to reject overlapping **supported launcher** invocations.

## 8. Verify installation

```powershell
python -m tests.verify_bronze_parquet
python -m tests.verify_bronze_publication
python -m tests.verify_silver_design
python -m tests.verify_gold_minio
python -m tests.verify_analytics_reports
```

To run the isolated API integration regression tests, point `DATABASE_URL` to the dedicated, correctly initialized **`ase_etl_test`** database first and follow Phase 7's test setup: `python -m pytest tests/api/ -v`. **Never run mutation-capable API integration tests against the production-like `ase_etl` dataset casually.**

## 9. FastAPI and Power BI

```powershell
uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

- Swagger documentation: `http://127.0.0.1:8000/docs`.
- Health endpoint: `http://127.0.0.1:8000/health`.
- Use the existing admin/viewer credentials with **Authorize**; protected database health is admin-only.
- Open `powerbi/ASE_Workforce_Intelligence.pbix` in Power BI Desktop. Its configured Power Query paths are bound to a specific prior report folder; before choosing a different run, update each source path and validate import types. A refresh alone does not discover newly timestamped folders.

## 10. Common setup pitfalls

| Symptom | Action |
|---|---|
| Docker CLI cannot contact engine on Windows | Open Docker Desktop and wait for engine startup; then retry Compose |
| Database connection refused on port 5432 | On the Windows host the provided Compose maps Postgres to **5433** |
| MinIO web console confused with S3 endpoint | Console is port **9001**; application client endpoint is **9000** |
| API immediately rejects JWT-secret format | Set `API_JWT_SECRET` to 64 secure hexadecimal characters |
| `403 Insufficient permissions` on viewer POST/DELETE | Expected role-based denial; sign in as admin for mutations |
| Luigi cannot find scheduler | Start `luigid` on `127.0.0.1:8082` in another terminal |
| MinIO mode has no input | Confirm `SOURCE_BUCKET`/`SOURCE_PREFIX` and uploaded CSV objects |
| Python cannot import `src` | Run from repository root using the activated project virtual environment |
| Power BI still shows previous export | Verify Power Query paths; the `.pbix` uses a fixed snapshot |

See `USAGE_GUIDE.md` for normal runs, recovery, API and report validation; `DATABASE_SCHEMA.md` for entity relationships.
