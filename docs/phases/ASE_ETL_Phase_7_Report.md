# ASE-ETL — Phase 7 Implementation Report

**FastAPI, Authentication, Employee CRUD, Timesheet Read API, and Controlled Warehouse Synchronization**  
**Project:** ASE-ETL  
**Status:** Functionally complete; release and orchestration hardening outstanding  
**Evidence date:** 18 October 2026  
**Application:** Python / FastAPI / SQLAlchemy / PostgreSQL / MinIO

## 1. Executive summary

Phase 7 implements an authenticated REST API for employee maintenance and timesheet retrieval over the existing Bronze–Silver–Gold data platform. Employee writes are managed in a dedicated operational PostgreSQL schema (`api`), rather than altering published `silver` or `gold` tables directly. A versioned change ledger captures creates, updates, and soft deletes; a deterministic Silver overlay replays those changes into a publishable snapshot; Gold then rebuilds its dimensional model from the published Silver state. The controlled release entry point acknowledges only the exact employee revisions that are covered by a matching, published Gold release.

Verification comprises **44 passing consolidated API tests** (one nonblocking Starlette/httpx deprecation warning) and live development-environment CREATE, UPDATE, unchanged rerun, and DELETE publication exercises across PostgreSQL and MinIO. The final read-only check showed 50 Silver profiles, 10,794 Gold identities, 381,732 Gold timesheet facts, matching Silver/Gold source fingerprints, a PUBLISHED Gold registry, and preserved operational change history. The development test employee was excluded from current Silver/Gold tables after deletion.

This report distinguishes functional completion from production hardening. A single active publisher is currently assumed; distributed mutual exclusion, stronger publication provenance checks, deployment automation, and further operational hardening remain follow-up work.

## 2. Scope and assignment traceability

The source assignment, *Assignment: ETL to Insights*, requests employee CRUD, read-only access to employee timesheets and filtered timesheet listings, authentication and authorization, configurable/modular code, logging, and appropriate error handling. Phase 7 implements the API portion, integrated with earlier Bronze, Silver, Gold, SQL analytics, and data mart work. Luigi orchestration and final Power BI visualization are later project phases, not completed by this report.

| Requirement | Implemented in Phase 7 | Evidence |
|---|---|---|
| Employee create/read/update/delete | API operational employee routes with soft deletion | CRUD integration tests; live CREATE/UPDATE/DELETE |
| Read-only timesheets | General and per-employee Gold fact queries, date/employee filtering, pagination | API integration tests |
| Authentication and authorization | JWT access tokens, role checks and DB-backed account state | Auth and security tests |
| Modular and validated API | Routers, Pydantic schemas, migrations, reusable DB/security dependencies | Implementation and 44-test regression |
| Data consistency after employee changes | Change ledger, Silver overlay, versioned Silver/Gold publication and acknowledgment | Stage/Gold/recovery tests plus live publication |
| Orchestration | Not in Phase 7 | Planned Phase 8: Luigi |
| Business visualizations | Not in Phase 7 | Planned Phase 9: Power BI |

## 3. Architecture and data ownership

### 3.1 Why a separate operational API schema

Direct REST writes to `silver.employees` or `gold.dim_employee` would bypass existing content-addressed MinIO snapshots, PostgreSQL publication registries, and Gold source-fingerprint checks. The API therefore owns `api.employees`, an operational projection for writable employee attributes. Published analytics remain derived from controlled data-platform runs.

### 3.2 Data movement

1. Administrator performs employee CREATE, PATCH, or soft DELETE using FastAPI.
2. PostgreSQL commits the operational change and trigger-generated versioned event in `api.employee_change_log`.
3. Silver pipeline reads Bronze source records, transforms and validates them, and applies a deterministic replay of committed employee overrides.
4. Silver stages employee/timesheet/audit Parquet datasets. `audit/api_employee_overrides` records the included employee revision and digest.
5. Silver publisher verifies local data, uploads and checks immutable MinIO objects, and transactionally publishes PostgreSQL `silver` tables and the active Silver fingerprint.
6. Gold pipeline builds dimensions/facts from the active Silver publication while retaining surviving surrogate keys and validating constraints, attributes, dates, and fact relationships.
7. Gold publisher exports and verifies immutable MinIO Parquet and finalizes its PUBLISHED registry only for the matching Silver source fingerprint.
8. Synchronization acknowledgment updates an employee to `synced` only if its current revision still equals the revision in the published Silver audit. Newer revisions remain pending.

**Logical flow:** `FastAPI → api.employees + change_log → Silver overlay → Silver PostgreSQL + MinIO → Gold PostgreSQL + MinIO → revision acknowledgment`.

### 3.3 Existing warehouse grain

`gold.dim_employee` stores distinct identities present in either Silver employee profiles or timesheets; some identities have no employee profile. `gold.dim_date` stores calendar dates; `gold.fact_timesheet` stores timesheet business events connected to employees and dates. This explains why the baseline has 50 current Silver profiles but 10,794 Gold employee identities. Soft deletion of a profiled employee does not eliminate a timesheet-backed identity or its historical facts.

## 4. API endpoints and behavior

| Method and path | Role / access | Function |
|---|---|---|
| `POST /api/v1/auth/token` | Public login | Authenticate credentials and issue signed JWT |
| `GET /api/v1/auth/me` | Authenticated | Inspect current active account |
| `GET /api/v1/auth/admin-check` | Admin | Authorization probe |
| `GET /health` | Public | Application health |
| `GET /health/db` | Admin | Authenticated database health |
| `GET /api/v1/employees` | Authenticated | List employee records with pagination |
| `GET /api/v1/employees/{employee_id}` | Authenticated | Retrieve employee record |
| `POST /api/v1/employees` | Admin | Create operational employee |
| `PATCH /api/v1/employees/{employee_id}` | Admin | Update allowed employee attributes |
| `DELETE /api/v1/employees/{employee_id}` | Admin | Soft-delete operational employee |
| `GET /api/v1/timesheets` | Authenticated | Read-only Gold timesheets; employee/date filters and pagination |
| `GET /api/v1/employees/{employee_id}/timesheets` | Authenticated | Read-only Gold timesheets for one employee |

The DELETE endpoint is a soft delete in operational storage, even though the user-facing API performs the requested logical employee deletion. Gold dimension cleanup depends on surviving timesheet identity references. Timesheet endpoints do not permit writes. Their data is sourced from the published Gold model, and therefore reflects changes only after synchronization publication.

The implementation rejects missing/invalid tokens (401), forbids unauthorized administrative actions (403), validates malformed requests (422), and rejects duplicate employee IDs or conflicts with existing Gold-only employee identities (409), as exercised by API tests. Exact optional filters and response schemas should be checked against the installed OpenAPI specification during final deployment review.

## 5. Security implementation

- Password hashing uses Argon2id via `pwdlib`; the database stores password hashes rather than plaintext credentials.
- JWT access tokens use HS256 with configured signing secret, issuer, audience, issued-at, not-before, expiry, unique token identifier, subject, and `token_version` claims. Access-token validity is short-lived (15 minutes).
- Authorization checks live PostgreSQL account state, including `is_active`, role and `token_version`; role changes and token revocation are enforced on requests.
- Roles: `admin` can create/update/soft-delete employees and access privileged health checks; `viewer` can use permitted read endpoints.
- Login activity is audited (`api.auth_events`) and username-based failed-login throttling is tracked (`api.login_attempts`).
- Development bootstrap utilities create admin/viewer accounts. Keep JWT secrets, passwords, and full database URLs out of Git, documentation screenshots and logs.
- Automated tests verify missing/malformed/forged/expired/wrong-audience tokens, mismatched token version, role boundaries, disabled accounts, throttling, and database-health authorization.

## 6. Database objects and synchronization controls

### 6.1 API schema

- `api.employees`: primary key `client_employee_id`; writable identity/profile fields; `created_at`, `updated_at`; `is_deleted`; `sync_status` (`pending`, `synced`, `failed`); and version (`revision`) for synchronization safety.
- `api.employee_change_log`: append-only, per-employee numbered CREATE/UPDATE/DELETE events, with change identifiers, event operations and attributes necessary for deterministic replay.
- `api.users`: identity, unique username, Argon2id hash, role, active state, token version and timestamps.
- `api.auth_events` / `api.login_attempts`: authentication auditing and login throttling.

### 6.2 Silver overlay contract

The employee Silver schema retains all 34 source business attributes and lineage metadata. API attributes overlay matching source records without losing unrelated source fields. API-created identities are added when active; soft-deleted API identities are excluded from the current Silver employee profile output. The employee override audit records exactly the included revision and event digest; original Bronze schema and source-row audits remain independently reconcilable.

### 6.3 Publication and acknowledgment

Silver/Gold snapshots are fingerprinted and validated; Gold publishes only for a verified active Silver fingerprint. Acknowledgment checks that the active Silver fingerprint matches Gold's PUBLISHED source fingerprint, then marks only identical current employee revisions as synced. An interrupted Gold publication leaves the corresponding API changes pending, allowing retry. The audit ledger remains append-only, including for soft-deleted records.

### 6.4 Known concurrency constraint

The current release workflow assumes only one staging/publishing process at a time. The existing PostgreSQL advisory locks serialize portions of publication, but local Parquet staging is not protected by a distributed filesystem/run lock. Luigi orchestration must enforce one writer; any future multi-worker deployment needs a durable staging-run identifier, stronger transactional coordination and monitoring. Registry checks are necessary but not alone sufficient for arbitrary concurrent publishers.

## 7. Validation results

### 7.1 Consolidated API regression

Command (with `DATABASE_URL` pointing to `ase_etl_test`): `python -m pytest tests/api/ -v`.

| Test group | Passing tests |
|---|---:|
| Authentication integration | 8 |
| Employee CRUD and Timesheet integration | 7 |
| Employee change ledger | 3 |
| API-to-Silver replay | 2 |
| Gold employee-dimension behavior | 1 |
| Publication recovery and fact integrity | 3 |
| Publication revision/audit matching | 4 |
| Silver stage validation | 5 |
| JWT security and authorization | 11 |
| **Total** | **44** |

Run result: **44 passed, 1 warning in 9.71 seconds**. Warning: Starlette TestClient reports that `httpx` support is deprecated in favor of `httpx2`; this was non-blocking, and no dependency changes were made merely to suppress the warning.

Tests confirmed authenticated CRUD, role boundaries, replay semantics, deterministic staging, detection of stale/corrupted audit revisions, surrogate-key preservation, failed-publication nonacknowledgment, retry with revision matching, retention of historical facts, and read-only Timesheet API behavior. The recovery tests used simulated registry rows or injected failures; those do not independently constitute a live MinIO outage/recovery drill.

### 7.2 Live changed-data publication exercise

The synthetic employee `ASE_SYNC_E2E_TEST_001` was created through authenticated FastAPI and had no timesheet history.

| Exercise | Observed evidence |
|---|---|
| Before CREATE publication | API rev 1 pending; one CREATE event; employee absent from Silver/Gold |
| CREATE publication | Silver 51 employees, Gold 10,795 identities; MinIO upload verified; API rev 1 synced |
| UPDATE before publication | API rev 2 pending; Gold retained old department |
| UPDATE publication | Department set to Data Platform Engineering in Silver and Gold; Gold key **47014** unchanged; rev 2 synced |
| Unchanged rerun | Silver skipped PostgreSQL reload; Gold skipped reload/export; synchronized changes 0 |
| Soft DELETE before publication | API rev 3 pending; ledger contained CREATE, UPDATE, DELETE |
| DELETE publication | Silver 50; Gold 10,794; 1 stale Gold-only test identity removed; zero facts removed; rev 3 synced |
| Final read-only preflight | Matching active Silver/Gold fingerprints; PUBLISHED; original active warehouse row counts restored |

The final active Silver fingerprint was `d0be6863d311cb1f9c258b34c94704afbe225f3e73e4fd4bbbd5c70cc6072820`; Gold's registry referenced the same Silver snapshot. The operational table retained 51 synced rows because the synthetic test employee is soft-deleted, and the ledger retained three historical events. Current Silver and Gold contain no row for that synthetic identity. Historical immutable MinIO snapshots persist by design.

### 7.3 Final active dataset reconciliation

| Dataset | Final row count |
|---|---:|
| `silver.employees` | 50 |
| `silver.timesheets` | 381,732 |
| `gold.dim_employee` | 10,794 |
| `gold.dim_date` | 183 |
| `gold.fact_timesheet` | 381,732 |

## 8. Running and verifying Phase 7

**Prerequisites:** active Python virtual environment, Docker Compose PostgreSQL and MinIO services, environment file, provisioned database tables, initialized users, secure `API_JWT_SECRET`.

```powershell
# From D:\ASE-ETL
# API development server
uvicorn src.api.main:app --host 127.0.0.1 --port 8000

# API test suite — ONLY with DATABASE_URL pointing to ase_etl_test
python -m pytest tests/api/ -v

# Read-only development publication preflight
python -m tests.api.preflight_sync_publication

# Controlled real Silver -> Gold sync; modifies PostgreSQL + MinIO
# Verify connection is ase_etl; do not run concurrently with other publishers
python -m src.api.employee_sync_release
```

For protected endpoints, first obtain a token from `POST /api/v1/auth/token` and include `Authorization: Bearer <token>`; do not commit or print production tokens. The live release command is **not** a fixture/test command. Use only after confirming registry health, database identity and absence of competing publishers.

## 9. Engineering decisions and trade-offs

| Choice | Reason | Consequence |
|---|---|---|
| Separate writable API table | Protect immutable warehouse publication semantics | Data is eventually consistent with Gold |
| Soft deletion | Retain operational history and avoid unintended fact loss | Tombstone remains in API; current Silver profile may disappear |
| Event ledger and per-employee revision | Reproducible changes, retry and late-update protection | Additional migration and audit complexity |
| Deterministic Silver overlay | Retain all source fields and lineage | Must maintain schema/reconciliation safeguards |
| Content-addressed MinIO snapshots | Traceable immutable releases and verification | Historical objects accumulate; lifecycle policy needed |
| Gold surrogate-key-preserving upsert | Keep existing timesheet relationships stable | Gold profile and identity semantics must be distinguished |
| Single controlled sync entry point | One ordered development workflow | Needs Luigi serialization and operational monitoring |

## 10. Outstanding work and limitations

1. **Phase 8:** Luigi dependency graph, serialized staging/publication, retry policy, alerts, logging, idempotent task completion markers, and orchestration of Bronze → Silver → Gold → analytics/reporting with sync stage included.
2. **Phase 9:** Power BI dashboard with at least three actionable visualizations based on SQL KPI outputs.
3. **Operational security:** final secret management, deployment HTTPS/proxy settings, CORS policy, rate-limit observability, token retention/log redaction, and explicit production deployment review.
4. **Publication durability:** strengthen provenance binding between staged audit and database publication for concurrent writers; introduce run-specific staging directories or durable locks; validate MinIO failure scenarios in an isolated integration environment.
5. **Dependencies:** evaluate Starlette/httpx compatibility warning during planned upgrades; do not blindly install a replacement package.
6. **Repository hygiene:** remove generated `.pre_7_7_2` backups from tracked changes, exclude `.env`, JWT secrets, credentials, caches, virtual environments, test snapshots and transient Parquet artifacts; retain application code, migrations, tests and documentation.
7. **Source extract capability:** the original assignment asks for direct local-file and MinIO extraction. Prior phase implementation choices and their current completeness must be assessed independently; Phase 7 does not itself satisfy that separate ETL-source requirement.

## 11. GitHub review and commit procedure

GitHub is not guaranteed to match the latest local `D:\ASE-ETL` working tree, so run the commands below locally and inspect before committing. Do not perform `git add .` without reviewing ignored and untracked artifacts.

```powershell
git status --short
git diff --stat
git diff --check
git status --ignored --short

# Ensure environment secrets do NOT enter Git
# Inspect all staged changes before committing
git add src/api src/silver/pipeline.py src/silver/publish.py tests/api docs/ASE_ETL_Phase_7_Report.md
git diff --cached --stat
git diff --cached --check
git diff --cached

# Only after inspecting the staged patch
git commit -m "feat(api): complete employee CRUD and versioned warehouse sync"
git push origin main
```

The `git add` example assumes your planned file tree and that this report is saved under `docs/`; adapt paths if files differ. Add `requirements.txt` and `.gitignore` only after auditing their changes. Exclude `src/silver/*.pre_7_7_2`, local generated MinIO exports, build artifacts, secrets, and one-off installers unless intentionally retained as source-controlled tools. Do not discard backups until code verification and Git review are complete.

## 12. Completion statement

Phase 7 is functionally complete for the assignment's employee CRUD, read-only Timesheet API, and authenticated access requirements. The controlled API→Silver→Gold synchronization sequence has passed a live changed-data CREATE/UPDATE/DELETE and idempotency exercise, with matching published fingerprints and restored active warehouse counts. Forty-four automated API tests passed in one consolidated run. Luigi orchestration and Power BI remain distinct upcoming deliverables, and the single-publisher assumption remains a documented operational limitation.
