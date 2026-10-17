"""Versioned, transactional API employee change ledger.

Run against ase_etl_test first, then ase_etl after verification.
Requires an existing api.employees table.
"""

from sqlalchemy import text
from src.api.database import engine


def migrate() -> None:
    with engine.begin() as connection:
        if connection.execute(text("SELECT to_regclass('api.employees')")).scalar_one() is None:
            raise RuntimeError("api.employees is missing; initialize Employee API storage first")

        connection.execute(text("""
            ALTER TABLE api.employees
            ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS api.employee_change_log (
                change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                client_employee_id VARCHAR NOT NULL,
                employee_revision BIGINT NOT NULL CHECK(employee_revision > 0),
                operation VARCHAR(10) NOT NULL
                    CHECK(operation IN ('create', 'update', 'delete')),
                changed_fields JSONB NOT NULL
                    CHECK(jsonb_typeof(changed_fields) = 'object'),
                actor_user_id BIGINT NULL REFERENCES api.users(user_id)
                    ON DELETE SET NULL,
                changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_api_employee_revision
                    UNIQUE(client_employee_id, employee_revision)
            )
        """))
        connection.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_employee_changes_id
            ON api.employee_change_log (client_employee_id, change_id)
        """))

        # Trigger assigns a strictly increasing per-employee revision for
        # meaningful business changes; status-only updates don't add events.
        connection.execute(text("""
            CREATE OR REPLACE FUNCTION api.employee_revision_before_write()
            RETURNS trigger LANGUAGE plpgsql AS $func$
            DECLARE
                old_business JSONB;
                new_business JSONB;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    IF NEW.sync_status = 'pending' THEN
                        NEW.revision := 1;
                    ELSE
                        NEW.revision := 0;
                    END IF;
                    RETURN NEW;
                END IF;

                IF NEW.client_employee_id IS DISTINCT FROM OLD.client_employee_id THEN
                    RAISE EXCEPTION 'Employee business identifier is immutable';
                END IF;

                old_business := to_jsonb(OLD)
                    - 'client_employee_id' - 'revision' - 'created_at'
                    - 'updated_at' - 'sync_status' - 'is_deleted';
                new_business := to_jsonb(NEW)
                    - 'client_employee_id' - 'revision' - 'created_at'
                    - 'updated_at' - 'sync_status' - 'is_deleted';

                IF old_business IS DISTINCT FROM new_business
                   OR OLD.is_deleted IS DISTINCT FROM NEW.is_deleted THEN
                    NEW.revision := OLD.revision + 1;
                    NEW.sync_status := 'pending';
                    NEW.updated_at := NOW();
                ELSE
                    NEW.revision := OLD.revision;
                    -- Prevent accidental marking of unchanged rows pending.
                    IF OLD.sync_status = 'synced' AND NEW.sync_status = 'pending' THEN
                        NEW.sync_status := OLD.sync_status;
                    END IF;
                END IF;
                RETURN NEW;
            END
            $func$
        """))

        connection.execute(text("""
            CREATE OR REPLACE FUNCTION api.employee_write_audit()
            RETURNS trigger LANGUAGE plpgsql AS $func$
            DECLARE
                previous_business JSONB;
                current_business JSONB;
                patch JSONB;
                operation_name VARCHAR(10);
                actor_id BIGINT;
            BEGIN
                actor_id := NULLIF(current_setting('api.actor_user_id', TRUE), '')::BIGINT;
                current_business := to_jsonb(NEW)
                    - 'client_employee_id' - 'revision' - 'created_at'
                    - 'updated_at' - 'sync_status' - 'is_deleted';

                IF TG_OP = 'INSERT' THEN
                    IF NEW.revision = 0 THEN
                        RETURN NEW; -- Existing Silver bootstrap, not an API edit.
                    END IF;
                    operation_name := 'create';
                    patch := current_business;
                ELSE
                    IF NEW.revision = OLD.revision THEN
                        RETURN NEW; -- No meaningful business change.
                    END IF;
                    IF NEW.is_deleted AND NOT OLD.is_deleted THEN
                        operation_name := 'delete';
                        patch := '{"is_deleted": true}'::JSONB;
                    ELSE
                        operation_name := 'update';
                        previous_business := to_jsonb(OLD)
                            - 'client_employee_id' - 'revision' - 'created_at'
                            - 'updated_at' - 'sync_status' - 'is_deleted';
                        SELECT COALESCE(jsonb_object_agg(n.key, n.value), '{}'::JSONB)
                        INTO patch
                        FROM jsonb_each(current_business) n
                        WHERE n.value IS DISTINCT FROM previous_business -> n.key;
                    END IF;
                END IF;

                INSERT INTO api.employee_change_log (
                    client_employee_id, employee_revision, operation,
                    changed_fields, actor_user_id
                ) VALUES (
                    NEW.client_employee_id, NEW.revision, operation_name,
                    patch, actor_id
                );
                RETURN NEW;
            END
            $func$
        """))

        connection.execute(text("""
            DROP TRIGGER IF EXISTS trg_api_employee_revision ON api.employees
        """))
        connection.execute(text("""
            CREATE TRIGGER trg_api_employee_revision
            BEFORE INSERT OR UPDATE ON api.employees
            FOR EACH ROW EXECUTE FUNCTION api.employee_revision_before_write()
        """))
        connection.execute(text("""
            DROP TRIGGER IF EXISTS trg_api_employee_audit ON api.employees
        """))
        connection.execute(text("""
            CREATE TRIGGER trg_api_employee_audit
            AFTER INSERT OR UPDATE ON api.employees
            FOR EACH ROW EXECUTE FUNCTION api.employee_write_audit()
        """))

        count = connection.execute(text("""
            SELECT COUNT(*) FROM api.employee_change_log
        """)).scalar_one()
    print("PASS: Employee change ledger and triggers verified")
    print(f"Existing change events: {count}")


if __name__ == '__main__':
    migrate()
