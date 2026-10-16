
"""Database migration for API authentication."""

from sqlalchemy import text

from src.api.database import engine


def initialize_auth_database():
    """Create authentication tables and constraints."""

    with engine.begin() as connection:

        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS api")
        )

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS api.users (
                    user_id BIGINT GENERATED ALWAYS
                        AS IDENTITY PRIMARY KEY,

                    username VARCHAR(100)
                        NOT NULL UNIQUE,

                    hashed_password TEXT NOT NULL,

                    role VARCHAR(20) NOT NULL
                        CHECK (role IN ('admin', 'viewer')),

                    is_active BOOLEAN NOT NULL
                        DEFAULT TRUE,

                    created_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW(),

                    updated_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW()
                )
            """)
        )

        connection.execute(
            text("""
                ALTER TABLE api.users
                ADD COLUMN IF NOT EXISTS token_version
                    INTEGER NOT NULL DEFAULT 0
            """)
        )

        count = connection.execute(
            text("SELECT COUNT(*) FROM api.users")
        ).scalar_one()

    print("PASS: Authentication schema verified")
    print("PASS: api.users table initialized")
    print(f"Registered API users: {count}")


if __name__ == "__main__":
    initialize_auth_database()
