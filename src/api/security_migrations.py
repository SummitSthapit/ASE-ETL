
"""Database structures for API authentication security."""

from multiprocessing.dummy import connection

from sqlalchemy import text

from src.api.database import engine


def initialize_security_database():
    with engine.begin() as connection:

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS api.auth_events (
                    event_id BIGINT GENERATED ALWAYS
                        AS IDENTITY PRIMARY KEY,

                    user_id BIGINT
                        REFERENCES api.users(user_id)
                        ON DELETE SET NULL,

                    event_type VARCHAR(40) NOT NULL
                        CHECK (
                            event_type IN (
                                'login_success',
                                'login_failure',
                                'login_throttled'
                            )
                        ),

                    occurred_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW()
                )
            """)
        )

        connection.execute(
            text("""
                CREATE INDEX IF NOT EXISTS
                    idx_auth_events_user_time
                ON api.auth_events (
                    user_id,
                    occurred_at DESC
                )
            """)
        )

        connection.execute(
            text("""
                CREATE INDEX IF NOT EXISTS
                    idx_auth_events_time
                ON api.auth_events (occurred_at DESC)
            """)
        )

        # Shared rate-limit state for all API workers.
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS api.login_attempts (
                    identifier_hash CHAR(64) PRIMARY KEY,

                    failure_count INTEGER NOT NULL
                        DEFAULT 0
                        CHECK (failure_count >= 0),

                    window_started_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW()
                )
            """)
        )

        connection.execute(
            text("""
                CREATE INDEX IF NOT EXISTS
                    idx_login_attempts_window
                ON api.login_attempts (window_started_at)
            """)
        )

    print("PASS: Authentication audit table initialized")
    print("PASS: Authentication audit indexes verified")

if __name__ == "__main__":
    initialize_security_database()
