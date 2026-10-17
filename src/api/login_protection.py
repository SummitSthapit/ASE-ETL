
"""Database-backed authentication abuse protection."""

import hashlib
import hmac

from datetime import timedelta

from sqlalchemy import text

from src.api.security import JWT_SECRET


MAX_FAILED_ATTEMPTS = 5
WINDOW_MINUTES = 15


def login_identifier_hash(username: str) -> str:
    """Produce a keyed, non-reversible login identifier."""

    return hmac.new(
        JWT_SECRET.encode("utf-8"),
        username.strip().lower().encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def lock_login_identifier(db, identifier_hash: str):
    """Serialize concurrent login checks for one identifier."""

    db.execute(
        text("""
            SELECT pg_advisory_xact_lock(
                hashtext(:identifier_hash)
            )
        """),
        {"identifier_hash": identifier_hash},
    )


def get_failure_count(db, identifier_hash: str) -> int:
    """Return current failures within the rolling window."""

    row = db.execute(
        text("""
            SELECT failure_count, window_started_at
            FROM api.login_attempts
            WHERE identifier_hash = :identifier_hash
        """),
        {"identifier_hash": identifier_hash},
    ).mappings().one_or_none()

    if row is None:
        return 0

    now = db.execute(
        text("SELECT NOW()")
    ).scalar_one()

    if now - row["window_started_at"] >= timedelta(
        minutes=WINDOW_MINUTES
    ):
        db.execute(
            text("""
                DELETE FROM api.login_attempts
                WHERE identifier_hash = :identifier_hash
            """),
            {"identifier_hash": identifier_hash},
        )
        return 0

    return row["failure_count"]


def record_failure(db, identifier_hash: str):
    """Record an unsuccessful authentication attempt."""

    db.execute(
        text("""
            INSERT INTO api.login_attempts (
                identifier_hash,
                failure_count,
                window_started_at
            )
            VALUES (:identifier_hash, 1, NOW())

            ON CONFLICT (identifier_hash)
            DO UPDATE SET
                failure_count =
                    api.login_attempts.failure_count + 1
        """),
        {"identifier_hash": identifier_hash},
    )


def clear_failures(db, identifier_hash: str):
    """Clear a login counter after successful authentication."""

    db.execute(
        text("""
            DELETE FROM api.login_attempts
            WHERE identifier_hash = :identifier_hash
        """),
        {"identifier_hash": identifier_hash},
    )


def record_auth_event(db, user_id, event_type: str):
    """Write a credential-free authentication audit event."""

    db.execute(
        text("""
            INSERT INTO api.auth_events (
                user_id,
                event_type
            )
            VALUES (
                :user_id,
                :event_type
            )
        """),
        {
            "user_id": user_id,
            "event_type": event_type,
        },
    )
