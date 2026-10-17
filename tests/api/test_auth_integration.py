
"""Isolated integration tests for ASE-ETL authentication."""

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.database import engine
from src.api.main import app
from src.api.security import (
    hash_password,
    JWT_SECRET,
    JWT_ALGORITHM,
    JWT_AUDIENCE,
    JWT_ISSUER,
)
from src.api.login_protection import login_identifier_hash


ADMIN_PASSWORD = "IntegrationAdminPassphrase2026!"
VIEWER_PASSWORD = "IntegrationViewerPassphrase2026!"


def assert_test_database():
    """Fail closed if connected to the wrong database."""

    with engine.connect() as connection:
        database = connection.execute(
            text("SELECT current_database()")
        ).scalar_one()

    if database != "ase_etl_test":
        raise RuntimeError(
            f"REFUSING TEST DATABASE WRITES: {database}"
        )


def clear_test_data():
    """Reset disposable authentication records."""

    assert_test_database()

    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM api.auth_events")
        )
        connection.execute(
            text("DELETE FROM api.login_attempts")
        )
        connection.execute(
            text("DELETE FROM api.users")
        )


def create_test_user(username, password, role):
    """Create an isolated user with an Argon2id hash."""

    assert_test_database()

    with engine.begin() as connection:
        return connection.execute(
            text("""
                INSERT INTO api.users (
                    username,
                    hashed_password,
                    role,
                    is_active,
                    token_version
                )
                VALUES (
                    :username,
                    :hashed_password,
                    :role,
                    TRUE,
                    0
                )
                RETURNING user_id
            """),
            {
                "username": username,
                "hashed_password": hash_password(password),
                "role": role,
            },
        ).scalar_one()


@pytest.fixture
def auth_client():
    """Set up disposable accounts and clean up afterward."""

    assert_test_database()
    clear_test_data()

    create_test_user(
        "integration_admin",
        ADMIN_PASSWORD,
        "admin",
    )

    create_test_user(
        "integration_viewer",
        VIEWER_PASSWORD,
        "viewer",
    )

    with TestClient(app) as client:
        yield client

    clear_test_data()


def login(client, username, password):
    return client.post(
        "/api/v1/auth/token",
        data={
            "username": username,
            "password": password,
        },
    )


def bearer(token):
    return {
        "Authorization": f"Bearer {token}"
    }


def set_user_state(username, **updates):
    """Change whitelisted test-account fields."""

    allowed = {
        "role",
        "is_active",
        "token_version",
    }

    assert updates
    assert set(updates).issubset(allowed)

    assert_test_database()

    assignments = ", ".join(
        f"{column} = :{column}"
        for column in updates
    )

    with engine.begin() as connection:
        result = connection.execute(
            text(f"""
                UPDATE api.users
                SET {assignments},
                    updated_at = NOW()
                WHERE username = :username
            """),
            {
                "username": username,
                **updates,
            },
        )

        assert result.rowcount == 1


def test_successful_login(auth_client):
    response = login(
        auth_client,
        "integration_admin",
        ADMIN_PASSWORD,
    )

    assert response.status_code == 200

    data = response.json()

    assert data["token_type"] == "bearer"
    assert data["access_token"]

    payload = jwt.decode(
        data["access_token"],
        JWT_SECRET,
        algorithms=[JWT_ALGORITHM],
        audience=JWT_AUDIENCE,
        issuer=JWT_ISSUER,
    )

    assert payload["token_type"] == "access"

    protected = auth_client.get(
        "/api/v1/auth/me",
        headers=bearer(data["access_token"]),
    )

    assert protected.status_code == 200
    assert protected.json()["username"] == "integration_admin"
    assert "hashed_password" not in protected.json()


def test_invalid_password(auth_client):
    response = login(
        auth_client,
        "integration_admin",
        "incorrect-password",
    )

    assert response.status_code == 401
    assert response.json()["detail"] == (
        "Invalid username or password"
    )


def test_unknown_username(auth_client):
    response = login(
        auth_client,
        "nonexistent_integration_user",
        "incorrect-password",
    )

    assert response.status_code == 401

    with engine.connect() as connection:
        event = connection.execute(
            text("""
                SELECT user_id, event_type
                FROM api.auth_events
                ORDER BY event_id DESC
                LIMIT 1
            """)
        ).mappings().one()

    assert event["user_id"] is None
    assert event["event_type"] == "login_failure"


def test_throttling_and_recovery(auth_client):
    for _ in range(5):
        response = login(
            auth_client,
            "integration_viewer",
            "incorrect-password",
        )

        assert response.status_code == 401

    response = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert response.status_code == 429
    assert "Retry-After" in response.headers

    # Simulate the expiration of the rate-limit window.
    key = login_identifier_hash(
        "integration_viewer"
    )

    assert_test_database()

    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE api.login_attempts
                SET window_started_at =
                    NOW() - INTERVAL '16 minutes'
                WHERE identifier_hash = :key
            """),
            {"key": key},
        )

    recovered = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert recovered.status_code == 200

    with engine.connect() as connection:
        remaining = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM api.login_attempts
                WHERE identifier_hash = :key
            """),
            {"key": key},
        ).scalar_one()

    assert remaining == 0


def test_disabled_account(auth_client):
    response = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert response.status_code == 200

    token = response.json()["access_token"]

    set_user_state(
        "integration_viewer",
        is_active=False,
    )

    protected = auth_client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert protected.status_code == 401

    blocked_login = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert blocked_login.status_code == 401

    set_user_state(
        "integration_viewer",
        is_active=True,
    )

    restored = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert restored.status_code == 200


def test_token_revocation(auth_client):
    response = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert response.status_code == 200
    old_token = response.json()["access_token"]

    set_user_state(
        "integration_viewer",
        token_version=1,
    )

    rejected = auth_client.get(
        "/api/v1/auth/me",
        headers=bearer(old_token),
    )

    assert rejected.status_code == 401

    fresh_login = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert fresh_login.status_code == 200

    accepted = auth_client.get(
        "/api/v1/auth/me",
        headers=bearer(
            fresh_login.json()["access_token"]
        ),
    )

    assert accepted.status_code == 200


def test_role_changes(auth_client):
    response = login(
        auth_client,
        "integration_viewer",
        VIEWER_PASSWORD,
    )

    assert response.status_code == 200
    token = response.json()["access_token"]

    denied = auth_client.get(
        "/api/v1/auth/admin-check",
        headers=bearer(token),
    )

    assert denied.status_code == 403

    set_user_state(
        "integration_viewer",
        role="admin",
    )

    permitted = auth_client.get(
        "/api/v1/auth/admin-check",
        headers=bearer(token),
    )

    assert permitted.status_code == 200

    set_user_state(
        "integration_viewer",
        role="viewer",
    )

    denied_again = auth_client.get(
        "/api/v1/auth/admin-check",
        headers=bearer(token),
    )

    assert denied_again.status_code == 403


def test_audit_records(auth_client):
    success = login(
        auth_client,
        "integration_admin",
        ADMIN_PASSWORD,
    )

    failure = login(
        auth_client,
        "integration_admin",
        "incorrect-password",
    )

    assert success.status_code == 200
    assert failure.status_code == 401

    with engine.connect() as connection:
        events = connection.execute(
            text("""
                SELECT event_type
                FROM api.auth_events
                ORDER BY event_id
            """)
        ).scalars().all()

        columns = connection.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'api'
                  AND table_name = 'auth_events'
            """)
        ).scalars().all()

    assert events == [
        "login_success",
        "login_failure",
    ]

    assert "hashed_password" not in columns
    assert "access_token" not in columns
