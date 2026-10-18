
"""Read-only integration tests for API authentication security."""

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from fastapi.testclient import TestClient
from sqlalchemy import text

from pwdlib import PasswordHash

from src.api.database import engine
from src.api.main import app
from src.api.security import (
    JWT_ALGORITHM,
    JWT_AUDIENCE,
    JWT_ISSUER,
    JWT_SECRET,
    create_access_token,
)


client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def security_test_users():
    """Create isolated users for security tests, then remove them."""

    usernames = (
        "security_viewer_fixture",
        "security_admin_fixture",
    )

    with engine.begin() as connection:
        database = connection.execute(
            text("SELECT current_database()")
        ).scalar_one()

        if database != "ase_etl_test":
            raise RuntimeError(
                f"REFUSING TEST WRITES TO {database!r}"
            )

        existing = connection.execute(
            text("""
                SELECT username
                FROM api.users
                WHERE username IN (:viewer, :admin)
            """),
            {
                "viewer": usernames[0],
                "admin": usernames[1],
            },
        ).scalars().all()

        if existing:
            raise RuntimeError(
                f"Security fixture usernames already exist: {existing}"
            )

        password_hash = PasswordHash.recommended().hash(
            "UnusedSecurityTestPassword_Only!"
        )

        created_ids = []

        for username, role in (
            (usernames[0], "viewer"),
            (usernames[1], "admin"),
        ):
            user_id = connection.execute(
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
                    "hashed_password": password_hash,
                    "role": role,
                },
            ).scalar_one()

            created_ids.append(user_id)

    try:
        yield
    finally:
        with engine.begin() as connection:
            database = connection.execute(
                text("SELECT current_database()")
            ).scalar_one()

            if database != "ase_etl_test":
                raise RuntimeError(
                    "Test database changed before cleanup"
                )

            connection.execute(
                text("""
                    DELETE FROM api.users
                    WHERE user_id IN (:viewer_id, :admin_id)
                """),
                {
                    "viewer_id": created_ids[0],
                    "admin_id": created_ids[1],
                },
            )

def get_test_user(username):
    with engine.connect() as connection:
        row = connection.execute(
            text("""
                SELECT user_id, role, token_version, is_active
                FROM api.users
                WHERE username = :username
            """),
            {"username": username},
        ).mappings().one_or_none()

    assert row is not None, f"Missing test user: {username}"
    assert row["is_active"], f"Inactive test user: {username}"

    return dict(row)


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def signed_token(user, **overrides):
    now = datetime.now(timezone.utc)

    payload = {
        "sub": str(user["user_id"]),
        "token_type": "access",
        "ver": user["token_version"],
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "nbf": now,
        "exp": now + timedelta(minutes=15),
        "jti": "security-test",
    }

    payload.update(overrides)

    return jwt.encode(
        payload,
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )


def test_missing_token_rejected():
    response = client.get("/api/v1/auth/me")
    assert response.status_code == 401


def test_malformed_token_rejected():
    response = client.get(
        "/api/v1/auth/me",
        headers=bearer("not-a-valid-jwt"),
    )
    assert response.status_code == 401


def test_forged_signature_rejected():
    user = get_test_user("security_viewer_fixture")

    token = jwt.encode(
        {
            "sub": str(user["user_id"]),
            "token_type": "access",
            "ver": user["token_version"],
            "iss": JWT_ISSUER,
            "aud": JWT_AUDIENCE,
            "iat": datetime.now(timezone.utc),
            "nbf": datetime.now(timezone.utc),
            "exp": datetime.now(timezone.utc)
                   + timedelta(minutes=15),
            "jti": "forged-test",
        },
        "an-attacker-controlled-signing-secret",
        algorithm=JWT_ALGORITHM,
    )

    response = client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert response.status_code == 401


def test_expired_token_rejected():
    user = get_test_user("security_viewer_fixture")
    now = datetime.now(timezone.utc)

    token = signed_token(
        user,
        iat=now - timedelta(hours=2),
        nbf=now - timedelta(hours=2),
        exp=now - timedelta(hours=1),
    )

    response = client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert response.status_code == 401


def test_wrong_audience_rejected():
    user = get_test_user("security_viewer_fixture")

    token = signed_token(
        user,
        aud="unrelated-service",
    )

    response = client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert response.status_code == 401


def test_wrong_token_version_rejected():
    user = get_test_user("security_viewer_fixture")

    token = signed_token(
        user,
        ver=user["token_version"] + 1,
    )

    response = client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert response.status_code == 401


def test_viewer_can_read_own_profile():
    user = get_test_user("security_viewer_fixture")

    token = create_access_token(
        user["user_id"],
        user["token_version"],
    )

    response = client.get(
        "/api/v1/auth/me",
        headers=bearer(token),
    )

    assert response.status_code == 200
    assert response.json()["role"] == "viewer"
    assert "hashed_password" not in response.json()


def test_viewer_cannot_access_admin():
    user = get_test_user("security_viewer_fixture")

    token = create_access_token(
        user["user_id"],
        user["token_version"],
    )

    response = client.get(
        "/api/v1/auth/admin-check",
        headers=bearer(token),
    )

    assert response.status_code == 403


def test_admin_can_access_admin():
    user = get_test_user("security_admin_fixture")

    token = create_access_token(
        user["user_id"],
        user["token_version"],
    )

    response = client.get(
        "/api/v1/auth/admin-check",
        headers=bearer(token),
    )

    assert response.status_code == 200
    assert response.json()["role"] == "admin"


def test_database_health_requires_authentication():
    response = client.get("/health/db")
    assert response.status_code == 401


def test_database_health_denies_viewer():
    user = get_test_user("security_viewer_fixture")

    token = create_access_token(
        user["user_id"],
        user["token_version"],
    )

    response = client.get(
        "/health/db",
        headers=bearer(token),
    )

    assert response.status_code == 403
