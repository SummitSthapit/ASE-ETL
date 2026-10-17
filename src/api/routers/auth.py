
"""Authentication endpoints for ASE-ETL."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.database import get_db
from src.api.dependencies import (
    get_current_user,
    require_admin,
)
from src.api.security import (
    create_access_token,
    verify_password,
)

from src.api.login_protection import (
    MAX_FAILED_ATTEMPTS,
    login_identifier_hash,
    lock_login_identifier,
    get_failure_count,
    record_failure,
    clear_failures,
    record_auth_event,
)
from src.api.security import hash_password


router = APIRouter(
    prefix="/api/v1/auth",
    tags=["Authentication"],
)

DUMMY_PASSWORD_HASH = hash_password(
    "dummy-password-for-unknown-api-account"
)

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    user_id: int
    username: str
    role: str
    is_active: bool



@router.post(
    "/token",
    response_model=TokenResponse,
)
def login(
    form_data: Annotated[
        OAuth2PasswordRequestForm, Depends()
    ],
    db: Annotated[Session, Depends(get_db)],
):
    """Authenticate with database-backed login throttling."""

    username = form_data.username.strip().lower()
    identifier = login_identifier_hash(username)

    outcome = None
    authenticated_user = None

    # Keep the locking, counters and audit events
    # inside one committed transaction.
    with db.begin():

        lock_login_identifier(db, identifier)

        failures = get_failure_count(db, identifier)

        if failures >= MAX_FAILED_ATTEMPTS:
            record_auth_event(
                db, None, "login_throttled"
            )
            outcome = "throttled"

        else:
            user = db.execute(
                text("""
                    SELECT
                        user_id,
                        username,
                        hashed_password,
                        role,
                        is_active,
                        token_version
                    FROM api.users
                    WHERE username = :username
                """),
                {"username": username},
            ).mappings().one_or_none()

            # Verify a dummy hash for unknown accounts.
            password_hash = (
                user["hashed_password"]
                if user is not None
                else DUMMY_PASSWORD_HASH
            )

            password_valid = verify_password(
                form_data.password,
                password_hash,
            )

            if (
                user is None
                or not password_valid
                or not user["is_active"]
            ):
                record_failure(db, identifier)

                record_auth_event(
                    db,
                    user["user_id"] if user else None,
                    "login_failure",
                )

                outcome = "invalid"

            else:
                clear_failures(db, identifier)

                record_auth_event(
                    db,
                    user["user_id"],
                    "login_success",
                )

                authenticated_user = {
                    "user_id": user["user_id"],
                    "token_version": user["token_version"],
                }

                outcome = "success"

    # All authentication state changes are committed
    # before returning HTTP errors.

    if outcome == "throttled":
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
            headers={"Retry-After": "900"},
        )

    if outcome != "success":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(
        user_id=authenticated_user["user_id"],
        token_version=authenticated_user["token_version"],
    )

    return TokenResponse(access_token=access_token)


@router.get(
    "/me",
    response_model=UserResponse,
)
def read_current_user(
    current_user: Annotated[
        dict, Depends(get_current_user)
    ],
):
    """Return the authenticated user's public details."""
    return current_user


@router.get(
    "/admin-check",
    response_model=UserResponse,
)
def admin_check(
    current_user: Annotated[
        dict, Depends(require_admin)
    ],
):
    """Temporary administrator authorization check."""
    return current_user
