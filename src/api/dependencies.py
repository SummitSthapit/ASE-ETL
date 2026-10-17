
"""Authentication and authorization dependencies."""

from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.database import get_db
from src.api.security import decode_access_token


oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/api/v1/auth/token"
)

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid or expired authentication credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: Annotated[Session, Depends(get_db)],
):
    """Validate JWT and retrieve the current database user."""

    try:
        payload = decode_access_token(token)
        user_id = int(payload["sub"])

    except (InvalidTokenError, ValueError, TypeError):
        raise credentials_exception from None

    user = db.execute(
        text("""
            SELECT
                user_id,
                username,
                role,
                is_active,
                token_version
            FROM api.users
            WHERE user_id = :user_id
        """),
        {"user_id": user_id},
    ).mappings().one_or_none()

    if user is None or not user["is_active"]:
        raise credentials_exception

    if user["token_version"] != payload["ver"]:
        raise credentials_exception

    return dict(user)


def require_admin(
    current_user: Annotated[
        dict, Depends(get_current_user)
    ],
):
    """Permit administrative operations only."""

    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions",
        )

    return current_user
