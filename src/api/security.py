
"""Password hashing and JWT security utilities."""

import os
import re
import uuid

from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt

from dotenv import load_dotenv
from jwt.exceptions import InvalidTokenError
from pwdlib import PasswordHash


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

JWT_SECRET = os.getenv("API_JWT_SECRET", "")

if not re.fullmatch(r"[0-9a-fA-F]{64}", JWT_SECRET):
    raise RuntimeError(
        "API_JWT_SECRET must be 64 hexadecimal characters "
        "generated using a cryptographically secure source."
    )

JWT_ALGORITHM = "HS256"
JWT_ISSUER = "ase-etl-api"
JWT_AUDIENCE = "ase-etl-users"

ACCESS_TOKEN_EXPIRE_MINUTES = 15

password_hasher = PasswordHash.recommended()


def hash_password(password: str) -> str:
    """Hash password using Argon2id."""
    return password_hasher.hash(password)


def verify_password(
    plain_password: str,
    hashed_password: str,
) -> bool:
    """Verify password against a stored hash."""
    return password_hasher.verify(
        plain_password,
        hashed_password,
    )


def create_access_token(
    user_id: int,
    token_version: int,
) -> str:
    """Create a short-lived signed access token."""

    now = datetime.now(timezone.utc)

    payload = {
        "sub": str(user_id),
        "token_type": "access",
        "ver": token_version,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "nbf": now,
        "exp": now + timedelta(
            minutes=ACCESS_TOKEN_EXPIRE_MINUTES
        ),
        "jti": uuid.uuid4().hex,
    }

    return jwt.encode(
        payload,
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )


def decode_access_token(token: str) -> dict:
    """Validate signature, expiry and expected claims."""

    payload = jwt.decode(
        token,
        JWT_SECRET,
        algorithms=[JWT_ALGORITHM],
        issuer=JWT_ISSUER,
        audience=JWT_AUDIENCE,
        options={
            "require": [
                "sub",
                "token_type",
                "ver",
                "iss",
                "aud",
                "iat",
                "nbf",
                "exp",
                "jti",
            ]
        },
    )

    if payload.get("token_type") != "access":
        raise InvalidTokenError(
            "Invalid token type"
        )

    subject = payload.get("sub")

    if (
        not isinstance(subject, str)
        or not subject.isdecimal()
        or int(subject) <= 0
    ):
        raise InvalidTokenError(
            "Invalid token subject"
        )

    version = payload.get("ver")

    if type(version) is not int or version < 0:
        raise InvalidTokenError(
            "Invalid token version"
        )

    return payload
