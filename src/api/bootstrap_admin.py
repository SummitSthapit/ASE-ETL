
"""One-time bootstrap of the first API administrator."""

import re
from getpass import getpass

from sqlalchemy import text

from src.api.database import engine
from src.api.security import hash_password


BOOTSTRAP_LOCK_ID = 7182403


def create_initial_admin():

    # Check before prompting for credentials.
    with engine.connect() as connection:
        existing = connection.execute(
            text("SELECT COUNT(*) FROM api.users")
        ).scalar_one()

    if existing != 0:
        raise SystemExit(
            "Bootstrap denied: API users already exist."
        )

    username = input(
        "Administrator username: "
    ).strip().lower()

    if not re.fullmatch(
        r"[a-z][a-z0-9_.-]{2,49}",
        username,
    ):
        raise ValueError(
            "Username must contain 3-50 valid "
            "lowercase characters."
        )

    password = getpass(
        "Administrator password: "
    )

    confirmation = getpass(
        "Confirm password: "
    )

    if password != confirmation:
        raise ValueError("Passwords do not match.")

    if not 14 <= len(password) <= 128:
        raise ValueError(
            "Password must contain 14-128 characters."
        )

    hashed = hash_password(password)

    with engine.begin() as connection:

        # Prevent concurrent first-admin bootstraps.
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock(:lock_id)"
            ),
            {"lock_id": BOOTSTRAP_LOCK_ID},
        )

        existing = connection.execute(
            text("SELECT COUNT(*) FROM api.users")
        ).scalar_one()

        if existing != 0:
            raise RuntimeError(
                "Bootstrap aborted: users already exist."
            )

        connection.execute(
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
                    'admin',
                    TRUE,
                    0
                )
            """),
            {
                "username": username,
                "hashed_password": hashed,
            },
        )

    print("PASS: Initial administrator created")


if __name__ == "__main__":
    create_initial_admin()
