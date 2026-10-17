
"""Provision additional API users from a trusted local CLI."""

import argparse
import re
from getpass import getpass

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.api.database import engine
from src.api.security import hash_password


def create_user(username: str, role: str) -> None:
    username = username.strip().lower()

    if not re.fullmatch(r"[a-z][a-z0-9_.-]{2,49}", username):
        raise ValueError("Invalid username format")

    if role not in {"admin", "viewer"}:
        raise ValueError("Invalid role")

    password = getpass("New user password: ")
    confirmation = getpass("Confirm password: ")

    if password != confirmation:
        raise ValueError("Passwords do not match")

    if not 14 <= len(password) <= 128:
        raise ValueError(
            "Password must contain 14-128 characters"
        )

    hashed_password = hash_password(password)

    try:
        with engine.begin() as connection:
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
                        :role,
                        TRUE,
                        0
                    )
                """),
                {
                    "username": username,
                    "hashed_password": hashed_password,
                    "role": role,
                },
            )

    except IntegrityError:
        raise SystemExit(
            "User creation failed: username unavailable"
        ) from None

    print(f"PASS: {role} account created")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--username",
        required=True,
    )

    parser.add_argument(
        "--role",
        choices=["admin", "viewer"],
        required=True,
    )

    args = parser.parse_args()

    create_user(args.username, args.role)
