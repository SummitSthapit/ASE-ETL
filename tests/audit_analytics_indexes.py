
"""Audit PostgreSQL Gold and analytics indexes."""

from sqlalchemy import text
from src.silver.publish import ENGINE


def main():
    with ENGINE.connect() as connection:
        rows = connection.execute(text("""
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname IN ('gold', 'mart')
            ORDER BY schemaname, tablename, indexname
        """)).mappings().all()

        print("EXISTING DATABASE INDEXES")
        print("=" * 65)

        for row in rows:
            print(
                f"\n{row['schemaname']}."
                f"{row['tablename']}"
            )
            print(f"Index: {row['indexname']}")
            print(row["indexdef"])

        print(f"\nTotal indexes: {len(rows)}")


if __name__ == "__main__":
    main()
