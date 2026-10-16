
"""Inspect indexed tables and index usage statistics."""

from sqlalchemy import text
from src.silver.publish import ENGINE


def main():
    with ENGINE.connect() as connection:
        rows = connection.execute(text("""
            SELECT
                s.schemaname,
                s.relname AS table_name,
                s.indexrelname AS index_name,
                s.idx_scan,
                pg_size_pretty(
                    pg_relation_size(s.indexrelid)
                ) AS index_size
            FROM pg_stat_user_indexes s
            WHERE s.schemaname IN ('gold', 'mart')
            ORDER BY
                s.schemaname,
                s.relname,
                s.indexrelname
        """)).mappings().all()

        for row in rows:
            print(
                f"{row['schemaname']}."
                f"{row['table_name']} | "
                f"{row['index_name']} | "
                f"scans={row['idx_scan']} | "
                f"size={row['index_size']}"
            )


if __name__ == "__main__":
    main()
