"""Inspect API employee synchronization state."""

from sqlalchemy import text

from src.api.database import engine


def get_api_sync_state():
    """Return current API synchronization information."""

    with engine.connect() as connection:
        status = connection.execute(
            text("""
                SELECT
                    COUNT(*) AS total_employees,
                    COUNT(*) FILTER (
                        WHERE sync_status = 'synced'
                    ) AS synced_employees,
                    COUNT(*) FILTER (
                        WHERE sync_status IS DISTINCT FROM 'synced'
                    ) AS pending_employees
                FROM api.employees
            """)
        ).mappings().one()

        event_count = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM api.employee_change_log
            """)
        ).scalar_one()

    return {
        "total_employees": status["total_employees"],
        "synced_employees": status["synced_employees"],
        "pending_employees": status["pending_employees"],
        "change_events": event_count,
    }


def has_pending_api_changes():
    """Determine whether API employee changes are pending."""

    state = get_api_sync_state()
    return state["pending_employees"] > 0


if __name__ == "__main__":
    state = get_api_sync_state()

    print("ASE-ETL API SYNCHRONIZATION STATE")
    print("-" * 50)

    for name, value in state.items():
        print(f"{name}: {value}")

    print("-" * 50)

    print(
        "Pending API changes:",
        has_pending_api_changes(),
    )