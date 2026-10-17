
"""ASE-ETL REST API application."""

from typing import Annotated

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.database import get_db
from src.api.dependencies import require_admin
from src.api.routers.auth import router as auth_router

from src.api.routers.employees import router as employees_router
from src.api.routers.timesheets import router as timesheets_router

app = FastAPI(
    title="ASE-ETL API",
    description=(
        "Secure employee management "
        "and read-only timesheet access."
    ),
    version="1.0.0",
)

app.include_router(auth_router)
app.include_router(employees_router)
app.include_router(timesheets_router)


@app.get("/health", tags=["Health"])
def health_check():
    """Basic application liveness check."""

    return {
        "status": "healthy",
        "service": "ASE-ETL API",
    }


@app.get(
    "/health/db",
    tags=["Health"],
)
def database_health(
    db: Annotated[Session, Depends(get_db)],
    _admin: Annotated[dict, Depends(require_admin)],
):
    """Admin-only database connectivity check."""

    db.execute(text("SELECT 1"))

    return {
        "status": "healthy",
        "database": "postgresql",
    }
