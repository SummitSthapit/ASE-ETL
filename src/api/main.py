"""ASE-ETL REST API application."""

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.database import get_db


app = FastAPI(
    title="ASE-ETL API",
    description=(
        "REST API for employee management "
        "and read-only timesheet access."
    ),
    version="1.0.0",
)


@app.get("/health", tags=["Health"])
def health_check():
    """Verify the API application is running."""

    return {
        "status": "healthy",
        "service": "ASE-ETL API",
    }


@app.get("/health/db", tags=["Health"])
def database_health(
    db: Session = Depends(get_db),
):
    """Verify PostgreSQL database connectivity."""

    db.execute(text("SELECT 1"))

    return {
        "status": "healthy",
        "database": "postgresql",
    }