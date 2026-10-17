"""Authenticated, read-only access to published Gold timesheets."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.database import get_db
from src.api.dependencies import get_current_user
from src.api.schemas import TimesheetPage

router = APIRouter(tags=["Timesheets"])

BASE_FROM = """
FROM gold.fact_timesheet AS t
JOIN gold.dim_date AS d
  ON d.date_key = t.punch_apply_date_key
"""


def build_filters(employee_id: str | None, start_date: date | None, end_date: date | None):
    """Include only supplied filters; values remain SQL parameters."""
    if start_date is not None and end_date is not None and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must be on or before end_date")

    conditions = []
    params = {}

    if employee_id is not None:
        conditions.append("t.client_employee_id = :employee_id")
        params["employee_id"] = employee_id
    if start_date is not None:
        conditions.append("d.full_date >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        conditions.append("d.full_date <= :end_date")
        params["end_date"] = end_date

    where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where_sql, params


def fetch_timesheets(db: Session, employee_id: str | None, start_date: date | None,
                     end_date: date | None, limit: int, offset: int):
    where_sql, params = build_filters(employee_id, start_date, end_date)
    total = db.execute(text(f"SELECT COUNT(*) {BASE_FROM} {where_sql}"), params).scalar_one()
    rows = db.execute(
        text(f"""
            SELECT t.timesheet_key, t.client_employee_id,
                   d.full_date AS punch_apply_date,
                   t.punch_in_datetime, t.punch_out_datetime,
                   t.scheduled_start_datetime, t.scheduled_end_datetime,
                   t.hours_worked, t.department_name, t.pay_code
            {BASE_FROM}
            {where_sql}
            ORDER BY d.full_date DESC, t.timesheet_key DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset},
    ).mappings().all()
    return {"items": [dict(row) for row in rows], "total": total,
            "limit": limit, "offset": offset}


@router.get("/api/v1/timesheets", response_model=TimesheetPage)
def list_timesheets(
    _: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    employee_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    return fetch_timesheets(db, employee_id, start_date, end_date, limit, offset)


@router.get("/api/v1/employees/{employee_id}/timesheets", response_model=TimesheetPage)
def employee_timesheets(
    employee_id: str,
    _: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    start_date: date | None = None,
    end_date: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    return fetch_timesheets(db, employee_id, start_date, end_date, limit, offset)
