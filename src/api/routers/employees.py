"""Authenticated employee CRUD on operational API-owned records only."""
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.api.database import get_db
from src.api.dependencies import get_current_user, require_admin
from src.api.schemas import EmployeeCreate, EmployeeUpdate, EmployeeOut, EmployeePage, EMPLOYEE_FIELDS

router = APIRouter(prefix='/api/v1/employees', tags=['Employees'])

SELECT_COLUMNS = 'client_employee_id, ' + ', '.join(EMPLOYEE_FIELDS) + ', created_at, updated_at, is_deleted, sync_status'

def fetch_employee(db: Session, employee_id: str):
    return db.execute(text(f'''SELECT {SELECT_COLUMNS} FROM api.employees
       WHERE client_employee_id = :id AND is_deleted = FALSE'''), {'id': employee_id}).mappings().one_or_none()

@router.get('', response_model=EmployeePage)
def list_employees(
    _: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    total = db.execute(text('SELECT COUNT(*) FROM api.employees WHERE is_deleted = FALSE')).scalar_one()
    rows = db.execute(text(f'''SELECT {SELECT_COLUMNS} FROM api.employees
       WHERE is_deleted = FALSE ORDER BY client_employee_id
       LIMIT :limit OFFSET :offset'''), {'limit': limit, 'offset': offset}).mappings().all()
    return {'items': [dict(r) for r in rows], 'total': total, 'limit': limit, 'offset': offset}

@router.get('/{employee_id}', response_model=EmployeeOut)
def get_employee(
    employee_id: str,
    _: Annotated[dict, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
):
    employee = fetch_employee(db, employee_id)
    if employee is None:
        raise HTTPException(status_code=404, detail='Employee not found')
    return dict(employee)

@router.post('', response_model=EmployeeOut, status_code=status.HTTP_201_CREATED)
def create_employee(
    payload: EmployeeCreate,
    admin: Annotated[dict, Depends(require_admin)],
    db: Annotated[Session, Depends(get_db)],
):
    values = payload.model_dump()
    # Authentication has already performed a SELECT on this Session,
    # so a transaction is active; do not call db.begin() again.
    try:
        db.execute(text("SELECT set_config('api.actor_user_id', :actor, true)"),
                   {'actor': str(admin['user_id'])})
        db.execute(text('SELECT pg_advisory_xact_lock(hashtext(:id))'), {'id': payload.client_employee_id})
        exists = db.execute(text('''SELECT EXISTS(
            SELECT 1 FROM api.employees WHERE client_employee_id = :id
            UNION ALL SELECT 1 FROM gold.dim_employee WHERE client_employee_id = :id
        )'''), {'id': payload.client_employee_id}).scalar_one()
        if exists:
            raise HTTPException(status_code=409, detail='Employee ID already exists')
        columns = list(values)
        sql = f'''INSERT INTO api.employees ({', '.join(columns)}, sync_status)
               VALUES ({', '.join(':'+x for x in columns)}, 'pending')'''
        db.execute(text(sql), values)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return dict(fetch_employee(db, payload.client_employee_id))

@router.patch('/{employee_id}', response_model=EmployeeOut)
def update_employee(
    employee_id: str,
    payload: EmployeeUpdate,
    admin: Annotated[dict, Depends(require_admin)],
    db: Annotated[Session, Depends(get_db)],
):
    changes = payload.model_dump(exclude_unset=True)
    try:
        db.execute(text("SELECT set_config('api.actor_user_id', :actor, true)"),
                   {'actor': str(admin['user_id'])})
        current = db.execute(text('''SELECT hire_date, term_date FROM api.employees
             WHERE client_employee_id=:id AND is_deleted=FALSE FOR UPDATE'''), {'id': employee_id}).mappings().one_or_none()
        if current is None:
            raise HTTPException(status_code=404, detail='Employee not found')
        hire = changes.get('hire_date', current['hire_date'])
        term = changes.get('term_date', current['term_date'])
        if hire is not None and term is not None and term < hire:
            raise HTTPException(status_code=422, detail='term_date cannot precede hire_date')
        assignments = ', '.join(f'{k} = :{k}' for k in changes)
        db.execute(text(f'''UPDATE api.employees SET {assignments},
             sync_status='pending', updated_at=NOW()
             WHERE client_employee_id=:id'''), {'id': employee_id, **changes})
        db.commit()
    except Exception:
        db.rollback()
        raise
    return dict(fetch_employee(db, employee_id))

@router.delete('/{employee_id}', status_code=status.HTTP_204_NO_CONTENT)
def delete_employee(
    employee_id: str,
    admin: Annotated[dict, Depends(require_admin)],
    db: Annotated[Session, Depends(get_db)],
):
    try:
        db.execute(text("SELECT set_config('api.actor_user_id', :actor, true)"),
                   {'actor': str(admin['user_id'])})
        result = db.execute(text('''UPDATE api.employees SET is_deleted=TRUE,
             sync_status='pending', updated_at=NOW()
             WHERE client_employee_id=:id AND is_deleted=FALSE'''), {'id': employee_id})
        if result.rowcount != 1:
            raise HTTPException(status_code=404, detail='Employee not found')
        db.commit()
    except Exception:
        db.rollback()
        raise
    return None
