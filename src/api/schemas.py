"""Request and response contracts for employee operations."""
from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EMPLOYEE_FIELDS = (
    'first_name', 'middle_name', 'last_name', 'preferred_name',
    'job_code', 'job_title', 'department_id', 'department_name',
    'organization_id', 'organization_name', 'hire_date',
    'term_date', 'active_status',
)

class EmployeeFields(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    first_name: str | None = Field(default=None, max_length=200)
    middle_name: str | None = Field(default=None, max_length=200)
    last_name: str | None = Field(default=None, max_length=200)
    preferred_name: str | None = Field(default=None, max_length=200)
    job_code: str | None = Field(default=None, max_length=200)
    job_title: str | None = Field(default=None, max_length=200)
    department_id: str | None = Field(default=None, max_length=200)
    department_name: str | None = Field(default=None, max_length=200)
    organization_id: str | None = Field(default=None, max_length=200)
    organization_name: str | None = Field(default=None, max_length=200)
    hire_date: date | None = None
    term_date: date | None = None
    active_status: str | None = Field(default=None, max_length=100)

    @field_validator('first_name', 'last_name')
    @classmethod
    def nonempty_name(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError('Name must not be blank')
        return value

class EmployeeCreate(EmployeeFields):
    client_employee_id: str = Field(min_length=1, max_length=200)
    first_name: str = Field(min_length=1, max_length=200)
    last_name: str = Field(min_length=1, max_length=200)

    @model_validator(mode='after')
    def check_dates(self):
        if self.hire_date and self.term_date and self.term_date < self.hire_date:
            raise ValueError('term_date cannot precede hire_date')
        return self

class EmployeeUpdate(EmployeeFields):
    @model_validator(mode='after')
    def nonempty(self):
        if not self.model_fields_set:
            raise ValueError('Provide at least one employee field')
        if 'first_name' in self.model_fields_set and self.first_name is None:
            raise ValueError('first_name cannot be null')
        if 'last_name' in self.model_fields_set and self.last_name is None:
            raise ValueError('last_name cannot be null')
        return self

class EmployeeOut(EmployeeFields):
    client_employee_id: str
    is_deleted: bool
    sync_status: Literal['pending', 'synced', 'failed']
    created_at: datetime
    updated_at: datetime

class EmployeePage(BaseModel):
    items: list[EmployeeOut]
    total: int
    limit: int
    offset: int

class TimesheetOut(BaseModel):
    timesheet_key: int
    client_employee_id: str
    punch_apply_date: date
    punch_in_datetime: datetime
    punch_out_datetime: datetime
    scheduled_start_datetime: datetime | None
    scheduled_end_datetime: datetime | None
    hours_worked: float
    department_name: str | None
    pay_code: str | None

class TimesheetPage(BaseModel):
    items: list[TimesheetOut]
    total: int
    limit: int
    offset: int
