# src/workflows/schemas.py
import uuid
from datetime import datetime
from pydantic import BaseModel, field_validator
from src.workflows.validator import validate_steps, VALID_ON_ERROR


class StepSchema(BaseModel):
    id: str
    type: str
    config: dict
    next: str | dict | None = None
    timeout_seconds: int = 60


class ProcessDefinitionCreate(BaseModel):
    name: str
    trigger: dict
    steps: list[StepSchema]
    on_error: str = "stop"

    @field_validator("steps")
    @classmethod
    def validate_steps_field(cls, v: list[StepSchema]) -> list[StepSchema]:
        errors = validate_steps([s.model_dump() for s in v])
        if errors:
            raise ValueError("; ".join(errors))
        return v

    @field_validator("on_error")
    @classmethod
    def validate_on_error(cls, v: str) -> str:
        if v not in VALID_ON_ERROR:
            raise ValueError(f"on_error must be one of {sorted(VALID_ON_ERROR)}")
        return v


class ProcessDefinitionRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    name: str
    trigger_config: dict
    steps: list[StepSchema]
    on_error: str
    version: int
    created_at: datetime

    model_config = {"from_attributes": True}
