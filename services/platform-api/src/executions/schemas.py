# src/executions/schemas.py
import uuid
from datetime import datetime
from pydantic import BaseModel


class ExecutionCreate(BaseModel):
    process_definition_id: uuid.UUID
    context: dict = {}


class ExecutionRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    process_definition_id: uuid.UUID
    status: str
    current_step_id: str | None
    context: dict
    triggered_by: uuid.UUID | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}
