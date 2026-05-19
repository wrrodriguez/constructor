# src/events.py
import uuid
from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class ExecutionStatusEvent(BaseModel):
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    current_step_id: str | None
    context: dict
    timestamp: datetime
