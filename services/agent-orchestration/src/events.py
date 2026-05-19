# src/events.py
import uuid
from typing import Literal
from pydantic import BaseModel


class AgentTask(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    task_type: str
    prompt: str
    context: dict
    tools_allowed: list[str]
    model_override: str | None = None
    timeout_seconds: int = 300
    max_iterations: int = 20


class AgentResult(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    status: Literal["completed", "failed", "timeout"]
    output: dict
    tokens_used: int
    model_used: str
    iterations: int
    error: str | None = None
