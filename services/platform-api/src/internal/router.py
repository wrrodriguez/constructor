# src/internal/router.py
import hmac
import uuid
from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from pydantic import BaseModel

from src.config import settings
from src.database import get_db
from src.workflows.models import ProcessDefinition
from src.executions.schemas import ExecutionCreate
from src.executions.service import create_execution

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)


class InternalExecutionCreate(BaseModel):
    process_definition_id: uuid.UUID
    context: dict = {}


@router.post("/executions", status_code=status.HTTP_202_ACCEPTED)
async def internal_trigger(
    body: InternalExecutionCreate,
    x_internal_secret: str | None = Header(default=None, alias="X-Internal-Secret"),
    db: AsyncSession = Depends(get_db),
):
    if not x_internal_secret or not hmac.compare_digest(x_internal_secret, settings.internal_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    # Query without RLS to get tenant_id (constructor role has superuser in dev)
    workflow = await db.scalar(
        select(ProcessDefinition).where(ProcessDefinition.id == body.process_definition_id)
    )
    if not workflow:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")

    # Set RLS context for this tenant
    await db.execute(
        text("SELECT set_config('app.current_tenant', :tid, false)"),
        {"tid": str(workflow.tenant_id)},
    )

    data = ExecutionCreate(
        process_definition_id=body.process_definition_id,
        context=body.context,
    )
    execution = await create_execution(
        db, data, workflow.tenant_id, triggered_by=None, trigger_type="schedule"
    )
    return {
        "execution_id": str(execution.id),
        "status": execution.status,
        "trigger_type": execution.trigger_type,
    }
