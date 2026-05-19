# src/executions/router.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.dependencies import get_scoped_db
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.executions.schemas import ExecutionCreate, ExecutionRead
from src.executions.service import create_execution, get_execution, list_executions

router = APIRouter(prefix="/executions", tags=["executions"])


@router.post("", response_model=ExecutionRead, status_code=status.HTTP_202_ACCEPTED)
async def trigger(
    body: ExecutionCreate,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_TRIGGER)),
):
    execution = await create_execution(db, body, current_user.tenant_id, current_user.id)
    if not execution:
        raise HTTPException(status_code=404, detail="ProcessDefinition not found")
    return execution


@router.get("/{execution_id}", response_model=ExecutionRead)
async def get_one(
    execution_id: uuid.UUID,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    execution = await get_execution(db, execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.get("", response_model=list[ExecutionRead])
async def list_all(
    status: str | None = Query(default=None),
    definition_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    return await list_executions(db, status=status, definition_id=definition_id, limit=limit, offset=offset)
