# src/workflows/router.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.workflows.service import create_process, list_processes, get_process
from src.workflows.schemas import ProcessDefinitionCreate, ProcessDefinitionRead
from src.auth.dependencies import get_scoped_db

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.post("", response_model=ProcessDefinitionRead, status_code=status.HTTP_201_CREATED)
async def create(
    body: ProcessDefinitionCreate,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_CREATE)),
) -> ProcessDefinitionRead:
    process = await create_process(db, body, current_user.tenant_id, current_user.id)
    await db.commit()
    return process


@router.get("", response_model=list[ProcessDefinitionRead])
async def list_all(
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
) -> list[ProcessDefinitionRead]:
    return await list_processes(db, current_user.tenant_id)


@router.get("/{workflow_id}", response_model=ProcessDefinitionRead)
async def get_one(
    workflow_id: uuid.UUID,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
) -> ProcessDefinitionRead:
    process = await get_process(db, workflow_id)
    if not process:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")
    return process
