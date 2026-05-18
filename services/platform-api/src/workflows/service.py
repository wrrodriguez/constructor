# src/workflows/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.workflows.models import ProcessDefinition
from src.workflows.schemas import ProcessDefinitionCreate


async def create_process(
    db: AsyncSession,
    data: ProcessDefinitionCreate,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> ProcessDefinition:
    process = ProcessDefinition(
        tenant_id=tenant_id,
        name=data.name,
        trigger_config=data.trigger,
        steps=[s.model_dump() for s in data.steps],
        on_error=data.on_error,
        created_by=user_id,
    )
    db.add(process)
    await db.flush()
    await db.refresh(process)
    return process


async def list_processes(
    db: AsyncSession, tenant_id: uuid.UUID
) -> list[ProcessDefinition]:
    result = await db.execute(
        select(ProcessDefinition).where(ProcessDefinition.tenant_id == tenant_id)
    )
    return list(result.scalars().all())


async def get_process(
    db: AsyncSession, process_id: uuid.UUID
) -> ProcessDefinition | None:
    return await db.scalar(
        select(ProcessDefinition).where(ProcessDefinition.id == process_id)
    )
