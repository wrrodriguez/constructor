# src/executions/service.py
import asyncio
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.workflows.models import ProcessDefinition, ProcessExecution
from src.executions.schemas import ExecutionCreate
from src.database import AsyncSessionFactory


async def create_execution(
    db: AsyncSession,
    data: ExecutionCreate,
    tenant_id: uuid.UUID,
    triggered_by: uuid.UUID | None,
    trigger_type: str = "manual",
) -> ProcessExecution:
    definition = await db.scalar(
        select(ProcessDefinition).where(
            ProcessDefinition.id == data.process_definition_id,
            ProcessDefinition.tenant_id == tenant_id,
        )
    )
    if not definition:
        return None

    execution = ProcessExecution(
        tenant_id=tenant_id,
        process_definition_id=data.process_definition_id,
        context=data.context,
        triggered_by=triggered_by,
        trigger_type=trigger_type,
        status="pending",
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)

    # Arranca el WorkflowExecutor en background — usa su propia sesión de DB
    asyncio.create_task(
        _run_workflow(execution.id, definition.id, tenant_id)
    )
    return execution


async def _run_workflow(
    execution_id: uuid.UUID,
    process_id: uuid.UUID,
    tenant_id: uuid.UUID,
) -> None:
    from src.executions.executor import execute_workflow
    from sqlalchemy import text
    async with AsyncSessionFactory() as db:
        await db.execute(
            text("SELECT set_config('app.current_tenant', :tid, false)"),
            {"tid": str(tenant_id)},
        )
        await execute_workflow(execution_id, process_id, tenant_id, db)


async def get_execution(db: AsyncSession, execution_id: uuid.UUID) -> ProcessExecution | None:
    return await db.scalar(
        select(ProcessExecution).where(ProcessExecution.id == execution_id)
    )


async def list_executions(
    db: AsyncSession,
    status: str | None = None,
    definition_id: uuid.UUID | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[ProcessExecution]:
    q = select(ProcessExecution)
    if status:
        q = q.where(ProcessExecution.status == status)
    if definition_id:
        q = q.where(ProcessExecution.process_definition_id == definition_id)
    q = q.order_by(ProcessExecution.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(q)
    return list(result.scalars().all())
