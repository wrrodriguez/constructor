# src/memory/long_term.py
import uuid
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy import text


class LongTermMemory:
    def __init__(self, engine: AsyncEngine):
        self._factory = async_sessionmaker(engine, expire_on_commit=False)

    async def write(
        self,
        tenant_id: uuid.UUID,
        execution_id: uuid.UUID,
        step_id: str,
        task_id: uuid.UUID,
        model_used: str,
        tokens_used: int,
        iterations: int,
        status: str,
    ) -> None:
        async with self._factory() as session:
            await session.execute(
                text("""
                    INSERT INTO agent_execution_logs
                        (tenant_id, execution_id, step_id, task_id, model_used,
                         tokens_used, iterations, status)
                    VALUES
                        (:tenant_id, :execution_id, :step_id, :task_id, :model_used,
                         :tokens_used, :iterations, :status)
                """),
                {
                    "tenant_id": str(tenant_id),
                    "execution_id": str(execution_id),
                    "step_id": step_id,
                    "task_id": str(task_id),
                    "model_used": model_used,
                    "tokens_used": tokens_used,
                    "iterations": iterations,
                    "status": status,
                },
            )
            await session.commit()
