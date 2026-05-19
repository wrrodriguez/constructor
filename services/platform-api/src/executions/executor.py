# src/executions/executor.py
import asyncio
import uuid
import logging
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from src.workflows.models import ProcessDefinition, ProcessExecution
from src.executions.condition import evaluate_condition
from src.executions.transform import apply_transform
from src.executions.events import AgentTask
from src.executions.dispatcher import register_pending
from src.database import get_redis

logger = logging.getLogger(__name__)

TASK_STREAM = "constructor:agent.task.created"


async def dispatch_agent_step(
    execution: ProcessExecution,
    step: dict,
) -> dict:
    """Publish AgentTask to Redis Streams and await the result via Future."""
    task = AgentTask(
        task_id=uuid.uuid4(),
        execution_id=execution.id,
        tenant_id=execution.tenant_id,
        step_id=step["id"],
        task_type=step["config"].get("task_type", "default"),
        prompt=step["config"].get("prompt", ""),
        context=execution.context,
        tools_allowed=step["config"].get("tools_allowed", []),
        model_override=step["config"].get("model_override"),
        timeout_seconds=step.get("timeout_seconds", 300),
        max_iterations=step["config"].get("max_iterations", 20),
    )
    future = register_pending(execution.id)
    redis = await get_redis()
    await redis.xadd(TASK_STREAM, {"data": task.model_dump_json()})
    return await asyncio.wait_for(future, timeout=task.timeout_seconds)


async def execute_workflow(
    execution_id: uuid.UUID,
    process_id: uuid.UUID,
    tenant_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """Main workflow execution loop. Runs as an asyncio.Task."""
    from src.workflows.models import ProcessExecution, ProcessDefinition

    execution = await db.get(ProcessExecution, execution_id)
    definition = await db.get(ProcessDefinition, process_id)

    if not execution or not definition:
        logger.error("Execution or definition not found: %s / %s", execution_id, process_id)
        return

    steps_map: dict[str, dict] = {s["id"]: s for s in definition.steps}
    current_step_id: str | None = definition.steps[0]["id"] if definition.steps else None

    execution.status = "running"
    execution.started_at = datetime.now(timezone.utc)
    await db.commit()

    try:
        while current_step_id:
            step = steps_map.get(current_step_id)
            if not step:
                raise ValueError(f"Step '{current_step_id}' not in definition")

            execution.current_step_id = current_step_id
            await db.commit()

            match step["type"]:
                case "condition":
                    current_step_id = evaluate_condition(step["config"], execution.context)
                    continue
                case "transform":
                    execution.context = apply_transform(step["config"], execution.context)
                case "agent":
                    result = await dispatch_agent_step(execution, step)
                    execution.context = {**execution.context, step["id"]: result}
                case _:
                    pass  # stub: notify, wait, human_approval — avanza al siguiente paso

            current_step_id = step.get("next")
            await db.commit()

        execution.status = "completed"
        execution.current_step_id = None
        execution.completed_at = datetime.now(timezone.utc)
        await db.commit()

    except asyncio.TimeoutError:
        execution.status = "failed"
        execution.context = {**execution.context, "_error": f"Timeout on step '{execution.current_step_id}'"}
        await db.commit()
    except Exception as exc:
        logger.error("Workflow execution failed: %s", exc)
        execution.status = "failed"
        execution.context = {**execution.context, "_error": str(exc)}
        await db.commit()
