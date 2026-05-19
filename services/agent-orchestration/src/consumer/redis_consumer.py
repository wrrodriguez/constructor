# src/consumer/redis_consumer.py
import asyncio
import logging
from redis.asyncio import Redis
from src.events import AgentTask
from src.runner.agent_runner import AgentRunner
from src.publisher.result_publisher import ResultPublisher
from src.memory.long_term import LongTermMemory

logger = logging.getLogger(__name__)

TASK_STREAM = "constructor:agent.task.created"
CONSUMER_GROUP = "agent-orchestration"
CONSUMER_NAME = "worker-1"


class RedisConsumer:
    def __init__(
        self,
        redis: Redis,
        runner: AgentRunner,
        publisher: ResultPublisher,
        long_term: LongTermMemory,
    ):
        self._redis = redis
        self._runner = runner
        self._publisher = publisher
        self._long_term = long_term

    async def start(self) -> None:
        try:
            await self._redis.xgroup_create(TASK_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except Exception:
            pass  # group already exists

        while True:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME,
                    {TASK_STREAM: ">"},
                    count=5, block=2000,
                )
                for _stream, entries in (messages or []):
                    for entry_id, fields in entries:
                        await self._process(entry_id, fields)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consumer error: %s", exc)
                await asyncio.sleep(1)

    async def _process(self, entry_id: str, fields: dict) -> None:
        task: AgentTask | None = None
        try:
            task = AgentTask.model_validate_json(fields["data"])
            result = await self._runner.run(task)
            await self._publisher.publish(result)
            await self._long_term.write(
                tenant_id=task.tenant_id, execution_id=task.execution_id,
                step_id=task.step_id, task_id=task.task_id,
                model_used=result.model_used, tokens_used=result.tokens_used,
                iterations=result.iterations, status=result.status,
            )
        except Exception as exc:
            logger.error("Failed to process task %s: %s", entry_id, exc)
            # Publish a failed result so Platform API can resolve the pending Future
            # instead of hanging until timeout.
            if task is not None:
                from src.events import AgentResult
                failed = AgentResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    tenant_id=task.tenant_id,
                    step_id=task.step_id,
                    status="failed",
                    output={},
                    tokens_used=0,
                    model_used="",
                    iterations=0,
                    error=str(exc),
                )
                try:
                    await self._publisher.publish(failed)
                except Exception as pub_exc:
                    logger.error("Failed to publish error result for %s: %s", entry_id, pub_exc)
        finally:
            await self._redis.xack(TASK_STREAM, CONSUMER_GROUP, entry_id)
