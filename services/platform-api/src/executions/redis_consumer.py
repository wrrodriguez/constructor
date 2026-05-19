# src/executions/redis_consumer.py
import asyncio
import logging
from src.database import get_redis
from src.executions.events import AgentResult
from src.executions.dispatcher import resolve_pending, reject_pending

logger = logging.getLogger(__name__)

RESULT_STREAM = "constructor:agent.result.ready"
CONSUMER_GROUP = "platform-api"
CONSUMER_NAME = "platform-api-1"


async def start_result_consumer() -> None:
    """Background task: consumes agent.result.ready and resolves pending Futures."""
    redis = await get_redis()
    try:
        await redis.xgroup_create(RESULT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass  # group already exists

    while True:
        try:
            messages = await redis.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME,
                {RESULT_STREAM: ">"},
                count=10, block=1000,
            )
            for _stream, entries in (messages or []):
                for entry_id, fields in entries:
                    try:
                        result = AgentResult.model_validate_json(fields["data"])
                        if result.status == "completed":
                            resolve_pending(str(result.execution_id), result.output)
                        else:
                            reject_pending(
                                str(result.execution_id),
                                result.error or f"Agent {result.status}",
                            )
                        await redis.xack(RESULT_STREAM, CONSUMER_GROUP, entry_id)
                    except Exception as exc:
                        logger.error("Error processing result entry %s: %s", entry_id, exc)
                        await redis.xack(RESULT_STREAM, CONSUMER_GROUP, entry_id)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Result consumer error: %s", exc)
            await asyncio.sleep(1)
