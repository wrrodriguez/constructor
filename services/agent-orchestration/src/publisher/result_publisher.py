# src/publisher/result_publisher.py
from redis.asyncio import Redis
from src.events import AgentResult

RESULT_STREAM = "constructor:agent.result.ready"


class ResultPublisher:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def publish(self, result: AgentResult) -> None:
        await self._redis.xadd(RESULT_STREAM, {"data": result.model_dump_json()})
