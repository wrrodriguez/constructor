# src/memory/short_term.py
import json
from redis.asyncio import Redis
from langchain_core.messages import BaseMessage, messages_to_dict, messages_from_dict


class ShortTermMemory:
    def __init__(self, redis: Redis):
        self._redis = redis

    def _key(self, tenant_id: str, execution_id: str) -> str:
        return f"tenant:{tenant_id}:execution:{execution_id}:messages"

    async def save(
        self,
        tenant_id: str,
        execution_id: str,
        messages: list[BaseMessage],
        ttl_seconds: int = 86400,
    ) -> None:
        key = self._key(tenant_id, execution_id)
        data = json.dumps(messages_to_dict(messages))
        await self._redis.set(key, data, ex=ttl_seconds)

    async def load(self, tenant_id: str, execution_id: str) -> list[BaseMessage]:
        key = self._key(tenant_id, execution_id)
        data = await self._redis.get(key)
        if not data:
            return []
        return messages_from_dict(json.loads(data))

    async def delete(self, tenant_id: str, execution_id: str) -> None:
        await self._redis.delete(self._key(tenant_id, execution_id))
