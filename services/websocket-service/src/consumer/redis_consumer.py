# src/consumer/redis_consumer.py
import asyncio
import logging
import os
import socket as _socket
from redis.asyncio import Redis
from src.events import ExecutionStatusEvent
from src.socket_manager import sio

logger = logging.getLogger(__name__)

STATUS_STREAM = "constructor:execution.status.changed"
CONSUMER_GROUP = "websocket-service"
CONSUMER_NAME = os.getenv("HOSTNAME", _socket.gethostname())


class StatusConsumer:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def start(self) -> None:
        try:
            await self._redis.xgroup_create(STATUS_STREAM, CONSUMER_GROUP, id="$", mkstream=True)
        except Exception:
            pass  # group already exists

        while True:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME,
                    {STATUS_STREAM: ">"},
                    count=10, block=2000,
                )
                for _stream, entries in (messages or []):
                    for entry_id, fields in entries:
                        await self._process(entry_id, fields)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consumer error: %s", exc)
                await asyncio.sleep(1)

    async def _process(self, entry_id: str, fields: dict[str, str]) -> None:
        # At-most-once delivery: xack is unconditional so a crash does not leave
        # messages in the PEL. A missed socket event is acceptable — clients
        # reconcile state on reconnect or on the next status transition.
        try:
            event = ExecutionStatusEvent.model_validate_json(fields["data"])
            await sio.emit(
                "execution_update",
                event.model_dump(mode="json"),
                room=str(event.execution_id),
            )
        except Exception as exc:
            logger.error("Failed to process status event %s: %s", entry_id, exc)
        finally:
            await self._redis.xack(STATUS_STREAM, CONSUMER_GROUP, entry_id)
