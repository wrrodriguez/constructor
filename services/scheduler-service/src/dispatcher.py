# src/dispatcher.py
import asyncio
import logging
import uuid

import httpx
from redis.asyncio import Redis

from src.config import settings

logger = logging.getLogger(__name__)

_LOCK_TTL = 55  # seconds — less than the minimum 1-minute interval
_MAX_RETRIES = 3


async def dispatch(workflow_id: uuid.UUID, tenant_id: uuid.UUID, redis: Redis) -> None:
    """Fire a scheduled workflow execution via Platform API's internal endpoint.

    Uses a Redis distributed lock (SET NX EX) to prevent duplicate executions
    when multiple scheduler-service instances are running.
    """
    lock_key = f"scheduler:lock:{workflow_id}"
    acquired = await redis.set(lock_key, "1", nx=True, ex=_LOCK_TTL)
    if not acquired:
        logger.info("Lock not acquired for workflow %s — another instance is handling it", workflow_id)
        return

    try:
        await _dispatch_with_retry(workflow_id)
    finally:
        await redis.delete(lock_key)


async def _dispatch_with_retry(workflow_id: uuid.UUID) -> None:
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{settings.platform_api_url}/internal/executions",
                    json={"process_definition_id": str(workflow_id), "context": {}},
                    headers={"X-Internal-Secret": settings.internal_secret},
                )
                resp.raise_for_status()
            logger.info("Dispatched workflow %s → execution created", workflow_id)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)  # 2s, 4s
                logger.warning(
                    "Dispatch attempt %d/%d failed for workflow %s: %s — retrying in %ds",
                    attempt + 1, _MAX_RETRIES, workflow_id, exc, wait,
                )
                await asyncio.sleep(wait)

    logger.error(
        "All %d dispatch attempts failed for workflow %s: %s",
        _MAX_RETRIES, workflow_id, last_exc,
    )
