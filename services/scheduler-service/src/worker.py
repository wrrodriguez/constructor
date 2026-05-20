"""Scheduler service entry point.

Starts the APScheduler loop and a background task that periodically reloads
scheduled workflows from the DB. Reload also happens at startup.

Horizontal scaling: multiple instances can run safely — the Redis distributed
lock in dispatcher.py prevents duplicate executions.
"""
import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import settings
from src.job_loader import load_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


async def _periodic_reload(
    scheduler: AsyncIOScheduler,
    engine,
    redis: Redis,
    interval: int,
) -> None:
    """Reload scheduled jobs every `interval` seconds as a fallback."""
    while True:
        await asyncio.sleep(interval)
        logger.info("Reloading scheduled jobs from DB...")
        try:
            await load_jobs(scheduler, engine, redis)
        except Exception as exc:
            logger.error("Failed to reload jobs: %s", exc)


async def main() -> None:
    engine = create_async_engine(settings.database_url, echo=False)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)

    scheduler = AsyncIOScheduler()

    # Initial load
    logger.info("Loading scheduled jobs...")
    try:
        await load_jobs(scheduler, engine, redis)
    except Exception as exc:
        logger.error("Initial job load failed: %s — will retry on next poll", exc)

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))

    try:
        await _periodic_reload(scheduler, engine, redis, settings.poll_interval_seconds)
    finally:
        scheduler.shutdown(wait=False)
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
