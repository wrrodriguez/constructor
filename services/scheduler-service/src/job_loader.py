# src/job_loader.py
import logging
import uuid
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

_QUERY = text(
    "SELECT id, tenant_id, trigger_config "
    "FROM process_definitions "
    "WHERE (trigger_config->>'type') = 'schedule'"
)


def _build_trigger(trigger_config: dict) -> CronTrigger | IntervalTrigger | None:
    if "cron" in trigger_config:
        try:
            return CronTrigger.from_crontab(
                trigger_config["cron"],
                timezone=trigger_config.get("timezone", "UTC"),
            )
        except Exception as exc:
            logger.warning("Invalid cron '%s': %s — skipping", trigger_config["cron"], exc)
            return None
    if "interval_minutes" in trigger_config:
        return IntervalTrigger(minutes=int(trigger_config["interval_minutes"]))
    logger.warning("Unknown schedule config %s — skipping", trigger_config)
    return None


async def load_jobs(
    scheduler: AsyncIOScheduler,
    engine: AsyncEngine,
    redis: Redis,
) -> None:
    """Sync APScheduler jobs with the DB — incremental, preserves existing job timers.

    - New workflows in DB → job added
    - Workflows removed from DB → job removed
    - Existing jobs kept as-is so their interval/cron timers are not reset
    """
    from src.dispatcher import dispatch

    async with engine.begin() as conn:
        result = await conn.execute(_QUERY)
        rows = list(result)

    db_ids = {str(row.id) for row in rows}
    existing_ids = {job.id for job in scheduler.get_jobs()}

    # Remove jobs no longer in DB
    for job in scheduler.get_jobs():
        if job.id not in db_ids:
            job.remove()
            logger.info("Removed job for workflow %s (no longer scheduled)", job.id)

    # Add only new jobs (skip existing to preserve trigger timers)
    for row in rows:
        if str(row.id) in existing_ids:
            continue
        trigger = _build_trigger(row.trigger_config)
        if trigger is None:
            continue
        scheduler.add_job(
            dispatch,
            trigger=trigger,
            args=[row.id, row.tenant_id, redis],
            id=str(row.id),
            replace_existing=True,
        )
        logger.info("Registered job for workflow %s", row.id)

    logger.info("Loaded %d scheduled jobs", len(scheduler.get_jobs()))
