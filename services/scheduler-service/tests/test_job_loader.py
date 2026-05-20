# tests/test_job_loader.py
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


def make_row(wf_id, tenant_id, trigger_config):
    row = MagicMock()
    row.id = wf_id
    row.tenant_id = tenant_id
    row.trigger_config = trigger_config
    return row


async def test_load_jobs_registers_cron_trigger(scheduler, mock_engine):
    from src.job_loader import load_jobs
    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    redis_mock = AsyncMock()
    rows = [make_row(wf_id, tenant_id, {"type": "schedule", "cron": "0 9 * * MON", "timezone": "UTC"})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    await load_jobs(scheduler, mock_engine, redis_mock)

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    assert isinstance(jobs[0].trigger, CronTrigger)
    assert jobs[0].args == (wf_id, tenant_id, redis_mock)
    assert jobs[0].id == str(wf_id)


async def test_load_jobs_registers_interval_trigger(scheduler, mock_engine):
    from src.job_loader import load_jobs
    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    redis_mock = AsyncMock()
    rows = [make_row(wf_id, tenant_id, {"type": "schedule", "interval_minutes": 15})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    await load_jobs(scheduler, mock_engine, redis_mock)

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    assert isinstance(jobs[0].trigger, IntervalTrigger)
    assert jobs[0].args == (wf_id, tenant_id, redis_mock)
    assert jobs[0].id == str(wf_id)


async def test_load_jobs_skips_invalid_cron(scheduler, mock_engine, caplog):
    from src.job_loader import load_jobs
    import logging
    rows = [make_row(uuid.uuid4(), uuid.uuid4(), {"type": "schedule", "cron": "not-a-cron"})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    with caplog.at_level(logging.WARNING):
        await load_jobs(scheduler, mock_engine, AsyncMock())

    assert scheduler.get_jobs() == []
    assert "invalid" in caplog.text.lower() or "skip" in caplog.text.lower()
