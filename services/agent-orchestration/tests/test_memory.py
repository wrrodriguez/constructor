# tests/test_memory.py
import pytest
import pytest_asyncio
import uuid
from testcontainers.redis import RedisContainer
from testcontainers.postgres import PostgresContainer
from redis.asyncio import Redis
from langchain_core.messages import HumanMessage, AIMessage
from src.memory.short_term import ShortTermMemory
from src.memory.long_term import LongTermMemory


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest_asyncio.fixture(scope="module")
async def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest_asyncio.fixture(scope="module")
async def pg_engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    url = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql+asyncpg"
    )
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agent_execution_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id UUID NOT NULL,
                execution_id UUID NOT NULL,
                step_id VARCHAR(100) NOT NULL,
                task_id UUID NOT NULL,
                model_used VARCHAR(100) NOT NULL,
                tokens_used INTEGER NOT NULL DEFAULT 0,
                iterations INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_short_term_save_and_load(redis_client):
    mem = ShortTermMemory(redis_client)
    tenant_id = str(uuid.uuid4())
    exec_id = str(uuid.uuid4())
    messages = [HumanMessage(content="hello"), AIMessage(content="world")]
    await mem.save(tenant_id, exec_id, messages, ttl_seconds=60)
    loaded = await mem.load(tenant_id, exec_id)
    assert len(loaded) == 2
    assert loaded[0].content == "hello"
    assert loaded[1].content == "world"


@pytest.mark.asyncio
async def test_short_term_load_missing_returns_empty(redis_client):
    mem = ShortTermMemory(redis_client)
    result = await mem.load("no-tenant", "no-exec")
    assert result == []


@pytest.mark.asyncio
async def test_long_term_write(pg_engine):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from sqlalchemy import text
    mem = LongTermMemory(pg_engine)
    exec_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    task_id = uuid.uuid4()
    await mem.write(
        tenant_id=tenant_id, execution_id=exec_id,
        step_id="step_1", task_id=task_id,
        model_used="claude-sonnet-4-6", tokens_used=150,
        iterations=2, status="completed",
    )
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as session:
        row = await session.execute(
            text("SELECT model_used, tokens_used FROM agent_execution_logs WHERE execution_id = :eid"),
            {"eid": str(exec_id)},
        )
        result = row.fetchone()
    assert result is not None
    assert result[0] == "claude-sonnet-4-6"
    assert result[1] == 150
