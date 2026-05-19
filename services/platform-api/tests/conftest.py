# tests/conftest.py
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.auth.service import hash_password
from src.main import app
from src.rbac.models import Role, UserRole
from src.tenants.models import Tenant
from src.users.models import User


@pytest.fixture(scope="session")
def redis_container():
    from testcontainers.redis import RedisContainer
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture(scope="session")
def postgres_container():
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="session")
async def db_engine(postgres_container):
    sync_url = postgres_container.get_connection_url()
    async_url = sync_url.replace("postgresql+psycopg2", "postgresql+asyncpg")

    # Run Alembic migrations via sync URL (absolute path for robustness)
    alembic_ini = Path(__file__).parent.parent / "alembic.ini"
    alembic_cfg = Config(str(alembic_ini))
    alembic_cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(alembic_cfg, "head")

    engine = create_async_engine(async_url)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine) -> AsyncSession:
    """Function-scoped session with rollback for direct service testing."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def seeded_user(db_engine) -> dict:
    """Creates a tenant + developer user. Uses its own session and commits."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        tenant = Tenant(
            name="Test Corp",
            slug=f"test-corp-{uuid.uuid4().hex[:8]}",
        )
        session.add(tenant)
        await session.flush()

        user = User(
            tenant_id=tenant.id,
            email=f"dev-{uuid.uuid4().hex[:8]}@test-corp.com",
            hashed_password=hash_password("secret123"),
        )
        session.add(user)
        await session.flush()

        role = await session.scalar(select(Role).where(Role.name == "developer"))
        assert role is not None, "developer role not found — check migration 002 seeds roles"

        session.add(UserRole(user_id=user.id, tenant_id=tenant.id, role_id=role.id))
        await session.commit()

        return {
            "id": str(user.id),
            "email": user.email,
            "password": "secret123",
            "tenant_slug": tenant.slug,
            "tenant_id": str(tenant.id),
        }


@pytest_asyncio.fixture
async def client(db_engine) -> AsyncClient:
    """HTTP client with get_db overridden to create fresh sessions from the test engine."""
    from src.database import get_db

    async def override_get_db():
        factory = async_sessionmaker(db_engine, expire_on_commit=False)
        async with factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest_asyncio.fixture
async def db_redis(redis_container):
    """Redis client conectado al TestContainer para tests e2e."""
    from redis.asyncio import Redis as AsyncRedis
    client = AsyncRedis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def e2e_client(db_engine, db_redis):
    """HTTP client que parchea Redis y DB para tests e2e.

    ASGITransport no dispara el lifespan de FastAPI, así que arrancamos
    el result consumer manualmente para que los Futures se resuelvan.
    """
    import asyncio as _asyncio
    import src.database as db_module
    import src.executions.service as service_module
    from src.executions.redis_consumer import start_result_consumer

    # Patch Redis so executor and result consumer use the test container
    original_redis = db_module._redis_client
    db_module._redis_client = db_redis

    test_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    original_factory = service_module.AsyncSessionFactory
    service_module.AsyncSessionFactory = test_factory

    from src.database import get_db

    async def override_get_db():
        factory = async_sessionmaker(db_engine, expire_on_commit=False)
        async with factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db

    # Start result consumer manually (ASGITransport doesn't fire lifespan)
    consumer_task = _asyncio.create_task(start_result_consumer())
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except _asyncio.CancelledError:
            pass
        app.dependency_overrides.pop(get_db, None)
        db_module._redis_client = original_redis
        service_module.AsyncSessionFactory = original_factory


@pytest_asyncio.fixture
async def seeded_workflow_definition(db_engine, seeded_user) -> uuid.UUID:
    """Crea ProcessDefinition con un agent step para tests e2e."""
    from src.workflows.models import ProcessDefinition

    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        tenant = await session.scalar(
            select(Tenant).where(Tenant.slug == seeded_user["tenant_slug"])
        )
        user = await session.scalar(
            select(User).where(User.email == seeded_user["email"])
        )

        defn = ProcessDefinition(
            tenant_id=tenant.id,
            name="E2E Test Workflow",
            trigger_config={"type": "manual"},
            steps=[{
                "id": "step_agent",
                "type": "agent",
                "config": {"task_type": "default", "prompt": "Say hello", "tools_allowed": []},
                "next": None,
                "timeout_seconds": 30,
            }],
            on_error="stop",
            created_by=user.id,
        )
        session.add(defn)
        await session.commit()
        return defn.id
