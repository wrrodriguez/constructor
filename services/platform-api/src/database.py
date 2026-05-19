# src/database.py
from collections.abc import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from redis.asyncio import Redis as AsyncRedis
from src.config import settings


engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionFactory = async_sessionmaker(engine, expire_on_commit=False)

_redis_client: AsyncRedis | None = None


async def get_redis() -> AsyncRedis:
    global _redis_client
    if _redis_client is None:
        _redis_client = AsyncRedis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Sesión de DB sin tenant scope. Usar solo en login y operaciones de super admin."""
    async with AsyncSessionFactory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def get_tenant_db(tenant_id: str) -> AsyncGenerator[AsyncSession, None]:
    """Lower-level session factory — use get_scoped_db from auth/dependencies.py in authenticated endpoints.
    Injects tenant_id into the PostgreSQL session so Row-Level Security policies filter all queries automatically.
    """
    async with AsyncSessionFactory() as session:
        await session.execute(
            text("SET app.current_tenant = :tenant_id"),
            {"tenant_id": tenant_id}
        )
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            try:
                await session.execute(text("RESET app.current_tenant"))
            except Exception:
                pass
