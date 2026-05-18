# src/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import text
from src.config import settings


engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionFactory = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    """FastAPI dependency: sesión de DB sin tenant (solo para auth y super admin)."""
    async with AsyncSessionFactory() as session:
        yield session


async def get_tenant_db(tenant_id: str):
    """FastAPI dependency: sesión de DB con tenant_id inyectado en la sesión PostgreSQL.
    RLS usa este valor para filtrar todas las queries automáticamente.
    """
    async with AsyncSessionFactory() as session:
        await session.execute(
            text("SET app.current_tenant = :tenant_id"),
            {"tenant_id": tenant_id}
        )
        try:
            yield session
        finally:
            await session.execute(text("RESET app.current_tenant"))
