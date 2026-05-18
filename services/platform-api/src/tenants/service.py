# src/tenants/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.tenants.models import Tenant
from src.tenants.schemas import TenantCreate


async def create_tenant(db: AsyncSession, data: TenantCreate) -> Tenant:
    tenant = Tenant(name=data.name, slug=data.slug, config=data.config)
    db.add(tenant)
    await db.flush()
    await db.refresh(tenant)
    return tenant


async def get_tenant_by_id(db: AsyncSession, tenant_id: uuid.UUID) -> Tenant | None:
    return await db.scalar(select(Tenant).where(Tenant.id == tenant_id))


async def list_tenants(db: AsyncSession) -> list[Tenant]:
    result = await db.execute(select(Tenant))
    return list(result.scalars().all())
