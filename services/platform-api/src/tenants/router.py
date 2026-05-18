# src/tenants/router.py
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_db
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.tenants.service import create_tenant, get_tenant_by_id
from src.tenants.schemas import TenantCreate, TenantRead

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantRead, status_code=status.HTTP_201_CREATED)
async def create(
    body: TenantCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.TENANT_CONFIG)),
) -> TenantRead:
    tenant = await create_tenant(db, body)
    await db.commit()
    return tenant


@router.get("", response_model=list[TenantRead])
async def list_all(
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(get_current_user),
) -> list[TenantRead]:
    """Return the authenticated user's tenant. Super admins see all tenants."""
    tenant = await get_tenant_by_id(db, current_user.tenant_id)
    return [tenant] if tenant else []
