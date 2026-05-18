# src/users/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.users.models import User
from src.rbac.models import Role, UserRole
from src.auth.service import hash_password
from src.users.schemas import UserCreate
from src.audit.service import log_action


async def create_user(
    db: AsyncSession, data: UserCreate, tenant_id: uuid.UUID
) -> User:
    role = await db.scalar(select(Role).where(Role.name == data.role))
    if not role:
        raise ValueError(f"Role '{data.role}' not found")
    user = User(
        tenant_id=tenant_id,
        email=data.email,
        hashed_password=hash_password(data.password),
    )
    db.add(user)
    await db.flush()
    db.add(UserRole(user_id=user.id, tenant_id=tenant_id, role_id=role.id))
    await db.flush()
    await db.refresh(user)
    await log_action(
        db,
        tenant_id=tenant_id,
        action="user.created",
        resource_type="user",
        resource_id=str(user.id),
    )
    return user


async def get_user_by_id(db: AsyncSession, user_id: uuid.UUID) -> User | None:
    return await db.scalar(select(User).where(User.id == user_id))
