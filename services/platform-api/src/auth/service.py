# src/auth/service.py
import uuid
import hashlib
from datetime import timedelta, datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from passlib.context import CryptContext

from src.users.models import User, RefreshToken
from src.tenants.models import Tenant
from src.rbac.models import UserRole, Role
from src.auth.jwt import create_access_token
from src.auth.schemas import TokenResponse
from src.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Lazily computed so bcrypt backend initialisation is deferred to runtime.
# Always run verify_password against this hash when the user is not found to
# prevent timing-based user-enumeration attacks.
_DUMMY_HASH: str | None = None


def _get_dummy_hash() -> str:
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        _DUMMY_HASH = pwd_context.hash("dummy")
    return _DUMMY_HASH


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def authenticate_user(
    db: AsyncSession, email: str, password: str, tenant_slug: str
) -> User | None:
    tenant = await db.scalar(select(Tenant).where(Tenant.slug == tenant_slug))
    user: User | None = None
    if tenant:
        user = await db.scalar(
            select(User).where(
                User.email == email,
                User.tenant_id == tenant.id,
                User.is_active.is_(True),
            )
        )
    # Always run bcrypt to prevent timing-based user enumeration
    candidate_hash = user.hashed_password if user else _get_dummy_hash()
    if not verify_password(password, candidate_hash):
        return None
    return user


async def get_user_roles(
    db: AsyncSession, user_id: uuid.UUID, tenant_id: uuid.UUID
) -> list[str]:
    result = await db.execute(
        select(Role.name)
        .join(UserRole, UserRole.role_id == Role.id)
        .where(UserRole.user_id == user_id, UserRole.tenant_id == tenant_id)
    )
    return [row[0] for row in result.all()]


async def create_tokens(
    db: AsyncSession, user: User
) -> tuple[TokenResponse, str]:
    roles = await get_user_roles(db, user.id, user.tenant_id)
    access_token = create_access_token(
        {"sub": str(user.id), "tenant_id": str(user.tenant_id), "roles": roles},
        expires_delta=timedelta(minutes=settings.jwt_access_token_expire_minutes),
    )
    raw_refresh = str(uuid.uuid4())
    token_hash = hashlib.sha256(raw_refresh.encode()).hexdigest()
    db.add(RefreshToken(
        user_id=user.id,
        tenant_id=user.tenant_id,
        token_hash=token_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.jwt_refresh_token_expire_days),
    ))
    await db.flush()
    return TokenResponse(
        access_token=access_token,
        expires_in=settings.jwt_access_token_expire_minutes * 60,
    ), raw_refresh
