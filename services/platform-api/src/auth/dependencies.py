# src/auth/dependencies.py
import uuid
from typing import AsyncGenerator

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.auth.jwt import decode_access_token
from src.auth.schemas import CurrentUser
from src.users.models import User
from src.database import get_db

bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> CurrentUser:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        payload = decode_access_token(credentials.credentials)
        user_id = uuid.UUID(payload["sub"])
        tenant_id = uuid.UUID(payload["tenant_id"])
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    user = await db.scalar(
        select(User).where(
            User.id == user_id,
            User.tenant_id == tenant_id,
            User.is_active.is_(True),
        )
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
        )

    return CurrentUser(
        id=user_id,
        email=user.email,
        tenant_id=tenant_id,
        roles=payload.get("roles", []),
    )


async def get_scoped_db(
    current_user: CurrentUser = Depends(get_current_user),
) -> AsyncGenerator[AsyncSession, None]:
    """Tenant-scoped DB session with RLS. Use in all authenticated endpoints.

    Note: Uses unscoped AsyncSessionFactory directly to ensure proper exception/rollback handling.
    The tenant scope is injected via SET app.current_tenant before yielding.
    """
    from sqlalchemy import text as sql_text
    from src.database import AsyncSessionFactory

    async with AsyncSessionFactory() as session:
        await session.execute(
            sql_text("SET app.current_tenant = :tenant_id"),
            {"tenant_id": str(current_user.tenant_id)},
        )
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            try:
                await session.execute(sql_text("RESET app.current_tenant"))
            except Exception:
                pass
