# src/users/router.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.database import get_db
from src.users.schemas import UserCreate, UserRead, UserMe
from src.users.service import create_user

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserMe)
async def me(
    current_user: CurrentUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserMe:
    from src.users.service import get_user_by_id
    user = await get_user_by_id(db, current_user.id)
    return UserMe(
        id=current_user.id,
        email=current_user.email,
        tenant_id=current_user.tenant_id,
        is_active=user.is_active if user else False,
        roles=current_user.roles,
    )


@router.post("", response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def create(
    body: UserCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.USER_MANAGE)),
) -> UserRead:
    try:
        user = await create_user(db, body, current_user.tenant_id)
        await db.commit()
        return user
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
