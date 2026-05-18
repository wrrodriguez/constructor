# src/auth/router.py
from fastapi import APIRouter, Depends, HTTPException, Response, status

from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.auth.service import authenticate_user, create_tokens
from src.auth.schemas import LoginRequest, TokenResponse
from src.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

REFRESH_COOKIE = "refresh_token"


@router.post("/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    user = await authenticate_user(db, body.email, body.password, body.tenant_slug)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )
    token_response, raw_refresh = await create_tokens(db, user)
    await db.commit()
    response.set_cookie(
        key=REFRESH_COOKIE,
        value=raw_refresh,
        httponly=True,
        secure=settings.environment != "development",
        samesite="strict",
        max_age=settings.jwt_refresh_token_expire_days * 86400,
    )
    return token_response
