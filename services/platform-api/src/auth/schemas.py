# src/auth/schemas.py
import uuid
from pydantic import BaseModel, EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    tenant_slug: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds


class RefreshRequest(BaseModel):
    refresh_token: str


class CurrentUser(BaseModel):
    id: uuid.UUID
    email: EmailStr
    tenant_id: uuid.UUID
    roles: list[str]
