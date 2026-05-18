# src/users/schemas.py
import uuid
from pydantic import BaseModel, EmailStr


class UserCreate(BaseModel):
    email: EmailStr
    password: str
    role: str = "developer"


class UserRead(BaseModel):
    id: uuid.UUID
    email: str
    tenant_id: uuid.UUID
    is_active: bool

    model_config = {"from_attributes": True}


class UserMe(UserRead):
    roles: list[str]
