# src/tenants/schemas.py
import uuid
from pydantic import BaseModel, Field


class TenantCreate(BaseModel):
    name: str
    slug: str
    config: dict = Field(default_factory=dict)


class TenantRead(BaseModel):
    id: uuid.UUID
    name: str
    slug: str
    status: str
    config: dict

    model_config = {"from_attributes": True}
