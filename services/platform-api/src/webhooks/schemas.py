import uuid
from datetime import datetime
from pydantic import BaseModel


class WebhookConfigCreate(BaseModel):
    workflow_id: uuid.UUID
    name: str
    payload_mapping: dict = {}


class WebhookConfigRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    workflow_id: uuid.UUID
    name: str
    payload_mapping: dict
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class WebhookConfigPublic(WebhookConfigRead):
    """Returned only on creation and secret rotation — includes plaintext secret."""
    secret: str
