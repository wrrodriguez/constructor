import hmac
import hashlib
import secrets
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import jmespath

from src.webhooks.models import WebhookConfig
from src.webhooks.schemas import WebhookConfigCreate


def verify_signature(secret: str, body: bytes, signature: str) -> bool:
    """Timing-safe HMAC-SHA256 verification. signature must be 'sha256=<hex>'."""
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def apply_payload_mapping(body: dict, mapping: dict) -> dict:
    """Apply JMESPath expressions to extract fields from webhook body into context.

    mapping: {"context_key": "jmespath.expression"}
    Fields with no match are silently omitted.
    JMESPath syntax: "data.id" (no $ prefix).
    """
    context = {}
    for key, expression in mapping.items():
        value = jmespath.search(expression, body)
        if value is not None:
            context[key] = value
    return context


def _generate_secret() -> str:
    return secrets.token_hex(32)


async def create_webhook(
    db: AsyncSession, data: WebhookConfigCreate, tenant_id: uuid.UUID
) -> tuple[WebhookConfig, str]:
    """Returns (webhook_config, plaintext_secret). Secret shown only once."""
    plain_secret = _generate_secret()
    webhook = WebhookConfig(
        tenant_id=tenant_id,
        workflow_id=data.workflow_id,
        name=data.name,
        secret=plain_secret,
        payload_mapping=data.payload_mapping,
    )
    db.add(webhook)
    await db.commit()
    await db.refresh(webhook)
    return webhook, plain_secret


async def list_webhooks(db: AsyncSession, tenant_id: uuid.UUID) -> list[WebhookConfig]:
    result = await db.execute(
        select(WebhookConfig)
        .where(WebhookConfig.tenant_id == tenant_id)
        .order_by(WebhookConfig.created_at.desc())
    )
    return list(result.scalars().all())


async def get_webhook(db: AsyncSession, webhook_id: uuid.UUID) -> WebhookConfig | None:
    return await db.scalar(select(WebhookConfig).where(WebhookConfig.id == webhook_id))


async def deactivate_webhook(db: AsyncSession, webhook_id: uuid.UUID, tenant_id: uuid.UUID) -> bool:
    webhook = await db.scalar(
        select(WebhookConfig).where(
            WebhookConfig.id == webhook_id, WebhookConfig.tenant_id == tenant_id
        )
    )
    if not webhook:
        return False
    webhook.is_active = False
    await db.commit()
    return True


async def rotate_secret(
    db: AsyncSession, webhook_id: uuid.UUID, tenant_id: uuid.UUID
) -> tuple[WebhookConfig, str] | None:
    webhook = await db.scalar(
        select(WebhookConfig).where(
            WebhookConfig.id == webhook_id, WebhookConfig.tenant_id == tenant_id
        )
    )
    if not webhook:
        return None
    plain_secret = _generate_secret()
    webhook.secret = plain_secret
    await db.commit()
    await db.refresh(webhook)
    return webhook, plain_secret
