# src/webhooks/router.py
import json
import uuid
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_scoped_db
from src.auth.schemas import CurrentUser
from src.database import get_db
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.webhooks.schemas import WebhookConfigCreate, WebhookConfigRead, WebhookConfigPublic
from src.webhooks.service import (
    create_webhook, list_webhooks, deactivate_webhook, rotate_secret, get_webhook,
    verify_signature, apply_payload_mapping,
)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# ── Management endpoints (JWT required) ──────────────────────────────────────

@router.post("", response_model=WebhookConfigPublic, status_code=status.HTTP_201_CREATED)
async def create(
    body: WebhookConfigCreate,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    webhook, plain_secret = await create_webhook(db, body, current_user.tenant_id)
    data = WebhookConfigPublic.model_validate(webhook)
    data.secret = plain_secret
    return data


@router.get("", response_model=list[WebhookConfigRead])
async def list_all(
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    return await list_webhooks(db, current_user.tenant_id)


@router.delete("/{webhook_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(
    webhook_id: uuid.UUID,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    deleted = await deactivate_webhook(db, webhook_id, current_user.tenant_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Webhook not found")


@router.post("/{webhook_id}/rotate", response_model=WebhookConfigPublic)
async def rotate(
    webhook_id: uuid.UUID,
    db: AsyncSession = Depends(get_scoped_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    result = await rotate_secret(db, webhook_id, current_user.tenant_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Webhook not found")
    webhook, plain_secret = result
    data = WebhookConfigPublic.model_validate(webhook)
    data.secret = plain_secret
    return data


# ── Public trigger endpoint (no JWT — HMAC auth) ───────────────────────────

@router.post("/{webhook_id}/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger(
    webhook_id: uuid.UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    from src.executions.service import create_execution
    from src.executions.schemas import ExecutionCreate

    body_bytes = await request.body()
    signature = request.headers.get("X-Constructor-Signature", "")

    webhook = await get_webhook(db, webhook_id)
    if not webhook or not webhook.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Webhook not found")

    if not verify_signature(webhook.secret, body_bytes, signature):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

    body = json.loads(body_bytes) if body_bytes else {}
    context = apply_payload_mapping(body, webhook.payload_mapping)

    # Set RLS context to webhook's tenant before calling create_execution
    await db.execute(
        text("SELECT set_config('app.current_tenant', :tid, false)"),
        {"tid": str(webhook.tenant_id)},
    )

    data = ExecutionCreate(
        process_definition_id=webhook.workflow_id,
        context=context,
    )
    execution = await create_execution(
        db, data, webhook.tenant_id, triggered_by=None, trigger_type="webhook"
    )
    return {"execution_id": str(execution.id), "status": execution.status}
