# Scheduled & Webhook Triggers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add scheduled (cron/interval) and webhook-triggered workflow executions, with a new `scheduler-service` container and a public HMAC-verified webhook endpoint in Platform API.

**Architecture:** Webhooks land on `POST /webhooks/{webhook_id}/trigger` in Platform API — HMAC-SHA256 signature verified, JMESPath payload mapped to context, `create_execution()` reused. The `scheduler-service` reads `process_definitions` with `trigger_config.type=schedule`, registers APScheduler jobs with Redis distributed locking (SET NX EX), and fires via `POST /internal/executions` protected by a shared secret.

**Tech Stack:** Platform API — jmespath 1.x (already in deps), passlib[bcrypt] (already in deps), FastAPI (existing). Scheduler Service — APScheduler 3.x, httpx[asyncio], sqlalchemy[asyncio], asyncpg, redis[asyncio], pydantic-settings.

---

## File Map

**Platform API — new files:**
- `services/platform-api/migrations/versions/006_triggers.py` — `webhook_configs` table + `trigger_type` on `process_executions`
- `services/platform-api/src/webhooks/models.py` — `WebhookConfig` SQLAlchemy model
- `services/platform-api/src/webhooks/schemas.py` — Pydantic schemas
- `services/platform-api/src/webhooks/service.py` — CRUD + HMAC verify + JMESPath mapping
- `services/platform-api/src/webhooks/router.py` — management routes + trigger endpoint
- `services/platform-api/src/internal/router.py` — `POST /internal/executions`
- `services/platform-api/tests/test_webhook_management.py`
- `services/platform-api/tests/test_webhook_trigger.py`
- `services/platform-api/tests/test_internal_endpoint.py`

**Platform API — modified files:**
- `services/platform-api/src/workflows/models.py` — add `trigger_type` to `ProcessExecution`
- `services/platform-api/src/executions/schemas.py` — add `trigger_type` to `ExecutionRead`
- `services/platform-api/src/executions/service.py` — add `trigger_type` param to `create_execution`
- `services/platform-api/src/config.py` — add `internal_secret`
- `services/platform-api/src/main.py` — include webhooks and internal routers

**Scheduler Service — new:**
- `services/scheduler-service/pyproject.toml`
- `services/scheduler-service/Dockerfile`
- `services/scheduler-service/src/__init__.py`
- `services/scheduler-service/src/config.py`
- `services/scheduler-service/src/job_loader.py`
- `services/scheduler-service/src/dispatcher.py`
- `services/scheduler-service/src/worker.py`
- `services/scheduler-service/tests/__init__.py`
- `services/scheduler-service/tests/conftest.py`
- `services/scheduler-service/tests/test_job_loader.py`
- `services/scheduler-service/tests/test_dispatcher.py`

**Modified:**
- `docker-compose.yml` — add `scheduler-service`, add `INTERNAL_SECRET` to platform-api

---

## Context for implementers

**Important: webhook secrets are stored as plaintext** (not bcrypt). HMAC-SHA256 requires the original plaintext secret to compute the signature — bcrypt is one-way and cannot be used. The secret is never returned after initial creation/rotation. In production, encrypt at rest using DB-level encryption.

**JMESPath syntax** (not JSONPath): mapping `{"policy_id": "data.id"}` extracts `body["data"]["id"]` and sets `context["policy_id"]`. No `$` prefix. Library: `jmespath` (already in deps).

**`trigger_type`** must be added as a column to `process_executions` with default `"manual"`. The `create_execution()` function gains an optional `trigger_type: str = "manual"` parameter.

**Internal endpoint** bypasses JWT. The DB role (constructor/superuser in dev) can query `process_definitions` without RLS to get the tenant_id, then sets the RLS context before calling `create_execution()`.

---

## Task 1: Migration 006_triggers

**Files:**
- Create: `services/platform-api/migrations/versions/006_triggers.py`

- [ ] **Step 1: Write the migration**

```python
# migrations/versions/006_triggers.py
from alembic import op
import sqlalchemy as sa
import uuid as _uuid

revision = "c3d4e5f6a7b8"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add trigger_type to process_executions (default "manual" for existing rows)
    op.add_column(
        "process_executions",
        sa.Column("trigger_type", sa.String(20), nullable=False, server_default="manual"),
    )

    # webhook_configs table
    op.create_table(
        "webhook_configs",
        sa.Column("id", sa.UUID(), nullable=False, default=_uuid.uuid4),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("workflow_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        sa.Column("payload_mapping", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["workflow_id"], ["process_definitions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute("ALTER TABLE webhook_configs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE webhook_configs FORCE ROW LEVEL SECURITY")
    op.execute("""
        CREATE POLICY tenant_isolation ON webhook_configs
        USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
    """)


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON webhook_configs")
    op.execute("ALTER TABLE webhook_configs DISABLE ROW LEVEL SECURITY")
    op.drop_table("webhook_configs")
    op.drop_column("process_executions", "trigger_type")
```

- [ ] **Step 2: Verify Alembic sees the migration**

```bash
cd services/platform-api
alembic heads
```
Expected: shows `c3d4e5f6a7b8` as the new head.

- [ ] **Step 3: Commit**

```bash
git add services/platform-api/migrations/versions/006_triggers.py
git commit -m "feat: migration 006 — webhook_configs table and trigger_type on executions"
```

---

## Task 2: WebhookConfig model + ProcessExecution update + schemas

**Files:**
- Create: `services/platform-api/src/webhooks/models.py`
- Create: `services/platform-api/src/webhooks/schemas.py`
- Modify: `services/platform-api/src/workflows/models.py`
- Modify: `services/platform-api/src/executions/schemas.py`

- [ ] **Step 1: Write the failing test for WebhookConfig model**

```python
# tests/test_webhook_management.py
import pytest

def test_webhook_config_model_has_expected_fields():
    from src.webhooks.models import WebhookConfig
    cols = {c.key for c in WebhookConfig.__table__.columns}
    assert {"id", "tenant_id", "workflow_id", "name", "secret",
            "payload_mapping", "is_active", "created_at"} <= cols
```

Run: `cd services/platform-api && pytest tests/test_webhook_management.py::test_webhook_config_model_has_expected_fields -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.webhooks'`

- [ ] **Step 2: Create `src/webhooks/models.py`**

```python
# src/webhooks/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, Text, Boolean, JSON, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class WebhookConfig(Base):
    __tablename__ = "webhook_configs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    workflow_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("process_definitions.id"), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[str] = mapped_column(Text, nullable=False)
    payload_mapping: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
```

- [ ] **Step 3: Create `src/webhooks/schemas.py`**

```python
# src/webhooks/schemas.py
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
```

- [ ] **Step 4: Add `trigger_type` to `ProcessExecution` model**

In `src/workflows/models.py`, add after the `triggered_by` field:

```python
trigger_type: Mapped[str] = mapped_column(String(20), default="manual")
```

Full updated `ProcessExecution` class:

```python
class ProcessExecution(Base):
    __tablename__ = "process_executions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    process_definition_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("process_definitions.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(30), default="pending")
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    current_step_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    trigger_type: Mapped[str] = mapped_column(String(20), default="manual")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
```

- [ ] **Step 5: Add `trigger_type` to `ExecutionRead` schema**

In `src/executions/schemas.py`, add `trigger_type: str = "manual"` to `ExecutionRead`:

```python
class ExecutionRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    process_definition_id: uuid.UUID
    status: str
    current_step_id: str | None
    context: dict
    triggered_by: uuid.UUID | None
    trigger_type: str
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}
```

- [ ] **Step 6: Run test — it should pass now**

Run: `cd services/platform-api && pytest tests/test_webhook_management.py::test_webhook_config_model_has_expected_fields -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add services/platform-api/src/webhooks/models.py \
        services/platform-api/src/webhooks/schemas.py \
        services/platform-api/src/workflows/models.py \
        services/platform-api/src/executions/schemas.py \
        services/platform-api/tests/test_webhook_management.py
git commit -m "feat: WebhookConfig model, trigger_type on ProcessExecution, and schemas"
```

---

## Task 3: Webhook service (HMAC + JMESPath + CRUD)

**Files:**
- Create: `services/platform-api/src/webhooks/__init__.py` (empty)
- Create: `services/platform-api/src/webhooks/service.py`
- Modify: `services/platform-api/tests/test_webhook_management.py`

- [ ] **Step 1: Write failing tests for webhook service functions**

Append to `tests/test_webhook_management.py`:

```python
import hmac as _hmac
import hashlib
import secrets

def test_verify_signature_valid():
    from src.webhooks.service import verify_signature
    body = b'{"data": {"id": "abc"}}'
    secret = "mysecret"
    sig = "sha256=" + _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is True


def test_verify_signature_invalid():
    from src.webhooks.service import verify_signature
    body = b'{"data": {"id": "abc"}}'
    assert verify_signature("right", body, "sha256=wronghex") is False


def test_apply_payload_mapping_extracts_fields():
    from src.webhooks.service import apply_payload_mapping
    body = {"data": {"id": "policy-123", "status": "active"}}
    mapping = {"policy_id": "data.id", "policy_status": "data.status"}
    result = apply_payload_mapping(body, mapping)
    assert result == {"policy_id": "policy-123", "policy_status": "active"}


def test_apply_payload_mapping_missing_field_is_omitted():
    from src.webhooks.service import apply_payload_mapping
    body = {"data": {}}
    mapping = {"policy_id": "data.id"}
    result = apply_payload_mapping(body, mapping)
    assert result == {}


def test_apply_payload_mapping_empty_mapping():
    from src.webhooks.service import apply_payload_mapping
    assert apply_payload_mapping({"any": "data"}, {}) == {}
```

Run: `cd services/platform-api && pytest tests/test_webhook_management.py -k "verify_signature or apply_payload" -v`
Expected: FAIL with `ImportError`

- [ ] **Step 2: Create `src/webhooks/service.py`**

```python
# src/webhooks/service.py
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
```

- [ ] **Step 3: Create empty `src/webhooks/__init__.py`**

```bash
touch services/platform-api/src/webhooks/__init__.py
```

- [ ] **Step 4: Run tests — should pass**

Run: `cd services/platform-api && pytest tests/test_webhook_management.py -k "verify_signature or apply_payload" -v`
Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/src/webhooks/__init__.py \
        services/platform-api/src/webhooks/service.py \
        services/platform-api/tests/test_webhook_management.py
git commit -m "feat: webhook service — HMAC verification, JMESPath mapping, and CRUD"
```

---

## Task 4: Webhook management router (CRUD)

**Files:**
- Create: `services/platform-api/src/webhooks/router.py`
- Modify: `services/platform-api/src/main.py`
- Modify: `services/platform-api/tests/test_webhook_management.py`

- [ ] **Step 1: Write failing tests for management endpoints**

Append to `tests/test_webhook_management.py`:

```python
import pytest_asyncio
from httpx import AsyncClient
import uuid as _uuid


@pytest_asyncio.fixture
async def auth_headers(client: AsyncClient, seeded_user: dict) -> dict:
    resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    assert resp.status_code == 200
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


@pytest_asyncio.fixture
async def seeded_workflow(client: AsyncClient, auth_headers: dict) -> _uuid.UUID:
    resp = await client.post("/workflows", headers=auth_headers, json={
        "name": "Webhook Test WF",
        "trigger_config": {"type": "webhook"},
        "steps": [{"id": "s1", "type": "agent", "config": {
            "task_type": "default", "prompt": "hello", "tools_allowed": []
        }, "next": None, "timeout_seconds": 30}],
        "on_error": "stop",
    })
    assert resp.status_code == 201
    return _uuid.UUID(resp.json()["id"])


async def test_create_webhook_returns_secret_once(
    client: AsyncClient, auth_headers: dict, seeded_workflow: _uuid.UUID
):
    resp = await client.post("/webhooks", headers=auth_headers, json={
        "workflow_id": str(seeded_workflow),
        "name": "My Webhook",
        "payload_mapping": {"policy_id": "data.id"},
    })
    assert resp.status_code == 201
    data = resp.json()
    assert "secret" in data
    assert len(data["secret"]) == 64  # secrets.token_hex(32)
    assert "id" in data


async def test_list_webhooks_does_not_expose_secret(
    client: AsyncClient, auth_headers: dict, seeded_workflow: _uuid.UUID
):
    await client.post("/webhooks", headers=auth_headers, json={
        "workflow_id": str(seeded_workflow), "name": "WH", "payload_mapping": {}
    })
    resp = await client.get("/webhooks", headers=auth_headers)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) >= 1
    assert "secret" not in items[0]


async def test_delete_webhook_deactivates_it(
    client: AsyncClient, auth_headers: dict, seeded_workflow: _uuid.UUID
):
    create_resp = await client.post("/webhooks", headers=auth_headers, json={
        "workflow_id": str(seeded_workflow), "name": "ToDelete", "payload_mapping": {}
    })
    wh_id = create_resp.json()["id"]
    resp = await client.delete(f"/webhooks/{wh_id}", headers=auth_headers)
    assert resp.status_code == 204


async def test_rotate_secret_returns_new_secret(
    client: AsyncClient, auth_headers: dict, seeded_workflow: _uuid.UUID
):
    create_resp = await client.post("/webhooks", headers=auth_headers, json={
        "workflow_id": str(seeded_workflow), "name": "ToRotate", "payload_mapping": {}
    })
    wh_id = create_resp.json()["id"]
    old_secret = create_resp.json()["secret"]

    rotate_resp = await client.post(f"/webhooks/{wh_id}/rotate", headers=auth_headers)
    assert rotate_resp.status_code == 200
    new_secret = rotate_resp.json()["secret"]
    assert new_secret != old_secret
```

Run: `cd services/platform-api && pytest tests/test_webhook_management.py -k "test_create or test_list or test_delete or test_rotate" -v`
Expected: FAIL with `404` (routes don't exist yet)

- [ ] **Step 2: Create `src/webhooks/router.py`**

```python
# src/webhooks/router.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from src.auth.dependencies import get_scoped_db, get_db
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.webhooks.schemas import WebhookConfigCreate, WebhookConfigRead, WebhookConfigPublic
from src.webhooks.service import (
    create_webhook, list_webhooks, deactivate_webhook, rotate_secret, get_webhook,
    verify_signature, apply_payload_mapping,
)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


# ── Management endpoints (JWT required) ────────────────────────────────────

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
```

- [ ] **Step 3: Register the router in `src/main.py`**

Add after the executions router block:

```python
from src.webhooks.router import router as webhooks_router
app.include_router(webhooks_router)
```

- [ ] **Step 4: Run tests — should pass**

Run: `cd services/platform-api && pytest tests/test_webhook_management.py -k "test_create or test_list or test_delete or test_rotate" -v`
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/src/webhooks/router.py \
        services/platform-api/src/main.py \
        services/platform-api/tests/test_webhook_management.py
git commit -m "feat: webhook management router — CRUD endpoints for webhook configs"
```

---

## Task 5: Webhook trigger endpoint + update create_execution

**Files:**
- Modify: `services/platform-api/src/executions/service.py`
- Modify: `services/platform-api/src/webhooks/router.py`
- Create: `services/platform-api/tests/test_webhook_trigger.py`

- [ ] **Step 1: Write failing tests for the trigger endpoint**

```python
# tests/test_webhook_trigger.py
import hmac as _hmac
import hashlib
import json
import uuid
import pytest
import pytest_asyncio
from httpx import AsyncClient


@pytest_asyncio.fixture
async def webhook_with_secret(client: AsyncClient, seeded_user: dict) -> dict:
    """Creates a workflow and a webhook config, returns id + secret."""
    resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    wf_resp = await client.post("/workflows", headers=headers, json={
        "name": "Webhook WF",
        "trigger_config": {"type": "webhook"},
        "steps": [{"id": "s1", "type": "agent", "config": {
            "task_type": "default", "prompt": "hello", "tools_allowed": []
        }, "next": None, "timeout_seconds": 30}],
        "on_error": "stop",
    })
    workflow_id = wf_resp.json()["id"]

    wh_resp = await client.post("/webhooks", headers=headers, json={
        "workflow_id": workflow_id,
        "name": "Trigger WH",
        "payload_mapping": {"policy_id": "data.id"},
    })
    return {"id": wh_resp.json()["id"], "secret": wh_resp.json()["secret"]}


def _sign(secret: str, body: bytes) -> str:
    return "sha256=" + _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def test_trigger_webhook_valid_signature_creates_execution(
    client: AsyncClient, webhook_with_secret: dict
):
    body = json.dumps({"data": {"id": "policy-999"}}).encode()
    sig = _sign(webhook_with_secret["secret"], body)

    resp = await client.post(
        f"/webhooks/{webhook_with_secret['id']}/trigger",
        content=body,
        headers={"Content-Type": "application/json", "X-Constructor-Signature": sig},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert "execution_id" in data
    assert data["status"] == "pending"


async def test_trigger_webhook_invalid_signature_returns_401(
    client: AsyncClient, webhook_with_secret: dict
):
    body = json.dumps({"data": {"id": "policy-999"}}).encode()
    resp = await client.post(
        f"/webhooks/{webhook_with_secret['id']}/trigger",
        content=body,
        headers={"Content-Type": "application/json", "X-Constructor-Signature": "sha256=badhex"},
    )
    assert resp.status_code == 401


async def test_trigger_nonexistent_webhook_returns_404(client: AsyncClient):
    body = b"{}"
    resp = await client.post(
        f"/webhooks/{uuid.uuid4()}/trigger",
        content=body,
        headers={"Content-Type": "application/json", "X-Constructor-Signature": "sha256=x"},
    )
    assert resp.status_code == 404


async def test_trigger_inactive_webhook_returns_404(
    client: AsyncClient, webhook_with_secret: dict, seeded_user: dict
):
    # Deactivate webhook first
    login = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    await client.delete(f"/webhooks/{webhook_with_secret['id']}", headers=headers)

    body = b"{}"
    sig = _sign(webhook_with_secret["secret"], body)
    resp = await client.post(
        f"/webhooks/{webhook_with_secret['id']}/trigger",
        content=body,
        headers={"Content-Type": "application/json", "X-Constructor-Signature": sig},
    )
    assert resp.status_code == 404
```

Run: `cd services/platform-api && pytest tests/test_webhook_trigger.py -v`
Expected: FAIL with `404` (trigger route doesn't exist yet)

- [ ] **Step 2: Update `create_execution` to accept `trigger_type`**

In `src/executions/service.py`, change the signature and execution construction:

```python
async def create_execution(
    db: AsyncSession,
    data: ExecutionCreate,
    tenant_id: uuid.UUID,
    triggered_by: uuid.UUID | None,
    trigger_type: str = "manual",
) -> ProcessExecution:
    definition = await db.scalar(
        select(ProcessDefinition).where(
            ProcessDefinition.id == data.process_definition_id,
            ProcessDefinition.tenant_id == tenant_id,
        )
    )
    if not definition:
        return None

    execution = ProcessExecution(
        tenant_id=tenant_id,
        process_definition_id=data.process_definition_id,
        context=data.context,
        triggered_by=triggered_by,
        trigger_type=trigger_type,
        status="pending",
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)

    asyncio.create_task(
        _run_workflow(execution.id, definition.id, tenant_id)
    )
    return execution
```

- [ ] **Step 3: Add the trigger endpoint to `src/webhooks/router.py`**

Add this import at the top of `router.py`:

```python
import json
from sqlalchemy import text
```

Add this endpoint after the `rotate` endpoint (before the end of the file):

```python
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

    # get_db gives a raw session without RLS — safe for public endpoint lookup
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
```

- [ ] **Step 4: Run tests — should pass**

Run: `cd services/platform-api && pytest tests/test_webhook_trigger.py -v`
Expected: 4 PASSED

Note: `test_trigger_webhook_valid_signature_creates_execution` may be slow (it actually runs the executor). If it times out, run with `-s` and verify the 202 response arrives; execution completion in the background is expected.

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `cd services/platform-api && pytest tests/ -v -m "not integration" --cov=src --cov-fail-under=50`
Expected: all existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add services/platform-api/src/executions/service.py \
        services/platform-api/src/webhooks/router.py \
        services/platform-api/tests/test_webhook_trigger.py
git commit -m "feat: webhook trigger endpoint with HMAC-SHA256 and JMESPath context mapping"
```

---

## Task 6: Internal executions endpoint

**Files:**
- Create: `services/platform-api/src/internal/__init__.py`
- Create: `services/platform-api/src/internal/router.py`
- Modify: `services/platform-api/src/config.py`
- Modify: `services/platform-api/src/main.py`
- Create: `services/platform-api/tests/test_internal_endpoint.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_internal_endpoint.py
import pytest
from httpx import AsyncClient


async def test_internal_executions_no_secret_returns_401(client: AsyncClient):
    resp = await client.post("/internal/executions", json={
        "process_definition_id": "00000000-0000-0000-0000-000000000000",
        "context": {},
    })
    assert resp.status_code == 401


async def test_internal_executions_wrong_secret_returns_401(client: AsyncClient):
    resp = await client.post(
        "/internal/executions",
        json={"process_definition_id": "00000000-0000-0000-0000-000000000000", "context": {}},
        headers={"X-Internal-Secret": "wrong"},
    )
    assert resp.status_code == 401


async def test_internal_executions_valid_secret_nonexistent_workflow_returns_404(
    client: AsyncClient,
):
    resp = await client.post(
        "/internal/executions",
        json={"process_definition_id": "00000000-0000-0000-0000-000000000000", "context": {}},
        headers={"X-Internal-Secret": "internal_dev_secret"},
    )
    assert resp.status_code == 404


async def test_internal_executions_valid_secret_creates_execution(
    client: AsyncClient, seeded_workflow_definition,
):
    resp = await client.post(
        "/internal/executions",
        json={"process_definition_id": str(seeded_workflow_definition), "context": {}},
        headers={"X-Internal-Secret": "internal_dev_secret"},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert "execution_id" in data
    assert data["status"] == "pending"
    assert data["trigger_type"] == "schedule"
```

Run: `cd services/platform-api && pytest tests/test_internal_endpoint.py -v`
Expected: FAIL with `404` (route not registered yet)

- [ ] **Step 2: Add `internal_secret` to `src/config.py`**

Add field to `Settings`:

```python
internal_secret: str = "internal_dev_secret"
```

- [ ] **Step 3: Create `src/internal/__init__.py`**

```bash
touch services/platform-api/src/internal/__init__.py
```

- [ ] **Step 4: Create `src/internal/router.py`**

```python
# src/internal/router.py
import hmac
import uuid
from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from pydantic import BaseModel

from src.config import settings
from src.database import get_db
from src.workflows.models import ProcessDefinition
from src.executions.schemas import ExecutionCreate
from src.executions.service import create_execution

router = APIRouter(prefix="/internal", tags=["internal"], include_in_schema=False)


class InternalExecutionCreate(BaseModel):
    process_definition_id: uuid.UUID
    context: dict = {}


@router.post("/executions", status_code=status.HTTP_202_ACCEPTED)
async def internal_trigger(
    body: InternalExecutionCreate,
    x_internal_secret: str | None = Header(default=None, alias="X-Internal-Secret"),
    db: AsyncSession = Depends(get_db),
):
    if not x_internal_secret or not hmac.compare_digest(x_internal_secret, settings.internal_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    # Query without RLS to get tenant_id (constructor role has superuser in dev)
    workflow = await db.scalar(
        select(ProcessDefinition).where(ProcessDefinition.id == body.process_definition_id)
    )
    if not workflow:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found")

    # Set RLS context for this tenant
    await db.execute(
        text("SELECT set_config('app.current_tenant', :tid, false)"),
        {"tid": str(workflow.tenant_id)},
    )

    data = ExecutionCreate(
        process_definition_id=body.process_definition_id,
        context=body.context,
    )
    execution = await create_execution(
        db, data, workflow.tenant_id, triggered_by=None, trigger_type="schedule"
    )
    return {
        "execution_id": str(execution.id),
        "status": execution.status,
        "trigger_type": execution.trigger_type,
    }
```

- [ ] **Step 5: Register internal router in `src/main.py`**

Add after the webhooks router block:

```python
from src.internal.router import router as internal_router
app.include_router(internal_router)
```

- [ ] **Step 6: Run tests**

Run: `cd services/platform-api && pytest tests/test_internal_endpoint.py -v`
Expected: 4 PASSED

- [ ] **Step 7: Run full suite**

Run: `cd services/platform-api && pytest tests/ -v -m "not integration" --cov=src --cov-fail-under=50`
Expected: all pass

- [ ] **Step 8: Commit**

```bash
git add services/platform-api/src/internal/__init__.py \
        services/platform-api/src/internal/router.py \
        services/platform-api/src/config.py \
        services/platform-api/src/main.py \
        services/platform-api/tests/test_internal_endpoint.py
git commit -m "feat: internal executions endpoint for scheduler-service with shared-secret auth"
```

---

## Task 7: Scheduler service scaffolding

**Files:**
- Create: `services/scheduler-service/pyproject.toml`
- Create: `services/scheduler-service/Dockerfile`
- Create: `services/scheduler-service/src/__init__.py`
- Create: `services/scheduler-service/src/config.py`
- Create: `services/scheduler-service/tests/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
# services/scheduler-service/pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "constructor-scheduler-service"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "apscheduler>=3.10.0",
    "httpx[asyncio]>=0.27.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.29.0",
    "redis[asyncio]>=5.0.0",
    "pydantic-settings>=2.3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "pytest-mock>=3.14.0",
]

[tool.hatch.build.targets.wheel]
packages = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
testpaths = ["tests"]
```

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
# services/scheduler-service/Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .
COPY src/ src/

CMD ["python", "-m", "src.worker"]
```

- [ ] **Step 3: Create `src/__init__.py` and `tests/__init__.py`**

```bash
mkdir -p services/scheduler-service/src services/scheduler-service/tests
touch services/scheduler-service/src/__init__.py services/scheduler-service/tests/__init__.py
```

- [ ] **Step 4: Create `src/config.py`**

```python
# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    platform_api_url: str = "http://platform-api:8000"
    internal_secret: str = "internal_dev_secret"
    poll_interval_seconds: int = 30


settings = Settings()
```

- [ ] **Step 5: Verify the module imports cleanly**

Run from the scheduler-service directory:

```bash
cd services/scheduler-service
pip install -e ".[dev]"
python -c "from src.config import settings; print('ok')"
```
Expected: `ok`

- [ ] **Step 6: Commit**

```bash
git add services/scheduler-service/
git commit -m "feat: scheduler-service scaffolding — pyproject.toml, Dockerfile, config"
```

---

## Task 8: Job loader

**Files:**
- Create: `services/scheduler-service/src/job_loader.py`
- Create: `services/scheduler-service/tests/conftest.py`
- Create: `services/scheduler-service/tests/test_job_loader.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/conftest.py
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from apscheduler.schedulers.asyncio import AsyncIOScheduler


@pytest.fixture
def scheduler():
    s = AsyncIOScheduler()
    return s


@pytest.fixture
def mock_engine():
    return AsyncMock()
```

```python
# tests/test_job_loader.py
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger


def make_row(wf_id, tenant_id, trigger_config):
    row = MagicMock()
    row.id = wf_id
    row.tenant_id = tenant_id
    row.trigger_config = trigger_config
    return row


async def test_load_jobs_registers_cron_trigger(scheduler, mock_engine):
    from src.job_loader import load_jobs
    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    rows = [make_row(wf_id, tenant_id, {"type": "schedule", "cron": "0 9 * * MON", "timezone": "UTC"})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    await load_jobs(scheduler, mock_engine, AsyncMock())

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    assert isinstance(jobs[0].trigger, CronTrigger)


async def test_load_jobs_registers_interval_trigger(scheduler, mock_engine):
    from src.job_loader import load_jobs
    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    rows = [make_row(wf_id, tenant_id, {"type": "schedule", "interval_minutes": 15})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    await load_jobs(scheduler, mock_engine, AsyncMock())

    jobs = scheduler.get_jobs()
    assert len(jobs) == 1
    assert isinstance(jobs[0].trigger, IntervalTrigger)


async def test_load_jobs_skips_invalid_cron(scheduler, mock_engine, caplog):
    from src.job_loader import load_jobs
    import logging
    rows = [make_row(uuid.uuid4(), uuid.uuid4(), {"type": "schedule", "cron": "not-a-cron"})]

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=AsyncMock(__iter__=lambda s: iter(rows)))
    mock_engine.begin = MagicMock(return_value=mock_conn)

    with caplog.at_level(logging.WARNING):
        await load_jobs(scheduler, mock_engine, AsyncMock())

    assert scheduler.get_jobs() == []
    assert "invalid" in caplog.text.lower() or "skip" in caplog.text.lower()
```

Run: `cd services/scheduler-service && pytest tests/test_job_loader.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 2: Create `src/job_loader.py`**

```python
# src/job_loader.py
import logging
import uuid
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

_QUERY = text(
    "SELECT id, tenant_id, trigger_config "
    "FROM process_definitions "
    "WHERE (trigger_config->>'type') = 'schedule'"
)


def _build_trigger(trigger_config: dict) -> CronTrigger | IntervalTrigger | None:
    if "cron" in trigger_config:
        try:
            return CronTrigger.from_crontab(
                trigger_config["cron"],
                timezone=trigger_config.get("timezone", "UTC"),
            )
        except Exception as exc:
            logger.warning("Invalid cron '%s': %s — skipping", trigger_config["cron"], exc)
            return None
    if "interval_minutes" in trigger_config:
        return IntervalTrigger(minutes=int(trigger_config["interval_minutes"]))
    logger.warning("Unknown schedule config %s — skipping", trigger_config)
    return None


async def load_jobs(
    scheduler: AsyncIOScheduler,
    engine: AsyncEngine,
    redis: Redis,
) -> None:
    """Read all scheduled workflows from DB and register them as APScheduler jobs.

    Clears existing jobs first (safe to call on reload).
    """
    from src.dispatcher import dispatch

    # Remove existing scheduled jobs before reloading
    for job in scheduler.get_jobs():
        job.remove()

    async with engine.begin() as conn:
        result = await conn.execute(_QUERY)
        rows = list(result)

    for row in rows:
        trigger = _build_trigger(row.trigger_config)
        if trigger is None:
            continue
        scheduler.add_job(
            dispatch,
            trigger=trigger,
            args=[row.id, row.tenant_id, redis],
            id=str(row.id),
            replace_existing=True,
        )
        logger.info("Registered job for workflow %s", row.id)

    logger.info("Loaded %d scheduled jobs", len(scheduler.get_jobs()))
```

- [ ] **Step 3: Run tests**

Run: `cd services/scheduler-service && pytest tests/test_job_loader.py -v`
Expected: 3 PASSED

- [ ] **Step 4: Commit**

```bash
git add services/scheduler-service/src/job_loader.py \
        services/scheduler-service/tests/conftest.py \
        services/scheduler-service/tests/test_job_loader.py
git commit -m "feat: scheduler job loader — reads scheduled workflows from DB and registers APScheduler jobs"
```

---

## Task 9: Dispatcher (distributed lock + HTTP + retry)

**Files:**
- Create: `services/scheduler-service/src/dispatcher.py`
- Create: `services/scheduler-service/tests/test_dispatcher.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_dispatcher.py
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


async def test_dispatch_acquires_lock_calls_api_releases_lock():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)  # lock acquired
    mock_redis.delete = AsyncMock()

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("src.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatch(wf_id, tenant_id, mock_redis)

    mock_redis.set.assert_called_once()
    mock_client.post.assert_called_once()
    mock_redis.delete.assert_called_once()


async def test_dispatch_skips_when_lock_not_acquired():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=None)  # lock NOT acquired (None = NX failed)
    mock_redis.delete = AsyncMock()

    with patch("src.dispatcher.httpx.AsyncClient") as mock_client_cls:
        await dispatch(wf_id, tenant_id, mock_redis)

    mock_client_cls.assert_not_called()
    mock_redis.delete.assert_not_called()


async def test_dispatch_releases_lock_even_on_api_failure():
    from src.dispatcher import dispatch

    wf_id = uuid.uuid4()
    tenant_id = uuid.uuid4()

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

    with patch("src.dispatcher.httpx.AsyncClient", return_value=mock_client):
        await dispatch(wf_id, tenant_id, mock_redis)  # must not raise

    mock_redis.delete.assert_called_once()
```

Run: `cd services/scheduler-service && pytest tests/test_dispatcher.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 2: Create `src/dispatcher.py`**

```python
# src/dispatcher.py
import asyncio
import logging
import uuid

import httpx
from redis.asyncio import Redis

from src.config import settings

logger = logging.getLogger(__name__)

_LOCK_TTL = 55  # seconds — less than the minimum 1-minute interval
_MAX_RETRIES = 3


async def dispatch(workflow_id: uuid.UUID, tenant_id: uuid.UUID, redis: Redis) -> None:
    """Fire a scheduled workflow execution via Platform API's internal endpoint.

    Uses a Redis distributed lock (SET NX EX) to prevent duplicate executions
    when multiple scheduler-service instances are running.
    """
    lock_key = f"scheduler:lock:{workflow_id}"
    acquired = await redis.set(lock_key, "1", nx=True, ex=_LOCK_TTL)
    if not acquired:
        logger.info("Lock not acquired for workflow %s — another instance is handling it", workflow_id)
        return

    try:
        await _dispatch_with_retry(workflow_id)
    finally:
        await redis.delete(lock_key)


async def _dispatch_with_retry(workflow_id: uuid.UUID) -> None:
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{settings.platform_api_url}/internal/executions",
                    json={"process_definition_id": str(workflow_id), "context": {}},
                    headers={"X-Internal-Secret": settings.internal_secret},
                )
                resp.raise_for_status()
            logger.info("Dispatched workflow %s → execution created", workflow_id)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1)  # 2s, 4s
                logger.warning(
                    "Dispatch attempt %d/%d failed for workflow %s: %s — retrying in %ds",
                    attempt + 1, _MAX_RETRIES, workflow_id, exc, wait,
                )
                await asyncio.sleep(wait)

    logger.error(
        "All %d dispatch attempts failed for workflow %s: %s",
        _MAX_RETRIES, workflow_id, last_exc,
    )
```

- [ ] **Step 3: Run tests**

Run: `cd services/scheduler-service && pytest tests/test_dispatcher.py -v`
Expected: 3 PASSED

- [ ] **Step 4: Run full scheduler test suite**

Run: `cd services/scheduler-service && pytest tests/ -v --cov=src`
Expected: 6 PASSED

- [ ] **Step 5: Commit**

```bash
git add services/scheduler-service/src/dispatcher.py \
        services/scheduler-service/tests/test_dispatcher.py
git commit -m "feat: scheduler dispatcher — Redis distributed lock and HTTP retry logic"
```

---

## Task 10: Worker entry point + docker-compose

**Files:**
- Create: `services/scheduler-service/src/worker.py`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Create `src/worker.py`**

```python
# src/worker.py
"""Scheduler service entry point.

Starts the APScheduler loop and a background task that periodically reloads
scheduled workflows from the DB. Reload also happens at startup.

Horizontal scaling: multiple instances can run safely — the Redis distributed
lock in dispatcher.py prevents duplicate executions.
"""
import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import settings
from src.job_loader import load_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


async def _periodic_reload(
    scheduler: AsyncIOScheduler,
    engine,
    redis: Redis,
    interval: int,
) -> None:
    """Reload scheduled jobs every `interval` seconds as a fallback."""
    while True:
        await asyncio.sleep(interval)
        logger.info("Reloading scheduled jobs from DB...")
        try:
            await load_jobs(scheduler, engine, redis)
        except Exception as exc:
            logger.error("Failed to reload jobs: %s", exc)


async def main() -> None:
    engine = create_async_engine(settings.database_url, echo=False)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)

    scheduler = AsyncIOScheduler()

    # Initial load
    logger.info("Loading scheduled jobs...")
    try:
        await load_jobs(scheduler, engine, redis)
    except Exception as exc:
        logger.error("Initial job load failed: %s — will retry on next poll", exc)

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))

    try:
        await _periodic_reload(scheduler, engine, redis, settings.poll_interval_seconds)
    finally:
        scheduler.shutdown(wait=False)
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Add `scheduler-service` to `docker-compose.yml`**

Add the following service block. In the existing `docker-compose.yml`, add `INTERNAL_SECRET` to platform-api's environment and append the new service.

Find the `platform-api` service's `environment:` block and add:
```yaml
      INTERNAL_SECRET: internal_dev_secret
```

Then add the new service (place it after websocket-service):

```yaml
  scheduler-service:
    build:
      context: ./services/scheduler-service
      dockerfile: Dockerfile
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      platform-api:
        condition: service_started
    environment:
      DATABASE_URL: postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
      REDIS_URL: redis://:redis_dev_password@redis:6379/0
      PLATFORM_API_URL: http://platform-api:8000
      INTERNAL_SECRET: internal_dev_secret
      POLL_INTERVAL_SECONDS: "30"
    restart: unless-stopped
```

- [ ] **Step 3: Build and bring up the new service**

```bash
docker compose build scheduler-service
docker compose up -d scheduler-service
```

- [ ] **Step 4: Verify it starts cleanly**

```bash
docker logs constructor-scheduler-service-1 --tail=20
```

Expected output (no errors):
```
... INFO src.worker Loading scheduled jobs...
... INFO src.worker Scheduler started with N jobs
```

- [ ] **Step 5: Smoke test — create a scheduled workflow via API and verify the scheduler picks it up**

```bash
# Get token
TOKEN=$(curl -s -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@demo.com","password":"admin123","tenant_slug":"demo"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Create a workflow with schedule trigger (every minute for testing)
curl -s -X POST http://localhost:8000/workflows \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Scheduled Test",
    "trigger_config": {"type": "schedule", "interval_minutes": 1},
    "steps": [{"id": "s1", "type": "agent", "config": {"task_type": "default", "prompt": "hello", "tools_allowed": []}, "next": null, "timeout_seconds": 30}],
    "on_error": "stop"
  }' | python3 -m json.tool

# Wait for next reload cycle, then check logs
sleep 35
docker logs constructor-scheduler-service-1 --tail=10
```

Expected: logs show "Registered job for workflow <id>" and "Scheduler started with 1 jobs"

- [ ] **Step 6: Smoke test — trigger via webhook**

```bash
# Create a webhook for the same workflow
WF_ID=<workflow_id_from_above>
WH=$(curl -s -X POST http://localhost:8000/webhooks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"workflow_id\": \"$WF_ID\", \"name\": \"Test WH\", \"payload_mapping\": {\"msg\": \"message\"}}")
WH_ID=$(echo $WH | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
SECRET=$(echo $WH | python3 -c "import sys,json; print(json.load(sys.stdin)['secret'])")

# Sign and trigger
BODY='{"message": "hello from webhook"}'
SIG=$(python3 -c "import hmac,hashlib; print('sha256=' + hmac.new('$SECRET'.encode(), b'$BODY', hashlib.sha256).hexdigest())")
curl -s -X POST "http://localhost:8000/webhooks/$WH_ID/trigger" \
  -H "Content-Type: application/json" \
  -H "X-Constructor-Signature: $SIG" \
  -d "$BODY" | python3 -m json.tool
```

Expected: `{"execution_id": "...", "status": "pending"}`

- [ ] **Step 7: Commit**

```bash
git add services/scheduler-service/src/worker.py docker-compose.yml
git commit -m "feat: scheduler worker entry point and docker-compose integration"
```
