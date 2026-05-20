import pytest
import hmac as _hmac
import hashlib
import secrets


def test_webhook_config_model_has_expected_fields():
    from src.webhooks.models import WebhookConfig
    cols = {c.key for c in WebhookConfig.__table__.columns}
    assert {"id", "tenant_id", "workflow_id", "name", "secret",
            "payload_mapping", "is_active", "created_at"} <= cols


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
        "trigger": {"type": "webhook"},
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
