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
        "trigger": {"type": "webhook"},
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
