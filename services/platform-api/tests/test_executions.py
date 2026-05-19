# tests/test_executions.py
import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from src.main import app


@pytest.mark.asyncio
async def test_trigger_execution_requires_auth():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/executions", json={"process_definition_id": str(uuid.uuid4()), "context": {}})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_trigger_execution_invalid_definition(seeded_user, client):
    """Dispara ejecución con definition_id inexistente → 404."""
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]
    resp = await client.post(
        "/executions",
        json={"process_definition_id": str(uuid.uuid4()), "context": {}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_executions_empty(seeded_user, client):
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]
    resp = await client.get("/executions", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
