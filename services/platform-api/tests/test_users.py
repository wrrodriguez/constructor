# tests/test_users.py
import pytest


@pytest.mark.asyncio
@pytest.mark.integration
async def test_users_me_requires_auth(client):
    resp = await client.get("/users/me")
    assert resp.status_code == 401


@pytest.mark.asyncio
@pytest.mark.integration
async def test_create_user_requires_auth(client):
    resp = await client.post("/users", json={
        "email": "new@example.com",
        "password": "pass123",
    })
    assert resp.status_code == 401
