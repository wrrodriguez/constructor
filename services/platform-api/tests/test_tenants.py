# tests/test_tenants.py
import pytest


@pytest.mark.asyncio
@pytest.mark.integration
async def test_list_tenants_requires_auth(client):
    resp = await client.get("/tenants")
    assert resp.status_code == 401
