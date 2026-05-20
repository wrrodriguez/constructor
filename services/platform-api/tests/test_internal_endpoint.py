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
