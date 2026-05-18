# tests/test_workflows.py
import pytest
from src.workflows.validator import validate_steps


def test_valid_steps_returns_no_errors():
    steps = [
        {"id": "s1", "type": "agent", "config": {}, "next": None, "timeout_seconds": 60}
    ]
    assert validate_steps(steps) == []


def test_empty_steps_returns_error():
    assert validate_steps([]) != []


def test_invalid_step_type_returns_error():
    steps = [{"id": "s1", "type": "bad_type", "config": {}, "next": None, "timeout_seconds": 60}]
    errors = validate_steps(steps)
    assert any("invalid type" in e for e in errors)


def test_missing_step_id_returns_error():
    steps = [{"type": "agent", "config": {}, "next": None, "timeout_seconds": 60}]
    errors = validate_steps(steps)
    assert any("'id' is required" in e for e in errors)


def test_zero_timeout_returns_error():
    steps = [{"id": "s1", "type": "agent", "config": {}, "next": None, "timeout_seconds": 0}]
    errors = validate_steps(steps)
    assert any("timeout_seconds" in e for e in errors)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_create_workflow_requires_auth(client):
    resp = await client.post("/workflows", json={})
    assert resp.status_code == 401


@pytest.mark.asyncio
@pytest.mark.integration
async def test_list_workflows_requires_auth(client):
    resp = await client.get("/workflows")
    assert resp.status_code == 401
