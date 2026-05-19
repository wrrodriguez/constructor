# tests/test_e2e_execution.py
"""
End-to-end: Platform API dispara workflow con un agent step.
Agent Orchestration (mockeado via Redis) procesa y publica resultado.
Platform API recibe resultado y completa la ejecución.
"""
import asyncio
import pytest
from src.executions.events import AgentTask, AgentResult

TASK_STREAM = "constructor:agent.task.created"
RESULT_STREAM = "constructor:agent.result.ready"


async def fake_agent_worker(redis, timeout: float = 15.0):
    """Simula Agent Orchestration: lee tasks y publica resultados de inmediato."""
    deadline = asyncio.get_event_loop().time() + timeout
    try:
        await redis.xgroup_create(TASK_STREAM, "fake-agent", id="0", mkstream=True)
    except Exception:
        pass

    while asyncio.get_event_loop().time() < deadline:
        messages = await redis.xreadgroup(
            "fake-agent", "worker-1",
            {TASK_STREAM: ">"},
            count=5, block=500,
        )
        for _stream, entries in (messages or []):
            for entry_id, fields in entries:
                task = AgentTask.model_validate_json(fields["data"])
                result = AgentResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    tenant_id=task.tenant_id,
                    step_id=task.step_id,
                    status="completed",
                    output={"text": "E2E agent response"},
                    tokens_used=42,
                    model_used="claude-sonnet-4-6",
                    iterations=1,
                    error=None,
                )
                await redis.xadd(RESULT_STREAM, {"data": result.model_dump_json()})
                await redis.xack(TASK_STREAM, "fake-agent", entry_id)


@pytest.mark.asyncio
async def test_full_workflow_execution(seeded_user, e2e_client, seeded_workflow_definition, db_redis):
    """
    e2e_client parchea Redis y DB antes de que el lifespan de FastAPI arranque,
    garantizando que el result consumer use el TestContainer Redis.
    """
    # Login
    login_resp = await e2e_client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    assert login_resp.status_code == 200, login_resp.text
    token = login_resp.json()["access_token"]

    # Arrancar fake agent worker en background
    worker_task = asyncio.create_task(fake_agent_worker(db_redis, timeout=15.0))

    # Disparar ejecución
    trigger_resp = await e2e_client.post(
        "/executions",
        json={"process_definition_id": str(seeded_workflow_definition), "context": {"input": "test"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert trigger_resp.status_code == 202, trigger_resp.text
    execution_id = trigger_resp.json()["id"]

    # Polling hasta que la ejecución complete (máx 10 segundos)
    for _ in range(20):
        await asyncio.sleep(0.5)
        status_resp = await e2e_client.get(
            f"/executions/{execution_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if status_resp.json()["status"] == "completed":
            break

    worker_task.cancel()

    final_resp = await e2e_client.get(
        f"/executions/{execution_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    data = final_resp.json()
    assert data["status"] == "completed", (
        f"Expected completed, got {data['status']}. Context: {data.get('context')}"
    )
    # El context debe incluir el output del agent step
    assert any("E2E agent response" in str(v) for v in data["context"].values())
