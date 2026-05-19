# tests/test_events.py
import uuid
from src.events import AgentTask, AgentResult


def test_agent_task_roundtrip():
    task = AgentTask(
        task_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        execution_id=uuid.UUID("87654321-4321-8765-4321-876543218765"),
        tenant_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        step_id="step_1",
        task_type="code_analysis",
        prompt="Analyze this code",
        context={"input": "def foo(): pass"},
        tools_allowed=["http_generic"],
    )
    restored = AgentTask.model_validate_json(task.model_dump_json())
    assert restored.task_id == task.task_id
    assert restored.context == task.context
    assert restored.max_iterations == 20  # default


def test_agent_result_failed_status():
    result = AgentResult(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="step_1",
        status="failed",
        output={},
        tokens_used=0,
        model_used="claude-sonnet-4-6",
        iterations=0,
        error="Something went wrong",
    )
    assert result.status == "failed"
    assert result.error == "Something went wrong"


def test_agent_task_model_override_default_none():
    task = AgentTask(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="s1",
        task_type="default",
        prompt="do something",
        context={},
        tools_allowed=[],
    )
    assert task.model_override is None
    assert task.timeout_seconds == 300
